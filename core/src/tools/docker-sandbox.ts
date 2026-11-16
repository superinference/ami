/**
 * Docker isolation backend for bash execution.
 *
 * When the sandbox policy is `mode: 'container'`, every bash command is wrapped
 * so it runs inside an ephemeral `docker run --rm` container whose ONLY writable
 * mount is the project directory. The host $HOME, `/`, SSH keys, and every other
 * path are NOT mounted, so a destructive command (`rm -rf /`, `rm -rf ~`,
 * `rm -rf $HOME`, `find / -delete`, …) can only touch the container's ephemeral
 * filesystem or the mounted project — never the real machine.
 *
 * The wrapped string is a host-level `docker run …` command: the host only needs
 * the `docker` CLI on PATH (and DOCKER_HOST for rootless). The actual work runs
 * inside the image.
 *
 * If Docker is unavailable, callers MUST fail closed (refuse to run) rather than
 * fall back to the host — that is the whole point of enforcement.
 */
import * as path from 'path';
import { shellHooks } from '../utils/shell';

// The project's own agent sandbox image, built from the core submodule's
// Dockerfile (core/Dockerfile → `openshell-ami`) and published by
// superinference/ci's openshell.yml to GHCR. It ships the full agent toolchain
// (Node 26, Python 3, git, build tools) on the NVIDIA OpenShell base. Using it
// here keeps the local bash sandbox consistent with the published agent image.
export const DEFAULT_DOCKER_IMAGE = 'ghcr.io/superinference/openshell-ami:latest';

export interface DockerSandboxConfig {
  image: string;
  /** Absolute path to the only writable mount (the project/workspace root). */
  projectDir: string;
  allowNetwork: boolean;
  maxMemoryMB?: number;
  maxProcesses?: number;
}

/**
 * Why a Docker availability probe failed. Collapsing every failure into a single
 * boolean hides the actual thing the user must fix; this classification lets the
 * fail-closed message name the real cause (CLI missing vs. permission vs. a slow
 * or unreachable daemon).
 */
export type DockerUnavailableCause =
  | 'cli-missing'
  | 'permission-denied'
  | 'daemon-slow'
  | 'daemon-unreachable'
  | 'aborted';

/** Structured result of a Docker availability probe. */
export interface DockerAvailability {
  available: boolean;
  /** Set only when `available` is false — the classified failure cause. */
  cause?: DockerUnavailableCause;
  /** Raw error text captured from the probe, for diagnostics. */
  detail?: string;
}

/** Options for the Docker availability probe. */
export interface DockerProbeOptions {
  /** If already aborted, the probe fails fast without shelling out to a (possibly
   *  hung) `docker version` call, so a user Ctrl-C mid-probe is honored. */
  signal?: AbortSignal;
  /** Override the probe timeout (ms). Defaults to {@link DOCKER_PROBE_TIMEOUT_MS}. */
  timeoutMs?: number;
}

/** Default wall-clock budget for a single `docker version` probe. */
export const DOCKER_PROBE_TIMEOUT_MS = 5000;

/** Pinned temp dir inside the container. The container's `/tmp` is a tmpfs mount
 *  (see {@link wrapWithDocker}); pinning `TMPDIR` to it as the LAST `-e` flag
 *  guarantees sandboxed commands write temp files into the writable jail and that
 *  an image `ENV TMPDIR=…` pointing elsewhere cannot repoint them out of it. */
export const DOCKER_SANDBOX_TMPDIR = '/tmp';

let _dockerAvailable: boolean | null = null;
let _dockerCause: DockerUnavailableCause | undefined;
let _dockerDetail: string | undefined;
let _dockerProbedAt = 0;

/**
 * How long a NEGATIVE availability result is trusted before it is re-probed.
 *
 * A positive probe is cached for the whole session — a reachable daemon is
 * stable, so there is no reason to keep shelling out `docker version`. A
 * negative probe, however, must be *recoverable*: if the first command runs
 * before the daemon is up (or the user starts rootless Docker in response to a
 * fail-closed hint), permanently caching `false` would wedge container mode off
 * for the rest of the session even though Docker is now reachable. Re-probing a
 * stale negative result lets a recovered daemon be picked up on the next
 * command, while the short TTL throttles the 5s probe so a genuinely-absent
 * daemon is not re-shelled on every call in a tight loop.
 */
export const DOCKER_NEGATIVE_PROBE_TTL_MS = 5000;

/**
 * Test/clock seam so the negative-cache TTL can be exercised deterministically
 * without real wall-clock waits. Overriding `now` lets a test advance time past
 * {@link DOCKER_NEGATIVE_PROBE_TTL_MS} to force a re-probe.
 */
export const _dockerProbeHooks = { now: (): number => Date.now() };

/**
 * Classify a caught probe error into a {@link DockerUnavailableCause}. ENOENT
 * means the `docker` CLI is not on PATH; EACCES/permission denied is the rootless
 * socket not being accessible; a timeout kill (`killed`/SIGTERM/ETIMEDOUT) means
 * the daemon did not answer in the budget (starting up or a hung socket); any
 * other non-zero exit means the daemon is unreachable/down.
 */
function classifyDockerError(e: unknown): DockerUnavailableCause {
  const err = e as { code?: string; killed?: boolean; signal?: string; message?: string } | null;
  const msg = String(err?.message ?? e ?? '');
  if (err?.code === 'ENOENT' || /\bENOENT\b/.test(msg)) return 'cli-missing';
  if (err?.code === 'EACCES' || /\bEACCES\b/.test(msg) || /permission denied/i.test(msg)) {
    return 'permission-denied';
  }
  if (
    err?.killed === true ||
    err?.signal === 'SIGTERM' ||
    err?.code === 'ETIMEDOUT' ||
    /\bETIMEDOUT\b|timed?\s*out/i.test(msg)
  ) {
    return 'daemon-slow';
  }
  return 'daemon-unreachable';
}

/**
 * Probe Docker availability and return a STRUCTURED result (available + cause).
 *
 * A positive result is cached for the session; a negative result is cached only
 * for {@link DOCKER_NEGATIVE_PROBE_TTL_MS} and then re-probed, so a daemon that
 * becomes reachable mid-session is recovered automatically. When a probe fails,
 * the caught error is classified so callers can tell the user the real cause
 * (CLI missing vs. permission vs. slow/unreachable daemon) instead of one
 * one-size message. An already-aborted `signal` fails the probe fast without
 * shelling out to a possibly-hung `docker version`.
 */
export function getDockerAvailability(opts: DockerProbeOptions = {}): DockerAvailability {
  // A reachable daemon is stable — cache the positive result for the session.
  if (_dockerAvailable === true) return { available: true };
  // Honor a cancellation before the (synchronous, uninterruptible) probe runs, so
  // a user abort during a hung daemon socket is respected instead of blocking for
  // the full timeout. This does not poison the cache — the next call re-probes.
  if (opts.signal?.aborted) {
    return { available: false, cause: 'aborted', detail: 'probe aborted before completion' };
  }
  // Trust a negative result only within the TTL window; past it, re-probe so a
  // recovered daemon/CLI is picked up instead of staying wedged off.
  if (
    _dockerAvailable === false &&
    _dockerProbeHooks.now() - _dockerProbedAt < DOCKER_NEGATIVE_PROBE_TTL_MS
  ) {
    return { available: false, cause: _dockerCause, detail: _dockerDetail };
  }
  try {
    shellHooks.execSync('docker version --format "{{.Server.Version}}"', {
      stdio: 'pipe',
      timeout: opts.timeoutMs && opts.timeoutMs > 0 ? opts.timeoutMs : DOCKER_PROBE_TIMEOUT_MS,
    });
    _dockerAvailable = true;
    _dockerCause = undefined;
    _dockerDetail = undefined;
    return { available: true };
  } catch (e) {
    _dockerAvailable = false;
    _dockerProbedAt = _dockerProbeHooks.now();
    _dockerCause = classifyDockerError(e);
    _dockerDetail = String((e as { message?: string } | null)?.message ?? e).slice(0, 400);
    return { available: false, cause: _dockerCause, detail: _dockerDetail };
  }
}

/**
 * True if a Docker daemon is reachable. Boolean accessor over
 * {@link getDockerAvailability} for callers that only need yes/no (and preserves
 * the original signature for existing consumers).
 */
export function isDockerAvailable(opts?: DockerProbeOptions): boolean {
  return getDockerAvailability(opts).available;
}

/**
 * Human-readable, cause-specific remediation for a failed Docker probe. Used in
 * the fail-closed bash message so a refusal tells the user what to actually fix.
 */
export function describeDockerUnavailable(a: DockerAvailability): string {
  switch (a.cause) {
    case 'cli-missing':
      return 'the `docker` CLI is not on PATH — install Docker (for rootless, also set '
        + 'DOCKER_HOST=unix:///run/user/$UID/docker.sock)';
    case 'permission-denied':
      return 'the Docker socket is not accessible (permission denied) — add your user to the '
        + '`docker` group, or set DOCKER_HOST to your rootless socket '
        + '(unix:///run/user/$UID/docker.sock)';
    case 'daemon-slow':
      return 'the Docker daemon did not respond in time — it may still be starting; retry, or '
        + 'check `docker version`';
    case 'aborted':
      return 'the availability probe was cancelled before it completed';
    default:
      return 'no Docker daemon is reachable — start Docker (rootless: `systemctl --user start docker`)';
  }
}

/** Signatures in stderr that indicate an outbound network call was blocked. */
const NETWORK_OFF_SIGNATURES: RegExp[] = [
  /could not resolve host/i,
  /temporary failure in name resolution/i,
  /name or service not known/i,
  /network is unreachable/i,
  /\bENETUNREACH\b/,
  /\bEAI_AGAIN\b/i,
  /getaddrinfo\b/i,
  /connection refused/i,
  /failed to connect/i,
];

/** Container-internal tmpfs mounts that DO exist inside the sandbox, so a missing
 *  path under them is NOT a sandbox-isolation artifact. */
const CONTAINER_TMPFS_PREFIXES = ['/tmp', '/root', '/home'];

/** True if `stderr` references an absolute path that is NOT inside the project
 *  mount nor one of the container tmpfs dirs — i.e. a host path that genuinely
 *  does not exist inside the container. */
function mentionsPathOutsideProject(stderr: string, projectDir: string): boolean {
  const project = path.resolve(projectDir);
  const paths = stderr.match(/\/[^\s'":]+/g) || [];
  return paths.some((p) => {
    if (p === project || p.startsWith(project + path.sep)) return false;
    if (CONTAINER_TMPFS_PREFIXES.some((t) => p === t || p.startsWith(t + '/'))) return false;
    return true;
  });
}

/**
 * Post-process a failed container-mode command's stderr to explain that the
 * failure may be a SANDBOX restriction rather than a genuine error, so the model
 * does not misdiagnose (retry blindly, or treat "No such file" as a real missing
 * file). Appends a note when network is off and the stderr shows a network
 * failure, and/or when an absolute path outside the project mount is reported
 * missing. Returns the stderr unchanged when nothing matches.
 */
export function annotateStderrWithSandboxFailures(
  command: string,
  stderr: string,
  cfg: { allowNetwork: boolean; projectDir: string },
): string {
  if (!stderr) return stderr;
  const notes: string[] = [];
  if (!cfg.allowNetwork && NETWORK_OFF_SIGNATURES.some((re) => re.test(stderr))) {
    notes.push(
      '[sandbox: network is disabled for this container (--network none); the error above may be '
        + 'caused by blocked network access, not a real failure. Retry with '
        + 'dangerouslyDisableSandbox: true if this command legitimately needs the network.]',
    );
  }
  if (/no such file or directory/i.test(stderr) && mentionsPathOutsideProject(stderr, cfg.projectDir)) {
    notes.push(
      `[sandbox: only the project directory (${path.resolve(cfg.projectDir)}) is mounted inside the `
        + 'container; files outside it do not exist there, so a "No such file or directory" above may '
        + 'be caused by sandbox isolation rather than a genuinely missing file.]',
    );
  }
  if (notes.length === 0) return stderr;
  return `${stderr}\n${notes.join('\n')}`;
}

/** Test seam: reset the cached availability probe. */
export function _resetDockerAvailableCache(): void {
  _dockerAvailable = null;
  _dockerCause = undefined;
  _dockerDetail = undefined;
  _dockerProbedAt = 0;
}

/** POSIX single-quote a string for safe inclusion in a `bash -c` host command. */
function shSingleQuote(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

/**
 * Build the host command string that runs `command` inside a locked-down
 * container. Only `projectDir` is bind-mounted read-write; `/tmp`, `/root`, and
 * `/home` are tmpfs; the working directory is clamped to inside the project.
 */
export function wrapWithDocker(command: string, cwd: string, cfg: DockerSandboxConfig): string {
  const project = path.resolve(cfg.projectDir);
  const resolvedCwd = path.resolve(cwd || project);
  // Clamp the container workdir to inside the project mount; anything outside is
  // not mounted and would not exist in the container.
  const workdir =
    resolvedCwd === project || resolvedCwd.startsWith(project + path.sep) ? resolvedCwd : project;

  const parts: string[] = [
    'docker', 'run', '--rm', '-i',
    // Rootless Docker maps container UID 0 → the invoking host user, so --user 0:0
    // lets the agent write the bind-mounted project (files stay owned by the host
    // user) while the host filesystem remains unreachable. --entrypoint bash runs
    // the command directly, overriding any image ENTRYPOINT (e.g. the openshell-ami
    // agent launcher) so the image doubles as a plain bash sandbox.
    '--user', '0:0',
    '--entrypoint', 'bash',
    '--network', cfg.allowNetwork ? 'bridge' : 'none',
    '--tmpfs', '/tmp',
    '--tmpfs', '/root',
    '--tmpfs', '/home',
    '-v', shSingleQuote(`${project}:${project}`),
    '-w', shSingleQuote(workdir),
  ];
  if (cfg.maxMemoryMB && cfg.maxMemoryMB > 0) {
    parts.push('--memory', `${cfg.maxMemoryMB}m`);
  }
  if (cfg.maxProcesses && cfg.maxProcesses > 0) {
    parts.push('--pids-limit', String(cfg.maxProcesses));
  }
  // Pin TMPDIR into the container tmpfs as the LAST -e so it wins over any image
  // `ENV TMPDIR=…`: sandboxed commands always write temp files into the writable
  // jail (the /tmp tmpfs), and the sandbox temp location cannot be repointed out
  // of it by image-baked env.
  parts.push('-e', shSingleQuote(`TMPDIR=${DOCKER_SANDBOX_TMPDIR}`));
  // Command runs as the entrypoint (bash) args: `… IMAGE -c '<command>'`.
  parts.push(shSingleQuote(cfg.image), '-c', shSingleQuote(command));
  return parts.join(' ');
}
