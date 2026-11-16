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
import * as fs from 'fs';
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
  /**
   * PARITY (filesystem write-deny — Claude's `SandboxFilesystemConfig.denyWrite`
   * / `getFsWriteConfig.denyWithinAllow`): absolute paths that must stay READ-ONLY
   * inside the container even though they sit inside the otherwise-writable project
   * mount. Each existing path is bind-remounted `:ro` ON TOP of the project RW
   * mount — a more-specific nested bind shadows the broader one, so a sandboxed
   * command cannot write it (e.g. protect `project/.git` or `project/.env` while
   * the rest of the project stays writable). Non-existing paths are skipped so
   * Docker does not auto-create a spurious empty source on the host.
   */
  denyWritePaths?: string[];
  /**
   * PARITY (filesystem read-deny — Claude's `SandboxFilesystemConfig.denyRead`):
   * absolute FILE paths hidden from reads inside the container by bind-mounting
   * `/dev/null` over each (a read returns empty). Only existing regular files are
   * applied; directories/missing paths are skipped.
   */
  denyReadPaths?: string[];
  /**
   * PARITY (filesystem write-allow — Claude's `SandboxFilesystemConfig.allowWrite`
   * + the `/add-dir` sandbox-mount sync): extra absolute paths bind-mounted
   * WRITABLE in addition to the project. Used to widen the container's writable set
   * to a directory granted outside the project (e.g. via `/add-dir`). Only existing
   * paths outside the project mount are added.
   */
  allowWritePaths?: string[];
  /**
   * PARITY (Unix-socket allowlist — Claude's
   * `SandboxNetworkConfig.allowUnixSockets` / `getAllowUnixSockets`): host
   * Unix-socket paths explicitly GRANTED into the container. The container mounts
   * NO host socket by default (deny-all — the Docker daemon, SSH/GPG-agent sockets
   * are unreachable, closing a classic sandbox-escape vector), so only the paths
   * listed here are bind-mounted in. Only existing sockets are applied.
   */
  allowUnixSockets?: string[];
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
 * Project-relative paths that are auto-discovered, auto-loaded, and executed LATER
 * with full agent privilege — the settings files that feed the sandbox policy
 * itself, and the skills/commands/agents directories whose contents run as the
 * agent. A sandboxed command that rewrites any of these escapes the sandbox on the
 * next load (flip `sandbox.mode` off, plant a project-tier SKILL.md/agent that then
 * loads at highest priority, …). The container therefore mounts each of them
 * read-only. Mirrors Claude's unconditional `denyWrite` of settings.json +
 * `.claude/skills` / `commands` / `agents` (CVE-class config-rewrite / privilege-
 * escalation escape).
 */
export const PRIVILEGED_AUTOLOAD_RELPATHS: readonly string[] = [
  '.claude/settings.json',
  '.claude/settings.local.json',
  '.claude/skills',
  '.claude/commands',
  '.claude/agents',
  '.ami/settings.json',
  '.ami/settings.local.json',
  '.ami/skills',
  '.ami/commands',
  '.ami/agents',
];

/**
 * The always-on read-only carve-out set for a project: every
 * {@link PRIVILEGED_AUTOLOAD_RELPATHS} entry that actually EXISTS under
 * `projectDir`. Non-existing paths are omitted so Docker does not auto-create a
 * spurious empty source directory on the host. Exported for the `/sandbox`
 * introspection surface and tests.
 */
export function defaultDenyWritePaths(projectDir: string): string[] {
  const project = path.resolve(projectDir);
  return PRIVILEGED_AUTOLOAD_RELPATHS
    .map((rel) => path.join(project, rel))
    .filter((p) => fs.existsSync(p));
}

/**
 * Recover a git worktree's MAIN-repo root from `cwd/.git`.
 *
 * A linked worktree has a `.git` FILE (not a directory) of the form
 * `gitdir: <mainrepo>/.git/worktrees/<name>`. When `cwd` is such a worktree, the
 * main repo's `.git` lives OUTSIDE the project mount, so a sandboxed `git commit`
 * / `git add` cannot write `index.lock` etc. and fails. This parses the `.git`
 * file, resolves the gitdir, and uses `lastIndexOf` of the exact marker
 * `${sep}.git${sep}worktrees${sep}` (deliberately NOT `indexOf('.git')`, which
 * would false-match paths like `/home/user/.github-projects/`) to recover the main
 * repo root so it can be added to the writable mount set. Returns `null` when
 * `cwd` is not a linked worktree. Mirrors Claude's `detectWorktreeMainRepoPath`.
 */
export function detectWorktreeMainRepoPath(cwd: string): string | null {
  const gitPath = path.join(path.resolve(cwd || '.'), '.git');
  let content: string;
  try {
    if (!fs.statSync(gitPath).isFile()) return null; // a real .git dir → not a linked worktree
    content = fs.readFileSync(gitPath, 'utf8');
  } catch {
    return null;
  }
  const m = content.match(/^gitdir:\s*(.+)$/m);
  if (!m) return null;
  let gitdir = m[1].trim();
  if (!path.isAbsolute(gitdir)) gitdir = path.resolve(cwd, gitdir);
  const marker = `${path.sep}.git${path.sep}worktrees${path.sep}`;
  const idx = gitdir.lastIndexOf(marker);
  if (idx === -1) return null;
  return gitdir.slice(0, idx);
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

  // PARITY (write-deny carve-outs): bind-remount each privileged-autoload default
  // (settings/skills/commands/agents) PLUS any operator-configured denyWrite path
  // read-only ON TOP of the project RW mount. A nested, more-specific bind shadows
  // the broader project mount, so these stay unwritable inside an otherwise-writable
  // project — closing the config-rewrite / SKILL-plant sandbox-escape vectors.
  const denyWriteSeen = new Set<string>();
  for (const p of [...defaultDenyWritePaths(project), ...(cfg.denyWritePaths ?? [])]) {
    const abs = path.resolve(p);
    if (denyWriteSeen.has(abs) || !fs.existsSync(abs)) continue;
    denyWriteSeen.add(abs);
    parts.push('-v', shSingleQuote(`${abs}:${abs}:ro`));
  }

  // PARITY (write-allow carve-outs): widen the writable set to directories granted
  // OUTSIDE the project (e.g. via /add-dir), and auto-include a git worktree's
  // main-repo root so sandboxed `git` can write the shared `.git` (index.lock, the
  // worktree's own `.git/worktrees/<name>` metadata). Paths inside the project are
  // already writable, and missing paths are skipped (Docker would auto-create them).
  const allowWriteSeen = new Set<string>([project]);
  const mainRepo = detectWorktreeMainRepoPath(workdir);
  for (const p of [...(mainRepo ? [mainRepo] : []), ...(cfg.allowWritePaths ?? [])]) {
    const abs = path.resolve(p);
    if (allowWriteSeen.has(abs) || abs === project || abs.startsWith(project + path.sep)) continue;
    if (!fs.existsSync(abs)) continue;
    allowWriteSeen.add(abs);
    parts.push('-v', shSingleQuote(`${abs}:${abs}`));
  }

  // PARITY (read-deny): hide specific files from reads inside the container by
  // bind-mounting /dev/null over each existing regular file.
  for (const p of cfg.denyReadPaths ?? []) {
    const abs = path.resolve(p);
    let isFile = false;
    try { isFile = fs.statSync(abs).isFile(); } catch { isFile = false; }
    if (isFile) parts.push('-v', shSingleQuote(`/dev/null:${abs}:ro`));
  }

  // PARITY (Unix-socket allowlist): the container mounts NO host socket by default
  // (deny-all — Docker daemon / agent sockets are unreachable). Grant ONLY the
  // existing sockets the operator explicitly allowlisted.
  const socketSeen = new Set<string>();
  for (const s of cfg.allowUnixSockets ?? []) {
    const abs = path.resolve(s);
    if (socketSeen.has(abs) || !fs.existsSync(abs)) continue;
    socketSeen.add(abs);
    parts.push('-v', shSingleQuote(`${abs}:${abs}`));
  }

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
