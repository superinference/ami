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
import type { SandboxPolicy } from '../types';
import { shellHooks } from '../utils/shell';
import {
  BARE_GIT_REPO_FILES,
  isValidProxyPort,
  buildNetworkAllowlist,
  buildDeniedDomainRules,
} from './bash-sandbox';
import { sandboxViolationStore, wrapSandboxViolations } from './sandbox-violations';
import { isSandboxHostGranted, getGrantedSandboxHosts } from './sandbox-permission-registry';
import { persistPositiveCapability } from '../reference-journal';

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
  /**
   * PARITY (per-host egress denylist — the reference's `SandboxNetworkConfig.deniedHosts`,
   * "deny wins over allow"): hostnames blocked for outbound network INSIDE the
   * container even when `allowNetwork` is true. Each host is blackholed via
   * `--add-host <host>:0.0.0.0`, so DNS resolution inside the container returns an
   * unroutable address and the connection fails closed — host-granular egress
   * control at the `docker run` level (SSRF / data-exfil guard). Only applied when
   * network is on (with `--network none` everything is already blocked).
   */
  deniedHosts?: string[];
  /**
   * PARITY (per-domain network ALLOWLIST — the reference's
   * `SandboxNetworkConfig.allowedDomains` / `allowManagedDomainsOnly`): hostnames
   * the container may reach for outbound network, with EVERYTHING ELSE denied —
   * the "network on, but only to these domains" knob the binary `--network
   * bridge|none` switch cannot express. When set (and `allowNetwork` is true) the
   * container is given `--cap-add NET_ADMIN` and the command is prefixed with an
   * iptables setup that ACCEPTs loopback + DNS + each resolved allowed host and
   * DROPs all other outbound traffic — the SAME {@link buildNetworkAllowlist}
   * rules the host (unshare) backend uses inside its network namespace, so
   * container and host modes enforce an identical allowlist (no second impl).
   * `deniedHosts` rules are emitted FIRST so deny wins over allow, and any host
   * the leader granted this session ({@link getGrantedSandboxHosts}) is folded in
   * so the per-host escalation flow works under a strict allowlist too. Empty /
   * unset keeps the broad `bridge` allow (minus any `deniedHosts` blackholes).
   */
  allowedHosts?: string[];
  /**
   * PARITY (proxy-routed egress — the reference's `SandboxNetworkConfig.httpProxyPort`):
   * host TCP port of an HTTP(S) egress proxy. When set, the container exports
   * HTTP(S)_PROXY pointing at `host.docker.internal:<port>` (a host-gateway
   * `--add-host` is added so the container can reach it), routing outbound HTTP(S)
   * through the host proxy that enforces the real allow/deny policy. 1..65535;
   * invalid values are ignored.
   */
  httpProxyPort?: number;
  /**
   * PARITY (proxy-routed egress — the reference's `SandboxNetworkConfig.socksProxyPort`):
   * host TCP port of a SOCKS egress proxy. When set, the container exports
   * ALL_PROXY=`socks5://host.docker.internal:<port>`. 1..65535; invalid ignored.
   */
  socksProxyPort?: number;
  maxMemoryMB?: number;
  maxProcesses?: number;
  /**
   * PARITY (filesystem write-deny — the reference's `SandboxFilesystemConfig.denyWrite`
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
   * PARITY (filesystem read-deny — the reference's `SandboxFilesystemConfig.denyRead`):
   * absolute FILE paths hidden from reads inside the container by bind-mounting
   * `/dev/null` over each (a read returns empty). Only existing regular files are
   * applied; directories/missing paths are skipped.
   */
  denyReadPaths?: string[];
  /**
   * PARITY (filesystem write-allow — the reference's `SandboxFilesystemConfig.allowWrite`
   * + the `/add-dir` sandbox-mount sync): extra absolute paths bind-mounted
   * WRITABLE in addition to the project. Used to widen the container's writable set
   * to a directory granted outside the project (e.g. via `/add-dir`). Only existing
   * paths outside the project mount are added.
   */
  allowWritePaths?: string[];
  /**
   * PARITY (Unix-socket allowlist — the reference's
   * `SandboxNetworkConfig.allowUnixSockets` / `getAllowUnixSockets`): host
   * Unix-socket paths explicitly GRANTED into the container. The container mounts
   * NO host socket by default (deny-all — the Docker daemon, SSH/GPG-agent sockets
   * are unreachable, closing a classic sandbox-escape vector), so only the paths
   * listed here are bind-mounted in. Only existing sockets are applied.
   */
  allowUnixSockets?: string[];
  /**
   * PARITY (custom seccomp syscall filter — the reference's `sandbox.seccomp.bpfPath` /
   * `applyPath`): absolute path to a Docker seccomp JSON profile. When set it is
   * passed as `--security-opt seccomp=<path>`, REPLACING Docker's default profile
   * with the operator-supplied one (on top of the always-applied
   * {@link DOCKER_SECURITY_OPTS}). Unset keeps Docker's hardened default profile.
   */
  seccompProfilePath?: string;
}

/**
 * Why a Docker availability probe failed. Collapsing every failure into a single
 * boolean hides the actual thing the user must fix; this classification lets the
 * fail-closed message name the real cause (CLI missing vs. permission vs. a slow
 * or unreachable daemon).
 */
export const DOCKER_UNAVAILABLE_CAUSES = ['cli-missing', 'permission-denied', 'daemon-slow', 'daemon-unreachable', 'aborted'] as const;
export type DockerUnavailableCause = (typeof DOCKER_UNAVAILABLE_CAUSES)[number];

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

/** Docker's special hostname that resolves to the host from inside a container
 *  (paired with `--add-host <alias>:host-gateway`). Used to reach a host-side
 *  egress proxy configured via {@link DockerSandboxConfig.httpProxyPort} /
 *  {@link DockerSandboxConfig.socksProxyPort}. */
export const DOCKER_HOST_GATEWAY_ALIAS = 'host.docker.internal';

/**
 * PARITY (seccomp syscall filtering / privilege-escalation isolation layer —
 * the reference's bwrap seccomp profile): Docker `--security-opt` flags applied to EVERY
 * sandbox container, as a defense-in-depth isolation layer on top of the mount /
 * network jail.
 *
 * - `no-new-privileges=true` sets the kernel `PR_SET_NO_NEW_PRIVS` bit so a
 *   sandboxed process can never gain privileges via a setuid/setgid binary — the
 *   classic in-container escalation step.
 *
 * Crucially, these flags are ADDED, never a `seccomp=unconfined`: Docker's DEFAULT
 * seccomp profile (which blocks the dangerous syscall surface) stays applied, so
 * the container keeps syscall filtering. The Unix-domain-socket escape that
 * the reference's seccomp profile targets is additionally closed here by the deny-all
 * socket-mount design (no host socket is bind-mounted unless explicitly
 * allowlisted — see {@link DockerSandboxConfig.allowUnixSockets}). Exported for
 * the `/sandbox` introspection surface and tests.
 */
export const DOCKER_SECURITY_OPTS: readonly string[] = ['no-new-privileges=true'];

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
    // Record the POSITIVE detection in the shared global config (asymmetric cache —
    // never a negative) so a diagnostic / startup path can learn this machine has a
    // reachable daemon without re-probing. Runs at most once per process (the
    // `_dockerAvailable === true` fast-path above short-circuits every later call).
    persistPositiveCapability('dockerAvailable', true);
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

/**
 * PARITY (the reference's `getSandboxUnavailableReason()` — the #34044 security-footgun
 * fix): when the container sandbox is explicitly ENABLED (`mode: 'container'`) but
 * cannot actually run, return a precise, human-readable reason + remediation meant
 * to be surfaced ONCE (e.g. at startup or via `/sandbox`), so a user whose security
 * setting is silently being ignored is told. Returns `null` when no container
 * sandbox is requested or when it CAN run (no warning needed).
 *
 * Covered causes, in order:
 *  - the current platform is excluded by `enabledPlatforms`;
 *  - the Docker daemon/CLI is unavailable (reusing {@link getDockerAvailability} +
 *    {@link describeDockerUnavailable} for the cause-specific remediation hint).
 *
 * The trailing clause reflects `failIfUnavailable`: a managed deployment that set it
 * will REFUSE commands (fail-closed), otherwise bash silently runs UNsandboxed — the
 * exact footgun this warning exists to make visible.
 */
export function getSandboxUnavailableReason(policy?: SandboxPolicy): string | null {
  if (!policy || policy.mode !== 'container') return null;
  const platforms = policy.enabledPlatforms;
  if (platforms && platforms.length > 0 && !platforms.includes(process.platform)) {
    return (
      `sandbox.mode is 'container' but the current platform (${process.platform}) is excluded by `
      + `enabledPlatforms (${platforms.join(', ')}); bash will NOT be sandboxed on this platform.`
    );
  }
  const availability = getDockerAvailability();
  if (!availability.available) {
    const tail = policy.failIfUnavailable
      ? 'commands will be REFUSED (failIfUnavailable is set).'
      : 'bash will run UNsandboxed — your security setting is being ignored.';
    return `sandbox.mode is 'container' but ${describeDockerUnavailable(availability)}. ${tail}`;
  }
  return null;
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

/**
 * Patterns that name the specific HOST a blocked outbound call was trying to reach,
 * ordered most-specific first. Used by {@link extractBlockedHost} to turn a generic
 * network-failure stderr into the concrete host the worker must escalate for egress
 * approval. Each capture group 1 is the candidate host.
 */
const BLOCKED_HOST_SIGNATURES: RegExp[] = [
  /could ?n[o']?t resolve host:?\s*'?([a-z0-9.-]+)/i,
  /resolving timed out after.*?'?([a-z0-9.-]+)'?/i,
  /failed to connect to ([a-z0-9.-]+)\b/i,
  /connect to ([a-z0-9.-]+) port \d+/i,
  /name or service not known.*?'?([a-z0-9.-]+)'?/i,
];

/** Hosts that are NOT a real egress target (loopback / the host-gateway alias the
 *  proxy uses), so they are never surfaced as an escalation candidate. */
const NON_EGRESS_HOSTS = new Set(['localhost', '127.0.0.1', '::1', DOCKER_HOST_GATEWAY_ALIAS]);

/**
 * Extract the concrete network HOST a blocked outbound command was trying to reach,
 * from the command's stderr (preferred — it names the host the resolver/connection
 * failed on) and, failing that, from an `http(s)://host` URL in the command itself.
 * Returns the lowercased host, or `null` when no real egress host can be identified
 * (so a generic "network is unreachable" with no host does not invent one). Loopback
 * and the host-gateway proxy alias are ignored. This is the detection half of the
 * per-host sandbox-egress delegation: the host it returns is what a worker escalates
 * to the leader for an allow/deny decision.
 */
export function extractBlockedHost(command: string, stderr: string): string | null {
  for (const re of BLOCKED_HOST_SIGNATURES) {
    const m = stderr.match(re);
    if (m && m[1]) {
      const host = m[1].toLowerCase().replace(/[.:]+$/, '');
      if (host && !NON_EGRESS_HOSTS.has(host)) return host;
    }
  }
  // Fall back to a URL host in the command (e.g. `curl https://api.example.com/x`).
  const url = command.match(/https?:\/\/([a-z0-9.-]+)/i);
  if (url && url[1]) {
    const host = url[1].toLowerCase().replace(/[.:]+$/, '');
    if (host && !NON_EGRESS_HOSTS.has(host)) return host;
  }
  return null;
}

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
 *
 * SIDE EFFECT (parity — the reference's violation store): each detected sandbox-caused
 * failure is also recorded into the {@link sandboxViolationStore} (unless the
 * operator allowlisted it via `cfg.ignoreViolations`), so the `/sandbox` surface
 * can show the user the running total + last-N of what the boundary blocked — AMI
 * otherwise blocks silently with no aggregated signal.
 */
export function annotateStderrWithSandboxFailures(
  command: string,
  stderr: string,
  cfg: { allowNetwork: boolean; projectDir: string; ignoreViolations?: Record<string, string[]> },
): string {
  if (!stderr) return stderr;
  // Honor the per-command ignore map (the reference's `sandbox.ignoreViolations`) for the
  // recorded-violation side effect only; the model-facing annotation is unaffected.
  sandboxViolationStore.setIgnoreViolations(cfg.ignoreViolations);
  const notes: string[] = [];
  if (!cfg.allowNetwork && NETWORK_OFF_SIGNATURES.some((re) => re.test(stderr))) {
    // Per-host egress delegation (worker→leader): name the concrete host the call
    // was trying to reach (when identifiable) and point the worker at the escalation
    // path — ask the lead to approve this host via a `sandbox_permission_request`
    // send_message and retry; an approved host is reachable on the next command
    // (processSandboxPermissionResponse records the grant, which the container
    // backend then stops blocking).
    const blockedHost = extractBlockedHost(command, stderr);
    const hostClause = blockedHost ? ` (host "${blockedHost}")` : '';
    const escalateClause = blockedHost
      ? ` If host "${blockedHost}" is legitimately needed, ask the lead to approve egress to it `
        + '(send a sandbox_permission_request via send_message) and retry after it is granted.'
      : '';
    notes.push(
      `[sandbox: network is disabled for this container (--network none)${hostClause}; the error above may be `
        + 'caused by blocked network access, not a real failure. Retry with '
        + `dangerouslyDisableSandbox: true if this command legitimately needs the network.${escalateClause}]`,
    );
    sandboxViolationStore.record({
      kind: 'network',
      command,
      detail: blockedHost
        ? `outbound network blocked (--network none) — host "${blockedHost}"`
        : 'outbound network blocked (--network none)',
    });
  }
  if (/no such file or directory/i.test(stderr) && mentionsPathOutsideProject(stderr, cfg.projectDir)) {
    notes.push(
      `[sandbox: only the project directory (${path.resolve(cfg.projectDir)}) is mounted inside the `
        + 'container; files outside it do not exist there, so a "No such file or directory" above may '
        + 'be caused by sandbox isolation rather than a genuinely missing file.]',
    );
    sandboxViolationStore.record({
      kind: 'filesystem',
      command,
      detail: `path outside the project mount (${path.resolve(cfg.projectDir)}) is not present in the container`,
    });
  }
  if (notes.length === 0) return stderr;
  // PARITY (the reference's `<sandbox_violations>` envelope): wrap the sandbox notes in a
  // `<sandbox_violations>` block so the MODEL keeps them (to explain that a failure
  // was the sandbox boundary, not a real error) while the CLI tool-error renderer
  // strips the block from the USER-facing display (removeSandboxViolationTags).
  return `${stderr}\n${wrapSandboxViolations(notes)}`;
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
 * read-only. Mirrors the reference's unconditional `denyWrite` of settings.json +
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
 * PARITY (bare-repo .git-file poisoning defense — the reference's CVE-#29316 guard):
 * absolute paths of the files git's `is_git_directory()` keys on — HEAD, objects,
 * refs, hooks, config — that ALREADY EXIST directly at `cwd` (the project root,
 * NOT under `.git/`). A directory containing these is treated by git as a (bare)
 * repo, and a planted/pre-existing `config` with `core.fsmonitor` (or a `hooks`
 * entry) runs an arbitrary command the next time an UNsandboxed host `git` runs
 * there — a sandbox escape. Returning the existing markers lets
 * {@link wrapWithDocker} bind each read-only so a sandboxed command cannot modify
 * a planted marker. Non-existing markers are omitted (Docker would otherwise
 * auto-create a 0-byte host stub that itself breaks git). The marker list is
 * shared with the unshare backend ({@link BARE_GIT_REPO_FILES}) — no duplicate.
 */
export function bareGitRepoDenyPaths(cwd: string): string[] {
  const base = path.resolve(cwd || '.');
  return BARE_GIT_REPO_FILES
    .map((f) => path.join(base, f))
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
 * `cwd` is not a linked worktree. Mirrors the reference's `detectWorktreeMainRepoPath`.
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

  // PARITY (seccomp / privilege-escalation isolation layer): apply the hardened
  // --security-opt flags to EVERY container (no-new-privileges), on top of the
  // kept-default seccomp syscall filter. See {@link DOCKER_SECURITY_OPTS}.
  for (const opt of DOCKER_SECURITY_OPTS) parts.push('--security-opt', opt);

  // PARITY (custom seccomp profile): when the operator points the policy at a
  // Docker seccomp JSON profile, apply it via `--security-opt seccomp=<path>`,
  // replacing Docker's default syscall filter with the supplied one. Added AFTER
  // the hardened defaults so it narrows (or customizes) the syscall surface; the
  // no-new-privileges flag above still stands.
  const seccompProfile = cfg.seccompProfilePath?.trim();
  if (seccompProfile) {
    parts.push('--security-opt', shSingleQuote(`seccomp=${path.resolve(seccompProfile)}`));
  }

  // PARITY (write-deny carve-outs): bind-remount each privileged-autoload default
  // (settings/skills/commands/agents) PLUS any operator-configured denyWrite path
  // read-only ON TOP of the project RW mount. A nested, more-specific bind shadows
  // the broader project mount, so these stay unwritable inside an otherwise-writable
  // project — closing the config-rewrite / SKILL-plant sandbox-escape vectors.
  // bareGitRepoDenyPaths(workdir) folds in any planted/pre-existing bare-repo
  // marker (HEAD/objects/refs/hooks/config) at the container cwd so it is bound
  // read-only too — closing the bare-repo `core.fsmonitor` poisoning escape.
  const denyWriteSeen = new Set<string>();
  for (const p of [
    ...defaultDenyWritePaths(project),
    ...bareGitRepoDenyPaths(workdir),
    ...(cfg.denyWritePaths ?? []),
  ]) {
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

  // PARITY (per-host network egress control): when network is ON, blackhole each
  // denied host by injecting `--add-host <host>:0.0.0.0` so its in-container DNS
  // resolves to an unroutable address and the connection fails closed (deny wins
  // over the broad `bridge` allow — host-granular egress / SSRF guard). Skipped
  // under `--network none` (everything is already blocked there). Route outbound
  // through a host egress proxy when a proxy port is configured: the proxy enforces
  // the real allowlist that `docker run` alone cannot express.
  const proxyEnv: string[] = [];
  if (isValidProxyPort(cfg.httpProxyPort)) {
    const url = `http://${DOCKER_HOST_GATEWAY_ALIAS}:${cfg.httpProxyPort}`;
    proxyEnv.push(`HTTP_PROXY=${url}`, `HTTPS_PROXY=${url}`, `http_proxy=${url}`, `https_proxy=${url}`);
  }
  if (isValidProxyPort(cfg.socksProxyPort)) {
    const url = `socks5://${DOCKER_HOST_GATEWAY_ALIAS}:${cfg.socksProxyPort}`;
    proxyEnv.push(`ALL_PROXY=${url}`, `all_proxy=${url}`);
  }
  // The command that will actually be handed to `bash -c` inside the container.
  // A per-domain allowlist prepends an iptables setup script to it (below).
  let innerCommand = command;
  if (cfg.allowNetwork) {
    const deniedSeen = new Set<string>();
    for (const h of cfg.deniedHosts ?? []) {
      const host = h.trim();
      if (host.length === 0 || deniedSeen.has(host)) continue;
      deniedSeen.add(host);
      // Per-host egress delegation: a host the leader has GRANTED for this session
      // is no longer blackholed, so the worker's subsequent command reaches it
      // (the grant was recorded by processSandboxPermissionResponse on the worker's
      // mailbox poll). Deny still wins for every host that was not explicitly granted.
      if (isSandboxHostGranted(host)) continue;
      parts.push('--add-host', shSingleQuote(`${host}:0.0.0.0`));
    }
    // PARITY (per-domain network ALLOWLIST): "network on, but ONLY these domains".
    // Realized the SAME way as the host (unshare) backend — an in-container iptables
    // allowlist (reusing buildNetworkAllowlist/buildDeniedDomainRules, not a second
    // impl) that ACCEPTs loopback + DNS + each resolved allowed host and DROPs the
    // rest. It needs CAP_NET_ADMIN to append rules, so --cap-add NET_ADMIN is added
    // only when an allowlist is actually requested. Deny rules are emitted FIRST
    // (iptables first-match wins ⇒ deny beats allow), and any leader-granted host is
    // folded in so the escalation flow also works under a strict allowlist.
    const allowHosts = Array.from(
      new Set(
        [...(cfg.allowedHosts ?? []), ...getGrantedSandboxHosts()]
          .map((h) => h.trim())
          .filter((h) => h.length > 0),
      ),
    );
    if (allowHosts.length > 0) {
      parts.push('--cap-add', 'NET_ADMIN');
      const setup = buildDeniedDomainRules(cfg.deniedHosts) + buildNetworkAllowlist(allowHosts);
      innerCommand = `${setup}${command}`;
    }
    // A host-gateway alias lets the container reach a proxy listening on the host.
    if (proxyEnv.length > 0) {
      parts.push('--add-host', shSingleQuote(`${DOCKER_HOST_GATEWAY_ALIAS}:host-gateway`));
      for (const e of proxyEnv) parts.push('-e', shSingleQuote(e));
    }
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
  // Command runs as the entrypoint (bash) args: `… IMAGE -c '<command>'`. When a
  // per-domain allowlist is active, `innerCommand` is the iptables setup + command.
  parts.push(shSingleQuote(cfg.image), '-c', shSingleQuote(innerCommand));
  return parts.join(' ');
}
