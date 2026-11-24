// Split from docker-sandbox.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { BARE_GIT_REPO_FILES } from './bash-sandbox';

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
export function classifyDockerError(e: unknown): DockerUnavailableCause {
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
export const NETWORK_OFF_SIGNATURES: RegExp[] = [
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
export const CONTAINER_TMPFS_PREFIXES = ['/tmp', '/root', '/home'];

/**
 * Patterns that name the specific HOST a blocked outbound call was trying to reach,
 * ordered most-specific first. Used by {@link extractBlockedHost} to turn a generic
 * network-failure stderr into the concrete host the worker must escalate for egress
 * approval. Each capture group 1 is the candidate host.
 */
export const BLOCKED_HOST_SIGNATURES: RegExp[] = [
  /could ?n[o']?t resolve host:?\s*'?([a-z0-9.-]+)/i,
  /resolving timed out after.*?'?([a-z0-9.-]+)'?/i,
  /failed to connect to ([a-z0-9.-]+)\b/i,
  /connect to ([a-z0-9.-]+) port \d+/i,
  /name or service not known.*?'?([a-z0-9.-]+)'?/i,
];

/** True if `stderr` references an absolute path that is NOT inside the project
 *  mount nor one of the container tmpfs dirs — i.e. a host path that genuinely
 *  does not exist inside the container. */
export function mentionsPathOutsideProject(stderr: string, projectDir: string): boolean {
  const project = path.resolve(projectDir);
  const paths = stderr.match(/\/[^\s'":]+/g) || [];
  return paths.some((p) => {
    if (p === project || p.startsWith(project + path.sep)) return false;
    if (CONTAINER_TMPFS_PREFIXES.some((t) => p === t || p.startsWith(t + '/'))) return false;
    return true;
  });
}

/** POSIX single-quote a string for safe inclusion in a `bash -c` host command. */
export function shSingleQuote(s: string): string {
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
