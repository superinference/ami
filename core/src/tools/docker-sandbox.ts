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

let _dockerAvailable: boolean | null = null;

/** True if a Docker daemon is reachable. Cached after the first probe. */
export function isDockerAvailable(): boolean {
  if (_dockerAvailable !== null) return _dockerAvailable;
  try {
    shellHooks.execSync('docker version --format "{{.Server.Version}}"', {
      stdio: 'pipe',
      timeout: 5000,
    });
    _dockerAvailable = true;
  } catch {
    _dockerAvailable = false;
  }
  return _dockerAvailable;
}

/** Test seam: reset the cached availability probe. */
export function _resetDockerAvailableCache(): void {
  _dockerAvailable = null;
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
  // Command runs as the entrypoint (bash) args: `… IMAGE -c '<command>'`.
  parts.push(shSingleQuote(cfg.image), '-c', shSingleQuote(command));
  return parts.join(' ');
}
