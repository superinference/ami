/**
 * oauth-port.ts — dynamic available-port discovery for the OAuth loopback
 * redirect listener.
 *
 * A hardcoded callback port (e.g. 33418) breaks whenever something else already
 * holds it — a second concurrent auth flow, a leftover listener, or an unrelated
 * local service. Mirroring the reference's `oauthPort` helper, this:
 *   1. honors an operator-pinned port via `MCP_OAUTH_CALLBACK_PORT` (so a user
 *      behind a firewall can allow-list a stable callback port);
 *   2. probes a preferred port, then a bounded run of sequential neighbours
 *      (keeps AMI's stable, allow-listable default when it is free);
 *   3. random-probes within the RFC 8252 loopback dynamic-port range — random
 *      selection gives an unpredictable listener port (defence in depth) and
 *      Windows is special-cased because it reserves 49152-65535 for its own
 *      dynamic allocation; and
 *   4. finally falls back to an OS-assigned ephemeral port (bind to :0 and read
 *      back the chosen port) so a redirect listener can always be stood up.
 *
 * Every probe binds on 127.0.0.1 only — the loopback host the redirect targets —
 * so a port free on another interface is not mistaken for usable here.
 */
import * as net from 'net';

/** The default preferred OAuth callback port (kept stable for users who allow-list it). */
export const DEFAULT_OAUTH_CALLBACK_PORT = 33418;

/** Loopback host the OAuth redirect listener binds to. */
export const OAUTH_CALLBACK_HOST = '127.0.0.1';

/** Env var (the reference-parity) pinning a fixed OAuth callback port. */
export const OAUTH_CALLBACK_PORT_ENV = 'MCP_OAUTH_CALLBACK_PORT';

/**
 * RFC 8252 loopback dynamic-port range for random listener selection. Windows
 * reserves 49152-65535 for its own dynamic allocation, so a lower band is probed
 * there to avoid clashing with OS-assigned ephemeral ports.
 */
export const LOOPBACK_PORT_RANGE = { min: 49152, max: 65535 } as const;
export const WINDOWS_LOOPBACK_PORT_RANGE = { min: 39152, max: 49151 } as const;

/** Select the platform-appropriate loopback dynamic-port range. */
export function loopbackPortRange(platform: string = process.platform): { min: number; max: number } {
  return platform === 'win32' ? WINDOWS_LOOPBACK_PORT_RANGE : LOOPBACK_PORT_RANGE;
}

/**
 * Read an operator-pinned callback port from the environment, or undefined when
 * unset/invalid. A valid value is a positive integer; anything else is ignored
 * so a typo never wedges the auth flow on a nonsense port.
 */
export function readConfiguredPort(env: NodeJS.ProcessEnv = process.env): number | undefined {
  const raw = env[OAUTH_CALLBACK_PORT_ENV];
  if (!raw) return undefined;
  const n = parseInt(raw, 10);
  return Number.isInteger(n) && n > 0 ? n : undefined;
}

export interface FindPortOptions {
  /** Port to try first (default DEFAULT_OAUTH_CALLBACK_PORT). */
  preferred?: number;
  /** How many sequential candidates to try after the preferred one (default 10). */
  attempts?: number;
  /** Host to probe (default 127.0.0.1). */
  host?: string;
  /** Injectable listen primitive for tests; defaults to a real net.Server bind. */
  probe?: (port: number, host: string) => Promise<number>;
  /** Env to read MCP_OAUTH_CALLBACK_PORT from (default process.env). */
  env?: NodeJS.ProcessEnv;
  /** Platform used to pick the loopback range (default process.platform). */
  platform?: string;
  /** How many random in-range candidates to try after the sequential run (default 100). */
  randomAttempts?: number;
  /** Injectable RNG returning [0,1) for deterministic tests (default Math.random). */
  random?: () => number;
}

/** Resolve with `port` if a server can bind it on `host`, else reject. */
export function probePort(port: number, host: string): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const srv = net.createServer();
    srv.once('error', (err) => { srv.close(); reject(err); });
    srv.listen(port, host, () => {
      const addr = srv.address();
      const bound = typeof addr === 'object' && addr ? addr.port : port;
      srv.close(() => resolve(bound));
    });
  });
}

/**
 * Find an available loopback port for the OAuth callback listener:
 *   1. an operator-pinned `MCP_OAUTH_CALLBACK_PORT` (returned as-is);
 *   2. the preferred port, then `attempts` sequential candidates;
 *   3. up to `randomAttempts` random candidates within the platform loopback
 *      range (Windows special-cased); then
 *   4. an OS-assigned ephemeral port (bind :0).
 * Throws only if even the ephemeral bind fails.
 */
export async function findAvailablePort(options: FindPortOptions = {}): Promise<number> {
  const preferred = options.preferred ?? DEFAULT_OAUTH_CALLBACK_PORT;
  const attempts = options.attempts ?? 10;
  const host = options.host ?? OAUTH_CALLBACK_HOST;
  const probe = options.probe ?? probePort;
  const random = options.random ?? Math.random;
  const randomAttempts = options.randomAttempts ?? 100;

  // 1. An operator-pinned port wins outright (firewall allow-listing) — returned
  //    without probing, matching the redirect URI the manager advertises for it.
  const configured = readConfiguredPort(options.env);
  if (configured !== undefined) {
    return configured;
  }

  // 2. Preferred port, then a short run of sequential neighbours (stable, allow-listable).
  for (let i = 0; i < attempts; i++) {
    const candidate = preferred + i;
    if (candidate > 65535) break;
    try {
      return await probe(candidate, host);
    } catch {
      /* in use — try the next candidate */
    }
  }

  // 3. Random probe within the platform loopback range — unpredictable listener
  //    port (RFC 8252); Windows reserves the upper dynamic band.
  const { min, max } = loopbackPortRange(options.platform);
  const range = max - min + 1;
  const tries = Math.max(0, Math.min(range, randomAttempts));
  for (let i = 0; i < tries; i++) {
    const candidate = min + Math.floor(random() * range);
    try {
      return await probe(candidate, host);
    } catch {
      /* in use — try another random port */
    }
  }

  // 4. Last resort: let the OS pick any free ephemeral port.
  return probe(0, host);
}

/** Build the loopback redirect URI for a discovered port. */
export function buildRedirectUri(port: number, host = OAUTH_CALLBACK_HOST, path = '/oauth/callback'): string {
  return `http://${host}:${port}${path}`;
}
