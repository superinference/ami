/**
 * oauth-port.ts — dynamic available-port discovery for the OAuth loopback
 * redirect listener.
 *
 * A hardcoded callback port (e.g. 33418) breaks whenever something else already
 * holds it — a second concurrent auth flow, a leftover listener, or an unrelated
 * local service. Mirroring Claude Code's `oauthPort` helper, this probes a
 * preferred port, then a bounded range of candidates, and finally falls back to
 * an OS-assigned ephemeral port (bind to :0 and read back the chosen port) so a
 * redirect listener can always be stood up. The probe binds on 127.0.0.1 only —
 * the loopback host the redirect targets — so a port free on another interface
 * is not mistaken for usable here.
 */
import * as net from 'net';

/** The default preferred OAuth callback port (kept stable for users who allow-list it). */
export const DEFAULT_OAUTH_CALLBACK_PORT = 33418;

/** Loopback host the OAuth redirect listener binds to. */
export const OAUTH_CALLBACK_HOST = '127.0.0.1';

export interface FindPortOptions {
  /** Port to try first (default DEFAULT_OAUTH_CALLBACK_PORT). */
  preferred?: number;
  /** How many sequential candidates to try after the preferred one (default 10). */
  attempts?: number;
  /** Host to probe (default 127.0.0.1). */
  host?: string;
  /** Injectable listen primitive for tests; defaults to a real net.Server bind. */
  probe?: (port: number, host: string) => Promise<number>;
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
 * Find an available loopback port for the OAuth callback listener: try the
 * preferred port, then `attempts` sequential candidates, then an OS-assigned
 * ephemeral port (bind :0). Throws only if even the ephemeral bind fails.
 */
export async function findAvailablePort(options: FindPortOptions = {}): Promise<number> {
  const preferred = options.preferred ?? DEFAULT_OAUTH_CALLBACK_PORT;
  const attempts = options.attempts ?? 10;
  const host = options.host ?? OAUTH_CALLBACK_HOST;
  const probe = options.probe ?? probePort;

  for (let i = 0; i < attempts; i++) {
    const candidate = preferred + i;
    if (candidate > 65535) break;
    try {
      return await probe(candidate, host);
    } catch {
      /* in use — try the next candidate */
    }
  }
  // Last resort: let the OS pick any free ephemeral port.
  return probe(0, host);
}

/** Build the loopback redirect URI for a discovered port. */
export function buildRedirectUri(port: number, host = OAUTH_CALLBACK_HOST, path = '/oauth/callback'): string {
  return `http://${host}:${port}${path}`;
}
