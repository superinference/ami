/**
 * gcp-credentials.ts — a timeout-guarded probe for GCP Application-Default
 * Credentials (ADC), the chain the Vertex SDK (`@ai-sdk/google-vertex`) uses.
 *
 * Parity: the reference's `checkGcpCredentialsValid` (utils/auth.ts). The subtle
 * special-case this exists for: when NO local credential source is configured (no
 * ADC file, no `CLOUDSDK_AUTH_ACCESS_TOKEN`), `google-auth-library` falls through
 * to the GCE metadata server, which HANGS for ~12s when AMI runs OUTSIDE GCP. A
 * naive "is Vertex authenticated?" probe would inherit that stall. So the token
 * fetch is raced against a short timeout (via the canonical {@link withTimeout}
 * boundary, no re-implemented Promise.race) and any timeout / auth error / missing
 * library resolves to `false` — the probe is responsive and never throws or hangs.
 *
 * This is a self-contained core building block (no engine/UI/orchestration
 * dependency), so it lives at the lowest layer and is imported upward by the
 * provider credential-refresh path (`common/src/provider-config.ts`) exactly the
 * way the Bedrock STS caller-identity preflight is.
 */
import { withTimeout } from './utils/sleep';

/**
 * Short timeout for the GCP ADC credentials probe. Without it, with no local
 * credential source, `google-auth-library` falls through to the GCE metadata
 * server which hangs ~12s outside GCP. Parity: the reference's
 * `GCP_CREDENTIALS_CHECK_TIMEOUT_MS`.
 */
export const GCP_CREDENTIALS_CHECK_TIMEOUT_MS = 5_000;

/**
 * Fetch (and discard) a GCP access token through the ADC chain. Resolves when the
 * ambient credentials authenticate, rejects otherwise. Injectable so tests drive
 * the fast / slow / throwing paths without a real GCP environment.
 */
export type GcpAccessTokenFetcher = () => Promise<void>;

/** Minimal surface of `google-auth-library`'s `GoogleAuth` used by the default fetcher. */
export type GoogleAuthLike = new (opts: { scopes: string[] }) => {
  getClient(): Promise<{ getAccessToken(): Promise<unknown> }>;
};

/** Options for {@link checkGcpCredentialsValid} (all injectable for tests). */
export interface CheckGcpCredentialsOptions {
  /** Override the ADC token fetch (default: dynamic import of google-auth-library). */
  fetchAccessToken?: GcpAccessTokenFetcher;
  /** Probe timeout in ms (default {@link GCP_CREDENTIALS_CHECK_TIMEOUT_MS}). */
  timeoutMs?: number;
  /**
   * Test seam for the default ADC fetcher's dynamic import. Production leaves this
   * unset so `google-auth-library` is loaded only when a real probe runs.
   */
  loadGoogleAuth?: () => Promise<{ GoogleAuth: GoogleAuthLike }>;
}

/**
 * Default ADC token fetch: dynamically import `google-auth-library` (so the module
 * is never loaded unless a probe actually runs) and fetch an access token with the
 * cloud-platform scope — the SAME authentication chain the Vertex SDK uses. A
 * missing library (never installed) or any auth error propagates as a rejection,
 * which {@link checkGcpCredentialsValid} turns into `false`.
 */
async function defaultFetchAccessToken(
  loadGoogleAuth?: () => Promise<{ GoogleAuth: GoogleAuthLike }>,
): Promise<void> {
  const { GoogleAuth } = await (loadGoogleAuth ?? (() => import('google-auth-library') as Promise<{ GoogleAuth: GoogleAuthLike }>))();
  const auth = new GoogleAuth({
    scopes: ['https://www.googleapis.com/auth/cloud-platform'],
  });
  const client = await auth.getClient();
  await client.getAccessToken();
}

/**
 * Probe whether GCP ADC currently authenticates by fetching an access token
 * through the Vertex auth chain, guarded by a short timeout. Returns `true` only
 * when a token is obtained within the bound; `false` on timeout, a missing
 * google-auth-library, or any auth error. Never throws and never hangs — the whole
 * point of the timeout is that an outside-GCP run with no local credential fails
 * fast instead of stalling ~12s on the GCE metadata server.
 */
export async function checkGcpCredentialsValid(
  options: CheckGcpCredentialsOptions = {},
): Promise<boolean> {
  const fetchAccessToken =
    options.fetchAccessToken ??
    (() => defaultFetchAccessToken(options.loadGoogleAuth));
  const timeoutMs = options.timeoutMs ?? GCP_CREDENTIALS_CHECK_TIMEOUT_MS;
  try {
    await withTimeout(
      fetchAccessToken(),
      timeoutMs,
      'GCP credentials check timed out',
    );
    return true;
  } catch {
    // Timeout, missing library, or an auth failure — none of which should bubble
    // up or block provider setup. The ambient chain simply isn't usable.
    return false;
  }
}
