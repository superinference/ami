/**
 * Shared SSRF protection and HTTP utilities for web-fetch and web-search tools.
 */

import * as http from 'http';
import * as https from 'https';
import * as dns from 'dns';
import { URL } from 'url';
import { AMI_VERSION } from '../version';
import { DEFAULT_HTTP_RETRY_DELAYS_MS, EGRESS_PROXY_BLOCKED_VALUE, EGRESS_PROXY_ERROR_HEADER, EgressBlockedError, MAX_HTTP_BODY_BYTES, SSRF_LOOPBACK_AFFORDANCE, decodeBody, delay, isLoopbackHost, isPrivateIP, isTransientHttpError, isValidIP, pinnedDnsLookup, urlHasUserinfo } from './web-utils-strip-html';
import type { HostLookupAll } from './web-utils-strip-html';
export { DEFAULT_HTTP_RETRY_DELAYS_MS, EGRESS_PROXY_BLOCKED_VALUE, EGRESS_PROXY_ERROR_HEADER, EgressBlockedError, MAX_HTTP_BODY_BYTES, SSRF_LOOPBACK_AFFORDANCE, decodeBody, isLoopbackHost, isPrivateIP, isTransientHttpError, isValidIP, parseCharset, pinnedDnsLookup, stripHtml, urlHasUserinfo } from './web-utils-strip-html';
export type { HostLookupAll } from './web-utils-strip-html';

export const BLOCKED_HOSTNAMES = new Set([
  'localhost', '127.0.0.1', '::1', '0.0.0.0',
  'metadata.google.internal', 'metadata.google.com',
  'instance-data', 'metadata',
]);

/**
 * URL schemes the web tools are permitted to fetch. Everything else —
 * `file:`, `ftp:`, `gopher:`, `data:`, `blob:`, `javascript:`, `ws:`/`wss:` —
 * is refused before any network or filesystem access. A crafted initial URL
 * (or a redirect to one) that reached a non-HTTP handler would be an SSRF/LFI
 * surface (`file:///etc/passwd`, `gopher://` request smuggling); the allowlist
 * closes it. ADDITIVE: pre-existing http/https callers are unaffected.
 */
export const ALLOWED_URL_SCHEMES = new Set(['http:', 'https:']);

/** True iff `url` parses and uses an allowed (http/https) scheme. */
export function isAllowedScheme(url: string): boolean {
  try {
    return ALLOWED_URL_SCHEMES.has(new URL(url).protocol.toLowerCase());
  } catch {
    return false;
  }
}

export function isBlockedRedirectTarget(url: string): boolean {
  try {
    const parsed = new URL(url);
    // A redirect that switches to a non-HTTP scheme (file:, gopher:, data:, …)
    // must never be followed — that is the classic "redirect to file://" SSRF/LFI
    // escape. Refuse any scheme outside the allowlist. ADDITIVE.
    if (!ALLOWED_URL_SCHEMES.has(parsed.protocol.toLowerCase())) return true;
    // Never follow a redirect to a credential-bearing target: the userinfo
    // would be sent as a Basic auth header to the (attacker-chosen) redirect
    // host, and `http://trusted@evil/` can disguise the real destination.
    if (urlHasUserinfo(parsed)) return true;
    const hostname = parsed.hostname.toLowerCase();
    if (BLOCKED_HOSTNAMES.has(hostname)) return true;
    // Block obvious private IP patterns in the hostname itself
    if (/^(10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.|127\.|0\.|169\.254\.)/.test(hostname)) return true;
    if (hostname === '::1' || hostname.startsWith('[::')) return true;
    // Bracketed IPv6 literal ([fe80::1], [fc00::1], [fd12::1], …): strip the
    // brackets and run the FULL private-range check. The "[::"-prefix test above
    // only catches loopback/unspecified; link-local and ULA redirect targets
    // slipped through. Public IPv6 (e.g. [2606:2800:...]) stays allowed. ADDITIVE.
    if (hostname.startsWith('[') && hostname.endsWith(']') && isPrivateIP(hostname.slice(1, -1))) {
      return true;
    }
    return false;
  } catch { return true; }
}

/**
 * True iff following `redirectUrl` from `originalUrl` would cross a security
 * boundary that must never be auto-followed, independent of host privateness:
 *
 *  - a **protocol change** — in particular the `https`→`http` TLS *downgrade* a
 *    server-controlled `Location` header can force, stripping transport
 *    encryption and exposing the follow-up request to MITM/interception (and
 *    any other scheme switch);
 *  - a **same-host port change** — a redirect that keeps the host but moves to a
 *    different port can pivot an approved fetch onto a different (e.g. internal,
 *    admin, debug) service listening on the SAME host.
 *
 * Mirrors the protocol/port arms of the reference's `isPermittedRedirect`
 * (`WebFetchTool/utils.ts`), which refuses any redirect whose protocol or port
 * differs from the original (within its same-origin follow policy). Ports are
 * compared on their raw `URL.port` (so an explicit non-default port differs from
 * an implied default, exactly as `new URL` yields in the reference).
 * (The embedded-credentials arm of the reference is already enforced upstream by
 * `isBlockedRedirectTarget` / `validateUrlSafety` via `urlHasUserinfo`, so it is
 * NOT re-checked here — one canonical credential guard, no duplication.)
 *
 * INTENTIONAL AMI SUPERSET DIVERGENCE: unlike the reference, AMI does NOT require
 * same-origin — AMI deliberately *follows* a cross-host redirect after
 * re-validating the target through `validateUrlSafety` (SSRF + DNS-pin), a kept
 * AMI differentiator. Because of that, the port check applies only when the host
 * is UNCHANGED (a cross-host port difference is just part of a cross-host redirect
 * that AMI re-validates). The protocol check applies to every host — the TLS
 * downgrade must be blocked regardless of destination. The private-host /
 * scheme-allowlist / credential checks live in `isBlockedRedirectTarget` and the
 * SSRF re-validation in the follow path. ADDITIVE.
 */
export function redirectCrossesSecurityBoundary(originalUrl: string, redirectUrl: string): boolean {
  let original: URL;
  let redirect: URL;
  try {
    original = new URL(originalUrl);
    redirect = new URL(redirectUrl);
  } catch {
    // An unparseable original or target is never safe to follow.
    return true;
  }
  // Protocol change — blocks the https→http TLS downgrade and any scheme switch,
  // on ANY host.
  if (redirect.protocol.toLowerCase() !== original.protocol.toLowerCase()) return true;
  // Same-host port pivot — raw-port comparison (reference parity). A cross-host
  // port difference is left to the SSRF re-validation of the cross-host follow.
  if (
    original.hostname.toLowerCase() === redirect.hostname.toLowerCase() &&
    redirect.port !== original.port
  ) {
    return true;
  }
  return false;
}

export async function validateUrlSafety(
  url: string,
  lookup: HostLookupAll = dns.lookup as HostLookupAll,
  opts?: { allowLoopback?: boolean },
): Promise<{ error: string } | { resolvedIP: string }> {
  const parsed = new URL(url);
  const hostname = parsed.hostname.toLowerCase();
  const allowLoopback = opts?.allowLoopback === true;

  // Scheme allowlist BEFORE any DNS/network work: refuse file:, ftp:, gopher:,
  // data:, etc. so a non-HTTP URL can never reach a fetch path. ADDITIVE —
  // http/https (the only schemes the web tools ever pass) are unaffected.
  if (!ALLOWED_URL_SCHEMES.has(parsed.protocol.toLowerCase())) {
    return { error: `Blocked: scheme "${parsed.protocol}" is not allowed (only http/https)` };
  }

  // Refuse credential-bearing URLs before any DNS/network work. Node would turn
  // the userinfo into an `Authorization: Basic …` header (secret on the wire),
  // and `user@host` can mask the true host. The redirect path reaches here via
  // validateRedirectSafety, so this closes credential-bearing redirect targets
  // too. ADDITIVE — web_fetch already strips userinfo from the initial URL.
  if (urlHasUserinfo(parsed)) {
    return { error: `Blocked: URL must not contain embedded credentials (userinfo)` };
  }

  if (BLOCKED_HOSTNAMES.has(hostname) && !(allowLoopback && isLoopbackHost(hostname))) {
    return { error: `Blocked: "${hostname}" is a private/metadata hostname. ${SSRF_LOOPBACK_AFFORDANCE}` };
  }

  // Reject single-label (dot-less) hostnames before any DNS/network work. A bare
  // "intranet" / "wiki" style name is not publicly resolvable and, via a
  // search-domain suffix, typically expands to a privileged internal host — so it
  // is an SSRF surface that must be refused pre-DNS. IP literals (v4 dotted or v6
  // colon-form) and, under allowLoopback, loopback names are exempt. Mirrors
  // the reference's validateURL publicly-resolvable filter (WebFetchTool/utils.ts
  // parts.length < 2). ADDITIVE — normal multi-label public hosts are unaffected.
  if (
    !isValidIP(hostname) &&
    !(allowLoopback && isLoopbackHost(hostname)) &&
    hostname.split('.').length < 2
  ) {
    return { error: `Blocked: "${hostname}" is a single-label (non-publicly-resolvable) hostname` };
  }

  // Resolve hostname and check for private IPs — return the resolved IP
  // so callers can pin it to prevent DNS rebinding (TOCTOU)
  try {
    const addresses = await new Promise<dns.LookupAddress[]>((resolve, reject) => {
      lookup(hostname, { all: true }, (err, addrs) => {
        if (err) reject(err);
        else resolve(addrs);
      });
    });
    const usable = addresses.filter(a => a.address && isValidIP(a.address));
    if (usable.length === 0) {
      return { error: `Blocked: DNS resolution failed for "${hostname}" — cannot verify safety` };
    }
    for (const addr of usable) {
      if (isPrivateIP(addr.address) && !(allowLoopback && isLoopbackHost(addr.address))) {
        return { error: `Blocked: "${hostname}" resolves to private IP ${addr.address}. ${SSRF_LOOPBACK_AFFORDANCE}` };
      }
    }
    return { resolvedIP: usable[0].address };
  } catch {
    return { error: `Blocked: DNS resolution failed for "${hostname}" — cannot verify safety` };
  }
}

// Identify the agent so site operators can recognize/allowlist the traffic
// (the reference's identify-yourself policy). Override with SUPERINFERENCE_WEB_USER_AGENT
// (e.g. a browser UA) for sites that require it. Read at call time so the env var
// takes effect regardless of import order and is unit-testable.
//
// The product token embeds the REAL build version from the single-source
// `AMI_VERSION` macro (core/src/version.ts), not a hardcoded "1.0" — so the
// traffic AMI emits is attributable to the exact release and a version bump
// flows through here automatically (parity with the reference's
// `claude-code/${MACRO.VERSION}` single-source User-Agent). ADDITIVE.
export const DEFAULT_WEB_USER_AGENT = `SuperInference-AMI/${AMI_VERSION} (+https://superinference.org)`;
export function getUserAgent(): string {
  return process.env.SUPERINFERENCE_WEB_USER_AGENT || DEFAULT_WEB_USER_AGENT;
}

/**
 * Pre-flight Content-Length ceiling. If the server announces a body
 * larger than this we reject before downloading any bytes.
 */
export const MAX_RESPONSE_BYTES = 50 * 1024 * 1024;

/**
 * Content-Types that indicate executable or opaque binary payloads —
 * the tool should refuse to download these rather than letting them
 * reach the LLM context (or disk).
 */
export const BLOCKED_CONTENT_TYPES = new Set([
  'application/octet-stream',
  'application/x-executable',
  'application/x-sharedlib',
  'application/x-mach-binary',
  'application/vnd.microsoft.portable-executable',
]);

/**
 * Cloud metadata endpoints. Requests to these should be blocked even
 * when `isPrivateIP` / `BLOCKED_HOSTNAMES` wouldn't catch them (e.g.
 * ECS task-metadata at 169.254.170.2, IMDSv2 IPv6 at fd00:ec2::254).
 */
const METADATA_HOSTS = new Set([
  '169.254.169.254',
  'metadata.google.internal',
  '169.254.170.2',
  'fd00:ec2::254',
]);

export function isMetadataEndpoint(url: string): boolean {
  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase().replace(/^\[|\]$/g, '');
    return METADATA_HOSTS.has(host);
  } catch {
    return false;
  }
}

/**
 * Dependency seam for the redirect-follow path. The real implementation calls
 * validateUrlSafety (live DNS) on the redirect target; tests override this to
 * drive the successful-follow recursion against a loopback server without a
 * public DNS lookup. Production uses the real validateUrlSafety.
 */
export const _webHooks: {
  validateRedirectSafety: (url: string) => Promise<{ error: string } | { resolvedIP: string }>;
  /** Seam over `requester.get` so tests can inject a synthetic response to
   *  drive defensive arms (absent statusCode, late data after settle) that a
   *  real HTTP response never produces. Production delegates to the real get. */
  rawGet: (
    requester: typeof http | typeof https,
    url: string,
    options: http.RequestOptions,
    callback: (res: http.IncomingMessage) => void,
  ) => http.ClientRequest;
} = {
  validateRedirectSafety: (url) => validateUrlSafety(url),
  rawGet: (requester, url, options, callback) => requester.get(url, options, callback),
};

/**
 * Perform an HTTP(S) GET following redirects up to `maxRedirects` hops.
 * Returns the final response body as a string along with status metadata.
 */
export function httpGet(
  url: string,
  signal: AbortSignal,
  options?: {
    maxRedirects?: number;
    timeoutMs?: number;
    includeContentType?: boolean;
    resolvedIP?: string;
    /** Also return the raw response bytes (bodyBuffer), uncorrupted by UTF-8
     *  decoding — required for PDFs/images/other binary content. */
    raw?: boolean;
  },
): Promise<{ body: string; bodyBuffer?: Buffer; statusCode: number; contentType: string; finalUrl: string; truncated?: boolean; headers: http.IncomingHttpHeaders; redirectStatusCode?: number }> {
  const maxRedirects = options?.maxRedirects ?? 5;
  const timeoutMs = options?.timeoutMs ?? 30000;

  return httpGetInternal(url, signal, maxRedirects, timeoutMs, maxRedirects, options?.resolvedIP, options?.raw);
}

function httpGetInternal(
  url: string,
  signal: AbortSignal,
  redirectsLeft: number,
  timeoutMs: number,
  // The original redirect ceiling (constant across the recursion) — reported in
  // the "Too many redirects" error so the consumer sees the hop limit that was
  // reached (parity with the reference's `Too many redirects (exceeded ${MAX_REDIRECTS})`).
  maxRedirects: number,
  resolvedIP?: string,
  raw?: boolean,
  // Status code of the FIRST redirect hop followed on the way to the final
  // response (e.g. 301/302/307/308). Carried through the recursion so a caller
  // that detects a cross-origin redirect can report the actual redirect status +
  // reason phrase (parity with the reference's redirect-status message) instead of the
  // final 200. Undefined when no redirect was followed.
  firstRedirectStatus?: number,
): Promise<{ body: string; bodyBuffer?: Buffer; statusCode: number; contentType: string; finalUrl: string; truncated?: boolean; headers: http.IncomingHttpHeaders; redirectStatusCode?: number }> {
  const MAX_RESPONSE_LENGTH = 50000;

  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new Error('Aborted'));
      return;
    }

    let parsed: URL;
    try {
      parsed = new URL(url);
    } catch {
      reject(new Error(`Invalid URL: ${url}`));
      return;
    }

    const requester = parsed.protocol === 'https:' ? https : http;

    const pinIP = resolvedIP && isValidIP(resolvedIP) ? resolvedIP : undefined;
    const lookupOverride: http.RequestOptions['lookup'] = pinIP ? pinnedDnsLookup(pinIP) : undefined;

    const req = _webHooks.rawGet(
      requester,
      url,
      {
        headers: {
          'User-Agent': getUserAgent(),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.5',
          'Accept-Encoding': 'identity',
          'DNT': '1',
          'Connection': 'keep-alive',
          'Upgrade-Insecure-Requests': '1',
        },
        timeout: timeoutMs,
        ...(lookupOverride ? { lookup: lookupOverride } : {}),
      },
      (res) => {
        const statusCode = res.statusCode ?? 0;

        // Handle redirects
        if (
          [301, 302, 303, 307, 308].includes(statusCode) &&
          res.headers.location
        ) {
          if (redirectsLeft <= 0) {
            reject(new Error(`Too many redirects (exceeded ${maxRedirects} hops)`));
            return;
          }
          // Resolve relative redirects against the original URL
          const redirectUrl = new URL(res.headers.location, url).toString();
          // Refuse to auto-follow a redirect that crosses a protocol/port
          // boundary — e.g. an https→http TLS downgrade forced by a
          // server-controlled Location header, or a pivot to a different port on
          // the same host. This runs BEFORE the private-host check so a
          // downgrade/port-change to a PUBLIC host is still refused. (Cross-host
          // redirects to the same protocol/port are still followed, after SSRF
          // re-validation; embedded credentials are refused by the check below.)
          if (redirectCrossesSecurityBoundary(url, redirectUrl)) {
            reject(new Error(
              `Blocked: redirect "${url}" → "${redirectUrl}" crosses a protocol/port boundary. ${SSRF_LOOPBACK_AFFORDANCE}`,
            ));
            return;
          }
          if (isBlockedRedirectTarget(redirectUrl)) {
            reject(new Error(`Blocked: redirect to private/internal URL "${redirectUrl}". ${SSRF_LOOPBACK_AFFORDANCE}`));
            return;
          }
          res.resume(); // Drain the response body
          _webHooks.validateRedirectSafety(redirectUrl).then((ssrfResult) => {
            if ('error' in ssrfResult) {
              reject(new Error(ssrfResult.error));
              return;
            }
            httpGetInternal(redirectUrl, signal, redirectsLeft - 1, timeoutMs, maxRedirects, ssrfResult.resolvedIP, raw, firstRedirectStatus ?? statusCode).then(resolve, reject);
          }, reject);
          return;
        }

        // Egress-proxy allow-list block: a 403 whose X-Proxy-Error header marks it
        // as blocked-by-allowlist did NOT come from the origin — the request never
        // left the egress boundary. Surface it as a distinct EgressBlockedError
        // (carrying the blocked host) so the consumer reports "allow-list the
        // domain", not a misleading origin "Forbidden". An ordinary 403 from the
        // origin (no such header) is unaffected and flows through as a normal body.
        // Egress-proxy allow-list block: a 403 whose X-Proxy-Error header marks it
        // as blocked-by-allowlist did NOT come from the origin — the request never
        // left the egress boundary. Surface it as a distinct EgressBlockedError
        // (carrying the blocked host) so the consumer reports "allow-list the
        // domain", not a misleading origin "Forbidden". An ordinary 403 from the
        // origin (no such header) is unaffected and flows through as a normal body.
        if (
          statusCode === 403 &&
          res.headers[EGRESS_PROXY_ERROR_HEADER] === EGRESS_PROXY_BLOCKED_VALUE
        ) {
          res.resume();
          let domain = url;
          try { domain = new URL(url).hostname; } catch { /* keep raw url */ }
          reject(new EgressBlockedError(domain));
          return;
        }

        const chunks: Buffer[] = [];
        let totalBytes = 0;
        let settled = false;
        const finish = (truncated = false) => {
          if (settled) return;
          settled = true;
          const buf = Buffer.concat(chunks);
          resolve({
            // Decode per the server-declared charset (utf-8 default) instead of
            // forcing utf-8, so non-utf-8 pages are not mojibake-corrupted.
            body: decodeBody(buf, res.headers['content-type']),
            // Preserve raw bytes for binary consumers (PDF/image/octet-stream).
            ...(raw ? { bodyBuffer: buf } : {}),
            statusCode,
            contentType: res.headers['content-type'] ?? '',
            finalUrl: url,
            headers: res.headers,
            ...(truncated ? { truncated: true } : {}),
            ...(firstRedirectStatus !== undefined ? { redirectStatusCode: firstRedirectStatus } : {}),
          });
        };
        res.on('data', (chunk: Buffer) => {
          if (settled) return;
          const room = MAX_HTTP_BODY_BYTES - totalBytes;
          // Keep only the bytes inside the cap. The chunk that crosses it
          // can contain the unread tail (Node may deliver that tail in the
          // same buffer), and an exact landing on the cap must not read on.
          if (chunk.length >= room) {
            if (room > 0) chunks.push(chunk.subarray(0, room));
            totalBytes = MAX_HTTP_BODY_BYTES;
            finish(true);
            res.destroy();
            return;
          }
          totalBytes += chunk.length;
          chunks.push(chunk);
        });
        res.on('end', () => {
          finish();
        });
        res.on('error', (err) => {
          if (!settled) reject(err);
        });
      },
    );

    req.on('timeout', () => {
      req.destroy();
      reject(new Error(`Request timed out after ${timeoutMs}ms`));
    });

    req.on('error', (err) => {
      reject(err);
    });

    // Respect abort signal
    const onAbort = () => {
      req.destroy();
      reject(new Error('Aborted'));
    };
    signal.addEventListener('abort', onAbort, { once: true });

    // Clean up the abort listener when the request finishes
    req.on('close', () => {
      signal.removeEventListener('abort', onAbort);
    });
  });
}

/**
 * `httpGet` with automatic exponential-backoff retry for TRANSIENT failures
 * (no-response network errors and 5xx server responses), parity with the reference
 * Code's `axiosGetWithRetry`. Non-transient outcomes (4xx, SSRF blocks, abort,
 * bad URL) return/throw immediately — they are never retried. A cancelled/aborted
 * signal stops the backoff at once. ADDITIVE: a single successful first attempt
 * behaves exactly like `httpGet`.
 */
export async function httpGetWithRetry(
  url: string,
  signal: AbortSignal,
  options?: Parameters<typeof httpGet>[2] & {
    retryDelaysMs?: readonly number[];
    /** Test seam: overrides the abort-aware backoff delay. */
    _delay?: (ms: number, signal: AbortSignal) => Promise<void>;
  },
): ReturnType<typeof httpGet> {
  const delays = options?.retryDelaysMs ?? DEFAULT_HTTP_RETRY_DELAYS_MS;
  const sleep = options?._delay ?? delay;
  let lastError: unknown;
  for (let attempt = 0; attempt <= delays.length; attempt++) {
    if (signal.aborted) throw new Error('Aborted');
    try {
      const res = await httpGet(url, signal, options);
      // A 5xx is a transient server error: retry it (httpGet resolves on 5xx).
      if (res.statusCode >= 500 && attempt < delays.length) {
        await sleep(delays[attempt], signal);
        continue;
      }
      return res;
    } catch (err) {
      lastError = err;
      if (!isTransientHttpError(err) || attempt >= delays.length) throw err;
      await sleep(delays[attempt], signal);
    }
  }
  // Unreachable in practice (the loop either returns or throws), but satisfies
  // the type checker and surfaces the final transient error if it ever is.
  throw lastError;
}
