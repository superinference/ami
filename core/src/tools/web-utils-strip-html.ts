// Split from web-utils.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as http from 'http';
import * as dns from 'dns';
import { URL } from 'url';

/**
 * Whether a host is loopback/localhost — the only host to which a cleartext
 * `http://` credential may safely be sent (shared url-safety helper so the
 * cleartext-credential guard and any future caller agree on one definition).
 */
export function isLoopbackHost(host: string): boolean {
  const h = host.toLowerCase().replace(/^\[|\]$/g, '');
  return (
    h === 'localhost' || h.endsWith('.localhost') ||
    h === '::1' || h === '0.0.0.0' ||
    /^127\./.test(h)
  );
}

/**
 * Allowed-alternative affordance appended to SSRF block messages so a caller who
 * hits the guard learns the permitted shape without reading docs — the same
 * self-correction clause the reference closes its ssrfError with
 * (`utils/hooks/ssrfGuard.ts` ssrfError L287). Single canonical string so the web
 * tool messages AND the HTTP-hook block message (`hooks.ts`
 * `formatHttpHookBlockedMessage`) agree on one wording. ADDITIVE.
 */
export const SSRF_LOOPBACK_AFFORDANCE = 'Loopback (127.0.0.1, ::1) is allowed for local dev.';

/**
 * True iff a parsed URL carries userinfo (a `user:password@` or bare `user@`
 * component). Such URLs must never be fetched or followed: Node turns the
 * userinfo into a `Authorization: Basic …` header, so the embedded secret is
 * transmitted on the wire, and the `@` can be used to disguise the real host
 * (`http://trusted.com@evil.com/`). Shared canonical check so the SSRF validator
 * and the redirect guard agree on one definition. ADDITIVE.
 */
export function urlHasUserinfo(parsed: URL): boolean {
  return parsed.username !== '' || parsed.password !== '';
}

/**
 * Expand an IPv6 literal (optionally with a trailing dotted-decimal IPv4 tail)
 * into exactly 8 numeric hextet groups. Handles the single `::` compression and
 * the `::ffff:a.b.c.d` form. Returns null when the address is not a well-formed
 * 8-group IPv6 literal, so callers can safely fall through to other checks.
 *
 * Mirrors the reference's ssrfGuard expansion (utils/hooks/ssrfGuard.ts) so an
 * IPv4-mapped metadata address written in hex/compressed/expanded form is
 * normalized to the same octets as its dotted-decimal twin.
 */
export function expandIPv6Groups(addr: string): number[] | null {
  let tailHextets: number[] = [];
  // Trailing dotted-decimal IPv4 (e.g. ::ffff:169.254.169.254) → two hextets.
  if (addr.includes('.')) {
    const lastColon = addr.lastIndexOf(':');
    if (lastColon === -1) return null;
    const v4 = addr.slice(lastColon + 1);
    addr = addr.slice(0, lastColon);
    const octets = v4.split('.').map(Number);
    if (octets.length !== 4 || octets.some((n) => !Number.isInteger(n) || n < 0 || n > 255)) {
      return null;
    }
    tailHextets = [(octets[0] << 8) | octets[1], (octets[2] << 8) | octets[3]];
  }

  const dbl = addr.indexOf('::');
  let head: string[];
  let tail: string[];
  if (dbl === -1) {
    head = addr === '' ? [] : addr.split(':');
    tail = [];
  } else {
    const headStr = addr.slice(0, dbl);
    const tailStr = addr.slice(dbl + 2);
    head = headStr === '' ? [] : headStr.split(':');
    tail = tailStr === '' ? [] : tailStr.split(':');
  }

  const target = 8 - tailHextets.length;
  const fill = target - head.length - tail.length;
  // No `::` means no compression: the groups must fill `target` exactly.
  // With `::` the fill must stand for at least one zero group.
  if (dbl === -1) {
    if (fill !== 0) return null;
  } else if (fill < 1) {
    return null;
  }

  const groups = [...head, ...new Array<string>(Math.max(fill, 0)).fill('0'), ...tail];
  const nums = groups.map((h) => (/^[0-9a-f]{1,4}$/.test(h) ? parseInt(h, 16) : NaN));
  if (nums.some((n) => Number.isNaN(n) || n < 0 || n > 0xffff)) return null;
  const full = [...nums, ...tailHextets];
  return full.length === 8 ? full : null;
}

/**
 * Extract the embedded IPv4 address from an IPv4-mapped IPv6 address
 * (`::ffff:X.Y`) in ANY representation — dotted (`::ffff:1.2.3.4`), hex
 * (`::ffff:aabb:ccdd`), or expanded (`0:0:0:0:0:ffff:X:Y`). Returns the
 * dotted-decimal IPv4 string, or null when `addr` is not IPv4-mapped.
 */
export function extractMappedIPv4(addr: string): string | null {
  const g = expandIPv6Groups(addr);
  if (!g) return null;
  // IPv4-mapped prefix: first 80 bits zero, next 16 bits 0xffff.
  if (g[0] === 0 && g[1] === 0 && g[2] === 0 && g[3] === 0 && g[4] === 0 && g[5] === 0xffff) {
    const hi = g[6];
    const lo = g[7];
    return `${hi >> 8}.${hi & 0xff}.${lo >> 8}.${lo & 0xff}`;
  }
  return null;
}

export function isPrivateIP(ip: string): boolean {
  // Case-insensitive for IPv6 literals: DNS returns lowercase, but a redirect
  // host / caller-supplied address can be uppercase (e.g. "FE80::1"), and the
  // hex-prefix checks below are case-sensitive — normalize so an uppercase
  // link-local/ULA/loopback literal is not silently treated as public.
  ip = ip.toLowerCase();
  // IPv4-mapped IPv6 in ANY representation — dotted (::ffff:1.2.3.4), hex
  // (::ffff:a9fe:a9fe), or expanded (0:0:0:0:0:ffff:a9fe:a9fe) — is decoded to
  // its embedded IPv4 and re-checked. Without this, a hex/expanded mapped
  // metadata address (::ffff:a9fe:a9fe = 169.254.169.254) would bypass the
  // v4 checks AND the v6 prefix checks and be treated as public (SSRF escape).
  if (ip.includes(':')) {
    const mapped = extractMappedIPv4(ip);
    if (mapped !== null) return isPrivateIP(mapped);
  }
  // IPv6 wildcard
  if (ip === '::' || ip === '0.0.0.0') return true;
  // Carrier-grade NAT
  if (/^100\.(6[4-9]|[7-9]\d|1[01]\d|12[0-7])\./.test(ip)) return true;
  // IPv4 private ranges
  if (/^10\./.test(ip)) return true;
  if (/^172\.(1[6-9]|2\d|3[01])\./.test(ip)) return true;
  if (/^192\.168\./.test(ip)) return true;
  // "This host on this network" (0.0.0.0/8) — the whole first-octet-0 range,
  // not just the 0.0.0.0 literal. Many stacks route 0.0.0.x to localhost, so
  // it can bypass a loopback-only block (SSRF micro-gap).
  if (/^0\./.test(ip)) return true;
  // Loopback
  if (/^127\./.test(ip)) return true;
  // Link-local
  if (/^169\.254\./.test(ip)) return true;
  // AWS/GCP/Azure metadata
  if (ip === '169.254.169.254') return true;
  // IPv6 private ranges (only meaningful for colon-form addresses; a v4 literal
  // can never start with the hex letters below). Mirrors the reference's
  // ssrfGuard isBlockedV6 ranges so the WHOLE link-local / unique-local space is
  // caught, not just the fe80:/fc00: string prefixes.
  if (ip.includes(':')) {
    if (ip === '::1') return true;
    // fc00::/7 — unique-local (fc00:: through fdff::): first hex pair fc or fd.
    // Previously only `fc00:` matched, so fc01::..fcff:: were treated as public.
    if (ip.startsWith('fc') || ip.startsWith('fd')) return true;
    // fe80::/10 — link-local: first hextet in the fe80..febf range. Previously
    // only the literal `fe80:` prefix matched, so fe81::..febf:: slipped through.
    const firstHextet = ip.split(':')[0];
    if (firstHextet.length === 4 && firstHextet >= 'fe80' && firstHextet <= 'febf') return true;
  }
  return false;
}

/** DNS lookup signature used by validateUrlSafety — a dependency seam so tests
 *  can simulate a resolution that yields no usable (valid) addresses. Defaults
 *  to the real dns.lookup; production never overrides it. */
export type HostLookupAll = (
  hostname: string,
  options: { all: true },
  callback: (err: NodeJS.ErrnoException | null, addresses: dns.LookupAddress[]) => void,
) => void;

/**
 * DNS pin for SSRF. Node calls this with `{ all: true }` and then reads
 * `addresses[0].address`. A `(err, ip, family)` callback makes that
 * `undefined` and the fetch fails with "Invalid IP address: undefined".
 */
export function pinnedDnsLookup(pinIP: string): NonNullable<http.RequestOptions['lookup']> {
  const family = pinIP.includes(':') ? 6 : 4;
  const lookup = (hostname: string, options: unknown, callback?: unknown) => {
    let opts = options;
    let cb = callback as (err: NodeJS.ErrnoException | null, address: unknown, extra?: number) => void;
    if (typeof options === 'function') {
      cb = options as typeof cb;
      opts = {};
    }
    const all = typeof opts === 'object' && opts !== null && (opts as { all?: boolean }).all === true;
    if (all) {
      cb(null, [{ address: pinIP, family }]);
      return;
    }
    cb(null, pinIP, family);
  };
  return lookup as NonNullable<http.RequestOptions['lookup']>;
}

export function isValidIP(ip: string): boolean {
  if (!ip || ip === 'undefined' || ip === 'null') return false;
  // v4 — flat form avoids security/detect-unsafe-regex (no repeated quantifier group)
  if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(ip)) return true;
  // v6 (coarse — enough to refuse hostnames / "undefined")
  if (ip.includes(':')) return true;
  return false;
}

/**
 * Turn a Unicode code point into its character, returning '' for anything out
 * of range or otherwise invalid rather than throwing. Used by stripHtml's
 * numeric-entity decoder so a malformed reference is dropped, not fatal.
 */
export function fromCodePointSafe(cp: number): string {
  if (!Number.isInteger(cp) || cp < 0 || cp > 0x10ffff) return '';
  try {
    return String.fromCodePoint(cp);
  } catch {
    return '';
  }
}

/**
 * Map a declared charset label to a Node Buffer encoding. Covers the labels a
 * web page realistically declares; unknown labels fall through to the caller's
 * utf-8 default.
 */
export const CHARSET_TO_ENCODING: Record<string, BufferEncoding> = {
  'utf-8': 'utf-8', 'utf8': 'utf-8',
  'ascii': 'ascii', 'us-ascii': 'ascii',
  'latin1': 'latin1', 'latin-1': 'latin1',
  'iso-8859-1': 'latin1', 'iso8859-1': 'latin1',
  'windows-1252': 'latin1', 'cp1252': 'latin1',
  'utf-16le': 'utf-16le', 'utf16le': 'utf-16le',
  'ucs-2': 'utf-16le', 'ucs2': 'utf-16le',
};

/**
 * Extract the `charset` parameter from a Content-Type header, normalized to
 * lowercase, or null if absent/unparsable.
 */
export function parseCharset(contentType: string | undefined): string | null {
  if (!contentType) return null;
  const m = /charset\s*=\s*"?([^";]+)"?/i.exec(contentType);
  return m ? m[1].trim().toLowerCase() : null;
}

/**
 * Decode an HTTP body buffer to text using the charset declared in its
 * Content-Type (falling back to utf-8 when none is declared or the label is
 * unrecognized). Previously every body was force-decoded as utf-8, which
 * mojibake-corrupted pages served as iso-8859-1/windows-1252/utf-16. ADDITIVE:
 * utf-8 and charset-less responses decode exactly as before.
 */
export function decodeBody(buf: Buffer, contentType?: string): string {
  const charset = parseCharset(contentType);
  const enc = charset ? CHARSET_TO_ENCODING[charset] : undefined;
  return buf.toString(enc ?? 'utf-8');
}

/**
 * Strip HTML tags and return clean text.
 *
 * Removes <script>, <style>, and <head> blocks entirely (including their
 * content), then strips the remaining tags, collapses whitespace runs and
 * trims every line.
 */
export function stripHtml(html: string): string {
  let text = html;

  // Remove script blocks
  text = text.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, '');

  // Remove style blocks
  text = text.replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, '');

  // Remove head block
  text = text.replace(/<head\b[^>]*>[\s\S]*?<\/head>/gi, '');

  // Remove HTML comments
  text = text.replace(/<!--[\s\S]*?-->/g, '');

  // Convert <br>, <p>, <div>, <li>, heading tags to newlines before stripping
  text = text.replace(/<br\s*\/?>/gi, '\n');
  text = text.replace(/<\/(p|div|h[1-6]|li|tr|blockquote)>/gi, '\n');
  text = text.replace(/<(p|div|h[1-6]|li|tr|blockquote)\b[^>]*>/gi, '\n');

  // Preserve anchor link targets as Markdown [text](href) BEFORE the catch-all
  // tag strip below — otherwise every <a href> URL is discarded and the model
  // receives link text with no citable destination. Mirrors the reference's
  // turndown HTML→Markdown conversion, which keeps link targets
  // (WebFetchTool/utils.ts). The href value may be single/double/unquoted; the
  // anchor text (which can hold nested inline tags) is captured and those inner
  // tags are removed by the generic strip that follows, leaving [text](href).
  // ADDITIVE: anchors without an href fall through to the plain tag strip. An
  // empty href yields a bare [text]() which the model reads as un-linked text.
  text = text.replace(
    /<a\b[^>]*?\bhref\s*=\s*("([^"]*)"|'([^']*)'|([^\s"'>]+))[^>]*>([\s\S]*?)<\/a>/gi,
    (_m, _raw, dq, sq, uq, inner) => `[${inner}](${dq ?? sq ?? uq ?? ''})`,
  );

  // Strip remaining tags
  text = text.replace(/<[^>]+>/g, '');

  // Decode common HTML entities
  text = text.replace(/&amp;/g, '&');
  text = text.replace(/&lt;/g, '<');
  text = text.replace(/&gt;/g, '>');
  text = text.replace(/&quot;/g, '"');
  text = text.replace(/&#39;/g, "'");
  text = text.replace(/&nbsp;/g, ' ');

  // Decode numeric character references — decimal (&#8364;) and hex
  // (&#x2014;) — so typographic punctuation, currency and other symbols become
  // real characters instead of surviving as literal entity text (the previous
  // decoder handled only a fixed named set). ADDITIVE.
  text = text.replace(/&#(\d+);/g, (_m, d) => fromCodePointSafe(parseInt(d, 10)));
  text = text.replace(/&#x([0-9a-f]+);/gi, (_m, h) => fromCodePointSafe(parseInt(h, 16)));

  // Collapse runs of whitespace on each line, then trim each line
  text = text
    .split('\n')
    .map((line) => line.replace(/[ \t]+/g, ' ').trim())
    .join('\n');

  // Collapse 3+ consecutive blank lines into 2
  text = text.replace(/\n{3,}/g, '\n\n');

  return text.trim();
}

/**
 * Hard cap on a single HTTP body. Large enough to keep the tail of a
 * typical page or document, small enough that a hostile URL cannot
 * stream without bound. Bytes past this cap are not read.
 */
export const MAX_HTTP_BODY_BYTES = 1_048_576;

/**
 * A network egress proxy in front of the agent signals an allow-list block with a
 * `403` whose `X-Proxy-Error` header is exactly `blocked-by-allowlist`. That is a
 * fundamentally different failure from an origin's own `403 Forbidden` (the origin
 * was never reached), so it must be surfaced distinctly: the fix is to allow-list
 * the domain in the egress policy, not to change the request. Header name/value
 * mirror the reference's egress-proxy contract (`WebFetchTool/utils.ts`). ADDITIVE.
 */
export const EGRESS_PROXY_ERROR_HEADER = 'x-proxy-error';

export const EGRESS_PROXY_BLOCKED_VALUE = 'blocked-by-allowlist';

/**
 * Raised when the egress proxy refused the fetch (403 + the blocked-by-allowlist
 * marker above) rather than the origin returning a 403. Carries the blocked
 * `domain` so a consumer can tell the user exactly what to allow-list. Mirrors
 * the reference's `EgressBlockedError`.
 */
export class EgressBlockedError extends Error {
  constructor(public readonly domain: string) {
    super(`Access to ${domain} is blocked by the network egress proxy (allow-list).`);
    this.name = 'EgressBlockedError';
  }
}

/**
 * Exponential-backoff delays (ms) for transient HTTP GET failures: 5 total
 * attempts (1 initial + 4 retries). Mirrors the reference's teleport retry schedule
 * (TELEPORT_RETRY_DELAYS = 2s, 4s, 8s, 16s).
 */
export const DEFAULT_HTTP_RETRY_DELAYS_MS: readonly number[] = [2000, 4000, 8000, 16000];

/**
 * Classify a thrown fetch error as a TRANSIENT network error worth retrying.
 * Parity with the reference's `isTransientNetworkError`: retry on no-response
 * network failures (connection refused/reset, DNS, socket timeout) but NOT on
 * deterministic, non-transient conditions — an aborted/deadline signal, an SSRF
 * "Blocked:" refusal, an unparseable URL, or a redirect loop. (5xx server
 * responses are classified at the status-code level by httpGetWithRetry, since
 * httpGet resolves — rather than throws — on a 5xx.)
 */
export function isTransientHttpError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const m = err.message;
  if (m === 'Aborted') return false;
  if (m.startsWith('Blocked:')) return false;
  if (m.startsWith('Too many redirects')) return false;
  if (m.startsWith('Invalid URL')) return false;
  return true;
}

/** Abort-aware delay used between retry attempts; rejects with `Aborted` if the
 *  signal fires while waiting so a cancelled fetch stops backing off immediately. */
export function delay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (signal.aborted) {
      reject(new Error('Aborted'));
      return;
    }
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = (): void => {
      clearTimeout(timer);
      reject(new Error('Aborted'));
    };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}
