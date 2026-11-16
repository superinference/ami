/**
 * Shared SSRF protection and HTTP utilities for web-fetch and web-search tools.
 */

import * as http from 'http';
import * as https from 'https';
import * as dns from 'dns';
import { URL } from 'url';
import { AMI_VERSION } from '../version';

export const BLOCKED_HOSTNAMES = new Set([
  'localhost', '127.0.0.1', '::1', '0.0.0.0',
  'metadata.google.internal', 'metadata.google.com',
  'instance-data', 'metadata',
]);

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
 * Mirrors Claude Code's ssrfGuard expansion (utils/hooks/ssrfGuard.ts) so an
 * IPv4-mapped metadata address written in hex/compressed/expanded form is
 * normalized to the same octets as its dotted-decimal twin.
 */
function expandIPv6Groups(addr: string): number[] | null {
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
function extractMappedIPv4(addr: string): string | null {
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
  // can never start with the hex letters below). Mirrors Claude Code's
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
 * Mirrors the protocol/port arms of Claude Code's `isPermittedRedirect`
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

/** DNS lookup signature used by validateUrlSafety — a dependency seam so tests
 *  can simulate a resolution that yields no usable (valid) addresses. Defaults
 *  to the real dns.lookup; production never overrides it. */
export type HostLookupAll = (
  hostname: string,
  options: { all: true },
  callback: (err: NodeJS.ErrnoException | null, addresses: dns.LookupAddress[]) => void,
) => void;

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
    return { error: `Blocked: "${hostname}" is a private/metadata hostname` };
  }

  // Reject single-label (dot-less) hostnames before any DNS/network work. A bare
  // "intranet" / "wiki" style name is not publicly resolvable and, via a
  // search-domain suffix, typically expands to a privileged internal host — so it
  // is an SSRF surface that must be refused pre-DNS. IP literals (v4 dotted or v6
  // colon-form) and, under allowLoopback, loopback names are exempt. Mirrors
  // Claude Code's validateURL publicly-resolvable filter (WebFetchTool/utils.ts
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
        return { error: `Blocked: "${hostname}" resolves to private IP ${addr.address}` };
      }
    }
    return { resolvedIP: usable[0].address };
  } catch {
    return { error: `Blocked: DNS resolution failed for "${hostname}" — cannot verify safety` };
  }
}

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
function fromCodePointSafe(cp: number): string {
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
const CHARSET_TO_ENCODING: Record<string, BufferEncoding> = {
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

// Identify the agent so site operators can recognize/allowlist the traffic
// (Claude Code's identify-yourself policy). Override with SUPERINFERENCE_WEB_USER_AGENT
// (e.g. a browser UA) for sites that require it. Read at call time so the env var
// takes effect regardless of import order and is unit-testable.
//
// The product token embeds the REAL build version from the single-source
// `AMI_VERSION` macro (core/src/version.ts), not a hardcoded "1.0" — so the
// traffic AMI emits is attributable to the exact release and a version bump
// flows through here automatically (parity with Claude's
// `claude-code/${MACRO.VERSION}` single-source User-Agent). ADDITIVE.
export const DEFAULT_WEB_USER_AGENT = `SuperInference-AMI/${AMI_VERSION} (+https://superinference.org)`;
export function getUserAgent(): string {
  return process.env.SUPERINFERENCE_WEB_USER_AGENT || DEFAULT_WEB_USER_AGENT;
}

/**
 * Hard cap on a single HTTP body. Large enough to keep the tail of a
 * typical page or document, small enough that a hostile URL cannot
 * stream without bound. Bytes past this cap are not read.
 */
export const MAX_HTTP_BODY_BYTES = 1_048_576;

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
 * A network egress proxy in front of the agent signals an allow-list block with a
 * `403` whose `X-Proxy-Error` header is exactly `blocked-by-allowlist`. That is a
 * fundamentally different failure from an origin's own `403 Forbidden` (the origin
 * was never reached), so it must be surfaced distinctly: the fix is to allow-list
 * the domain in the egress policy, not to change the request. Header name/value
 * mirror Claude Code's egress-proxy contract (`WebFetchTool/utils.ts`). ADDITIVE.
 */
export const EGRESS_PROXY_ERROR_HEADER = 'x-proxy-error';
export const EGRESS_PROXY_BLOCKED_VALUE = 'blocked-by-allowlist';

/**
 * Raised when the egress proxy refused the fetch (403 + the blocked-by-allowlist
 * marker above) rather than the origin returning a 403. Carries the blocked
 * `domain` so a consumer can tell the user exactly what to allow-list. Mirrors
 * Claude Code's `EgressBlockedError`.
 */
export class EgressBlockedError extends Error {
  constructor(public readonly domain: string) {
    super(`Access to ${domain} is blocked by the network egress proxy (allow-list).`);
    this.name = 'EgressBlockedError';
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

  return httpGetInternal(url, signal, maxRedirects, timeoutMs, options?.resolvedIP, options?.raw);
}

function httpGetInternal(
  url: string,
  signal: AbortSignal,
  redirectsLeft: number,
  timeoutMs: number,
  resolvedIP?: string,
  raw?: boolean,
  // Status code of the FIRST redirect hop followed on the way to the final
  // response (e.g. 301/302/307/308). Carried through the recursion so a caller
  // that detects a cross-origin redirect can report the actual redirect status +
  // reason phrase (parity with Claude's redirect-status message) instead of the
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
            reject(new Error('Too many redirects'));
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
              `Blocked: redirect "${url}" → "${redirectUrl}" crosses a protocol/port boundary`,
            ));
            return;
          }
          if (isBlockedRedirectTarget(redirectUrl)) {
            reject(new Error(`Blocked: redirect to private/internal URL "${redirectUrl}"`));
            return;
          }
          res.resume(); // Drain the response body
          _webHooks.validateRedirectSafety(redirectUrl).then((ssrfResult) => {
            if ('error' in ssrfResult) {
              reject(new Error(ssrfResult.error));
              return;
            }
            httpGetInternal(redirectUrl, signal, redirectsLeft - 1, timeoutMs, ssrfResult.resolvedIP, raw, firstRedirectStatus ?? statusCode).then(resolve, reject);
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
 * Exponential-backoff delays (ms) for transient HTTP GET failures: 5 total
 * attempts (1 initial + 4 retries). Mirrors Claude Code's teleport retry schedule
 * (TELEPORT_RETRY_DELAYS = 2s, 4s, 8s, 16s).
 */
export const DEFAULT_HTTP_RETRY_DELAYS_MS: readonly number[] = [2000, 4000, 8000, 16000];

/**
 * Classify a thrown fetch error as a TRANSIENT network error worth retrying.
 * Parity with Claude Code's `isTransientNetworkError`: retry on no-response
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
  if (m === 'Too many redirects') return false;
  if (m.startsWith('Invalid URL')) return false;
  return true;
}

/** Abort-aware delay used between retry attempts; rejects with `Aborted` if the
 *  signal fires while waiting so a cancelled fetch stops backing off immediately. */
function delay(ms: number, signal: AbortSignal): Promise<void> {
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

/**
 * `httpGet` with automatic exponential-backoff retry for TRANSIENT failures
 * (no-response network errors and 5xx server responses), parity with Claude
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
