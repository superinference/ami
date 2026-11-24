// Split from provider.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { assembleCATrustStore } from './utils/mtls';
import { getCACertificates } from './ca-certs';
import { isEnvTruthy } from './analytics';
import { recordDiagnostic } from './logger';
import { EXTENDED_CACHE_TTL_BETA, INTERLEAVED_THINKING_BETA, ONE_MILLION_CONTEXT_BETA, PROMPT_CACHING_SCOPE_BETA, STRUCTURED_OUTPUTS_BETA, TOKEN_EFFICIENT_TOOLS_BETA, getExtraAnthropicBetas, mergeBetaTokens } from './provider-inject-anthropic-cache-control';
import type { AnthropicBetaDescriptor } from './provider-inject-anthropic-cache-control';

/**
 * A static, deterministic catalog of every `anthropic-beta` token AMI's provider
 * layer can attach to an outgoing Anthropic request, each paired with its
 * activation condition, PLUS the operator escape-hatch tokens currently forced on
 * via `ANTHROPIC_BETAS`. This is the introspection/diagnostic surface the request
 * path lacked (parity: the reference's `getSdkBetas()` session-metadata field): a consumer
 * can discover WHICH betas are in effect / available without decoding a live
 * request. Unlike {@link computeAnthropicBetas} it needs no model or API key, so it
 * is safe to emit from `ami --introspect`. Reads `ANTHROPIC_BETAS` live through the
 * single {@link getExtraAnthropicBetas} resolver (No-Duplication).
 */
export function describeAnthropicBetas(): {
  catalog: AnthropicBetaDescriptor[];
  operatorBetas: string[];
} {
  return {
    catalog: [
      { token: EXTENDED_CACHE_TTL_BETA, condition: '1h prompt-cache TTL (config.cacheTtl === "1h"), native Anthropic path only' },
      { token: ONE_MILLION_CONTEXT_BETA, condition: 'the active model serves a 1M-token context window (supportsOneMillionContextBeta) and 1M context is not disabled' },
      { token: PROMPT_CACHING_SCOPE_BETA, condition: 'cross-session prompt-cache scope opt-in (AMI_PROMPT_CACHE_SCOPE)' },
      { token: INTERLEAVED_THINKING_BETA, condition: 'the active model supports interleaved thinking and it is not opted out; stripped by the experimental-betas kill switch' },
      { token: TOKEN_EFFICIENT_TOOLS_BETA, condition: 'token-efficient tools opt-in; stripped by the experimental-betas kill switch' },
      { token: STRUCTURED_OUTPUTS_BETA, condition: 'the active model supports structured outputs and it is opted in; stripped by the experimental-betas kill switch' },
    ],
    operatorBetas: getExtraAnthropicBetas(),
  };
}

/**
 * Wrap a fetch so every outgoing request carries `betaToken` in its
 * `anthropic-beta` header, comma-joined with any token already set (never
 * duplicated). Used to add the 1M-context beta on the anthropic-vertex path,
 * where the header cannot be passed to the SDK constructor.
 */
export function withAnthropicBetaHeader(
  baseFetch: typeof globalThis.fetch,
  betaToken: string | string[],
): typeof globalThis.fetch {
  return (async (input: any, init?: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
    const headers = new Headers(init?.headers as Record<string, string> | undefined);
    headers.set('anthropic-beta', mergeBetaTokens(headers.get('anthropic-beta'), betaToken));
    return baseFetch(input, { ...init, headers });
  }) as typeof globalThis.fetch;
}

// ---------------------------------------------------------------------------
// Accurate provider token-count path (the reference parity: countMessagesTokensWithAPI).
//
// AMI's context accounting is otherwise a char-based heuristic (estimateTokens).
// This adds an OPTIONAL authoritative count via Anthropic's
// `/v1/messages/count_tokens` endpoint, governed by the same tunable knobs the reference
// uses, and the proactive Vertex beta allowlist. It is consumed as the injectable
// `TokenCounter` (common `ContextManager.setTokenCounter`) so the compaction
// waterfall prefers a real count and only falls back to the heuristic when this
// path is absent / returns null / throws.
// ---------------------------------------------------------------------------

/**
 * Minimal `thinking.budget_tokens` used for a count request when the transcript
 * contains thinking blocks. the reference parity: `TOKEN_COUNT_THINKING_BUDGET` — the API
 * requires `max_tokens > thinking.budget_tokens`, so this is kept small.
 */
export const TOKEN_COUNT_THINKING_BUDGET = 1024;

/**
 * Companion ceiling to {@link TOKEN_COUNT_THINKING_BUDGET} (the reference parity:
 * `TOKEN_COUNT_MAX_TOKENS`). The `count_tokens` endpoint does not consume
 * `max_tokens`, but the constant documents the API constraint
 * (`max_tokens > thinking.budget_tokens`) the budget is sized against.
 */
export const TOKEN_COUNT_MAX_TOKENS = 2048;

/** Inputs for an accurate provider token count. */
export interface CountTokensRequest {
  /** Anthropic Messages-format messages whose input tokens to count. */
  messages: Array<Record<string, unknown>>;
  /** Tool definitions (Anthropic wire shape); included in the count when present. */
  tools?: Array<Record<string, unknown>>;
  /** True when the transcript contains thinking blocks (enables the thinking budget). */
  thinking?: boolean;
  /** Abort signal for the request. */
  signal?: AbortSignal;
  /** Injectable fetch (tests); defaults to the global fetch. */
  fetchImpl?: typeof globalThis.fetch;
  /**
   * Bounded additional attempts on a *transient* failure before giving up and
   * returning `null`. the reference parity: `countMessagesTokensWithAPI` builds its
   * Anthropic client with `maxRetries: 1`, so a single flaky 5xx / network blip
   * on the best-effort count path self-heals instead of immediately degrading to
   * the heuristic. A non-transient failure (a 4xx other than 408/409/429, a 200
   * with a non-numeric body, an abort, or a non-network throw) is NOT retried —
   * re-sending it would just loop. Defaults to `1`; set `0` to disable.
   */
  maxRetries?: number;
}

// ---------------------------------------------------------------------------
// Provider-native (server-side) context editing — cache-preserving compaction.
//
// Every client-side compaction rewrites the message array, which changes the
// request prefix and busts the prompt cache on the next call. Anthropic's
// `context_management.edits` lets the PROVIDER clear old tool uses/inputs
// server-side while the cached prefix stays intact. This builds that edit (gated
// by an opt-in env knob) computing `clear_at_least = trigger - keepTarget`, and it
// rides every request via {@link withExtraBody}; {@link CONTEXT_MANAGEMENT_BETA}
// is added to the header by {@link resolveModel}. Mirrors the reference's
// `getAPIContextManagement` (services/compact/apiMicrocompact.ts).
// ---------------------------------------------------------------------------

/** Default server-side context-edit trigger (the reference parity: `DEFAULT_MAX_INPUT_TOKENS`). */
export const DEFAULT_CONTEXT_EDIT_MAX_INPUT_TOKENS = 180_000;

/** Default tokens to keep after a server-side edit (the reference parity: `DEFAULT_TARGET_INPUT_TOKENS`). */
export const DEFAULT_CONTEXT_EDIT_TARGET_INPUT_TOKENS = 40_000;

/** Opt-in knob enabling provider-native `context_management` server-side edits. */
export const API_CONTEXT_EDIT_ENV = 'AMI_API_CONTEXT_EDIT';

/** Overrides the server-side edit trigger (input-token threshold). */
export const API_CONTEXT_EDIT_MAX_INPUT_TOKENS_ENV = 'AMI_API_CONTEXT_EDIT_MAX_INPUT_TOKENS';

/** Overrides the server-side edit keep-target (tokens retained). */
export const API_CONTEXT_EDIT_TARGET_INPUT_TOKENS_ENV = 'AMI_API_CONTEXT_EDIT_TARGET_INPUT_TOKENS';

/** Parse a positive-integer env value, else the fallback. */
export function positiveIntEnv(raw: string | undefined, fallback: number): number {
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}

/**
 * Build the Anthropic `context_management` config (a `clear_tool_uses_20250919`
 * edit) when {@link API_CONTEXT_EDIT_ENV} is truthy, else `undefined` (feature
 * OFF — client-side compaction unchanged). `clear_at_least` is computed as
 * `trigger - keepTarget` (clamped at 0), exactly as the reference computes it, so
 * the server clears down to roughly `keepTarget` input tokens. `env` is injectable
 * for testing.
 */
export function buildAnthropicContextManagement(
  env: NodeJS.ProcessEnv = process.env,
): { edits: Array<Record<string, unknown>> } | undefined {
  if (!isEnvTruthy(env[API_CONTEXT_EDIT_ENV])) return undefined;
  const trigger = positiveIntEnv(env[API_CONTEXT_EDIT_MAX_INPUT_TOKENS_ENV], DEFAULT_CONTEXT_EDIT_MAX_INPUT_TOKENS);
  const keepTarget = positiveIntEnv(env[API_CONTEXT_EDIT_TARGET_INPUT_TOKENS_ENV], DEFAULT_CONTEXT_EDIT_TARGET_INPUT_TOKENS);
  const clearAtLeast = Math.max(0, trigger - keepTarget);
  return {
    edits: [
      {
        type: 'clear_tool_uses_20250919',
        trigger: { type: 'input_tokens', value: trigger },
        clear_at_least: { type: 'input_tokens', value: clearAtLeast },
        clear_tool_inputs: true,
      },
    ],
  };
}

// ---------------------------------------------------------------------------
// Proxy support (HTTPS_PROXY / HTTP_PROXY / NO_PROXY). NODE_EXTRA_CA_CERTS is
// already honored by Node at startup, so only proxy routing is wired here.
// ---------------------------------------------------------------------------

/** Parse a dotted-quad IPv4 literal to a 32-bit integer, or null when invalid. */
export function ipv4ToInt(ip: string): number | null {
  const parts = ip.split('.');
  if (parts.length !== 4) return null;
  let acc = 0;
  for (const p of parts) {
    if (!/^\d{1,3}$/.test(p)) return null;
    const n = Number(p);
    if (n > 255) return null;
    acc = acc * 256 + n;
  }
  return acc >>> 0;
}

/** Parse an IPv6 literal (with optional `::` compression) to a BigInt, or null. */
export function ipv6ToBigInt(ip: string): bigint | null {
  if (!ip.includes(':')) return null;
  const halves = ip.split('::');
  if (halves.length > 2) return null;
  const head = halves[0] ? halves[0].split(':') : [];
  const tail = halves.length === 2 ? (halves[1] ? halves[1].split(':') : []) : [];
  const groups = halves.length === 2
    ? [...head, ...Array(8 - head.length - tail.length).fill('0'), ...tail]
    : head;
  if (groups.length !== 8) return null;
  let acc = 0n;
  for (const g of groups) {
    if (!/^[0-9a-f]{1,4}$/.test(g)) return null;
    acc = (acc << 16n) + BigInt(parseInt(g, 16));
  }
  return acc;
}

/**
 * True when the literal IP `host` falls inside the CIDR `entry` (e.g.
 * `10.0.0.0/8`, `fd00::/8`). Returns false for non-IP hosts or malformed CIDRs
 * so a domain name never matches an IP range.
 */
export function ipInCidr(host: string, entry: string): boolean {
  const slash = entry.indexOf('/');
  const base = entry.slice(0, slash);
  const prefix = Number(entry.slice(slash + 1));
  if (!Number.isInteger(prefix) || prefix < 0) return false;
  if (base.includes(':') || host.includes(':')) {
    const net = ipv6ToBigInt(base);
    const addr = ipv6ToBigInt(host);
    if (net === null || addr === null || prefix > 128) return false;
    const shift = BigInt(128 - prefix);
    return (net >> shift) === (addr >> shift);
  }
  const net = ipv4ToInt(base);
  const addr = ipv4ToInt(host);
  if (net === null || addr === null || prefix > 32) return false;
  const mask = prefix === 0 ? 0 : (0xffffffff << (32 - prefix)) >>> 0;
  return (net & mask) === (addr & mask);
}

/**
 * NO_PROXY matcher supporting wildcard (`*`), exact host, domain suffix
 * (`.example.com`, glob `*.example.com`, or bare `example.com`), port-specific
 * entries (`example.com:8080`, compared against the request host:port), and
 * CIDR / IP-range entries (`10.0.0.0/8`, `fd00::/8`, matched only when the host
 * is a literal IP). A leading `*.` glob prefix is treated the same as a leading
 * `.` (both the apex and any subdomain match). `port` is the resolved request
 * port (protocol default applied by the caller).
 */
export function hostMatchesNoProxy(
  noProxy: string | undefined,
  hostname: string,
  port?: string,
): boolean {
  if (!noProxy) return false;
  // URL.hostname wraps IPv6 literals in brackets ([fd00::1]); strip them so the
  // bare address can be compared / range-matched.
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, '');
  const hostWithPort = port ? `${host}:${port}` : undefined;
  for (const raw of noProxy.split(/[,\s]+/)) {
    const entry = raw.trim().toLowerCase();
    if (!entry) continue;
    if (entry === '*') return true;
    // CIDR / IP-range entry — only a literal-IP host can match.
    if (entry.includes('/')) {
      if (ipInCidr(host, entry)) return true;
      continue;
    }
    // Port-specific entry `host:port` (single colon, not an IPv6 literal).
    if (hostWithPort && entry.includes(':') && ipv6ToBigInt(entry) === null) {
      if (hostWithPort === entry) return true;
      continue;
    }
    // Strip a leading `.` (domain suffix) OR a `*.` glob prefix — both forms
    // bypass the apex and every subdomain. A bare `*.` (empty remainder) matches
    // nothing, mirroring how a lone `.` would.
    const bare = entry.replace(/^(?:\*\.|\.)/, '');
    if (bare && (host === bare || host.endsWith('.' + bare))) return true;
  }
  return false;
}

/**
 * Test seam for the undici transport constructors so the proxy/mTLS dispatcher
 * wiring is unit-testable without a real proxy or monkeypatching the frozen
 * `undici` namespace. When a hook is set it overrides the lazily-imported ctor;
 * unset (the default) → the real undici ctor is used. Mirrors the `retryHooks` /
 * `requestIdHooks` DI pattern already used in this module.
 */
export const proxyTransportHooks: {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ProxyAgent?: new (opts: any) => unknown;
  Agent?: new (opts: unknown) => unknown;
} = {};

/**
 * Env knob (AMI_PROXY_RESOLVES_HOSTS) enabling proxy-side DNS: when truthy the
 * dispatcher hands the raw hostname to the proxy instead of resolving it locally,
 * for sandboxes where DNS is only reachable THROUGH the proxy. Mirrors the reference
 * Code's CLAUDE_CODE_PROXY_RESOLVES_HOSTS (utils/proxy.ts). Also accepts the
 * the reference name for drop-in parity.
 */
export function proxyResolvesHosts(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env.AMI_PROXY_RESOLVES_HOSTS) || isEnvTruthy(env.CLAUDE_CODE_PROXY_RESOLVES_HOSTS);
}

/**
 * Normalize a DNS address-family hint (0 | 4 | 6 | 'IPv4' | 'IPv6' | undefined)
 * to the numeric family undici's `lookup` callback expects. Mirrors the reference's
 * getAddressFamily (utils/proxy.ts). `0` means "unspecified / either family".
 */
export function getAddressFamily(family: number | string | undefined): 0 | 4 | 6 {
  if (family === 4 || family === 'IPv4') return 4;
  if (family === 6 || family === 'IPv6') return 6;
  return 0;
}

export type ProxyLookup = (
  hostname: string,
  options: { family?: number | string },
  callback: (err: Error | null, address: string | { address: string; family: number }[], family?: number) => void,
) => void;

/**
 * A DNS `lookup` that SKIPS local resolution and returns the hostname verbatim so
 * the proxy resolves it (proxy-side DNS). Installed on the dispatcher connect
 * options when {@link proxyResolvesHosts} is on.
 */
export function makeProxyLookup(): ProxyLookup {
  return (hostname, options, callback) => {
    callback(null, [{ address: hostname, family: getAddressFamily(options?.family) || 4 }]);
  };
}

export type ProxyConnectOptions = {
  cert?: string;
  key?: string;
  passphrase?: string;
  ca?: string | string[];
  lookup?: ProxyLookup;
};

/**
 * Build the undici `connect` / `requestTls` options for a custom dispatcher from
 * the configured mTLS material + the ambient env: merges the CA trust store (so a
 * custom `ca` never replaces the default roots — see {@link assembleCATrustStore})
 * and installs a proxy-side DNS lookup when {@link proxyResolvesHosts} is set.
 * Returns `undefined` when there is nothing to apply, so the default dispatcher
 * stays byte-for-byte unchanged. Exported for direct unit testing.
 */
export function buildProxyConnectOptions(
  env: NodeJS.ProcessEnv,
  tls: { cert?: string; key?: string; passphrase?: string; ca?: string } | undefined,
): ProxyConnectOptions | undefined {
  const opts: ProxyConnectOptions = {};
  if (tls?.cert) opts.cert = tls.cert;
  if (tls?.key) opts.key = tls.key;
  if (tls?.passphrase) opts.passphrase = tls.passphrase;
  // No client-cert material: resolve the env-only trust store through the memoized
  // {@link getCACertificates} (identical value to assembleCATrustStore(env, undefined),
  // but cached + invalidatable at runtime). With material, assemble per-material.
  const caBundle = tls?.ca !== undefined
    ? assembleCATrustStore(env, tls.ca)
    : getCACertificates(env);
  if (caBundle !== undefined) opts.ca = caBundle;
  if (proxyResolvesHosts(env)) opts.lookup = makeProxyLookup();
  return Object.keys(opts).length > 0 ? opts : undefined;
}

/** Cheap, content-light cache tag for a connect-options bundle (never the PEM). */
export function connectOptionsCacheTag(opts: ProxyConnectOptions | undefined): string {
  if (!opts) return '';
  const caLen = Array.isArray(opts.ca) ? opts.ca.length : (opts.ca ? 1 : 0);
  return `cert:${opts.cert ? 1 : 0}|ca:${caLen}|dns:${opts.lookup ? 1 : 0}`;
}

/** The structured-diagnostic event name for a resolved HTTP-dispatcher trust/proxy posture. */
export const NETWORK_TRUST_DIAGNOSTIC_EVENT = 'network_trust_resolved';

/**
 * Emit a PII-free structured diagnostic describing the TLS-trust / proxy posture
 * resolved for a just-created HTTP dispatcher — the machine-readable complement to
 * the human `log('provider', …)` line at each dispatcher-creation site. Routed to
 * the `AMI_DIAGNOSTICS_FILE` channel via {@link recordDiagnostic} (a zero-cost
 * no-op when that channel is unconfigured), it gives an operator an AUDITABLE
 * trail of HOW the CA-cert trust store and proxy routing were resolved for
 * outbound API traffic — so a TLS handshake failure behind a corporate proxy /
 * private CA is diagnosable from the diagnostics log without a live debugger.
 *
 * Only COARSE, non-PII fields are recorded — the cert COUNT + contributing SOURCES
 * already summarized by {@link describeCATrustStore} (e.g. `3 CA cert(s)
 * [bundled+extra-file]`), plus boolean posture flags — NEVER a proxy URL,
 * hostname, file path, or any key material (the diagnostics channel's no-PII
 * contract). The pre-computed `trustStoreDesc` is passed in rather than
 * re-assembled so the (file-reading) trust store is built at most once per
 * dispatcher. Mirrors the reference's `caCertsConfig.ts` `logForDebugging`
 * CA-resolution trail, routed into AMI's structured diagnostics channel.
 */
export function recordNetworkTrustDiagnostics(
  dispatcher: 'proxy' | 'mtls',
  trustStoreDesc: string | undefined,
  posture: { proxySideDns?: boolean; clientCert?: boolean } = {},
): void {
  recordDiagnostic('info', NETWORK_TRUST_DIAGNOSTIC_EVENT, {
    dispatcher,
    customTrustStore: trustStoreDesc !== undefined,
    ...(trustStoreDesc !== undefined ? { trustStore: trustStoreDesc } : {}),
    proxySideDns: posture.proxySideDns === true,
    clientCert: posture.clientCert === true,
  });
}

export function requestUrl(input: string | URL | { url?: unknown }): string | undefined {
  if (typeof input === 'string') return input;
  if (input instanceof URL) return input.toString();
  const maybe = (input as { url?: unknown }).url;
  return typeof maybe === 'string' ? maybe : undefined;
}
