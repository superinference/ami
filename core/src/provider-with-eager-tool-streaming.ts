// Split from provider.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { gzipSync } from 'node:zlib';
import { tmpdir } from 'node:os';
import { join as pathJoin } from 'node:path';
import { AWS_BEARER_TOKEN_BEDROCK_ENV } from './bedrock';
import { readAwsIniCredentials } from './utils/aws';
import { log } from './logger';
import type { ProviderConfig } from './types';
import { getAWSRegion } from './provider-interactive-auth-status';

/**
 * Validate an UNTRUSTED `tool_choice` value (e.g. from an API request body) into
 * the {@link ProviderConfig.toolChoice} shape, returning `undefined` for any
 * unrecognized value so the caller falls back to the model default. Accepts the
 * string modes `'auto' | 'required' | 'none'` and the pinned-tool object
 * `{ type: 'tool', toolName: <string> }` (with the OpenAI-wire `function.name`
 * form also normalized). Parity: the reference's `tool_choice` request parameter.
 */
export function normalizeToolChoice(value: unknown): ProviderConfig['toolChoice'] | undefined {
  if (value === 'auto' || value === 'required' || value === 'none') return value;
  if (isPlainRecord(value)) {
    // OpenAI wire form: { type: 'function', function: { name } }.
    if (value.type === 'function' && isPlainRecord(value.function) && typeof value.function.name === 'string' && value.function.name) {
      return { type: 'tool', toolName: value.function.name };
    }
    // Anthropic/native form: { type: 'tool', toolName }.
    if (value.type === 'tool' && typeof value.toolName === 'string' && value.toolName) {
      return { type: 'tool', toolName: value.toolName };
    }
  }
  return undefined;
}

/**
 * Resolve the gateway endpoint + auth for the amazon-bedrock OpenAI-compatible
 * path. Two ADDITIVE affordances over routing a raw `apiKey`/`baseUrl`:
 *
 *  1. **Bearer-token auth** — when {@link AWS_BEARER_TOKEN_BEDROCK_ENV} is set it
 *     takes precedence over `config.apiKey`, enabling the increasingly common
 *     Bedrock API-key auth mode without AWS access keys.
 *  2. **Region-derived default endpoint** — when the user gives no explicit
 *     `baseUrl`, construct Bedrock's OpenAI-compatible endpoint for the resolved
 *     region ({@link getAWSRegion}) instead of an empty base URL, so IAM-role /
 *     `AWS_DEFAULT_REGION` setups reach the correct regional endpoint.
 *  3. **`~/.aws/credentials` fallback** — when neither a bearer token nor an
 *     explicit/env `apiKey` is present, fall back to the access key id from the
 *     AWS shared-credentials file ({@link readAwsIniCredentials}), so an ini-only
 *     setup authenticates and a mid-session credential rotation is picked up once
 *     the cache is invalidated ({@link clearAwsIniCache}). Mirrors the reference's
 *     `fromIni` credential source.
 *
 * An explicit `config.baseUrl` (trailing slashes trimmed) always wins over the
 * derived default so a user's custom gateway is never overridden.
 */
export function resolveBedrockGateway(
  config: Pick<ProviderConfig, 'apiKey' | 'baseUrl'>,
  env: NodeJS.ProcessEnv = process.env,
): { apiKey: string; baseURL: string } {
  const bearer = (env[AWS_BEARER_TOKEN_BEDROCK_ENV] ?? '').trim();
  const apiKey = bearer
    || config.apiKey
    || readAwsIniCredentials({ env }).accessKeyId
    || '';
  const explicit = (config.baseUrl || '').replace(/\/+$/, '');
  const baseURL = explicit
    || `https://bedrock-runtime.${getAWSRegion(env)}.amazonaws.com/openai/v1`;
  return { apiKey, baseURL };
}

// ---------------------------------------------------------------------------
// Generic extra-body passthrough for OpenAI-compatible endpoints
// ---------------------------------------------------------------------------

/** True for a non-null, non-array plain object. */
export function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Deep-merge `overlay` into `base`, returning a new object. Nested plain
 * objects are merged recursively; every other value (scalars, arrays) from
 * `overlay` overrides the corresponding value in `base`.
 */
export function deepMergeRecords(
  base: Record<string, unknown>,
  overlay: Record<string, unknown>,
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...base };
  for (const [key, value] of Object.entries(overlay)) {
    const current = out[key];
    if (isPlainRecord(current) && isPlainRecord(value)) {
      out[key] = deepMergeRecords(current, value);
    } else {
      out[key] = value;
    }
  }
  return out;
}

/** Client request-id header name (L6). Carries a per-request id for log correlation / traceability. */
export const REQUEST_ID_HEADER = 'x-superinference-request-id';

/**
 * Append the client-generated request-id to a terminal API-error message so a user
 * can hand it to their provider/gateway and find the exact failed call in server
 * logs. The id is the ONLY correlation handle on a timeout/connection failure (the
 * server returned no response id). Idempotent (never appends twice) and a no-op
 * when there is no id. Mirrors the reference's `API error x-client-request-id=…` log.
 */
export function appendRequestIdToError(errorText: string | undefined, requestId: string | undefined): string {
  const base = errorText ?? '';
  if (!requestId || base.includes(REQUEST_ID_HEADER)) return base;
  const hint = `(${REQUEST_ID_HEADER}=${requestId} — give this id to your provider/gateway to find this request in their logs)`;
  return base ? `${base} ${hint}` : hint;
}

// ---------------------------------------------------------------------------
// Transport-boundary prompt/response debug capture (AMI_DUMP_PROMPTS).
//
// Parity with the reference's `createDumpPromptsFetch` (services/api/dumpPrompts.ts):
// a fetch wrapper that records the EXACT outgoing request (system prompt + full
// tool schemas + model params — everything in the JSON body) and the raw/streaming
// response to a JSONL file, for `/issue`-style debugging. Two invariants make it
// safe to leave composed into the live fetch chain:
//   1. It never PERTURBS the real API call — the request parse is deferred via
//      setImmediate (the body can be MBs), the response is read from a `clone()`
//      (so the caller's stream is untouched), and EVERY error is swallowed.
//   2. It is a zero-overhead identity passthrough when the env flag is unset, so
//      wrapping it unconditionally leaves non-debug behavior byte-for-byte identical.
// ---------------------------------------------------------------------------

/**
 * Whether raw prompt/response dumping is enabled. Any non-empty value other than
 * `0`/`false` enables it (so `AMI_DUMP_PROMPTS=1` and `AMI_DUMP_PROMPTS=/path/dir`
 * both enable), matching the "set to turn on" convention of the other AMI knobs.
 */
export function dumpPromptsEnabled(): boolean {
  const v = process.env.AMI_DUMP_PROMPTS;
  return typeof v === 'string' && v.trim() !== '' && v.trim() !== '0' && v.trim().toLowerCase() !== 'false';
}

/**
 * Resolve the directory the JSONL dump files are written to. When
 * `AMI_DUMP_PROMPTS` is itself a path (contains a path separator) it is used
 * directly; otherwise a per-user temp subdirectory is used.
 */
export function getPromptDumpDir(): string {
  const v = process.env.AMI_DUMP_PROMPTS?.trim();
  if (v && (v.includes('/') || v.includes('\\'))) return v;
  return pathJoin(tmpdir(), 'ami-prompt-dumps');
}

/** Parse an SSE body into its decoded `data:` JSON chunks (best-effort). */
export async function readDumpSseChunks(body: ReadableStream<Uint8Array>): Promise<unknown[]> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
    }
  } finally {
    reader.releaseLock();
  }
  const chunks: unknown[] = [];
  for (const event of buffer.split('\n\n')) {
    for (const line of event.split('\n')) {
      if (line.startsWith('data: ') && line !== 'data: [DONE]') {
        try { chunks.push(JSON.parse(line.slice(6))); } catch { /* ignore malformed SSE line */ }
      }
    }
  }
  return chunks;
}

export interface PromptDumpDeps {
  /** Override the env-derived enablement (tests). */
  enabled?: boolean;
  /** Override the dump directory (tests). */
  dir?: string;
  /** Distinguishes dump files per session/agent (defaults to the process id). */
  sessionId?: string;
  /** Timestamp source (tests). */
  now?: () => string;
  /** Defer function for the request dump (defaults to setImmediate). */
  defer?: (fn: () => void) => void;
  /** JSONL writer (tests); defaults to a best-effort fs append. */
  write?: (filePath: string, line: string) => void | Promise<void>;
}

export function withExtraHeaders(
  extraHeaders: Record<string, string> | undefined,
  baseFetch: typeof globalThis.fetch = globalThis.fetch,
): typeof globalThis.fetch | undefined {
  if (!extraHeaders || Object.keys(extraHeaders).length === 0) return undefined;
  const wrapped: typeof globalThis.fetch = (input, init) => {
    const existingHeaders = init?.headers ?? {};
    const merged = existingHeaders instanceof Headers
      ? (() => { const h = new Headers(existingHeaders); for (const [k, v] of Object.entries(extraHeaders)) h.set(k, v); return h; })()
      : Array.isArray(existingHeaders)
        ? [...existingHeaders, ...Object.entries(extraHeaders)]
        : { ...existingHeaders, ...extraHeaders };
    return baseFetch(input, { ...init, headers: merged });
  };
  return wrapped;
}

/**
 * Build a `fetch` wrapper that deep-merges `extraBody` into the JSON request
 * body of every outgoing OpenAI-compatible request. Returns `undefined` when
 * there is nothing to merge, so callers can pass it straight to the AI SDK
 * (which then uses its default fetch). Non-JSON bodies are passed through
 * untouched.
 */
export function withExtraBody(
  extraBody: Record<string, unknown> | undefined,
  baseFetch: typeof globalThis.fetch = globalThis.fetch,
): typeof globalThis.fetch | undefined {
  if (!extraBody || Object.keys(extraBody).length === 0) return undefined;
  const wrapped: typeof globalThis.fetch = (input, init) => {
    if (init && typeof init.body === 'string') {
      try {
        const parsed: unknown = JSON.parse(init.body);
        if (isPlainRecord(parsed)) {
          init = { ...init, body: JSON.stringify(deepMergeRecords(parsed, extraBody)) };
        }
      } catch {
        // Non-JSON body — leave it untouched.
      }
    }
    return baseFetch(input, init);
  };
  return wrapped;
}

export const GZIP_THRESHOLD_BYTES = 1024;

export function withGzipCompression(
  baseFetch: typeof globalThis.fetch = globalThis.fetch,
): typeof globalThis.fetch {
  return (input, init) => {
    if (init && typeof init.body === 'string' && init.body.length > GZIP_THRESHOLD_BYTES) {
      const compressed = gzipSync(Buffer.from(init.body, 'utf-8'));
      const existingHeaders = init.headers ?? {};
      const merged = existingHeaders instanceof Headers
        ? (() => { const h = new Headers(existingHeaders); h.set('content-encoding', 'gzip'); return h; })()
        : Array.isArray(existingHeaders)
          ? [...existingHeaders, ['content-encoding', 'gzip'] as [string, string]]
          : { ...existingHeaders, 'content-encoding': 'gzip' };
      return baseFetch(input, { ...init, body: compressed, headers: merged });
    }
    return baseFetch(input, init);
  };
}

/**
 * Add Anthropic's per-tool `eager_input_streaming: true` field to tools in a RAW
 * Anthropic Messages request body (fine-grained tool streaming). This is set on
 * the wire body rather than through the AI SDK because the SDK exposes no per-tool
 * passthrough for it. Returns the original text unchanged on a non-JSON body, a
 * body with no `tools` array, or when the knob is off — so it is a safe no-op in
 * every path except first-party Anthropic with the opt-in set.
 *
 * When `onlyToolNames` is omitted the field is applied to EVERY tool (the
 * process-wide `SUPERINFERENCE_FINE_GRAINED_TOOL_STREAMING` opt-in). When it is
 * provided, the field is applied ONLY to tools whose `name` is in the set — the
 * per-tool {@link ToolDefinition.eagerInputStreaming} opt-in — leaving all other
 * tools untouched.
 *
 * Gated (by the caller) to the first-party `api.anthropic.com` path only: proxies
 * (LiteLLM etc.) and Bedrock/Vertex reject this field with a 400, exactly as
 * documented in the reference's `buildToolSchema` (utils/api.ts) — which is why the
 * field is applied here, after provider routing has confirmed the native path.
 */
export function withEagerToolStreaming(
  baseFetch: typeof globalThis.fetch = globalThis.fetch,
  onlyToolNames?: ReadonlySet<string>,
): typeof globalThis.fetch {
  return (input, init) => {
    if (init && typeof init.body === 'string') {
      try {
        const parsed: unknown = JSON.parse(init.body);
        if (isPlainRecord(parsed) && Array.isArray(parsed.tools) && parsed.tools.length > 0) {
          parsed.tools = parsed.tools.map((t) => {
            if (!isPlainRecord(t)) return t;
            if (onlyToolNames && !(typeof t.name === 'string' && onlyToolNames.has(t.name))) return t;
            return { ...t, eager_input_streaming: true };
          });
          init = { ...init, body: JSON.stringify(parsed) };
        }
      } catch {
        // Non-JSON body — leave it untouched.
      }
    }
    return baseFetch(input, init);
  };
}

/**
 * OpenAI routes a conversation to one cache when every turn sends the same
 * `prompt_cache_key`. vLLM and other compatible servers reject or ignore
 * unknown fields, so the key is attached only for official OpenAI hosts.
 * An explicit `extraBody.prompt_cache_key` is left unchanged.
 */
export function openAIPromptCacheBody(
  extraBody: Record<string, unknown> | undefined,
  cacheKey: string | undefined,
  provider: string | undefined,
  baseUrl: string | undefined,
): Record<string, unknown> | undefined {
  if (!cacheKey) return extraBody;
  const host = baseUrl ?? '';
  const official =
    host.includes('api.openai.com') ||
    host.includes('openai.azure.com') ||
    ((provider === 'openai' || provider === 'azure-openai') && host.length === 0);
  if (!official) return extraBody;
  if (extraBody && Object.prototype.hasOwnProperty.call(extraBody, 'prompt_cache_key')) {
    return extraBody;
  }
  return { ...(extraBody ?? {}), prompt_cache_key: cacheKey };
}

/**
 * Env vars carrying a JSON object that is deep-merged into every OpenAI-compatible
 * request body (parity: the reference's `CLAUDE_CODE_EXTRA_BODY` / `getExtraBodyParams`).
 * `AMI_EXTRA_BODY` is AMI's native spelling; the reference name is honored for drop-in
 * compatibility. A zero-code, deploy-time override for gateways/proxies that must
 * inject an extra body field fleet-wide without a CLI flag or config file.
 */
export const EXTRA_BODY_ENV_VARS = ['AMI_EXTRA_BODY', 'CLAUDE_CODE_EXTRA_BODY'] as const;

/**
 * Parse the extra-body env var into a plain object, or `undefined` when unset,
 * malformed, or not a JSON object. The first env var that parses to an object wins;
 * malformed / non-object values are logged (mirroring the reference's `getExtraBodyParams`
 * error log) and skipped rather than thrown, so a bad env never aborts a request.
 */
export function resolveExtraBodyFromEnv(
  env: NodeJS.ProcessEnv = process.env,
): Record<string, unknown> | undefined {
  for (const name of EXTRA_BODY_ENV_VARS) {
    const raw = env[name];
    if (!raw || raw.trim() === '') continue;
    try {
      const parsed: unknown = JSON.parse(raw);
      if (isPlainRecord(parsed)) return { ...parsed };
      log('provider', `${name} must be a JSON object; ignoring`, { value: raw }, 'error');
    } catch (err) {
      log('provider', `failed to parse ${name} as JSON; ignoring`, { error: err instanceof Error ? err.message : String(err) }, 'error');
    }
  }
  return undefined;
}

/**
 * Env vars carrying a stable user/session identifier forwarded to the provider as
 * the request metadata user id (Anthropic `metadata.user_id` / OpenAI `user`).
 * Parity: the reference's `getAPIMetadata().user_id` (services/api/claude.ts). The
 * AMI-native spelling takes precedence; the reference name is accepted for parity.
 */
export const METADATA_USER_ID_ENV_VARS = ['AMI_METADATA_USER_ID', 'CLAUDE_CODE_METADATA_USER_ID'] as const;

/**
 * Resolve the request metadata user id: an explicit `config.metadata.userId` wins,
 * otherwise the first non-empty env knob. `undefined` ⇒ no metadata is attached.
 */
export function resolveMetadataUserId(
  config: ProviderConfig,
  env: NodeJS.ProcessEnv = process.env,
): string | undefined {
  const explicit = config.metadata?.userId;
  if (typeof explicit === 'string' && explicit.trim() !== '') return explicit;
  for (const name of METADATA_USER_ID_ENV_VARS) {
    const raw = env[name];
    if (raw && raw.trim() !== '') return raw;
  }
  return undefined;
}

/**
 * Env vars carrying arbitrary extra request metadata as a JSON object, merged into
 * the forwarded metadata value for provider/gateway-side correlation. Parity:
 * the reference's `CLAUDE_CODE_EXTRA_METADATA` (getAPIMetadata, services/api/claude.ts).
 * AMI-native spelling first, the reference name accepted as a drop-in alias.
 */
export const EXTRA_METADATA_ENV_VARS = ['AMI_EXTRA_METADATA', 'CLAUDE_CODE_EXTRA_METADATA'] as const;

/**
 * Resolve the extra-metadata object: `config.metadata.extra` wins, otherwise the
 * first env knob that parses to a plain JSON object. A set-but-invalid value (non
 * JSON, or JSON that is not a plain object) is logged and ignored — never thrown —
 * so a malformed knob can never fail a request. `undefined` ⇒ no extra metadata.
 */
export function resolveExtraMetadata(
  config: ProviderConfig,
  env: NodeJS.ProcessEnv = process.env,
): Record<string, unknown> | undefined {
  const explicit = config.metadata?.extra;
  if (isPlainRecord(explicit)) return explicit;
  for (const name of EXTRA_METADATA_ENV_VARS) {
    const raw = env[name];
    if (!raw || raw.trim() === '') continue;
    try {
      const parsed = JSON.parse(raw);
      if (isPlainRecord(parsed)) return parsed;
      log('api', `${name} must be a JSON object; ignoring`, undefined, 'error');
    } catch {
      log('api', `${name} is not valid JSON; ignoring`, undefined, 'error');
    }
    return undefined;
  }
  return undefined;
}
