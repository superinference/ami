// Split from analytics.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as crypto from 'crypto';
import { redactCredentials, redactPaths } from './credential-guard';
import { isEnvTruthy } from './privacy-level';

export interface AnalyticsEvent {
  timestamp: string;
  type: string;
  sessionId?: string;
  data: Record<string, unknown>;
  /** Monotonic per-session ordering counter, assigned on record. */
  sequence?: number;
}

export const MAX_IN_MEMORY_EVENTS = 1000;

// ---------------------------------------------------------------------------
// Redaction / data-minimization for analytics event payloads
// ---------------------------------------------------------------------------

/** String values longer than this are truncated (with a marker) before egress. */
export const MAX_ANALYTICS_STRING_LEN = 2048;

/** Bounded recursion depth when sanitizing nested tool-input-like values. */
export const MAX_SANITIZE_DEPTH = 6;

export function toolDetailsEnabled(env: NodeJS.ProcessEnv): boolean {
  return isEnvTruthy(env.AMI_LOG_TOOL_DETAILS) || isEnvTruthy(env.OTEL_LOG_TOOL_DETAILS);
}

/**
 * Whether raw tool input/output CONTENT may be exported — an explicit opt-in via
 * `OTEL_LOG_TOOL_CONTENT` (mirrors the reference's isToolContentLoggingEnabled). Distinct
 * from tool-DETAILS (which only unlocks the structured tool-input serializer) and
 * from user-prompt logging (which unlocks prompts/messages).
 */
export function isToolContentLoggingEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env.OTEL_LOG_TOOL_CONTENT);
}

// Tunable bounds for telemetry tool-input serialization (mirror the reference's metadata.ts).
/** Strings longer than this (chars) are clipped in bounded tool-input serialization. */
export const TOOL_INPUT_STRING_TRUNCATE_AT = 512;

/** Clipped tool-input strings keep this many leading chars (+ a length marker). */
export const TOOL_INPUT_STRING_TRUNCATE_TO = 128;

/** Max array entries / object keys kept per level (+ an overflow count marker). */
export const TOOL_INPUT_MAX_COLLECTION_ITEMS = 20;

/** Max nesting depth walked before collapsing to `<nested>`. */
export const TOOL_INPUT_MAX_DEPTH = 2;

/**
 * Bounds-safe recursive serializer for an arbitrary tool-input value before it
 * enters telemetry: long strings are clipped (keeping a `[N chars]` marker), arrays
 * and objects are capped at {@link TOOL_INPUT_MAX_COLLECTION_ITEMS} entries (with an
 * overflow marker), nesting is bounded at {@link TOOL_INPUT_MAX_DEPTH}, and internal
 * marker keys (`_*`) are stripped. Mirrors the reference's `truncateToolInputValue` —
 * defends against pathological (huge / deep / wide) payloads.
 */
export function truncateToolInputValue(value: unknown, depth = 0): unknown {
  if (typeof value === 'string') {
    if (value.length > TOOL_INPUT_STRING_TRUNCATE_AT) {
      return `${value.slice(0, TOOL_INPUT_STRING_TRUNCATE_TO)}…[${value.length} chars]`;
    }
    return value;
  }
  if (typeof value === 'number' || typeof value === 'boolean' || value === null || value === undefined) {
    return value;
  }
  if (depth >= TOOL_INPUT_MAX_DEPTH) return '<nested>';
  if (Array.isArray(value)) {
    const mapped: unknown[] = value.slice(0, TOOL_INPUT_MAX_COLLECTION_ITEMS).map(v => truncateToolInputValue(v, depth + 1));
    if (value.length > TOOL_INPUT_MAX_COLLECTION_ITEMS) mapped.push(`…[${value.length} items]`);
    return mapped;
  }
  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>).filter(([k]) => !k.startsWith('_'));
    const mapped: Array<[string, unknown]> = entries
      .slice(0, TOOL_INPUT_MAX_COLLECTION_ITEMS)
      .map(([k, v]) => [k, truncateToolInputValue(v, depth + 1)]);
    if (entries.length > TOOL_INPUT_MAX_COLLECTION_ITEMS) mapped.push(['…', `${entries.length} keys`]);
    return Object.fromEntries(mapped);
  }
  return String(value);
}

/**
 * Collapse a user-controlled MCP tool name (`mcp__<server>__<tool>`) to the fixed
 * literal `mcp_tool` before it enters analytics — the server segment reveals
 * user-specific MCP config and may embed filepaths. Built-in names are a fixed
 * vocabulary and pass through. Gated off by AMI_LOG_TOOL_DETAILS / OTEL_LOG_TOOL_DETAILS.
 */
export function sanitizeToolNameForAnalytics(name: string, opts?: { logToolDetails?: boolean }): string {
  if (!name) return name;
  if (opts?.logToolDetails) return name;
  if (name.startsWith('mcp__')) return 'mcp_tool';
  return name;
}

/**
 * Longest file extension (without the dot) recorded verbatim in analytics; anything
 * longer collapses to 'other' so hash-named files can't become high-cardinality tags.
 */
export const MAX_FILE_EXTENSION_LENGTH = 10;

/**
 * Reduce a file path to just its extension for analytics (never log the raw path —
 * it can embed project names / usernames / PII). Returns '' for a path with no
 * extension and 'other' for an implausibly long extension (mirrors the reference's
 * getFileExtensionForAnalytics).
 */
export function getFileExtensionForAnalytics(filePath: string): string {
  if (!filePath) return '';
  const base = filePath.replace(/[\\/]+$/, '').split(/[\\/]/).pop() ?? '';
  const dot = base.lastIndexOf('.');
  if (dot <= 0) return ''; // no dot, or a leading-dot dotfile (".env")
  const ext = base.slice(dot + 1).toLowerCase();
  if (!ext) return '';
  return ext.length > MAX_FILE_EXTENSION_LENGTH ? 'other' : ext;
}

/** Shell operators/whitespace a bash command is split on when extracting file extensions. */
export const BASH_TOKEN_SPLIT = /[\s;&|<>()]+/;

/**
 * Collapse a user-defined agent/subagent name to the constant 'user-defined' before
 * it enters analytics (custom names can embed project names, paths, or identifiers).
 * A name present in `builtins` is a fixed vocabulary and passes through unchanged.
 */
export function sanitizeAgentName(name: string, builtins: ReadonlySet<string>): string {
  if (!name) return name;
  return builtins.has(name) ? name : 'user-defined';
}

/** Length (hex chars) of a privacy-preserving file-path hash (matches the reference's 16). */
export const FILE_PATH_HASH_LEN = 16;

/**
 * Privacy-preserving, STABLE hash of a file path for analytics: a truncated
 * ({@link FILE_PATH_HASH_LEN}-char) SHA-256 so the same path always maps to the same
 * token (enabling per-file correlation/dedup downstream) while the raw path — which
 * can embed usernames, project names, or other PII — never leaves the machine.
 * Mirrors the reference's `hashFilePath`.
 */
export function hashFilePath(filePath: string): string {
  return crypto.createHash('sha256').update(filePath).digest('hex').slice(0, FILE_PATH_HASH_LEN);
}

export function redactString(s: string): string {
  // Scrub secrets AND local file paths before a free-text string (e.g. an inline
  // error message) is persisted to the analytics JSONL: analytics already hashes
  // KNOWN path fields (hashFilePath), but an absolute path embedded in an opaque
  // string value would otherwise leak the local username/machine layout off the
  // machine. Mirrors the credential scrub already applied here. (URLs are left
  // intact — an endpoint/host is legitimate telemetry; the loggable-error path
  // uses toLoggableError for URL scrubbing.)
  const redacted = redactPaths(redactCredentials(s));
  if (redacted.length <= MAX_ANALYTICS_STRING_LEN) return redacted;
  return redacted.slice(0, MAX_ANALYTICS_STRING_LEN) + `…[+${redacted.length - MAX_ANALYTICS_STRING_LEN}]`;
}

/** Recursively sanitize a single value (credential-redact + truncate strings, bound depth). */
export function sanitizeAnalyticsValue(value: unknown, depth = 0): unknown {
  if (typeof value === 'string') return redactString(value);
  if (value === null || typeof value !== 'object') return value;
  if (depth >= MAX_SANITIZE_DEPTH) return '[nested]';
  if (Array.isArray(value)) return value.map(v => sanitizeAnalyticsValue(v, depth + 1));
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    if (k.startsWith('_')) continue;
    out[k] = sanitizeAnalyticsValue(v, depth + 1);
  }
  return out;
}

export interface SanitizeOpts {
  env?: NodeJS.ProcessEnv;
  /** When false (default), user-prompt keys (prompt/message/…) are redacted to length-only. */
  logUserPrompts?: boolean;
  /**
   * When false (default), tool input/output CONTENT keys are redacted to length-only.
   * Defaults to `logUserPrompts || OTEL_LOG_TOOL_CONTENT || beta detailed tracing`, so
   * an operator can export tool IO content for debugging without exporting prompts.
   */
  logToolContent?: boolean;
  logToolDetails?: boolean;
}

/**
 * Compile-time brand marking a metadata record that has passed the canonical
 * analytics redaction / data-minimization pass — AMI's counterpart of the reference's
 * `AnalyticsMetadata_I_VERIFIED_THIS_IS_NOT_CODE_OR_FILEPATHS` guard. The brand is
 * produced ONLY by {@link sanitizeAnalyticsData} (the single redactor) or the
 * explicit {@link markAnalyticsSafe} escape hatch, and the telemetry egress sink
 * accepts ONLY branded data — so raw, unverified metadata containing code or file
 * paths cannot reach the on-disk JSONL / OTLP export by construction (it is a type
 * error), complementing the runtime stripping the pass already performs.
 */
export declare const ANALYTICS_SAFE: unique symbol;

export type AnalyticsSafeMetadata = Record<string, unknown> & { readonly [ANALYTICS_SAFE]: true };

/**
 * Brand a metadata record as analytics-safe WITHOUT running the redaction pass —
 * the escape hatch for data the caller has already verified contains no code, file
 * paths, or secrets (e.g. a fixed-vocabulary enum such as a slash-command source).
 * Mirrors the reference's `value as AnalyticsMetadata_I_VERIFIED_THIS_IS_NOT_CODE_OR_FILEPATHS`
 * cast at trusted call sites. Prefer {@link sanitizeAnalyticsData} whenever the
 * data derives from user or tool input.
 */
export function markAnalyticsSafe(data: Record<string, unknown>): AnalyticsSafeMetadata {
  return data as AnalyticsSafeMetadata;
}

/** Decide whether to keep an event given a 0..1 sample rate (0 drops all, >=1 keeps all). */
export function shouldSampleEvent(sampleRate: number, rand: () => number = Math.random): boolean {
  if (!(sampleRate > 0)) return false;
  if (sampleRate >= 1) return true;
  return rand() < sampleRate;
}

/**
 * Resolve the effective 0..1 sample rate for one event type from a per-type rate
 * map, falling back to `fallback` (the tracker-wide default) when the type has no
 * entry or carries an out-of-range/non-finite value (mirrors the reference's
 * EventSamplingConfig validation — bad values are treated as "no per-type rate").
 */
export function resolveSampleRate(
  type: string,
  rates: Record<string, number> | undefined,
  fallback: number,
): number {
  const r = rates?.[type];
  if (typeof r === 'number' && Number.isFinite(r) && r >= 0) return r;
  return fallback;
}

/**
 * JSON-serialize a value, returning null when it throws (BigInt, circular ref, a
 * throwing toJSON). Used to quarantine a single "poison" telemetry event so it can
 * never block the head of the queue or sink a whole batch.
 */
export function trySerialize(value: unknown): string | null {
  try {
    // JSON.stringify returns `undefined` (not a string) for `undefined`/function/
    // symbol inputs; coalesce that to null. Circular / BigInt inputs still throw.
    return JSON.stringify(value) ?? null;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Beta detailed content tracing toggle (the reference parity)
// ---------------------------------------------------------------------------

/** Resolved detailed-content-tracing configuration. */
export interface BetaTracingConfig {
  /** True only when BOTH the opt-in flag is truthy AND a dedicated endpoint is set. */
  enabled: boolean;
  /** The dedicated tracing endpoint (BETA_TRACING_ENDPOINT), or null when unset. */
  endpoint: string | null;
}

/**
 * Resolve the detailed-content-tracing toggle: the reference gates full-content tracing on
 * an explicit boolean (`ENABLE_BETA_TRACING_DETAILED`, via isEnvTruthy) AND a
 * dedicated endpoint (`BETA_TRACING_ENDPOINT`) — detailed tracing is enabled only
 * when both are present, so a stray flag with no endpoint stays off (and vice-versa).
 * Mirrors the reference's `isBetaTracingEnabled` base gate.
 */
export function resolveBetaTracingConfig(env: NodeJS.ProcessEnv = process.env): BetaTracingConfig {
  const endpoint = env.BETA_TRACING_ENDPOINT?.trim() || null;
  const enabled = isEnvTruthy(env.ENABLE_BETA_TRACING_DETAILED) && endpoint !== null;
  return { enabled, endpoint };
}

// ---------------------------------------------------------------------------
// Egress allowlist (the reference datadog.ts parity): a double gate limiting WHICH
// event names and WHICH data fields are allowed to leave the machine.
// ---------------------------------------------------------------------------

/** A double egress gate: an optional event-name allowlist and an optional field allowlist. */
export interface EgressAllowlist {
  /** When set, ONLY events whose TYPE is listed are exported (others are dropped). */
  events?: ReadonlySet<string> | readonly string[];
  /** When set, ONLY these data FIELDS survive on the exported copy (others removed). */
  fields?: ReadonlySet<string> | readonly string[];
}

export function toSet(v: ReadonlySet<string> | readonly string[] | undefined): ReadonlySet<string> | null {
  if (v === undefined) return null;
  return v instanceof Set ? v : new Set(v);
}

/**
 * Apply an egress allowlist to one event just before it leaves the machine. Returns
 * null when the event TYPE is not on the event allowlist (drop it entirely); else
 * returns the data filtered to the FIELD allowlist (unlisted fields removed). An
 * unset axis means "no restriction on that axis". Mirrors the reference's datadog.ts
 * DATADOG_ALLOWED_EVENTS (event-name gate) + TAG_FIELDS (field gate) double control.
 */
export function filterEventForEgress(
  type: string,
  data: Record<string, unknown>,
  allow: EgressAllowlist,
): Record<string, unknown> | null {
  const eventSet = toSet(allow.events);
  if (eventSet && !eventSet.has(type)) return null;
  const fieldSet = toSet(allow.fields);
  if (!fieldSet) return data;
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(data)) {
    if (fieldSet.has(k)) out[k] = v;
  }
  return out;
}

/**
 * A matcher selecting data FIELDS that are local-only: present on the privileged,
 * on-machine sinks (in-memory + JSONL + console) but stripped from the general-access,
 * off-machine OTLP export. Either an explicit set of field NAMES, a key PREFIX, or
 * both (a key matches if it is in `names` OR starts with `prefix`).
 */
export interface LocalOnlyFieldMatcher {
  names?: ReadonlySet<string>;
  prefix?: string;
}

/**
 * Copy-on-write strip of local-only fields from a payload destined for off-machine
 * export: returns the SAME reference when no field matches (the common case — no
 * allocation), else a shallow clone with the matching keys removed. Mirrors the reference's
 * `stripProtoFields` copy-on-write optimization — the privileged/local sinks keep the
 * full payload, only the general-access (OTLP) copy is minimized.
 */
export function stripLocalOnlyFields<V>(
  data: Record<string, V>,
  matcher: LocalOnlyFieldMatcher,
): Record<string, V> {
  const { names, prefix } = matcher;
  let out: Record<string, V> | undefined;
  for (const key of Object.keys(data)) {
    if ((names && names.has(key)) || (prefix && prefix.length > 0 && key.startsWith(prefix))) {
      if (out === undefined) out = { ...data };
      delete out[key];
    }
  }
  return out ?? data;
}

/**
 * Resolve the local-only field matcher from explicit opts merged with the env knobs
 * `AMI_ANALYTICS_LOCAL_ONLY_FIELDS` (comma list of field names) and
 * `AMI_ANALYTICS_LOCAL_ONLY_PREFIX` (a key prefix). Returns null when nothing is
 * configured (so the common differential-free path stays a cheap null check).
 */
export function resolveLocalOnlyFields(
  env: NodeJS.ProcessEnv = process.env,
  explicit: { localOnlyFields?: readonly string[]; localOnlyFieldPrefix?: string } = {},
): LocalOnlyFieldMatcher | null {
  const names = new Set<string>();
  for (const f of explicit.localOnlyFields ?? []) { if (f) names.add(f); }
  for (const tok of (env.AMI_ANALYTICS_LOCAL_ONLY_FIELDS || '').split(',')) {
    const t = tok.trim();
    if (t) names.add(t);
  }
  const prefix = explicit.localOnlyFieldPrefix ?? env.AMI_ANALYTICS_LOCAL_ONLY_PREFIX ?? undefined;
  if (names.size === 0 && !prefix) return null;
  return { ...(names.size > 0 ? { names } : {}), ...(prefix ? { prefix } : {}) };
}

// ---------------------------------------------------------------------------
// Per-sink killswitch (the reference sinkKillswitch.ts parity): disable an individual
// analytics sink (in-memory / on-disk JSONL / off-machine OTLP) at runtime without
// tearing the tracker down. Fail-OPEN: an unset/malformed entry leaves the sink ON
// and only a strict `=== true` disables it.
// ---------------------------------------------------------------------------

/** The three analytics sinks that can be individually killed. */
export const SINK_NAMES = ['memory', 'file', 'otlp'] as const;

export type SinkName = (typeof SINK_NAMES)[number];

/**
 * Resolve the set of killed sinks from the environment, merged over an explicit map
 * (explicit wins). `AMI_NO_ANALYTICS` (truthy) kills ALL sinks. `SUPERINFERENCE_DISABLE_SINKS`
 * is a comma list of sink names (`memory,file,otlp`); unknown tokens are ignored.
 * Fail-open: returns a map containing only the sinks set strictly to true.
 */
export function resolveKilledSinks(
  env: NodeJS.ProcessEnv = process.env,
  explicit: Partial<Record<SinkName, boolean>> = {},
): Partial<Record<SinkName, boolean>> {
  const out: Partial<Record<SinkName, boolean>> = {};
  if (isEnvTruthy(env.AMI_NO_ANALYTICS)) {
    for (const s of SINK_NAMES) out[s] = true;
  }
  for (const tok of (env.SUPERINFERENCE_DISABLE_SINKS || '').split(',')) {
    const name = tok.trim();
    if ((SINK_NAMES as readonly string[]).includes(name)) out[name as SinkName] = true;
  }
  for (const s of SINK_NAMES) {
    if (explicit[s] === true) out[s] = true;
  }
  return out;
}
