import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as crypto from 'crypto';
import { execSync } from 'child_process';
import { createRequire } from 'module';
import { AsyncLocalStorage } from 'async_hooks';
import { detectWslVersion } from './tools/bash-sandbox';
import { AMI_CLIENT_NAME, AMI_VERSION } from './version';
import { redactCredentials, redactPaths } from './credential-guard';
import { ndjsonSafeStringify } from './reference-journal';
import { createBufferedWriter, type BufferedWriter } from './buffered-writer';
import type { SpanNode } from './profiler';
import {
  isEnvTruthy,
  getPrivacyLevel,
  isTelemetryDisabled,
  isEssentialTrafficOnly,
  getEssentialTrafficOnlyReason,
  getPrivacyPosture,
  isNonessentialFeatureAllowedOnMiss,
  PRIVACY_LEVELS,
  ESSENTIAL_TRAFFIC_VARS,
  ESSENTIAL_TRAFFIC_DENY_ON_MISS,
  type PrivacyLevel,
  type PrivacyPosture,
} from './privacy-level';

// Re-exported so the analytics subsystem keeps a stable public surface; the
// canonical definitions live in ./privacy-level (single source of truth).
export {
  isEnvTruthy,
  getPrivacyLevel,
  isTelemetryDisabled,
  isEssentialTrafficOnly,
  getEssentialTrafficOnlyReason,
  getPrivacyPosture,
  isNonessentialFeatureAllowedOnMiss,
  PRIVACY_LEVELS,
  ESSENTIAL_TRAFFIC_VARS,
  ESSENTIAL_TRAFFIC_DENY_ON_MISS,
};
export type { PrivacyLevel, PrivacyPosture };

export interface AnalyticsEvent {
  timestamp: string;
  type: string;
  sessionId?: string;
  data: Record<string, unknown>;
  /** Monotonic per-session ordering counter, assigned on record. */
  sequence?: number;
}

const MAX_IN_MEMORY_EVENTS = 1000;

/**
 * Default in-memory event TTL (1h) applied when age-eviction is enabled via a truthy
 * non-numeric `AMI_ANALYTICS_EVENT_TTL_MS`. Mirrors Claude's SPAN_TTL_MS orphan sweep:
 * bounds retained events by AGE, complementing the {@link MAX_IN_MEMORY_EVENTS} count cap.
 */
export const DEFAULT_IN_MEMORY_EVENT_TTL_MS = 60 * 60 * 1000;

/**
 * Resolve the in-memory event TTL (ms) from an explicit opt or the
 * `AMI_ANALYTICS_EVENT_TTL_MS` env var. A positive number (opt or env) is that TTL;
 * a truthy non-numeric env value selects {@link DEFAULT_IN_MEMORY_EVENT_TTL_MS};
 * anything else (unset, <=0, falsey) returns undefined (age sweep disabled).
 */
export function resolveInMemoryEventTtlMs(env: NodeJS.ProcessEnv, opt?: number): number | undefined {
  if (opt !== undefined) return opt > 0 ? opt : undefined;
  const raw = env.AMI_ANALYTICS_EVENT_TTL_MS;
  if (raw === undefined) return undefined;
  const n = Number(raw);
  if (Number.isFinite(n)) return n > 0 ? n : undefined;
  return isEnvTruthy(raw) ? DEFAULT_IN_MEMORY_EVENT_TTL_MS : undefined;
}

// ---------------------------------------------------------------------------
// Redaction / data-minimization for analytics event payloads
// ---------------------------------------------------------------------------

/** String values longer than this are truncated (with a marker) before egress. */
export const MAX_ANALYTICS_STRING_LEN = 2048;
/** Bounded recursion depth when sanitizing nested tool-input-like values. */
const MAX_SANITIZE_DEPTH = 6;

const TOOL_NAME_KEYS = new Set(['tool', 'toolname', 'tool_name', 'name']);
/**
 * Keys whose VALUE is a raw filesystem path — replaced on egress with a truncated
 * SHA-256 so a self-hosted collector can still correlate/dedup by file (same path →
 * same hash) without the path itself (which can embed project names, usernames, PII)
 * ever leaving the machine. Mirrors Claude's fileOperationAnalytics hashFilePath.
 */
const PATH_KEYS = new Set(['file_path', 'filepath', 'path', 'filename', 'file', 'cwd', 'dir', 'directory']);
/** Keys carrying raw user/model prose (prompts, messages) — redacted unless user-prompt logging is opted in. */
const USER_PROMPT_KEYS = new Set(['prompt', 'user_prompt', 'message', 'system_prompt', 'text']);
/**
 * Keys carrying raw tool input/output CONTENT — redacted to length-only unless
 * tool-content logging is opted in (OTEL_LOG_TOOL_CONTENT) OR user-prompt logging /
 * beta detailed tracing is on. Kept distinct from {@link USER_PROMPT_KEYS} so an
 * operator can export tool IO content for debugging without also exporting prompts.
 */
const TOOL_CONTENT_KEYS = new Set(['content', 'output', 'input', 'result']);
/** Keys carrying structured tool INPUT arguments — bounded-serialized (not length-only) when tool details are on. */
const TOOL_INPUT_KEYS = new Set(['tool_input', 'arguments']);

function toolDetailsEnabled(env: NodeJS.ProcessEnv): boolean {
  return isEnvTruthy(env.AMI_LOG_TOOL_DETAILS) || isEnvTruthy(env.OTEL_LOG_TOOL_DETAILS);
}

/**
 * Whether raw tool input/output CONTENT may be exported — an explicit opt-in via
 * `OTEL_LOG_TOOL_CONTENT` (mirrors Claude's isToolContentLoggingEnabled). Distinct
 * from tool-DETAILS (which only unlocks the structured tool-input serializer) and
 * from user-prompt logging (which unlocks prompts/messages).
 */
export function isToolContentLoggingEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env.OTEL_LOG_TOOL_CONTENT);
}

// Tunable bounds for telemetry tool-input serialization (mirror Claude's metadata.ts).
/** Strings longer than this (chars) are clipped in bounded tool-input serialization. */
export const TOOL_INPUT_STRING_TRUNCATE_AT = 512;
/** Clipped tool-input strings keep this many leading chars (+ a length marker). */
export const TOOL_INPUT_STRING_TRUNCATE_TO = 128;
/** Hard cap (chars) on the final serialized tool-input JSON. */
export const TOOL_INPUT_MAX_JSON_CHARS = 4 * 1024;
/** Max array entries / object keys kept per level (+ an overflow count marker). */
export const TOOL_INPUT_MAX_COLLECTION_ITEMS = 20;
/** Max nesting depth walked before collapsing to `<nested>`. */
export const TOOL_INPUT_MAX_DEPTH = 2;

/**
 * Bounds-safe recursive serializer for an arbitrary tool-input value before it
 * enters telemetry: long strings are clipped (keeping a `[N chars]` marker), arrays
 * and objects are capped at {@link TOOL_INPUT_MAX_COLLECTION_ITEMS} entries (with an
 * overflow marker), nesting is bounded at {@link TOOL_INPUT_MAX_DEPTH}, and internal
 * marker keys (`_*`) are stripped. Mirrors Claude's `truncateToolInputValue` —
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
 * Serialize a tool's input arguments to a bounded JSON string for telemetry,
 * returning undefined unless tool-detail logging is enabled (OTEL_LOG_TOOL_DETAILS /
 * AMI_LOG_TOOL_DETAILS). The value is first bounded-serialized via
 * {@link truncateToolInputValue} then hard-capped at {@link TOOL_INPUT_MAX_JSON_CHARS}.
 * Mirrors Claude's `extractToolInputForTelemetry`.
 */
export function extractToolInputForTelemetry(input: unknown, env: NodeJS.ProcessEnv = process.env): string | undefined {
  if (!toolDetailsEnabled(env)) return undefined;
  const truncated = truncateToolInputValue(input);
  let json = trySerialize(truncated) ?? 'null';
  if (json.length > TOOL_INPUT_MAX_JSON_CHARS) json = json.slice(0, TOOL_INPUT_MAX_JSON_CHARS) + '…[truncated]';
  return json;
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
 * extension and 'other' for an implausibly long extension (mirrors Claude's
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
const BASH_TOKEN_SPLIT = /[\s;&|<>()]+/;
/** Commands whose arguments are file paths (so their extensions are analytics-safe signal). */
const FILE_COMMANDS = new Set([
  'rm', 'mv', 'cp', 'touch', 'cat', 'grep', 'rg', 'sed', 'awk', 'head', 'tail',
  'less', 'more', 'nano', 'vim', 'vi', 'code', 'wc', 'diff', 'chmod', 'chown',
]);

/**
 * Extract the set of distinct file extensions referenced by file-oriented commands
 * in a bash string — file-TYPE usage telemetry without logging raw paths. Walks
 * tokens, and after a known file command collects extensions of subsequent
 * non-flag arguments until the next command boundary (mirrors Claude's
 * getFileExtensionsFromBashCommand).
 */
export function getFileExtensionsFromBashCommand(command: string): string[] {
  const exts = new Set<string>();
  if (!command) return [];
  let inFileCommand = false;
  for (const tok of command.split(BASH_TOKEN_SPLIT)) {
    if (!tok) continue;
    if (FILE_COMMANDS.has(tok)) { inFileCommand = true; continue; }
    if (tok.startsWith('-')) continue; // flag
    if (inFileCommand) {
      const ext = getFileExtensionForAnalytics(tok);
      if (ext) exts.add(ext);
    }
  }
  return [...exts].sort();
}

/**
 * Collapse a user-defined agent/subagent name to the constant 'user-defined' before
 * it enters analytics (custom names can embed project names, paths, or identifiers).
 * A name present in `builtins` is a fixed vocabulary and passes through unchanged.
 */
export function sanitizeAgentName(name: string, builtins: ReadonlySet<string>): string {
  if (!name) return name;
  return builtins.has(name) ? name : 'user-defined';
}

/** Length (hex chars) of a privacy-preserving file-path hash (matches Claude's 16). */
export const FILE_PATH_HASH_LEN = 16;
/**
 * Max content bytes hashed by {@link hashFileContent} — above this the content is
 * not hashed (returns undefined) to avoid memory blow-ups on huge/base64 blobs
 * (mirrors Claude's MAX_CONTENT_HASH_SIZE = 100KB).
 */
export const MAX_CONTENT_HASH_SIZE = 100 * 1024;

/**
 * Privacy-preserving, STABLE hash of a file path for analytics: a truncated
 * ({@link FILE_PATH_HASH_LEN}-char) SHA-256 so the same path always maps to the same
 * token (enabling per-file correlation/dedup downstream) while the raw path — which
 * can embed usernames, project names, or other PII — never leaves the machine.
 * Mirrors Claude's `hashFilePath`.
 */
export function hashFilePath(filePath: string): string {
  return crypto.createHash('sha256').update(filePath).digest('hex').slice(0, FILE_PATH_HASH_LEN);
}

/**
 * Full (64-char) SHA-256 of file CONTENT for change-detection/dedup analytics, or
 * undefined when the content exceeds {@link MAX_CONTENT_HASH_SIZE} (size-guarded to
 * avoid hashing huge/base64 blobs). Mirrors Claude's `hashFileContent`.
 */
export function hashFileContent(content: string): string | undefined {
  if (content.length > MAX_CONTENT_HASH_SIZE) return undefined;
  return crypto.createHash('sha256').update(content).digest('hex');
}

function redactString(s: string): string {
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
 * analytics redaction / data-minimization pass — AMI's counterpart of Claude's
 * `AnalyticsMetadata_I_VERIFIED_THIS_IS_NOT_CODE_OR_FILEPATHS` guard. The brand is
 * produced ONLY by {@link sanitizeAnalyticsData} (the single redactor) or the
 * explicit {@link markAnalyticsSafe} escape hatch, and the telemetry egress sink
 * accepts ONLY branded data — so raw, unverified metadata containing code or file
 * paths cannot reach the on-disk JSONL / OTLP export by construction (it is a type
 * error), complementing the runtime stripping the pass already performs.
 */
declare const ANALYTICS_SAFE: unique symbol;
export type AnalyticsSafeMetadata = Record<string, unknown> & { readonly [ANALYTICS_SAFE]: true };

/**
 * Brand a metadata record as analytics-safe WITHOUT running the redaction pass —
 * the escape hatch for data the caller has already verified contains no code, file
 * paths, or secrets (e.g. a fixed-vocabulary enum such as a slash-command source).
 * Mirrors Claude's `value as AnalyticsMetadata_I_VERIFIED_THIS_IS_NOT_CODE_OR_FILEPATHS`
 * cast at trusted call sites. Prefer {@link sanitizeAnalyticsData} whenever the
 * data derives from user or tool input.
 */
export function markAnalyticsSafe(data: Record<string, unknown>): AnalyticsSafeMetadata {
  return data as AnalyticsSafeMetadata;
}

/**
 * Data-minimization pass applied to every event payload before it is persisted
 * to disk or exported off-machine:
 *  - internal marker keys (`_*`) are dropped
 *  - tool-name fields are MCP-sanitized
 *  - user-prompt keys are redacted to `<REDACTED>` + a `<key>_length` unless
 *    OTEL_LOG_USER_PROMPTS / SUPERINFERENCE_LOG_USER_PROMPTS (or beta detailed tracing) opts in
 *  - tool input/output CONTENT keys are redacted the same way unless additionally
 *    OTEL_LOG_TOOL_CONTENT opts in
 *  - structured tool-INPUT keys are bounded-serialized when tool details are on, else
 *    redacted to length-only
 *  - all remaining string values are credential-redacted and length-capped
 */
export function sanitizeAnalyticsData(data: Record<string, unknown>, opts: SanitizeOpts = {}): AnalyticsSafeMetadata {
  const env = opts.env ?? process.env;
  const beta = resolveBetaTracingConfig(env);
  // Beta detailed tracing emits full content, so it unlocks prompts AND tool content.
  const logUserPrompts = opts.logUserPrompts
    ?? (isEnvTruthy(env.OTEL_LOG_USER_PROMPTS) || isEnvTruthy(env.SUPERINFERENCE_LOG_USER_PROMPTS) || beta.enabled);
  const logToolContent = opts.logToolContent ?? (logUserPrompts || isToolContentLoggingEnabled(env) || beta.enabled);
  const logToolDetails = opts.logToolDetails ?? toolDetailsEnabled(env);
  // Derive the low-cardinality `file_language` dimension for code-editing-tool events
  // from the RAW path BEFORE the path is hashed below — so segmentation by language is
  // possible without the path ever leaving the machine (Claude permissionLogging parity).
  data = deriveCodeEditLanguage(data);
  const out: Record<string, unknown> = {};
  const redactToLength = (key: string, value: unknown): void => {
    const len = typeof value === 'string' ? value.length : JSON.stringify(value ?? '').length;
    out[key] = '<REDACTED>';
    out[`${key}_length`] = len;
  };
  for (const [key, value] of Object.entries(data)) {
    if (key.startsWith('_')) continue;
    const lk = key.toLowerCase();
    if (TOOL_NAME_KEYS.has(lk) && typeof value === 'string') {
      out[key] = sanitizeToolNameForAnalytics(value, { logToolDetails });
      continue;
    }
    if (TOOL_INPUT_KEYS.has(lk)) {
      const serialized = extractToolInputForTelemetry(value, env);
      if (serialized !== undefined) out[key] = serialized;
      else redactToLength(key, value);
      continue;
    }
    // Raw path fields are replaced on egress with a stable truncated hash — the
    // path never leaves the machine but downstream correlation/dedup by file still
    // works (same path → same hash). Skipped when raw paths are explicitly opted in.
    if (PATH_KEYS.has(lk) && typeof value === 'string' && value !== '' && !logUserPrompts) {
      out[`${key}_hash`] = hashFilePath(value);
      continue;
    }
    if (USER_PROMPT_KEYS.has(lk) && !logUserPrompts) {
      redactToLength(key, value);
      continue;
    }
    if (TOOL_CONTENT_KEYS.has(lk) && !logToolContent) {
      redactToLength(key, value);
      continue;
    }
    out[key] = sanitizeAnalyticsValue(value, 0);
  }
  // Cardinality-reduction normalization (Claude datadog.ts parity), applied just
  // before egress: a raw `model` id gets a bounded `model_family` companion dimension
  // (AMI keeps the raw id for self-hosted fidelity — additive superset), and a `version`
  // field is dev-truncated to base+date (stripping per-build timestamp/sha noise).
  if (typeof out.model === 'string' && out.model) {
    out.model_family = normalizeModelForAnalytics(out.model);
  }
  if (typeof out.version === 'string') {
    out.version = truncateDevVersion(out.version);
  }
  return out as AnalyticsSafeMetadata;
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
 * entry or carries an out-of-range/non-finite value (mirrors Claude's
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
    const s = JSON.stringify(value);
    return typeof s === 'string' ? s : null;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Beta detailed content tracing toggle (Claude-Code parity)
// ---------------------------------------------------------------------------

/** Resolved detailed-content-tracing configuration. */
export interface BetaTracingConfig {
  /** True only when BOTH the opt-in flag is truthy AND a dedicated endpoint is set. */
  enabled: boolean;
  /** The dedicated tracing endpoint (BETA_TRACING_ENDPOINT), or null when unset. */
  endpoint: string | null;
}

/**
 * Resolve the detailed-content-tracing toggle: Claude gates full-content tracing on
 * an explicit boolean (`ENABLE_BETA_TRACING_DETAILED`, via isEnvTruthy) AND a
 * dedicated endpoint (`BETA_TRACING_ENDPOINT`) — detailed tracing is enabled only
 * when both are present, so a stray flag with no endpoint stays off (and vice-versa).
 * Mirrors Claude's `isBetaTracingEnabled` base gate.
 */
export function resolveBetaTracingConfig(env: NodeJS.ProcessEnv = process.env): BetaTracingConfig {
  const endpoint = env.BETA_TRACING_ENDPOINT?.trim() || null;
  const enabled = isEnvTruthy(env.ENABLE_BETA_TRACING_DETAILED) && endpoint !== null;
  return { enabled, endpoint };
}

// ---------------------------------------------------------------------------
// Egress allowlist (Claude-Code datadog.ts parity): a double gate limiting WHICH
// event names and WHICH data fields are allowed to leave the machine.
// ---------------------------------------------------------------------------

/** A double egress gate: an optional event-name allowlist and an optional field allowlist. */
export interface EgressAllowlist {
  /** When set, ONLY events whose TYPE is listed are exported (others are dropped). */
  events?: ReadonlySet<string> | readonly string[];
  /** When set, ONLY these data FIELDS survive on the exported copy (others removed). */
  fields?: ReadonlySet<string> | readonly string[];
}

function toSet(v: ReadonlySet<string> | readonly string[] | undefined): ReadonlySet<string> | null {
  if (v === undefined) return null;
  return v instanceof Set ? v : new Set(v);
}

/**
 * Apply an egress allowlist to one event just before it leaves the machine. Returns
 * null when the event TYPE is not on the event allowlist (drop it entirely); else
 * returns the data filtered to the FIELD allowlist (unlisted fields removed). An
 * unset axis means "no restriction on that axis". Mirrors Claude's datadog.ts
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
 * allocation), else a shallow clone with the matching keys removed. Mirrors Claude's
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
// Per-sink killswitch (Claude-Code sinkKillswitch.ts parity): disable an individual
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

// ---------------------------------------------------------------------------
// Hash-based content deduplication (Claude-Code betaSessionTracing.ts parity):
// large/repeated content (system prompts, tool schemas) is logged in full only the
// first time its SHA-256 is seen; thereafter only a compact stand-in is emitted.
// ---------------------------------------------------------------------------

/** Minimum string length (chars) before a field is eligible for content dedup. */
export const DEDUP_MIN_CONTENT_LEN = 1024;
/** How many leading chars of a de-duplicated value are kept as a preview. */
export const DEDUP_PREVIEW_LEN = 500;

/**
 * Session-scoped content-deduplication cache. For each long string field, the full
 * value is emitted the FIRST time its 12-char SHA-256 prefix is seen; every later
 * occurrence of the same content is collapsed to a `<key>` preview plus `<key>_hash`
 * and `<key>_length` — so large repeated bodies cost one full log per unique hash
 * per session, not one per event. Mirrors Claude's `seenHashes` dedup. Reset per
 * session via {@link reset}.
 */
export class TelemetryContentDedup {
  private _seen = new Set<string>();
  constructor(
    private readonly _minLen: number = DEDUP_MIN_CONTENT_LEN,
    private readonly _previewLen: number = DEDUP_PREVIEW_LEN,
  ) {}

  /** Forget all hashes (start of a new session). */
  reset(): void {
    this._seen.clear();
  }

  /** Number of distinct content hashes seen this session. */
  get seenCount(): number {
    return this._seen.size;
  }

  /**
   * De-duplicate every long string field of `data`. Short strings and non-strings
   * pass through untouched. Returns the original object when nothing changed (so a
   * caller can cheaply detect a no-op).
   */
  dedupe(data: Record<string, unknown>): Record<string, unknown> {
    let changed = false;
    const out: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(data)) {
      if (typeof value === 'string' && value.length >= this._minLen) {
        const hash = crypto.createHash('sha256').update(value).digest('hex').slice(0, 12);
        if (this._seen.has(hash)) {
          out[key] = value.slice(0, this._previewLen);
          out[`${key}_hash`] = hash;
          out[`${key}_length`] = value.length;
          changed = true;
          continue;
        }
        this._seen.add(hash);
      }
      out[key] = value;
    }
    return changed ? out : data;
  }
}

// ---------------------------------------------------------------------------
// otelHeadersHelper (Claude-Code auth.ts parity): OTLP auth headers produced
// dynamically by running a user-configured command, debounced-cached.
// ---------------------------------------------------------------------------

/** Default debounce window reused between otelHeadersHelper invocations (29 min). */
export const DEFAULT_OTEL_HEADERS_DEBOUNCE_MS = 29 * 60 * 1000;

/**
 * Resolve the otelHeadersHelper debounce window (ms) from
 * `AMI_OTEL_HEADERS_HELPER_DEBOUNCE_MS` (then the Claude-compatible
 * `CLAUDE_CODE_OTEL_HEADERS_HELPER_DEBOUNCE_MS`), defaulting to
 * {@link DEFAULT_OTEL_HEADERS_DEBOUNCE_MS}. A literal `0` is honored (always re-run);
 * a non-finite/negative value falls back to the default.
 */
export function resolveOtelHeadersDebounceMs(env: NodeJS.ProcessEnv = process.env): number {
  const raw = env.AMI_OTEL_HEADERS_HELPER_DEBOUNCE_MS ?? env.CLAUDE_CODE_OTEL_HEADERS_HELPER_DEBOUNCE_MS;
  const n = raw === undefined ? NaN : Number(raw);
  return Number.isFinite(n) && n >= 0 ? Math.floor(n) : DEFAULT_OTEL_HEADERS_DEBOUNCE_MS;
}

/** Injected dependencies for {@link OtelHeadersHelper} (command runner + clock + env). */
export interface OtelHeadersHelperDeps {
  /** Run the helper command and return its stdout (throws on non-zero exit / spawn failure). */
  exec?: (command: string) => string;
  env?: NodeJS.ProcessEnv;
  now?: () => number;
}

/**
 * Validate that `parsed` is a JSON object of string→string header pairs (throwing
 * the same actionable errors Claude's getOtelHeadersFromHelper does). Exported so a
 * direct caller can reuse the validation without the caching wrapper.
 */
export function validateOtelHeaders(parsed: unknown): Record<string, string> {
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    throw new Error('otelHeadersHelper must return a JSON object with string key-value pairs');
  }
  for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
    if (typeof value !== 'string') {
      throw new Error(`otelHeadersHelper returned non-string value for key "${key}": ${typeof value}`);
    }
  }
  return parsed as Record<string, string>;
}

/**
 * Resolve OTLP auth headers from a user-configured helper command whose stdout is a
 * JSON object of string→string header pairs (enterprise OTLP backends often need a
 * freshly-minted bearer token). The parsed result is cached and reused until the
 * debounce window ({@link resolveOtelHeadersDebounceMs}, default 29 min) elapses, so
 * the (potentially expensive) auth call is not made per export. Mirrors Claude's
 * `getOtelHeadersFromHelper`. The command runner is injected for testability; the
 * default runs it with a 30s timeout.
 */
export class OtelHeadersHelper {
  private _cache: Record<string, string> | null = null;
  private _ts = 0;
  private readonly _exec: (command: string) => string;
  private readonly _env: NodeJS.ProcessEnv;
  private readonly _now: () => number;

  constructor(private readonly _command: string, deps: OtelHeadersHelperDeps = {}) {
    this._exec = deps.exec ?? ((cmd) => execSync(cmd, { timeout: 30_000, encoding: 'utf8' }).toString());
    this._env = deps.env ?? process.env;
    this._now = deps.now ?? Date.now;
  }

  /**
   * Return the headers, running the helper command only when there is no fresh
   * cached result. Throws (propagating the command/validation error) when the helper
   * produces no output or invalid JSON — the caller decides whether to degrade.
   */
  get(): Record<string, string> {
    if (!this._command) return {};
    const now = this._now();
    const debounce = resolveOtelHeadersDebounceMs(this._env);
    if (this._cache && now - this._ts < debounce) return this._cache;
    const raw = this._exec(this._command)?.toString().trim();
    if (!raw) throw new Error('otelHeadersHelper did not return a valid value');
    const headers = validateOtelHeaders(JSON.parse(raw));
    this._cache = headers;
    this._ts = now;
    return headers;
  }
}

// ---------------------------------------------------------------------------
// Bounded graceful shutdown drain (Claude-Code HybridTransport.ts parity).
// ---------------------------------------------------------------------------

/** Default best-effort drain window (ms) given to queued telemetry on close. */
export const CLOSE_GRACE_MS = 3000;

/**
 * Await the first of `promise` (settled, success OR failure) or a `ms` timeout —
 * so a best-effort drain cannot block shutdown beyond `ms`. Mirrors Claude's
 * `Promise.race([flush(), timeout(CLOSE_GRACE_MS)])` graceful-close bound. The sleep
 * is injectable for deterministic tests.
 */
export async function raceWithTimeout(
  promise: Promise<unknown>,
  ms: number,
  sleepImpl: (ms: number) => Promise<void> = (m) => new Promise(r => setTimeout(r, m)),
): Promise<void> {
  await Promise.race([promise.then(() => undefined, () => undefined), sleepImpl(ms)]);
}

// ---------------------------------------------------------------------------
// OTEL exporter / protocol / cadence configuration knobs (Claude-Code parity)
// ---------------------------------------------------------------------------

/** OTLP wire protocols AMI recognizes (only `http/json` is actually emitted). */
export const OTLP_PROTOCOLS = ['http/json', 'http/protobuf', 'grpc'] as const;
export type OtlpProtocol = (typeof OTLP_PROTOCOLS)[number];

/**
 * Parse an `OTEL_*_EXPORTER` value into an ordered exporter-type list. Per the OTEL
 * spec the literal `none` means "no automatically configured exporter for this
 * signal", so `none` tokens are filtered out (a bare `none` yields `[]` → disabled).
 * Blanks are dropped. An unset/empty value yields `[]` so the caller can apply its
 * own default. Mirrors Claude's parseExporterTypes.
 */
export function parseExporterTypes(value: string | undefined): string[] {
  return (value || '')
    .trim()
    .split(',')
    .map(t => t.trim())
    .filter(t => t !== '' && t !== 'none');
}

/**
 * Resolve the OTLP wire protocol for one signal, honoring the per-signal
 * `OTEL_EXPORTER_OTLP_{SIGNAL}_PROTOCOL` knob then the generic
 * `OTEL_EXPORTER_OTLP_PROTOCOL`, defaulting to `http/json` when unset. Throws on an
 * unrecognized value (mirrors Claude's switch default). Case/whitespace-insensitive.
 */
export function resolveOtlpProtocol(
  env: NodeJS.ProcessEnv,
  signal: 'logs' | 'metrics' | 'traces',
): OtlpProtocol {
  const perSignal = env[`OTEL_EXPORTER_OTLP_${signal.toUpperCase()}_PROTOCOL`];
  const raw = (perSignal ?? env.OTEL_EXPORTER_OTLP_PROTOCOL ?? '').trim().toLowerCase();
  if (!raw) return 'http/json';
  if ((OTLP_PROTOCOLS as readonly string[]).includes(raw)) return raw as OtlpProtocol;
  throw new Error(`Unknown OTLP protocol: ${raw}`);
}

/**
 * Whether telemetry EGRESS (OTLP POST / console dump) must be suppressed because
 * this is a test run. Defaults to on under `NODE_ENV==='test'` (so a test harness
 * never ships real telemetry — mirrors Claude's logOTelEvent NODE_ENV guard); an
 * explicit `AMI_TELEMETRY_ALLOW_IN_TEST` opts back in. Local in-memory/disk records
 * are unaffected — only off-machine/stdout egress is gated.
 */
export function isTelemetrySuppressedForTest(env: NodeJS.ProcessEnv): boolean {
  if (env.NODE_ENV !== 'test') return false;
  return !isEnvTruthy(env.AMI_TELEMETRY_ALLOW_IN_TEST);
}

/** Per-signal export-cadence defaults (ms): logs/traces fast, metrics slow. */
export const DEFAULT_EXPORT_INTERVALS: Record<'logs' | 'metrics' | 'traces', number> = {
  logs: 5000,
  metrics: 60000,
  traces: 5000,
};

const EXPORT_INTERVAL_ENV_KEYS: Record<'logs' | 'metrics' | 'traces', string> = {
  logs: 'OTEL_LOGS_EXPORT_INTERVAL',
  metrics: 'OTEL_METRIC_EXPORT_INTERVAL',
  traces: 'OTEL_TRACES_EXPORT_INTERVAL',
};

/**
 * Resolve the export flush interval (ms) for a signal from its env knob, falling
 * back to {@link DEFAULT_EXPORT_INTERVALS} for an unset / non-positive / unparseable
 * value. Lets an operator tune export cadence per signal (Claude parity).
 */
export function resolveExportIntervalMs(
  signal: 'logs' | 'metrics' | 'traces',
  env: NodeJS.ProcessEnv,
): number {
  const raw = env[EXPORT_INTERVAL_ENV_KEYS[signal]];
  const n = raw === undefined ? NaN : Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : DEFAULT_EXPORT_INTERVALS[signal];
}

/** Default overall shutdown-flush budget (ms) for the whole OTLP drain. */
export const DEFAULT_FLUSH_TIMEOUT_MS = 5000;

/**
 * Resolve the overall flush/shutdown budget (ms) the OTLP drain may block for,
 * from `AMI_OTEL_FLUSH_TIMEOUT_MS`, defaulting to {@link DEFAULT_FLUSH_TIMEOUT_MS}.
 * A literal `0` is honored (meaning "no time to flush"); a non-finite/negative
 * value falls back to the default (mirrors Claude's CLAUDE_CODE_OTEL_FLUSH_TIMEOUT_MS).
 */
export function resolveFlushTimeoutMs(env: NodeJS.ProcessEnv): number {
  const raw = env.AMI_OTEL_FLUSH_TIMEOUT_MS;
  const n = raw === undefined ? NaN : Number(raw);
  return Number.isFinite(n) && n >= 0 ? Math.floor(n) : DEFAULT_FLUSH_TIMEOUT_MS;
}

/**
 * Default shutdown-drain budget (ms) the graceful close may block for. AMI keeps its
 * own deliberate {@link CLOSE_GRACE_MS} value as the default (additive superset — a
 * knob is ADDED, AMI's default is not regressed to Claude's 2000ms).
 */
export const DEFAULT_SHUTDOWN_TIMEOUT_MS = CLOSE_GRACE_MS;

/**
 * Resolve the graceful-shutdown drain budget (ms), honoring `AMI_OTEL_SHUTDOWN_TIMEOUT_MS`
 * then the Claude-compatible `CLAUDE_CODE_OTEL_SHUTDOWN_TIMEOUT_MS`, defaulting to
 * {@link DEFAULT_SHUTDOWN_TIMEOUT_MS}. This is SEPARATE from the per-flush budget
 * ({@link resolveFlushTimeoutMs}): shutdown bounds how long the FINAL close drain may
 * block on a slow collector, independent of the in-session flush bound (mirrors
 * Claude's distinct shutdown-vs-flush timeout knobs). A literal `0` is honored (do not
 * block on close); a non-finite/negative value falls back to the default.
 */
export function resolveShutdownTimeoutMs(env: NodeJS.ProcessEnv = process.env): number {
  const raw = env.AMI_OTEL_SHUTDOWN_TIMEOUT_MS ?? env.CLAUDE_CODE_OTEL_SHUTDOWN_TIMEOUT_MS;
  const n = raw === undefined ? NaN : Number(raw);
  return Number.isFinite(n) && n >= 0 ? Math.floor(n) : DEFAULT_SHUTDOWN_TIMEOUT_MS;
}

/**
 * Actionable, numbered remediation shown when the OTLP flush exceeds its budget —
 * so a user pointing AMI at an unreachable/slow collector gets a self-correcting
 * hint instead of silence (mirrors Claude's flush-timeout guidance; AGENTS.md
 * requires failures to surface a remedy).
 */
export function formatFlushTimeoutGuidance(timeoutMs: number): string {
  return [
    `Telemetry flush exceeded its ${timeoutMs}ms budget; remaining events were re-queued.`,
    'To resolve this, you can:',
    '  1. Increase the timeout by setting AMI_OTEL_FLUSH_TIMEOUT_MS to a larger value.',
    '  2. Check that your OTLP collector endpoint is reachable and healthy.',
    '  3. Disable telemetry by unsetting the OTLP endpoint (or OTEL_LOGS_EXPORTER=none).',
  ].join('\n');
}

// ---------------------------------------------------------------------------
// Telemetry environment fingerprint, stable install id, and low-cardinality
// derivations attached to OTLP resource / log attributes. All are
// provider-neutral and let a self-hosted collector segment and estimate usage
// without exporting raw high-cardinality identifiers.
// ---------------------------------------------------------------------------

const JETBRAINS_IDES = [
  'intellij', 'pycharm', 'webstorm', 'phpstorm', 'rubymine', 'clion',
  'goland', 'datagrip', 'rider', 'appcode', 'jetbrains', 'androidstudio',
];

/**
 * Resolve a provider-neutral terminal identifier from the environment (never the
 * raw terminal path — just a stable low-cardinality name), so a self-hosted
 * dashboard can segment usage by terminal. Returns null when nothing identifiable
 * is present (so the `terminal.type` attribute is attached only when resolved).
 * Mirrors Claude Code's detectTerminal fallbacks, env-driven for testability.
 */
export function detectTerminalType(
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
): string | null {
  if (env.CURSOR_TRACE_ID) return 'cursor';
  const askpass = env.VSCODE_GIT_ASKPASS_MAIN;
  if (askpass?.includes('cursor')) return 'cursor';
  if (askpass?.includes('windsurf')) return 'windsurf';
  const bundleId = env.__CFBundleIdentifier?.toLowerCase();
  if (bundleId?.includes('vscodium')) return 'codium';
  if (bundleId?.includes('windsurf')) return 'windsurf';
  if (bundleId) {
    for (const ide of JETBRAINS_IDES) {
      if (bundleId.includes(ide)) return ide;
    }
  }
  if (env.VisualStudioVersion) return 'visualstudio';
  if (env.TERMINAL_EMULATOR === 'JetBrains-JediTerm') return platform === 'darwin' ? 'pycharm' : 'pycharm';
  if (env.TERM === 'xterm-ghostty') return 'ghostty';
  if (env.TERM?.includes('kitty')) return 'kitty';
  if (env.TERM_PROGRAM) return env.TERM_PROGRAM;
  if (env.TMUX) return 'tmux';
  if (env.STY) return 'screen';
  if (env.KONSOLE_VERSION) return 'konsole';
  if (env.GNOME_TERMINAL_SERVICE) return 'gnome-terminal';
  if (env.XTERM_VERSION) return 'xterm';
  if (env.VTE_VERSION) return 'vte-based';
  if (env.ALACRITTY_LOG) return 'alacritty';
  if (env.WT_SESSION) return 'windows-terminal';
  if (env.WSL_DISTRO_NAME) return `wsl-${env.WSL_DISTRO_NAME}`;
  if (env.TERM) return env.TERM;
  return null;
}

/** Accepted values for the {@link getHostPlatformForAnalytics} override knob. */
export const HOST_PLATFORM_OVERRIDES = ['win32', 'darwin', 'linux'] as const;

/**
 * Host platform reported in telemetry. `AMI_HOST_PLATFORM` (win32|darwin|linux)
 * overrides the detected `process.platform` — essential for AMI's Docker-sandboxed
 * mode, where process.platform reports the container OS but the real host differs
 * (mirrors Claude's getHostPlatformForAnalytics / CLAUDE_CODE_HOST_PLATFORM knob).
 */
export function getHostPlatformForAnalytics(
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
): string {
  const override = env.AMI_HOST_PLATFORM;
  if (override === 'win32' || override === 'darwin' || override === 'linux') return override;
  return platform;
}

/**
 * Resolve the `wsl.version` telemetry dimension (`'1'` | `'2'`) when running under
 * the Windows Subsystem for Linux, else null (so the attribute is attached only on
 * WSL). Detection requires a WSL env marker (`WSL_DISTRO_NAME` / `WSL_INTEROP`) so
 * the dimension is deterministic off-WSL (never a false positive from a stray kernel
 * string), then classifies the generation via the shared {@link detectWslVersion}
 * kernel classifier (single source of truth, reused — no duplicate WSL logic). A
 * marker present with an untagged kernel assumes WSL2 (the modern default), matching
 * bash-sandbox's `currentWslVersion`. Mirrors Claude's instrumentation.ts WSL
 * resource special-case (`wsl.version`).
 */
export function detectWslForAnalytics(
  env: NodeJS.ProcessEnv = process.env,
  kernel: string = os.release(),
): string | null {
  if (!env.WSL_DISTRO_NAME && !env.WSL_INTEROP) return null;
  const v = detectWslVersion(kernel);
  if (v === 'wsl1') return '1';
  return '2'; // 'wsl2' or marker-present-but-untagged → modern WSL2 default
}

/**
 * Vendor-neutral environment fingerprint for telemetry: host platform (honoring
 * the {@link getHostPlatformForAnalytics} override), CPU arch, Node version, and —
 * only when running under WSL — the `wsl.version` generation. Attached to every OTLP
 * resource so a collector can segment by environment.
 */
export function getEnvironmentFingerprint(
  env: NodeJS.ProcessEnv = process.env,
  info: { platform?: NodeJS.Platform; arch?: string; nodeVersion?: string; kernel?: string } = {},
): Record<string, string> {
  const out: Record<string, string> = {
    'host.platform': getHostPlatformForAnalytics(env, info.platform ?? process.platform),
    'host.arch': info.arch ?? process.arch,
    'node.version': info.nodeVersion ?? process.version,
  };
  const wsl = detectWslForAnalytics(env, info.kernel);
  if (wsl) out['wsl.version'] = wsl;
  return out;
}

/** Point-in-time process resource metrics attached to analytics events (opt-in). */
export interface ProcessMetrics {
  uptime: number;
  rss: number;
  heapTotal: number;
  heapUsed: number;
  external: number;
  cpuUserUs: number;
  cpuSystemUs: number;
  /** CPU utilization since the previous sample (undefined on the first sample). */
  cpuPercent?: number;
}

/** Injectable process readers for {@link ProcessMetricsSampler} (deterministic tests). */
export interface ProcessMetricsDeps {
  memoryUsage?: () => NodeJS.MemoryUsage;
  cpuUsage?: () => NodeJS.CpuUsage;
  uptime?: () => number;
  now?: () => number;
}

/**
 * Samples process memory + CPU usage and derives a CPU% figure from the delta since
 * the PREVIOUS sample — the first sample has no `cpuPercent` (no baseline yet). State
 * is per-instance (not module-global) so independent trackers don't corrupt each
 * other's deltas. Best-effort: a reader that throws yields `undefined`. Mirrors
 * Claude's buildProcessMetrics (metadata.ts) CPU%-delta pattern.
 */
export class ProcessMetricsSampler {
  private _prevCpu: NodeJS.CpuUsage | null = null;
  private _prevWallMs: number | null = null;
  private readonly _mem: () => NodeJS.MemoryUsage;
  private readonly _cpu: () => NodeJS.CpuUsage;
  private readonly _uptime: () => number;
  private readonly _now: () => number;

  constructor(deps: ProcessMetricsDeps = {}) {
    this._mem = deps.memoryUsage ?? (() => process.memoryUsage());
    this._cpu = deps.cpuUsage ?? (() => process.cpuUsage());
    this._uptime = deps.uptime ?? (() => process.uptime());
    this._now = deps.now ?? Date.now;
  }

  sample(): ProcessMetrics | undefined {
    try {
      const mem = this._mem();
      const cpu = this._cpu();
      const now = this._now();
      let cpuPercent: number | undefined;
      if (this._prevCpu && this._prevWallMs !== null) {
        const wallDeltaMs = now - this._prevWallMs;
        if (wallDeltaMs > 0) {
          const deltaUs = (cpu.user - this._prevCpu.user) + (cpu.system - this._prevCpu.system);
          cpuPercent = (deltaUs / (wallDeltaMs * 1000)) * 100;
        }
      }
      this._prevCpu = cpu;
      this._prevWallMs = now;
      return {
        uptime: this._uptime(),
        rss: mem.rss,
        heapTotal: mem.heapTotal,
        heapUsed: mem.heapUsed,
        external: mem.external,
        cpuUserUs: cpu.user,
        cpuSystemUs: cpu.system,
        ...(cpuPercent !== undefined ? { cpuPercent } : {}),
      };
    } catch {
      return undefined;
    }
  }
}

const CI_CONTEXT_VARS: Record<string, string> = {
  'ci.actor': 'GITHUB_ACTOR',
  'ci.actor_id': 'GITHUB_ACTOR_ID',
  'ci.repository': 'GITHUB_REPOSITORY',
  'ci.repository_id': 'GITHUB_REPOSITORY_ID',
  'ci.repository_owner': 'GITHUB_REPOSITORY_OWNER',
  'ci.repository_owner_id': 'GITHUB_REPOSITORY_OWNER_ID',
};

/**
 * Capture CI context when running under GitHub Actions (gated on GITHUB_ACTIONS):
 * actor / repository / owner (+ their ids). Returns null outside CI so the
 * dimensions are attached only when meaningful. Mirrors Claude's
 * githubActionsMetadata capture; provider-neutral (no Anthropic coupling).
 */
export function getCiContext(env: NodeJS.ProcessEnv = process.env): Record<string, string> | null {
  if (!isEnvTruthy(env.GITHUB_ACTIONS)) return null;
  const out: Record<string, string> = { ci: 'true' };
  for (const [attr, envVar] of Object.entries(CI_CONTEXT_VARS)) {
    const v = env[envVar];
    if (v) out[attr] = v;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Local git identity (Claude-Code utils/user.ts getGitEmail parity): the
// git-configured `user.email` is a purely local, vendor-neutral identity signal
// (useful for per-user usage attribution and co-authorship checks). It is resolved
// by spawning `git config --get user.email` (reject-free: stdout trimmed, undefined
// on a non-zero exit / git absent / no email set) and memoized per cwd so repeated
// reads never re-spawn. It is NEVER auto-exported — an explicit opt-in attaches only
// a privacy-preserving HASH (see {@link AnalyticsTrackerOpts.includeGitUser}).
// ---------------------------------------------------------------------------

/** Per-cwd memo of the resolved git email (undefined is a cached "none"). */
const _gitEmailCache = new Map<string, string | undefined>();

/**
 * Resolve the git-configured `user.email` for `cwd` via `git config --get user.email`,
 * trimmed; undefined when git is absent, the dir is not a repo, or no email is set
 * (reject-free, like Claude's memoized getGitEmail). Memoized per cwd. The command
 * runner is injectable for deterministic tests; the default runs git with stderr
 * silenced so a non-repo dir produces no console noise.
 */
export function getGitEmail(
  cwd: string = process.cwd(),
  exec: (dir: string) => string = (dir) =>
    execSync('git config --get user.email', { cwd: dir, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).toString(),
): string | undefined {
  if (_gitEmailCache.has(cwd)) return _gitEmailCache.get(cwd);
  let email: string | undefined;
  try {
    const out = exec(cwd).trim();
    email = out || undefined;
  } catch {
    email = undefined; // git absent / not a repo / no user.email configured
  }
  _gitEmailCache.set(cwd, email);
  return email;
}

/** Filename holding the stable per-install telemetry id, co-located with the log dir. */
export const USER_ID_FILE = 'telemetry-user-id';

/**
 * Read (or lazily create + persist) a stable, locally-generated install id so a
 * self-hosted collector can correlate activity across sessions without AMI ever
 * sending any real identity. When `dir` is given the id is persisted to
 * `<dir>/telemetry-user-id` (0o600); when it is absent, or persistence fails, an
 * ephemeral per-process id is returned. Mirrors Claude's getOrCreateUserID.
 */
export function getOrCreateUserId(dir?: string): string {
  const genId = () => crypto.randomBytes(32).toString('hex');
  if (!dir) return genId();
  const file = path.join(dir, USER_ID_FILE);
  try {
    const existing = fs.readFileSync(file, 'utf8').trim();
    if (/^[0-9a-f]{64}$/.test(existing)) return existing;
  } catch { /* not created yet */ }
  const id = genId();
  try {
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    fs.writeFileSync(file, id + '\n', { mode: 0o600 });
  } catch { /* best effort — return the generated id even if it could not be saved */ }
  return id;
}

/** Fixed number of privacy-preserving user buckets (cardinality ceiling). */
export const NUM_USER_BUCKETS = 30;

/**
 * Map a user id into one of `numBuckets` buckets via SHA-256 so unique users can
 * be estimated by counting distinct buckets WITHOUT exporting the raw id — privacy
 * plus bounded cardinality (mirrors Claude's getUserBucket, NUM_USER_BUCKETS=30).
 */
export function getUserBucket(userId: string, numBuckets: number = NUM_USER_BUCKETS): number {
  const n = Number.isFinite(numBuckets) && numBuckets >= 1 ? Math.floor(numBuckets) : NUM_USER_BUCKETS;
  const hash = crypto.createHash('sha256').update(userId).digest('hex');
  return parseInt(hash.slice(0, 8), 16) % n;
}

/**
 * Derive a low-cardinality `Nxx` range from an HTTP status (100..599), else
 * undefined — so a numeric status is exported as a bounded dimension instead of
 * one series per code (mirrors Claude's http_status_range).
 */
export function bucketHttpStatus(status: number | string | undefined | null): string | undefined {
  if (status === undefined || status === null || status === '') return undefined;
  const n = typeof status === 'number' ? status : Number(status);
  if (!Number.isFinite(n) || n < 100 || n > 599) return undefined;
  return `${Math.floor(n / 100)}xx`;
}

/**
 * When event data carries a numeric HTTP `status`, rename it to `http_status` and
 * add a low-cardinality `http_status_range` (`Nxx`) before export — avoiding a
 * high-cardinality raw-status dimension. A non-HTTP `status` (e.g. 'ok') is left
 * untouched. Returns the original object when no transform applies.
 */
export function deriveHttpStatusFields(data: Record<string, unknown>): Record<string, unknown> {
  const range = bucketHttpStatus(data.status as number | string | undefined);
  if (range === undefined) return data;
  const { status, ...rest } = data;
  return { ...rest, http_status: String(status), http_status_range: range };
}

// ---------------------------------------------------------------------------
// Attribute cardinality-reduction normalization (Claude-Code datadog.ts parity):
// before an event leaves the machine, high-cardinality identifiers are collapsed
// to a bounded vocabulary so a self-hosted collector's tag/series count stays
// finite (mirrors datadog.ts L196-217 model→known-or-'other' + dev-version trunc).
// ---------------------------------------------------------------------------

/**
 * Low-cardinality model FAMILY vocabulary kept verbatim in analytics; any model id
 * not matching one of these collapses to 'other' (mirrors Claude's model→known-cost
 * -set-or-'other' reduction). Provider-neutral — no vendor coupling.
 */
export const KNOWN_ANALYTICS_MODEL_FAMILIES = [
  'opus', 'sonnet', 'haiku', 'gpt', 'o1', 'o3', 'o4',
  'gemini', 'grok', 'llama', 'mistral', 'deepseek', 'qwen',
] as const;

/**
 * Collapse a raw model id to its low-cardinality FAMILY (`opus`, `gpt`, …) or
 * 'other' when it matches no known family — a bounded telemetry dimension that
 * can't explode a collector's series count as new model ids ship. A trailing
 * Anthropic `[1m]` long-context marker is stripped first (Claude parity). Empty
 * input passes through unchanged. Match is longest-family-first so `gpt-4o` does
 * not shadow `o4`/`o1`/`o3` when those tokens appear.
 */
export function normalizeModelForAnalytics(model: string): string {
  if (!model) return model;
  const lc = model.toLowerCase().replace(/\[1m\]$/i, '');
  // Prefer the longest family token so multi-char families win over 2-char ones.
  const ordered = [...KNOWN_ANALYTICS_MODEL_FAMILIES].sort((a, b) => b.length - a.length);
  for (const fam of ordered) {
    if (lc.includes(fam)) return fam;
  }
  return 'other';
}

/** Matches a `-dev.YYYYMMDD.tHHMMSS.sha<hex>` suffix whose timestamp+sha is cardinality noise. */
const DEV_VERSION_RE = /^(\d+\.\d+\.\d+-dev\.\d{8})\.t\d+\.sha[a-f0-9]+$/i;

/**
 * Truncate a dev build version to `base-dev.YYYYMMDD`, dropping the per-build
 * timestamp + commit sha that would otherwise make every build a unique tag — e.g.
 * `2.0.53-dev.20251124.t173302.sha526cc6a` → `2.0.53-dev.20251124`. A release
 * version (or any string not matching the dev pattern) is returned unchanged.
 * Mirrors Claude's datadog.ts version-truncation cardinality reduction.
 */
export function truncateDevVersion(version: string): string {
  return version.replace(DEV_VERSION_RE, '$1');
}

// ---------------------------------------------------------------------------
// Code-editing-tool language segmentation (Claude-Code permissionLogging.ts parity):
// for code-editing tools (Edit/Write/NotebookEdit → AMI file_edit/notebook_edit) the
// target file's programming LANGUAGE is derived from its extension and attached as a
// low-cardinality dimension, so edit telemetry can be segmented by language without
// ever exporting the raw path. Mirrors buildCodeEditToolAttributes (L41-65).
// ---------------------------------------------------------------------------

/** Tool names treated as code-editing (AMI canonical + Claude-Code aliases). */
export const CODE_EDITING_TOOLS: ReadonlySet<string> = new Set([
  'file_edit', 'notebook_edit',          // AMI canonical
  'Edit', 'Write', 'MultiEdit', 'NotebookEdit', // Claude-Code names
]);

/** Whether a tool name is a code-editing tool (edit/write/notebook family). */
export function isCodeEditingTool(toolName: string): boolean {
  return CODE_EDITING_TOOLS.has(toolName);
}

/**
 * Static file-extension → programming-language map for analytics segmentation.
 * Bounded + provider-neutral (no highlight.js dependency); an unmapped/absent
 * extension yields 'unknown' (matching Claude's getLanguageName fallback), keeping
 * the `file_language` dimension's cardinality finite.
 */
const EXTENSION_LANGUAGE: Record<string, string> = {
  ts: 'TypeScript', tsx: 'TypeScript', mts: 'TypeScript', cts: 'TypeScript',
  js: 'JavaScript', jsx: 'JavaScript', mjs: 'JavaScript', cjs: 'JavaScript',
  py: 'Python', rb: 'Ruby', go: 'Go', rs: 'Rust', java: 'Java',
  kt: 'Kotlin', kts: 'Kotlin', swift: 'Swift', scala: 'Scala',
  c: 'C', h: 'C', cc: 'C++', cpp: 'C++', cxx: 'C++', hpp: 'C++', hh: 'C++',
  cs: 'C#', php: 'PHP', pl: 'Perl', pm: 'Perl', lua: 'Lua', r: 'R',
  sh: 'Shell', bash: 'Shell', zsh: 'Shell', fish: 'Shell',
  html: 'HTML', htm: 'HTML', css: 'CSS', scss: 'SCSS', sass: 'Sass', less: 'Less',
  json: 'JSON', yaml: 'YAML', yml: 'YAML', toml: 'TOML', xml: 'XML',
  md: 'Markdown', markdown: 'Markdown', sql: 'SQL',
  dart: 'Dart', ex: 'Elixir', exs: 'Elixir', erl: 'Erlang', clj: 'Clojure',
  hs: 'Haskell', ml: 'OCaml', fs: 'F#', vue: 'Vue', svelte: 'Svelte',
  ipynb: 'Jupyter',
};

/**
 * Resolve the programming language of a file for analytics from its extension,
 * returning 'unknown' when there is no (mapped) extension — a bounded dimension that
 * never leaks the raw path. Reuses {@link getFileExtensionForAnalytics} so the same
 * extension-parsing rules apply. Mirrors Claude's getLanguageName (telemetry use).
 */
export function getLanguageForAnalytics(filePath: string): string {
  const ext = getFileExtensionForAnalytics(filePath);
  if (!ext) return 'unknown';
  return EXTENSION_LANGUAGE[ext] ?? 'unknown';
}

/**
 * When an event records a code-editing tool (via a tool-name field) AND a target
 * file path (via a path field), attach a low-cardinality `file_language` dimension
 * derived from the path's extension — WITHOUT consuming the raw path (which is still
 * hashed downstream). Returns the original object when no transform applies (not a
 * code-edit tool, no path, or `file_language` already present). Mirrors Claude's
 * buildCodeEditToolAttributes language enrichment.
 */
export function deriveCodeEditLanguage(data: Record<string, unknown>): Record<string, unknown> {
  if ('file_language' in data) return data;
  let toolName: string | undefined;
  let pathValue: string | undefined;
  for (const [key, value] of Object.entries(data)) {
    const lk = key.toLowerCase();
    if (toolName === undefined && TOOL_NAME_KEYS.has(lk) && typeof value === 'string') toolName = value;
    else if (pathValue === undefined && PATH_KEYS.has(lk) && typeof value === 'string' && value !== '') pathValue = value;
  }
  if (toolName === undefined || pathValue === undefined || !isCodeEditingTool(toolName)) return data;
  return { ...data, file_language: getLanguageForAnalytics(pathValue) };
}

// ---------------------------------------------------------------------------
// Invocation-source vocabulary (Claude-Code workloadContext.ts parity): a fixed,
// low-cardinality set of source tags so the usage view / analytics can segment a
// turn by what triggered it. AMI already has interactive/detached + a per-turn
// source field; the `cron` value lets a scheduler-fired turn be distinguished
// from an interactive/detached one (Claude's WORKLOAD_CRON).
// ---------------------------------------------------------------------------

/** Invocation-source tag for a turn fired by the cron scheduler (Claude's WORKLOAD_CRON). */
export const ANALYTICS_SOURCE_CRON = 'cron';

// ---------------------------------------------------------------------------
// Structured file-operation analytics event (Claude-Code fileOperationAnalytics.ts
// parity): a typed read/write/edit record carrying an operation + tool enum, a
// privacy-preserving truncated path hash, and (size-guarded) a full content hash,
// so file activity becomes a queryable structured analytics record without the raw
// path or content ever leaving the machine. Reuses the existing {@link hashFilePath}
// / {@link hashFileContent} (+ {@link MAX_CONTENT_HASH_SIZE} guard) — no duplicate hashing.
// ---------------------------------------------------------------------------

/** The file operation being recorded. */
export type FileOperationKind = 'read' | 'write' | 'edit';
/** AMI canonical file-tool names (vs Claude's FileReadTool/FileWriteTool/FileEditTool). */
export type FileOperationTool = 'file_read' | 'file_write' | 'file_edit';

/** Inputs for a structured file-operation analytics event. */
export interface FileOperationParams {
  operation: FileOperationKind;
  tool: FileOperationTool;
  /** Raw path — hashed (never logged verbatim). */
  filePath: string;
  /** File content — hashed only when present AND within {@link MAX_CONTENT_HASH_SIZE}. */
  content?: string;
  /** Distinguishes a newly-created file from an updated one (write/edit only). */
  type?: 'create' | 'update';
}

/**
 * Build the privacy-preserving metadata for a `file_operation` analytics event:
 * the operation + tool enums, a truncated SHA-256 `file_path_hash`, an optional
 * full `content_hash` (only when content is present and not too large — the
 * {@link hashFileContent} size guard returns undefined above {@link MAX_CONTENT_HASH_SIZE},
 * preventing memory blow-ups on base64 blobs), and an optional create/update `type`.
 * Mirrors Claude's logFileOperation metadata shape.
 */
export function buildFileOperationEvent(params: FileOperationParams): Record<string, unknown> {
  const md: Record<string, unknown> = {
    operation: params.operation,
    tool: params.tool,
    file_path_hash: hashFilePath(params.filePath),
  };
  if (params.content !== undefined) {
    const contentHash = hashFileContent(params.content); // undefined when over the size guard
    if (contentHash !== undefined) md.content_hash = contentHash;
  }
  if (params.type !== undefined) md.type = params.type;
  return md;
}

// ---------------------------------------------------------------------------
// Local git commit/PR operation classification (Claude-Code gitOperationTracking.ts
// parity): detect `git commit` (+ --amend), `git push`, `gh pr <action>`,
// `glab mr create`, and curl-based PR creation in a raw command string (succeeded
// only, exitCode===0) so the usage view can count local commit/PR activity. Pure
// text matching on the raw argv — identical for any shell.
// ---------------------------------------------------------------------------

/** The set of git/PR operations a single command can be classified as. */
export type GitOperationName =
  | 'commit' | 'commit_amend' | 'push'
  | 'pr_create' | 'pr_edit' | 'pr_merge' | 'pr_comment' | 'pr_close' | 'pr_ready';

/** Result of classifying one (successful) command for git/PR usage counters. */
export interface GitOperationClassification {
  /** All distinct operations matched (in a stable order). */
  operations: GitOperationName[];
  /** Amount to add to the local commit counter (0 or 1). */
  commitDelta: number;
  /** Amount to add to the local PR counter (one per pr-create source matched). */
  prDelta: number;
}

/**
 * Build a regex matching `git <subcmd>` while tolerating git's global options
 * between `git` and the subcommand (e.g. `git -c commit.gpgsign=false commit`,
 * `git -C path push`, `git --git-dir=path commit`). Mirrors Claude's gitCmdRe.
 */
function gitSubcmdRe(subcmd: string, suffix = ''): RegExp {
  return new RegExp(`\\bgit(?:\\s+-[cC]\\s+\\S+|\\s+--\\S+=\\S+)*\\s+${subcmd}\\b${suffix}`);
}

const GIT_COMMIT_RE = gitSubcmdRe('commit');
const GIT_PUSH_RE = gitSubcmdRe('push');
const GH_PR_ACTIONS: readonly { re: RegExp; op: GitOperationName; isCreate: boolean }[] = [
  { re: /\bgh\s+pr\s+create\b/, op: 'pr_create', isCreate: true },
  { re: /\bgh\s+pr\s+edit\b/, op: 'pr_edit', isCreate: false },
  { re: /\bgh\s+pr\s+merge\b/, op: 'pr_merge', isCreate: false },
  { re: /\bgh\s+pr\s+comment\b/, op: 'pr_comment', isCreate: false },
  { re: /\bgh\s+pr\s+close\b/, op: 'pr_close', isCreate: false },
  { re: /\bgh\s+pr\s+ready\b/, op: 'pr_ready', isCreate: false },
];
/** curl with an explicit or implicit (`-d`) POST — a PR-create candidate. */
const CURL_POST_RE = /\bcurl\b/;
const CURL_EXPLICIT_POST_RE = /-X\s*POST\b|--request\s*=?\s*POST\b|\s-d\s/i;
/** A PR-create REST endpoint, but NOT a sub-resource like /pulls/123/comments. */
const PR_ENDPOINT_RE = /https?:\/\/[^\s'"]*\/(pulls|pull-requests|merge[-_]requests)(?!\/\d)/i;

/**
 * Classify a (successful) command string into local commit/PR operations + the
 * counter deltas that feed the usage view. A non-zero `exitCode` yields an empty
 * classification (only successful operations are counted). `git commit` counts a
 * commit (and additionally emits `commit_amend` for `--amend`); `git push` emits
 * `push`; each `gh pr <action>` / `glab mr create` / curl-POST-to-a-PR-endpoint
 * emits the relevant op and (for creates) increments the PR counter. Mirrors
 * Claude's trackGitOperations detection, including the curl PR-create branch.
 */
export function classifyGitOperation(command: string, exitCode: number): GitOperationClassification {
  const empty: GitOperationClassification = { operations: [], commitDelta: 0, prDelta: 0 };
  if (exitCode !== 0 || !command) return empty;
  const operations: GitOperationName[] = [];
  let commitDelta = 0;
  let prDelta = 0;
  if (GIT_COMMIT_RE.test(command)) {
    operations.push('commit');
    commitDelta = 1;
    if (/--amend\b/.test(command)) operations.push('commit_amend');
  }
  if (GIT_PUSH_RE.test(command)) operations.push('push');
  for (const { re, op, isCreate } of GH_PR_ACTIONS) {
    if (re.test(command)) {
      operations.push(op);
      if (isCreate) prDelta++;
    }
  }
  if (/\bglab\s+mr\s+create\b/.test(command)) {
    operations.push('pr_create');
    prDelta++;
  }
  if (CURL_POST_RE.test(command) && CURL_EXPLICIT_POST_RE.test(command) && PR_ENDPOINT_RE.test(command)) {
    operations.push('pr_create');
    prDelta++;
  }
  if (operations.length === 0) return empty;
  return { operations, commitDelta, prDelta };
}

// ---------------------------------------------------------------------------
// Per-agent incremental context delta (Claude-Code betaSessionTracing.ts parity):
// track the hash of the last-reported message per invocation source (main thread /
// subagent / warmup have independent conversations) so a trace/record can carry only
// the NEW user/tool-result messages added since the previous request, instead of
// re-dumping the whole (huge) conversation each turn.
// ---------------------------------------------------------------------------

/** The new-message delta for one source since its previous report. */
export interface IncrementalContextDelta<T> {
  /** Only the NEW user messages added since the last report (assistant msgs filtered out). */
  newMessages: T[];
  /** `newMessages.length` — the `new_context_message_count` dimension. */
  count: number;
  /** Index in the full array at which the new slice began (0 when the prior hash was not found). */
  startIndex: number;
}

/**
 * Tracks, per invocation source (querySource), the hash of the last message
 * reported so each call returns ONLY the user messages added since the previous
 * one. Keyed per source because the main thread, subagents, and warmup each own an
 * independent conversation. When the previously-reported hash is no longer present
 * in the array (compaction/reset), it falls back to the whole array (startIndex 0).
 * The remembered hash advances to the LAST message only when a non-empty delta is
 * produced (matching Claude's lastReportedMessageHash update being gated on
 * newMessages.length > 0). Mirrors Claude's betaSessionTracing new_context tracking.
 */
export class IncrementalContextTracker {
  private readonly _last = new Map<string, string>();

  /**
   * Compute the new-user-message delta for `source` over `messages`. `hashOf`
   * produces a stable per-message hash; `isUser` selects the messages that count as
   * new context (user input / tool results — assistant output is excluded).
   */
  delta<T>(
    source: string,
    messages: readonly T[],
    hashOf: (m: T) => string,
    isUser: (m: T) => boolean,
  ): IncrementalContextDelta<T> {
    const lastHash = this._last.get(source);
    let startIndex = 0;
    if (lastHash !== undefined) {
      for (let i = 0; i < messages.length; i++) {
        if (hashOf(messages[i]) === lastHash) {
          startIndex = i + 1; // start AFTER the last reported message
          break;
        }
      }
      // lastHash not found → startIndex stays 0 (report everything).
    }
    const newMessages = messages.slice(startIndex).filter(isUser);
    if (newMessages.length > 0) {
      // Advance the remembered hash to the final message of the WHOLE array so the
      // next delta starts after everything seen this turn (incl. assistant msgs).
      this._last.set(source, hashOf(messages[messages.length - 1]));
    }
    return { newMessages, count: newMessages.length, startIndex };
  }

  /** Forget the last-reported hash for one source (or all sources when omitted). */
  reset(source?: string): void {
    if (source === undefined) this._last.clear();
    else this._last.delete(source);
  }

  /** Number of sources currently being tracked (introspection/testability). */
  get trackedSourceCount(): number {
    return this._last.size;
  }
}

export interface AnalyticsTrackerOpts {
  otlpEndpoint?: string;
  otlpFetch?: typeof fetch;
  /** Overrides process.env for privacy/redaction resolution (testability). */
  env?: NodeJS.ProcessEnv;
  /** Keep events verbatim on egress (skip the redaction pass). Default: redact. */
  redact?: boolean;
  /** Per-event probabilistic keep rate (0..1). Default 1 (keep all). */
  sampleRate?: number;
  /**
   * Per-event-TYPE keep rate (0..1) overriding the tracker-wide `sampleRate` for
   * named high-volume events. When an event is kept under a rate < 1, its applied
   * rate is stamped into the event data as `sample_rate` so a downstream consumer
   * can re-weight (un-bias) counts.
   */
  sampleRates?: Record<string, number>;
  /** OTLP export retry knobs. */
  otlpMaxAttempts?: number;
  otlpBaseBackoffMs?: number;
  otlpMaxBackoffMs?: number;
  /** Per-POST request timeout (ms) for every OTLP export. Default 5000. */
  otlpTimeoutMs?: number;
  /**
   * Inter-batch pause (ms) inserted between consecutive batch POSTs during the
   * shutdown drain so a large chunked export does not hammer the collector.
   * Default 0 (back-to-back). Mirrors Claude's batchDelayMs.
   */
  otlpBatchDelayMs?: number;
  /**
   * Runtime killswitch probe consulted before every export. While it returns true
   * the sender produces zero network traffic (events stay queued) and resumes once
   * it clears — a sink can be disabled at runtime without tearing it down. Mirrors
   * Claude's injectable isKilled() probe.
   */
  isKilled?: () => boolean;
  /**
   * Directory for the stable per-install telemetry id (user.id / user.bucket
   * resource attributes). Defaults to the analytics log dir; when neither is set an
   * ephemeral per-process id is used.
   */
  userIdDir?: string;
  /** Telemetry batching knobs threaded into the lazily-created TelemetryBatchSender. */
  telemetryMaxBatchSize?: number;
  telemetryMaxBatchBytes?: number;
  /**
   * Cross-batch circuit breaker for the shutdown drain: after this many WHOLE
   * batches fail to export in a row (each already retried to exhaustion via
   * {@link otlpMaxAttempts}), {@link flushOtlp} stops draining and requeues the
   * remainder for the next flush — bounding wasted effort against a persistently
   * down collector. A single successful send resets the streak. Undefined (default)
   * keeps the stop-on-first-failure behaviour; when set, the drain ADVANCES past a
   * dropped batch (with a fresh per-batch budget) until the streak is reached.
   * Mirrors Claude's SerialBatchEventUploader `maxConsecutiveFailures`.
   */
  maxConsecutiveFailures?: number;
  /** Invoked when a fully-retried OTLP batch is permanently dropped (shutdown diagnostics). */
  onBatchDropped?: (eventCount: number, droppedBatchCount: number) => void;
  /**
   * Static headers attached to every OTLP POST (e.g. an Authorization/x-api-key for
   * a self-hosted collector). Merged over headers parsed from OTEL_EXPORTER_OTLP_HEADERS.
   */
  otlpHeaders?: Record<string, string>;
  /**
   * Extra OTLP resource attributes (e.g. deployment.environment, service.namespace)
   * stamped onto every exported payload. Merged over attributes parsed from
   * OTEL_RESOURCE_ATTRIBUTES, which an explicit entry here overrides.
   */
  otlpResourceAttrs?: Record<string, string>;
  /**
   * When true the configured {@link otlpEndpoint} is a signal-specific logs endpoint
   * (from `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`) and is POSTed verbatim — `/v1/logs` is
   * NOT appended (mirrors Claude's per-signal endpoint selection). Default false: the
   * endpoint is treated as generic and `/v1/logs` is appended. Resolve both from
   * {@link resolveOtlpLogsEndpoint}.
   */
  otlpLogsEndpointPerSignal?: boolean;
  /**
   * Attach a privacy-preserving HASH of the local git `user.email` as the
   * `user.git_email` resource attribute on every OTLP export, so a self-hosted
   * collector can attribute usage per developer WITHOUT the raw email ever leaving
   * the machine. Off by default; also enabled by the `AMI_OTEL_INCLUDE_GIT_USER` env
   * knob. The raw email is resolved via {@link getGitEmail} (git absent / no email →
   * the attribute is simply omitted). Mirrors Claude's getGitEmail identity signal.
   */
  includeGitUser?: boolean;
  /** Working directory whose git `user.email` is read for {@link includeGitUser}. Default process.cwd(). */
  gitUserCwd?: string;
  /** Injected git-email resolver for {@link includeGitUser} (deterministic tests). Default {@link getGitEmail}. */
  gitEmailResolver?: (cwd: string) => string | undefined;
  /**
   * Pre-resolved undici dispatcher (proxy / mTLS / custom-CA) attached to every OTLP
   * POST. When unset, one is auto-resolved from the OTLP proxy + TLS env vars (see
   * {@link resolveOtlpTransport}) against the configured endpoint. Mirrors Claude's
   * HttpsProxyAgent + httpAgentOptions transport. Injectable for tests.
   */
  otlpDispatcher?: unknown;
  /** undici loader injection for the auto-resolved transport (tests). */
  undiciLoader?: () => unknown;
  /** readFile injection for the auto-resolved mTLS PEM material (tests). */
  otlpTlsReadFile?: (p: string) => string;
  /**
   * Time-to-live (ms) for in-memory events: entries older than this are swept on the
   * next {@link AnalyticsTracker.log} and on every read ({@link AnalyticsTracker.getEvents}
   * etc.), bounding memory by AGE in addition to the {@link MAX_IN_MEMORY_EVENTS} count
   * cap. <=0/undefined disables the age sweep. Seeded from `AMI_ANALYTICS_EVENT_TTL_MS`
   * (a positive number of ms, or a truthy non-number → {@link DEFAULT_IN_MEMORY_EVENT_TTL_MS}).
   * Mirrors Claude's SPAN_TTL_MS orphan eviction.
   */
  inMemoryEventTtlMs?: number;
  /** Sleep + RNG injection for deterministic tests. */
  sleepImpl?: (ms: number) => Promise<void>;
  rand?: () => number;
  /** Monotonic clock injection (ms) for deterministic flush-budget tests. Default Date.now. */
  now?: () => number;
  /**
   * Diagnostic bridge for one-time telemetry warnings (no sink configured,
   * unsupported protocol, flush-timeout remediation). A consumer wires this to its
   * logger; unset → silent (best-effort, like {@link onBatchDropped}).
   */
  onDiagnostic?: (message: string) => void;
  /**
   * Error sink for genuine telemetry FAILURES (OTLP mirror exception, headers-helper
   * exception) — distinct from the informational {@link onDiagnostic} stream. Wired
   * through a level-filtered {@link createTelemetryDiagLogger}: failures surface here
   * as an Error AND on {@link onDiagnostic}, while routine info/debug chatter is
   * suppressed. A consumer wires this to its error logger; unset → silent.
   */
  onError?: (err: Error) => void;
  /**
   * Buffer disk-sink appends through a non-blocking {@link createBufferedWriter}
   * instead of a synchronous {@link fs.appendFileSync} per event: batched writes
   * drain on a size boundary (deferred via setImmediate) or this idle interval (ms),
   * so a slow disk append never stalls the render/keystroke tick. The buffer is
   * drained synchronously on {@link AnalyticsTracker.shutdown}/{@link AnalyticsTracker.closeWithGrace}
   * (exit-safe). <=0/undefined keeps the synchronous append. Seeded from the
   * `AMI_ANALYTICS_FILE_FLUSH_MS` env knob. Mirrors Claude's bufferedWriter disk sink.
   */
  fileFlushIntervalMs?: number;
  /**
   * Sink for the `console` exporter type (OTEL_LOGS_EXPORTER=console): invoked with a
   * one-line JSON dump of each event's redacted payload for local introspection with
   * no collector. Defaults to writing a line to process.stdout.
   */
  consoleSink?: (line: string) => void;
  /**
   * Egress event-name allowlist: when set, ONLY events of these types ever leave the
   * machine (disk JSONL / console / OTLP); others stay in-memory only. Mirrors
   * Claude's DATADOG_ALLOWED_EVENTS hard allowlist.
   */
  egressEventAllowlist?: readonly string[];
  /**
   * Egress field allowlist: when set, ONLY these data fields survive on the exported
   * copy of every event (others are stripped). Mirrors Claude's TAG_FIELDS gate.
   */
  egressFieldAllowlist?: readonly string[];
  /**
   * When true, large/repeated string content is de-duplicated per session before
   * egress: the full body is logged once per unique SHA-256, then collapsed to a
   * preview + hash + length. Mirrors Claude's seenHashes content dedup.
   */
  dedupeContent?: boolean;
  /**
   * A command whose JSON-object stdout supplies OTLP auth headers, merged over the
   * static headers on every export and debounced-cached (29 min). `exec`/`now` are
   * injectable for tests; the default runs the command with a 30s timeout. Mirrors
   * Claude's otelHeadersHelper setting.
   */
  otlpHeadersHelper?: { command: string; exec?: (command: string) => string; now?: () => number };
  /**
   * Per-sink killswitch: individually disable the in-memory, on-disk JSONL, or
   * off-machine OTLP sink. Merged over the env knobs (`AMI_NO_ANALYTICS` kills all;
   * `SUPERINFERENCE_DISABLE_SINKS=memory,file,otlp`). Fail-open; mutable at runtime via
   * {@link AnalyticsTracker.setKilledSinks}. Mirrors Claude's per-sink killswitch config.
   */
  killedSinks?: Partial<Record<SinkName, boolean>>;
  /**
   * Attach point-in-time process metrics (memory + CPU% delta) under a `process`
   * field on every logged event. Off by default (noise); also enabled by the
   * `AMI_OTEL_PROCESS_METRICS` env knob. Readers are injectable for tests. Mirrors
   * Claude's buildProcessMetrics attachment.
   */
  includeProcessMetrics?: boolean;
  processMetricsDeps?: ProcessMetricsDeps;
  /**
   * Persist OTLP batches that are permanently dropped (after in-process retries are
   * exhausted) to disk, and re-send them on a later flush/session — so a collector
   * outage spanning a whole session does not silently lose telemetry. Enabled by this
   * flag or the `AMI_OTEL_PERSIST_FAILED` env knob; spool dir defaults to
   * `<logDir>/telemetry-failed` (override with {@link failedBatchDir}). Mirrors
   * Claude's cross-run failed-batch retry.
   */
  persistFailedBatches?: boolean;
  /** Override directory for the durable failed-batch spool. */
  failedBatchDir?: string;
  /**
   * Bypass telemetry batching: when true, every logged event is flushed to the OTLP
   * send path immediately (one batch per event) instead of accumulating to the
   * size/byte/interval boundary — a low-latency/debugging pass-through. Off by default;
   * also enabled by the `AMI_OTEL_IMMEDIATE_FLUSH` env knob. Mirrors Claude's
   * bufferedWriter `immediateMode`.
   */
  telemetryImmediateMode?: boolean;
  /**
   * Field NAMES that are local-only: kept on the privileged on-machine sinks (in-memory
   * + JSONL + console) but stripped from the general-access off-machine OTLP export.
   * Merged with the `AMI_ANALYTICS_LOCAL_ONLY_FIELDS` env list. Mirrors Claude's
   * differential-by-sink redaction (full payload to the privileged 1P sink, stripped
   * copy to the general-access backend).
   */
  localOnlyFields?: readonly string[];
  /**
   * Key PREFIX marking local-only fields (stripped from the OTLP export only). Merged
   * with the `AMI_ANALYTICS_LOCAL_ONLY_PREFIX` env knob. Mirrors Claude's `_PROTO_*`
   * prefix strip.
   */
  localOnlyFieldPrefix?: string;
  /**
   * Point-in-time numeric metrics provider (typically the Profiler's getAll() split
   * into gauges/counters). When set, {@link AnalyticsTracker.flushOtlp} exports an OTLP
   * METRICS payload ({@link toOtlpMetricsPayload}) on every flush/shutdown — bridging
   * AMI's rich quantitative data to any OTLP metrics backend. Mirrors Claude's metrics
   * signal (PeriodicExportingMetricReader). The seam keeps the engine's profiler wiring
   * a single injected function (no engine import into core).
   */
  metricsSource?: () => MetricSnapshot;
  /**
   * Span-tree provider (typically the Profiler's getSpanTree()). When set,
   * {@link AnalyticsTracker.flushOtlp} exports an OTLP TRACES payload
   * ({@link toOtlpTracePayload}) on every flush/shutdown — surfacing the nested
   * interaction/llm/tool/hook timeline in any OTLP trace backend. Mirrors Claude's
   * sessionTracing span emission; injected to avoid a core→engine dependency.
   */
  traceSource?: () => readonly SpanNode[];
}

// ---------------------------------------------------------------------------
// Durable failed-batch spool (Claude-Code firstPartyEventLoggingExporter.ts parity):
// a telemetry batch that is permanently dropped after exhausting its in-process
// retries is written to disk, and a LATER process (next session's flush) re-reads
// and re-POSTs it, deleting the file on success — so a transient collector outage
// across a whole session does not silently lose telemetry.
// ---------------------------------------------------------------------------

/** Prefix of every spooled failed-batch file (distinguishes them in the dir). */
export const FAILED_BATCH_FILE_PREFIX = 'failed-batch-';
const FAILED_BATCH_FILE_RE = /^failed-batch-.*\.jsonl$/;

/**
 * Append-only, crash-safe disk spool for telemetry batches that could not be sent.
 * Each spooled batch is one file of NDJSON event lines, written via a temp file +
 * atomic rename (a reader never sees a half-written file). Files are created 0o600 in
 * a 0o700 dir. All ops are best-effort (never throw) — the spool is a backstop, not a
 * critical path. Mirrors Claude's per-batch disk persistence + cross-run retry.
 */
export class FailedBatchStore {
  constructor(private readonly _dir: string) {}

  /** Persist one batch's events to a new spool file. Returns the path, or null on failure. */
  spool(events: TelemetryBatch['events'], rand: () => number = Math.random): string | null {
    if (events.length === 0) return null;
    try {
      fs.mkdirSync(this._dir, { recursive: true, mode: 0o700 });
      const id = `${Date.now()}-${Math.floor(rand() * 1e9).toString(36)}`;
      const file = path.join(this._dir, `${FAILED_BATCH_FILE_PREFIX}${id}.jsonl`);
      const tmp = `${file}.tmp`;
      // Quarantine any un-serializable (poison) event rather than losing the file.
      const body = events.map(e => trySerialize(e)).filter((l): l is string => l !== null).join('\n');
      if (!body) return null;
      fs.writeFileSync(tmp, body + '\n', { mode: 0o600 });
      fs.renameSync(tmp, file);
      return file;
    } catch {
      return null;
    }
  }

  /** List absolute paths of all spooled failed-batch files (oldest first). Best-effort. */
  list(): string[] {
    try {
      return fs.readdirSync(this._dir)
        .filter(f => FAILED_BATCH_FILE_RE.test(f))
        .sort()
        .map(f => path.join(this._dir, f));
    } catch {
      return [];
    }
  }

  /** Load one spool file back into an event array (best-effort; [] on any failure). */
  load(file: string): TelemetryBatch['events'] {
    try {
      const out: TelemetryBatch['events'] = [];
      for (const line of fs.readFileSync(file, 'utf8').split('\n')) {
        if (!line.trim()) continue;
        try { out.push(JSON.parse(line)); } catch { /* skip poison line */ }
      }
      return out;
    } catch {
      return [];
    }
  }

  /** Remove a spool file (after a successful re-send). Best-effort. */
  delete(file: string): void {
    try { fs.unlinkSync(file); } catch { /* already gone */ }
  }
}

/**
 * Resolve the disk-sink buffered-flush interval (ms) from an explicit opt or the
 * `AMI_ANALYTICS_FILE_FLUSH_MS` env var. A positive number (opt or env) enables the
 * non-blocking buffered disk writer at that idle cadence; anything else (unset, <=0,
 * non-numeric) returns 0 → the synchronous per-event append is kept.
 */
export function resolveFileFlushIntervalMs(env: NodeJS.ProcessEnv, opt?: number): number {
  if (opt !== undefined) return opt > 0 ? opt : 0;
  const raw = env.AMI_ANALYTICS_FILE_FLUSH_MS;
  if (raw === undefined) return 0;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

// ---------------------------------------------------------------------------
// Level-filtered telemetry diagnostic boundary (Claude-Code
// utils/telemetry/logger.ts ClaudeCodeDiagLogger parity): an error-only bridge
// for the telemetry subsystem's own internal diagnostics. error()/warn() surface
// the message as BOTH a real Error (onError) and a debug line (onDebug), while
// info()/debug()/verbose() are explicit no-ops so routine SDK chatter never
// becomes noise. This gives the telemetry pipeline a single, severity-aware
// place to decide what reaches the user vs. what is suppressed.
// ---------------------------------------------------------------------------

/** Diagnostic severity levels recognised by {@link createTelemetryDiagLogger}. */
export type TelemetryDiagLevel = 'error' | 'warn' | 'info' | 'debug' | 'verbose';

/** The five-level diagnostic sink the telemetry subsystem logs through. */
export interface TelemetryDiagLogger {
  error(message: string): void;
  warn(message: string): void;
  info(message: string): void;
  debug(message: string): void;
  verbose(message: string): void;
}

/** Sinks for {@link createTelemetryDiagLogger}; both best-effort and optional. */
export interface TelemetryDiagLoggerDeps {
  /** Error sink: receives an Error for error/warn-level diagnostics (surfaced). */
  onError?: (err: Error) => void;
  /** Debug sink: receives the (verbatim) message for error/warn-level diagnostics. */
  onDebug?: (message: string) => void;
}

/**
 * Build a {@link TelemetryDiagLogger} that routes error/warn diagnostics to BOTH
 * the error sink (as an Error) and the debug sink (verbatim), and suppresses
 * info/debug/verbose entirely. Mirrors Claude's ClaudeCodeDiagLogger, whose
 * error()/warn() call logError + logForDebugging while info/debug/verbose return.
 * Both sinks are isolated (a throwing sink never propagates into the hot path).
 */
export function createTelemetryDiagLogger(deps: TelemetryDiagLoggerDeps = {}): TelemetryDiagLogger {
  const surface = (message: string): void => {
    try { deps.onError?.(new Error(message)); } catch { /* best effort */ }
    try { deps.onDebug?.(message); } catch { /* best effort */ }
  };
  const suppress = (): void => { /* info/debug/verbose are suppressed by design */ };
  return {
    error: surface,
    warn: surface,
    info: suppress,
    debug: suppress,
    verbose: suppress,
  };
}

export class AnalyticsTracker {
  private events: AnalyticsEvent[] = [];
  /** Per-event record time (ms, via {@link _now}) parallel to {@link events}, for the age sweep. */
  private _eventTimes: number[] = [];
  private logFile: string | null;
  private dirInitialized = false;
  private _sessionId: string | undefined;
  /** Per-prompt correlation id stamped onto every EVENT (not resource/metrics — Claude's prompt.id). */
  private _promptId: string | undefined;
  /** Local commit counter feeding the usage view (Claude's getCommitCounter). */
  private _commitCount = 0;
  /** Local PR counter feeding the usage view (Claude's getPrCounter). */
  private _prCount = 0;
  /** Per-source incremental-context tracker, lazily created on first use. */
  private _incrementalContext: IncrementalContextTracker | null = null;
  private _allowedSources: Set<string> | null = null;
  private _sourceStack: string[] = [];
  private _droppedEvents = 0;
  private _droppedBatches = 0;
  /** Running streak of whole-batch export failures (reset by any successful send). */
  private _consecutiveFailures = 0;
  private _sequence = 0;
  private _otlpEndpoint: string | null;
  private _otlpFetch: typeof fetch | undefined;
  private _telemetry: TelemetryBatchSender | null = null;
  private _env: NodeJS.ProcessEnv;
  private _redact: boolean;
  private _sampleRate: number;
  private _sampleRates: Record<string, number> | undefined;
  private _maxAttempts: number;
  private _baseBackoffMs: number;
  private _maxBackoffMs: number;
  private _otlpTimeoutMs: number | undefined;
  private _otlpBatchDelayMs: number;
  private _isKilled: (() => boolean) | undefined;
  private _telemetryMaxBatchSize: number | undefined;
  private _telemetryMaxBatchBytes: number | undefined;
  private _maxConsecutiveFailures: number | undefined;
  private _onBatchDropped: ((eventCount: number, droppedBatchCount: number) => void) | undefined;
  private _otlpHeaders: Record<string, string>;
  private _otlpResourceAttrs: Record<string, string>;
  /** True when the OTLP endpoint is signal-specific (used verbatim, no /v1/logs appended). */
  private _otlpPerSignal: boolean;
  /** Privacy-preserving hash of the local git user.email (introspection); undefined when off/unresolved. */
  private _gitEmailHash: string | undefined;
  /** Resolved undici proxy/mTLS dispatcher attached to every OTLP POST; undefined = fetch default. */
  private _otlpDispatcher: unknown;
  /** In-memory event age cap (ms); undefined disables the age sweep. */
  private _eventTtlMs: number | undefined;
  private _sleep: (ms: number) => Promise<void>;
  private _rand: () => number;
  private _now: () => number;
  private _onDiagnostic: ((message: string) => void) | undefined;
  /** Level-filtered diagnostic logger for genuine telemetry failures (error/warn surfaced). */
  private _diagLog: TelemetryDiagLogger;
  /** Non-blocking deferred-flush writer for the disk sink; null = synchronous append. */
  private _fileWriter: BufferedWriter | null = null;
  /** Resolved disk-sink flush interval (ms); 0 = synchronous append (introspection). */
  private _fileFlushIntervalMs = 0;
  private _consoleSink: (line: string) => void;
  /** Resolved egress allowlist (event names + fields); null when no restriction. */
  private _egressAllowlist: EgressAllowlist | null;
  /** Per-session content-dedup cache, lazily created when dedupeContent is enabled. */
  private _dedup: TelemetryContentDedup | null;
  /** Dynamic OTLP auth-header helper, created when otlpHeadersHelper is configured. */
  private _headersHelper: OtelHeadersHelper | null;
  /** One-time latch for the otelHeadersHelper failure diagnostic. */
  private _warnedHeadersHelper = false;
  /** Resolved egress-exporter flags from OTEL_LOGS_EXPORTER + protocol gating. */
  private _otlpExport: boolean;
  private _consoleExport: boolean;
  /** Resolved per-signal (logs) export interval, threaded into the batch sender. */
  private _exportIntervalMs: number;
  /** Set true once shutdown() has run its final flush — rejects all later egress. */
  private _shutdown = false;
  /** One-time latch for the "no telemetry sink configured" diagnostic. */
  private _warnedNoSink = false;
  /** Resolved per-sink killswitch map (memory/file/otlp), mutable at runtime. */
  private _killedSinks: Partial<Record<SinkName, boolean>>;
  /** Per-event process-metrics sampler, created only when the feature is enabled. */
  private _processMetrics: ProcessMetricsSampler | null;
  /** Durable spool for permanently-dropped batches (cross-run retry); null when off. */
  private _failedStore: FailedBatchStore | null;
  /** One-time latch for the unsupported-protocol diagnostic. */
  private _warnedProtocol = false;
  /** One-time latch for the OTLP-mirror exception-isolation diagnostic. */
  private _warnedMirror = false;
  /** Serializes mid-session (onBatch) sends so concurrent boundary flushes don't interleave. */
  private _sendChain: Promise<void> = Promise.resolve();
  /** Resolved local-only field matcher (stripped from OTLP egress only); null = none. */
  private _localOnly: LocalOnlyFieldMatcher | null;
  /** When true the batch sender flushes one batch per event (immediate-mode bypass). */
  private _immediate: boolean;
  /** Injected numeric-metrics provider; when set, OTLP metrics are exported on flush. */
  private _metricsSource: (() => MetricSnapshot) | undefined;
  /** Injected span-tree provider; when set, OTLP traces are exported on flush. */
  private _traceSource: (() => readonly SpanNode[]) | undefined;
  /** Count of runtime telemetry reconfigurations applied via {@link reconfigureTelemetry}. */
  private _reconfigureCount = 0;
  /**
   * ALS-backed invocation source. Unlike the mutable push/pop {@link _sourceStack},
   * a source established via {@link runWithSource} survives every `await` boundary in
   * that async chain and is isolated from sibling/parent chains — so a void-detached
   * background task (cron, forked slash command, subagent) whose synchronous
   * continuation runs before it resumes still resolves ITS own source rather than a
   * clobbered global. Mirrors Claude's workloadContext AsyncLocalStorage.
   */
  private _sourceAls = new AsyncLocalStorage<{ source: string | undefined }>();
  /** Re-entrancy guard for persisted-batch recovery (Claude's `isRetrying` parity). */
  private _recovering = false;

  constructor(logDir?: string, opts?: AnalyticsTrackerOpts) {
    this.logFile = logDir ? `${logDir}/analytics.jsonl` : null;
    this._otlpEndpoint = opts?.otlpEndpoint ?? null;
    this._otlpFetch = opts?.otlpFetch;
    this._env = opts?.env ?? process.env;
    this._redact = opts?.redact ?? true;
    this._sampleRate = opts?.sampleRate ?? 1;
    this._sampleRates = opts?.sampleRates;
    this._maxAttempts = Math.max(1, opts?.otlpMaxAttempts ?? 3);
    this._baseBackoffMs = Math.max(0, opts?.otlpBaseBackoffMs ?? 500);
    this._maxBackoffMs = Math.max(0, opts?.otlpMaxBackoffMs ?? 30_000);
    this._otlpTimeoutMs = opts?.otlpTimeoutMs;
    this._otlpBatchDelayMs = Math.max(0, opts?.otlpBatchDelayMs ?? 0);
    this._isKilled = opts?.isKilled;
    this._telemetryMaxBatchSize = opts?.telemetryMaxBatchSize;
    this._telemetryMaxBatchBytes = opts?.telemetryMaxBatchBytes;
    this._maxConsecutiveFailures = opts?.maxConsecutiveFailures !== undefined
      ? Math.max(1, Math.floor(opts.maxConsecutiveFailures))
      : undefined;
    this._onBatchDropped = opts?.onBatchDropped;
    // Headers from OTEL_EXPORTER_OTLP_HEADERS, overridden by any explicit opt.
    this._otlpHeaders = { ...parseOtelHeaders(this._env.OTEL_EXPORTER_OTLP_HEADERS), ...(opts?.otlpHeaders ?? {}) };
    this._otlpPerSignal = opts?.otlpLogsEndpointPerSignal ?? false;
    // Resource attrs from OTEL_RESOURCE_ATTRIBUTES, overridden by any explicit opt.
    this._otlpResourceAttrs = { ...parseOtelResourceAttrs(this._env.OTEL_RESOURCE_ATTRIBUTES), ...(opts?.otlpResourceAttrs ?? {}) };
    // Opt-in local git-identity resource attribute: stamp a privacy-preserving HASH of
    // the git user.email (never the raw address) so a collector can attribute usage per
    // developer. Only when telemetry is active; an explicit resource-attr of the same
    // key still wins (merged below with _otlpResourceAttrs taking precedence).
    if (this._otlpEndpoint && (opts?.includeGitUser || isEnvTruthy(this._env.AMI_OTEL_INCLUDE_GIT_USER))) {
      const resolver = opts?.gitEmailResolver ?? getGitEmail;
      const email = resolver(opts?.gitUserCwd ?? process.cwd());
      if (email) {
        this._gitEmailHash = hashFilePath(email);
        this._otlpResourceAttrs = { 'user.git_email': this._gitEmailHash, ...this._otlpResourceAttrs };
      }
    }
    // Stamp a stable per-install id + its privacy-preserving bucket onto every
    // export, so a self-hosted collector can correlate/estimate users across
    // sessions without AMI emitting any raw identity. Only when telemetry is
    // active and a persistence dir is resolvable (explicit opt or the log dir);
    // an explicit resource-attr of the same key still wins.
    if (this._otlpEndpoint) {
      const userIdDir = opts?.userIdDir ?? (logDir ?? undefined);
      if (userIdDir) {
        const userId = getOrCreateUserId(userIdDir);
        this._otlpResourceAttrs = {
          'user.id': userId,
          'user.bucket': String(getUserBucket(userId)),
          ...this._otlpResourceAttrs,
        };
      }
    }
    // OTLP transport (proxy / mTLS / custom CA): an explicit dispatcher wins; else
    // auto-resolve from the proxy + TLS env vars against the configured endpoint
    // (undici is only loaded when a proxy or client-TLS is actually configured —
    // the no-transport common case resolves to undefined, keeping the fetch default).
    this._otlpDispatcher = this._otlpEndpoint
      ? (opts?.otlpDispatcher ?? resolveOtlpTransport(this._otlpEndpoint, this._env, {
          readFile: opts?.otlpTlsReadFile,
          undiciLoader: opts?.undiciLoader as (() => UndiciDispatchers) | undefined,
        }))
      : undefined;
    this._eventTtlMs = resolveInMemoryEventTtlMs(this._env, opts?.inMemoryEventTtlMs);
    this._sleep = opts?.sleepImpl ?? ((ms) => new Promise(r => setTimeout(r, ms)));
    this._rand = opts?.rand ?? Math.random;
    this._now = opts?.now ?? Date.now;
    this._onDiagnostic = opts?.onDiagnostic;
    // Level-filtered diagnostic boundary: genuine telemetry failures surface as an
    // Error (onError) AND on the informational onDiagnostic stream; routine
    // info/debug/verbose are suppressed. onDebug stays verbatim so existing
    // onDiagnostic consumers see the same message text.
    this._diagLog = createTelemetryDiagLogger({
      onError: opts?.onError,
      onDebug: (m) => this._diag(m),
    });
    this._consoleSink = opts?.consoleSink ?? ((line) => process.stdout.write(line + '\n'));
    // Non-blocking deferred-flush disk sink: when a positive flush interval is
    // configured (opt or AMI_ANALYTICS_FILE_FLUSH_MS) and a log file exists, route
    // appends through a buffered writer so a slow fs.appendFileSync never stalls the
    // caller's tick. Drained synchronously on shutdown/close for exit safety.
    this._fileFlushIntervalMs = resolveFileFlushIntervalMs(this._env, opts?.fileFlushIntervalMs);
    if (this.logFile && this._fileFlushIntervalMs > 0) {
      const target = this.logFile;
      this._fileWriter = createBufferedWriter({
        writeFn: (content) => fs.appendFileSync(target, content, { mode: 0o600 }),
        flushIntervalMs: this._fileFlushIntervalMs,
        immediateMode: false,
      });
    }

    // Egress allowlist: only build the double gate when at least one axis is set,
    // so the common (no-restriction) path stays a cheap null check.
    this._egressAllowlist = (opts?.egressEventAllowlist || opts?.egressFieldAllowlist)
      ? { events: opts?.egressEventAllowlist, fields: opts?.egressFieldAllowlist }
      : null;
    this._killedSinks = resolveKilledSinks(this._env, opts?.killedSinks ?? {});
    this._processMetrics = (opts?.includeProcessMetrics || isEnvTruthy(this._env.AMI_OTEL_PROCESS_METRICS))
      ? new ProcessMetricsSampler(opts?.processMetricsDeps)
      : null;
    // Durable failed-batch spool: only when persistence is requested AND a directory
    // is resolvable (explicit opt, or `<logDir>/telemetry-failed`).
    const persist = opts?.persistFailedBatches || isEnvTruthy(this._env.AMI_OTEL_PERSIST_FAILED);
    const failedDir = opts?.failedBatchDir ?? (logDir ? path.join(logDir, 'telemetry-failed') : undefined);
    this._failedStore = persist && failedDir ? new FailedBatchStore(failedDir) : null;
    this._dedup = opts?.dedupeContent ? new TelemetryContentDedup() : null;
    this._localOnly = resolveLocalOnlyFields(this._env, {
      localOnlyFields: opts?.localOnlyFields,
      localOnlyFieldPrefix: opts?.localOnlyFieldPrefix,
    });
    this._immediate = opts?.telemetryImmediateMode ?? isEnvTruthy(this._env.AMI_OTEL_IMMEDIATE_FLUSH);
    this._metricsSource = opts?.metricsSource;
    this._traceSource = opts?.traceSource;
    this._headersHelper = opts?.otlpHeadersHelper
      ? new OtelHeadersHelper(opts.otlpHeadersHelper.command, {
          exec: opts.otlpHeadersHelper.exec,
          now: opts.otlpHeadersHelper.now,
          env: this._env,
        })
      : null;

    // Exporter-type selection (OTEL_LOGS_EXPORTER): an unset value keeps AMI's
    // endpoint-driven OTLP default; 'none' disables all egress; 'console' enables a
    // local stdout dump. Unsupported/unknown types warn once (no crash).
    const rawExporter = this._env.OTEL_LOGS_EXPORTER;
    const exporterTypes = rawExporter === undefined ? ['otlp'] : parseExporterTypes(rawExporter);
    this._otlpExport = exporterTypes.includes('otlp');
    this._consoleExport = exporterTypes.includes('console');
    for (const ty of exporterTypes) {
      if (ty !== 'otlp' && ty !== 'console') {
        this._diag(`[telemetry] Unsupported OTEL_LOGS_EXPORTER type '${ty}' ignored (supported: otlp, console).`);
      }
    }

    // Protocol gating: AMI emits OTLP/HTTP+JSON only. A grpc / http/protobuf (or
    // unknown) protocol can't be honored → disable OTLP export and warn once rather
    // than silently POSTing JSON a protobuf/gRPC-only collector would reject.
    if (this._otlpExport) {
      let protocol: OtlpProtocol | null = null;
      try {
        protocol = resolveOtlpProtocol(this._env, 'logs');
      } catch (e) {
        this._warnProtocol(`[telemetry] ${(e as Error).message}; OTLP export disabled (AMI supports http/json only).`);
        this._otlpExport = false;
      }
      if (protocol && protocol !== 'http/json') {
        this._warnProtocol(`[telemetry] OTLP protocol '${protocol}' is not supported; OTLP export disabled (AMI supports http/json only).`);
        this._otlpExport = false;
      }
    }

    this._exportIntervalMs = resolveExportIntervalMs('logs', this._env);
  }

  /** Emit a diagnostic through the injected bridge (best-effort, never throws). */
  private _diag(message: string): void {
    try { this._onDiagnostic?.(message); } catch { /* best effort */ }
  }

  private _warnProtocol(message: string): void {
    if (this._warnedProtocol) return;
    this._warnedProtocol = true;
    this._diag(message);
  }

  /** Resolved logs export interval (ms) — introspection + batch-sender cadence. */
  getExportIntervalMs(): number {
    return this._exportIntervalMs;
  }

  /** Whether the OTLP endpoint is a signal-specific logs endpoint (used verbatim). */
  isOtlpLogsEndpointPerSignal(): boolean {
    return this._otlpPerSignal;
  }

  /**
   * The privacy-preserving hash of the local git `user.email` stamped as the
   * `user.git_email` resource attribute, or undefined when the git-user attribution
   * opt is off / the email could not be resolved. Introspection surface.
   */
  getGitUserHash(): string | undefined {
    return this._gitEmailHash;
  }

  /**
   * Resolved disk-sink buffered-flush interval (ms); 0 means the synchronous
   * per-event append is used. Introspection surface for the non-blocking disk sink.
   */
  getFileFlushIntervalMs(): number {
    return this._fileFlushIntervalMs;
  }

  /** Whether this tracker has been shut down (post-shutdown export guard). */
  isShutdown(): boolean {
    return this._shutdown;
  }

  /** Whether the named sink is currently killed (strict `=== true`; fail-open). */
  isSinkKilled(sink: SinkName): boolean {
    return this._killedSinks[sink] === true;
  }

  /**
   * Enable/disable individual sinks at runtime (merged over the current map; a
   * `false` re-enables a previously-killed sink). Fail-open. Lets a consumer kill a
   * noisy/leaky sink without reconstructing the tracker (Claude per-sink killswitch).
   */
  setKilledSinks(update: Partial<Record<SinkName, boolean>>): void {
    for (const s of SINK_NAMES) {
      if (update[s] !== undefined) this._killedSinks[s] = update[s];
    }
  }

  /** The resolved privacy level (default | no-telemetry | essential-traffic). */
  getPrivacyLevel(): PrivacyLevel {
    return getPrivacyLevel(this._env);
  }

  getDroppedBatchCount(): number {
    return this._droppedBatches;
  }

  /** Current streak of consecutive whole-batch export failures (0 after a success). */
  getConsecutiveFailureCount(): number {
    return this._consecutiveFailures;
  }

  setSessionId(sessionId: string): void {
    this._sessionId = sessionId;
  }

  /**
   * Set (or clear, with undefined) the per-prompt correlation id stamped onto every
   * subsequent event's data as `prompt.id` — so a turn's events can be grouped to the
   * specific prompt that produced them. Event-level only (never a resource/metric
   * dimension). Mirrors Claude's per-turn getPromptId(). */
  setPromptId(promptId: string | undefined): void {
    this._promptId = promptId;
  }

  /** The current per-prompt correlation id (undefined when none set). */
  getPromptId(): string | undefined {
    return this._promptId;
  }

  /**
   * Record a structured `file_operation` analytics event (read/write/edit) with a
   * privacy-preserving path hash and size-guarded content hash. Delegates to
   * {@link buildFileOperationEvent} (reuses the existing hashing helpers) and routes
   * through the normal {@link log} pipeline so the event is retained/exported like any
   * other. Mirrors Claude's logFileOperation. */
  logFileOperation(params: FileOperationParams): void {
    this.log('file_operation', buildFileOperationEvent(params));
  }

  /**
   * Classify a finished command for local git/PR usage ({@link classifyGitOperation}),
   * increment the commit/PR counters, and emit one `git_operation` event per matched
   * operation. A non-zero exit or a non-git/PR command is a no-op. The counters feed
   * the usage view (Claude's getCommitCounter/getPrCounter parity). */
  trackGitOperation(command: string, exitCode: number): void {
    const c = classifyGitOperation(command, exitCode);
    if (c.operations.length === 0) return;
    this._commitCount += c.commitDelta;
    this._prCount += c.prDelta;
    for (const operation of c.operations) this.log('git_operation', { operation });
  }

  /** Local successful-commit count this session (usage view). */
  getCommitOperationCount(): number {
    return this._commitCount;
  }

  /** Local PR-create count this session (usage view). */
  getPrOperationCount(): number {
    return this._prCount;
  }

  /**
   * Compute the per-source incremental-context delta (only the NEW user messages
   * since this source's previous call) via a lazily-created
   * {@link IncrementalContextTracker}, AND emit a privacy-safe `context_delta` event
   * carrying the SOURCE and `new_context_message_count` (never the raw message bodies
   * — those stay local). Returns the delta so a caller can additionally trace content
   * under an explicit opt-in. Mirrors Claude's betaSessionTracing new_context tracking. */
  recordIncrementalContext<T>(
    source: string,
    messages: readonly T[],
    hashOf: (m: T) => string,
    isUser: (m: T) => boolean,
  ): IncrementalContextDelta<T> {
    if (!this._incrementalContext) this._incrementalContext = new IncrementalContextTracker();
    const delta = this._incrementalContext.delta(source, messages, hashOf, isUser);
    if (delta.count > 0) {
      this.log('context_delta', { source, new_context_message_count: delta.count });
    }
    return delta;
  }

  setAllowedSources(sources: string[] | null): void {
    this._allowedSources = sources ? new Set(sources) : null;
  }

  pushSource(source: string): void {
    this._sourceStack.push(source);
  }

  popSource(): string | undefined {
    return this._sourceStack.pop();
  }

  /**
   * Run `fn` within an async-isolated invocation-source context: {@link currentSource}
   * (and the {@link setAllowedSources} gate it drives) resolve to `source` for the
   * entire async chain of `fn` — across every `await` — and NOT for sibling/parent
   * chains. ALWAYS establishes a fresh boundary (even for `undefined`) so a leaked
   * ambient context can never bleed in (Claude workloadContext parity). Returns `fn`'s
   * return value (including a Promise, whose continuation keeps the context). Prefer
   * this over {@link pushSource}/{@link popSource} for void-detached background work
   * (cron, forked slash commands, subagents) whose continuation outlives the caller.
   */
  runWithSource<T>(source: string | undefined, fn: () => T): T {
    return this._sourceAls.run({ source }, fn);
  }

  currentSource(): string | undefined {
    // An ALS boundary (survives awaits, isolated per chain) always wins — even when it
    // was established with an undefined source (so a fresh `runWithSource(undefined, …)`
    // shadows a leaked ambient stack rather than falling through to it). Only with NO
    // ALS boundary at all do we consult the synchronous push/pop stack.
    const fromAls = this._sourceAls.getStore();
    if (fromAls !== undefined) return fromAls.source;
    return this._sourceStack.length > 0 ? this._sourceStack[this._sourceStack.length - 1] : undefined;
  }

  getDroppedEventCount(): number {
    return this._droppedEvents;
  }

  log(type: string, data: Record<string, unknown> = {}): void {
    if (this._allowedSources !== null) {
      const source = this.currentSource();
      if (source === undefined || !this._allowedSources.has(source)) {
        this._droppedEvents++;
        return;
      }
    }

    // Per-event sampling: a per-TYPE rate (when configured) overrides the
    // tracker-wide default. Dropped events are counted; kept events sampled below
    // full rate carry their applied `sample_rate` so counts can be un-biased.
    const effectiveRate = resolveSampleRate(type, this._sampleRates, this._sampleRate);
    if (!shouldSampleEvent(effectiveRate, this._rand)) {
      this._droppedEvents++;
      return;
    }
    const sampled = effectiveRate < 1;
    let recordData = sampled ? { ...data, sample_rate: effectiveRate } : data;

    // Optional per-event process metrics (memory + CPU% delta since the last event).
    if (this._processMetrics) {
      const pm = this._processMetrics.sample();
      if (pm) recordData = { ...recordData, process: pm };
    }

    // Per-prompt correlation id: stamped onto every EVENT so a session's events can
    // be grouped to the specific prompt/turn that produced them. Deliberately an
    // event-level dimension (never a resource/metric one, where it would be unbounded
    // cardinality) — mirrors Claude's getPromptId() `prompt.id` attribute. An explicit
    // `prompt.id` already in the data wins.
    if (this._promptId !== undefined && !('prompt.id' in recordData)) {
      recordData = { ...recordData, 'prompt.id': this._promptId };
    }

    // In-memory events keep the RAW data for faithful local introspection; the
    // egress copies (JSONL on disk + OTLP batch) are data-minimized/redacted.
    const event: AnalyticsEvent = {
      timestamp: new Date().toISOString(),
      type,
      ...(this._sessionId ? { sessionId: this._sessionId } : {}),
      data: recordData,
      sequence: this._sequence++,
    } as AnalyticsEvent;
    // In-memory sink (killable): skip retention when the memory sink is disabled.
    if (!this.isSinkKilled('memory')) {
      // Age sweep first (bounds by TTL), then append and apply the count cap.
      this._evictExpiredEvents();
      this.events.push(event);
      this._eventTimes.push(this._now());
      if (this.events.length > MAX_IN_MEMORY_EVENTS) {
        this.events = this.events.slice(-MAX_IN_MEMORY_EVENTS);
        this._eventTimes = this._eventTimes.slice(-MAX_IN_MEMORY_EVENTS);
      }
    }

    // Branded so only redaction-verified (or explicitly-marked) metadata can reach
    // the disk/OTLP egress sinks; the redact=false path opts out via the escape hatch.
    let egressData: AnalyticsSafeMetadata = this._redact
      ? sanitizeAnalyticsData(recordData, { env: this._env })
      : markAnalyticsSafe(recordData);

    // Egress allowlist (Claude datadog double gate): drop the whole event from every
    // egress sink when its TYPE is not allowed (it stays in-memory), else strip the
    // payload to the allowed fields. Applied before dedup/append so disk + console +
    // OTLP all see the identical minimized copy.
    if (this._egressAllowlist) {
      const filtered = filterEventForEgress(type, egressData, this._egressAllowlist);
      if (filtered === null) return;
      egressData = markAnalyticsSafe(filtered);
    }

    // Content dedup: collapse large repeated bodies to a preview+hash+length after the
    // first time their SHA-256 is seen this session (re-branded; filtering only shrinks).
    if (this._dedup) {
      egressData = markAnalyticsSafe(this._dedup.dedupe(egressData));
    }

    this._appendEgress(event, egressData);

    // Telemetry EGRESS (off-machine OTLP / stdout console dump) is suppressed in a
    // test environment by default, and entirely after shutdown — local in-memory +
    // disk records above still happen.
    const suppressed = isTelemetrySuppressedForTest(this._env);
    if (suppressed || this._shutdown) return;

    // Console exporter (OTEL_LOGS_EXPORTER=console): a human-readable live dump of
    // the redacted payload for local introspection with no collector wired.
    if (this._consoleExport) {
      const dump = trySerialize({ type, ...(this._sessionId ? { sessionId: this._sessionId } : {}), data: egressData });
      if (dump !== null) this._consoleSink(dump);
    }

    // Warn ONCE if an event has nowhere durable to go (no disk log, no active OTLP
    // export, no console exporter) — surfacing a misconfigured/absent sink instead
    // of silently losing telemetry (mirrors Claude's hasWarnedNoEventLogger latch).
    const hasSink = !!this.logFile
      || (!!this._otlpEndpoint && this._otlpExport && !isTelemetryDisabled(this._env))
      || this._consoleExport;
    if (!hasSink && !this._warnedNoSink) {
      this._warnedNoSink = true;
      this._diag(`[telemetry] Event dropped (no telemetry sink configured): ${type}`);
    }

    // Mirror into the OTLP batch sender when an endpoint is configured, the exporter
    // selection keeps OTLP enabled, AND the user has not opted out of telemetry.
    // Lazily created so the batch carries the (by-then-known) session id; a
    // size/byte boundary flush is POSTed mid-session via the onBatch hook, and the
    // resolved per-signal interval drives the periodic timer flush.
    if (this._otlpEndpoint && this._otlpExport && !isTelemetryDisabled(this._env) && !this.isSinkKilled('otlp')) {
      // Exception-isolated like the file-append sink above: a throw from sender
      // construction / enqueue (e.g. a bad batch opt) must never propagate into the
      // engine's hot loop — analytics is always best-effort (mirrors Claude's
      // backend-mirror try/catch isolation). A failure warns once, then is swallowed.
      try {
        if (!this._telemetry) {
          this._telemetry = new TelemetryBatchSender({
            sessionId: this._sessionId ?? 'unknown',
            endpoint: this._otlpEndpoint,
            maxBatchSize: this._telemetryMaxBatchSize,
            maxBatchBytes: this._telemetryMaxBatchBytes,
            flushIntervalMs: this._exportIntervalMs,
            immediateMode: this._immediate,
            onBatch: (batch) => this._scheduleSend(batch),
          });
          this._telemetry.startAutoFlush();
        }
        // Differential redaction: the off-machine OTLP copy drops local-only fields
        // (copy-on-write) while the on-machine disk + console sinks above kept the full
        // payload — full to the privileged/local sinks, minimized to general-access.
        const otlpEgress = this._localOnly
          ? markAnalyticsSafe(stripLocalOnlyFields(egressData, this._localOnly))
          : egressData;
        this._telemetry.enqueue(type, otlpEgress, event.sequence);
      } catch (e) {
        if (!this._warnedMirror) {
          this._warnedMirror = true;
          this._diagLog.warn(`[telemetry] OTLP mirror failed (events stay local): ${(e as Error).message}`);
        }
      }
    }
  }

  /**
   * Append one redaction-verified event to the JSONL log. Requires branded
   * {@link AnalyticsSafeMetadata} so unredacted metadata cannot reach disk by
   * construction. The line is NDJSON-safe (U+2028/U+2029 escaped so a
   * line-splitting reader can't truncate it) and the file is created with a
   * restrictive 0o600 mode (dir 0o700) — the log holds session telemetry and must
   * not be world-readable. Best-effort: never throws; a poison (unserializable)
   * event is skipped rather than crashing the writer.
   */
  private _appendEgress(event: AnalyticsEvent, egressData: AnalyticsSafeMetadata): void {
    if (!this.logFile || this.isSinkKilled('file')) return;
    try {
      if (!this.dirInitialized) {
        fs.mkdirSync(path.dirname(this.logFile), { recursive: true, mode: 0o700 });
        this.dirInitialized = true;
      }
      const line = ndjsonSafeStringify({ ...event, data: egressData }) + '\n';
      // Deferred-flush disk sink (when enabled) keeps a slow appendFileSync off the
      // caller's tick; otherwise append synchronously (the default).
      if (this._fileWriter) this._fileWriter.write(line);
      else fs.appendFileSync(this.logFile, line, { mode: 0o600 });
    } catch {
      /* best effort */
    }
  }

  /**
   * Drop the leading run of in-memory events older than the configured TTL (events
   * are chronological, so the expired ones are always a prefix). No-op when the age
   * sweep is disabled. A lazy sweep: invoked before each append and on every read so
   * stale entries never surface even in a session that stops logging.
   */
  private _evictExpiredEvents(): void {
    if (this._eventTtlMs === undefined || this.events.length === 0) return;
    const cutoff = this._now() - this._eventTtlMs;
    let i = 0;
    while (i < this._eventTimes.length && this._eventTimes[i] < cutoff) i++;
    if (i > 0) {
      this.events = this.events.slice(i);
      this._eventTimes = this._eventTimes.slice(i);
    }
  }

  getEvents(): AnalyticsEvent[] {
    this._evictExpiredEvents();
    return [...this.events];
  }

  getEventsByType(type: string): AnalyticsEvent[] {
    this._evictExpiredEvents();
    return this.events.filter(e => e.type === type);
  }

  getSummary(): Record<string, number> {
    this._evictExpiredEvents();
    const counts: Record<string, number> = {};
    for (const e of this.events) {
      counts[e.type] = (counts[e.type] || 0) + 1;
    }
    return counts;
  }

  reset(): void {
    this.events = [];
    this._eventTimes = [];
    // A reset begins a fresh logical session, so forget deduped content hashes —
    // content seen before reset should log in full again afterwards.
    this._dedup?.reset();
    // Fresh session: forget per-source incremental-context hashes so the next delta
    // reports from the start, and zero the local usage counters.
    this._incrementalContext?.reset();
    this._commitCount = 0;
    this._prCount = 0;
  }

  /** The resolved beta detailed-content-tracing config (flag + dedicated endpoint). */
  getBetaTracingConfig(): BetaTracingConfig {
    return resolveBetaTracingConfig(this._env);
  }

  /**
   * Total telemetry events not yet successfully exported (queued + retained ready
   * batches), readable even after {@link shutdown} — exit/shutdown diagnostics.
   * Mirrors Claude's SerialBatchEventUploader.pendingCount.
   */
  getPendingTelemetryCount(): number {
    return this._telemetry?.pendingCount ?? 0;
  }

  /**
   * Bounded graceful close: give the final {@link flushOtlp} a best-effort `graceMs`
   * window (racing it against a timeout) before latching shutdown, so a slow/hung
   * collector can never block exit beyond the grace period. Mirrors Claude's
   * HybridTransport close() `Promise.race([flush(), timeout(CLOSE_GRACE_MS)])`. Any
   * events still queued are left for a later process to recover. Idempotent;
   * best-effort (never throws).
   */
  async closeWithGrace(graceMs: number = resolveShutdownTimeoutMs(this._env)): Promise<void> {
    if (this._shutdown) return;
    await raceWithTimeout(this.flushOtlp(), graceMs, this._sleep);
    this._shutdown = true;
    this._telemetry?.shutdown();
    // Exit-safe: synchronously drain any buffered disk-sink writes (incl. a queued
    // deferred overflow that may not have fired its setImmediate yet).
    this._fileWriter?.dispose();
  }

  /** Drain any queued telemetry and POST it to the configured OTLP endpoint with
   *  bounded retry. No-op (and never throws) when no endpoint is set, OTLP export is
   *  disabled, the tracker has been shut down, or the user has opted out of
   *  telemetry. The whole drain is bounded by an overall flush budget
   *  ({@link resolveFlushTimeoutMs}); on exhaustion the remaining batches are
   *  re-queued (at-least-once) and an actionable remediation diagnostic is emitted
   *  so a slow/unreachable collector never blocks exit unboundedly. Best-effort. */
  async flushOtlp(): Promise<void> {
    if (!this._otlpEndpoint || !this._otlpExport || this._shutdown) return;
    // OTLP sink killed at runtime: leave everything queued for a later flush.
    if (this.isSinkKilled('otlp')) return;
    // First re-send anything a previous run/flush spooled to disk (cross-run retry),
    // so a recovered collector drains the durable backlog before new batches. This
    // runs even when nothing was logged THIS session (no live sender) — the whole
    // point is to recover a prior session's dropped telemetry.
    if (this._failedStore) await this.recoverPersistedBatches();
    // Export the metrics + traces signals (when a source is wired) alongside logs, so a
    // flush/shutdown drains all configured OTLP signals — even in a session that queued
    // no log events (metrics/traces are independent of the logs batch queue).
    await this._exportMetricsAndTraces();
    if (!this._telemetry) return;
    // Let any in-flight mid-session (onBatch) sends settle first so a boundary
    // batch is never POSTed concurrently with the shutdown drain.
    await this._sendChain;
    // Runtime killswitch: produce zero traffic while killed; leave everything
    // queued so a later flush (after the flag clears) still delivers it.
    if (this._isKilled?.()) return;
    const batches = this._telemetry.drain();
    if (isTelemetryDisabled(this._env)) return; // opted out: drop without sending
    const maxCons = this._maxConsecutiveFailures;
    // Overall flush budget: the whole drain must finish within this many ms (each
    // batch is also per-request bounded by otlpTimeoutMs). When the deadline passes
    // mid-drain, requeue the remainder and surface numbered remediation guidance.
    const budgetMs = resolveFlushTimeoutMs(this._env);
    const deadline = this._now() + budgetMs;
    for (let i = 0; i < batches.length; i++) {
      if (this._now() >= deadline) {
        this._telemetry.requeueFront(batches.slice(i));
        this._diag(formatFlushTimeoutGuidance(budgetMs));
        return;
      }
      const ok = await this._sendWithRetry(batches[i]);
      if (ok) {
        this._consecutiveFailures = 0; // success breaks the failure streak
        // Pace consecutive POSTs so a large chunked drain does not hammer the
        // collector (no delay after the final batch).
        if (this._otlpBatchDelayMs > 0 && i < batches.length - 1) await this._sleep(this._otlpBatchDelayMs);
        continue;
      }
      // Batch was retried to exhaustion and permanently dropped (counted in
      // _dropBatch). Extend the failure streak, and durably spool it so a later
      // process can re-send it (cross-run retry) rather than losing it outright.
      this._consecutiveFailures++;
      this._failedStore?.spool(batches[i].events, this._rand);
      // Default (no circuit breaker): the endpoint looks down after the first
      // fully-retried failure — short-circuit and re-queue the rest for the next
      // flush (at-least-once, order preserved). With maxConsecutiveFailures set,
      // keep ADVANCING past dropped batches (fresh per-batch budget) until that
      // many fail in a row, then give up this flush and requeue the remainder.
      if (maxCons === undefined || this._consecutiveFailures >= maxCons) {
        this._telemetry.requeueFront(batches.slice(i + 1));
        break;
      }
    }
  }

  /**
   * Gracefully shut the telemetry pipeline down: run one final {@link flushOtlp}
   * while still live, then latch `_shutdown` so every later {@link log} egress and
   * {@link flushOtlp} is rejected and the batch sender refuses further events — a
   * post-shutdown export guard mirroring Claude's exporter `isShutdown` flag. Safe
   * to call more than once. Best-effort: never throws.
   */
  async shutdown(): Promise<void> {
    if (this._shutdown) return;
    await this.flushOtlp();
    this._shutdown = true;
    this._telemetry?.shutdown();
    // Exit-safe: synchronously drain any buffered disk-sink writes before teardown.
    this._fileWriter?.dispose();
  }

  /**
   * Queue a boundary-flushed batch for a serialized, best-effort mid-session POST
   * (invoked from the TelemetryBatchSender onBatch hook). A failed send is re-queued
   * at the front so it is retried on the next flush. Never throws; sends are chained
   * so they never overlap. Await via {@link whenSent}.
   */
  private _scheduleSend(batch: TelemetryBatch): void {
    if (!this._otlpEndpoint || isTelemetryDisabled(this._env)) return;
    this._sendChain = this._sendChain.then(async () => {
      // Killswitch: requeue the boundary batch for a later flush instead of sending.
      if (this._isKilled?.()) { this._telemetry?.requeueFront([batch]); return; }
      const ok = await this._sendWithRetry(batch);
      if (ok) {
        // A successful mid-session send proves the collector is healthy — immediately
        // drain any previously-spooled failed batches instead of waiting for the next
        // flush/backoff tick (Claude's "success means healthy, retry queued" parity).
        if (this._failedStore) await this.recoverPersistedBatches();
      } else if (this._telemetry) {
        this._telemetry.requeueFront([batch]);
      }
    });
  }

  /** Resolve once all currently-scheduled mid-session sends have settled (testability). */
  async whenSent(): Promise<void> {
    await this._sendChain;
  }

  /**
   * Resolve the OTLP headers for a POST: the static headers, with any dynamically
   * minted otelHeadersHelper headers merged over them (helper wins). A failing helper
   * never aborts the export — it warns once and the static headers are used.
   */
  private _resolveHeaders(): Record<string, string> | undefined {
    let merged = this._otlpHeaders;
    if (this._headersHelper) {
      try {
        const dynamic = this._headersHelper.get();
        merged = { ...this._otlpHeaders, ...dynamic };
      } catch (e) {
        if (!this._warnedHeadersHelper) {
          this._warnedHeadersHelper = true;
          this._diagLog.warn(`[telemetry] otelHeadersHelper failed; using static headers only: ${(e as Error).message}`);
        }
      }
    }
    return Object.keys(merged).length > 0 ? merged : undefined;
  }

  /** POST one batch, retrying retryable failures with capped quadratic backoff + jitter. */
  private async _sendWithRetry(batch: TelemetryBatch): Promise<boolean> {
    const headers = this._resolveHeaders();
    const resourceAttrs = this._otlpResourceAttrs;
    for (let attempt = 1; attempt <= this._maxAttempts; attempt++) {
      const res = await postOtlpBatch(this._otlpEndpoint!, batch, { fetchImpl: this._otlpFetch, env: this._env, headers, resourceAttrs, timeoutMs: this._otlpTimeoutMs, dispatcher: this._otlpDispatcher, perSignal: this._otlpPerSignal });
      if (res.ok) return true;
      if (!res.retryable) return this._dropBatch(batch); // 4xx: permanent, drop
      if (attempt === this._maxAttempts) return this._dropBatch(batch);
      const base = Math.min(this._maxBackoffMs, this._baseBackoffMs * attempt * attempt);
      const jittered = res.retryAfterMs ?? Math.round(base * (0.5 + this._rand() * 0.5));
      if (jittered > 0) await this._sleep(jittered);
    }
    return false;
  }

  /** Record a permanently-dropped batch, firing the onBatchDropped diagnostic hook. */
  private _dropBatch(batch: TelemetryBatch): false {
    this._droppedBatches++;
    this._onBatchDropped?.(batch.events.length, this._droppedBatches);
    return false;
  }

  /**
   * Export the metrics + traces OTLP signals when their source providers are wired.
   * Best-effort and exception-isolated (a throwing source/export never breaks the logs
   * drain). Invoked from {@link flushOtlp} so all configured signals drain together.
   */
  private async _exportMetricsAndTraces(): Promise<void> {
    if (this._metricsSource) {
      try { await this.exportMetrics(this._metricsSource()); } catch { /* best effort */ }
    }
    if (this._traceSource) {
      try { await this.exportTrace(this._traceSource()); } catch { /* best effort */ }
    }
  }

  /**
   * Export a point-in-time {@link MetricSnapshot} as an OTLP METRICS payload to the
   * resolved metrics endpoint (dedicated `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, else the
   * generic endpoint with `/v1/metrics` appended), with the same retry/backoff + headers
   * + proxy/mTLS transport as the logs path. Honors every egress guard (no endpoint /
   * OTLP export disabled / shut down / telemetry disabled / sink killed / killswitch /
   * test-suppressed) exactly like {@link flushOtlp}. Returns whether the POST succeeded.
   */
  async exportMetrics(snapshot: MetricSnapshot): Promise<boolean> {
    if (!this._canExportSignal()) return false;
    const ep = resolveOtlpSignalEndpoint('metrics', this._env) ?? { url: this._otlpEndpoint!, perSignal: false };
    const payload = toOtlpMetricsPayload(snapshot, this._sessionId ?? 'unknown', this._otlpResourceAttrs, this._env);
    return this._postSignal(otlpSignalUrl(ep.url, 'metrics', ep.perSignal), payload);
  }

  /**
   * Export a Profiler span tree as an OTLP TRACES payload to the resolved traces
   * endpoint (dedicated `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, else the generic endpoint
   * with `/v1/traces` appended), preserving the parent-child hierarchy
   * ({@link toOtlpTracePayload}). Shares the metrics/logs guards, retry, headers and
   * transport. A no-op (returns false) when there are no spans. Returns POST success.
   */
  async exportTrace(spans: readonly SpanNode[]): Promise<boolean> {
    if (spans.length === 0) return false;
    if (!this._canExportSignal()) return false;
    const ep = resolveOtlpSignalEndpoint('traces', this._env) ?? { url: this._otlpEndpoint!, perSignal: false };
    const payload = toOtlpTracePayload(spans, this._sessionId ?? 'unknown', this._otlpResourceAttrs, this._env);
    return this._postSignal(otlpSignalUrl(ep.url, 'traces', ep.perSignal), payload);
  }

  /** Whether off-machine OTLP egress is currently permitted (shared metrics/traces guard). */
  private _canExportSignal(): boolean {
    if (!this._otlpEndpoint || !this._otlpExport || this._shutdown) return false;
    if (isTelemetryDisabled(this._env) || this.isSinkKilled('otlp') || this._isKilled?.()) return false;
    if (isTelemetrySuppressedForTest(this._env)) return false;
    return true;
  }

  /** POST a pre-built OTLP signal payload with the shared retry/backoff + headers/transport. */
  private async _postSignal(url: string, payload: Record<string, unknown>): Promise<boolean> {
    const headers = this._resolveHeaders();
    for (let attempt = 1; attempt <= this._maxAttempts; attempt++) {
      const res = await postOtlpPayload(url, payload, {
        fetchImpl: this._otlpFetch, headers, timeoutMs: this._otlpTimeoutMs, dispatcher: this._otlpDispatcher,
      });
      if (res.ok) return true;
      if (!res.retryable || attempt === this._maxAttempts) return false;
      const base = Math.min(this._maxBackoffMs, this._baseBackoffMs * attempt * attempt);
      const jittered = res.retryAfterMs ?? Math.round(base * (0.5 + this._rand() * 0.5));
      if (jittered > 0) await this._sleep(jittered);
    }
    return false;
  }

  /**
   * Reconfigure the live telemetry batch pipeline at runtime (event-loss-safe: the
   * current buffer is flushed before the param swap — see
   * {@link TelemetryBatchSender.reconfigure}) AND keep the tracker's own resolved
   * cadence/batch knobs in sync so a later lazily-created sender inherits the new values.
   * Mirrors Claude's reinitialize1PEventLoggingIfConfigChanged.
   */
  reconfigureTelemetry(opts: TelemetryReconfigureOpts): void {
    this._telemetry?.reconfigure(opts);
    if (opts.flushIntervalMs !== undefined && opts.flushIntervalMs > 0) this._exportIntervalMs = opts.flushIntervalMs;
    if (opts.maxBatchSize !== undefined) this._telemetryMaxBatchSize = opts.maxBatchSize;
    if (opts.maxBatchBytes !== undefined) this._telemetryMaxBatchBytes = opts.maxBatchBytes ?? undefined;
    if (opts.immediateMode !== undefined) this._immediate = opts.immediateMode;
    this._reconfigureCount++;
  }

  /** Number of runtime telemetry reconfigurations applied (introspection surface). */
  getTelemetryReconfigureCount(): number {
    return this._reconfigureCount;
  }

  /**
   * Re-send any batches a PREVIOUS run (or an earlier flush) spooled to disk: each
   * file is reloaded and POSTed with the normal retry; a file that sends successfully
   * is deleted, one that still fails is left for the next attempt. No-op when
   * persistence is off, no endpoint is configured, OTLP is disabled/killed, or the
   * tracker is shut down. Returns the number of files successfully re-sent. Invoked
   * automatically at the head of {@link flushOtlp}; also callable directly.
   */
  async recoverPersistedBatches(): Promise<number> {
    if (!this._failedStore || !this._otlpEndpoint || !this._otlpExport || this._shutdown) return 0;
    if (isTelemetryDisabled(this._env) || this.isSinkKilled('otlp') || this._isKilled?.()) return 0;
    // Re-entrancy guard (Claude's `isRetrying`): a recovery triggered by a successful
    // mid-session send must not overlap one already running at a flush head.
    if (this._recovering) return 0;
    this._recovering = true;
    try {
      return await this._recoverPersistedBatchesInner();
    } finally {
      this._recovering = false;
    }
  }

  private async _recoverPersistedBatchesInner(): Promise<number> {
    // Non-null: the public entry point guards `_failedStore`/`_otlpEndpoint` before
    // delegating here.
    const store = this._failedStore!;
    const endpoint = this._otlpEndpoint!;
    let recovered = 0;
    for (const file of store.list()) {
      const events = store.load(file);
      if (events.length === 0) { store.delete(file); continue; }
      const batch: TelemetryBatch = { events, sessionId: this._sessionId ?? 'unknown', batchId: -1 };
      const headers = this._resolveHeaders();
      const res = await postOtlpBatch(endpoint, batch, {
        fetchImpl: this._otlpFetch, env: this._env, headers,
        resourceAttrs: this._otlpResourceAttrs, timeoutMs: this._otlpTimeoutMs,
        dispatcher: this._otlpDispatcher, perSignal: this._otlpPerSignal,
      });
      if (res.ok) {
        store.delete(file);
        recovered++;
      } else if (!res.retryable) {
        // Permanent (4xx) — the collector will never accept it; stop re-spooling.
        store.delete(file);
      }
      // Retryable failure: leave the file in place for the next recovery pass.
    }
    return recovered;
  }
}

/**
 * Generate a compact summary of tool results for a turn.
 *
 * Each tool result is condensed to one line: the tool name, OK/ERROR status,
 * and the first line of output (up to 100 chars).
 */
export interface TelemetryBatch {
  events: Array<{ type: string; timestamp: number; data: Record<string, unknown>; sequence?: number }>;
  sessionId: string;
  batchId: number;
}

export interface TelemetryBatchSenderOpts {
  sessionId: string;
  endpoint?: string;
  maxBatchSize?: number;
  flushIntervalMs?: number;
  maxQueueSize?: number;
  /**
   * Max cumulative serialized-JSON bytes per batch. The first item of a batch is
   * ALWAYS included (so a single oversized event still ships, just alone);
   * subsequent items are added only while the running total stays under the cap.
   * null (default) disables the byte ceiling (count-based batching only).
   */
  maxBatchBytes?: number;
  /**
   * Invoked with each batch produced by an auto-flush (size/byte boundary or the
   * interval timer) so a consumer can POST it mid-session instead of waiting for
   * drain(). When unset, auto-flushed batches are retained for the next drain()
   * (never discarded — one-shot flush at the boundary must not lose events).
   */
  onBatch?: (batch: TelemetryBatch) => void;
  /**
   * Bypass batching: flush one batch per {@link TelemetryBatchSender.enqueue} call
   * instead of accumulating to the size/byte boundary (the interval timer is moot with
   * nothing buffered). A single config knob turning the buffered sink into a
   * pass-through for low-latency/debug sinks. Mirrors Claude's bufferedWriter
   * `immediateMode`.
   */
  immediateMode?: boolean;
  /**
   * Apply LOSSLESS backpressure instead of lossy drop when the queue is full:
   * {@link TelemetryBatchSender.enqueueAsync} awaits until drain frees space rather
   * than dropping the newest event. Off by default (the synchronous {@link TelemetryBatchSender.enqueue}
   * stays lossy — best-effort telemetry). Blocked producers are released when drain
   * frees space OR on {@link TelemetryBatchSender.shutdown}. Mirrors Claude's
   * SerialBatchEventUploader backpressure (enqueue awaits, close releases waiters).
   */
  backpressure?: boolean;
}

/**
 * Runtime-reconfigurable batch-pipeline parameters (Claude-Code firstPartyEventLogger.ts
 * reinitialize1PEventLoggingIfConfigChanged parity). Any subset may be changed on a live
 * sender via {@link TelemetryBatchSender.reconfigure}; omitted fields are left unchanged.
 */
export interface TelemetryReconfigureOpts {
  maxBatchSize?: number;
  maxBatchBytes?: number | null;
  flushIntervalMs?: number;
  maxQueueSize?: number;
  immediateMode?: boolean;
}

export class TelemetryBatchSender {
  private _queue: TelemetryBatch['events'] = [];
  /** Auto-flushed batches awaiting drain() when no onBatch consumer is wired. */
  private _ready: TelemetryBatch[] = [];
  private _batchId = 0;
  private _maxBatchSize: number;
  private _maxBatchBytes: number | null;
  private _flushIntervalMs: number;
  private _timer: ReturnType<typeof setTimeout> | null = null;
  private _endpoint: string | null;
  private _sessionId: string;
  private _totalSent = 0;
  private _totalDropped = 0;
  private _maxQueueSize: number;
  private _onBatch: ((batch: TelemetryBatch) => void) | null;
  /** When true, every enqueue() flushes immediately (batching bypass). */
  private _immediate: boolean;
  /** Latched by shutdown() — rejects all further enqueue()/flush() (export guard). */
  private _shutdown = false;
  /** When true, {@link enqueueAsync} blocks (awaits) on a full queue instead of dropping. */
  private _backpressure: boolean;
  /** Resolvers for producers blocked in {@link enqueueAsync}; released as space frees / on shutdown. */
  private _backpressureResolvers: Array<() => void> = [];
  /** Count of successful runtime {@link reconfigure} calls (introspection). */
  private _reconfigureCount = 0;

  constructor(opts: TelemetryBatchSenderOpts) {
    this._sessionId = opts.sessionId;
    this._endpoint = opts.endpoint ?? null;
    this._maxBatchSize = opts.maxBatchSize ?? 100;
    this._maxBatchBytes = opts.maxBatchBytes ?? null;
    this._flushIntervalMs = opts.flushIntervalMs ?? 30_000;
    this._maxQueueSize = opts.maxQueueSize ?? 10_000;
    this._onBatch = opts.onBatch ?? null;
    this._immediate = opts.immediateMode ?? false;
    this._backpressure = opts.backpressure ?? false;
  }

  enqueue(type: string, data: Record<string, unknown>, sequence?: number): boolean {
    if (this._shutdown) {
      this._totalDropped++;
      return false;
    }
    if (this._queue.length >= this._maxQueueSize) {
      this._totalDropped++;
      return false;
    }
    this._queue.push({ type, timestamp: Date.now(), data, ...(sequence !== undefined ? { sequence } : {}) });
    // Immediate mode: pass-through — flush this event right away instead of waiting for
    // the size/byte/interval boundary.
    if (this._immediate || this._queue.length >= this._maxBatchSize) {
      this._emit();
    }
    return true;
  }

  /**
   * LOSSLESS enqueue for callers that must not drop events: when backpressure mode is
   * enabled (constructor opt) and the queue is full, this AWAITS until drain frees space
   * rather than dropping the newest event, then enqueues it. Resolves `false` only when
   * the sender is (or becomes) shut down. With backpressure OFF it is equivalent to the
   * synchronous {@link enqueue} (no blocking — the lossy best-effort default). Blocked
   * producers are released as {@link takeBatch} frees space or by {@link shutdown}.
   * Mirrors Claude's SerialBatchEventUploader.enqueue await-until-space.
   */
  async enqueueAsync(type: string, data: Record<string, unknown>, sequence?: number): Promise<boolean> {
    if (this._shutdown) { this._totalDropped++; return false; }
    if (!this._backpressure) return this.enqueue(type, data, sequence);
    while (this._queue.length >= this._maxQueueSize && !this._shutdown) {
      await new Promise<void>(resolve => { this._backpressureResolvers.push(resolve); });
    }
    if (this._shutdown) { this._totalDropped++; return false; }
    return this.enqueue(type, data, sequence);
  }

  /** Release every producer blocked in {@link enqueueAsync} (space freed, or shutdown). */
  private _releaseBackpressure(): void {
    if (this._backpressureResolvers.length === 0) return;
    const resolvers = this._backpressureResolvers;
    this._backpressureResolvers = [];
    for (const resolve of resolvers) resolve();
  }

  /** Number of producers currently blocked awaiting space in {@link enqueueAsync}. */
  get blockedProducerCount(): number { return this._backpressureResolvers.length; }

  /**
   * Pull the next batch's worth of events off the queue, honoring both the count
   * cap (maxBatchSize) and the byte cap (maxBatchBytes, first-item-always), and
   * quarantining any un-serializable (poison) event by dropping+counting it in
   * place so it can never block the head of the queue.
   */
  private takeBatch(): TelemetryBatch['events'] {
    const events: TelemetryBatch['events'] = [];
    let bytes = 0;
    while (this._queue.length > 0 && events.length < this._maxBatchSize) {
      const ev = this._queue[0];
      const serialized = trySerialize(ev);
      if (serialized === null) {
        this._queue.shift();
        this._totalDropped++;
        continue;
      }
      const size = Buffer.byteLength(serialized, 'utf8');
      if (events.length > 0 && this._maxBatchBytes !== null && bytes + size > this._maxBatchBytes) {
        break; // byte cap reached — leave the rest for the next batch
      }
      this._queue.shift();
      events.push(ev);
      bytes += size;
    }
    // Space may have freed — wake any producers blocked in enqueueAsync (backpressure).
    if (events.length > 0 && this._queue.length < this._maxQueueSize) this._releaseBackpressure();
    return events;
  }

  flush(): TelemetryBatch | null {
    if (this._shutdown) return null;
    const events = this.takeBatch();
    if (events.length === 0) return null;
    const batch: TelemetryBatch = {
      events,
      sessionId: this._sessionId,
      batchId: ++this._batchId,
    };
    this._totalSent += events.length;
    return batch;
  }

  /** Produce a batch at an auto-boundary and route it to onBatch, or retain it for drain(). */
  private _emit(): void {
    const batch = this.flush();
    if (!batch) return;
    if (this._onBatch) this._onBatch(batch);
    else this._ready.push(batch);
  }

  startAutoFlush(): void {
    if (this._timer) return;
    this._timer = setInterval(() => this._emit(), this._flushIntervalMs);
    if (this._timer.unref) this._timer.unref();
  }

  stopAutoFlush(): void {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }

  /**
   * Tear the sender down: stop the auto-flush timer and latch the shutdown guard so
   * any further enqueue()/flush() is rejected (mirrors an OTEL exporter's isShutdown
   * flag — telemetry emitted after teardown is never queued or sent). Idempotent.
   */
  shutdown(): void {
    this._shutdown = true;
    this.stopAutoFlush();
    // Release blocked producers so they observe the shutdown and resolve false
    // (never left hanging) — mirrors Claude's close() waking backpressure waiters.
    this._releaseBackpressure();
  }

  /**
   * Reconfigure the batch pipeline on a LIVE sender (Claude's
   * reinitialize1PEventLoggingIfConfigChanged parity) with event-loss-safe ordering:
   * (1) pause the auto-flush timer, (2) FORCE-FLUSH the current buffer so every
   * already-queued event is drained through onBatch / retained for drain() BEFORE the
   * swap (nothing buffered is lost), (3) apply the new batch params, (4) restart the
   * timer (with the new interval) only if it was running. A no-op after {@link shutdown}.
   * An omitted field is left unchanged; a non-positive flushIntervalMs is ignored.
   */
  reconfigure(opts: TelemetryReconfigureOpts): void {
    if (this._shutdown) return;
    const wasRunning = this._timer !== null;
    this.stopAutoFlush();
    // Loss-safe flush-before-swap: drain the buffered events first (each takeBatch
    // always removes >=1 event when the queue is non-empty, so this terminates).
    while (this._queue.length > 0) this._emit();
    if (opts.maxBatchSize !== undefined) this._maxBatchSize = Math.max(1, Math.floor(opts.maxBatchSize));
    if (opts.maxBatchBytes !== undefined) this._maxBatchBytes = opts.maxBatchBytes;
    if (opts.flushIntervalMs !== undefined && opts.flushIntervalMs > 0) this._flushIntervalMs = opts.flushIntervalMs;
    if (opts.maxQueueSize !== undefined) this._maxQueueSize = Math.max(1, Math.floor(opts.maxQueueSize));
    if (opts.immediateMode !== undefined) this._immediate = opts.immediateMode;
    this._reconfigureCount++;
    if (wasRunning) this.startAutoFlush();
  }

  /** Number of successful runtime {@link reconfigure} calls (introspection). */
  get reconfigureCount(): number { return this._reconfigureCount; }

  /** Whether this sender has been shut down (post-shutdown export guard). */
  get isShutdown(): boolean { return this._shutdown; }

  get queueSize(): number { return this._queue.length; }
  get totalSent(): number { return this._totalSent; }
  get totalDropped(): number { return this._totalDropped; }
  get batchId(): number { return this._batchId; }
  /** Number of auto-flushed batches currently awaiting drain() (no onBatch wired). */
  get readyCount(): number { return this._ready.length; }
  /**
   * Total events not yet drained: the live queue plus every event retained in
   * ready-but-undrained batches. Readable after {@link shutdown} (which leaves the
   * queue intact) so exit diagnostics can report un-exported depth. Mirrors Claude's
   * SerialBatchEventUploader.pendingCount.
   */
  get pendingCount(): number {
    return this._queue.length + this._ready.reduce((n, b) => n + b.events.length, 0);
  }

  drain(): TelemetryBatch[] {
    const batches: TelemetryBatch[] = this._ready;
    this._ready = [];
    let batch = this.flush();
    while (batch) {
      batches.push(batch);
      batch = this.flush();
    }
    this.stopAutoFlush();
    return batches;
  }

  /**
   * Re-queue previously-drained batches at the FRONT of the queue so a failed
   * export is retried on the next flush with original ordering preserved
   * (at-least-once delivery). Respects maxQueueSize, counting any overflow drop.
   */
  requeueFront(batches: TelemetryBatch[]): void {
    const events = batches.flatMap(b => b.events);
    if (events.length === 0) return;
    const capacity = Math.max(0, this._maxQueueSize - this._queue.length);
    const kept = events.slice(0, capacity);
    this._totalDropped += events.length - kept.length;
    this._queue.unshift(...kept);
  }
}

// ---------------------------------------------------------------------------
// OTLP (OpenTelemetry Protocol) log export over HTTP/JSON
// ---------------------------------------------------------------------------

/**
 * Max serialized bytes for a single OTLP attribute value — 60KB, headroom under
 * the common 64KB backend attribute limit (mirrors Claude's MAX_CONTENT_SIZE).
 */
export const MAX_OTLP_ATTR_BYTES = 60 * 1024;

/**
 * Cap a string at `maxSize` serialized bytes, appending a marker and reporting the
 * original length so a consumer knows the value was clipped (mirrors Claude's
 * truncateContent, which pairs the clipped value with `*_truncated`/`*_original_length`).
 */
export function truncateContent(
  content: string,
  maxSize = MAX_OTLP_ATTR_BYTES,
): { value: string; truncated: boolean; originalLength: number } {
  const originalLength = content.length;
  if (Buffer.byteLength(content, 'utf8') <= maxSize) {
    return { value: content, truncated: false, originalLength };
  }
  let sliced = content.slice(0, maxSize);
  while (sliced.length > 0 && Buffer.byteLength(sliced, 'utf8') > maxSize) sliced = sliced.slice(0, -1);
  return { value: `${sliced}…[TRUNCATED ${originalLength} chars]`, truncated: true, originalLength };
}

/** Per-attribute telemetry cardinality defaults (session.id ON, service.version ON). */
export const OTEL_CARDINALITY_DEFAULTS: Record<string, boolean> = {
  'session.id': true,
  'service.version': true,
};

const CARDINALITY_ENV_KEYS: Record<string, string> = {
  'session.id': 'AMI_OTEL_INCLUDE_SESSION_ID',
  'service.version': 'AMI_OTEL_INCLUDE_VERSION',
};

/**
 * Whether a cardinality-controlled resource attribute should be included: an
 * undefined env var uses the per-key default, a set env var is parsed truthy.
 * Lets an operator suppress high-cardinality dimensions (e.g. session.id) to
 * avoid metric-series explosion in their own collector.
 */
export function shouldIncludeTelemetryAttr(key: string, env: NodeJS.ProcessEnv = process.env): boolean {
  const envKey = CARDINALITY_ENV_KEYS[key];
  if (!envKey) return true;
  const raw = env[envKey];
  return raw === undefined ? (OTEL_CARDINALITY_DEFAULTS[key] ?? true) : isEnvTruthy(raw);
}

/** Build the OTLP resource attributes honoring the cardinality knobs + any extras. */
export function buildTelemetryResourceAttrs(
  sessionId: string,
  env: NodeJS.ProcessEnv = process.env,
  extra: Record<string, string> = {},
): Record<string, string> {
  const attrs: Record<string, string> = { 'service.name': AMI_CLIENT_NAME };
  if (shouldIncludeTelemetryAttr('service.version', env)) attrs['service.version'] = AMI_VERSION;
  if (shouldIncludeTelemetryAttr('session.id', env)) attrs['session.id'] = sessionId;
  // Provider-neutral environment dimensions so a self-hosted collector can
  // segment usage: host platform/arch/node version, terminal type (only when
  // resolved), and CI context (only under GitHub Actions).
  Object.assign(attrs, getEnvironmentFingerprint(env));
  const terminal = detectTerminalType(env);
  if (terminal) attrs['terminal.type'] = terminal;
  const ci = getCiContext(env);
  if (ci) Object.assign(attrs, ci);
  return { ...attrs, ...extra };
}

/**
 * Parse an OTEL_EXPORTER_OTLP_HEADERS-style string ("k1=v1,k2=v2") into a header
 * map. A value may itself contain '=' (API keys/tokens) — only the FIRST '=' is
 * the key/value separator, the remainder is preserved verbatim.
 */
export function parseOtelHeaders(raw: string | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  if (!raw) return out;
  for (const pair of raw.split(',')) {
    const trimmed = pair.trim();
    const eq = trimmed.indexOf('=');
    if (eq <= 0) continue;
    const key = trimmed.slice(0, eq).trim();
    if (key) out[key] = trimmed.slice(eq + 1).trim();
  }
  return out;
}

/**
 * Parse an OTEL_RESOURCE_ATTRIBUTES-style string ("k1=v1,k2=v2") into a
 * resource-attribute map. Shares the comma / first-'=' grammar with
 * {@link parseOtelHeaders} (W3C-Baggage-shaped): blanks and keyless pairs are
 * skipped, and a value may itself contain '=' (only the first '=' separates).
 * Lets an operator stamp deployment dimensions (e.g. `deployment.environment`,
 * `service.namespace`) onto every exported OTLP payload.
 */
export function parseOtelResourceAttrs(raw: string | undefined): Record<string, string> {
  return parseOtelHeaders(raw);
}

function otlpAttrs(obj: Record<string, unknown>): Array<{ key: string; value: { stringValue: string } }> {
  const out: Array<{ key: string; value: { stringValue: string } }> = [];
  for (const [key, v] of Object.entries(obj)) {
    const raw = typeof v === 'string' ? v : (JSON.stringify(v) ?? '');
    const t = truncateContent(raw);
    out.push({ key, value: { stringValue: t.value } });
    if (t.truncated) {
      out.push({ key: `${key}_truncated`, value: { stringValue: 'true' } });
      out.push({ key: `${key}_original_length`, value: { stringValue: String(t.originalLength) } });
    }
  }
  return out;
}

/** Convert a telemetry batch into an OTLP/JSON LogsData payload. */
export function toOtlpLogsPayload(
  batch: TelemetryBatch,
  resourceAttrs: Record<string, string> = {},
  env: NodeJS.ProcessEnv = process.env,
): Record<string, unknown> {
  return {
    resourceLogs: [{
      resource: {
        attributes: otlpAttrs(buildTelemetryResourceAttrs(batch.sessionId, env, resourceAttrs)),
      },
      scopeLogs: [{
        scope: { name: AMI_CLIENT_NAME, version: AMI_VERSION },
        logRecords: batch.events.map(e => {
          // Low-cardinality HTTP-status shaping (status -> http_status + *_range)
          // before the attributes are serialized for export.
          const data = deriveHttpStatusFields(e.data);
          return {
            timeUnixNano: String(e.timestamp * 1_000_000),
            severityText: 'INFO',
            body: { stringValue: e.type },
            // A session-monotonic sequence disambiguates same-millisecond events.
            attributes: otlpAttrs(e.sequence !== undefined ? { ...data, 'event.sequence': e.sequence } : data),
          };
        }),
      }],
    }],
  };
}

/**
 * Resolve the OTLP logs URL. A `perSignal` endpoint (sourced from the dedicated
 * `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`) is used VERBATIM — the OTEL spec says the
 * exporter must not append the `/v1/logs` signal path to a signal-specific endpoint,
 * so double-appending (e.g. `.../custom/logs/v1/logs`) is avoided. A generic endpoint
 * gets `/v1/logs` appended unless it already ends in an explicit `/v1/{signal}` path.
 */
export function otlpLogsUrl(endpoint: string, perSignal = false): string {
  return otlpSignalUrl(endpoint, 'logs', perSignal);
}

/**
 * Resolve the OTLP URL for ANY signal (logs / metrics / traces). A `perSignal`
 * endpoint (sourced from the dedicated `OTEL_EXPORTER_OTLP_{SIGNAL}_ENDPOINT`) is used
 * VERBATIM — the OTEL spec forbids appending the signal path to a signal-specific
 * endpoint. A generic endpoint gets `/v1/{signal}` appended unless it already ends in
 * an explicit `/v1/{logs|metrics|traces}` path. Single source of truth shared by the
 * logs, metrics, and traces exporters (no per-signal URL duplication).
 */
export function otlpSignalUrl(endpoint: string, signal: 'logs' | 'metrics' | 'traces', perSignal = false): string {
  if (perSignal) return endpoint; // signal-specific endpoint: used as-is (no /v1/{signal})
  const trimmed = endpoint.replace(/\/+$/, '');
  return /\/v1\/(logs|traces|metrics)$/.test(trimmed) ? trimmed : `${trimmed}/v1/${signal}`;
}

/** A resolved OTLP logs endpoint plus whether it was sourced signal-specifically. */
export interface OtlpLogsEndpoint {
  /** The endpoint URL (verbatim for a per-signal source, base otherwise). */
  url: string;
  /**
   * True when sourced from the dedicated `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` — then
   * the URL is used verbatim ({@link otlpLogsUrl} appends no `/v1/logs`). Mirrors
   * Claude's per-signal endpoint selection distinct from the generic endpoint.
   */
  perSignal: boolean;
}

/**
 * Resolve the OTLP LOGS endpoint honoring signal-specific selection (Claude parity):
 * the dedicated `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` wins and is marked `perSignal`
 * (used verbatim); otherwise the generic `OTEL_EXPORTER_OTLP_ENDPOINT` (then AMI's
 * `SUPERINFERENCE_OTLP_ENDPOINT`) is used and gets `/v1/logs` appended. Returns null
 * when no endpoint is configured. Blank/whitespace-only values are ignored.
 */
export function resolveOtlpLogsEndpoint(env: NodeJS.ProcessEnv = process.env): OtlpLogsEndpoint | null {
  return resolveOtlpSignalEndpoint('logs', env);
}

/**
 * Resolve the OTLP endpoint for ANY signal honoring signal-specific selection: the
 * dedicated `OTEL_EXPORTER_OTLP_{SIGNAL}_ENDPOINT` wins and is marked `perSignal`
 * (used verbatim); otherwise the generic `OTEL_EXPORTER_OTLP_ENDPOINT` (then AMI's
 * `SUPERINFERENCE_OTLP_ENDPOINT`) is used and gets `/v1/{signal}` appended. Returns
 * null when no endpoint is configured. Single source of truth shared by logs, metrics,
 * and traces resolution (no per-signal copy). Blank/whitespace-only values are ignored.
 */
export function resolveOtlpSignalEndpoint(
  signal: 'logs' | 'metrics' | 'traces',
  env: NodeJS.ProcessEnv = process.env,
): OtlpLogsEndpoint | null {
  const perSignal = env[`OTEL_EXPORTER_OTLP_${signal.toUpperCase()}_ENDPOINT`]?.trim();
  if (perSignal) return { url: perSignal, perSignal: true };
  const generic = (env.OTEL_EXPORTER_OTLP_ENDPOINT || env.SUPERINFERENCE_OTLP_ENDPOINT)?.trim();
  if (generic) return { url: generic, perSignal: false };
  return null;
}

// ---------------------------------------------------------------------------
// OTLP METRICS signal (Claude-Code instrumentation.ts L113-215 parity): AMI's
// logs-only OTLP export is extended with a metrics signal so the rich quantitative
// data the Profiler already computes (per-label durations, TTFT, p50/p95/p99,
// counters/gauges via its getAll() snapshot) can be bridged to any OTLP metrics
// backend over HTTP/JSON (AMI ships no @opentelemetry SDK). The aggregation
// temporality defaults to 'delta' — the saner default Claude picks — while remaining
// overridable via OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE.
// ---------------------------------------------------------------------------

/** OTLP aggregation-temporality enum values (proto: DELTA=1, CUMULATIVE=2). */
export const OTLP_TEMPORALITY = { delta: 1, cumulative: 2 } as const;
export type MetricsTemporality = keyof typeof OTLP_TEMPORALITY;

/**
 * Resolve the metrics aggregation-temporality preference, DEFAULTING TO 'delta' (the
 * saner default Claude's instrumentation.ts L113-116 sets) while honoring
 * `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE`. Only an explicit `cumulative`
 * selects cumulative; anything else (unset / unknown / blank) falls back to delta.
 */
export function resolveMetricsTemporalityPreference(env: NodeJS.ProcessEnv = process.env): MetricsTemporality {
  const raw = env.OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE?.trim().toLowerCase();
  return raw === 'cumulative' ? 'cumulative' : 'delta';
}

/**
 * A point-in-time numeric metrics snapshot split by instrument kind: `gauges` are
 * point-in-time values (percentiles, memory, current depth) and `counters` are
 * monotonic sums (event/tool/turn totals). Mirrors the shape of the Profiler's
 * getAll() output once a caller has classified each key.
 */
export interface MetricSnapshot {
  gauges?: Record<string, number>;
  counters?: Record<string, number>;
}

/**
 * Convert a numeric {@link MetricSnapshot} into an OTLP/JSON MetricsData payload:
 * gauges become `gauge` dataPoints and counters become monotonic `sum` dataPoints
 * carrying the resolved aggregation temporality ({@link resolveMetricsTemporalityPreference},
 * default delta). Non-finite values are skipped. The resource attributes mirror the
 * logs payload (same {@link buildTelemetryResourceAttrs}), so a collector segments
 * metrics and logs identically. Mirrors Claude's metrics signal, hand-built over
 * HTTP/JSON since AMI has no OTEL SDK dependency.
 */
export function toOtlpMetricsPayload(
  snapshot: MetricSnapshot,
  sessionId: string,
  resourceAttrs: Record<string, string> = {},
  env: NodeJS.ProcessEnv = process.env,
): Record<string, unknown> {
  const now = String(Date.now() * 1_000_000);
  const temporality = OTLP_TEMPORALITY[resolveMetricsTemporalityPreference(env)];
  const metrics: Array<Record<string, unknown>> = [];
  for (const [name, value] of Object.entries(snapshot.gauges ?? {})) {
    if (!Number.isFinite(value)) continue;
    metrics.push({ name, gauge: { dataPoints: [{ asDouble: value, timeUnixNano: now }] } });
  }
  for (const [name, value] of Object.entries(snapshot.counters ?? {})) {
    if (!Number.isFinite(value)) continue;
    metrics.push({
      name,
      sum: {
        dataPoints: [{ asDouble: value, timeUnixNano: now }],
        aggregationTemporality: temporality,
        isMonotonic: true,
      },
    });
  }
  return {
    resourceMetrics: [{
      resource: { attributes: otlpAttrs(buildTelemetryResourceAttrs(sessionId, env, resourceAttrs)) },
      scopeMetrics: [{
        scope: { name: AMI_CLIENT_NAME, version: AMI_VERSION },
        metrics,
      }],
    }],
  };
}

// ---------------------------------------------------------------------------
// OTLP TRACES signal (Claude-Code sessionTracing.ts L176-655 parity): AMI's Profiler
// already reconstructs a parent-child span tree (interaction -> llm_request / tool ->
// tool.execution / blocked_on_user / hook) via getSpanTree(); this bridges that tree
// to an OTLP/JSON TracesData payload so the nested interaction timeline renders in any
// OTLP trace backend (Jaeger, console exporter) as a navigable introspection surface.
// otlpLogsUrl already recognised a /v1/traces path; now there is a real traces body.
// ---------------------------------------------------------------------------

/** Deterministic 16-hex-char OTLP spanId from a profiler span id (stable across a run). */
function otlpSpanId(id: string): string {
  return crypto.createHash('sha256').update(id).digest('hex').slice(0, 16);
}

/** Deterministic 32-hex-char OTLP traceId from a seed (one trace per session export). */
function otlpTraceId(seed: string): string {
  return crypto.createHash('sha256').update(seed).digest('hex').slice(0, 32);
}

/**
 * Convert a Profiler span tree ({@link SpanNode}[] from `getSpanTree()`) into an
 * OTLP/JSON TracesData payload, preserving the parent-child hierarchy: each node
 * becomes a span with a deterministic spanId (and the parent's spanId as parentSpanId),
 * start/end nanos derived from its ms timings, and attributes carrying `span.type`,
 * `agent.id`, `duration_ms` (when closed) plus the node's own attrs. All spans share
 * one session-scoped traceId so the whole interaction renders as a single navigable
 * trace. Mirrors Claude's claude_code.interaction/llm_request/tool span emission.
 */
export function toOtlpTracePayload(
  spans: readonly SpanNode[],
  sessionId: string,
  resourceAttrs: Record<string, string> = {},
  env: NodeJS.ProcessEnv = process.env,
): Record<string, unknown> {
  const traceId = otlpTraceId(sessionId || AMI_CLIENT_NAME);
  const out: Array<Record<string, unknown>> = [];
  const walk = (node: SpanNode, parentSpanId?: string): void => {
    const spanId = otlpSpanId(node.id);
    const startNano = String(Math.round(node.startMs * 1_000_000));
    const endNano = node.endMs !== undefined ? String(Math.round(node.endMs * 1_000_000)) : startNano;
    const attrs: Record<string, unknown> = {
      'span.type': node.type,
      'agent.id': node.agentId,
      ...(node.parentAgentId !== undefined ? { 'parent.agent.id': node.parentAgentId } : {}),
      ...(node.durationMs !== undefined ? { duration_ms: node.durationMs } : {}),
      ...node.attrs,
    };
    out.push({
      traceId,
      spanId,
      ...(parentSpanId ? { parentSpanId } : {}),
      name: node.name,
      kind: 1, // SPAN_KIND_INTERNAL
      startTimeUnixNano: startNano,
      endTimeUnixNano: endNano,
      attributes: otlpAttrs(attrs),
    });
    for (const child of node.children) walk(child, spanId);
  };
  for (const root of spans) walk(root);
  return {
    resourceSpans: [{
      resource: { attributes: otlpAttrs(buildTelemetryResourceAttrs(sessionId, env, resourceAttrs)) },
      scopeSpans: [{
        scope: { name: AMI_CLIENT_NAME, version: AMI_VERSION },
        spans: out,
      }],
    }],
  };
}

// ---------------------------------------------------------------------------
// OTLP transport config: proxy (HTTP[S]_PROXY / NO_PROXY), mutual-TLS client
// certs, and a custom CA — mirroring Claude's getProxyUrl / shouldBypassProxy /
// HttpsProxyAgent + httpAgentOptions. The export still goes over the global
// fetch; a resolved undici dispatcher is attached to the request init so a
// self-hosted collector reachable only through a corporate proxy or presenting a
// private CA / requiring client certs can be exported to without touching the
// hot path. All resolution is pure/deterministic; the dispatcher is built lazily
// (undici only loaded when proxy/TLS is actually configured).
// ---------------------------------------------------------------------------

/** Client-TLS material for mutual-TLS / custom-CA OTLP export (PEM contents). */
export interface OtlpTlsConfig {
  /** Client certificate chain (PEM) for mutual TLS. */
  cert?: string;
  /** Client private key (PEM) for mutual TLS. */
  key?: string;
  /** Custom CA bundle (PEM) to trust the collector's server certificate. */
  ca?: string;
  /** Passphrase decrypting an encrypted client {@link key}. */
  passphrase?: string;
}

/** Minimal shape of the undici dispatcher constructors this module uses. */
interface UndiciDispatchers {
  ProxyAgent: new (opts: { uri: string; requestTls?: OtlpTlsConfig }) => unknown;
  Agent: new (opts: { connect?: OtlpTlsConfig }) => unknown;
}

let _undiciCache: UndiciDispatchers | undefined;
/** Lazily resolve undici's dispatcher constructors (bundled with Node; never imported unless proxy/TLS is configured). */
function loadUndici(): UndiciDispatchers {
  if (!_undiciCache) {
    const req = createRequire(import.meta.url);
    _undiciCache = req('undici') as UndiciDispatchers;
  }
  return _undiciCache;
}

/**
 * Whether `targetUrl` is exempted from proxying by a NO_PROXY-style list.
 * Supports `*` (bypass everything), exact host, and a leading-dot / `*.`-prefixed
 * domain suffix (`.example.com` / `*.example.com` match `api.example.com`). A port
 * in a NO_PROXY entry is ignored (host-level match, mirroring common runtimes).
 */
export function shouldBypassProxy(targetUrl: string, noProxy: string | undefined): boolean {
  if (!noProxy) return false;
  let host: string;
  try {
    host = new URL(targetUrl).hostname.toLowerCase();
  } catch {
    return false;
  }
  for (const rawEntry of noProxy.split(',')) {
    const entry = rawEntry.trim();
    if (!entry) continue;
    if (entry === '*') return true;
    const bare = entry.replace(/^\*?\./, '').replace(/:\d+$/, '').toLowerCase();
    if (!bare) continue;
    if (host === bare || host.endsWith('.' + bare)) return true;
  }
  return false;
}

/**
 * Resolve the proxy URL to use for an OTLP POST to `targetUrl`, or undefined when
 * no proxy applies. Precedence: an explicit `OTEL_EXPORTER_OTLP_PROXY`, else the
 * scheme-matched `HTTPS_PROXY`/`HTTP_PROXY` (lower-case accepted), else
 * `ALL_PROXY`. A `NO_PROXY` match (see {@link shouldBypassProxy}) suppresses it.
 */
export function resolveOtlpProxyUrl(targetUrl: string, env: NodeJS.ProcessEnv = process.env): string | undefined {
  let scheme: string;
  try {
    scheme = new URL(targetUrl).protocol;
  } catch {
    return undefined;
  }
  const schemeProxy = scheme === 'https:'
    ? (env.HTTPS_PROXY ?? env.https_proxy)
    : (env.HTTP_PROXY ?? env.http_proxy);
  const proxy = env.OTEL_EXPORTER_OTLP_PROXY ?? schemeProxy ?? env.ALL_PROXY ?? env.all_proxy;
  if (!proxy) return undefined;
  if (shouldBypassProxy(targetUrl, env.NO_PROXY ?? env.no_proxy)) return undefined;
  return proxy;
}

/**
 * Resolve client-TLS material from the OTLP env vars, reading each PEM file from
 * disk. Signal-specific `OTEL_EXPORTER_OTLP_LOGS_*` overrides the generic
 * `OTEL_EXPORTER_OTLP_*`; `OTEL_EXPORTER_OTLP_CERTIFICATE` is the CA bundle,
 * `*_CLIENT_CERTIFICATE`/`*_CLIENT_KEY` the mTLS pair, and
 * `AMI_OTEL_CLIENT_KEY_PASSPHRASE` the key passphrase. Returns undefined when
 * none are set; a file that cannot be read collapses the whole config to
 * undefined (best-effort telemetry never crashes the caller).
 */
export function resolveOtlpTlsConfig(
  env: NodeJS.ProcessEnv = process.env,
  readFile: (p: string) => string = (p) => fs.readFileSync(p, 'utf8'),
): OtlpTlsConfig | undefined {
  const pick = (base: string): string | undefined =>
    env[`OTEL_EXPORTER_OTLP_LOGS_${base}`] ?? env[`OTEL_EXPORTER_OTLP_${base}`];
  const certPath = pick('CLIENT_CERTIFICATE');
  const keyPath = pick('CLIENT_KEY');
  const caPath = pick('CERTIFICATE');
  const passphrase = env.AMI_OTEL_CLIENT_KEY_PASSPHRASE;
  if (!certPath && !keyPath && !caPath && !passphrase) return undefined;
  const cfg: OtlpTlsConfig = {};
  try {
    if (certPath) cfg.cert = readFile(certPath);
    if (keyPath) cfg.key = readFile(keyPath);
    if (caPath) cfg.ca = readFile(caPath);
  } catch {
    return undefined; // unreadable PEM → skip mTLS rather than crash the export
  }
  if (passphrase) cfg.passphrase = passphrase;
  return Object.keys(cfg).length > 0 ? cfg : undefined;
}

/**
 * Build an undici dispatcher for a proxied and/or mutually-authenticated OTLP
 * export, or undefined when neither a proxy nor TLS material is configured (the
 * common case — the global fetch default is used, undici never loaded). A proxy
 * yields a `ProxyAgent` (carrying any client-TLS as `requestTls` for the upstream
 * collector); TLS alone yields an `Agent` with the material on `connect`.
 */
export function buildOtlpDispatcher(
  cfg: { proxyUrl?: string; tls?: OtlpTlsConfig },
  loader: () => UndiciDispatchers = loadUndici,
): unknown | undefined {
  const { proxyUrl, tls } = cfg;
  if (!proxyUrl && !tls) return undefined;
  const undici = loader();
  if (proxyUrl) {
    return new undici.ProxyAgent({ uri: proxyUrl, ...(tls ? { requestTls: tls } : {}) });
  }
  return new undici.Agent({ connect: tls });
}

/**
 * Resolve the full OTLP transport (proxy + mTLS/CA) for `targetUrl` into an undici
 * dispatcher, or undefined when nothing is configured. Combines
 * {@link resolveOtlpProxyUrl} + {@link resolveOtlpTlsConfig} + {@link buildOtlpDispatcher};
 * `readFile`/`undiciLoader` are injectable for tests.
 */
export function resolveOtlpTransport(
  targetUrl: string,
  env: NodeJS.ProcessEnv = process.env,
  deps: { readFile?: (p: string) => string; undiciLoader?: () => UndiciDispatchers } = {},
): unknown | undefined {
  const proxyUrl = resolveOtlpProxyUrl(targetUrl, env);
  const tls = resolveOtlpTlsConfig(env, deps.readFile);
  return buildOtlpDispatcher({ proxyUrl, tls }, deps.undiciLoader);
}

/** Rich result of one OTLP POST attempt — drives retry/backoff decisions. */
export interface OtlpSendResult {
  ok: boolean;
  /** HTTP status if a response was received (undefined on network error/timeout). */
  status?: number;
  /** Whether a failed attempt is worth retrying (429/5xx/network) vs permanent (other 4xx). */
  retryable: boolean;
  /** Server-supplied Retry-After converted to ms, when present. */
  retryAfterMs?: number;
}

/** A failed send is retryable on network error/timeout, HTTP 429, or any 5xx. */
export function isRetryableStatus(status: number | undefined): boolean {
  if (status === undefined) return true; // network error / timeout
  if (status === 429) return true;
  return status >= 500 && status <= 599;
}

function parseRetryAfterMs(header: string | null | undefined): number | undefined {
  if (!header) return undefined;
  const secs = Number(header);
  if (Number.isFinite(secs) && secs >= 0) return Math.round(secs * 1000);
  const when = Date.parse(header);
  if (!Number.isNaN(when)) return Math.max(0, when - Date.now());
  return undefined;
}

/**
 * POST one telemetry batch to an OTLP/HTTP logs endpoint, returning a rich result
 * with retryability + Retry-After. Best-effort: never throws.
 */
export async function postOtlpBatch(
  endpoint: string,
  batch: TelemetryBatch,
  deps: { fetchImpl?: typeof fetch; timeoutMs?: number; headers?: Record<string, string>; resourceAttrs?: Record<string, string>; env?: NodeJS.ProcessEnv; dispatcher?: unknown; perSignal?: boolean } = {},
): Promise<OtlpSendResult> {
  const env = deps.env ?? process.env;
  // Extra resource attributes: an explicit map wins; otherwise fall back to
  // OTEL_RESOURCE_ATTRIBUTES so a direct caller honors the env knob too.
  const resourceAttrs = deps.resourceAttrs ?? parseOtelResourceAttrs(env.OTEL_RESOURCE_ATTRIBUTES);
  return postOtlpPayload(otlpLogsUrl(endpoint, deps.perSignal), toOtlpLogsPayload(batch, resourceAttrs, env), {
    fetchImpl: deps.fetchImpl, timeoutMs: deps.timeoutMs, headers: deps.headers, dispatcher: deps.dispatcher,
  });
}

/**
 * POST an already-built OTLP/JSON payload (logs, metrics, OR traces) to a resolved
 * signal URL, returning the rich retryable/Retry-After result. The single HTTP egress
 * primitive shared by all three signals (logs via {@link postOtlpBatch}, metrics via
 * {@link toOtlpMetricsPayload}, traces via {@link toOtlpTracePayload}) — one place that
 * attaches the proxy/mTLS dispatcher, bounds the request with an abort timeout, and
 * classifies the response. Best-effort: never throws (network error/timeout → retryable).
 */
export async function postOtlpPayload(
  url: string,
  payload: Record<string, unknown>,
  deps: { fetchImpl?: typeof fetch; timeoutMs?: number; headers?: Record<string, string>; dispatcher?: unknown } = {},
): Promise<OtlpSendResult> {
  const fetchImpl = deps.fetchImpl ?? (globalThis.fetch as typeof fetch | undefined);
  if (!fetchImpl) return { ok: false, retryable: false };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), deps.timeoutMs ?? 5000);
  try {
    // A proxy/mTLS dispatcher (undici) is attached to the request init so a
    // collector reachable only through a proxy or requiring client certs / a
    // private CA can still be exported to over the global fetch.
    const init: RequestInit = {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(deps.headers ?? {}) },
      body: JSON.stringify(payload),
      signal: controller.signal,
    };
    if (deps.dispatcher) (init as { dispatcher?: unknown }).dispatcher = deps.dispatcher;
    const res = await fetchImpl(url, init);
    if (res.ok) return { ok: true, status: res.status, retryable: false };
    const retryAfterMs = parseRetryAfterMs(res.headers?.get?.('retry-after'));
    return { ok: false, status: res.status, retryable: isRetryableStatus(res.status), retryAfterMs };
  } catch {
    return { ok: false, retryable: true }; // network error / timeout → retryable
  } finally {
    clearTimeout(timer);
  }
}

/** Boolean convenience over {@link postOtlpBatch}: true iff the POST got a 2xx. */
export async function sendOtlpBatch(
  endpoint: string,
  batch: TelemetryBatch,
  deps: { fetchImpl?: typeof fetch; timeoutMs?: number; headers?: Record<string, string>; resourceAttrs?: Record<string, string>; env?: NodeJS.ProcessEnv; dispatcher?: unknown; perSignal?: boolean } = {},
): Promise<boolean> {
  return (await postOtlpBatch(endpoint, batch, deps)).ok;
}

/** Max chars of the first output line kept verbatim in a tool summary line. */
export const TOOL_SUMMARY_LINE_MAX = 100;

export function generateToolSummary(
  toolResults: Array<{ toolName: string; output: string; isError: boolean }>,
): string {
  const summaries = toolResults.map(r => {
    const lines = r.output.split('\n');
    // Clip the first line and, when it was clipped, append a '…' so a truncated
    // summary is visually distinguishable from a complete one (mirrors Claude's
    // truncate.ts indicator) — otherwise an exactly-100-char line and a clipped
    // 100-char line would look identical.
    let shortOutput = lines[0].substring(0, TOOL_SUMMARY_LINE_MAX);
    if (lines[0].length > TOOL_SUMMARY_LINE_MAX) shortOutput += '…';
    let summary = `${r.toolName}: ${r.isError ? 'ERROR' : 'OK'} — ${shortOutput}`;
    // When the output spanned multiple lines, append a '(+N more lines)' marker so
    // a single-line summary of a large multi-line output is not mistaken for the
    // whole result.
    const moreLines = lines.length - 1;
    if (moreLines > 0) summary += ` (+${moreLines} more lines)`;
    return summary;
  });
  return summaries.join('\n');
}
