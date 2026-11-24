import * as fs from 'fs';
import 'os';
import * as path from 'path';
import * as crypto from 'crypto';
import { execSync } from 'child_process';
import { createRequire } from 'module';
import { getWorkload, hasWorkloadBoundary, runWithWorkload, WORKLOAD_CRON } from './workload-context';
import './tools/bash-sandbox';
import { AMI_CLIENT_NAME, AMI_VERSION } from './version';
import './credential-guard';
import { ndjsonSafeStringify } from './reference-journal';
import { createBufferedWriter, type BufferedWriter } from './buffered-writer';
import type { SpanNode } from './profiler';
import { sleep } from './utils/sleep';
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
import { BASH_TOKEN_SPLIT, MAX_IN_MEMORY_EVENTS, SINK_NAMES, filterEventForEgress, getFileExtensionForAnalytics, hashFilePath, isToolContentLoggingEnabled, markAnalyticsSafe, resolveBetaTracingConfig, resolveKilledSinks, resolveLocalOnlyFields, resolveSampleRate, sanitizeAnalyticsValue, sanitizeToolNameForAnalytics, shouldSampleEvent, stripLocalOnlyFields, toolDetailsEnabled, truncateToolInputValue, trySerialize } from './analytics-truncate-tool-input-value';
import type { AnalyticsEvent, AnalyticsSafeMetadata, BetaTracingConfig, EgressAllowlist, LocalOnlyFieldMatcher, SanitizeOpts, SinkName } from './analytics-truncate-tool-input-value';
import { CLOSE_GRACE_MS, TelemetryContentDedup, formatFlushTimeoutGuidance, isTelemetrySuppressedForTest, parseExporterTypes, raceWithTimeout, resolveExportIntervalMs, resolveFlushTimeoutMs, resolveOtelHeadersHelperCommand, resolveOtlpProtocol, validateOtelHeaders } from './analytics-telemetry-content-dedup';
import type { OtelHeadersHelperDeps, OtlpProtocol } from './analytics-telemetry-content-dedup';
import { CURL_EXPLICIT_POST_RE, CURL_POST_RE, GH_PR_ACTIONS, ProcessMetricsSampler, deriveHttpStatusFields, getLanguageForAnalytics, getOrCreateUserId, getUserBucket, gitSubcmdRe, normalizeModelForAnalytics, truncateDevVersion } from './analytics-detect-deployment-environment';
import type { FileOperationParams, GitOperationClassification, GitOperationName } from './analytics-detect-deployment-environment';
import { FailedBatchStore, IncrementalContextTracker, PR_ENDPOINT_RE, createTelemetryDiagLogger, resolveFileFlushIntervalMs } from './analytics-failed-batch-store';
import type { IncrementalContextDelta, TelemetryBatch, TelemetryDiagLogger, TelemetryReconfigureOpts } from './analytics-failed-batch-store';
import { OTLP_TEMPORALITY, TelemetryBatchSender, buildTelemetryResourceAttrs, otlpLogsUrl, otlpSignalUrl, parseOtelHeaders, parseOtelResourceAttrs, resolveMetricsTemporalityPreference, resolveOtlpSignalEndpoint } from './analytics-telemetry-batch-sender';
import { otlpSpanId, otlpTraceId, postOtlpPayload, resolveOtlpTlsConfig } from './analytics-analytics-tracker-opts';
import type { AnalyticsTrackerOpts, MetricSnapshot, OtlpSendResult, OtlpTlsConfig, UndiciDispatchers } from './analytics-analytics-tracker-opts';
export { FILE_PATH_HASH_LEN, MAX_ANALYTICS_STRING_LEN, MAX_FILE_EXTENSION_LENGTH, SINK_NAMES, TOOL_INPUT_MAX_COLLECTION_ITEMS, TOOL_INPUT_MAX_DEPTH, TOOL_INPUT_STRING_TRUNCATE_AT, TOOL_INPUT_STRING_TRUNCATE_TO, filterEventForEgress, getFileExtensionForAnalytics, hashFilePath, isToolContentLoggingEnabled, markAnalyticsSafe, resolveBetaTracingConfig, resolveKilledSinks, resolveLocalOnlyFields, resolveSampleRate, sanitizeAgentName, sanitizeAnalyticsValue, sanitizeToolNameForAnalytics, shouldSampleEvent, stripLocalOnlyFields, truncateToolInputValue, trySerialize } from './analytics-truncate-tool-input-value';
export type { AnalyticsEvent, AnalyticsSafeMetadata, BetaTracingConfig, EgressAllowlist, LocalOnlyFieldMatcher, SanitizeOpts, SinkName } from './analytics-truncate-tool-input-value';
export { CLOSE_GRACE_MS, DEDUP_MIN_CONTENT_LEN, DEDUP_PREVIEW_LEN, DEFAULT_EXPORT_INTERVALS, DEFAULT_FLUSH_TIMEOUT_MS, HOST_PLATFORM_OVERRIDES, OTEL_HEADERS_HELPER_ENV_VARS, OTLP_PROTOCOLS, TelemetryContentDedup, detectTerminalType, detectWslForAnalytics, formatFlushTimeoutGuidance, getEnvironmentFingerprint, getHostPlatformForAnalytics, isTelemetrySuppressedForTest, parseExporterTypes, raceWithTimeout, resolveExportIntervalMs, resolveFlushTimeoutMs, resolveOtelHeadersHelperCommand, resolveOtlpProtocol, validateOtelHeaders } from './analytics-telemetry-content-dedup';
export type { OtelHeadersHelperDeps, OtlpProtocol, ProcessMetrics, ProcessMetricsDeps } from './analytics-telemetry-content-dedup';
export { FILE_OPERATION_KINDS, FILE_OPERATION_TOOLS, GIT_OPERATION_NAMES, KNOWN_ANALYTICS_MODEL_FAMILIES, NUM_USER_BUCKETS, ProcessMetricsSampler, USER_ID_FILE, bucketHttpStatus, deriveHttpStatusFields, detectDeploymentEnvironment, getCiContext, getLanguageForAnalytics, getOrCreateUserId, getUserBucket, isCI, normalizeModelForAnalytics, truncateDevVersion } from './analytics-detect-deployment-environment';
export type { DeploymentEnvDeps, FileOperationKind, FileOperationParams, FileOperationTool, GitOperationClassification, GitOperationName } from './analytics-detect-deployment-environment';
export { FAILED_BATCH_FILE_PREFIX, FailedBatchStore, IncrementalContextTracker, TELEMETRY_DIAG_LEVELS, createTelemetryDiagLogger, resolveFileFlushIntervalMs } from './analytics-failed-batch-store';
export type { IncrementalContextDelta, TelemetryBatch, TelemetryBatchSenderOpts, TelemetryDiagLevel, TelemetryDiagLogger, TelemetryDiagLoggerDeps, TelemetryReconfigureOpts } from './analytics-failed-batch-store';
export { OTEL_CARDINALITY_DEFAULTS, OTLP_TEMPORALITY, TelemetryBatchSender, buildTelemetryResourceAttrs, otlpLogsUrl, otlpSignalUrl, parseOtelHeaders, parseOtelResourceAttrs, resolveMetricsTemporalityPreference, resolveOtlpLogsEndpoint, resolveOtlpSignalEndpoint, shouldIncludeTelemetryAttr } from './analytics-telemetry-batch-sender';
export type { MetricsTemporality, OtlpLogsEndpoint } from './analytics-telemetry-batch-sender';
export { TOOL_SUMMARY_LINE_MAX, generateToolSummary, isRetryableStatus, postOtlpPayload, resolveOtlpTlsConfig } from './analytics-analytics-tracker-opts';
export type { AnalyticsTrackerOpts, MetricSnapshot, OtlpSendResult, OtlpTlsConfig } from './analytics-analytics-tracker-opts';

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

/**
 * Default in-memory event TTL (1h) applied when age-eviction is enabled via a truthy
 * non-numeric `AMI_ANALYTICS_EVENT_TTL_MS`. Mirrors the reference's SPAN_TTL_MS orphan sweep:
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

const TOOL_NAME_KEYS = new Set(['tool', 'toolname', 'tool_name', 'name']);
/**
 * Keys whose VALUE is a raw filesystem path — replaced on egress with a truncated
 * SHA-256 so a self-hosted collector can still correlate/dedup by file (same path →
 * same hash) without the path itself (which can embed project names, usernames, PII)
 * ever leaving the machine. Mirrors the reference's fileOperationAnalytics hashFilePath.
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
/** Hard cap (chars) on the final serialized tool-input JSON. */
export const TOOL_INPUT_MAX_JSON_CHARS = 4 * 1024;

/**
 * Serialize a tool's input arguments to a bounded JSON string for telemetry,
 * returning undefined unless tool-detail logging is enabled (OTEL_LOG_TOOL_DETAILS /
 * AMI_LOG_TOOL_DETAILS). The value is first bounded-serialized via
 * {@link truncateToolInputValue} then hard-capped at {@link TOOL_INPUT_MAX_JSON_CHARS}.
 * Mirrors the reference's `extractToolInputForTelemetry`.
 */
export function extractToolInputForTelemetry(input: unknown, env: NodeJS.ProcessEnv = process.env): string | undefined {
  if (!toolDetailsEnabled(env)) return undefined;
  const truncated = truncateToolInputValue(input);
  let json = trySerialize(truncated) ?? 'null';
  if (json.length > TOOL_INPUT_MAX_JSON_CHARS) json = json.slice(0, TOOL_INPUT_MAX_JSON_CHARS) + '…[truncated]';
  return json;
}
/** Commands whose arguments are file paths (so their extensions are analytics-safe signal). */
const FILE_COMMANDS = new Set([
  'rm', 'mv', 'cp', 'touch', 'cat', 'grep', 'rg', 'sed', 'awk', 'head', 'tail',
  'less', 'more', 'nano', 'vim', 'vi', 'code', 'wc', 'diff', 'chmod', 'chown',
]);

/**
 * Extract the set of distinct file extensions referenced by file-oriented commands
 * in a bash string — file-TYPE usage telemetry without logging raw paths. Walks
 * tokens, and after a known file command collects extensions of subsequent
 * non-flag arguments until the next command boundary (mirrors the reference's
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
 * Max content bytes hashed by {@link hashFileContent} — above this the content is
 * not hashed (returns undefined) to avoid memory blow-ups on huge/base64 blobs
 * (mirrors the reference's MAX_CONTENT_HASH_SIZE = 100KB).
 */
export const MAX_CONTENT_HASH_SIZE = 100 * 1024;

/**
 * Full (64-char) SHA-256 of file CONTENT for change-detection/dedup analytics, or
 * undefined when the content exceeds {@link MAX_CONTENT_HASH_SIZE} (size-guarded to
 * avoid hashing huge/base64 blobs). Mirrors the reference's `hashFileContent`.
 */
export function hashFileContent(content: string): string | undefined {
  if (content.length > MAX_CONTENT_HASH_SIZE) return undefined;
  return crypto.createHash('sha256').update(content).digest('hex');
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
  // possible without the path ever leaving the machine (the reference permissionLogging parity).
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
  // Cardinality-reduction normalization (the reference datadog.ts parity), applied just
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

// ---------------------------------------------------------------------------
// otelHeadersHelper (the reference auth.ts parity): OTLP auth headers produced
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

/**
 * Resolve OTLP auth headers from a user-configured helper command whose stdout is a
 * JSON object of string→string header pairs (enterprise OTLP backends often need a
 * freshly-minted bearer token). The parsed result is cached and reused until the
 * debounce window ({@link resolveOtelHeadersDebounceMs}, default 29 min) elapses, so
 * the (potentially expensive) auth call is not made per export. Mirrors the reference's
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

/**
 * Default shutdown-drain budget (ms) the graceful close may block for. AMI keeps its
 * own deliberate {@link CLOSE_GRACE_MS} value as the default (additive superset — a
 * knob is ADDED, AMI's default is not regressed to the reference's 2000ms).
 */
export const DEFAULT_SHUTDOWN_TIMEOUT_MS = CLOSE_GRACE_MS;

/**
 * Resolve the graceful-shutdown drain budget (ms), honoring `AMI_OTEL_SHUTDOWN_TIMEOUT_MS`
 * then the Claude-compatible `CLAUDE_CODE_OTEL_SHUTDOWN_TIMEOUT_MS`, defaulting to
 * {@link DEFAULT_SHUTDOWN_TIMEOUT_MS}. This is SEPARATE from the per-flush budget
 * ({@link resolveFlushTimeoutMs}): shutdown bounds how long the FINAL close drain may
 * block on a slow collector, independent of the in-session flush bound (mirrors
 * the reference's distinct shutdown-vs-flush timeout knobs). A literal `0` is honored (do not
 * block on close); a non-finite/negative value falls back to the default.
 */
export function resolveShutdownTimeoutMs(env: NodeJS.ProcessEnv = process.env): number {
  const raw = env.AMI_OTEL_SHUTDOWN_TIMEOUT_MS ?? env.CLAUDE_CODE_OTEL_SHUTDOWN_TIMEOUT_MS;
  const n = raw === undefined ? NaN : Number(raw);
  return Number.isFinite(n) && n >= 0 ? Math.floor(n) : DEFAULT_SHUTDOWN_TIMEOUT_MS;
}

// ---------------------------------------------------------------------------
// Local git identity (the reference utils/user.ts getGitEmail parity): the
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
 * (reject-free, like the reference's memoized getGitEmail). Memoized per cwd. The command
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

// ---------------------------------------------------------------------------
// Code-editing-tool language segmentation (the reference permissionLogging.ts parity):
// for code-editing tools (Edit/Write/NotebookEdit → AMI file_edit/notebook_edit) the
// target file's programming LANGUAGE is derived from its extension and attached as a
// low-cardinality dimension, so edit telemetry can be segmented by language without
// ever exporting the raw path. Mirrors buildCodeEditToolAttributes (L41-65).
// ---------------------------------------------------------------------------

/** Tool names treated as code-editing (AMI canonical + the reference aliases). */
export const CODE_EDITING_TOOLS: ReadonlySet<string> = new Set([
  'file_edit', 'notebook_edit',          // AMI canonical
  'Edit', 'Write', 'MultiEdit', 'NotebookEdit', // the reference names
]);

/** Whether a tool name is a code-editing tool (edit/write/notebook family). */
export function isCodeEditingTool(toolName: string): boolean {
  return CODE_EDITING_TOOLS.has(toolName);
}

/**
 * When an event records a code-editing tool (via a tool-name field) AND a target
 * file path (via a path field), attach a low-cardinality `file_language` dimension
 * derived from the path's extension — WITHOUT consuming the raw path (which is still
 * hashed downstream). Returns the original object when no transform applies (not a
 * code-edit tool, no path, or `file_language` already present). Mirrors the reference's
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
// Invocation-source vocabulary (the reference workloadContext.ts parity): a fixed,
// low-cardinality set of source tags so the usage view / analytics can segment a
// turn by what triggered it. AMI already has interactive/detached + a per-turn
// source field; the `cron` value lets a scheduler-fired turn be distinguished
// from an interactive/detached one (the reference's WORKLOAD_CRON).
// ---------------------------------------------------------------------------

/**
 * Invocation-source tag for a turn fired by the cron scheduler. Aliases the
 * canonical {@link WORKLOAD_CRON} from `workload-context` so there is ONE source
 * of truth for the `cron` origin tag across analytics and the ALS primitive.
 */
export const ANALYTICS_SOURCE_CRON = WORKLOAD_CRON;

/**
 * Build the privacy-preserving metadata for a `file_operation` analytics event:
 * the operation + tool enums, a truncated SHA-256 `file_path_hash`, an optional
 * full `content_hash` (only when content is present and not too large — the
 * {@link hashFileContent} size guard returns undefined above {@link MAX_CONTENT_HASH_SIZE},
 * preventing memory blow-ups on base64 blobs), and an optional create/update `type`.
 * Mirrors the reference's logFileOperation metadata shape.
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

const GIT_COMMIT_RE = gitSubcmdRe('commit');
const GIT_PUSH_RE = gitSubcmdRe('push');

/**
 * Classify a (successful) command string into local commit/PR operations + the
 * counter deltas that feed the usage view. A non-zero `exitCode` yields an empty
 * classification (only successful operations are counted). `git commit` counts a
 * commit (and additionally emits `commit_amend` for `--amend`); `git push` emits
 * `push`; each `gh pr <action>` / `glab mr create` / curl-POST-to-a-PR-endpoint
 * emits the relevant op and (for creates) increments the PR counter. Mirrors
 * the reference's trackGitOperations detection, including the curl PR-create branch.
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

export class AnalyticsTracker {
  private events: AnalyticsEvent[] = [];
  /** Per-event record time (ms, via {@link _now}) parallel to {@link events}, for the age sweep. */
  private _eventTimes: number[] = [];
  private logFile: string | null;
  private dirInitialized = false;
  private _sessionId: string | undefined;
  /** Per-prompt correlation id stamped onto every EVENT (not resource/metrics — the reference's prompt.id). */
  private _promptId: string | undefined;
  /** Local commit counter feeding the usage view (the reference's getCommitCounter). */
  private _commitCount = 0;
  /** Local PR counter feeding the usage view (the reference's getPrCounter). */
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
  /** Re-entrancy guard for persisted-batch recovery (the reference's `isRetrying` parity). */
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
    // Best-effort telemetry delay: route through the shared sleep with
    // `{ unref: true }` so a pending inter-batch delay or graceful-close drain
    // timer never holds the process open at shutdown (parity: the reference's unref'd
    // grace sleep). Analytics is strictly best-effort — droppable on exit.
    this._sleep = opts?.sleepImpl ?? ((ms) => sleep(ms, undefined, { unref: true }));
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
    // otelHeadersHelper: an explicit opt wins; otherwise resolve the command from the
    // environment (AMI_OTEL_HEADERS_HELPER / CLAUDE_CODE_OTEL_HEADERS_HELPER) so the
    // knob is reachable in production — the engine builds this tracker with the real
    // process env, so setting the var activates dynamic OTLP auth-header minting without
    // any caller having to thread an option through. The opt's exec/now injection is
    // preserved for tests; env-sourced helpers use the default runner.
    const helperCommand = opts?.otlpHeadersHelper?.command ?? resolveOtelHeadersHelperCommand(this._env);
    this._headersHelper = helperCommand
      ? new OtelHeadersHelper(helperCommand, {
          exec: opts?.otlpHeadersHelper?.exec,
          now: opts?.otlpHeadersHelper?.now,
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
   * noisy/leaky sink without reconstructing the tracker (the reference per-sink killswitch).
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
   * dimension). Mirrors the reference's per-turn getPromptId(). */
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
   * other. Mirrors the reference's logFileOperation. */
  logFileOperation(params: FileOperationParams): void {
    this.log('file_operation', buildFileOperationEvent(params));
  }

  /**
   * Classify a finished command for local git/PR usage ({@link classifyGitOperation}),
   * increment the commit/PR counters, and emit one `git_operation` event per matched
   * operation. A non-zero exit or a non-git/PR command is a no-op. The counters feed
   * the usage view (the reference's getCommitCounter/getPrCounter parity). */
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
   * under an explicit opt-in. Mirrors the reference's betaSessionTracing new_context tracking. */
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
   * ambient context can never bleed in (the reference workloadContext parity). Returns `fn`'s
   * return value (including a Promise, whose continuation keeps the context). Prefer
   * this over {@link pushSource}/{@link popSource} for void-detached background work
   * (cron, forked slash commands, subagents) whose continuation outlives the caller.
   */
  runWithSource<T>(source: string | undefined, fn: () => T): T {
    return runWithWorkload(source, fn);
  }

  currentSource(): string | undefined {
    // The shared workload ALS boundary (survives awaits, isolated per chain) always
    // wins — even when it was established with an undefined workload (so a fresh
    // `runWithSource(undefined, …)` shadows a leaked ambient stack rather than falling
    // through to it). Only with NO ALS boundary at all do we consult the synchronous
    // push/pop stack.
    if (hasWorkloadBoundary()) return getWorkload();
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

    // Workload-origin attribution: when the event is logged inside an ALS workload
    // boundary (a cron fire, forked slash command, or sub-agent run established via
    // {@link runWithWorkload}) and the caller hasn't set an explicit `source`, stamp
    // the current workload so scheduler/detached-origin turns are attributable in the
    // usage view without every call site threading it. Gated on an ALS boundary only
    // (never the synchronous push/pop stack), so stack-based event shapes are unchanged.
    if (!('source' in recordData) && hasWorkloadBoundary()) {
      const workload = getWorkload();
      if (workload !== undefined) recordData = { ...recordData, source: workload };
    }

    // Optional per-event process metrics (memory + CPU% delta since the last event).
    if (this._processMetrics) {
      const pm = this._processMetrics.sample();
      if (pm) recordData = { ...recordData, process: pm };
    }

    // Per-prompt correlation id: stamped onto every EVENT so a session's events can
    // be grouped to the specific prompt/turn that produced them. Deliberately an
    // event-level dimension (never a resource/metric one, where it would be unbounded
    // cardinality) — mirrors the reference's getPromptId() `prompt.id` attribute. An explicit
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

    // Egress allowlist (the reference datadog double gate): drop the whole event from every
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
    // of silently losing telemetry (mirrors the reference's hasWarnedNoEventLogger latch).
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
      // engine's hot loop — analytics is always best-effort (mirrors the reference's
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
   * Mirrors the reference's SerialBatchEventUploader.pendingCount.
   */
  getPendingTelemetryCount(): number {
    return this._telemetry?.pendingCount ?? 0;
  }

  /**
   * Snapshot of the batch-telemetry transport's lifetime counters for introspection
   * (surfaced by `/analytics`). the reference frames droppedBatchCount/pendingCount as
   * explicit introspection points ("callers can snapshot before flush() and compare
   * after to detect silent drops"); this exposes the full set so a user-facing
   * diagnostic can report queue depth + silent telemetry loss rather than it being
   * invisible. `active` is false when no OTLP telemetry transport was ever started
   * (nothing to report); `droppedEvents` is the source-gated per-event drop count
   * (distinct from the transport's batch-level `totalDropped`).
   */
  getTelemetrySnapshot(): {
    active: boolean;
    totalSent: number;
    totalDropped: number;
    queueSize: number;
    pendingCount: number;
    batchId: number;
    droppedEvents: number;
  } {
    const t = this._telemetry;
    return {
      active: t !== null,
      totalSent: t?.totalSent ?? 0,
      totalDropped: t?.totalDropped ?? 0,
      queueSize: t?.queueSize ?? 0,
      pendingCount: t?.pendingCount ?? 0,
      batchId: t?.batchId ?? 0,
      droppedEvents: this._droppedEvents,
    };
  }

  /**
   * Bounded graceful close: give the final {@link flushOtlp} a best-effort `graceMs`
   * window (racing it against a timeout) before latching shutdown, so a slow/hung
   * collector can never block exit beyond the grace period. Mirrors the reference's
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
   * post-shutdown export guard mirroring the reference's exporter `isShutdown` flag. Safe
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
        // flush/backoff tick (the reference's "success means healthy, retry queued" parity).
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
    // `_maxAttempts` is clamped to ≥1 at construction, and every iteration returns
    // (ok / permanent drop / final attempt), so a post-loop fallthrough was unreachable.
    let attempt = 1;
    for (;;) {
      const res = await postOtlpBatch(this._otlpEndpoint!, batch, { fetchImpl: this._otlpFetch, env: this._env, headers, resourceAttrs, timeoutMs: this._otlpTimeoutMs, dispatcher: this._otlpDispatcher, perSignal: this._otlpPerSignal });
      if (res.ok) return true;
      if (!res.retryable) return this._dropBatch(batch); // 4xx: permanent, drop
      if (attempt >= this._maxAttempts) return this._dropBatch(batch);
      const base = Math.min(this._maxBackoffMs, this._baseBackoffMs * attempt * attempt);
      const jittered = res.retryAfterMs ?? Math.round(base * (0.5 + this._rand() * 0.5));
      if (jittered > 0) await this._sleep(jittered);
      attempt++;
    }
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
    // The metrics and traces exports hit different endpoints and share no mutable
    // state, yet each can block for seconds of retry/backoff against a slow collector.
    // Run them concurrently so the flush/shutdown drain pays max(metrics, traces)
    // instead of the sum. Each source is evaluated synchronously in call order (as
    // before) and each export is independently exception-isolated. Only await when a
    // source is actually wired so the no-source case stays tick-for-tick as it was
    // (an extra awaited microtask would let an instant grace timeout pre-empt the
    // sibling logs flush).
    const tasks: Promise<void>[] = [];
    if (this._metricsSource) {
      tasks.push((async () => { try { await this.exportMetrics(this._metricsSource!()); } catch { /* best effort */ } })());
    }
    if (this._traceSource) {
      tasks.push((async () => { try { await this.exportTrace(this._traceSource!()); } catch { /* best effort */ } })());
    }
    if (tasks.length > 0) await Promise.all(tasks);
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
    // Same invariant as `_sendWithRetry`: every iteration returns, so a post-loop
    // `return false` was unreachable dead code.
    let attempt = 1;
    for (;;) {
      const res = await postOtlpPayload(url, payload, {
        fetchImpl: this._otlpFetch, headers, timeoutMs: this._otlpTimeoutMs, dispatcher: this._otlpDispatcher,
      });
      if (res.ok) return true;
      if (!res.retryable || attempt >= this._maxAttempts) return false;
      const base = Math.min(this._maxBackoffMs, this._baseBackoffMs * attempt * attempt);
      const jittered = res.retryAfterMs ?? Math.round(base * (0.5 + this._rand() * 0.5));
      if (jittered > 0) await this._sleep(jittered);
      attempt++;
    }
  }

  /**
   * Reconfigure the live telemetry batch pipeline at runtime (event-loss-safe: the
   * current buffer is flushed before the param swap — see
   * {@link TelemetryBatchSender.reconfigure}) AND keep the tracker's own resolved
   * cadence/batch knobs in sync so a later lazily-created sender inherits the new values.
   * Mirrors the reference's reinitialize1PEventLoggingIfConfigChanged.
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
    // Re-entrancy guard (the reference's `isRetrying`): a recovery triggered by a successful
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

// ---------------------------------------------------------------------------
// OTLP (OpenTelemetry Protocol) log export over HTTP/JSON
// ---------------------------------------------------------------------------

/**
 * Max serialized bytes for a single OTLP attribute value — 60KB, headroom under
 * the common 64KB backend attribute limit (mirrors the reference's MAX_CONTENT_SIZE).
 */
export const MAX_OTLP_ATTR_BYTES = 60 * 1024;

/**
 * Cap a string at `maxSize` serialized bytes, appending a marker and reporting the
 * original length so a consumer knows the value was clipped (mirrors the reference's
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
  // Binary-search the largest character count whose UTF-8 byte length stays <=
  // maxSize (byteLength is monotonic in slice length), instead of trimming one char
  // at a time while re-measuring the whole slice each step — the old loop was O(k·n)
  // for multi-byte content overshooting the cap. Result is a byte-identical cut.
  let lo = 0;
  let hi = Math.min(content.length, maxSize);
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (Buffer.byteLength(content.slice(0, mid), 'utf8') <= maxSize) lo = mid;
    else hi = mid - 1;
  }
  const sliced = content.slice(0, lo);
  return { value: `${sliced}…[TRUNCATED ${originalLength} chars]`, truncated: true, originalLength };
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
 * Convert a numeric {@link MetricSnapshot} into an OTLP/JSON MetricsData payload:
 * gauges become `gauge` dataPoints and counters become monotonic `sum` dataPoints
 * carrying the resolved aggregation temporality ({@link resolveMetricsTemporalityPreference},
 * default delta). Non-finite values are skipped. The resource attributes mirror the
 * logs payload (same {@link buildTelemetryResourceAttrs}), so a collector segments
 * metrics and logs identically. Mirrors the reference's metrics signal, hand-built over
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

/**
 * Convert a Profiler span tree ({@link SpanNode}[] from `getSpanTree()`) into an
 * OTLP/JSON TracesData payload, preserving the parent-child hierarchy: each node
 * becomes a span with a deterministic spanId (and the parent's spanId as parentSpanId),
 * start/end nanos derived from its ms timings, and attributes carrying `span.type`,
 * `agent.id`, `duration_ms` (when closed) plus the node's own attrs. All spans share
 * one session-scoped traceId so the whole interaction renders as a single navigable
 * trace. Mirrors the reference's claude_code.interaction/llm_request/tool span emission.
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

/** Boolean convenience over {@link postOtlpBatch}: true iff the POST got a 2xx. */
export async function sendOtlpBatch(
  endpoint: string,
  batch: TelemetryBatch,
  deps: { fetchImpl?: typeof fetch; timeoutMs?: number; headers?: Record<string, string>; resourceAttrs?: Record<string, string>; env?: NodeJS.ProcessEnv; dispatcher?: unknown; perSignal?: boolean } = {},
): Promise<boolean> {
  return (await postOtlpBatch(endpoint, batch, deps)).ok;
}
