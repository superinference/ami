import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { AMI_CLIENT_NAME, AMI_VERSION } from './version';
import { redactCredentials } from './credential-guard';
import { ndjsonSafeStringify } from './reference-journal';
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

// ---------------------------------------------------------------------------
// Redaction / data-minimization for analytics event payloads
// ---------------------------------------------------------------------------

/** String values longer than this are truncated (with a marker) before egress. */
export const MAX_ANALYTICS_STRING_LEN = 2048;
/** Bounded recursion depth when sanitizing nested tool-input-like values. */
const MAX_SANITIZE_DEPTH = 6;

const TOOL_NAME_KEYS = new Set(['tool', 'toolname', 'tool_name', 'name']);
/** Keys that may carry raw code / prompt / output text (redacted to length-only by default). */
const SENSITIVE_CONTENT_KEYS = new Set([
  'prompt', 'user_prompt', 'message', 'content', 'output', 'system_prompt', 'text', 'input', 'result',
]);

function toolDetailsEnabled(env: NodeJS.ProcessEnv): boolean {
  return isEnvTruthy(env.AMI_LOG_TOOL_DETAILS) || isEnvTruthy(env.OTEL_LOG_TOOL_DETAILS);
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

function redactString(s: string): string {
  const redacted = redactCredentials(s);
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
  /** When false (default), free-text content keys are redacted to length-only. */
  logUserPrompts?: boolean;
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
 *  - free-text content keys are redacted to `<REDACTED>` + a `<key>_length` unless
 *    OTEL_LOG_USER_PROMPTS / SUPERINFERENCE_LOG_USER_PROMPTS opts in
 *  - all remaining string values are credential-redacted and length-capped
 */
export function sanitizeAnalyticsData(data: Record<string, unknown>, opts: SanitizeOpts = {}): AnalyticsSafeMetadata {
  const env = opts.env ?? process.env;
  const logUserPrompts = opts.logUserPrompts ?? (isEnvTruthy(env.OTEL_LOG_USER_PROMPTS) || isEnvTruthy(env.SUPERINFERENCE_LOG_USER_PROMPTS));
  const logToolDetails = opts.logToolDetails ?? toolDetailsEnabled(env);
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(data)) {
    if (key.startsWith('_')) continue;
    const lk = key.toLowerCase();
    if (TOOL_NAME_KEYS.has(lk) && typeof value === 'string') {
      out[key] = sanitizeToolNameForAnalytics(value, { logToolDetails });
      continue;
    }
    if (SENSITIVE_CONTENT_KEYS.has(lk) && !logUserPrompts) {
      const len = typeof value === 'string' ? value.length : JSON.stringify(value ?? '').length;
      out[key] = '<REDACTED>';
      out[`${key}_length`] = len;
      continue;
    }
    out[key] = sanitizeAnalyticsValue(value, 0);
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
 * Vendor-neutral environment fingerprint for telemetry: host platform (honoring
 * the {@link getHostPlatformForAnalytics} override), CPU arch, and Node version.
 * Attached to every OTLP resource so a collector can segment by environment.
 */
export function getEnvironmentFingerprint(
  env: NodeJS.ProcessEnv = process.env,
  info: { platform?: NodeJS.Platform; arch?: string; nodeVersion?: string } = {},
): Record<string, string> {
  return {
    'host.platform': getHostPlatformForAnalytics(env, info.platform ?? process.platform),
    'host.arch': info.arch ?? process.arch,
    'node.version': info.nodeVersion ?? process.version,
  };
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
   * Sink for the `console` exporter type (OTEL_LOGS_EXPORTER=console): invoked with a
   * one-line JSON dump of each event's redacted payload for local introspection with
   * no collector. Defaults to writing a line to process.stdout.
   */
  consoleSink?: (line: string) => void;
}

export class AnalyticsTracker {
  private events: AnalyticsEvent[] = [];
  private logFile: string | null;
  private dirInitialized = false;
  private _sessionId: string | undefined;
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
  private _sleep: (ms: number) => Promise<void>;
  private _rand: () => number;
  private _now: () => number;
  private _onDiagnostic: ((message: string) => void) | undefined;
  private _consoleSink: (line: string) => void;
  /** Resolved egress-exporter flags from OTEL_LOGS_EXPORTER + protocol gating. */
  private _otlpExport: boolean;
  private _consoleExport: boolean;
  /** Resolved per-signal (logs) export interval, threaded into the batch sender. */
  private _exportIntervalMs: number;
  /** Set true once shutdown() has run its final flush — rejects all later egress. */
  private _shutdown = false;
  /** One-time latch for the "no telemetry sink configured" diagnostic. */
  private _warnedNoSink = false;
  /** One-time latch for the unsupported-protocol diagnostic. */
  private _warnedProtocol = false;
  /** Serializes mid-session (onBatch) sends so concurrent boundary flushes don't interleave. */
  private _sendChain: Promise<void> = Promise.resolve();

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
    // Resource attrs from OTEL_RESOURCE_ATTRIBUTES, overridden by any explicit opt.
    this._otlpResourceAttrs = { ...parseOtelResourceAttrs(this._env.OTEL_RESOURCE_ATTRIBUTES), ...(opts?.otlpResourceAttrs ?? {}) };
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
    this._sleep = opts?.sleepImpl ?? ((ms) => new Promise(r => setTimeout(r, ms)));
    this._rand = opts?.rand ?? Math.random;
    this._now = opts?.now ?? Date.now;
    this._onDiagnostic = opts?.onDiagnostic;
    this._consoleSink = opts?.consoleSink ?? ((line) => process.stdout.write(line + '\n'));

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

  /** Whether this tracker has been shut down (post-shutdown export guard). */
  isShutdown(): boolean {
    return this._shutdown;
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

  setAllowedSources(sources: string[] | null): void {
    this._allowedSources = sources ? new Set(sources) : null;
  }

  pushSource(source: string): void {
    this._sourceStack.push(source);
  }

  popSource(): string | undefined {
    return this._sourceStack.pop();
  }

  currentSource(): string | undefined {
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
    const recordData = sampled ? { ...data, sample_rate: effectiveRate } : data;

    // In-memory events keep the RAW data for faithful local introspection; the
    // egress copies (JSONL on disk + OTLP batch) are data-minimized/redacted.
    const event: AnalyticsEvent = {
      timestamp: new Date().toISOString(),
      type,
      ...(this._sessionId ? { sessionId: this._sessionId } : {}),
      data: recordData,
      sequence: this._sequence++,
    } as AnalyticsEvent;
    this.events.push(event);
    if (this.events.length > MAX_IN_MEMORY_EVENTS) {
      this.events = this.events.slice(-MAX_IN_MEMORY_EVENTS);
    }

    // Branded so only redaction-verified (or explicitly-marked) metadata can reach
    // the disk/OTLP egress sinks; the redact=false path opts out via the escape hatch.
    const egressData: AnalyticsSafeMetadata = this._redact
      ? sanitizeAnalyticsData(recordData, { env: this._env })
      : markAnalyticsSafe(recordData);

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
    if (this._otlpEndpoint && this._otlpExport && !isTelemetryDisabled(this._env)) {
      if (!this._telemetry) {
        this._telemetry = new TelemetryBatchSender({
          sessionId: this._sessionId ?? 'unknown',
          endpoint: this._otlpEndpoint,
          maxBatchSize: this._telemetryMaxBatchSize,
          maxBatchBytes: this._telemetryMaxBatchBytes,
          flushIntervalMs: this._exportIntervalMs,
          onBatch: (batch) => this._scheduleSend(batch),
        });
        this._telemetry.startAutoFlush();
      }
      this._telemetry.enqueue(type, egressData, event.sequence);
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
    if (!this.logFile) return;
    try {
      if (!this.dirInitialized) {
        fs.mkdirSync(path.dirname(this.logFile), { recursive: true, mode: 0o700 });
        this.dirInitialized = true;
      }
      const line = ndjsonSafeStringify({ ...event, data: egressData });
      fs.appendFileSync(this.logFile, line + '\n', { mode: 0o600 });
    } catch {
      /* best effort */
    }
  }

  getEvents(): AnalyticsEvent[] {
    return [...this.events];
  }

  getEventsByType(type: string): AnalyticsEvent[] {
    return this.events.filter(e => e.type === type);
  }

  getSummary(): Record<string, number> {
    const counts: Record<string, number> = {};
    for (const e of this.events) {
      counts[e.type] = (counts[e.type] || 0) + 1;
    }
    return counts;
  }

  reset(): void {
    this.events = [];
  }

  /** Drain any queued telemetry and POST it to the configured OTLP endpoint with
   *  bounded retry. No-op (and never throws) when no endpoint is set, OTLP export is
   *  disabled, the tracker has been shut down, or the user has opted out of
   *  telemetry. The whole drain is bounded by an overall flush budget
   *  ({@link resolveFlushTimeoutMs}); on exhaustion the remaining batches are
   *  re-queued (at-least-once) and an actionable remediation diagnostic is emitted
   *  so a slow/unreachable collector never blocks exit unboundedly. Best-effort. */
  async flushOtlp(): Promise<void> {
    if (!this._otlpEndpoint || !this._otlpExport || this._shutdown || !this._telemetry) return;
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
      // _dropBatch). Extend the failure streak.
      this._consecutiveFailures++;
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
      if (!ok && this._telemetry) this._telemetry.requeueFront([batch]);
    });
  }

  /** Resolve once all currently-scheduled mid-session sends have settled (testability). */
  async whenSent(): Promise<void> {
    await this._sendChain;
  }

  /** POST one batch, retrying retryable failures with capped quadratic backoff + jitter. */
  private async _sendWithRetry(batch: TelemetryBatch): Promise<boolean> {
    const headers = Object.keys(this._otlpHeaders).length > 0 ? this._otlpHeaders : undefined;
    const resourceAttrs = this._otlpResourceAttrs;
    for (let attempt = 1; attempt <= this._maxAttempts; attempt++) {
      const res = await postOtlpBatch(this._otlpEndpoint!, batch, { fetchImpl: this._otlpFetch, env: this._env, headers, resourceAttrs, timeoutMs: this._otlpTimeoutMs });
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
  /** Latched by shutdown() — rejects all further enqueue()/flush() (export guard). */
  private _shutdown = false;

  constructor(opts: TelemetryBatchSenderOpts) {
    this._sessionId = opts.sessionId;
    this._endpoint = opts.endpoint ?? null;
    this._maxBatchSize = opts.maxBatchSize ?? 100;
    this._maxBatchBytes = opts.maxBatchBytes ?? null;
    this._flushIntervalMs = opts.flushIntervalMs ?? 30_000;
    this._maxQueueSize = opts.maxQueueSize ?? 10_000;
    this._onBatch = opts.onBatch ?? null;
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
    if (this._queue.length >= this._maxBatchSize) {
      this._emit();
    }
    return true;
  }

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
  }

  /** Whether this sender has been shut down (post-shutdown export guard). */
  get isShutdown(): boolean { return this._shutdown; }

  get queueSize(): number { return this._queue.length; }
  get totalSent(): number { return this._totalSent; }
  get totalDropped(): number { return this._totalDropped; }
  get batchId(): number { return this._batchId; }
  /** Number of auto-flushed batches currently awaiting drain() (no onBatch wired). */
  get readyCount(): number { return this._ready.length; }

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

/** Resolve the OTLP logs URL: use an explicit /v1/{signal} path as-is, else append /v1/logs. */
export function otlpLogsUrl(endpoint: string): string {
  const trimmed = endpoint.replace(/\/+$/, '');
  return /\/v1\/(logs|traces|metrics)$/.test(trimmed) ? trimmed : `${trimmed}/v1/logs`;
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
  deps: { fetchImpl?: typeof fetch; timeoutMs?: number; headers?: Record<string, string>; resourceAttrs?: Record<string, string>; env?: NodeJS.ProcessEnv } = {},
): Promise<OtlpSendResult> {
  const fetchImpl = deps.fetchImpl ?? (globalThis.fetch as typeof fetch | undefined);
  if (!fetchImpl) return { ok: false, retryable: false };
  const env = deps.env ?? process.env;
  // Extra resource attributes: an explicit map wins; otherwise fall back to
  // OTEL_RESOURCE_ATTRIBUTES so a direct caller honors the env knob too.
  const resourceAttrs = deps.resourceAttrs ?? parseOtelResourceAttrs(env.OTEL_RESOURCE_ATTRIBUTES);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), deps.timeoutMs ?? 5000);
  try {
    const res = await fetchImpl(otlpLogsUrl(endpoint), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(deps.headers ?? {}) },
      body: JSON.stringify(toOtlpLogsPayload(batch, resourceAttrs, env)),
      signal: controller.signal,
    });
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
  deps: { fetchImpl?: typeof fetch; timeoutMs?: number; headers?: Record<string, string>; resourceAttrs?: Record<string, string>; env?: NodeJS.ProcessEnv } = {},
): Promise<boolean> {
  return (await postOtlpBatch(endpoint, batch, deps)).ok;
}

export function generateToolSummary(
  toolResults: Array<{ toolName: string; output: string; isError: boolean }>,
): string {
  const summaries = toolResults.map(r => {
    const shortOutput = r.output.split('\n')[0].substring(0, 100);
    return `${r.toolName}: ${r.isError ? 'ERROR' : 'OK'} — ${shortOutput}`;
  });
  return summaries.join('\n');
}
