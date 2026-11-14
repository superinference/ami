import * as fs from 'fs';
import * as path from 'path';
import { AMI_CLIENT_NAME, AMI_VERSION } from './version';
import { redactCredentials } from './credential-guard';

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
// Privacy-level resolution + telemetry opt-out gating
// ---------------------------------------------------------------------------

/** Standard env-flag truthiness: anything set and not '', '0', 'false', 'no', 'off'. */
export function isEnvTruthy(v: string | undefined): boolean {
  if (v === undefined) return false;
  const s = v.trim().toLowerCase();
  return s !== '' && s !== '0' && s !== 'false' && s !== 'no' && s !== 'off';
}

/** Ordered privacy tiers; a more-restrictive signal always wins. */
export type PrivacyLevel = 'default' | 'no-telemetry' | 'essential-traffic';
const PRIVACY_RANK: Record<PrivacyLevel, number> = {
  'default': 0,
  'no-telemetry': 1,
  'essential-traffic': 2,
};

/**
 * Resolve the effective privacy level from the standard opt-out env vars,
 * taking the MOST restrictive signal (mirrors Claude's getPrivacyLevel):
 *  - DISABLE_TELEMETRY / DO_NOT_TRACK → no-telemetry
 *  - *_DISABLE_NONESSENTIAL_TRAFFIC  → essential-traffic (strictest)
 */
export function getPrivacyLevel(env: NodeJS.ProcessEnv = process.env): PrivacyLevel {
  let level: PrivacyLevel = 'default';
  const bump = (l: PrivacyLevel): void => { if (PRIVACY_RANK[l] > PRIVACY_RANK[level]) level = l; };
  if (isEnvTruthy(env.DISABLE_TELEMETRY) || isEnvTruthy(env.DO_NOT_TRACK)) bump('no-telemetry');
  if (isEnvTruthy(env.SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC) || isEnvTruthy(env.CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC)) {
    bump('essential-traffic');
  }
  return level;
}

/** True when analytics/telemetry transmission must be suppressed (no-telemetry or stricter). */
export function isTelemetryDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return PRIVACY_RANK[getPrivacyLevel(env)] >= PRIVACY_RANK['no-telemetry'];
}

/** True when only strictly-essential (product-function) traffic may leave the machine. */
export function isEssentialTrafficOnly(env: NodeJS.ProcessEnv = process.env): boolean {
  return getPrivacyLevel(env) === 'essential-traffic';
}

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
 * Data-minimization pass applied to every event payload before it is persisted
 * to disk or exported off-machine:
 *  - internal marker keys (`_*`) are dropped
 *  - tool-name fields are MCP-sanitized
 *  - free-text content keys are redacted to `<REDACTED>` + a `<key>_length` unless
 *    OTEL_LOG_USER_PROMPTS / SUPERINFERENCE_LOG_USER_PROMPTS opts in
 *  - all remaining string values are credential-redacted and length-capped
 */
export function sanitizeAnalyticsData(data: Record<string, unknown>, opts: SanitizeOpts = {}): Record<string, unknown> {
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
  return out;
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
  /** Telemetry batching knobs threaded into the lazily-created TelemetryBatchSender. */
  telemetryMaxBatchSize?: number;
  telemetryMaxBatchBytes?: number;
  /** Invoked when a fully-retried OTLP batch is permanently dropped (shutdown diagnostics). */
  onBatchDropped?: (eventCount: number, droppedBatchCount: number) => void;
  /**
   * Static headers attached to every OTLP POST (e.g. an Authorization/x-api-key for
   * a self-hosted collector). Merged over headers parsed from OTEL_EXPORTER_OTLP_HEADERS.
   */
  otlpHeaders?: Record<string, string>;
  /** Sleep + RNG injection for deterministic tests. */
  sleepImpl?: (ms: number) => Promise<void>;
  rand?: () => number;
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
  private _telemetryMaxBatchSize: number | undefined;
  private _telemetryMaxBatchBytes: number | undefined;
  private _onBatchDropped: ((eventCount: number, droppedBatchCount: number) => void) | undefined;
  private _otlpHeaders: Record<string, string>;
  private _sleep: (ms: number) => Promise<void>;
  private _rand: () => number;
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
    this._telemetryMaxBatchSize = opts?.telemetryMaxBatchSize;
    this._telemetryMaxBatchBytes = opts?.telemetryMaxBatchBytes;
    this._onBatchDropped = opts?.onBatchDropped;
    // Headers from OTEL_EXPORTER_OTLP_HEADERS, overridden by any explicit opt.
    this._otlpHeaders = { ...parseOtelHeaders(this._env.OTEL_EXPORTER_OTLP_HEADERS), ...(opts?.otlpHeaders ?? {}) };
    this._sleep = opts?.sleepImpl ?? ((ms) => new Promise(r => setTimeout(r, ms)));
    this._rand = opts?.rand ?? Math.random;
  }

  /** The resolved privacy level (default | no-telemetry | essential-traffic). */
  getPrivacyLevel(): PrivacyLevel {
    return getPrivacyLevel(this._env);
  }

  getDroppedBatchCount(): number {
    return this._droppedBatches;
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

    const egressData = this._redact ? sanitizeAnalyticsData(recordData, { env: this._env }) : recordData;

    // Mirror into the OTLP batch sender when an endpoint is configured AND the
    // user has not opted out of telemetry. Lazily created so the batch carries
    // the (by-then-known) session id. A size/byte boundary flush is POSTed
    // mid-session via the onBatch hook instead of being buffered until shutdown.
    if (this._otlpEndpoint && !isTelemetryDisabled(this._env)) {
      if (!this._telemetry) {
        this._telemetry = new TelemetryBatchSender({
          sessionId: this._sessionId ?? 'unknown',
          endpoint: this._otlpEndpoint,
          maxBatchSize: this._telemetryMaxBatchSize,
          maxBatchBytes: this._telemetryMaxBatchBytes,
          onBatch: (batch) => this._scheduleSend(batch),
        });
      }
      this._telemetry.enqueue(type, egressData, event.sequence);
    }

    if (this.logFile) {
      try {
        if (!this.dirInitialized) {
          fs.mkdirSync(path.dirname(this.logFile), { recursive: true });
          this.dirInitialized = true;
        }
        const egressEvent = { ...event, data: egressData };
        fs.appendFileSync(this.logFile, JSON.stringify(egressEvent) + '\n');
      } catch {
        /* best effort */
      }
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
   *  bounded retry. No-op (and never throws) when no endpoint is set, or when the
   *  user has opted out of telemetry. Best-effort. */
  async flushOtlp(): Promise<void> {
    if (!this._otlpEndpoint || !this._telemetry) return;
    // Let any in-flight mid-session (onBatch) sends settle first so a boundary
    // batch is never POSTed concurrently with the shutdown drain.
    await this._sendChain;
    const batches = this._telemetry.drain();
    if (isTelemetryDisabled(this._env)) return; // opted out: drop without sending
    for (let i = 0; i < batches.length; i++) {
      const ok = await this._sendWithRetry(batches[i]);
      // Endpoint looks down after a fully-retried batch failed — short-circuit the
      // rest and re-queue them for the next flush (at-least-once, order preserved).
      if (!ok) {
        this._telemetry.requeueFront(batches.slice(i + 1));
        break;
      }
    }
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
    for (let attempt = 1; attempt <= this._maxAttempts; attempt++) {
      const res = await postOtlpBatch(this._otlpEndpoint!, batch, { fetchImpl: this._otlpFetch, env: this._env, headers });
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
        logRecords: batch.events.map(e => ({
          timeUnixNano: String(e.timestamp * 1_000_000),
          severityText: 'INFO',
          body: { stringValue: e.type },
          // A session-monotonic sequence disambiguates same-millisecond events.
          attributes: otlpAttrs(e.sequence !== undefined ? { ...e.data, 'event.sequence': e.sequence } : e.data),
        })),
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
  deps: { fetchImpl?: typeof fetch; timeoutMs?: number; headers?: Record<string, string>; env?: NodeJS.ProcessEnv } = {},
): Promise<OtlpSendResult> {
  const fetchImpl = deps.fetchImpl ?? (globalThis.fetch as typeof fetch | undefined);
  if (!fetchImpl) return { ok: false, retryable: false };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), deps.timeoutMs ?? 5000);
  try {
    const res = await fetchImpl(otlpLogsUrl(endpoint), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(deps.headers ?? {}) },
      body: JSON.stringify(toOtlpLogsPayload(batch, {}, deps.env ?? process.env)),
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
  deps: { fetchImpl?: typeof fetch; timeoutMs?: number; headers?: Record<string, string>; env?: NodeJS.ProcessEnv } = {},
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
