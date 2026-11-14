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

export interface AnalyticsTrackerOpts {
  otlpEndpoint?: string;
  otlpFetch?: typeof fetch;
  /** Overrides process.env for privacy/redaction resolution (testability). */
  env?: NodeJS.ProcessEnv;
  /** Keep events verbatim on egress (skip the redaction pass). Default: redact. */
  redact?: boolean;
  /** Per-event probabilistic keep rate (0..1). Default 1 (keep all). */
  sampleRate?: number;
  /** OTLP export retry knobs. */
  otlpMaxAttempts?: number;
  otlpBaseBackoffMs?: number;
  otlpMaxBackoffMs?: number;
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
  private _maxAttempts: number;
  private _baseBackoffMs: number;
  private _maxBackoffMs: number;
  private _sleep: (ms: number) => Promise<void>;
  private _rand: () => number;

  constructor(logDir?: string, opts?: AnalyticsTrackerOpts) {
    this.logFile = logDir ? `${logDir}/analytics.jsonl` : null;
    this._otlpEndpoint = opts?.otlpEndpoint ?? null;
    this._otlpFetch = opts?.otlpFetch;
    this._env = opts?.env ?? process.env;
    this._redact = opts?.redact ?? true;
    this._sampleRate = opts?.sampleRate ?? 1;
    this._maxAttempts = Math.max(1, opts?.otlpMaxAttempts ?? 3);
    this._baseBackoffMs = Math.max(0, opts?.otlpBaseBackoffMs ?? 500);
    this._maxBackoffMs = Math.max(0, opts?.otlpMaxBackoffMs ?? 30_000);
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

    // Per-event probabilistic sampling (default rate 1 keeps everything).
    if (!shouldSampleEvent(this._sampleRate, this._rand)) {
      this._droppedEvents++;
      return;
    }

    // In-memory events keep the RAW data for faithful local introspection; the
    // egress copies (JSONL on disk + OTLP batch) are data-minimized/redacted.
    const event: AnalyticsEvent = {
      timestamp: new Date().toISOString(),
      type,
      ...(this._sessionId ? { sessionId: this._sessionId } : {}),
      data,
      sequence: this._sequence++,
    } as AnalyticsEvent;
    this.events.push(event);
    if (this.events.length > MAX_IN_MEMORY_EVENTS) {
      this.events = this.events.slice(-MAX_IN_MEMORY_EVENTS);
    }

    const egressData = this._redact ? sanitizeAnalyticsData(data, { env: this._env }) : data;

    // Mirror into the OTLP batch sender when an endpoint is configured AND the
    // user has not opted out of telemetry. Lazily created so the batch carries
    // the (by-then-known) session id.
    if (this._otlpEndpoint && !isTelemetryDisabled(this._env)) {
      if (!this._telemetry) {
        this._telemetry = new TelemetryBatchSender({ sessionId: this._sessionId ?? 'unknown', endpoint: this._otlpEndpoint });
      }
      this._telemetry.enqueue(type, egressData);
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

  /** POST one batch, retrying retryable failures with capped quadratic backoff + jitter. */
  private async _sendWithRetry(batch: TelemetryBatch): Promise<boolean> {
    for (let attempt = 1; attempt <= this._maxAttempts; attempt++) {
      const res = await postOtlpBatch(this._otlpEndpoint!, batch, { fetchImpl: this._otlpFetch });
      if (res.ok) return true;
      if (!res.retryable) { this._droppedBatches++; return false; } // 4xx: permanent, drop
      if (attempt === this._maxAttempts) { this._droppedBatches++; return false; }
      const base = Math.min(this._maxBackoffMs, this._baseBackoffMs * attempt * attempt);
      const jittered = res.retryAfterMs ?? Math.round(base * (0.5 + this._rand() * 0.5));
      if (jittered > 0) await this._sleep(jittered);
    }
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
  events: Array<{ type: string; timestamp: number; data: Record<string, unknown> }>;
  sessionId: string;
  batchId: number;
}

export class TelemetryBatchSender {
  private _queue: TelemetryBatch['events'] = [];
  private _batchId = 0;
  private _maxBatchSize: number;
  private _flushIntervalMs: number;
  private _timer: ReturnType<typeof setTimeout> | null = null;
  private _endpoint: string | null;
  private _sessionId: string;
  private _totalSent = 0;
  private _totalDropped = 0;
  private _maxQueueSize: number;

  constructor(opts: {
    sessionId: string;
    endpoint?: string;
    maxBatchSize?: number;
    flushIntervalMs?: number;
    maxQueueSize?: number;
  }) {
    this._sessionId = opts.sessionId;
    this._endpoint = opts.endpoint ?? null;
    this._maxBatchSize = opts.maxBatchSize ?? 100;
    this._flushIntervalMs = opts.flushIntervalMs ?? 30_000;
    this._maxQueueSize = opts.maxQueueSize ?? 10_000;
  }

  enqueue(type: string, data: Record<string, unknown>): boolean {
    if (this._queue.length >= this._maxQueueSize) {
      this._totalDropped++;
      return false;
    }
    this._queue.push({ type, timestamp: Date.now(), data });
    if (this._queue.length >= this._maxBatchSize) {
      this.flush();
    }
    return true;
  }

  flush(): TelemetryBatch | null {
    if (this._queue.length === 0) return null;
    const events = this._queue.splice(0, this._maxBatchSize);
    const batch: TelemetryBatch = {
      events,
      sessionId: this._sessionId,
      batchId: ++this._batchId,
    };
    this._totalSent += events.length;
    return batch;
  }

  startAutoFlush(): void {
    if (this._timer) return;
    this._timer = setInterval(() => this.flush(), this._flushIntervalMs);
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

  drain(): TelemetryBatch[] {
    const batches: TelemetryBatch[] = [];
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

function otlpAttrs(obj: Record<string, unknown>): Array<{ key: string; value: { stringValue: string } }> {
  return Object.entries(obj).map(([key, v]) => ({
    key,
    value: { stringValue: typeof v === 'string' ? v : JSON.stringify(v) },
  }));
}

/** Convert a telemetry batch into an OTLP/JSON LogsData payload. */
export function toOtlpLogsPayload(
  batch: TelemetryBatch,
  resourceAttrs: Record<string, string> = {},
): Record<string, unknown> {
  return {
    resourceLogs: [{
      resource: {
        attributes: otlpAttrs({
          'service.name': AMI_CLIENT_NAME,
          'service.version': AMI_VERSION,
          'session.id': batch.sessionId,
          ...resourceAttrs,
        }),
      },
      scopeLogs: [{
        scope: { name: AMI_CLIENT_NAME, version: AMI_VERSION },
        logRecords: batch.events.map(e => ({
          timeUnixNano: String(e.timestamp * 1_000_000),
          severityText: 'INFO',
          body: { stringValue: e.type },
          attributes: otlpAttrs(e.data),
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
  deps: { fetchImpl?: typeof fetch; timeoutMs?: number; headers?: Record<string, string> } = {},
): Promise<OtlpSendResult> {
  const fetchImpl = deps.fetchImpl ?? (globalThis.fetch as typeof fetch | undefined);
  if (!fetchImpl) return { ok: false, retryable: false };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), deps.timeoutMs ?? 5000);
  try {
    const res = await fetchImpl(otlpLogsUrl(endpoint), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(deps.headers ?? {}) },
      body: JSON.stringify(toOtlpLogsPayload(batch)),
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
  deps: { fetchImpl?: typeof fetch; timeoutMs?: number; headers?: Record<string, string> } = {},
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
