// Split from analytics.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { AMI_CLIENT_NAME, AMI_VERSION } from './version';
import { isEnvTruthy } from './privacy-level';
import { trySerialize } from './analytics-truncate-tool-input-value';
import { detectTerminalType, getEnvironmentFingerprint } from './analytics-telemetry-content-dedup';
import { detectDeploymentEnvironment, getCiContext, isCI, type DeploymentEnvDeps } from './analytics-detect-deployment-environment';
import type { TelemetryBatch, TelemetryBatchSenderOpts, TelemetryReconfigureOpts } from './analytics-failed-batch-store';

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
   * Mirrors the reference's SerialBatchEventUploader.enqueue await-until-space.
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
    // (never left hanging) — mirrors the reference's close() waking backpressure waiters.
    this._releaseBackpressure();
  }

  /**
   * Reconfigure the batch pipeline on a LIVE sender (the reference's
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
   * queue intact) so exit diagnostics can report un-exported depth. Mirrors the reference's
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

/** Per-attribute telemetry cardinality defaults (session.id ON, service.version ON). */
export const OTEL_CARDINALITY_DEFAULTS: Record<string, boolean> = {
  'session.id': true,
  'service.version': true,
  'deployment.environment': true,
};

export const CARDINALITY_ENV_KEYS: Record<string, string> = {
  'session.id': 'AMI_OTEL_INCLUDE_SESSION_ID',
  'service.version': 'AMI_OTEL_INCLUDE_VERSION',
  'deployment.environment': 'AMI_OTEL_INCLUDE_DEPLOYMENT',
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
  deploymentDeps: DeploymentEnvDeps = {},
): Record<string, string> {
  const attrs: Record<string, string> = { 'service.name': AMI_CLIENT_NAME };
  if (shouldIncludeTelemetryAttr('service.version', env)) attrs['service.version'] = AMI_VERSION;
  if (shouldIncludeTelemetryAttr('session.id', env)) attrs['session.id'] = sessionId;
  // Provider-neutral environment dimensions so a self-hosted collector can
  // segment usage: host platform/arch/node version, terminal type (only when
  // resolved), deployment environment, and CI context (only under GitHub Actions).
  Object.assign(attrs, getEnvironmentFingerprint(env));
  const terminal = detectTerminalType(env);
  if (terminal) attrs['terminal.type'] = terminal;
  // Deployment environment (codespaces/docker/kubernetes/aws-*/…): a low-cardinality
  // dimension so a collector can split usage by where AMI runs. The generic `unknown*`
  // fallbacks carry no signal, so they are elided to keep the attribute meaningful.
  if (shouldIncludeTelemetryAttr('deployment.environment', env)) {
    const deployment = detectDeploymentEnvironment(env, deploymentDeps);
    if (!deployment.startsWith('unknown')) attrs['deployment.environment'] = deployment;
  }
  // Generic CI flag (any CI, not just GitHub Actions) alongside the richer GitHub context.
  if (isCI(env)) attrs['ci'] = 'true';
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
   * the reference's per-signal endpoint selection distinct from the generic endpoint.
   */
  perSignal: boolean;
}

/**
 * Resolve the OTLP LOGS endpoint honoring signal-specific selection (the reference parity):
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
// OTLP METRICS signal (the reference instrumentation.ts L113-215 parity): AMI's
// logs-only OTLP export is extended with a metrics signal so the rich quantitative
// data the Profiler already computes (per-label durations, TTFT, p50/p95/p99,
// counters/gauges via its getAll() snapshot) can be bridged to any OTLP metrics
// backend over HTTP/JSON (AMI ships no @opentelemetry SDK). The aggregation
// temporality defaults to 'delta' — the saner default the reference picks — while remaining
// overridable via OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE.
// ---------------------------------------------------------------------------

/** OTLP aggregation-temporality enum values (proto: DELTA=1, CUMULATIVE=2). */
export const OTLP_TEMPORALITY = { delta: 1, cumulative: 2 } as const;

export type MetricsTemporality = keyof typeof OTLP_TEMPORALITY;

/**
 * Resolve the metrics aggregation-temporality preference, DEFAULTING TO 'delta' (the
 * saner default the reference's instrumentation.ts L113-116 sets) while honoring
 * `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE`. Only an explicit `cumulative`
 * selects cumulative; anything else (unset / unknown / blank) falls back to delta.
 */
export function resolveMetricsTemporalityPreference(env: NodeJS.ProcessEnv = process.env): MetricsTemporality {
  const raw = env.OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE?.trim().toLowerCase();
  return raw === 'cumulative' ? 'cumulative' : 'delta';
}
