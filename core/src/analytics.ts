import * as fs from 'fs';
import * as path from 'path';
import { AMI_CLIENT_NAME, AMI_VERSION } from './version';

export interface AnalyticsEvent {
  timestamp: string;
  type: string;
  sessionId?: string;
  data: Record<string, unknown>;
}

const MAX_IN_MEMORY_EVENTS = 1000;

export class AnalyticsTracker {
  private events: AnalyticsEvent[] = [];
  private logFile: string | null;
  private dirInitialized = false;
  private _sessionId: string | undefined;
  private _allowedSources: Set<string> | null = null;
  private _sourceStack: string[] = [];
  private _droppedEvents = 0;
  private _otlpEndpoint: string | null;
  private _otlpFetch: typeof fetch | undefined;
  private _telemetry: TelemetryBatchSender | null = null;

  constructor(logDir?: string, opts?: { otlpEndpoint?: string; otlpFetch?: typeof fetch }) {
    this.logFile = logDir ? `${logDir}/analytics.jsonl` : null;
    this._otlpEndpoint = opts?.otlpEndpoint ?? null;
    this._otlpFetch = opts?.otlpFetch;
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

    const event: AnalyticsEvent = {
      timestamp: new Date().toISOString(),
      type,
      ...(this._sessionId ? { sessionId: this._sessionId } : {}),
      data,
    } as AnalyticsEvent;
    this.events.push(event);
    if (this.events.length > MAX_IN_MEMORY_EVENTS) {
      this.events = this.events.slice(-MAX_IN_MEMORY_EVENTS);
    }

    // Mirror into the OTLP batch sender when an endpoint is configured. Lazily
    // created so the batch carries the (by-then-known) session id.
    if (this._otlpEndpoint) {
      if (!this._telemetry) {
        this._telemetry = new TelemetryBatchSender({ sessionId: this._sessionId ?? 'unknown', endpoint: this._otlpEndpoint });
      }
      this._telemetry.enqueue(type, data);
    }

    if (this.logFile) {
      try {
        if (!this.dirInitialized) {
          fs.mkdirSync(path.dirname(this.logFile), { recursive: true });
          this.dirInitialized = true;
        }
        fs.appendFileSync(this.logFile, JSON.stringify(event) + '\n');
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

  /** Drain any queued telemetry and POST it to the configured OTLP endpoint.
   *  No-op (and never throws) when no endpoint is set. Best-effort. */
  async flushOtlp(): Promise<void> {
    if (!this._otlpEndpoint || !this._telemetry) return;
    for (const batch of this._telemetry.drain()) {
      await sendOtlpBatch(this._otlpEndpoint, batch, { fetchImpl: this._otlpFetch });
    }
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

/** POST one telemetry batch to an OTLP/HTTP logs endpoint. Best-effort: returns
 *  false (never throws) on network error, timeout, non-2xx, or no fetch impl. */
export async function sendOtlpBatch(
  endpoint: string,
  batch: TelemetryBatch,
  deps: { fetchImpl?: typeof fetch; timeoutMs?: number; headers?: Record<string, string> } = {},
): Promise<boolean> {
  const fetchImpl = deps.fetchImpl ?? (globalThis.fetch as typeof fetch | undefined);
  if (!fetchImpl) return false;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), deps.timeoutMs ?? 5000);
  try {
    const res = await fetchImpl(otlpLogsUrl(endpoint), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(deps.headers ?? {}) },
      body: JSON.stringify(toOtlpLogsPayload(batch)),
      signal: controller.signal,
    });
    return res.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(timer);
  }
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
