// Split from analytics.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as crypto from 'crypto';
import type { SpanNode } from './profiler';
import type { SinkName } from './analytics-truncate-tool-input-value';
import type { ProcessMetricsDeps } from './analytics-telemetry-content-dedup';

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
   * Default 0 (back-to-back). Mirrors the reference's batchDelayMs.
   */
  otlpBatchDelayMs?: number;
  /**
   * Runtime killswitch probe consulted before every export. While it returns true
   * the sender produces zero network traffic (events stay queued) and resumes once
   * it clears — a sink can be disabled at runtime without tearing it down. Mirrors
   * the reference's injectable isKilled() probe.
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
   * Mirrors the reference's SerialBatchEventUploader `maxConsecutiveFailures`.
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
   * NOT appended (mirrors the reference's per-signal endpoint selection). Default false: the
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
   * the attribute is simply omitted). Mirrors the reference's getGitEmail identity signal.
   */
  includeGitUser?: boolean;
  /** Working directory whose git `user.email` is read for {@link includeGitUser}. Default process.cwd(). */
  gitUserCwd?: string;
  /** Injected git-email resolver for {@link includeGitUser} (deterministic tests). Default {@link getGitEmail}. */
  gitEmailResolver?: (cwd: string) => string | undefined;
  /**
   * Pre-resolved undici dispatcher (proxy / mTLS / custom-CA) attached to every OTLP
   * POST. When unset, one is auto-resolved from the OTLP proxy + TLS env vars (see
   * {@link resolveOtlpTransport}) against the configured endpoint. Mirrors the reference's
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
   * Mirrors the reference's SPAN_TTL_MS orphan eviction.
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
   * `AMI_ANALYTICS_FILE_FLUSH_MS` env knob. Mirrors the reference's bufferedWriter disk sink.
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
   * the reference's DATADOG_ALLOWED_EVENTS hard allowlist.
   */
  egressEventAllowlist?: readonly string[];
  /**
   * Egress field allowlist: when set, ONLY these data fields survive on the exported
   * copy of every event (others are stripped). Mirrors the reference's TAG_FIELDS gate.
   */
  egressFieldAllowlist?: readonly string[];
  /**
   * When true, large/repeated string content is de-duplicated per session before
   * egress: the full body is logged once per unique SHA-256, then collapsed to a
   * preview + hash + length. Mirrors the reference's seenHashes content dedup.
   */
  dedupeContent?: boolean;
  /**
   * A command whose JSON-object stdout supplies OTLP auth headers, merged over the
   * static headers on every export and debounced-cached (29 min). `exec`/`now` are
   * injectable for tests; the default runs the command with a 30s timeout. Mirrors
   * the reference's otelHeadersHelper setting.
   */
  otlpHeadersHelper?: { command: string; exec?: (command: string) => string; now?: () => number };
  /**
   * Per-sink killswitch: individually disable the in-memory, on-disk JSONL, or
   * off-machine OTLP sink. Merged over the env knobs (`AMI_NO_ANALYTICS` kills all;
   * `SUPERINFERENCE_DISABLE_SINKS=memory,file,otlp`). Fail-open; mutable at runtime via
   * {@link AnalyticsTracker.setKilledSinks}. Mirrors the reference's per-sink killswitch config.
   */
  killedSinks?: Partial<Record<SinkName, boolean>>;
  /**
   * Attach point-in-time process metrics (memory + CPU% delta) under a `process`
   * field on every logged event. Off by default (noise); also enabled by the
   * `AMI_OTEL_PROCESS_METRICS` env knob. Readers are injectable for tests. Mirrors
   * the reference's buildProcessMetrics attachment.
   */
  includeProcessMetrics?: boolean;
  processMetricsDeps?: ProcessMetricsDeps;
  /**
   * Persist OTLP batches that are permanently dropped (after in-process retries are
   * exhausted) to disk, and re-send them on a later flush/session — so a collector
   * outage spanning a whole session does not silently lose telemetry. Enabled by this
   * flag or the `AMI_OTEL_PERSIST_FAILED` env knob; spool dir defaults to
   * `<logDir>/telemetry-failed` (override with {@link failedBatchDir}). Mirrors
   * the reference's cross-run failed-batch retry.
   */
  persistFailedBatches?: boolean;
  /** Override directory for the durable failed-batch spool. */
  failedBatchDir?: string;
  /**
   * Bypass telemetry batching: when true, every logged event is flushed to the OTLP
   * send path immediately (one batch per event) instead of accumulating to the
   * size/byte/interval boundary — a low-latency/debugging pass-through. Off by default;
   * also enabled by the `AMI_OTEL_IMMEDIATE_FLUSH` env knob. Mirrors the reference's
   * bufferedWriter `immediateMode`.
   */
  telemetryImmediateMode?: boolean;
  /**
   * Field NAMES that are local-only: kept on the privileged on-machine sinks (in-memory
   * + JSONL + console) but stripped from the general-access off-machine OTLP export.
   * Merged with the `AMI_ANALYTICS_LOCAL_ONLY_FIELDS` env list. Mirrors the reference's
   * differential-by-sink redaction (full payload to the privileged 1P sink, stripped
   * copy to the general-access backend).
   */
  localOnlyFields?: readonly string[];
  /**
   * Key PREFIX marking local-only fields (stripped from the OTLP export only). Merged
   * with the `AMI_ANALYTICS_LOCAL_ONLY_PREFIX` env knob. Mirrors the reference's `_PROTO_*`
   * prefix strip.
   */
  localOnlyFieldPrefix?: string;
  /**
   * Point-in-time numeric metrics provider (typically the Profiler's getAll() split
   * into gauges/counters). When set, {@link AnalyticsTracker.flushOtlp} exports an OTLP
   * METRICS payload ({@link toOtlpMetricsPayload}) on every flush/shutdown — bridging
   * AMI's rich quantitative data to any OTLP metrics backend. Mirrors the reference's metrics
   * signal (PeriodicExportingMetricReader). The seam keeps the engine's profiler wiring
   * a single injected function (no engine import into core).
   */
  metricsSource?: () => MetricSnapshot;
  /**
   * Span-tree provider (typically the Profiler's getSpanTree()). When set,
   * {@link AnalyticsTracker.flushOtlp} exports an OTLP TRACES payload
   * ({@link toOtlpTracePayload}) on every flush/shutdown — surfacing the nested
   * interaction/llm/tool/hook timeline in any OTLP trace backend. Mirrors the reference's
   * sessionTracing span emission; injected to avoid a core→engine dependency.
   */
  traceSource?: () => readonly SpanNode[];
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

// ---------------------------------------------------------------------------
// OTLP TRACES signal (the reference sessionTracing.ts L176-655 parity): AMI's Profiler
// already reconstructs a parent-child span tree (interaction -> llm_request / tool ->
// tool.execution / blocked_on_user / hook) via getSpanTree(); this bridges that tree
// to an OTLP/JSON TracesData payload so the nested interaction timeline renders in any
// OTLP trace backend (Jaeger, console exporter) as a navigable introspection surface.
// otlpLogsUrl already recognised a /v1/traces path; now there is a real traces body.
// ---------------------------------------------------------------------------

/** Deterministic 16-hex-char OTLP spanId from a profiler span id (stable across a run). */
export function otlpSpanId(id: string): string {
  return crypto.createHash('sha256').update(id).digest('hex').slice(0, 16);
}

/** Deterministic 32-hex-char OTLP traceId from a seed (one trace per session export). */
export function otlpTraceId(seed: string): string {
  return crypto.createHash('sha256').update(seed).digest('hex').slice(0, 32);
}

// ---------------------------------------------------------------------------
// OTLP transport config: proxy (HTTP[S]_PROXY / NO_PROXY), mutual-TLS client
// certs, and a custom CA — mirroring the reference's getProxyUrl / shouldBypassProxy /
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
export interface UndiciDispatchers {
  ProxyAgent: new (opts: { uri: string; requestTls?: OtlpTlsConfig }) => unknown;
  Agent: new (opts: { connect?: OtlpTlsConfig }) => unknown;
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

export function parseRetryAfterMs(header: string | null | undefined): number | undefined {
  if (!header) return undefined;
  const secs = Number(header);
  if (Number.isFinite(secs) && secs >= 0) return Math.round(secs * 1000);
  const when = Date.parse(header);
  if (!Number.isNaN(when)) return Math.max(0, when - Date.now());
  return undefined;
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

/** Max chars of the first output line kept verbatim in a tool summary line. */
export const TOOL_SUMMARY_LINE_MAX = 100;

export function generateToolSummary(
  toolResults: Array<{ toolName: string; output: string; isError: boolean }>,
): string {
  const summaries = toolResults.map(r => {
    const lines = r.output.split('\n');
    // Clip the first line and, when it was clipped, append a '…' so a truncated
    // summary is visually distinguishable from a complete one (mirrors the reference's
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
