// Split from analytics.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { trySerialize } from './analytics-truncate-tool-input-value';

/** A PR-create REST endpoint, but NOT a sub-resource like /pulls/123/comments. */
export const PR_ENDPOINT_RE = /https?:\/\/[^\s'"]*\/(pulls|pull-requests|merge[-_]requests)(?!\/\d)/i;

// ---------------------------------------------------------------------------
// Per-agent incremental context delta (the reference betaSessionTracing.ts parity):
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
 * produced (matching the reference's lastReportedMessageHash update being gated on
 * newMessages.length > 0). Mirrors the reference's betaSessionTracing new_context tracking.
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

// ---------------------------------------------------------------------------
// Durable failed-batch spool (the reference firstPartyEventLoggingExporter.ts parity):
// a telemetry batch that is permanently dropped after exhausting its in-process
// retries is written to disk, and a LATER process (next session's flush) re-reads
// and re-POSTs it, deleting the file on success — so a transient collector outage
// across a whole session does not silently lose telemetry.
// ---------------------------------------------------------------------------

/** Prefix of every spooled failed-batch file (distinguishes them in the dir). */
export const FAILED_BATCH_FILE_PREFIX = 'failed-batch-';

export const FAILED_BATCH_FILE_RE = /^failed-batch-.*\.jsonl$/;

/**
 * Append-only, crash-safe disk spool for telemetry batches that could not be sent.
 * Each spooled batch is one file of NDJSON event lines, written via a temp file +
 * atomic rename (a reader never sees a half-written file). Files are created 0o600 in
 * a 0o700 dir. All ops are best-effort (never throw) — the spool is a backstop, not a
 * critical path. Mirrors the reference's per-batch disk persistence + cross-run retry.
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
// Level-filtered telemetry diagnostic boundary (the reference
// utils/telemetry/logger.ts ClaudeCodeDiagLogger parity): an error-only bridge
// for the telemetry subsystem's own internal diagnostics. error()/warn() surface
// the message as BOTH a real Error (onError) and a debug line (onDebug), while
// info()/debug()/verbose() are explicit no-ops so routine SDK chatter never
// becomes noise. This gives the telemetry pipeline a single, severity-aware
// place to decide what reaches the user vs. what is suppressed.
// ---------------------------------------------------------------------------

/** Diagnostic severity levels recognised by {@link createTelemetryDiagLogger}. */
export const TELEMETRY_DIAG_LEVELS = ['error', 'warn', 'info', 'debug', 'verbose'] as const;

export type TelemetryDiagLevel = (typeof TELEMETRY_DIAG_LEVELS)[number];

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
 * info/debug/verbose entirely. Mirrors the reference's ClaudeCodeDiagLogger, whose
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
   * pass-through for low-latency/debug sinks. Mirrors the reference's bufferedWriter
   * `immediateMode`.
   */
  immediateMode?: boolean;
  /**
   * Apply LOSSLESS backpressure instead of lossy drop when the queue is full:
   * {@link TelemetryBatchSender.enqueueAsync} awaits until drain frees space rather
   * than dropping the newest event. Off by default (the synchronous {@link TelemetryBatchSender.enqueue}
   * stays lossy — best-effort telemetry). Blocked producers are released when drain
   * frees space OR on {@link TelemetryBatchSender.shutdown}. Mirrors the reference's
   * SerialBatchEventUploader backpressure (enqueue awaits, close releases waiters).
   */
  backpressure?: boolean;
}

/**
 * Runtime-reconfigurable batch-pipeline parameters (the reference firstPartyEventLogger.ts
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
