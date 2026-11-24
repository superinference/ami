// Split from logger.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { redactCredentials } from './credential-guard';
import { LEVEL_ORDER } from './logger-http-error-context';
import type { LogLevel } from './logger-http-error-context';

/** Result of {@link readLogTail}: the tail lines + full size, or a miss/error. */
export type LogTailResult =
  | { ok: true; lines: string[]; size: number }
  | { ok: false; missing: boolean; error?: string };

export function isENOENT(e: unknown): boolean {
  return !!e && typeof e === 'object' && (e as NodeJS.ErrnoException).code === 'ENOENT';
}

// ---------------------------------------------------------------------------
// Human-readable value summarizer for diagnostic output.
// ---------------------------------------------------------------------------
/**
 * Compact one value for a diagnostic description: `Array[N]`, `Object{N keys}`,
 * strings truncated to 80 chars with an ellipsis, everything else `String(v)`.
 * Keeps diagnostic lines bounded and readable. Mirrors the per-value summary in
 * the reference's slow-operation description builder.
 */
export function summarizeValue(v: unknown): string {
  if (Array.isArray(v)) return `Array[${v.length}]`;
  if (v !== null && typeof v === 'object') return `Object{${Object.keys(v as Record<string, unknown>).length} keys}`;
  if (typeof v === 'string') return v.length > 80 ? `${v.slice(0, 80)}…` : v;
  return String(v);
}

// ---------------------------------------------------------------------------
// Recent-API-request capture (for bug reports / diagnostics).
// ---------------------------------------------------------------------------

/** One captured request: an ISO timestamp plus the credential-redacted params. */
export interface CapturedApiRequest {
  timestamp: string;
  params: Record<string, unknown>;
}

/** How many of the most recent API requests the diagnostic ring keeps (parity:
 *  the reference's `MAX_CACHED_REQUESTS = 5` dumpPrompts ring). */
export const MAX_CACHED_API_REQUESTS = 5;

/** Bounded FIFO ring of the most recent {@link MAX_CACHED_API_REQUESTS} requests
 *  (oldest first). A later bug report / `/doctor` can attach the exact recent calls
 *  that preceded a failure — not just the single last one. */
export const apiRequestRing: CapturedApiRequest[] = [];

/**
 * Record an API request's parameters onto the bounded diagnostic ring so a later
 * bug report or `/doctor` can include "the last few calls that led to the failure"
 * (parity: the reference's `addApiRequestToCache`, which keeps the last
 * {@link MAX_CACHED_API_REQUESTS}). String values are credential-redacted on
 * capture so an API key in a header never lingers in memory. Pushing past the cap
 * drops the oldest entry.
 */
export function captureApiRequest(params: Record<string, unknown>): void {
  const redacted: Record<string, unknown> = {};
  for (const [k, val] of Object.entries(params)) {
    redacted[k] = typeof val === 'string' ? redactCredentials(val) : val;
  }
  apiRequestRing.push({ timestamp: new Date().toISOString(), params: redacted });
  if (apiRequestRing.length > MAX_CACHED_API_REQUESTS) apiRequestRing.shift();
}

/** Return a copy of the MOST RECENT captured API request, or null if none. */
export function getLastApiRequest(): CapturedApiRequest | null {
  const last = apiRequestRing[apiRequestRing.length - 1];
  return last ? { timestamp: last.timestamp, params: { ...last.params } } : null;
}

/** Return a copy of every captured request (oldest first, at most
 *  {@link MAX_CACHED_API_REQUESTS}). Parity: `getLastApiRequests()`. */
export function getLastApiRequests(): CapturedApiRequest[] {
  return apiRequestRing.map((r) => ({ timestamp: r.timestamp, params: { ...r.params } }));
}

/** Clear the recent-request ring (parity: `clearApiRequestCache()`). */
export function clearApiRequestCache(): void {
  apiRequestRing.length = 0;
}

// ---------------------------------------------------------------------------
// Severity-filtered diagnostic bridge + async timing wrapper.
// ---------------------------------------------------------------------------
/**
 * Whether a severity should be surfaced to the telemetry/bug-report pipeline:
 * `warn` and `error` surface; `info`/`debug`/`verbose` are suppressed (they stay
 * only in the on-disk debug log). Lets a noisy diagnostic channel feed just the
 * actionable entries into the retrievable error ring.
 */
export function shouldSurfaceToTelemetry(level: LogLevel): boolean {
  return LEVEL_ORDER[level] >= LEVEL_ORDER.warn;
}

/** Resolve a positive-integer env knob, falling back to `fallback` when unset/invalid. */
export function getPositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw !== undefined) {
    const parsed = Number(raw);
    if (Number.isInteger(parsed) && parsed > 0) return parsed;
  }
  return fallback;
}

/** Slow-operation threshold (ms); off by default. `AMI_SLOW_OPERATION_THRESHOLD_MS`. */
export function getSlowOpThresholdMs(): number {
  const raw = process.env.AMI_SLOW_OPERATION_THRESHOLD_MS;
  if (raw !== undefined) {
    const parsed = Number(raw);
    if (!Number.isNaN(parsed) && parsed >= 0) return parsed;
  }
  return Infinity;
}
