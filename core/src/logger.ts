import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { redactCredentials } from './credential-guard';
import { isEnvTruthy } from './privacy-level';
import './skill-catalog';
import { createBufferedWriter, type BufferedWriter } from './buffered-writer';
import './version';
import { DEFAULT_TAIL_LINES, DIR_MODE, FILE_MODE, LEVEL_ORDER, MAX_IN_MEMORY_ERRORS, MAX_PENDING_WRITES, categoriesForLine, dateToFilename, debugBody, debugTruncate, escapeLogField, getErrorsDir, getMinLogLevel, hasPerSessionFile, httpErrorContext, isDebugToStderr, logFileName, runtimeHooks, shouldShowDebugCategories, standardLogFields } from './logger-http-error-context';
import type { DebugFilter, LogLevel } from './logger-http-error-context';
import { apiRequestRing, captureApiRequest, getPositiveIntEnv, getSlowOpThresholdMs, isENOENT, shouldSurfaceToTelemetry } from './logger-capture-api-request';
import type { LogTailResult } from './logger-capture-api-request';
export { LOG_LEVELS, dateToFilename, debugBody, debugTruncate, extractDebugCategories, extractServerMessage, httpErrorContext, runtimeHooks, shouldShowDebugCategories } from './logger-http-error-context';
export type { DebugFilter, LogLevel } from './logger-http-error-context';
export { MAX_CACHED_API_REQUESTS, captureApiRequest, clearApiRequestCache, getLastApiRequest, getLastApiRequests, shouldSurfaceToTelemetry, summarizeValue } from './logger-capture-api-request';
export type { CapturedApiRequest, LogTailResult } from './logger-capture-api-request';

const DEFAULT_LOG_DIR = path.join(os.tmpdir(), 'superinference', 'core');
const MAX_LOG_SIZE = 10 * 1024 * 1024; // 10MB

// Memoize parsing so repeated per-call lookups of the same spec string are cheap
// (the spec rarely changes within a session). Keyed by the raw spec string.
const debugFilterCache = new Map<string, DebugFilter | null>();

/**
 * Parse a debug-filter spec string into `{include, exclude, isExclusive}`, or
 * `null` for "no filtering / show all". Defensive by design:
 *   - undefined / empty / whitespace-only  → null (show all)
 *   - the `*` sentinel                      → null (AMI show-all affordance)
 *   - stray empty tokens (`api,,hooks`)     → dropped
 *   - mixed inclusive + exclusive tokens    → null (invalid → show all)
 * Tokens are trimmed and lowercased for case-insensitive matching. Mirrors
 * the reference's `parseDebugFilter`.
 */
export function parseDebugFilter(filterString?: string): DebugFilter | null {
  const key = filterString ?? '\u0000undefined';
  const cached = debugFilterCache.get(key);
  if (cached !== undefined) return cached;

  const result = ((): DebugFilter | null => {
    if (!filterString || filterString.trim() === '') return null;
    // AMI superset affordance: an explicit `*` means "log everything".
    if (filterString.trim() === '*') return null;

    const filters = filterString
      .split(',')
      .map((f) => f.trim())
      .filter(Boolean);
    if (filters.length === 0) return null;

    const hasExclusive = filters.some((f) => f.startsWith('!'));
    const hasInclusive = filters.some((f) => !f.startsWith('!'));
    // A spec that mixes include and exclude tokens is contradictory; treat it as
    // invalid and fall back to show-all rather than guessing an intent.
    if (hasExclusive && hasInclusive) return null;

    const clean = filters.map((f) => f.replace(/^!/, '').toLowerCase());
    return {
      include: hasExclusive ? [] : clean,
      exclude: hasExclusive ? clean : [],
      isExclusive: hasExclusive,
    };
  })();

  debugFilterCache.set(key, result);
  return result;
}

// ---------------------------------------------------------------------------
// Destination resolution — override the on-disk log location.
// ---------------------------------------------------------------------------
/**
 * Resolve the active log directory + file via a path waterfall (mirrors the reference
 * Code's getDebugLogPath): an explicit file override wins, then a directory
 * override (AMI first, then the Claude-compatible env), else the default tmp
 * location. Resolved once per init so rotation and byte-accounting stay
 * consistent for the whole session.
 */
function resolveLogTarget(): { dir: string; file: string } {
  const fileOverride = process.env.AMI_DEBUG_FILE?.trim();
  if (fileOverride) {
    return { dir: path.dirname(fileOverride), file: fileOverride };
  }
  const dirOverride = (process.env.AMI_DEBUG_LOGS_DIR ?? process.env.CLAUDE_CODE_DEBUG_LOGS_DIR)?.trim();
  const dir = dirOverride || DEFAULT_LOG_DIR;
  return { dir, file: path.join(dir, logFileName()) };
}

/**
 * For per-session runs, maintain a `latest` symlink in the log directory that
 * points at the active session file so a bug-report/`/doctor`/share flow can
 * always jump straight to the current session's log. Best-effort: silently
 * no-ops on a platform/filesystem that forbids symlinks. Mirrors the reference's
 * `debug/latest` symlink affordance.
 */
function updateLatestSymlink(): void {
  if (!hasPerSessionFile()) return;
  const link = path.join(logDir, 'latest');
  try { fsHooks.unlinkSync(link); } catch {}
  try { fsHooks.symlinkSync(logFile, link); } catch {}
}

/**
 * Mockable filesystem seam (ESM-safe). Tests replace these with
 * `mock.method(fsHooks, 'name', ...)` to exercise rotation and failure paths
 * without touching the shared on-disk log. Behavior-preserving: production code
 * routes every fs call through this object, which defaults to the real `fs`.
 */
export const fsHooks = {
  mkdirSync: fs.mkdirSync as typeof fs.mkdirSync,
  statSync: fs.statSync as typeof fs.statSync,
  unlinkSync: fs.unlinkSync as typeof fs.unlinkSync,
  renameSync: fs.renameSync as typeof fs.renameSync,
  appendFileSync: fs.appendFileSync as typeof fs.appendFileSync,
  openSync: fs.openSync as typeof fs.openSync,
  readSync: fs.readSync as typeof fs.readSync,
  closeSync: fs.closeSync as typeof fs.closeSync,
  symlinkSync: fs.symlinkSync as typeof fs.symlinkSync,
};

let initialized = false;
// Active destination, resolved from the env waterfall at init time.
let logDir = DEFAULT_LOG_DIR;
let logFile = path.join(DEFAULT_LOG_DIR, 'engine.log');
// Running byte count of the active log file, maintained in-process so the size
// bound is enforced on EVERY append (continuous rotation) rather than only once
// at startup. Seeded from the on-disk size during init.
let bytesWritten = 0;
let inMemoryErrors: Array<{ error: string; timestamp: string }> = [];

/**
 * Return a copy of the recent-errors ring (most recent last). Surfaced by
 * `/doctor` so a user hitting a session bug can see the last errors without
 * tailing the on-disk log. Mirrors the reference's getInMemoryErrors.
 */
export function getInMemoryErrors(): Array<{ error: string; timestamp: string }> {
  return [...inMemoryErrors];
}

/**
 * Push a redacted entry onto the bounded recent-errors ring (oldest evicted at
 * {@link MAX_IN_MEMORY_ERRORS}). Single internal helper so `logError` and the
 * severity-filtered diagnostic bridge share ONE ring-maintenance path (no
 * duplicated shift/push logic).
 */
function pushErrorRing(text: string): void {
  const entry = { error: redactCredentials(text), timestamp: new Date().toISOString() };
  if (inMemoryErrors.length >= MAX_IN_MEMORY_ERRORS) {
    inMemoryErrors.shift(); // drop the oldest
  }
  inMemoryErrors.push(entry);
}

/** Test-only seam: force the next log() call to re-run directory setup/rotation. */
export function __resetLoggerForTest(): void {
  initialized = false;
  bytesWritten = 0;
  logDir = DEFAULT_LOG_DIR;
  logFile = path.join(DEFAULT_LOG_DIR, 'engine.log');
  inMemoryErrors = [];
  pendingWrites = [];
  runtimeDebugEnabled = false;
  apiRequestRing.length = 0;
  debugFilterCache.clear();
  for (const writer of diagnosticsWriters.values()) writer.dispose();
  diagnosticsWriters.clear();
}

/** Rotate the current log to `.prev`, discarding any older backup. */
function rotate(): void {
  const backup = logFile + '.prev';
  try { fsHooks.unlinkSync(backup); } catch {}
  try { fsHooks.renameSync(logFile, backup); } catch {}
  bytesWritten = 0;
}
let pendingWrites: string[] = [];

/** Append a line to the active log file, returning whether the write succeeded. */
function tryAppend(line: string): boolean {
  try {
    fsHooks.appendFileSync(logFile, line, { mode: FILE_MODE });
    return true;
  } catch {
    return false;
  }
}

/** Buffer a line for later replay, evicting the oldest once the bound is hit. */
function enqueuePending(line: string): void {
  if (pendingWrites.length >= MAX_PENDING_WRITES) pendingWrites.shift();
  pendingWrites.push(line);
}

/**
 * Replay buffered lines in order. Idempotent — a no-op when nothing is queued.
 * Stops at the first line that still cannot be written (keeping it and the
 * remainder buffered so ordering is preserved for a later attempt). Returns the
 * count of lines successfully drained. Introspection/manual-drain surface for the
 * deferred-write queue (also invoked automatically by {@link log}).
 */
export function flushPendingLogWrites(): number {
  let drained = 0;
  while (pendingWrites.length > 0) {
    const line = pendingWrites[0]!;
    if (!tryAppend(line)) break;
    pendingWrites.shift();
    bytesWritten += Buffer.byteLength(line);
    drained++;
  }
  return drained;
}

function ensureDir(): void {
  if (initialized) return;
  const target = resolveLogTarget();
  logDir = target.dir;
  logFile = target.file;
  try {
    fsHooks.mkdirSync(logDir, { recursive: true, mode: DIR_MODE });
    // Seed the in-process byte count from the existing file and rotate if it is
    // already over the cap (e.g. left over from a previous run).
    try {
      const stat = fsHooks.statSync(logFile);
      bytesWritten = stat.size;
      if (stat.size > MAX_LOG_SIZE) {
        rotate();
      }
    } catch {
      bytesWritten = 0;
    }
    updateLatestSymlink();
    initialized = true;
  } catch {}
}

// ---------------------------------------------------------------------------
// Debug-mode enable gate (env/flag + mid-session runtime toggle).
//
// AMI keeps its default-ON logging (a superset perk: diagnostics are always
// captured so a bug report never lacks the log). These knobs are the
// Claude-compatible debug-mode signal that additionally FORCES logging on where
// it would otherwise be suppressed (notably under `NODE_ENV=test`), and provide
// the on/off affordance the reference exposes via `DEBUG`/`--debug`. Mirrors the reference
// Code's isDebugMode / enableDebugLogging.
// ---------------------------------------------------------------------------
// Mid-session runtime enable (the `/debug on` path). Starts false.
let runtimeDebugEnabled = false;

/**
 * Whether debug mode is active: the union of the `DEBUG` / `DEBUG_SDK` /
 * `AMI_DEBUG` env vars, the `--debug` / `-d` / `--debug=<pattern>` argv flags,
 * the stderr-routing flag, and a mid-session runtime enable. Evaluated per call
 * (no memoization) so a value set in-session — or in a test — takes effect
 * immediately. Mirrors the reference's isDebugMode.
 */
export function isDebugMode(): boolean {
  return (
    runtimeDebugEnabled ||
    isEnvTruthy(process.env.DEBUG) ||
    isEnvTruthy(process.env.DEBUG_SDK) ||
    isEnvTruthy(process.env.AMI_DEBUG) ||
    process.argv.includes('--debug') ||
    process.argv.includes('-d') ||
    process.argv.some((a) => a.startsWith('--debug=')) ||
    isDebugToStderr()
  );
}

/**
 * Turn debug logging on for the rest of the session (the `/debug on` path), so
 * capture can start WITHOUT a restart — even under test-env suppression.
 * Idempotent; returns whether debug mode was ALREADY active before the call.
 * Mirrors the reference's enableDebugLogging.
 */
export function enableDebugLogging(): boolean {
  const wasActive = isDebugMode();
  runtimeDebugEnabled = true;
  return wasActive;
}

/**
 * Boundary gate applied before any at-rest write (mirrors the reference's
 * shouldLogDebugMessage prelude):
 *   1. a hard non-node-runtime guard so logging never throws in a degraded env;
 *   2. `NODE_ENV=test` suppression — avoids cross-test contamination of the
 *      shared log file — UNLESS debug mode is explicitly requested;
 *   3. the `AMI_DISABLE_DEBUG_LOG` data-minimization opt-out (absolute: an
 *      explicit opt-out is honored even in debug mode).
 */
function shouldPersistToDisk(): boolean {
  if (!runtimeHooks.nodeVersion()) return false;
  if (process.env.NODE_ENV === 'test' && !isDebugMode()) return false;
  if (isEnvTruthy(process.env.AMI_DISABLE_DEBUG_LOG)) return false;
  return true;
}

export function log(
  component: string,
  msg: string,
  data?: Record<string, unknown>,
  level: LogLevel = 'debug',
): void {
  // Verbosity gate: drop records below the active minimum severity, and records
  // that an active component/category filter excludes, before any disk work.
  if (LEVEL_ORDER[level] < LEVEL_ORDER[getMinLogLevel()]) return;
  const filter = parseDebugFilter(process.env.AMI_DEBUG_COMPONENTS);
  if (!shouldShowDebugCategories(categoriesForLine(component, msg), filter)) return;
  const ts = new Date().toISOString();
  const dataStr = data ? ' ' + JSON.stringify(data) : '';
  // Escape newlines in the free-form fields, then redact any credentials before
  // the record ever touches disk (tool inputs/outputs and error text can echo
  // API keys, Bearer headers, etc.).
  const composed = `[${ts}] [${level.toUpperCase()}] [${escapeLogField(component)}] ${escapeLogField(msg)}${dataStr}`;
  const line = redactCredentials(composed) + '\n';

  // Live stderr routing: stream the formatted line to stderr and bypass the file
  // writer entirely (parity with the reference's --debug-to-stderr).
  if (isDebugToStderr()) {
    try { process.stderr.write(line); } catch {}
    return;
  }

  // Boundary gate (non-node runtime / NODE_ENV=test / AMI_DISABLE_DEBUG_LOG
  // data-minimization opt-out) — skip all at-rest work when it says so. Mirrors
  // the reference's shouldLogDebugMessage prelude.
  if (!shouldPersistToDisk()) return;

  ensureDir();
  const size = Buffer.byteLength(line);
  // Continuous bound: rotate before the active file would exceed the cap.
  if (bytesWritten > 0 && bytesWritten + size > MAX_LOG_SIZE) {
    rotate();
  }
  // Replay any lines deferred by an earlier failed write first, so ordering is
  // preserved, then write this line.
  flushPendingLogWrites();
  if (tryAppend(line)) {
    bytesWritten += size;
    return;
  }
  // Self-heal: the directory may have been removed mid-session (temp cleanup,
  // another process). Recreate it and retry once.
  try { fsHooks.mkdirSync(logDir, { recursive: true, mode: DIR_MODE }); } catch {}
  if (tryAppend(line)) {
    bytesWritten += size;
    return;
  }
  // Still unwritable — DEFER the line for replay on a later successful write
  // rather than dropping it.
  enqueuePending(line);
}

export function logToolCall(
  toolName: string,
  input: Record<string, unknown>,
  output: string,
  isError: boolean,
  durationMs: number,
): void {
  // Route both payloads through the one configurable debug-payload formatter:
  // the input is redacted-then-truncated (debugBody), the output is
  // newline-collapsed-then-truncated (debugTruncate). Both reveal the original
  // length via a `... (N chars)` suffix instead of silently clipping.
  log('tool', `${toolName} (${durationMs}ms) ${isError ? 'ERROR' : 'OK'}`, {
    input: debugBody(input),
    output: debugTruncate(output),
    isError,
    durationMs,
  });
}

export function logApiCall(
  model: string,
  messageCount: number,
  toolCount: number,
  thinkingEnabled: boolean,
): void {
  log('api', `call model=${model} messages=${messageCount} tools=${toolCount} thinking=${thinkingEnabled}`);
  // Feed the bounded recent-request diagnostic ring so a bug report / `/doctor` can
  // attach the last few calls that preceded a failure (not just the latest).
  captureApiRequest({ model, messageCount, toolCount, thinkingEnabled });
}

export function logApiResponse(
  finishReason: string,
  contentLength: number,
  toolCallCount: number,
  usage?: { promptTokens: number; completionTokens: number },
): void {
  log('api', `response finish=${finishReason} content=${contentLength}chars toolCalls=${toolCallCount}`, usage as Record<string, unknown> | undefined);
}

export function logError(component: string, error: string, context?: Record<string, unknown>): void {
  // Enrich with HTTP/network request context (url/status/server-body) when the
  // caller threads the originating error object through `context.httpError`.
  // The raw error object itself is NOT written to the structured `data` (it may
  // be large/circular and leak internals) — only the extracted bracketed prefix.
  const { httpError, ...restContext } = context ?? {};
  const enriched = `${httpErrorContext(httpError)}${error}`;
  // Every error entry is stamped with the standard enrichment fields (sessionId
  // + cwd + version) so it is attributable to the exact run; caller-supplied
  // structured context is merged on top (and can override). Mirrors the reference's
  // errorLogSink entry enrichment.
  const dataContext = { ...standardLogFields(), ...restContext };

  // Hard-fail mode: surface silently-swallowed errors by throwing immediately,
  // so a logged error crashes the run instead of being buried. Checked FIRST so
  // it fires regardless of the error-reporting opt-out. Mirrors the reference's
  // `--hard-fail` (AMI gates on the `AMI_LOG_HARD_FAIL` env flag).
  if (isEnvTruthy(process.env.AMI_LOG_HARD_FAIL)) {
    throw new Error(`[HARD FAIL] [${component}] ${enriched}`);
  }

  // Opt-out of error capture entirely (data minimization). Mirrors the reference's
  // `DISABLE_ERROR_REPORTING` knob: skip both the in-memory ring and disk.
  if (isEnvTruthy(process.env.DISABLE_ERROR_REPORTING)) return;

  // Always capture the error in the in-memory ring (redacted, bounded) so it is
  // retrievable for diagnostics/bug reports even if the disk write is filtered
  // or fails. Mirrors the reference's "always add to in-memory log" behavior.
  pushErrorRing(`[${component}] ${enriched}`);
  log(component, `ERROR: ${enriched}`, dataContext, 'error');
  // Also append a machine-readable record to the dated structured error sink
  // (no-op unless AMI_ERRORS_DIR is configured). Carries the same enrichment so a
  // per-run JSONL error log is independently greppable. Mirrors the reference's
  // errorLogSink dated-JSONL file.
  appendErrorRecord({ level: 'error', component, error: enriched, ...dataContext });
}

// Resolved ONCE per process so every error in a run lands in the same JSONL file
// (a stable per-run log), matching the reference's module-level dated filename.
const ERROR_LOG_DATE = dateToFilename(new Date());

/**
 * Path to the active structured error-log file — `<errorsDir>/<date>.jsonl` — or
 * null when the sink is not configured. Introspection surface (consumed by
 * `/debug`) so a user/bug-report flow can point straight at the machine-readable
 * error log. Mirrors the reference's `getErrorsPath`.
 */
export function getErrorsPath(): string | null {
  const dir = getErrorsDir();
  return dir ? path.join(dir, `${ERROR_LOG_DATE}.jsonl`) : null;
}

/**
 * Append one credential-redacted `{timestamp, ...record}` JSONL line to the dated
 * error sink. No-op — zero cost — when the sink is unconfigured. Reuses the
 * self-healing {@link appendDiagnosticsRaw} sink (shared missing-dir mkdir+retry),
 * so there is no second copy of the append-with-recovery logic. The whole
 * serialized line is redacted so a secret in the error text or context never
 * reaches disk, and JSON encoding keeps every record on its own parseable line.
 */
function appendErrorRecord(record: Record<string, unknown>): void {
  const file = getErrorsPath();
  if (!file) return;
  const entry = { timestamp: new Date().toISOString(), ...record };
  appendDiagnosticsRaw(file, redactCredentials(JSON.stringify(entry)) + '\n');
}

// ---------------------------------------------------------------------------
// Memory-bounded tail read of the (unbounded) log — for /doctor & bug reports.
// ---------------------------------------------------------------------------
const TAIL_READ_BYTES = 64 * 1024;

/**
 * Read the last N lines of a log file WITHOUT loading the whole thing: stat for
 * the size, positioned-read the trailing window (default 64KB), then keep the
 * last `maxLines` lines. Debug logs grow unbounded in long sessions, so a full
 * read would spike RSS. A missing file is special-cased (`missing: true`) so a
 * caller can distinguish "no log yet" from a real read failure. Mirrors the reference
 * Code's bundled debug-skill tail read.
 */
export function readLogTail(opts?: { file?: string; maxBytes?: number; maxLines?: number }): LogTailResult {
  const file = opts?.file ?? getLogPath();
  const maxBytes = opts?.maxBytes ?? TAIL_READ_BYTES;
  const maxLines = opts?.maxLines ?? DEFAULT_TAIL_LINES;
  let fd: number | undefined;
  try {
    const stat = fsHooks.statSync(file);
    const readSize = Math.min(stat.size, maxBytes);
    const startOffset = stat.size - readSize;
    const buffer = Buffer.alloc(readSize);
    fd = fsHooks.openSync(file, 'r');
    const bytesRead = fsHooks.readSync(fd, buffer, 0, readSize, startOffset);
    const lines = buffer
      .toString('utf-8', 0, bytesRead)
      .split('\n')
      .filter((l) => l.length > 0)
      .slice(-maxLines);
    return { ok: true, lines, size: stat.size };
  } catch (e) {
    if (isENOENT(e)) return { ok: false, missing: true };
    return { ok: false, missing: false, error: e instanceof Error ? e.message : String(e) };
  } finally {
    if (fd !== undefined) {
      try { fsHooks.closeSync(fd); } catch {}
    }
  }
}

/**
 * Record a structured diagnostic event. Always writes to the on-disk debug log
 * at `level`; additionally, when the severity is surfaced
 * ({@link shouldSurfaceToTelemetry}), pushes a compact entry onto the retrievable
 * in-memory error ring so warn/error diagnostics show up in bug reports while
 * low-severity chatter does not.
 */
/**
 * Append a structured `{timestamp, level, event, data}` JSONL record to the file
 * named ONLY by `AMI_DIAGNOSTICS_FILE` (Claude-compatible
 * `CLAUDE_CODE_DIAGNOSTICS_FILE` as a fallback). No-op — zero cost — when the env
 * var is unset. This is a machine-monitored health channel SEPARATE from the
 * human debug log and the analytics pipeline; by contract callers MUST NOT pass
 * PII (file paths, prompts, project/repo names), only coarse level/event/data.
 * JSON-encoded one-object-per-line so each record is independently parseable and
 * cannot be split by embedded newlines. Mirrors the reference's
 * `logForDiagnosticsNoPII`.
 */
/**
 * Append one JSONL line to `file`, self-healing a missing parent directory with a
 * single mkdir + retry. Shared by the diagnostics buffered writer's sink so the
 * batched and immediate paths recover identically. Best-effort — a persistent
 * failure is swallowed (a diagnostics write must never break the run).
 */
function appendDiagnosticsRaw(file: string, content: string): void {
  try {
    fsHooks.appendFileSync(file, content);
  } catch {
    // The directory may not exist yet — create it and retry once.
    try {
      fsHooks.mkdirSync(path.dirname(file), { recursive: true });
      fsHooks.appendFileSync(file, content);
    } catch {}
  }
}

// Per-path batched writers for the diagnostics channel. Keyed by destination file
// so concurrent channels (unusual, but supported) don't share a buffer. By
// default each writer is in immediate mode (every record hits disk at once, same
// as before); `AMI_DIAGNOSTICS_BUFFERED` opts into coalesced batching to cut
// syscalls on a high-volume diagnostics stream, with `flush()` draining on exit.
const diagnosticsWriters = new Map<string, BufferedWriter>();
let exitFlushRegistered = false;

function getDiagnosticsWriter(file: string): BufferedWriter {
  let writer = diagnosticsWriters.get(file);
  if (!writer) {
    const buffered = isEnvTruthy(process.env.AMI_DIAGNOSTICS_BUFFERED);
    writer = createBufferedWriter({
      writeFn: (content) => appendDiagnosticsRaw(file, content),
      flushIntervalMs: getPositiveIntEnv('AMI_DIAGNOSTICS_FLUSH_INTERVAL_MS', 1000),
      maxBufferSize: getPositiveIntEnv('AMI_DIAGNOSTICS_MAX_BUFFER', 100),
      immediateMode: !buffered,
    });
    diagnosticsWriters.set(file, writer);
    // Register a one-time exit hook the first time a batching writer exists so a
    // buffered tail is flushed synchronously before the process ends.
    if (buffered && !exitFlushRegistered) {
      exitFlushRegistered = true;
      process.once('exit', flushDiagnosticsWriters);
    }
  }
  return writer;
}

/**
 * Synchronously flush every open diagnostics writer's buffered tail to disk.
 * Introspection/diagnostic surface for the batched diagnostics channel: a caller
 * (or the process-exit hook) can force a drain so no batched record is lost.
 */
export function flushDiagnosticsWriters(): void {
  for (const writer of diagnosticsWriters.values()) writer.flush();
}

export function logForDiagnosticsNoPII(level: LogLevel, event: string, data?: Record<string, unknown>): void {
  const file = (process.env.AMI_DIAGNOSTICS_FILE ?? process.env.CLAUDE_CODE_DIAGNOSTICS_FILE)?.trim();
  if (!file) return;
  const entry = { timestamp: new Date().toISOString(), level, event, data: data ?? {} };
  const line = JSON.stringify(entry) + '\n';
  getDiagnosticsWriter(file).write(line);
}

export function recordDiagnostic(level: LogLevel, event: string, data?: Record<string, unknown>): void {
  log('diag', event, data, level);
  // Also emit to the PII-free JSONL diagnostics channel (no-op when unconfigured)
  // so the timing/lifecycle events this records feed container-side monitoring.
  logForDiagnosticsNoPII(level, event, data);
  if (shouldSurfaceToTelemetry(level)) {
    const dataStr = data ? ' ' + JSON.stringify(data) : '';
    pushErrorRing(`[diag:${level}] ${event}${dataStr}`);
  }
}

/**
 * Wrap an async operation with start/complete/fail diagnostic timing events
 * (`<event>_started`, `<event>_completed`, `<event>_failed`) carrying
 * `duration_ms`, plus automatic slow-operation detection: if the op exceeds
 * `AMI_SLOW_OPERATION_THRESHOLD_MS` a `[SLOW OPERATION DETECTED]` warn record is
 * emitted. `getData` optionally extracts extra fields from the result for the
 * completion event. Mirrors the reference's `withDiagnosticsTiming` +
 * slow-operation logging. The original error is always rethrown unchanged.
 */
export async function withDiagnosticsTiming<T>(
  event: string,
  fn: () => Promise<T>,
  getData?: (result: T) => Record<string, unknown>,
): Promise<T> {
  const start = Date.now();
  recordDiagnostic('debug', `${event}_started`);
  try {
    const result = await fn();
    const duration = Date.now() - start;
    const extra = getData ? getData(result) : {};
    recordDiagnostic('debug', `${event}_completed`, { duration_ms: duration, ...extra });
    maybeWarnSlowOp(event, duration);
    return result;
  } catch (err) {
    const duration = Date.now() - start;
    recordDiagnostic('error', `${event}_failed`, { duration_ms: duration });
    maybeWarnSlowOp(event, duration);
    throw err;
  }
}

/** Emit a `[SLOW OPERATION DETECTED]` warn record when `duration` exceeds the threshold. */
function maybeWarnSlowOp(description: string, duration: number): void {
  if (duration > getSlowOpThresholdMs()) {
    recordDiagnostic('warn', `[SLOW OPERATION DETECTED] ${description} (${duration}ms)`);
  }
}

export function getLogPath(): string {
  return initialized ? logFile : resolveLogTarget().file;
}
