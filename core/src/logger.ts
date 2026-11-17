import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { redactCredentials } from './credential-guard';
import { isEnvTruthy } from './privacy-level';
import { isSafeSegment } from './skill-catalog';
import { createBufferedWriter, type BufferedWriter } from './buffered-writer';
import { AMI_VERSION } from './version';

const DEFAULT_LOG_DIR = path.join(os.tmpdir(), 'superinference', 'core');
const MAX_LOG_SIZE = 10 * 1024 * 1024; // 10MB

// Restrictive modes: the log records raw tool inputs/outputs and error text that
// can contain secrets and file contents. On a shared host the predictable /tmp
// path would otherwise be world-readable, so the directory is created 0700 and
// the file 0600 (owner-only) — a data-minimization/least-exposure defense.
const DIR_MODE = 0o700;
const FILE_MODE = 0o600;

// ---------------------------------------------------------------------------
// Configurable debug-payload formatter. ONE tunable limit governs how much of a
// debug body (tool input/output, API payload, etc.) is persisted; both helpers
// collapse newlines into `\n` and append a `... (N chars)` suffix that reveals
// the ORIGINAL length so a truncated record is never mistaken for the whole
// payload. Mirrors Claude Code's DEBUG_MSG_LIMIT / debugTruncate / debugBody
// (bridge/debugUtils.ts) — a single knob instead of scattered magic slices.
// ---------------------------------------------------------------------------
const DEFAULT_DEBUG_MSG_LIMIT = 2000;

/** Resolve the debug-body length cap (`AMI_DEBUG_MSG_LIMIT`, default 2000). */
function getDebugMsgLimit(): number {
  const raw = process.env.AMI_DEBUG_MSG_LIMIT;
  if (raw !== undefined) {
    const parsed = Number(raw);
    if (Number.isInteger(parsed) && parsed >= 0) return parsed;
  }
  return DEFAULT_DEBUG_MSG_LIMIT;
}

/**
 * Collapse newlines and truncate a string for debug logging, appending
 * `... (N chars)` with the original length when it overflows the limit. Mirrors
 * Claude Code's `debugTruncate`.
 */
export function debugTruncate(s: string): string {
  const flat = s.replace(/\r\n|\r|\n/g, '\\n');
  const limit = getDebugMsgLimit();
  if (flat.length <= limit) return flat;
  return flat.slice(0, limit) + `... (${flat.length} chars)`;
}

/**
 * Serialize a JSON-serializable value, REDACT credentials, THEN truncate to the
 * limit (redact-before-truncate so a secret split across the cut boundary is
 * still removed). Circular/unstringifiable values fall back to `String(data)`.
 * Mirrors Claude Code's `debugBody`.
 */
export function debugBody(data: unknown): string {
  let raw: string;
  if (typeof data === 'string') {
    raw = data;
  } else {
    try {
      raw = JSON.stringify(data) ?? String(data);
    } catch {
      raw = String(data);
    }
  }
  const s = redactCredentials(raw);
  const limit = getDebugMsgLimit();
  if (s.length <= limit) return s;
  return s.slice(0, limit) + `... (${s.length} chars)`;
}

// ---------------------------------------------------------------------------
// Severity levels (ordered) — a verbosity knob, not a new log channel.
// ---------------------------------------------------------------------------
/**
 * Ordered log severities. Records tagged below the active minimum level are
 * dropped before they touch disk, letting a user raise or lower verbosity at
 * runtime without a rebuild. Mirrors Claude Code's DebugLogLevel.
 */
export type LogLevel = 'verbose' | 'debug' | 'info' | 'warn' | 'error';

const LEVEL_ORDER: Record<LogLevel, number> = {
  verbose: 0,
  debug: 1,
  info: 2,
  warn: 3,
  error: 4,
};

const DEFAULT_MIN_LEVEL: LogLevel = 'debug';

/**
 * Minimum severity to persist. Reads `AMI_DEBUG_LOG_LEVEL` first, then the
 * Claude-compatible `CLAUDE_CODE_DEBUG_LOG_LEVEL`, and falls back safely to
 * `'debug'` on an unset or invalid value (so `verbose` high-volume diagnostics
 * are filtered out by default). Read per call so a value set mid-session (and in
 * tests) takes effect immediately. Mirrors Claude Code's getMinDebugLogLevel.
 */
function getMinLogLevel(): LogLevel {
  const raw = (process.env.AMI_DEBUG_LOG_LEVEL ?? process.env.CLAUDE_CODE_DEBUG_LOG_LEVEL)
    ?.toLowerCase()
    .trim();
  if (raw && Object.prototype.hasOwnProperty.call(LEVEL_ORDER, raw)) {
    return raw as LogLevel;
  }
  return DEFAULT_MIN_LEVEL;
}

// ---------------------------------------------------------------------------
// Debug component/category filter (AMI's analogue of Claude Code's
// `--debug=<pattern>` / debugFilter.ts). The `AMI_DEBUG_COMPONENTS` spec is a
// comma list that selects which log lines reach disk by their component tag and
// any category heuristically extracted from the message:
//   - `api,hooks`   → inclusive: show ONLY the `api` and `hooks` categories
//   - `!1p,!file`   → exclusive: show everything EXCEPT `1p` and `file`
//   - `*` / empty / undefined / whitespace → no filtering (show all; default)
//   - a mixed spec (`api,!file`) is invalid and falls back to show-all
// This mirrors Claude Code's parseDebugFilter/extractDebugCategories/
// shouldShowDebugCategories so a user can narrow noisy debug output by subsystem.
// ---------------------------------------------------------------------------

/** Parsed debug-filter configuration. Mirrors Claude Code's `DebugFilter`. */
export interface DebugFilter {
  include: string[];
  exclude: string[];
  isExclusive: boolean;
}

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
 * Claude Code's `parseDebugFilter`.
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

/**
 * Extract lowercase categories from a free-form message, so a filter can match
 * on content as well as the explicit component tag. Supports `category: msg`,
 * `[CATEGORY] msg`, `MCP server "name": msg`, and a `1p event:` marker. Heuristic
 * matches are bounded (secondary category must be <30 chars and contain no space)
 * to keep pathological input from being treated as a category, and the result is
 * de-duped. Mirrors Claude Code's `extractDebugCategories`.
 */
export function extractDebugCategories(message: string): string[] {
  const categories: string[] = [];

  // MCP pattern first, to avoid the generic prefix matcher stealing it.
  const mcpMatch = message.match(/^MCP server ["']([^"']+)["']/);
  if (mcpMatch && mcpMatch[1]) {
    categories.push('mcp');
    categories.push(mcpMatch[1].toLowerCase());
  } else {
    const prefixMatch = message.match(/^([^:[]+):/);
    if (prefixMatch && prefixMatch[1]) {
      categories.push(prefixMatch[1].trim().toLowerCase());
    }
  }

  const bracketMatch = message.match(/^\[([^\]]+)]/);
  if (bracketMatch && bracketMatch[1]) {
    categories.push(bracketMatch[1].trim().toLowerCase());
  }

  if (message.toLowerCase().includes('1p event:')) {
    categories.push('1p');
  }

  const secondaryMatch = message.match(/:\s*([^:]+):/);
  if (secondaryMatch && secondaryMatch[1]) {
    // Strip a trailing qualifier word so "Installation type: dev" → "installation".
    // (Done in code rather than in the regex to keep the pattern ReDoS-safe —
    // star height 1, no quantifier nested under the optional group.)
    const secondary = secondaryMatch[1].trim().toLowerCase().replace(/\s+(?:type|mode|status|event)$/, '');
    // Only accept a reasonable category name — reject absurdly long or
    // whitespace-laden fragments.
    if (secondary.length < 30 && !secondary.includes(' ')) {
      categories.push(secondary);
    }
  }

  return Array.from(new Set(categories));
}

/**
 * Decide whether a line with the given categories passes the filter. Fail-closed:
 * when a filter is active but no categories were found, the line is dropped in
 * BOTH modes (uncategorized output is excluded by default when a user has scoped
 * debugging). Mirrors Claude Code's `shouldShowDebugCategories`.
 */
export function shouldShowDebugCategories(categories: string[], filter: DebugFilter | null): boolean {
  if (!filter) return true;
  if (categories.length === 0) return false;
  if (filter.isExclusive) {
    return !categories.some((cat) => filter.exclude.includes(cat));
  }
  return categories.some((cat) => filter.include.includes(cat));
}

/**
 * Build the category set for a log line from its explicit component tag plus any
 * categories heuristically extracted from the message, de-duped. The explicit
 * component leads so an empty/blank component yields a purely message-derived set
 * (and an empty set when nothing is extractable — which the fail-closed boundary
 * then drops under an active filter).
 */
function categoriesForLine(component: string, msg: string): string[] {
  const cats = extractDebugCategories(msg);
  const c = component.trim().toLowerCase();
  if (c) cats.unshift(c);
  return Array.from(new Set(cats));
}

// ---------------------------------------------------------------------------
// Destination resolution — override the on-disk log location.
// ---------------------------------------------------------------------------
/**
 * Resolve the active log directory + file via a path waterfall (mirrors Claude
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
 * Resolve the active log FILE NAME. When `AMI_DEBUG_SESSION_ID` is set to a
 * path-safe segment, each session writes to its own `<sessionId>.log` so
 * concurrent sessions never interleave and each run is independently readable;
 * otherwise the shared `engine.log`. The id is validated with the same
 * path-safety guard the skill catalog uses, so a malicious value cannot escape
 * the log directory. Mirrors Claude Code's per-session `<sessionId>.txt`.
 */
function logFileName(): string {
  const sid = process.env.AMI_DEBUG_SESSION_ID?.trim();
  if (sid && isSafeSegment(sid)) return `${sid}.log`;
  return 'engine.log';
}

/** Whether a per-session log file (not the shared `engine.log`) is active. */
function hasPerSessionFile(): boolean {
  const sid = process.env.AMI_DEBUG_SESSION_ID?.trim();
  return !!sid && isSafeSegment(sid) && !process.env.AMI_DEBUG_FILE?.trim();
}

/**
 * For per-session runs, maintain a `latest` symlink in the log directory that
 * points at the active session file so a bug-report/`/doctor`/share flow can
 * always jump straight to the current session's log. Best-effort: silently
 * no-ops on a platform/filesystem that forbids symlinks. Mirrors Claude Code's
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

// ---------------------------------------------------------------------------
// In-memory recent-errors ring (bug-report / diagnostics introspection).
// ---------------------------------------------------------------------------
const MAX_IN_MEMORY_ERRORS = 100;
let inMemoryErrors: Array<{ error: string; timestamp: string }> = [];

/**
 * Return a copy of the recent-errors ring (most recent last). Surfaced by
 * `/doctor` so a user hitting a session bug can see the last errors without
 * tailing the on-disk log. Mirrors Claude Code's getInMemoryErrors.
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

/**
 * Standard enrichment fields stamped onto every persisted error entry (the dated
 * JSONL error sink below) AND merged into the structured `data` of each
 * `logError()` debug line. Mirrors Claude Code's errorLogSink `appendToLog`,
 * which tags every record with `cwd` + `sessionId` + `version` so a later
 * bug-report/triage flow can attribute an error to the exact run, working
 * directory, and build that produced it. `sessionId` reuses `AMI_DEBUG_SESSION_ID`
 * (the same id that names the per-session log file, path-safety validated);
 * `version` is the single-source {@link AMI_VERSION}; `cwd` is the live process
 * working directory.
 */
function standardLogFields(): { sessionId: string; cwd: string; version: string } {
  const sid = process.env.AMI_DEBUG_SESSION_ID?.trim();
  return {
    sessionId: sid && isSafeSegment(sid) ? sid : 'unknown',
    cwd: process.cwd(),
    version: AMI_VERSION,
  };
}

/** Test-only seam: force the next log() call to re-run directory setup/rotation. */
export function __resetLoggerForTest(): void {
  initialized = false;
  bytesWritten = 0;
  logDir = DEFAULT_LOG_DIR;
  logFile = path.join(DEFAULT_LOG_DIR, 'engine.log');
  inMemoryErrors = [];
  lastApiRequest = null;
  debugFilterCache.clear();
  for (const writer of diagnosticsWriters.values()) writer.dispose();
  diagnosticsWriters.clear();
}

/**
 * Escape embedded CR/LF so a single logical record cannot be split into multiple
 * physical lines (log-injection / line-forging defense). Only the free-form
 * `component` and `msg` need this; structured `data` is already newline-escaped
 * by JSON.stringify.
 */
function escapeLogField(s: string): string {
  return s.replace(/\r\n|\r|\n/g, '\\n');
}

/** Rotate the current log to `.prev`, discarding any older backup. */
function rotate(): void {
  const backup = logFile + '.prev';
  try { fsHooks.unlinkSync(backup); } catch {}
  try { fsHooks.renameSync(logFile, backup); } catch {}
  bytesWritten = 0;
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
// Output routing + at-rest persistence gate.
// ---------------------------------------------------------------------------
/**
 * Whether debug output is routed to stderr instead of the on-disk file. Honors
 * the `AMI_DEBUG_TO_STDERR` env flag and the Claude-compatible
 * `--debug-to-stderr` / `-d2e` argv flags. When active, `log()` writes the
 * formatted line to stderr and bypasses the file writer, so a user can stream
 * debug output live without a log file. Mirrors Claude Code's `isDebugToStdErr`.
 */
function isDebugToStderr(): boolean {
  return (
    isEnvTruthy(process.env.AMI_DEBUG_TO_STDERR) ||
    process.argv.includes('--debug-to-stderr') ||
    process.argv.includes('-d2e')
  );
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
  // writer entirely (parity with Claude's --debug-to-stderr).
  if (isDebugToStderr()) {
    try { process.stderr.write(line); } catch {}
    return;
  }

  // Data-minimization opt-out: a user can disable at-rest debug-log persistence
  // so sensitive tool inputs/outputs and error text never touch disk. Mirrors
  // Claude Code's gating of whether debug messages are written at all.
  if (isEnvTruthy(process.env.AMI_DISABLE_DEBUG_LOG)) return;

  ensureDir();
  const size = Buffer.byteLength(line);
  // Continuous bound: rotate before the active file would exceed the cap.
  if (bytesWritten > 0 && bytesWritten + size > MAX_LOG_SIZE) {
    rotate();
  }
  try {
    fsHooks.appendFileSync(logFile, line, { mode: FILE_MODE });
    bytesWritten += size;
  } catch {
    // Self-heal: the directory may have been removed mid-session (temp cleanup,
    // another process). Recreate it and retry once so logging does not silently
    // die for the rest of the run.
    try {
      fsHooks.mkdirSync(logDir, { recursive: true, mode: DIR_MODE });
      fsHooks.appendFileSync(logFile, line, { mode: FILE_MODE });
      bytesWritten += size;
    } catch {}
  }
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
  // structured context is merged on top (and can override). Mirrors Claude Code's
  // errorLogSink entry enrichment.
  const dataContext = { ...standardLogFields(), ...restContext };

  // Hard-fail mode: surface silently-swallowed errors by throwing immediately,
  // so a logged error crashes the run instead of being buried. Checked FIRST so
  // it fires regardless of the error-reporting opt-out. Mirrors Claude Code's
  // `--hard-fail` (AMI gates on the `AMI_LOG_HARD_FAIL` env flag).
  if (isEnvTruthy(process.env.AMI_LOG_HARD_FAIL)) {
    throw new Error(`[HARD FAIL] [${component}] ${enriched}`);
  }

  // Opt-out of error capture entirely (data minimization). Mirrors Claude Code's
  // `DISABLE_ERROR_REPORTING` knob: skip both the in-memory ring and disk.
  if (isEnvTruthy(process.env.DISABLE_ERROR_REPORTING)) return;

  // Always capture the error in the in-memory ring (redacted, bounded) so it is
  // retrievable for diagnostics/bug reports even if the disk write is filtered
  // or fails. Mirrors Claude Code's "always add to in-memory log" behavior.
  pushErrorRing(`[${component}] ${enriched}`);
  log(component, `ERROR: ${enriched}`, dataContext, 'error');
  // Also append a machine-readable record to the dated structured error sink
  // (no-op unless AMI_ERRORS_DIR is configured). Carries the same enrichment so a
  // per-run JSONL error log is independently greppable. Mirrors Claude Code's
  // errorLogSink dated-JSONL file.
  appendErrorRecord({ level: 'error', component, error: enriched, ...dataContext });
}

// ---------------------------------------------------------------------------
// Dated structured JSONL error sink (opt-in, PII-redacted) — a machine-readable
// per-run error log SEPARATE from the human debug log. Mirrors Claude Code's
// errorLogSink: one date-stamped `.jsonl` file whose records carry the standard
// enrichment fields, so errors from a run can be parsed/triaged after the fact.
// ---------------------------------------------------------------------------
/**
 * Convert a Date to a filesystem-safe, lexically-sortable stamp by replacing the
 * `:` and `.` that some filesystems forbid with `-` (e.g.
 * `2026-11-17T08-30-00-000Z`). Mirrors Claude Code's `dateToFilename` so dated
 * log files sort by time and never collide on a colon.
 */
export function dateToFilename(date: Date): string {
  return date.toISOString().replace(/[:.]/g, '-');
}

// Resolved ONCE per process so every error in a run lands in the same JSONL file
// (a stable per-run log), matching Claude Code's module-level dated filename.
const ERROR_LOG_DATE = dateToFilename(new Date());

/**
 * Resolve the structured error-sink directory from `AMI_ERRORS_DIR` (Claude-
 * compatible `CLAUDE_CODE_ERRORS_DIR` fallback), or undefined when neither is
 * set — so the dated JSONL error file is strictly opt-in and zero-cost by
 * default.
 */
function getErrorsDir(): string | undefined {
  const dir = (process.env.AMI_ERRORS_DIR ?? process.env.CLAUDE_CODE_ERRORS_DIR)?.trim();
  return dir || undefined;
}

/**
 * Path to the active structured error-log file — `<errorsDir>/<date>.jsonl` — or
 * null when the sink is not configured. Introspection surface (consumed by
 * `/debug`) so a user/bug-report flow can point straight at the machine-readable
 * error log. Mirrors Claude Code's `getErrorsPath`.
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
// HTTP / network error enrichment (url + status + server body message).
// ---------------------------------------------------------------------------
/**
 * Extract a human-readable server message from an error's response payload.
 * Accepts a raw string body, `{ message }`, or `{ error: { message } }`.
 * Mirrors Claude Code's `extractServerMessage`.
 */
export function extractServerMessage(data: unknown): string | undefined {
  if (typeof data === 'string') return data;
  if (data && typeof data === 'object') {
    const obj = data as Record<string, unknown>;
    if (typeof obj.message === 'string') return obj.message;
    const inner = obj.error;
    if (inner && typeof inner === 'object' && typeof (inner as Record<string, unknown>).message === 'string') {
      return (inner as Record<string, unknown>).message as string;
    }
  }
  return undefined;
}

/**
 * Build a bracketed `[url=…,status=…,body=…] ` prefix from an HTTP/network error
 * object for inclusion in the persisted error log. Duck-types both axios-style
 * (`error.config.url`, `error.response.status`, `error.response.data`) and
 * fetch-style (`error.url`, `error.status`, `error.body`) shapes so no HTTP
 * client dependency is required. Returns `''` when no request context is found.
 * Mirrors the axios-error enrichment in Claude Code's `errorLogSink`.
 */
export function httpErrorContext(error: unknown): string {
  if (!error || typeof error !== 'object') return '';
  const err = error as Record<string, unknown>;
  const config = err.config as Record<string, unknown> | undefined;
  const response = err.response as Record<string, unknown> | undefined;
  const url = (config?.url ?? err.url) as unknown;
  if (typeof url !== 'string' || url === '') return '';

  const parts = [`url=${url}`];
  const status = (response?.status ?? err.status) as unknown;
  if (typeof status === 'number') parts.push(`status=${status}`);
  const body = extractServerMessage(response?.data ?? err.body ?? err.data);
  if (body) parts.push(`body=${body}`);
  return `[${parts.join(',')}] `;
}

// ---------------------------------------------------------------------------
// Memory-bounded tail read of the (unbounded) log — for /doctor & bug reports.
// ---------------------------------------------------------------------------
const TAIL_READ_BYTES = 64 * 1024;
const DEFAULT_TAIL_LINES = 20;

/** Result of {@link readLogTail}: the tail lines + full size, or a miss/error. */
export type LogTailResult =
  | { ok: true; lines: string[]; size: number }
  | { ok: false; missing: boolean; error?: string };

function isENOENT(e: unknown): boolean {
  return !!e && typeof e === 'object' && (e as NodeJS.ErrnoException).code === 'ENOENT';
}

/**
 * Read the last N lines of a log file WITHOUT loading the whole thing: stat for
 * the size, positioned-read the trailing window (default 64KB), then keep the
 * last `maxLines` lines. Debug logs grow unbounded in long sessions, so a full
 * read would spike RSS. A missing file is special-cased (`missing: true`) so a
 * caller can distinguish "no log yet" from a real read failure. Mirrors Claude
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

// ---------------------------------------------------------------------------
// Human-readable value summarizer for diagnostic output.
// ---------------------------------------------------------------------------
/**
 * Compact one value for a diagnostic description: `Array[N]`, `Object{N keys}`,
 * strings truncated to 80 chars with an ellipsis, everything else `String(v)`.
 * Keeps diagnostic lines bounded and readable. Mirrors the per-value summary in
 * Claude Code's slow-operation description builder.
 */
export function summarizeValue(v: unknown): string {
  if (Array.isArray(v)) return `Array[${v.length}]`;
  if (v !== null && typeof v === 'object') return `Object{${Object.keys(v as Record<string, unknown>).length} keys}`;
  if (typeof v === 'string') return v.length > 80 ? `${v.slice(0, 80)}…` : v;
  return String(v);
}

// ---------------------------------------------------------------------------
// Last-API-request capture (for bug reports / diagnostics).
// ---------------------------------------------------------------------------
let lastApiRequest: { timestamp: string; params: Record<string, unknown> } | null = null;

/**
 * Record the most recent API request parameters so a later bug report or
 * `/doctor` can include "what was the last call that failed". String values are
 * credential-redacted on capture so an API key in a header never lingers in
 * memory. Only the latest is kept. Mirrors Claude Code's captureAPIRequest.
 */
export function captureApiRequest(params: Record<string, unknown>): void {
  const redacted: Record<string, unknown> = {};
  for (const [k, val] of Object.entries(params)) {
    redacted[k] = typeof val === 'string' ? redactCredentials(val) : val;
  }
  lastApiRequest = { timestamp: new Date().toISOString(), params: redacted };
}

/** Return a copy of the last captured API request, or null if none. */
export function getLastApiRequest(): { timestamp: string; params: Record<string, unknown> } | null {
  return lastApiRequest ? { timestamp: lastApiRequest.timestamp, params: { ...lastApiRequest.params } } : null;
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
 * cannot be split by embedded newlines. Mirrors Claude Code's
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

/** Resolve a positive-integer env knob, falling back to `fallback` when unset/invalid. */
function getPositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw !== undefined) {
    const parsed = Number(raw);
    if (Number.isInteger(parsed) && parsed > 0) return parsed;
  }
  return fallback;
}

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

/** Slow-operation threshold (ms); off by default. `AMI_SLOW_OPERATION_THRESHOLD_MS`. */
function getSlowOpThresholdMs(): number {
  const raw = process.env.AMI_SLOW_OPERATION_THRESHOLD_MS;
  if (raw !== undefined) {
    const parsed = Number(raw);
    if (!Number.isNaN(parsed) && parsed >= 0) return parsed;
  }
  return Infinity;
}

/**
 * Wrap an async operation with start/complete/fail diagnostic timing events
 * (`<event>_started`, `<event>_completed`, `<event>_failed`) carrying
 * `duration_ms`, plus automatic slow-operation detection: if the op exceeds
 * `AMI_SLOW_OPERATION_THRESHOLD_MS` a `[SLOW OPERATION DETECTED]` warn record is
 * emitted. `getData` optionally extracts extra fields from the result for the
 * completion event. Mirrors Claude Code's `withDiagnosticsTiming` +
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
