import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { redactCredentials } from './credential-guard';

const DEFAULT_LOG_DIR = path.join(os.tmpdir(), 'superinference', 'core');
const MAX_LOG_SIZE = 10 * 1024 * 1024; // 10MB

// Restrictive modes: the log records raw tool inputs/outputs and error text that
// can contain secrets and file contents. On a shared host the predictable /tmp
// path would otherwise be world-readable, so the directory is created 0700 and
// the file 0600 (owner-only) — a data-minimization/least-exposure defense.
const DIR_MODE = 0o700;
const FILE_MODE = 0o600;

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

/**
 * Optional component allow-filter (AMI's analogue of Claude Code's
 * `--debug=<pattern>`). When `AMI_DEBUG_COMPONENTS` is a comma list of component
 * names, only those components are persisted; the sentinel `*` or an unset/empty
 * value logs everything (the default — no behavior change).
 */
function componentAllowed(component: string): boolean {
  const spec = process.env.AMI_DEBUG_COMPONENTS?.trim();
  if (!spec || spec === '*') return true;
  const names = spec
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  if (names.length === 0) return true;
  return names.includes(component.toLowerCase());
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
  return { dir, file: path.join(dir, 'engine.log') };
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

/** Test-only seam: force the next log() call to re-run directory setup/rotation. */
export function __resetLoggerForTest(): void {
  initialized = false;
  bytesWritten = 0;
  logDir = DEFAULT_LOG_DIR;
  logFile = path.join(DEFAULT_LOG_DIR, 'engine.log');
  inMemoryErrors = [];
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
    initialized = true;
  } catch {}
}

export function log(
  component: string,
  msg: string,
  data?: Record<string, unknown>,
  level: LogLevel = 'debug',
): void {
  // Verbosity gate: drop records below the active minimum severity, and records
  // from components not in an explicit allow-filter, before any disk work.
  if (LEVEL_ORDER[level] < LEVEL_ORDER[getMinLogLevel()]) return;
  if (!componentAllowed(component)) return;
  ensureDir();
  const ts = new Date().toISOString();
  const dataStr = data ? ' ' + JSON.stringify(data) : '';
  // Escape newlines in the free-form fields, then redact any credentials before
  // the record ever touches disk (tool inputs/outputs and error text can echo
  // API keys, Bearer headers, etc.).
  const composed = `[${ts}] [${level.toUpperCase()}] [${escapeLogField(component)}] ${escapeLogField(msg)}${dataStr}`;
  const line = redactCredentials(composed) + '\n';
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
  const inputStr = JSON.stringify(input).slice(0, 500);
  const outputStr = output.slice(0, 1000);
  log('tool', `${toolName} (${durationMs}ms) ${isError ? 'ERROR' : 'OK'}`, {
    input: inputStr,
    output: outputStr,
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
  // Always capture the error in the in-memory ring (redacted, bounded) so it is
  // retrievable for diagnostics/bug reports even if the disk write is filtered
  // or fails. Mirrors Claude Code's "always add to in-memory log" behavior.
  const entry = { error: redactCredentials(`[${component}] ${error}`), timestamp: new Date().toISOString() };
  if (inMemoryErrors.length >= MAX_IN_MEMORY_ERRORS) {
    inMemoryErrors.shift(); // drop the oldest
  }
  inMemoryErrors.push(entry);
  log(component, `ERROR: ${error}`, context, 'error');
}

export function getLogPath(): string {
  return initialized ? logFile : resolveLogTarget().file;
}
