import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { redactCredentials } from './credential-guard';

const LOG_DIR = path.join(os.tmpdir(), 'superinference', 'core');
const LOG_FILE = path.join(LOG_DIR, 'engine.log');
const MAX_LOG_SIZE = 10 * 1024 * 1024; // 10MB

// Restrictive modes: the log records raw tool inputs/outputs and error text that
// can contain secrets and file contents. On a shared host the predictable /tmp
// path would otherwise be world-readable, so the directory is created 0700 and
// the file 0600 (owner-only) — a data-minimization/least-exposure defense.
const DIR_MODE = 0o700;
const FILE_MODE = 0o600;

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
// Running byte count of the active log file, maintained in-process so the size
// bound is enforced on EVERY append (continuous rotation) rather than only once
// at startup. Seeded from the on-disk size during init.
let bytesWritten = 0;

/** Test-only seam: force the next log() call to re-run directory setup/rotation. */
export function __resetLoggerForTest(): void {
  initialized = false;
  bytesWritten = 0;
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
  const backup = LOG_FILE + '.prev';
  try { fsHooks.unlinkSync(backup); } catch {}
  try { fsHooks.renameSync(LOG_FILE, backup); } catch {}
  bytesWritten = 0;
}

function ensureDir(): void {
  if (initialized) return;
  try {
    fsHooks.mkdirSync(LOG_DIR, { recursive: true, mode: DIR_MODE });
    // Seed the in-process byte count from the existing file and rotate if it is
    // already over the cap (e.g. left over from a previous run).
    try {
      const stat = fsHooks.statSync(LOG_FILE);
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

export function log(component: string, msg: string, data?: Record<string, unknown>): void {
  ensureDir();
  const ts = new Date().toISOString();
  const dataStr = data ? ' ' + JSON.stringify(data) : '';
  // Escape newlines in the free-form fields, then redact any credentials before
  // the record ever touches disk (tool inputs/outputs and error text can echo
  // API keys, Bearer headers, etc.).
  const composed = `[${ts}] [${escapeLogField(component)}] ${escapeLogField(msg)}${dataStr}`;
  const line = redactCredentials(composed) + '\n';
  const size = Buffer.byteLength(line);
  // Continuous bound: rotate before the active file would exceed the cap.
  if (bytesWritten > 0 && bytesWritten + size > MAX_LOG_SIZE) {
    rotate();
  }
  try {
    fsHooks.appendFileSync(LOG_FILE, line, { mode: FILE_MODE });
    bytesWritten += size;
  } catch {
    // Self-heal: the directory may have been removed mid-session (temp cleanup,
    // another process). Recreate it and retry once so logging does not silently
    // die for the rest of the run.
    try {
      fsHooks.mkdirSync(LOG_DIR, { recursive: true, mode: DIR_MODE });
      fsHooks.appendFileSync(LOG_FILE, line, { mode: FILE_MODE });
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
  log(component, `ERROR: ${error}`, context);
}

export function getLogPath(): string {
  return LOG_FILE;
}
