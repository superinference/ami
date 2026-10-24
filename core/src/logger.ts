import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

const LOG_DIR = path.join(os.tmpdir(), 'superinference', 'core');
const LOG_FILE = path.join(LOG_DIR, 'engine.log');
const MAX_LOG_SIZE = 10 * 1024 * 1024; // 10MB

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

/** Test-only seam: force the next log() call to re-run directory setup/rotation. */
export function __resetLoggerForTest(): void {
  initialized = false;
}

function ensureDir(): void {
  if (initialized) return;
  try {
    fsHooks.mkdirSync(LOG_DIR, { recursive: true });
    // Rotate if too large
    try {
      const stat = fsHooks.statSync(LOG_FILE);
      if (stat.size > MAX_LOG_SIZE) {
        const backup = LOG_FILE + '.prev';
        try { fsHooks.unlinkSync(backup); } catch {}
        fsHooks.renameSync(LOG_FILE, backup);
      }
    } catch {}
    initialized = true;
  } catch {}
}

export function log(component: string, msg: string, data?: Record<string, unknown>): void {
  ensureDir();
  const ts = new Date().toISOString();
  const dataStr = data ? ' ' + JSON.stringify(data) : '';
  const line = `[${ts}] [${component}] ${msg}${dataStr}\n`;
  try {
    fsHooks.appendFileSync(LOG_FILE, line);
  } catch {}
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
