// Split from task-tracker.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { ToolContext } from '../types';
import { FileLockOptions } from '../utils/file-lock';

/**
 * Emit a live `task_updated` child event (PARITY): mirrors the reference's
 * reactive task signal + auto-expand-task-view affordance — every mutating
 * action (create/update/claim/release/delete) surfaces the change to the
 * frontend so a live task panel reflects it immediately, instead of the state
 * being visible only through the tool's text output. The `task_updated`
 * EngineEvent is already declared in types.ts and handled by
 * event-processor.ts / repl.ts / detached-runner.ts; this is the producer that
 * was missing. Fire-and-forget: a frontend that provides no `_emitChild` is a
 * no-op and a listener must never be able to fail the mutation.
 */
/** Exported so tests can drive the optional emit + catch arms. */
export function emitTaskUpdated(
  context: ToolContext,
  taskId: number,
  status: string,
  subject: string,
): void {
  try {
    context._emitChild?.({ type: 'task_updated', taskId, status, subject });
  } catch {
    /* a frontend listener failure must never fail the mutation */
  }
}

export interface Task {
  id: number;
  subject: string;
  description: string;
  status: 'pending' | 'in_progress' | 'completed';
  activeForm?: string;
  owner?: string;
  metadata?: Record<string, unknown>;
  blocks: number[];
  blockedBy: number[];
}

/**
 * Default task-list identifier used when no explicit team/task-list scope is set,
 * mirroring the reference's `DEFAULT_TASKS_MODE_TASK_LIST_ID`. Kept as a named
 * constant so scope-resolution logic has a single source of truth.
 */
export const DEFAULT_TASKS_MODE_TASK_LIST_ID = 'tasklist';

/**
 * Sanitize a task-list identifier for safe use in a file name — mirrors the reference
 * Code's `sanitizeForFilePath`: strip everything but ASCII letters, digits,
 * hyphen and underscore so a list id can never escape the state dir via
 * traversal (`../`) or path separators. An id that sanitizes to empty is
 * treated as unset.
 */
export function sanitizeTaskListId(raw: unknown): string {
  if (typeof raw !== 'string') return '';
  return raw.trim().replace(/[^A-Za-z0-9_-]/g, '');
}

/**
 * Resolve the active task-list id (PARITY): an explicit argument wins, then the
 * `AMI_TASK_LIST_ID` env knob, then the Claude-Code-compatible
 * `CLAUDE_CODE_TASK_LIST_ID` (cross-framework compat), falling back to the
 * default list. A scoped list lets teammates/teams share one named task file
 * while isolated sessions keep separate state. Mirrors the reference's
 * `getTaskListId()` priority waterfall.
 */
export function resolveTaskListId(explicit?: string): string {
  return (
    sanitizeTaskListId(explicit) ||
    sanitizeTaskListId(process.env.AMI_TASK_LIST_ID) ||
    sanitizeTaskListId(process.env.CLAUDE_CODE_TASK_LIST_ID) ||
    DEFAULT_TASKS_MODE_TASK_LIST_ID
  );
}

/**
 * File name for a given task-list id. The default list keeps the canonical
 * `tasks.json` path; any scoped list gets its own `tasks-<id>.json` so multiple
 * named lists coexist in the same state dir without clobbering each other.
 */
export function taskFileName(listId: string): string {
  return listId === DEFAULT_TASKS_MODE_TASK_LIST_ID ? 'tasks.json' : `tasks-${listId}.json`;
}

/**
 * Hard cap on the characters a single task_tracker call may return, mirroring
 * the reference's `maxResultSizeChars` bound on the task tools. A runaway list of
 * thousands of tasks must not blow up the model's context — overflow is truncated
 * with an explicit marker.
 */
export const MAX_RESULT_SIZE_CHARS = 30_000;

/**
 * Lock retry budget for concurrency-safe task mutations. Exposed so the default
 * can be overridden per call-site; sized for ~10 concurrent swarm writers.
 */
export const TASK_LOCK_OPTIONS: FileLockOptions = {
  retries: 30,
  minTimeout: 5,
  maxTimeout: 100,
};

/**
 * Strict task-id parser (PARITY FIX). the reference's task tools treat the id as
 * an opaque identifier and reject anything that is not a clean integer; Node's
 * `parseInt` is lenient — `parseInt('3abc', 10) === 3` and `parseInt('3.9', 10)
 * === 3` — so a malformed id silently retargets the call at the WRONG task
 * (e.g. deleting/claiming #3 when the caller typed "3abc"). We accept only an
 * optionally-whitespace-padded run of digits and return null otherwise so the
 * caller surfaces the malformed id instead of mutating an unintended task.
 */
export function parseTaskId(raw: unknown): number | null {
  if (typeof raw === 'number') {
    return Number.isSafeInteger(raw) && raw >= 0 ? raw : null;
  }
  if (typeof raw !== 'string') return null;
  const s = raw.trim();
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  return Number.isSafeInteger(n) ? n : null;
}

/**
 * Normalize an owner/assignee (PARITY FIX): trim surrounding whitespace and
 * treat a blank or whitespace-only value as "no owner". Mirrors the reference's
 * assignee handling, where "  alice  " and "alice" are the SAME agent and a
 * blank assignee is unset. Without this, whitespace-variant owners fragment
 * getAgentStatuses into duplicate rows and a whitespace-only owner makes a task
 * look claimed while being owned by nobody (so `next` can never surface it).
 */
export function normalizeOwner(v: unknown): string | undefined {
  if (typeof v !== 'string') return undefined;
  const t = v.trim();
  return t.length > 0 ? t : undefined;
}

/**
 * Default debounce interval (ms) for the task-list directory watcher — mirrors
 * the reference's useTaskListWatcher `DEBOUNCE_MS = 1000`. Rapid successive writes by
 * concurrent swarm/teammate processes to the shared tasks file would otherwise
 * trigger a re-scan storm; the debounce coalesces a burst of fs events into a single
 * reconcile pass.
 */
export const DEFAULT_TASK_WATCH_DEBOUNCE_MS = 1000;

/**
 * Resolve the task-watch debounce interval (PARITY): an explicit argument wins,
 * then the `AMI_TASK_WATCH_DEBOUNCE_MS` env knob, then the Claude-Code-compatible
 * `CLAUDE_CODE_TASK_WATCH_DEBOUNCE_MS`, falling back to the default. A non-positive,
 * non-integer, or unparseable value is ignored (falls through to the next source) so
 * a bad env value can never disable debouncing or arm a zero/negative timer. Mirrors
 * the env-waterfall of {@link resolveTaskListId} and the fs.watch+debounce cadence
 * used by config.ts's hot-reload.
 */
export function resolveTaskWatchDebounceMs(explicit?: number): number {
  const pick = (v: unknown): number | null => {
    if (v === undefined || v === null || v === '') return null;
    const n = typeof v === 'number' ? v : Number(String(v).trim());
    return Number.isInteger(n) && n > 0 ? n : null;
  };
  return (
    pick(explicit) ??
    pick(process.env.AMI_TASK_WATCH_DEBOUNCE_MS) ??
    pick(process.env.CLAUDE_CODE_TASK_WATCH_DEBOUNCE_MS) ??
    DEFAULT_TASK_WATCH_DEBOUNCE_MS
  );
}

/** A task change observed on disk that the in-memory store had not yet applied. */
export interface ExternalTaskChange {
  taskId: number;
  status: string;
  subject: string;
}

/** Handle to a live task-list watcher; `close()` is idempotent. */
export interface TaskListWatcher {
  close(): void;
}

export const STATUS_ICONS: Record<string, string> = {
  pending: '[ ]',
  in_progress: '[>]',
  completed: '[x]',
};

/** Clamp tool output to the per-call size cap with an explicit truncation marker. */
export function capOutput(output: string): string {
  if (output.length <= MAX_RESULT_SIZE_CHARS) return output;
  const kept = output.slice(0, MAX_RESULT_SIZE_CHARS);
  return `${kept}\n... [output truncated at ${MAX_RESULT_SIZE_CHARS} chars]`;
}

/**
 * Result of releasing a departing teammate's in-flight tasks: the tasks that were
 * reset, plus a human-readable notification to surface to the team lead. Mirrors
 * the reference's `UnassignTasksResult`.
 */
export interface UnassignTasksResult {
  unassignedTasks: Array<{ id: number; subject: string }>;
  notificationMessage: string;
}
