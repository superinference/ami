import * as fs from 'fs';
import { projectStateDir } from '../reference-journal';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { withFileLock, FileLockOptions } from '../utils/file-lock';
import { log as debugLog } from '../logger';
import { isTaskTrackerEnabled } from './tool-gates';

/**
 * Lifecycle debug trail (PARITY): Claude Code's task watcher emits a
 * step-by-step `logForDebugging` trail for the claim/submit/release lifecycle
 * ('Found available task #N: <subject>', 'Failed to claim task #N: <reason>',
 * 'releasing claim') — the only way to introspect why an autonomous/teammate
 * agent did or did not pick up a task. AMI's claim/release/owner mutations were
 * otherwise silent. This routes those transitions through the shared `log()`
 * diagnostic channel (component `task-tracker`), so they surface in the debug log
 * file, via `--debug`/`AMI_DEBUG`, and `--debug-to-stderr`. Best-effort: a logging
 * failure must never fail a task mutation.
 */
function taskDebug(msg: string, data?: Record<string, unknown>): void {
  try {
    debugLog('task-tracker', msg, data);
  } catch {
    /* diagnostics must never fail the task operation */
  }
}

/**
 * Emit a live `task_updated` child event (PARITY): mirrors Claude Code's
 * reactive task signal + auto-expand-task-view affordance — every mutating
 * action (create/update/claim/release/delete) surfaces the change to the
 * frontend so a live task panel reflects it immediately, instead of the state
 * being visible only through the tool's text output. The `task_updated`
 * EngineEvent is already declared in types.ts and handled by
 * event-processor.ts / repl.ts / detached-runner.ts; this is the producer that
 * was missing. Fire-and-forget: a frontend that provides no `_emitChild` is a
 * no-op and a listener must never be able to fail the mutation.
 */
function emitTaskUpdated(
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

interface Task {
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
 * mirroring Claude Code's `DEFAULT_TASKS_MODE_TASK_LIST_ID`. Kept as a named
 * constant so scope-resolution logic has a single source of truth.
 */
export const DEFAULT_TASKS_MODE_TASK_LIST_ID = 'tasklist';

/**
 * Sanitize a task-list identifier for safe use in a file name — mirrors Claude
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
 * while isolated sessions keep separate state. Mirrors Claude Code's
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
function taskFileName(listId: string): string {
  return listId === DEFAULT_TASKS_MODE_TASK_LIST_ID ? 'tasks.json' : `tasks-${listId}.json`;
}

/**
 * Hard cap on the characters a single task_tracker call may return, mirroring
 * Claude Code's `maxResultSizeChars` bound on the task tools. A runaway list of
 * thousands of tasks must not blow up the model's context — overflow is truncated
 * with an explicit marker.
 */
const MAX_RESULT_SIZE_CHARS = 30_000;

/**
 * Lock retry budget for concurrency-safe task mutations. Exposed so the default
 * can be overridden per call-site; sized for ~10 concurrent swarm writers.
 */
const TASK_LOCK_OPTIONS: FileLockOptions = {
  retries: 30,
  minTimeout: 5,
  maxTimeout: 100,
};

let nextId = 1;
const tasks = new Map<number, Task>();
let persistPath: string | null = null;

export function resetTaskState(): void {
  nextId = 1;
  tasks.clear();
  // Detach from any persisted file so a reset returns to a clean in-memory
  // slate; callers that want persistence re-arm it via setTaskPersistPath.
  persistPath = null;
}

export function getTaskState(): Map<number, Task> {
  return tasks;
}

export function setTaskPersistPath(cwd: string, listId?: string): void {
  const resolved = resolveTaskListId(listId);
  persistPath = path.join(projectStateDir(cwd), taskFileName(resolved));
  loadFromDisk();
}

/**
 * Delete a scoped task-list file from disk (parity: Claude Code removes a team's
 * tasks directory on disband — in AMI a team's 1:1 task list is a single
 * `tasks-<id>.json` file). The DEFAULT shared list (`tasks.json`) is never removed
 * — a bad listId can only ever delete its own scoped file. Also unbinds the
 * in-memory persist path when it pointed at the deleted file so a later mutation
 * does not resurrect it. Best-effort: a missing file is not an error. Returns true
 * only when a scoped file was actually removed.
 */
export function deleteTaskList(cwd: string, listId: string): boolean {
  const resolved = resolveTaskListId(listId);
  if (resolved === DEFAULT_TASKS_MODE_TASK_LIST_ID) return false;
  const file = path.join(projectStateDir(cwd), taskFileName(resolved));
  let removed = false;
  try {
    if (fs.existsSync(file)) {
      fs.rmSync(file, { force: true });
      removed = true;
    }
  } catch {
    /* best-effort — a failed removal must not block team teardown */
  }
  if (persistPath === file) persistPath = null;
  return removed;
}

const VALID_STATUSES = new Set(['pending', 'in_progress', 'completed']);

/**
 * The non-mutating task_tracker actions. These read the store (or disk) but never
 * write it, so they are classified read-only per-invocation (see
 * `taskTrackerTool.isReadOnlyForInput`) — this is the single source of truth for
 * which actions join the concurrent read batch / auto-allow, mirroring Claude
 * Code's dedicated read-only TaskList/TaskGet tools.
 */
const TASK_READ_ACTIONS = new Set(['list', 'get', 'next', 'status']);

/**
 * Strict task-id parser (PARITY FIX). Claude Code's task tools treat the id as
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
 * treat a blank or whitespace-only value as "no owner". Mirrors Claude Code's
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
 * Would adding "blockerId must complete before blockedId" (i.e. pushing
 * blockerId onto blockedId.blockedBy) introduce a dependency cycle? (PARITY
 * FIX.) A cycle means a task transitively blocks itself, producing a deadlock no
 * agent can ever claim out of — `next`/`claim` would permanently report every
 * node in the cycle as blocked. Claude Code rejects such edges; we walk the
 * existing blockedBy graph from blockerId and report a cycle if blockedId is
 * reachable (blockerId already depends, directly or transitively, on blockedId).
 */
export function wouldCreateCycle(blockerId: number, blockedId: number): boolean {
  if (blockerId === blockedId) return true;
  const seen = new Set<number>();
  const stack = [blockerId];
  while (stack.length > 0) {
    const cur = stack.pop()!;
    if (cur === blockedId) return true;
    if (seen.has(cur)) continue;
    seen.add(cur);
    const t = tasks.get(cur);
    if (t) for (const dep of t.blockedBy) stack.push(dep);
  }
  return false;
}

/**
 * Structural validation of a single persisted task record. A corrupt or
 * partially-written record (hand-edited file, interrupted write, schema drift
 * from an older version) must be dropped rather than poisoning the in-memory
 * store — mirrors Claude Code's per-record `TaskSchema.safeParse` on read.
 */
function isValidTaskRecord(t: unknown): t is Task {
  if (!t || typeof t !== 'object') return false;
  const r = t as Record<string, unknown>;
  if (typeof r.id !== 'number' || !Number.isFinite(r.id)) return false;
  if (typeof r.subject !== 'string') return false;
  if (typeof r.description !== 'string') return false;
  if (typeof r.status !== 'string' || !VALID_STATUSES.has(r.status)) return false;
  if (!Array.isArray(r.blocks) || !r.blocks.every((x) => typeof x === 'number')) return false;
  if (!Array.isArray(r.blockedBy) || !r.blockedBy.every((x) => typeof x === 'number')) return false;
  if (r.activeForm !== undefined && typeof r.activeForm !== 'string') return false;
  if (r.owner !== undefined && typeof r.owner !== 'string') return false;
  if (r.metadata !== undefined && (typeof r.metadata !== 'object' || r.metadata === null || Array.isArray(r.metadata))) return false;
  return true;
}

/**
 * Reload the in-memory store from disk with per-record schema validation. Invalid
 * records are skipped (resilient load — one bad record never discards the rest).
 * The high-water-mark (persisted `nextId`) is honored so task IDs are never reused
 * across restarts even after the highest task was deleted.
 */
function loadFromDisk(): void {
  if (!persistPath) return;
  try {
    const raw = fs.readFileSync(persistPath, 'utf-8');
    const data = JSON.parse(raw) as { nextId?: number; tasks?: unknown[] };
    tasks.clear();
    let maxId = 0;
    for (const t of Array.isArray(data.tasks) ? data.tasks : []) {
      if (!isValidTaskRecord(t)) continue;
      tasks.set(t.id, t);
      if (t.id > maxId) maxId = t.id;
    }
    // High-water-mark: max of persisted nextId and max existing ID + 1.
    nextId = Math.max(data.nextId || 1, maxId + 1);
  } catch {
    // No file or invalid JSON — start fresh.
  }
}

function saveToDisk(): void {
  if (!persistPath) return;
  try {
    const dir = path.dirname(persistPath);
    fs.mkdirSync(dir, { recursive: true });
    const data = { nextId, tasks: Array.from(tasks.values()) };
    fs.writeFileSync(persistPath, JSON.stringify(data, null, 2), 'utf-8');
  } catch {
    // Best effort — don't break tool execution on write failure.
  }
}

/**
 * Run a mutating critical section with cross-process safety: under an exclusive
 * file lock, reload the latest state from disk (so a concurrent process's writes
 * are seen — fresh read on every operation), apply `fn` against the in-memory
 * store, then persist. When persistence is disabled (pure in-memory unit use),
 * `fn` runs directly. This is the single serialization point for all task
 * mutations, preventing lost updates and TOCTOU double-claims across processes.
 */
async function mutate<T>(fn: () => T): Promise<T> {
  if (!persistPath) return fn();
  // The lock file lives next to the persist file, so its directory must exist
  // before we try to create the lock (ENOENT otherwise on a first-ever write).
  try { fs.mkdirSync(path.dirname(persistPath), { recursive: true }); } catch { /* exists/broken */ }
  try {
    return await withFileLock(
      persistPath,
      () => {
        loadFromDisk();
        const result = fn();
        saveToDisk();
        return result;
      },
      TASK_LOCK_OPTIONS,
    );
  } catch {
    // Locking/persistence is unavailable (e.g. a broken state dir, or the lock
    // could not be acquired). Never crash the tool: fall back to an in-memory
    // mutation with best-effort persistence (saveToDisk swallows its own errors).
    const result = fn();
    saveToDisk();
    return result;
  }
}

/** Fresh read-only snapshot from disk (no write lock — read path stays cheap). */
function reloadForRead(): void {
  if (persistPath) loadFromDisk();
}

/**
 * Default debounce interval (ms) for the task-list directory watcher — mirrors
 * Claude Code's useTaskListWatcher `DEBOUNCE_MS = 1000`. Rapid successive writes by
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

/**
 * Read a persisted task file into a fresh Map WITHOUT mutating the global store —
 * used to diff the on-disk state against the in-memory state when detecting another
 * process's writes. Invalid/corrupt records are skipped (same per-record validation
 * as {@link loadFromDisk}); a missing/invalid file yields an empty snapshot.
 */
function readTaskFile(file: string): Map<number, Task> {
  const out = new Map<number, Task>();
  try {
    const raw = fs.readFileSync(file, 'utf-8');
    const data = JSON.parse(raw) as { tasks?: unknown[] };
    for (const t of Array.isArray(data.tasks) ? data.tasks : []) {
      if (isValidTaskRecord(t)) out.set(t.id, t);
    }
  } catch {
    /* missing/invalid file → empty snapshot */
  }
  return out;
}

/**
 * Cross-process task surfacing (PARITY): reconcile the currently-bound task file on
 * disk against the in-memory store and return the tasks another process
 * created/changed/deleted since we last observed them. AMI's in-process
 * {@link emitTaskUpdated} only surfaces THIS session's own mutations; when
 * teammates/swarm writers share a scoped `tasks-<id>.json`, their writes were
 * invisible until the model happened to run a read action. This diff powers the live
 * cross-process `task_updated` feed (the push counterpart to the pull-based
 * `next`/`claim` loop).
 *
 * A change is reported only when the on-disk version actually differs from memory
 * (status or subject), so a self-write (memory already matches the file it just
 * wrote) produces NO spurious event. The in-memory store is reconciled to disk for
 * each reported change so the same external delta is not re-emitted on the next scan.
 * Returns an empty list when persistence is disabled. Mirrors Claude Code's
 * task-watcher reconcile that re-lists the dir and surfaces newly-available tasks.
 */
export function reconcileExternalTaskChanges(): ExternalTaskChange[] {
  if (!persistPath) return [];
  const onDisk = readTaskFile(persistPath);
  const changes: ExternalTaskChange[] = [];
  // Added or modified on disk relative to memory.
  for (const [id, diskTask] of onDisk) {
    const mem = tasks.get(id);
    if (!mem || mem.status !== diskTask.status || mem.subject !== diskTask.subject) {
      changes.push({ taskId: id, status: diskTask.status, subject: diskTask.subject });
      tasks.set(id, diskTask);
    }
  }
  // Removed on disk but still in memory (another process deleted it).
  for (const [id, mem] of [...tasks]) {
    if (!onDisk.has(id)) {
      changes.push({ taskId: id, status: 'deleted', subject: mem.subject });
      tasks.delete(id);
    }
  }
  changes.sort((a, b) => a.taskId - b.taskId);
  return changes;
}

/** Handle to a live task-list watcher; `close()` is idempotent. */
export interface TaskListWatcher {
  close(): void;
}

/**
 * Watch the bound task-list directory and invoke `onChange` (debounced) with the
 * tasks another process created/changed/deleted, so a consumer (the engine's live
 * `task_updated` feed) can surface cross-process writes between this session's own
 * turns. Mirrors Claude Code's useTaskListWatcher: an fs.watch on the tasks dir, a
 * DEBOUNCE_MS timer collapsing bursts, a filename filter so only events for the
 * BOUND list's file trigger a reconcile, and unref() on both the watcher and the
 * timer so the watch never keeps the process alive.
 *
 * Best-effort throughout: fs.watch throwing (ENOENT race, inotify exhaustion)
 * degrades to a no-op watcher rather than crashing the caller, and an onChange
 * callback that throws is isolated so it can never break the watch loop.
 */
export function watchTaskList(
  cwd: string,
  onChange: (changes: ExternalTaskChange[]) => void,
  opts: { debounceMs?: number } = {},
): TaskListWatcher {
  const dir = projectStateDir(cwd);
  const debounceMs = resolveTaskWatchDebounceMs(opts.debounceMs);
  let timer: ReturnType<typeof setTimeout> | null = null;
  let watcher: fs.FSWatcher | null = null;
  let closed = false;

  const fire = (): void => {
    timer = null;
    if (closed) return;
    try {
      const changes = reconcileExternalTaskChanges();
      if (changes.length > 0) onChange(changes);
    } catch {
      /* a reconcile/consumer failure must never break the watch loop */
    }
  };
  const schedule = (filename: string | null): void => {
    if (closed || !persistPath) return;
    // The dir may hold several scoped lists — only react to the bound file.
    if (filename && filename !== path.basename(persistPath)) return;
    if (timer) clearTimeout(timer);
    timer = setTimeout(fire, debounceMs);
    timer.unref?.();
  };

  try { fs.mkdirSync(dir, { recursive: true }); } catch { /* best-effort */ }
  try {
    watcher = fs.watch(dir, (_event, filename) => schedule(filename != null ? String(filename) : null));
    watcher.unref?.();
  } catch {
    /* fs.watch throws synchronously on ENOENT / inotify exhaustion — degrade to no-op */
  }

  return {
    close(): void {
      if (closed) return;
      closed = true;
      if (timer) { clearTimeout(timer); timer = null; }
      try { watcher?.close(); } catch { /* already closed */ }
      watcher = null;
    },
  };
}

const STATUS_ICONS: Record<string, string> = {
  pending: '[ ]',
  in_progress: '[>]',
  completed: '[x]',
};

function unresolvedBlockers(task: Task): number[] {
  return task.blockedBy.filter(id => {
    const dep = tasks.get(id);
    return dep && dep.status !== 'completed';
  });
}

function formatTask(task: Task): string {
  const icon = STATUS_ICONS[task.status];
  const lines = [
    `${icon} Task #${task.id}: ${task.subject} [${task.status}]`,
  ];
  if (task.owner) {
    lines.push(`    Owner: ${task.owner}`);
  }
  if (task.description) {
    lines.push(`    ${task.description}`);
  }
  const blockers = unresolvedBlockers(task);
  if (blockers.length > 0) {
    lines.push(`    Blocked by: ${blockers.map(id => `#${id}`).join(', ')}`);
  }
  if (task.blocks.length > 0) {
    lines.push(`    Blocks: ${task.blocks.map(id => `#${id}`).join(', ')}`);
  }
  if (task.activeForm) lines.push(`  Active: ${task.activeForm}`);
  if (task.metadata && Object.keys(task.metadata).length > 0) {
    const metaStr = Object.entries(task.metadata)
      .filter(([k]) => !k.startsWith('_'))
      .map(([k, v]) => `${k}=${JSON.stringify(v)}`)
      .join(', ');
    if (metaStr) lines.push(`  Metadata: ${metaStr}`);
  }
  return lines.join('\n');
}

/** Clamp tool output to the per-call size cap with an explicit truncation marker. */
function capOutput(output: string): string {
  if (output.length <= MAX_RESULT_SIZE_CHARS) return output;
  const kept = output.slice(0, MAX_RESULT_SIZE_CHARS);
  return `${kept}\n... [output truncated at ${MAX_RESULT_SIZE_CHARS} chars]`;
}

/**
 * Derive each owner's idle/busy status from the current store — an agent is
 * "busy" if it owns at least one unresolved (pending/in_progress) task. Mirrors
 * Claude Code's `getAgentStatuses` ownership-based introspection. Used by the
 * agent-busy claim guard below.
 */
function busyTasksFor(owner: string): number[] {
  const out: number[] = [];
  for (const t of tasks.values()) {
    if (t.status !== 'completed' && t.owner === owner) out.push(t.id);
  }
  return out;
}

/**
 * Select the NEXT available task to work on — the lowest-id pending task that is
 * unblocked and either unowned or owned by `owner` — mirroring Claude Code's
 * findAvailableTask used by the pull-based teammate self-claim loop. Returns null
 * when there is nothing claimable.
 */
export function findAvailableTask(owner?: string): Task | null {
  const candidates = [...tasks.values()]
    .filter(t =>
      t.status === 'pending' &&
      (!t.owner || t.owner === owner) &&
      unresolvedBlockers(t).length === 0)
    .sort((a, b) => a.id - b.id);
  return candidates[0] ?? null;
}

/**
 * Per-owner idle/busy introspection derived from task ownership — an owner is
 * "busy" if it holds an unresolved (pending/in_progress) task, else "idle".
 * Mirrors Claude Code's getAgentStatuses. Returns one row per owner that appears
 * on any task.
 */
export function getAgentStatuses(): Array<{ owner: string; status: 'busy' | 'idle'; tasks: number[] }> {
  const owners = new Set<string>();
  for (const t of tasks.values()) if (t.owner) owners.add(t.owner);
  return [...owners].sort().map(owner => {
    const busy = busyTasksFor(owner);
    return { owner, status: busy.length > 0 ? 'busy' : 'idle', tasks: busy };
  });
}

/**
 * Result of releasing a departing teammate's in-flight tasks: the tasks that were
 * reset, plus a human-readable notification to surface to the team lead. Mirrors
 * Claude Code's `UnassignTasksResult`.
 */
export interface UnassignTasksResult {
  unassignedTasks: Array<{ id: number; subject: string }>;
  notificationMessage: string;
}

/**
 * Cancellation/abort boundary (PARITY): when a teammate is killed or gracefully
 * shuts down, release every unresolved (non-completed) task it still owns back to
 * the pool — clear the owner and reset the status to `pending` — so the work is not
 * orphaned and an idle teammate can claim it. Completed tasks and tasks owned by
 * other agents are left untouched. Mirrors Claude Code's `unassignTeammateTasks`
 * (src/utils/tasks.ts:L818-L860): finds the agent's unresolved tasks, resets them,
 * and builds a notification message naming the released tasks.
 *
 * `ownerIds` is matched against each task's owner, so pass every identity the
 * teammate can own tasks under (its stable agentId AND its display name, since a
 * task may be auto-assigned under either). The mutation runs under the same
 * cross-process task lock as every other write, so it is atomic with concurrent
 * claims — a task cannot be both claimed and unassigned.
 */
export async function unassignTeammateTasks(
  ownerIds: string[],
  teammateName: string,
  reason: 'terminated' | 'shutdown',
): Promise<UnassignTasksResult> {
  const matchSet = new Set(
    ownerIds.map(normalizeOwner).filter((o): o is string => o !== undefined),
  );
  const unassignedTasks = await mutate(() => {
    const released: Array<{ id: number; subject: string }> = [];
    if (matchSet.size === 0) return released;
    for (const task of tasks.values()) {
      if (task.status !== 'completed' && task.owner && matchSet.has(task.owner)) {
        released.push({ id: task.id, subject: task.subject });
        task.owner = undefined;
        task.status = 'pending';
      }
    }
    return released;
  });
  // Lowest id first so the notification reads deterministically.
  unassignedTasks.sort((a, b) => a.id - b.id);

  const actionVerb = reason === 'terminated' ? 'was terminated' : 'has shut down';
  let notificationMessage = `${teammateName} ${actionVerb}.`;
  if (unassignedTasks.length > 0) {
    const taskList = unassignedTasks.map(t => `#${t.id} "${t.subject}"`).join(', ');
    notificationMessage +=
      ` ${unassignedTasks.length} task(s) were unassigned: ${taskList}.` +
      ` Use task_tracker action "list" to check availability and "update" with an owner to reassign them to idle teammates.`;
  }
  return { unassignedTasks, notificationMessage };
}

/**
 * Post-completion affordance (PARITY): after a task transitions to completed,
 * point the agent at its next available work and, when the whole list is done,
 * nudge for a verification step. Mirrors Claude Code's TaskUpdateTool completion
 * reminders — the "find your next / now-unblocked task" hint plus the structural
 * "all done, nothing covers verification" nudge (all-completed && >=3 tasks &&
 * no subject matching /verif/i).
 */
function completionHints(completedId: number): string {
  const lines: string[] = [];
  const unblocked: number[] = [];
  for (const t of tasks.values()) {
    if (t.status !== 'completed' && t.blockedBy.includes(completedId) && unresolvedBlockers(t).length === 0) {
      unblocked.push(t.id);
    }
  }
  unblocked.sort((a, b) => a - b);
  if (unblocked.length > 0) {
    lines.push(`Now unblocked: ${unblocked.map(i => `#${i}`).join(', ')}. Use action "next"/"list" to pick your next task and "claim" it.`);
  } else {
    lines.push('Use action "next"/"list" to find your next available task and "claim" it; dependent tasks may now be unblocked.');
  }
  const visible = [...tasks.values()].filter(t => !t.metadata?._internal);
  if (visible.length >= 3 && visible.every(t => t.status === 'completed') && !visible.some(t => /verif/i.test(t.subject))) {
    lines.push('All tasks are completed and none covers verification — consider adding a task to verify the work (tests/build/review) before finishing.');
  }
  return lines.length > 0 ? `\n\n${lines.join('\n')}` : '';
}

// Keys the schema accepts; anything else is a malformed call and is rejected so
// typos (e.g. `taskId` vs `task_id`) surface instead of silently no-op'ing.
const ALLOWED_INPUT_KEYS = new Set([
  'action', 'subject', 'description', 'activeForm', 'task_id', 'status',
  'owner', 'metadata', 'addBlocks', 'addBlockedBy', 'checkAgentBusy',
]);

export const taskTrackerTool: ToolDefinition = {
  name: 'task_tracker',
  // Gated by the task-tracker enablement knob (default ON) so an SDK/headless caller
  // can opt the richer task tool out and rely on `todo_write` alone, mirroring Claude
  // Code's `CLAUDE_CODE_ENABLE_TASKS` task-mode toggle. See {@link isTaskTrackerEnabled}.
  isEnabled: () => isTaskTrackerEnabled(),
  description:
    'Create and manage tasks to track progress on complex work. This demonstrates thoroughness to the user and helps the user understand the progress of their requests, so keep the list honest and up to date. ' +
    'Actions: "create" (new task), "update" (change status/details), "list" (show all), "get" (single task), "delete" (remove), "claim" (atomically assign and start), "release" (give up a claim: clear owner and revert in_progress back to pending), "next" (find the next available task to work on), "status" (per-owner idle/busy introspection). ' +
    'Supports task dependencies via addBlocks/addBlockedBy to express ordering constraints. ' +
    'Prefer tasks in id order (lowest id first). Check the list first to avoid creating duplicate tasks. ' +
    'Authoring for teammates: tasks are created with status "pending" and no owner — assign one with "update"/"claim" using the owner parameter. Include enough detail in the description for another agent to understand and complete the task on its own. ' +
    'When to "list": to see available work (pending, unowned, unblocked), check overall progress, find blocked tasks, and after completing a task to surface newly unblocked work. A task whose blockedBy is non-empty cannot be claimed until those dependencies complete. ' +
    'When to "get": to read the full details before starting a task — it returns the fields subject, description, status, blocks (tasks waiting on this one) and blockedBy (tasks that must complete first); verify blockedBy is empty before you begin, and use "list" to see all tasks in summary form. ' +
    'Pull-based teammates: call "next" (optionally with owner) to get the next pending/unowned/unblocked task, then "claim" it, work it, mark it completed, and repeat. If you claim a task but cannot start it, "release" it so another teammate can pick it up. ' +
    'Staleness: in shared/multi-owner task lists others may change a task between your reads — re-read the latest state with action "get" before you "update" or "claim" a task you did not just create, so you do not act on stale data. ' +
    'Completion discipline: only mark a task completed when the work is FULLY accomplished and verified. Never mark a task completed if tests are failing, the implementation is partial, you hit unresolved errors, or you could not find required files or dependencies — leave it in_progress and fix the root cause first; if blocked, create a new blocker task describing what must be resolved instead of closing.',
  inputSchema: {
    type: 'object',
    properties: {
      action: {
        type: 'string',
        enum: ['create', 'update', 'list', 'get', 'delete', 'claim', 'release', 'next', 'status'],
        description: 'The action to perform.',
      },
      subject: {
        type: 'string',
        description: 'Brief title for the task (create/update).',
      },
      description: {
        type: 'string',
        description: 'What needs to be done (create/update).',
      },
      activeForm: {
        type: 'string',
        description: 'Present-continuous form shown during execution, e.g. "Running tests" (create/update). The spinner falls back to the subject when omitted.',
      },
      task_id: {
        type: 'string',
        description: 'Task ID for update/get/delete/claim/release actions.',
      },
      status: {
        type: 'string',
        enum: ['pending', 'in_progress', 'completed', 'deleted'],
        description: 'New status for update action. Use "deleted" to permanently remove a task. Never set "completed" when the work failed.',
      },
      owner: {
        type: 'string',
        description: 'Owner/assignee for the task (update/claim).',
      },
      metadata: {
        type: 'object',
        description: 'Arbitrary metadata to attach (create/update). Set a key to null to remove it.',
      },
      addBlocks: {
        type: 'array',
        items: { type: 'string' },
        description: 'Task IDs that cannot start until this task completes (update).',
      },
      addBlockedBy: {
        type: 'array',
        items: { type: 'string' },
        description: 'Task IDs that must complete before this task can start (update).',
      },
      checkAgentBusy: {
        type: 'boolean',
        description: 'When claiming, atomically refuse if the owner already holds an unresolved task (one open task per agent).',
      },
    },
    required: ['action'],
  },
  isReadOnly: false,
  // Per-invocation read-only classification (PARITY): Claude Code ships task
  // LISTING/GET as dedicated tools that declare isReadOnly()=>true AND
  // isConcurrencySafe()=>true, so a pure task read joins the concurrent read-only
  // execution batch and auto-allows without a permission prompt. AMI consolidates
  // read + write into one task_tracker tool, so the STATIC isReadOnly flag has to
  // stay false (the tool CAN mutate). This dynamic verdict recovers the lost
  // boundary: the non-mutating actions ("list", "get", "next", "status") are
  // reported read-only for THIS call, so the executor batches them concurrently
  // (tool-executor segments on readOnlyForInputSafe && isToolConcurrencySafe —
  // which falls back to the read-only verdict) instead of making every task read a
  // serial write barrier, and the permission layer auto-allows them. Every
  // mutating action falls through to writable (false). See
  // core/tools/tool-categories.isToolReadOnlyForInput / isToolConcurrencySafe.
  isReadOnlyForInput: (input: Record<string, unknown>): boolean =>
    TASK_READ_ACTIONS.has(input.action as string),

  async execute(input, context: ToolContext): Promise<ToolResult> {
    const action = input.action as string;

    // Strict schema: reject unknown/unexpected keys so malformed calls surface.
    const unknownKeys = Object.keys(input).filter(k => !ALLOWED_INPUT_KEYS.has(k));
    if (unknownKeys.length > 0) {
      return { output: `Error: unknown input key(s): ${unknownKeys.join(', ')}. Allowed: ${[...ALLOWED_INPUT_KEYS].join(', ')}.`, isError: true };
    }

    switch (action) {
      case 'create': {
        const subject = input.subject as string | undefined;
        if (!subject || subject.trim().length === 0) {
          return { output: 'Error: subject is required for create action.', isError: true };
        }
        const result = await mutate(() => {
          const task: Task = {
            id: nextId++,
            subject: subject.trim(),
            description: ((input.description as string) || '').trim(),
            status: 'pending',
            activeForm: (input.activeForm as string) || undefined,
            owner: normalizeOwner(input.owner),
            metadata: input.metadata ? { ...(input.metadata as Record<string, unknown>) } : undefined,
            blocks: [],
            blockedBy: [],
          };
          tasks.set(task.id, task);
          return task;
        });
        // Blocking TaskCreated hooks can VETO a just-created task (PARITY): await
        // the hook, and if any hook returns a blockingError perform a compensating
        // rollback — delete the task we just created — and surface the error instead
        // of reporting a phantom success. The surrounding call's abortSignal is
        // forwarded so a long-running hook is cancellable with the turn. A throwing
        // hook never blocks (best-effort), mirroring the TaskCompleted path.
        if (context._hookManager) {
          let blocking: string[] = [];
          try {
            const r = await context._hookManager.executeTaskCreated({
              taskId: String(result.id),
              subject: result.subject,
              description: result.description,
              signal: context.abortSignal,
            });
            const errs = (r as { blockingErrors?: unknown } | void)?.blockingErrors;
            if (Array.isArray(errs)) blocking = errs.filter((e): e is string => typeof e === 'string');
          } catch {
            /* a throwing taskCreated hook must not block creation */
          }
          if (blocking.length > 0) {
            await mutate(() => { tasks.delete(result.id); });
            return { output: `Error: creation of task #${result.id} blocked by hook: ${blocking.join('; ')} [reason: creation_blocked]`, isError: true };
          }
        }
        emitTaskUpdated(context, result.id, result.status, result.subject);
        return { output: capOutput(`Task #${result.id} created successfully: ${result.subject} [pending]`), isError: false };
      }

      case 'update': {
        const id = parseTaskId(input.task_id);
        if (id === null) {
          return { output: 'Error: task_id is required for update action.', isError: true };
        }
        // Validate status before taking the lock so a bad value fails cleanly.
        if (input.status && input.status !== 'deleted' && !VALID_STATUSES.has(input.status as string)) {
          return { output: `Error: invalid status "${input.status}". Must be one of: ${[...VALID_STATUSES].join(', ')}, deleted`, isError: true };
        }
        // Blocking TaskCompleted hooks can VETO a status->completed transition
        // (PARITY): consult them BEFORE mutating and refuse the completion when a
        // hook returns a blockingError, instead of fire-and-forget after the fact.
        // A throwing hook never blocks (best-effort, matching the prior behavior).
        if (input.status === 'completed' && context._hookManager) {
          reloadForRead();
          const existing = tasks.get(id);
          if (existing && existing.status !== 'completed') {
            let blocking: string[] = [];
            try {
              const r = await context._hookManager.executeTaskCompleted({ taskId: String(id), subject: existing.subject, signal: context.abortSignal });
              const errs = (r as { blockingErrors?: unknown } | void)?.blockingErrors;
              if (Array.isArray(errs)) blocking = errs.filter((e): e is string => typeof e === 'string');
            } catch {
              /* a throwing completion hook must not block the transition */
            }
            if (blocking.length > 0) {
              return { output: `Error: completion of task #${id} blocked by hook: ${blocking.join('; ')} [reason: completion_blocked]`, isError: true };
            }
          }
        }
        let notFound = false;
        let deleted = false;
        let cycleErr = '';
        let startBlocked: number[] = [];
        let emitStatus = '';
        let emitSubject = '';
        const out = await mutate(() => {
          const task = tasks.get(id);
          if (!task) { notFound = true; return ''; }
          if (input.status === 'deleted') {
            emitStatus = 'deleted';
            emitSubject = task.subject;
            tasks.delete(id);
            for (const t of tasks.values()) {
              t.blocks = t.blocks.filter(bid => bid !== id);
              t.blockedBy = t.blockedBy.filter(bid => bid !== id);
            }
            deleted = true;
            return `Task #${id} deleted.`;
          }
          // --- Atomic pre-validation (PARITY FIX): reject the whole update
          // before mutating anything, so a bad edge/transition never leaves a
          // partially-applied (and possibly deadlocked) task.
          // Dependency-cycle guard for requested edges.
          if (input.addBlocks) {
            for (const bid of input.addBlocks as string[]) {
              const blockId = parseTaskId(bid);
              if (blockId === null || blockId === id || !tasks.has(blockId)) continue;
              // "id blocks blockId" => id must finish before blockId.
              if (wouldCreateCycle(id, blockId)) { cycleErr = `#${id} -> #${blockId}`; return ''; }
            }
          }
          if (input.addBlockedBy) {
            for (const bid of input.addBlockedBy as string[]) {
              const blockerId = parseTaskId(bid);
              if (blockerId === null || blockerId === id || !tasks.has(blockerId)) continue;
              // "id blocked by blockerId" => blockerId must finish before id.
              if (wouldCreateCycle(blockerId, id)) { cycleErr = `#${blockerId} -> #${id}`; return ''; }
            }
          }
          // Blocked-task start guard: refuse to move a task to in_progress while
          // it still has unresolved blockers, mirroring the claim-path guard so
          // `update` is not a backdoor around dependency ordering.
          if (input.status === 'in_progress') {
            const blk = unresolvedBlockers(task);
            if (blk.length > 0) { startBlocked = blk; return ''; }
          }
          const prevStatus = task.status;
          const prevSubject = task.subject;
          const prevDescription = task.description;
          const prevActiveForm = task.activeForm;
          const prevOwner = task.owner;
          const blocksBefore = task.blocks.length;
          const blockedByBefore = task.blockedBy.length;
          if (input.status) {
            task.status = input.status as Task['status'];
          }
          if (input.subject) task.subject = (input.subject as string).trim();
          if (input.description !== undefined) task.description = ((input.description as string) || '').trim();
          if (input.activeForm !== undefined) task.activeForm = (input.activeForm as string) || undefined;
          if (input.owner !== undefined) task.owner = normalizeOwner(input.owner);
          // Auto-assign owner (PARITY): when a teammate moves a task to in_progress
          // without naming an owner and the task has none, default ownership to the
          // acting agent identity so status/getAgentStatuses attribute the work.
          if (input.status === 'in_progress' && input.owner === undefined && !task.owner) {
            const self = normalizeOwner(context.agentId);
            if (self) task.owner = self;
          }
          if (input.metadata) {
            if (!task.metadata) task.metadata = {};
            for (const [k, v] of Object.entries(input.metadata as Record<string, unknown>)) {
              if (v === null) delete task.metadata[k];
              else task.metadata[k] = v;
            }
          }
          if (input.addBlocks) {
            for (const bid of input.addBlocks as string[]) {
              const blockId = parseTaskId(bid);
              if (blockId !== null && tasks.has(blockId) && blockId !== id) {
                if (!task.blocks.includes(blockId)) task.blocks.push(blockId);
                const blockedTask: Task = tasks.get(blockId)!;
                if (!blockedTask.blockedBy.includes(id)) blockedTask.blockedBy.push(id);
              }
            }
          }
          if (input.addBlockedBy) {
            for (const bid of input.addBlockedBy as string[]) {
              const blockerId = parseTaskId(bid);
              if (blockerId !== null && tasks.has(blockerId) && blockerId !== id) {
                if (!task.blockedBy.includes(blockerId)) task.blockedBy.push(blockerId);
                const blockerTask: Task = tasks.get(blockerId)!;
                if (!blockerTask.blocks.includes(id)) blockerTask.blocks.push(id);
              }
            }
          }
          // Per-field change detection (PARITY): report exactly which fields the
          // update actually changed (updatedFields), plus the status transition.
          const changedFields: string[] = [];
          if (prevStatus !== task.status) changedFields.push('status');
          if (prevSubject !== task.subject) changedFields.push('subject');
          if (prevDescription !== task.description) changedFields.push('description');
          if (prevActiveForm !== task.activeForm) changedFields.push('activeForm');
          if (prevOwner !== task.owner) changedFields.push('owner');
          if (input.metadata) changedFields.push('metadata');
          if (blocksBefore !== task.blocks.length) changedFields.push('blocks');
          if (blockedByBefore !== task.blockedBy.length) changedFields.push('blockedBy');
          emitStatus = task.status;
          emitSubject = task.subject;
          const didComplete = prevStatus !== 'completed' && task.status === 'completed';
          const fieldsLine = changedFields.length > 0
            ? `Updated task #${id} (fields: ${changedFields.join(', ')}).\n`
            : '';
          const statusLine = (input.status && prevStatus !== task.status)
            ? `Status: ${prevStatus} -> ${task.status}\n`
            : '';
          return fieldsLine + statusLine + formatTask(task) + (didComplete ? completionHints(id) : '');
        });
        if (notFound) {
          return { output: `Error: task #${id} not found.`, isError: true };
        }
        if (cycleErr) {
          return { output: `Error: adding that dependency would create a cycle (${cycleErr}). [reason: dependency_cycle]`, isError: true };
        }
        if (startBlocked.length > 0) {
          return { output: `Error: task #${id} is blocked by: ${startBlocked.map(b => `#${b}`).join(', ')} — resolve blockers before starting. [reason: blocked]`, isError: true };
        }
        void deleted;
        emitTaskUpdated(context, id, emitStatus, emitSubject);
        return { output: capOutput(out), isError: false };
      }

      case 'list': {
        reloadForRead();
        const visibleTasks = [...tasks.values()].filter(t => !t.metadata?._internal);
        if (visibleTasks.length === 0) {
          return { output: 'No tasks.', isError: false };
        }
        // Lowest id first — the tie-break heuristic for selecting available work.
        const sorted = visibleTasks.sort((a, b) => a.id - b.id);
        return { output: capOutput(sorted.map(formatTask).join('\n')), isError: false };
      }

      case 'get': {
        const id = parseTaskId(input.task_id);
        if (id === null) {
          return { output: 'Error: task_id is required for get action.', isError: true };
        }
        reloadForRead();
        const task = tasks.get(id);
        if (!task) {
          // Graceful, NON-error "not found" on read (PARITY): Claude Code's
          // TaskGet returns a normal tool result ("Task not found") for a missing
          // id rather than an error — a read that finds nothing is a valid answer,
          // not a tool failure. A teammate polling for a task it may have already
          // completed/deleted must not see this as an error it has to recover from.
          return { output: `Task #${id} not found.`, isError: false };
        }
        return { output: capOutput(formatTask(task)), isError: false };
      }

      case 'delete': {
        const id = parseTaskId(input.task_id);
        if (id === null) {
          return { output: 'Error: task_id is required for delete action.', isError: true };
        }
        let notFound = false;
        let deletedSubject = '';
        await mutate(() => {
          const existing = tasks.get(id);
          if (!existing) { notFound = true; return; }
          deletedSubject = existing.subject;
          tasks.delete(id);
          for (const t of tasks.values()) {
            t.blocks = t.blocks.filter(bid => bid !== id);
            t.blockedBy = t.blockedBy.filter(bid => bid !== id);
          }
        });
        if (notFound) {
          return { output: `Error: task #${id} not found.`, isError: true };
        }
        emitTaskUpdated(context, id, 'deleted', deletedSubject);
        return { output: `Task #${id} deleted.`, isError: false };
      }

      case 'claim': {
        const id = parseTaskId(input.task_id);
        if (id === null) {
          return { output: 'Error: task_id is required for claim action.', isError: true };
        }
        const owner = normalizeOwner(input.owner);
        const checkAgentBusy = input.checkAgentBusy === true;
        // Structured claim-failure reasons mirror Claude Code's ClaimTaskResult.
        let reason: 'task_not_found' | 'already_claimed' | 'already_resolved' | 'blocked' | 'agent_busy' | null = null;
        let detail = '';
        let claimedSubject = '';
        const out = await mutate(() => {
          const task = tasks.get(id);
          if (!task) { reason = 'task_not_found'; return ''; }
          if (task.status === 'in_progress') {
            reason = 'already_claimed';
            detail = task.owner ? ` (owned by ${task.owner})` : '';
            return '';
          }
          if (task.status === 'completed') { reason = 'already_resolved'; return ''; }
          const blockers = unresolvedBlockers(task);
          if (blockers.length > 0) {
            reason = 'blocked';
            detail = blockers.map(b => `#${b}`).join(', ');
            return '';
          }
          // Agent-busy guard: atomically (under the same lock) refuse if the
          // claimant already owns an unresolved task elsewhere.
          if (checkAgentBusy && owner) {
            const busy = busyTasksFor(owner).filter(bid => bid !== id);
            if (busy.length > 0) {
              reason = 'agent_busy';
              detail = busy.map(b => `#${b}`).join(', ');
              return '';
            }
          }
          task.status = 'in_progress';
          claimedSubject = task.subject;
          // Explicit owner wins; otherwise auto-assign the acting agent identity
          // to an unowned task (PARITY: a teammate claiming without naming itself
          // still becomes the owner, so status/next attribute the work correctly).
          if (owner) task.owner = owner;
          else if (!task.owner) {
            const self = normalizeOwner(context.agentId);
            if (self) task.owner = self;
          }
          return `Claimed task #${id}: ${task.subject} [in_progress]${task.owner ? ` (owner: ${task.owner})` : ''}`;
        });
        if (reason !== null) {
          taskDebug(`Failed to claim task #${id}: ${reason}`, { owner, detail: detail || undefined });
          if (reason === 'task_not_found') return { output: `Error: task #${id} not found. [reason: task_not_found]`, isError: true };
          if (reason === 'already_claimed') return { output: `Error: task #${id} is already in_progress${detail}. [reason: already_claimed]`, isError: true };
          if (reason === 'already_resolved') return { output: `Error: task #${id} is already completed. [reason: already_resolved]`, isError: true };
          if (reason === 'blocked') return { output: `Error: task #${id} is blocked by: ${detail} [reason: blocked]`, isError: true };
          return { output: `Error: "${owner}" is already busy with: ${detail} [reason: agent_busy]`, isError: true };
        }
        taskDebug(`Claimed task #${id}: ${claimedSubject}`, { owner: owner || context.agentId });
        emitTaskUpdated(context, id, 'in_progress', claimedSubject);
        return { output: capOutput(out), isError: false };
      }

      case 'release': {
        // Claim rollback / orphaned-claim recovery (PARITY): a teammate that
        // claimed a task but cannot start it (submission rejected, agent aborted,
        // work failed to begin) gives the claim back — clear the owner and revert
        // an in_progress task to pending so `next`/`claim` can surface it to the
        // next worker. Mirrors Claude Code's watcher releasing a claim via
        // updateTask(..., { owner: undefined }) when onSubmitTask returns false.
        const id = parseTaskId(input.task_id);
        if (id === null) {
          return { output: 'Error: task_id is required for release action.', isError: true };
        }
        let notFound = false;
        let alreadyCompleted = false;
        let releasedStatus = '';
        let releasedSubject = '';
        const out = await mutate(() => {
          const task = tasks.get(id);
          if (!task) { notFound = true; return ''; }
          if (task.status === 'completed') { alreadyCompleted = true; return ''; }
          const hadOwner = task.owner;
          const wasInProgress = task.status === 'in_progress';
          task.owner = undefined;
          if (wasInProgress) task.status = 'pending';
          releasedStatus = task.status;
          releasedSubject = task.subject;
          const changed: string[] = [];
          if (hadOwner) changed.push('owner cleared');
          if (wasInProgress) changed.push('reverted in_progress -> pending');
          const detail = changed.length > 0 ? ` (${changed.join(', ')})` : ' (already unclaimed)';
          return `Released task #${id}${detail}.\n${formatTask(task)}`;
        });
        if (notFound) {
          return { output: `Error: task #${id} not found. [reason: task_not_found]`, isError: true };
        }
        if (alreadyCompleted) {
          return { output: `Error: task #${id} is already completed — nothing to release. [reason: already_resolved]`, isError: true };
        }
        taskDebug(`Releasing claim on task #${id}: ${releasedSubject}`, { status: releasedStatus });
        emitTaskUpdated(context, id, releasedStatus, releasedSubject);
        return { output: capOutput(out), isError: false };
      }

      case 'next': {
        reloadForRead();
        const owner = input.owner as string | undefined;
        const task = findAvailableTask(owner);
        if (!task) {
          taskDebug('No available task to pick up', { owner });
          return { output: owner
            ? `No available tasks for "${owner}" (nothing pending, unowned, and unblocked).`
            : 'No available tasks (nothing pending, unowned, and unblocked).', isError: false };
        }
        taskDebug(`Found available task #${task.id}: ${task.subject}`, { owner });
        return { output: capOutput(`Next available task:\n${formatTask(task)}\n\nClaim it with action "claim", task_id "${task.id}".`), isError: false };
      }

      case 'status': {
        reloadForRead();
        const rows = getAgentStatuses();
        if (rows.length === 0) {
          return { output: 'No owned tasks — all agents idle.', isError: false };
        }
        const body = rows
          .map(r => `${r.owner}: ${r.status}${r.tasks.length ? ` (${r.tasks.map(id => `#${id}`).join(', ')})` : ''}`)
          .join('\n');
        return { output: capOutput(body), isError: false };
      }

      default:
        return { output: `Error: unknown action "${action}". Must be one of: create, update, list, get, delete, claim, release, next, status.`, isError: true };
    }
  },
};
