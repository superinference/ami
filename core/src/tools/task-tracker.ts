import * as fs from 'fs';
import { projectStateDir } from '../reference-journal';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { withFileLock, FileLockOptions } from '../utils/file-lock';

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

export function setTaskPersistPath(cwd: string): void {
  persistPath = path.join(projectStateDir(cwd), 'tasks.json');
  loadFromDisk();
}

const VALID_STATUSES = new Set(['pending', 'in_progress', 'completed']);

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

// Keys the schema accepts; anything else is a malformed call and is rejected so
// typos (e.g. `taskId` vs `task_id`) surface instead of silently no-op'ing.
const ALLOWED_INPUT_KEYS = new Set([
  'action', 'subject', 'description', 'activeForm', 'task_id', 'status',
  'owner', 'metadata', 'addBlocks', 'addBlockedBy', 'checkAgentBusy',
]);

export const taskTrackerTool: ToolDefinition = {
  name: 'task_tracker',
  description:
    'Create and manage tasks to track progress on complex work. ' +
    'Actions: "create" (new task), "update" (change status/details), "list" (show all), "get" (single task), "delete" (remove), "claim" (atomically assign and start), "next" (find the next available task to work on), "status" (per-owner idle/busy introspection). ' +
    'Supports task dependencies via addBlocks/addBlockedBy to express ordering constraints. ' +
    'Prefer tasks in id order (lowest id first). Check the list first to avoid creating duplicate tasks. ' +
    'Pull-based teammates: call "next" (optionally with owner) to get the next pending/unowned/unblocked task, then "claim" it, work it, mark it completed, and repeat. ' +
    'Never mark a task completed on failure or error — leave it in_progress and fix the root cause first; if blocked, create a blocker task instead of closing.',
  inputSchema: {
    type: 'object',
    properties: {
      action: {
        type: 'string',
        enum: ['create', 'update', 'list', 'get', 'delete', 'claim', 'next', 'status'],
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
        description: 'Task ID for update/get/delete/claim actions.',
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
            owner: (input.owner as string) || undefined,
            metadata: input.metadata ? { ...(input.metadata as Record<string, unknown>) } : undefined,
            blocks: [],
            blockedBy: [],
          };
          tasks.set(task.id, task);
          return task;
        });
        if (context._hookManager) {
          context._hookManager.executeTaskCreated({ taskId: String(result.id), subject: result.subject, description: result.description }).catch(() => {});
        }
        return { output: capOutput(`Task #${result.id} created successfully: ${result.subject} [pending]`), isError: false };
      }

      case 'update': {
        const id = parseInt(input.task_id as string, 10);
        if (isNaN(id)) {
          return { output: 'Error: task_id is required for update action.', isError: true };
        }
        // Validate status before taking the lock so a bad value fails cleanly.
        if (input.status && input.status !== 'deleted' && !VALID_STATUSES.has(input.status as string)) {
          return { output: `Error: invalid status "${input.status}". Must be one of: ${[...VALID_STATUSES].join(', ')}, deleted`, isError: true };
        }
        let notFound = false;
        let deleted = false;
        let completed = false;
        const out = await mutate(() => {
          const task = tasks.get(id);
          if (!task) { notFound = true; return ''; }
          if (input.status === 'deleted') {
            tasks.delete(id);
            for (const t of tasks.values()) {
              t.blocks = t.blocks.filter(bid => bid !== id);
              t.blockedBy = t.blockedBy.filter(bid => bid !== id);
            }
            deleted = true;
            return `Task #${id} deleted.`;
          }
          const prevStatus = task.status;
          if (input.status) {
            task.status = input.status as Task['status'];
            if (input.status === 'completed') completed = true;
          }
          if (input.subject) task.subject = (input.subject as string).trim();
          if (input.description !== undefined) task.description = ((input.description as string) || '').trim();
          if (input.activeForm !== undefined) task.activeForm = (input.activeForm as string) || undefined;
          if (input.owner !== undefined) task.owner = (input.owner as string) || undefined;
          if (input.metadata) {
            if (!task.metadata) task.metadata = {};
            for (const [k, v] of Object.entries(input.metadata as Record<string, unknown>)) {
              if (v === null) delete task.metadata[k];
              else task.metadata[k] = v;
            }
          }
          if (input.addBlocks) {
            for (const bid of input.addBlocks as string[]) {
              const blockId = parseInt(bid, 10);
              if (!isNaN(blockId) && tasks.has(blockId) && blockId !== id) {
                if (!task.blocks.includes(blockId)) task.blocks.push(blockId);
                const blockedTask = tasks.get(blockId)!;
                if (!blockedTask.blockedBy.includes(id)) blockedTask.blockedBy.push(id);
              }
            }
          }
          if (input.addBlockedBy) {
            for (const bid of input.addBlockedBy as string[]) {
              const blockerId = parseInt(bid, 10);
              if (!isNaN(blockerId) && tasks.has(blockerId) && blockerId !== id) {
                if (!task.blockedBy.includes(blockerId)) task.blockedBy.push(blockerId);
                const blockerTask = tasks.get(blockerId)!;
                if (!blockerTask.blocks.includes(id)) blockerTask.blocks.push(id);
              }
            }
          }
          const statusLine = (input.status && prevStatus !== task.status)
            ? `Status: ${prevStatus} -> ${task.status}\n`
            : '';
          return statusLine + formatTask(task);
        });
        if (notFound) {
          return { output: `Error: task #${id} not found.`, isError: true };
        }
        if (completed && context._hookManager) {
          context._hookManager.executeTaskCompleted({ taskId: String(id), subject: tasks.get(id)?.subject ?? '' }).catch(() => {});
        }
        void deleted;
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
        const id = parseInt(input.task_id as string, 10);
        if (isNaN(id)) {
          return { output: 'Error: task_id is required for get action.', isError: true };
        }
        reloadForRead();
        const task = tasks.get(id);
        if (!task) {
          return { output: `Error: task #${id} not found.`, isError: true };
        }
        return { output: capOutput(formatTask(task)), isError: false };
      }

      case 'delete': {
        const id = parseInt(input.task_id as string, 10);
        if (isNaN(id)) {
          return { output: 'Error: task_id is required for delete action.', isError: true };
        }
        let notFound = false;
        await mutate(() => {
          if (!tasks.has(id)) { notFound = true; return; }
          tasks.delete(id);
          for (const t of tasks.values()) {
            t.blocks = t.blocks.filter(bid => bid !== id);
            t.blockedBy = t.blockedBy.filter(bid => bid !== id);
          }
        });
        if (notFound) {
          return { output: `Error: task #${id} not found.`, isError: true };
        }
        return { output: `Task #${id} deleted.`, isError: false };
      }

      case 'claim': {
        const id = parseInt(input.task_id as string, 10);
        if (isNaN(id)) {
          return { output: 'Error: task_id is required for claim action.', isError: true };
        }
        const owner = input.owner as string | undefined;
        const checkAgentBusy = input.checkAgentBusy === true;
        // Structured claim-failure reasons mirror Claude Code's ClaimTaskResult.
        let reason: 'task_not_found' | 'already_claimed' | 'already_resolved' | 'blocked' | 'agent_busy' | null = null;
        let detail = '';
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
          if (owner) task.owner = owner;
          return `Claimed task #${id}: ${task.subject} [in_progress]${task.owner ? ` (owner: ${task.owner})` : ''}`;
        });
        if (reason === 'task_not_found') return { output: `Error: task #${id} not found. [reason: task_not_found]`, isError: true };
        if (reason === 'already_claimed') return { output: `Error: task #${id} is already in_progress${detail}. [reason: already_claimed]`, isError: true };
        if (reason === 'already_resolved') return { output: `Error: task #${id} is already completed. [reason: already_resolved]`, isError: true };
        if (reason === 'blocked') return { output: `Error: task #${id} is blocked by: ${detail} [reason: blocked]`, isError: true };
        if (reason === 'agent_busy') return { output: `Error: "${owner}" is already busy with: ${detail} [reason: agent_busy]`, isError: true };
        return { output: capOutput(out), isError: false };
      }

      case 'next': {
        reloadForRead();
        const owner = input.owner as string | undefined;
        const task = findAvailableTask(owner);
        if (!task) {
          return { output: owner
            ? `No available tasks for "${owner}" (nothing pending, unowned, and unblocked).`
            : 'No available tasks (nothing pending, unowned, and unblocked).', isError: false };
        }
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
        return { output: `Error: unknown action "${action}". Must be one of: create, update, list, get, delete, claim, next, status.`, isError: true };
    }
  },
};
