import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';

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

let nextId = 1;
const tasks = new Map<number, Task>();
let persistPath: string | null = null;

export function resetTaskState(): void {
  nextId = 1;
  tasks.clear();
}

export function getTaskState(): Map<number, Task> {
  return tasks;
}

export function setTaskPersistPath(cwd: string): void {
  persistPath = path.join(cwd, '.superinference', 'tasks.json');
  loadFromDisk();
}

function loadFromDisk(): void {
  if (!persistPath) return;
  try {
    const raw = fs.readFileSync(persistPath, 'utf-8');
    const data = JSON.parse(raw) as { nextId: number; tasks: Task[] };
    tasks.clear();
    let maxId = 0;
    for (const t of data.tasks) {
      tasks.set(t.id, t);
      if (t.id > maxId) maxId = t.id;
    }
    // High-water-mark: always use max of stored nextId and max existing ID + 1
    nextId = Math.max(data.nextId || 1, maxId + 1);
  } catch {
    // No file or invalid — start fresh
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
    // Best effort — don't break tool execution on write failure
  }
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
  const icon = STATUS_ICONS[task.status] || '[ ]';
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

export const taskTrackerTool: ToolDefinition = {
  name: 'task_tracker',
  description:
    'Create and manage tasks to track progress on complex work. ' +
    'Actions: "create" (new task), "update" (change status/details), "list" (show all), "get" (single task), "delete" (remove), "claim" (atomically assign and start). ' +
    'Supports task dependencies via addBlocks/addBlockedBy to express ordering constraints.',
  inputSchema: {
    type: 'object',
    properties: {
      action: {
        type: 'string',
        enum: ['create', 'update', 'list', 'get', 'delete', 'claim'],
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
        description: 'Present-tense form shown during execution, e.g. "Running tests" (create/update).',
      },
      task_id: {
        type: 'string',
        description: 'Task ID for update/get/delete actions.',
      },
      status: {
        type: 'string',
        enum: ['pending', 'in_progress', 'completed', 'deleted'],
        description: 'New status for update action. Use "deleted" to permanently remove a task.',
      },
      owner: {
        type: 'string',
        description: 'Owner/assignee for the task (update).',
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
    },
    required: ['action'],
  },
  isReadOnly: false,

  async execute(input, context: ToolContext): Promise<ToolResult> {
    const action = input.action as string;

    switch (action) {
      case 'create': {
        const subject = input.subject as string | undefined;
        if (!subject || subject.trim().length === 0) {
          return { output: 'Error: subject is required for create action.', isError: true };
        }
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
        if (context._hookManager) {
          context._hookManager.executeTaskCreated({ taskId: String(task.id), subject: task.subject, description: task.description }).catch(() => {});
        }
        saveToDisk();
        return { output: `Task #${task.id} created: ${task.subject} [pending]`, isError: false };
      }

      case 'update': {
        const id = parseInt(input.task_id as string, 10);
        if (isNaN(id)) {
          return { output: 'Error: task_id is required for update action.', isError: true };
        }
        const task = tasks.get(id);
        if (!task) {
          return { output: `Error: task #${id} not found.`, isError: true };
        }
        if (input.status) {
          if (input.status === 'deleted') {
            tasks.delete(id);
            for (const t of tasks.values()) {
              t.blocks = t.blocks.filter(bid => bid !== id);
              t.blockedBy = t.blockedBy.filter(bid => bid !== id);
            }
            saveToDisk();
            return { output: `Task #${id} deleted.`, isError: false };
          }
          const validStatuses = ['pending', 'in_progress', 'completed'];
          if (!validStatuses.includes(input.status as string)) {
            return { output: `Error: invalid status "${input.status}". Must be one of: ${validStatuses.join(', ')}, deleted`, isError: true };
          }
          task.status = input.status as Task['status'];
          if (context._hookManager && input.status === 'completed') {
            context._hookManager.executeTaskCompleted({ taskId: String(input.task_id), subject: task.subject }).catch(() => {});
          }
        }
        if (input.subject) task.subject = (input.subject as string).trim();
        if (input.description !== undefined) task.description = ((input.description as string) || '').trim();
        if (input.activeForm !== undefined) task.activeForm = (input.activeForm as string) || undefined;
        if (input.owner !== undefined) task.owner = (input.owner as string) || undefined;

        if (input.metadata) {
          if (!task.metadata) task.metadata = {};
          for (const [k, v] of Object.entries(input.metadata as Record<string, unknown>)) {
            if (v === null) {
              delete task.metadata[k];
            } else {
              task.metadata[k] = v;
            }
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

        saveToDisk();
        return { output: formatTask(task), isError: false };
      }

      case 'list': {
        const visibleTasks = [...tasks.values()].filter(t => 
          !t.metadata?._internal
        );
        if (visibleTasks.length === 0) {
          return { output: 'No tasks.', isError: false };
        }
        const sorted = visibleTasks.sort((a, b) => a.id - b.id);
        return { output: sorted.map(formatTask).join('\n'), isError: false };
      }

      case 'get': {
        const id = parseInt(input.task_id as string, 10);
        if (isNaN(id)) {
          return { output: 'Error: task_id is required for get action.', isError: true };
        }
        const task = tasks.get(id);
        if (!task) {
          return { output: `Error: task #${id} not found.`, isError: true };
        }
        return { output: formatTask(task), isError: false };
      }

      case 'delete': {
        const id = parseInt(input.task_id as string, 10);
        if (isNaN(id)) {
          return { output: 'Error: task_id is required for delete action.', isError: true };
        }
        if (!tasks.has(id)) {
          return { output: `Error: task #${id} not found.`, isError: true };
        }
        tasks.delete(id);
        for (const t of tasks.values()) {
          t.blocks = t.blocks.filter(bid => bid !== id);
          t.blockedBy = t.blockedBy.filter(bid => bid !== id);
        }
        saveToDisk();
        return { output: `Task #${id} deleted.`, isError: false };
      }

      case 'claim': {
        const id = parseInt(input.task_id as string, 10);
        if (isNaN(id)) {
          return { output: 'Error: task_id is required for claim action.', isError: true };
        }
        const task = tasks.get(id);
        if (!task) {
          return { output: `Error: task #${id} not found.`, isError: true };
        }
        if (task.status === 'in_progress') {
          return { output: `Error: task #${id} is already in_progress${task.owner ? ` (owned by ${task.owner})` : ''}.`, isError: true };
        }
        if (task.status === 'completed') {
          return { output: `Error: task #${id} is already completed.`, isError: true };
        }
        const blockers = unresolvedBlockers(task);
        if (blockers.length > 0) {
          return { output: `Error: task #${id} is blocked by: ${blockers.map(b => `#${b}`).join(', ')}`, isError: true };
        }
        task.status = 'in_progress';
        if (input.owner) task.owner = (input.owner as string);
        saveToDisk();
        return { output: `Claimed task #${id}: ${task.subject} [in_progress]${task.owner ? ` (owner: ${task.owner})` : ''}`, isError: false };
      }

      default:
        return { output: `Error: unknown action "${action}". Must be one of: create, update, list, get, delete, claim.`, isError: true };
    }
  },
};
