import { ToolDefinition, ToolResult } from '../types';

interface Task {
  id: number;
  subject: string;
  description: string;
  status: 'pending' | 'in_progress' | 'completed';
  activeForm?: string;
}

let nextId = 1;
const tasks = new Map<number, Task>();

export function resetTaskState(): void {
  nextId = 1;
  tasks.clear();
}

export function getTaskState(): Map<number, Task> {
  return tasks;
}

const STATUS_ICONS: Record<string, string> = {
  pending: '[ ]',
  in_progress: '[>]',
  completed: '[x]',
};

function formatTask(task: Task): string {
  const icon = STATUS_ICONS[task.status] || '[ ]';
  const lines = [
    `${icon} Task #${task.id}: ${task.subject} [${task.status}]`,
  ];
  if (task.description) {
    lines.push(`    ${task.description}`);
  }
  return lines.join('\n');
}

export const taskTrackerTool: ToolDefinition = {
  name: 'task_tracker',
  description:
    'Create and manage tasks to track progress on complex work. ' +
    'Actions: "create" (new task), "update" (change status/details), "list" (show all), "get" (single task), "delete" (remove). ' +
    'Use for multi-step tasks to maintain visibility into progress.',
  inputSchema: {
    type: 'object',
    properties: {
      action: {
        type: 'string',
        enum: ['create', 'update', 'list', 'get', 'delete'],
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
        enum: ['pending', 'in_progress', 'completed'],
        description: 'New status for update action.',
      },
    },
    required: ['action'],
  },
  isReadOnly: false,

  async execute(input): Promise<ToolResult> {
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
        };
        tasks.set(task.id, task);
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
          const validStatuses = ['pending', 'in_progress', 'completed'];
          if (!validStatuses.includes(input.status as string)) {
            return { output: `Error: invalid status "${input.status}". Must be one of: ${validStatuses.join(', ')}`, isError: true };
          }
          task.status = input.status as Task['status'];
        }
        if (input.subject) task.subject = (input.subject as string).trim();
        if (input.description !== undefined) task.description = ((input.description as string) || '').trim();
        if (input.activeForm !== undefined) task.activeForm = (input.activeForm as string) || undefined;
        return { output: formatTask(task), isError: false };
      }

      case 'list': {
        if (tasks.size === 0) {
          return { output: 'No tasks.', isError: false };
        }
        const sorted = Array.from(tasks.values()).sort((a, b) => a.id - b.id);
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
        return { output: `Task #${id} deleted.`, isError: false };
      }

      default:
        return { output: `Error: unknown action "${action}". Must be one of: create, update, list, get, delete.`, isError: true };
    }
  },
};
