import { ToolContext, ToolDefinition, ToolResult, TodoItemView } from '../types';

type TodoStatus = 'pending' | 'in_progress' | 'completed' | 'cancelled';

interface TodoItem {
  id: string;
  content: string;
  status: TodoStatus;
  /** Present-continuous label shown while the item is in progress (T5). */
  activeForm?: string;
}

/** Raw item shape as received from the model — `id` and `activeForm` optional (T5). */
interface TodoItemInput {
  id?: string;
  content: string;
  status: TodoStatus;
  activeForm?: string;
}

/**
 * Session/agent-scoped todo stores (T2). Keyed by
 * `${sessionId ?? 'default'}:${agentId ?? ''}` so concurrent sessions and
 * subagents keep independent lists. A bare/test context (no ids) falls back to
 * the shared `default:` key, which is also what the no-arg `getTodos` /
 * `resetTodos` back-compat helpers operate on.
 */
const sessionTodos = new Map<string, TodoItem[]>();

/** Derive the store key for a given tool context (T2). */
function keyFor(context?: ToolContext): string {
  return `${context?.sessionId ?? 'default'}:${context?.agentId ?? ''}`;
}

/** Reset a session's todo store. No-arg clears the shared `default:` store
 *  (back-compat for existing callers). Pass a key for a specific session. */
export function resetTodos(key: string = keyFor()): void {
  sessionTodos.delete(key);
}

/** Read a session's todos. No-arg reads the shared `default:` store
 *  (back-compat). Pass a key for a specific session. */
export function getTodos(key: string = keyFor()): TodoItem[] {
  return [...(sessionTodos.get(key) ?? [])];
}

function toView(item: TodoItem): TodoItemView {
  const view: TodoItemView = { id: item.id, content: item.content, status: item.status };
  if (item.activeForm !== undefined) view.activeForm = item.activeForm;
  return view;
}

export const todoWriteTool: ToolDefinition = {
  name: 'todo_write',
  description:
    'Create or update a structured todo list for the current task. ' +
    'Use this for non-trivial, multi-step work: tasks that need 3+ distinct ' +
    'steps, span multiple files, or where the user gave several requirements — ' +
    'write the plan up front, then keep exactly one item in_progress and mark ' +
    'items completed as you finish them (do not batch completions). ' +
    'Do NOT use it for trivial, single-step tasks, purely conversational ' +
    'replies, or work that is already done — tracking those just adds noise. ' +
    'By default the provided list REPLACES the stored list; pass merge:true to ' +
    'update existing items by id and append new ones instead of replacing ' +
    '(an AMI extension). Each item may carry an optional activeForm — a ' +
    'present-continuous label (e.g. "Running tests") shown while it is in ' +
    'progress. Item id is optional; if omitted, an index-based id is assigned.',
  inputSchema: {
    type: 'object',
    properties: {
      todos: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            id: { type: 'string', description: 'Stable item id (optional). If omitted, an index-based id is generated so merge-by-id still works.' },
            content: { type: 'string', description: 'Imperative description of the task.' },
            status: { type: 'string', enum: ['pending', 'in_progress', 'completed', 'cancelled'] },
            activeForm: { type: 'string', description: 'Optional present-continuous label shown while this item is in_progress (e.g. "Running tests").' },
          },
          required: ['content', 'status'],
        },
        description: 'Array of todo items. By default REPLACES the current list.',
      },
      merge: { type: 'boolean', description: 'If true, merge with the existing list by id (update matching ids, append new ones) instead of replacing. If false/omitted, replace all.' },
    },
    required: ['todos'],
  },
  isReadOnly: false,
  async execute(input: Record<string, unknown>, context?: ToolContext): Promise<ToolResult> {
    const key = keyFor(context);
    const rawItems = (input.todos as TodoItemInput[]) ?? [];
    const items: TodoItem[] = rawItems.map((it, idx) => {
      const item: TodoItem = {
        id: it.id ?? `todo-${idx + 1}`,
        content: it.content,
        status: it.status,
      };
      if (it.activeForm !== undefined) item.activeForm = it.activeForm;
      return item;
    });
    const merge = (input.merge as boolean) ?? false;
    const existing = sessionTodos.get(key) ?? [];
    const oldTodos = [...existing];

    let current: TodoItem[];
    if (merge) {
      current = [...existing];
      for (const item of items) {
        const idx = current.findIndex(t => t.id === item.id);
        if (idx >= 0) current[idx] = { ...current[idx], ...item };
        else current.push(item);
      }
    } else {
      current = items;
    }
    sessionTodos.set(key, current);

    let summary = current.map(t => {
      const icon = t.status === 'completed' ? '[x]' : t.status === 'in_progress' ? '[>]' : t.status === 'cancelled' ? '[-]' : '[ ]';
      return `${icon} ${t.id}: ${t.content}`;
    }).join('\n');

    // T6: only nudge when EVERY item is completed AND we are on the main thread
    // (not a subagent). Subagents and partial progress get no note.
    const allCompleted = current.length > 0 && current.every(t => t.status === 'completed');
    if (allCompleted && !context?.agentId) {
      summary += '\n\n[Verification suggested: all items completed. Consider reviewing the results.]';
    }

    // T1: emit the current list so frontends can render it live.
    context?._emitChild?.({ type: 'todo_update', todos: current.map(toView) });

    const changed = items.map(i => i.id);
    const result: ToolResult = {
      output: `Todos updated (${current.length} items, ${changed.length} changed):\n${summary}`,
      metadata: { oldTodos: oldTodos.map(t => t.id), newTodos: current.map(t => t.id) },
    } as ToolResult;

    // T7: once everything is done, clear the store so a fresh task starts clean.
    if (allCompleted) sessionTodos.delete(key);

    return result;
  },
};
