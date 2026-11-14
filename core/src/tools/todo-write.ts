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

const VALID_STATUSES: readonly TodoStatus[] = ['pending', 'in_progress', 'completed', 'cancelled'];
const ALLOWED_TOP_KEYS = new Set(['todos', 'merge']);
const ALLOWED_ITEM_KEYS = new Set(['id', 'content', 'status', 'activeForm']);

/** Hard cap on the serialized todo result size (parity with Claude's maxResultSizeChars). */
export const TODO_MAX_RESULT_CHARS = 100_000;

/**
 * Strict runtime validation of the todo_write input: reject unknown top-level
 * and per-item keys, require a `todos` array, and enforce per-item non-empty
 * content, valid status enum, and non-empty activeForm when present. Returns an
 * error message, or null when the input is valid.
 */
export function validateTodoInput(input: Record<string, unknown>): string | null {
  for (const key of Object.keys(input)) {
    if (!ALLOWED_TOP_KEYS.has(key)) return `Unknown parameter "${key}". Allowed: todos, merge.`;
  }
  // An omitted todos list defaults to empty (clears the list); a present one
  // must be a well-formed array.
  if (input.todos === undefined) return null;
  if (!Array.isArray(input.todos)) return 'todos must be an array.';
  for (let i = 0; i < input.todos.length; i++) {
    const raw = input.todos[i];
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return `todos[${i}] must be an object.`;
    const item = raw as Record<string, unknown>;
    for (const key of Object.keys(item)) {
      if (!ALLOWED_ITEM_KEYS.has(key)) return `todos[${i}] has unknown property "${key}".`;
    }
    if (typeof item.content !== 'string' || item.content.trim().length === 0) {
      return `todos[${i}].content cannot be empty.`;
    }
    if (typeof item.status !== 'string' || !(VALID_STATUSES as readonly string[]).includes(item.status)) {
      return `todos[${i}].status must be one of ${VALID_STATUSES.join(', ')}.`;
    }
    if (item.id !== undefined && typeof item.id !== 'string') return `todos[${i}].id must be a string.`;
    if (item.activeForm !== undefined && (typeof item.activeForm !== 'string' || item.activeForm.trim().length === 0)) {
      return `todos[${i}].activeForm cannot be empty when provided.`;
    }
  }
  return null;
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
    'progress. Item id is optional; if omitted, an index-based id is assigned.\n' +
    'Discipline: mark an item in_progress BEFORE you start it, and keep exactly ' +
    'one item in_progress at a time — complete the current item before starting ' +
    'the next. ONLY mark an item completed when it is FULLY accomplished; if you ' +
    'hit an error, are blocked, or only partially finished, keep it in_progress ' +
    'and add a new task describing what must be resolved. Capture new work as you ' +
    'learn it: immediately record new user instructions, and append follow-up or ' +
    'blocker tasks discovered mid-work. Remove tasks that are no longer relevant.',
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
    const validationError = validateTodoInput(input);
    if (validationError) return { output: `Error: ${validationError}`, isError: true };

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

    // T6: nudge for verification only at a precise close-out condition — the
    // main thread (not a subagent) finishing a 3+ item list that contains no
    // verification step of its own. Trivial (1-2 item) lists and lists that
    // already include a verify task never trigger it (parity with Claude).
    const allCompleted = current.length > 0 && current.every(t => t.status === 'completed');
    const hasVerifyStep = current.some(t => /verif/i.test(t.content));
    if (allCompleted && current.length >= 3 && !hasVerifyStep && !context?.agentId) {
      summary += '\n\n[Verification suggested: all items completed and no verification step was tracked. Review/verify the results before finishing.]';
    }

    // T1: emit the current list so frontends can render it live.
    context?._emitChild?.({ type: 'todo_update', todos: current.map(toView) });

    const changed = items.map(i => i.id);
    // Persistent forward-looking affordance so the model keeps using the list.
    const affordance = '\n\nTodos have been updated. Continue to use the todo list to track progress; proceed with the current tasks.';
    let output = `Todos updated (${current.length} items, ${changed.length} changed):\n${summary}${affordance}`;
    if (output.length > TODO_MAX_RESULT_CHARS) {
      output = output.slice(0, TODO_MAX_RESULT_CHARS) + '\n[... todo result truncated ...]';
    }
    const result: ToolResult = {
      output,
      metadata: { oldTodos: oldTodos.map(t => t.id), newTodos: current.map(t => t.id) },
    } as ToolResult;

    // T7: once everything is done, clear the store so a fresh task starts clean.
    if (allCompleted) sessionTodos.delete(key);

    return result;
  },
};
