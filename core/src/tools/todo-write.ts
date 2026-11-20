import { ToolContext, ToolDefinition, ToolResult, TodoItemView, Message } from '../types';
import { coerceBool } from '../utils/coerce';

type TodoStatus = 'pending' | 'in_progress' | 'completed' | 'cancelled';

/** Optional per-item bookkeeping metadata. An item flagged `_internal` is tracked
 *  in the store but hidden from the user-facing list and excluded from the
 *  completion/clear computation (parity: the reference's useTasksV2 / TaskListTool
 *  `t => !t.metadata?._internal` filter). */
interface TodoMetadata {
  _internal?: boolean;
  [key: string]: unknown;
}

interface TodoItem {
  id: string;
  content: string;
  status: TodoStatus;
  /** Present-continuous label shown while the item is in progress (T5). */
  activeForm?: string;
  /** Hidden-task bookkeeping (see {@link TodoMetadata}). */
  metadata?: TodoMetadata;
}

/** Raw item shape as received from the model — `id` and `activeForm` optional (T5). */
interface TodoItemInput {
  id?: string;
  content: string;
  status: TodoStatus;
  activeForm?: string;
  metadata?: TodoMetadata;
}

/** True when an item is an internal/hidden bookkeeping task that must never appear
 *  in the user-facing list or participate in the completion/hide computation. */
function isInternal(item: TodoItem): boolean {
  return item.metadata?._internal === true;
}

/**
 * Session/agent-scoped todo stores (T2). Keyed by
 * `${sessionId ?? 'default'}:${agentId ?? ''}` so concurrent sessions and
 * subagents keep independent lists. A bare/test context (no ids) falls back to
 * the shared `default:` key, which is also what the no-arg `getTodos` /
 * `resetTodos` back-compat helpers operate on.
 */
const sessionTodos = new Map<string, TodoItem[]>();

/** Derive the store key for a given tool context (T2). Accepts any object
 *  carrying `sessionId`/`agentId` so the session-restore path can key the store
 *  without constructing a full {@link ToolContext}. */
function keyFor(context?: Pick<ToolContext, 'sessionId' | 'agentId'>): string {
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

/** Overwrite a session's todo store (restore/rehydration seam). Replaces the
 *  whole list for `key`; an empty array clears it. Used by
 *  {@link rehydrateTodosFromTranscript} so a resumed session repopulates the
 *  in-memory store without going through the model-facing `execute` path. */
export function setTodos(items: readonly TodoItem[], key: string = keyFor()): void {
  if (items.length === 0) sessionTodos.delete(key);
  else sessionTodos.set(key, [...items]);
}

/** Normalize the raw model-supplied items into stored {@link TodoItem}s: assign
 *  an index-based id when omitted and copy through the optional fields. Shared by
 *  {@link todoWriteTool.execute} and {@link extractTodosFromTranscript} so the
 *  resume-rehydration path produces byte-identical items to a live write. */
function normalizeItems(rawItems: readonly TodoItemInput[]): TodoItem[] {
  return rawItems.map((it, idx) => {
    const item: TodoItem = {
      id: it.id ?? `todo-${idx + 1}`,
      content: it.content,
      status: it.status,
    };
    if (it.activeForm !== undefined) item.activeForm = it.activeForm;
    if (it.metadata !== undefined) item.metadata = it.metadata;
    return item;
  });
}

function toView(item: TodoItem): TodoItemView {
  const view: TodoItemView = { id: item.id, content: item.content, status: item.status };
  if (item.activeForm !== undefined) view.activeForm = item.activeForm;
  return view;
}

const VALID_STATUSES: readonly TodoStatus[] = ['pending', 'in_progress', 'completed', 'cancelled'];
const ALLOWED_TOP_KEYS = new Set(['todos', 'merge']);
const ALLOWED_ITEM_KEYS = new Set(['id', 'content', 'status', 'activeForm', 'metadata']);

/** Hard cap on the serialized todo result size (parity with the reference's maxResultSizeChars). */
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
    if (item.metadata !== undefined && (typeof item.metadata !== 'object' || item.metadata === null || Array.isArray(item.metadata))) {
      return `todos[${i}].metadata must be an object.`;
    }
  }
  return null;
}

/**
 * Parity with the reference's `activeForm: z.string().min(1)` contract: the
 * present-continuous label is what the live-progress UI renders while an item is
 * in progress, so every item that ends up `in_progress` must carry a non-empty
 * `activeForm`. Checked on the RESOLVED (post-merge) list so AMI's merge
 * extension can carry a label stored on an earlier write. Returns an error
 * message, or null when every in-progress item has a label.
 */
export function validateInProgressActiveForm(items: readonly TodoItem[]): string | null {
  for (let i = 0; i < items.length; i++) {
    const t = items[i];
    if (t.status === 'in_progress' && (typeof t.activeForm !== 'string' || t.activeForm.trim().length === 0)) {
      return `todos[${i}].activeForm is required (a present-continuous label, e.g. "Running tests") when status is in_progress.`;
    }
  }
  return null;
}

/**
 * Scan a restored transcript (newest-first) for the most recent `todo_write`
 * tool call and return its parsed, normalized todos. Parity with the reference's
 * `extractTodosFromTranscript` (sessionRestore.ts): the model's todo list must
 * survive a session restart even without on-disk task persistence. A malformed /
 * unparseable call, or no call at all, yields an empty list (never throws).
 */
export function extractTodosFromTranscript(messages: readonly Message[]): TodoItem[] {
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (!msg || msg.role !== 'assistant') continue;
    const call = msg.tool_calls?.find(c => c.function?.name === 'todo_write');
    if (!call) continue;
    let input: unknown;
    try {
      input = JSON.parse(call.function.arguments);
    } catch {
      return [];
    }
    if (!input || typeof input !== 'object' || Array.isArray(input)) return [];
    const rec = input as Record<string, unknown>;
    // Reuse the exact same strict validator the live write path uses, so a
    // transcript carrying a malformed call restores nothing rather than a
    // half-valid list.
    if (validateTodoInput(rec)) return [];
    return normalizeItems((rec.todos as TodoItemInput[]) ?? []);
  }
  return [];
}

/**
 * Repopulate the session todo store from a restored transcript. Called from the
 * engine's session-restore path on `--resume`/`sessionId` so the model's last
 * todo list is live again immediately. Returns the number of todos restored (0
 * when the transcript has no usable todo_write call). Parity:
 * `restoreSessionStateFromLog`'s todo branch.
 */
export function rehydrateTodosFromTranscript(
  messages: readonly Message[],
  context?: { sessionId?: string; agentId?: string },
): number {
  const items = extractTodosFromTranscript(messages);
  if (items.length === 0) return 0;
  setTodos(items, keyFor(context));
  return items.length;
}

export const todoWriteTool: ToolDefinition = {
  name: 'todo_write',
  // Terse, intent-focused discovery phrase (synonyms not in the long
  // description) so tool_search can surface this by purpose. Parity: the reference's
  // TodoWriteTool.searchHint ('manage the session task checklist').
  searchHint: 'manage the session task checklist plan progress',
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
    '(an AMI extension). ALWAYS provide both forms for each task: the imperative ' +
    'content (e.g. "Run the tests") AND an activeForm — the present-continuous ' +
    'label (e.g. "Running tests") shown while the item is in progress. activeForm ' +
    'is REQUIRED for any item whose status is in_progress. Item id is optional; ' +
    'if omitted, an index-based id is assigned.\n' +
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
            content: { type: 'string', minLength: 1, description: 'Imperative description of the task.' },
            status: { type: 'string', enum: ['pending', 'in_progress', 'completed', 'cancelled'] },
            activeForm: { type: 'string', minLength: 1, description: 'Present-continuous label shown while this item is in_progress (e.g. "Running tests"). Always provide it; it is REQUIRED whenever status is in_progress.' },
            metadata: { type: 'object', description: 'Optional bookkeeping metadata. Set `_internal: true` to track a task that must stay hidden from the user-facing list and excluded from completion/clear.' },
          },
          required: ['content', 'status'],
          // Strict object: reject unknown per-item keys at the advertised-schema
          // layer too (mirrors validateTodoInput and the reference's z.strictObject).
          additionalProperties: false,
        },
        description: 'Array of todo items. By default REPLACES the current list.',
      },
      merge: { type: 'boolean', description: 'If true, merge with the existing list by id (update matching ids, append new ones) instead of replacing. If false/omitted, replace all.' },
    },
    required: ['todos'],
  },
  isReadOnly: false,
  // Declarative per-tool result-size cap (parity: the reference's TodoWriteTool
  // `maxResultSizeChars: 100_000`). execute() reads this field so the knob is the
  // single authoritative bound on the echoed-back summary, matching todoReadTool
  // and the mcp-resources tools.
  maxResultSizeChars: TODO_MAX_RESULT_CHARS,
  async execute(input: Record<string, unknown>, context?: ToolContext): Promise<ToolResult> {
    const validationError = validateTodoInput(input);
    if (validationError) return { output: `Error: ${validationError}`, isError: true };

    const key = keyFor(context);
    const rawItems = (input.todos as TodoItemInput[]) ?? [];
    const items: TodoItem[] = normalizeItems(rawItems);
    // Parity: route the boolean `merge` arg through coerceBool so a model/provider
    // that serializes it as the STRING "false" (or "true") is handled correctly — a
    // naive `(input.merge as boolean)` treats the non-empty string "false" as truthy
    // and wrongly merges. (the reference's semanticBoolean wrapper applied uniformly.)
    const merge = coerceBool(input.merge);
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

    // Parity: activeForm is required (non-empty) for any item that ends up
    // in_progress — checked on the RESOLVED list so a merge can carry a label
    // stored by an earlier write. Reject without mutating the store.
    const activeFormError = validateInProgressActiveForm(current);
    if (activeFormError) return { output: `Error: ${activeFormError}`, isError: true };

    sessionTodos.set(key, current);

    // Internal bookkeeping tasks are hidden from everything the user/model sees:
    // the rendered summary, the live-update event, and the completion/clear gate.
    const visible = current.filter(t => !isInternal(t));

    let summary = visible.map(t => {
      const icon = t.status === 'completed' ? '[x]' : t.status === 'in_progress' ? '[>]' : t.status === 'cancelled' ? '[-]' : '[ ]';
      return `${icon} ${t.id}: ${t.content}`;
    }).join('\n');

    // T6: nudge for verification only at a precise close-out condition — the
    // main thread (not a subagent) finishing a 3+ item list that contains no
    // verification step of its own. Trivial (1-2 item) lists and lists that
    // already include a verify task never trigger it (parity with the reference).
    // Internal tasks never count toward the threshold or completion.
    const allCompleted = visible.length > 0 && visible.every(t => t.status === 'completed');
    const hasVerifyStep = visible.some(t => /verif/i.test(t.content));
    if (allCompleted && visible.length >= 3 && !hasVerifyStep && !context?.agentId) {
      // Directive, actionable nudge (parity: the reference's TaskUpdate/TodoWrite
      // close-out note) — name the concrete next action and forbid self-declared
      // success, rather than a vague "consider reviewing".
      summary +=
        `\n\n[Verification required: you just closed out a ${visible.length}-item task list and none of ` +
        'the items was a verification step. Before writing your final summary, verify the results — ' +
        'run the relevant tests/build or spawn a verification subagent — and do NOT declare success by ' +
        'listing caveats without a real check.]';
    }

    // T1: emit the (visible) current list so frontends can render it live.
    context?._emitChild?.({ type: 'todo_update', todos: visible.map(toView) });

    const changed = items.filter(i => !isInternal(i)).map(i => i.id);
    // Persistent forward-looking affordance so the model keeps using the list.
    const affordance = '\n\nTodos have been updated. Continue to use the todo list to track progress; proceed with the current tasks.';
    let output = `Todos updated (${visible.length} items, ${changed.length} changed):\n${summary}${affordance}`;
    const cap = todoWriteTool.maxResultSizeChars ?? TODO_MAX_RESULT_CHARS;
    if (output.length > cap) {
      output = output.slice(0, cap) + '\n[... todo result truncated ...]';
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

/**
 * Render the current (visible) todo list as a text block, or a "no todos" line
 * when empty. Shared by {@link todoReadTool}. Internal/hidden tasks are filtered
 * out (parity: the reference's TaskListTool `t => !t.metadata?._internal`).
 */
export function formatTodoReadOutput(key: string = keyFor()): string {
  const items = getTodos(key).filter(t => !isInternal(t));
  if (items.length === 0) return 'No todos found. The session todo list is empty.';
  const lines = items.map(t => {
    const icon = t.status === 'completed' ? '[x]' : t.status === 'in_progress' ? '[>]' : t.status === 'cancelled' ? '[-]' : '[ ]';
    const label = t.status === 'in_progress' && t.activeForm ? ` (${t.activeForm})` : '';
    return `${icon} ${t.id} [${t.status}] ${t.content}${label}`;
  });
  return `Current todos (${items.length}):\n${lines.join('\n')}`;
}

/**
 * Read-only sibling of {@link todoWriteTool}: re-read the current session's todo
 * list at any time WITHOUT mutating it (parity: the reference's TaskListTool —
 * `searchHint 'list all tasks'`, `isReadOnly`, `isConcurrencySafe`). Lets the
 * model (or an introspection surface) observe progress without a no-op write.
 */
export const todoReadTool: ToolDefinition = {
  name: 'todo_read',
  searchHint: 'list all tasks read current todo checklist plan progress',
  description:
    'Read the current session todo list without modifying it. Use this to re-check ' +
    'the plan and progress at any time (e.g. after resuming, or before deciding the ' +
    'next step) instead of issuing a no-op todo_write. Returns each item with its ' +
    'status; internal/hidden tasks are omitted. To create or update items, use todo_write.',
  inputSchema: {
    type: 'object',
    properties: {},
  },
  isReadOnly: true,
  isConcurrencySafe: true,
  maxResultSizeChars: TODO_MAX_RESULT_CHARS,
  async execute(_input: Record<string, unknown>, context?: ToolContext): Promise<ToolResult> {
    let output = formatTodoReadOutput(keyFor(context));
    const cap = todoReadTool.maxResultSizeChars ?? TODO_MAX_RESULT_CHARS;
    if (output.length > cap) {
      output = output.slice(0, cap) + '\n[... todo result truncated ...]';
    }
    return { output };
  },
};
