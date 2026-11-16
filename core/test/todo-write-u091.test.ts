import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { ToolContext, EngineEvent } from '../src/types';
import {
  todoWriteTool,
  todoReadTool,
  getTodos,
  resetTodos,
  formatTodoReadOutput,
  validateInProgressActiveForm,
} from '../src/tools/todo-write';
import { createDefaultTools } from '../src/tools/index';
import { PermissionManager } from '../src/permissions';

/** Build a plain ToolContext with an _emitChild capture buffer. */
function ctx(overrides?: Partial<ToolContext>): { context: ToolContext; events: EngineEvent[] } {
  const events: EngineEvent[] = [];
  const context: ToolContext = {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _emitChild: (e: EngineEvent) => { events.push(e); },
    ...overrides,
  };
  return { context, events };
}

describe('U091 — todo-write parity additions', () => {
  beforeEach(() => {
    resetTodos();
    resetTodos('s1:');
    resetTodos('s1:a1');
  });

  // --- Gap: activeForm required (non-empty) when in_progress -----------------
  describe('activeForm required when in_progress (parity: z.string().min(1))', () => {
    it('rejects an in_progress item with no activeForm', async () => {
      const { context } = ctx({ sessionId: 's1' });
      const r = await todoWriteTool.execute({
        todos: [{ id: '1', content: 'build', status: 'in_progress' }],
      }, context);
      assert.equal(r.isError, true);
      assert.match(r.output, /activeForm is required/);
      // store must NOT be mutated on a rejected write
      assert.equal(getTodos('s1:').length, 0);
    });

    it('rejects an in_progress item whose activeForm is whitespace', async () => {
      const r = await todoWriteTool.execute({
        todos: [{ id: '1', content: 'build', status: 'in_progress', activeForm: '   ' }],
      });
      // empty-when-provided fires first; both guard the same contract
      assert.equal(r.isError, true);
      assert.match(r.output, /activeForm/);
    });

    it('accepts an in_progress item WITH a non-empty activeForm', async () => {
      const { context } = ctx({ sessionId: 's1' });
      const r = await todoWriteTool.execute({
        todos: [{ id: '1', content: 'build', status: 'in_progress', activeForm: 'Building' }],
      }, context);
      assert.ok(!r.isError);
      assert.equal(getTodos('s1:')[0].activeForm, 'Building');
    });

    it('allows pending/completed items without activeForm (AMI optional extension kept)', async () => {
      const r = await todoWriteTool.execute({
        todos: [
          { id: '1', content: 'a', status: 'pending' },
          { id: '2', content: 'b', status: 'completed' },
        ],
      });
      assert.ok(!r.isError);
    });

    it('merge: a stored activeForm satisfies the in_progress requirement (delta omits it)', async () => {
      const { context } = ctx({ sessionId: 's1' });
      await todoWriteTool.execute({
        todos: [{ id: '1', content: 'build', status: 'pending', activeForm: 'Building' }],
      }, context);
      // delta flips status to in_progress but carries no activeForm — the stored
      // label must carry over so this is accepted (merge extension preserved).
      const r = await todoWriteTool.execute({
        todos: [{ id: '1', content: 'build', status: 'in_progress' }],
        merge: true,
      }, context);
      assert.ok(!r.isError, r.output);
      assert.equal(getTodos('s1:')[0].status, 'in_progress');
    });

    it('validateInProgressActiveForm is exported and pure', () => {
      assert.equal(validateInProgressActiveForm([{ id: '1', content: 'x', status: 'completed' }] as any), null);
      assert.match(
        validateInProgressActiveForm([{ id: '1', content: 'x', status: 'in_progress' }] as any) ?? '',
        /activeForm is required/,
      );
    });
  });

  // --- Gap: internal (hidden) bookkeeping tasks via metadata._internal -------
  describe('internal metadata filtering (parity: t => !t.metadata?._internal)', () => {
    it('hides _internal items from the emitted todo_update view', async () => {
      const { context, events } = ctx({ sessionId: 's1' });
      await todoWriteTool.execute({
        todos: [
          { id: '1', content: 'visible', status: 'pending' },
          { id: '2', content: 'hidden', status: 'pending', metadata: { _internal: true } },
        ],
      }, context);
      const ev = events.find(e => e.type === 'todo_update') as Extract<EngineEvent, { type: 'todo_update' }>;
      assert.equal(ev.todos.length, 1);
      assert.equal(ev.todos[0].id, '1');
    });

    it('still persists the _internal item in the store (tracked, just hidden)', async () => {
      const { context } = ctx({ sessionId: 's1' });
      await todoWriteTool.execute({
        todos: [
          { id: '1', content: 'visible', status: 'pending' },
          { id: '2', content: 'hidden', status: 'pending', metadata: { _internal: true } },
        ],
      }, context);
      assert.equal(getTodos('s1:').length, 2);
    });

    it('excludes _internal items from the user-facing summary and count', async () => {
      const { context } = ctx({ sessionId: 's1' });
      const r = await todoWriteTool.execute({
        todos: [
          { id: '1', content: 'visible', status: 'pending' },
          { id: '2', content: 'hidden', status: 'pending', metadata: { _internal: true } },
        ],
      }, context);
      assert.match(r.output, /1 items/);
      assert.doesNotMatch(r.output, /hidden/);
    });

    it('does not count _internal items toward the all-completed clear/nudge gate', async () => {
      const { context } = ctx({ sessionId: 's1' });
      // 3 visible completed would normally nudge; but an internal pending item
      // must NOT keep it from being "all visible completed".
      const r = await todoWriteTool.execute({
        todos: [
          { id: '1', content: 'a', status: 'completed' },
          { id: '2', content: 'b', status: 'completed' },
          { id: '3', content: 'c', status: 'completed' },
          { id: '4', content: 'bookkeeping', status: 'pending', metadata: { _internal: true } },
        ],
      }, context);
      assert.match(r.output, /Verification required/);
      // allCompleted (visible) => store cleared
      assert.equal(getTodos('s1:').length, 0);
    });

    it('rejects a non-object metadata value', async () => {
      const r = await todoWriteTool.execute({
        todos: [{ id: '1', content: 'a', status: 'pending', metadata: 'nope' }],
      } as any);
      assert.equal(r.isError, true);
      assert.match(r.output, /metadata must be an object/);
    });

    it('rejects a null metadata value', async () => {
      const r = await todoWriteTool.execute({
        todos: [{ id: '1', content: 'a', status: 'pending', metadata: null }],
      } as any);
      assert.equal(r.isError, true);
      assert.match(r.output, /metadata must be an object/);
    });

    it('rejects an array metadata value', async () => {
      const r = await todoWriteTool.execute({
        todos: [{ id: '1', content: 'a', status: 'pending', metadata: [] }],
      } as any);
      assert.equal(r.isError, true);
      assert.match(r.output, /metadata must be an object/);
    });

    it('accepts non-internal metadata (shown like any normal item)', async () => {
      const { context, events } = ctx({ sessionId: 's1' });
      const r = await todoWriteTool.execute({
        todos: [{ id: '1', content: 'labelled', status: 'pending', metadata: { note: 'x' } }],
      }, context);
      assert.ok(!r.isError);
      const ev = events.find(e => e.type === 'todo_update') as Extract<EngineEvent, { type: 'todo_update' }>;
      assert.equal(ev.todos.length, 1);
    });

    it('advertises the metadata property in the item schema', () => {
      const itemSchema = (todoWriteTool.inputSchema.properties as any).todos.items;
      assert.ok(itemSchema.properties.metadata);
      assert.equal(itemSchema.properties.metadata.type, 'object');
    });
  });

  // --- Gap: read-only todo_read tool (parity: TaskListTool) ------------------
  describe('todo_read read-only tool (parity: TaskListTool)', () => {
    it('is registered in the default tool registry', () => {
      const registry = createDefaultTools('/tmp');
      const tool = registry.get('todo_read');
      assert.ok(tool, 'todo_read must be registered');
      assert.equal(tool!.isReadOnly, true);
      assert.equal(tool!.isConcurrencySafe, true);
    });

    it('reads the current session list without mutating it', async () => {
      const w = ctx({ sessionId: 's1' });
      await todoWriteTool.execute({
        todos: [
          { id: '1', content: 'first', status: 'in_progress', activeForm: 'Doing first' },
          { id: '2', content: 'second', status: 'pending' },
        ],
      }, w.context);
      const before = getTodos('s1:').length;
      const r = await todoReadTool.execute({}, w.context);
      assert.ok(!r.isError);
      assert.match(r.output, /first/);
      assert.match(r.output, /second/);
      assert.match(r.output, /Doing first/); // activeForm shown for in_progress
      assert.equal(getTodos('s1:').length, before); // unchanged
    });

    it('filters _internal items from the read output', async () => {
      const w = ctx({ sessionId: 's1' });
      await todoWriteTool.execute({
        todos: [
          { id: '1', content: 'visible', status: 'pending' },
          { id: '2', content: 'secret', status: 'pending', metadata: { _internal: true } },
        ],
      }, w.context);
      const r = await todoReadTool.execute({}, w.context);
      assert.match(r.output, /visible/);
      assert.doesNotMatch(r.output, /secret/);
    });

    it('reports an empty list clearly', async () => {
      const r = await todoReadTool.execute({}, ctx({ sessionId: 's1' }).context);
      assert.match(r.output, /No todos found/);
    });

    it('renders every status icon', async () => {
      const w = ctx({ sessionId: 's1' });
      await todoWriteTool.execute({
        todos: [
          { id: '1', content: 'p', status: 'pending' },
          { id: '2', content: 'w', status: 'in_progress', activeForm: 'Working' },
          { id: '3', content: 'd', status: 'completed' },
          { id: '4', content: 'x', status: 'cancelled' },
        ],
      }, w.context);
      const r = await todoReadTool.execute({}, w.context);
      assert.match(r.output, /\[ \] 1 \[pending\] p/);
      assert.match(r.output, /\[>\] 2 \[in_progress\] w \(Working\)/);
      assert.match(r.output, /\[x\] 3 \[completed\] d/);
      assert.match(r.output, /\[-\] 4 \[cancelled\] x/);
    });

    it('truncates an oversized read result', async () => {
      const w = ctx({ sessionId: 's1' });
      await todoWriteTool.execute({
        todos: [{ id: '1', content: 'Z'.repeat(120_000), status: 'pending' }],
      }, w.context);
      const r = await todoReadTool.execute({}, w.context);
      assert.match(r.output, /todo result truncated/);
    });

    it('works without a context (default key)', async () => {
      await todoWriteTool.execute({ todos: [{ id: 'd', content: 'deflt', status: 'pending' }] });
      const r = await todoReadTool.execute({});
      assert.match(r.output, /deflt/);
    });

    it('declares a concise searchHint distinct from the description', () => {
      assert.equal(typeof todoReadTool.searchHint, 'string');
      assert.match(todoReadTool.searchHint as string, /task|todo|list/i);
      assert.ok((todoReadTool.searchHint as string).length < todoReadTool.description.length);
    });

    it('is allowed in plan mode (read-only) while todo_write is denied', async () => {
      const pm = new PermissionManager('plan');
      assert.equal(await pm.check('todo_read', {}), 'allow');
      assert.equal(await pm.check('todo_write', { todos: [] }), 'deny');
    });

    it('formatTodoReadOutput helper renders the shared view', async () => {
      await todoWriteTool.execute({ todos: [{ id: 'x', content: 'solo', status: 'pending' }] }); // default key
      assert.match(formatTodoReadOutput(), /solo/);
      resetTodos();
      assert.match(formatTodoReadOutput(), /No todos found/);
    });
  });

  // --- Gap: description/contract strengthening (activeForm mandatory) --------
  it('description reinforces the always-provide-both-forms contract', () => {
    assert.match(todoWriteTool.description, /ALWAYS provide both forms/);
    assert.match(todoWriteTool.description, /REQUIRED for any item whose status is in_progress/);
  });
});
