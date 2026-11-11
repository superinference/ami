import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { ToolContext, EngineEvent } from '../src/types';
import { todoWriteTool, getTodos, resetTodos } from '../src/tools/todo-write';

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

describe('todo-write cluster (T1-T8)', () => {
  beforeEach(() => {
    // Clear every store key we touch in these tests.
    resetTodos();                 // default:
    resetTodos('s1:');
    resetTodos('s2:');
    resetTodos('s1:a1');
  });

  // --- T1: emit --------------------------------------------------------------
  it('T1: emits todo_update with the current list after updating', async () => {
    const { context, events } = ctx({ sessionId: 's1' });
    await todoWriteTool.execute({
      todos: [{ id: '1', content: 'Write code', status: 'in_progress', activeForm: 'Writing code' }],
    }, context);

    const emitted = events.filter(e => e.type === 'todo_update');
    assert.equal(emitted.length, 1);
    const ev = emitted[0] as Extract<EngineEvent, { type: 'todo_update' }>;
    assert.equal(ev.todos.length, 1);
    assert.equal(ev.todos[0].content, 'Write code');
    assert.equal(ev.todos[0].status, 'in_progress');
    assert.equal(ev.todos[0].activeForm, 'Writing code');
  });

  it('T1: works without _emitChild (no throw, back-compat)', async () => {
    const r = await todoWriteTool.execute({
      todos: [{ id: '1', content: 'x', status: 'pending' }],
    }); // no context at all
    assert.ok(!r.isError);
  });

  // --- T2: session-keyed store ----------------------------------------------
  it('T2: two sessionIds keep separate lists', async () => {
    const a = ctx({ sessionId: 's1' });
    const b = ctx({ sessionId: 's2' });
    await todoWriteTool.execute({ todos: [{ id: '1', content: 'A', status: 'pending' }] }, a.context);
    await todoWriteTool.execute({ todos: [{ id: '2', content: 'B', status: 'pending' }] }, b.context);

    assert.deepEqual(getTodos('s1:').map(t => t.id), ['1']);
    assert.deepEqual(getTodos('s2:').map(t => t.id), ['2']);
  });

  it('T2: same session + different agentId keep separate lists', async () => {
    const main = ctx({ sessionId: 's1' });
    const sub = ctx({ sessionId: 's1', agentId: 'a1' });
    await todoWriteTool.execute({ todos: [{ id: 'm', content: 'main', status: 'pending' }] }, main.context);
    await todoWriteTool.execute({ todos: [{ id: 's', content: 'sub', status: 'pending' }] }, sub.context);

    assert.deepEqual(getTodos('s1:').map(t => t.id), ['m']);
    assert.deepEqual(getTodos('s1:a1').map(t => t.id), ['s']);
  });

  it('T2: no-arg getTodos/resetTodos operate on the default key (back-compat)', async () => {
    await todoWriteTool.execute({ todos: [{ id: 'd', content: 'default', status: 'pending' }] }); // no context
    assert.equal(getTodos().length, 1);
    assert.equal(getTodos()[0].id, 'd');
    resetTodos();
    assert.equal(getTodos().length, 0);
  });

  // --- T5: activeForm + optional id -----------------------------------------
  it('T5: generates a fallback id when id is omitted', async () => {
    const { context } = ctx({ sessionId: 's1' });
    await todoWriteTool.execute({
      todos: [
        { content: 'first', status: 'pending' },
        { content: 'second', status: 'pending' },
      ],
    }, context);
    const todos = getTodos('s1:');
    assert.deepEqual(todos.map(t => t.id), ['todo-1', 'todo-2']);
  });

  it('T5: merge by generated id still works', async () => {
    const { context } = ctx({ sessionId: 's1' });
    await todoWriteTool.execute({ todos: [{ content: 'first', status: 'pending' }] }, context);
    await todoWriteTool.execute({
      todos: [{ id: 'todo-1', content: 'first done', status: 'in_progress' }],
      merge: true,
    }, context);
    const todos = getTodos('s1:');
    assert.equal(todos.length, 1);
    assert.equal(todos[0].content, 'first done');
    assert.equal(todos[0].status, 'in_progress');
  });

  it('T5: merge updates by explicit id and appends new', async () => {
    const { context } = ctx({ sessionId: 's1' });
    await todoWriteTool.execute({
      todos: [
        { id: '1', content: 'one', status: 'pending' },
        { id: '2', content: 'two', status: 'pending' },
      ],
    }, context);
    await todoWriteTool.execute({
      todos: [
        { id: '2', content: 'two updated', status: 'in_progress' },
        { id: '3', content: 'three', status: 'pending' },
      ],
      merge: true,
    }, context);
    const todos = getTodos('s1:');
    assert.deepEqual(todos.map(t => t.id), ['1', '2', '3']);
    assert.equal(todos[1].content, 'two updated');
  });

  it('T5: inputSchema makes id optional but keeps content/status required', () => {
    const itemSchema = (todoWriteTool.inputSchema.properties as any).todos.items;
    assert.deepEqual(itemSchema.required, ['content', 'status']);
    assert.ok(itemSchema.properties.activeForm);
    assert.deepEqual(itemSchema.properties.status.enum, ['pending', 'in_progress', 'completed', 'cancelled']);
  });

  // --- T6: nudge gate --------------------------------------------------------
  it('T6: nudges when ALL items completed on the main thread', async () => {
    const { context } = ctx({ sessionId: 's1' });
    const r = await todoWriteTool.execute({
      todos: [
        { id: '1', content: 'a', status: 'completed' },
        { id: '2', content: 'b', status: 'completed' },
      ],
    }, context);
    assert.match(r.output, /Verification suggested/);
  });

  it('T6: does NOT nudge when running inside a subagent (agentId set)', async () => {
    const { context } = ctx({ sessionId: 's1', agentId: 'a1' });
    const r = await todoWriteTool.execute({
      todos: [
        { id: '1', content: 'a', status: 'completed' },
        { id: '2', content: 'b', status: 'completed' },
      ],
    }, context);
    assert.doesNotMatch(r.output, /Verification suggested/);
  });

  it('T6: does NOT nudge on partial completion', async () => {
    const { context } = ctx({ sessionId: 's1' });
    const r = await todoWriteTool.execute({
      todos: [
        { id: '1', content: 'a', status: 'completed' },
        { id: '2', content: 'b', status: 'pending' },
      ],
    }, context);
    assert.doesNotMatch(r.output, /Verification suggested/);
  });

  it('T6: does NOT nudge for an empty list', async () => {
    const { context } = ctx({ sessionId: 's1' });
    const r = await todoWriteTool.execute({ todos: [] }, context);
    assert.doesNotMatch(r.output, /Verification suggested/);
  });

  // --- T7: clear-when-done ---------------------------------------------------
  it('T7: clears the session store once every item is completed', async () => {
    const { context, events } = ctx({ sessionId: 's1' });
    await todoWriteTool.execute({
      todos: [
        { id: '1', content: 'a', status: 'completed' },
        { id: '2', content: 'b', status: 'completed' },
      ],
    }, context);
    // Emitted list reflects the completed items...
    const ev = events.find(e => e.type === 'todo_update') as Extract<EngineEvent, { type: 'todo_update' }>;
    assert.equal(ev.todos.length, 2);
    // ...but the store is cleared afterward so a fresh task starts clean.
    assert.equal(getTodos('s1:').length, 0);
  });

  it('T7: subagent store is also cleared when all completed (even without nudge)', async () => {
    const { context } = ctx({ sessionId: 's1', agentId: 'a1' });
    await todoWriteTool.execute({
      todos: [{ id: '1', content: 'a', status: 'completed' }],
    }, context);
    assert.equal(getTodos('s1:a1').length, 0);
  });

  it('T7: does NOT clear while work is still pending', async () => {
    const { context } = ctx({ sessionId: 's1' });
    await todoWriteTool.execute({
      todos: [
        { id: '1', content: 'a', status: 'completed' },
        { id: '2', content: 'b', status: 'in_progress' },
      ],
    }, context);
    assert.equal(getTodos('s1:').length, 2);
  });
});
