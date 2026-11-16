import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskTool, resetAgentRegistry, SUBAGENT_RETURN_DIRECTIVE } from '../src/tools/task';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

function gen(events: any[]) {
  return async function* (_p: string) {
    for (const e of events) yield e;
  };
}

// ===========================================================================
// task.ts — universal plan_mode guard (the unconditional strip applied to
// EVERY resolved tool set, regardless of how it was built). This is the guard
// that runs after all mode/agent-def branches: even a tool set that already
// excludes plan_mode must pass through it unchanged, and one that reintroduced
// plan_mode via an explicit agent-def allow-list must have it removed.
// ===========================================================================

describe('taskTool — universal plan_mode guard (post-branch strip)', () => {
  beforeEach(() => resetAgentRegistry());

  it('strips plan_mode from an agent-def allow-list that explicitly re-adds it', async () => {
    let cfg: any = null;
    await taskTool.execute(
      { prompt: 'plan something', subagent_type: 'planner' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _skillManager: {
          // The allow-list names plan_mode AND file_read; the guard must remove
          // only plan_mode and keep the rest intact.
          getAgent: () => ({ name: 'planner', tools: ['file_read', 'grep', 'plan_mode'] }),
          listAgents: () => [],
        } as any,
        _engineFactory: (c: any) => { cfg = c; return { submit: gen([{ type: 'text_delta', text: 'ok' }]) }; },
      }),
    );
    const names = cfg.tools.map((t: any) => t.name);
    assert.ok(!names.includes('plan_mode'), 'plan_mode must be stripped even when allow-listed by an agent def');
    assert.ok(names.includes('file_read'), 'other allow-listed tools survive the guard');
    assert.ok(names.includes('grep'), 'other allow-listed tools survive the guard');
  });

  it('never advertises plan_mode to a default general-mode subagent engine', async () => {
    let cfg: any = null;
    await taskTool.execute(
      { prompt: 'do some work' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: (c: any) => { cfg = c; return { submit: gen([{ type: 'text_delta', text: 'ok' }]) }; },
      }),
    );
    const names = cfg.tools.map((t: any) => t.name);
    assert.ok(!names.includes('plan_mode'), 'the guard keeps plan_mode out of a general subagent');
    assert.ok(names.includes('file_read'), 'the full general tool set is otherwise present');
  });
});

// ===========================================================================
// task.ts — fork mode threads the parent's REAL messages into the child engine
// via subConfig.initialMessages (not a lossy "[Parent conversation context]"
// text summary prepended to the submitted prompt). The submitted prompt stays
// the plain return-directive + task; the history rides in initialMessages.
// ===========================================================================

describe('taskTool — fork mode seeds the child with real parent messages', () => {
  beforeEach(() => resetAgentRegistry());

  it('fork: parent messages are threaded as initialMessages, not prepended to the prompt', async () => {
    let submitted = '';
    let cfg: any = null;
    await taskTool.execute(
      { prompt: 'summarize the module', mode: 'fork' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _parentMessages: [
          { role: 'user', content: 'what does this do?' },
          { role: 'assistant', content: 'it parses the config' },
        ] as any,
        _engineFactory: (c: any) => {
          cfg = c;
          return { submit: (p: string) => { submitted = p; return gen([{ type: 'text_delta', text: 'done' }])(p); } };
        },
      }),
    );
    assert.ok(
      !submitted.includes('[Parent conversation context]'),
      'parent context must NOT be prepended as a text summary anymore',
    );
    // The mode-only subagent still gets the return-and-stop directive + its task.
    assert.ok(submitted.startsWith(SUBAGENT_RETURN_DIRECTIVE), 'return directive still prepended');
    assert.ok(submitted.includes('summarize the module'), 'original task text preserved');
    // The real parent messages ride in initialMessages (both USER and ASSISTANT).
    assert.equal(cfg.initialMessages.length, 2, 'all real parent messages are seeded');
    assert.ok(cfg.initialMessages.some((m: any) => m.role === 'user' && m.content === 'what does this do?'));
    assert.ok(cfg.initialMessages.some((m: any) => m.role === 'assistant' && m.content === 'it parses the config'));
  });

  it('fork: empty parent messages leaves initialMessages unset (no seed)', async () => {
    let cfg: any = null;
    await taskTool.execute(
      { prompt: 'continue', mode: 'fork' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _parentMessages: [] as any,
        _engineFactory: (c: any) => { cfg = c; return { submit: gen([{ type: 'text_delta', text: 'done' }]) }; },
      }),
    );
    assert.equal(cfg.initialMessages, undefined, 'no parent history -> no seed');
  });
});
