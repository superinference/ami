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
// task.ts — fork mode: parent messages present but NONE qualify for the
// context summary (filtered out), so `contextSummary` is empty and the
// `if (contextSummary)` false branch is taken: no "[Parent conversation
// context]" section is prepended even though _parentMessages is non-empty.
// ===========================================================================

describe('taskTool — fork mode with no summarizable parent messages', () => {
  beforeEach(() => resetAgentRegistry());

  it('fork: non-empty _parentMessages with no assistant-string content adds NO context section', async () => {
    let submitted = '';
    await taskTool.execute(
      { prompt: 'summarize the module', mode: 'fork' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _parentMessages: [
          { role: 'user', content: 'what does this do?' },
          // assistant present but content is not a string -> filtered out
          { role: 'assistant', content: 42 },
        ] as any,
        _engineFactory: (_c: any) => ({
          submit: (p: string) => { submitted = p; return gen([{ type: 'text_delta', text: 'done' }])(p); },
        }),
      }),
    );
    assert.ok(
      !submitted.includes('[Parent conversation context]'),
      'empty summary must not prepend a parent-context section',
    );
    // The mode-only subagent still gets the return-and-stop directive + its task.
    assert.ok(submitted.startsWith(SUBAGENT_RETURN_DIRECTIVE), 'return directive still prepended');
    assert.ok(submitted.includes('summarize the module'), 'original task text preserved');
  });

  it('fork: an assistant string message DOES produce a parent-context section (contrast)', async () => {
    let submitted = '';
    await taskTool.execute(
      { prompt: 'continue', mode: 'fork' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _parentMessages: [
          { role: 'assistant', content: 'earlier assistant insight worth forwarding' },
        ] as any,
        _engineFactory: (_c: any) => ({
          submit: (p: string) => { submitted = p; return gen([{ type: 'text_delta', text: 'done' }])(p); },
        }),
      }),
    );
    assert.ok(submitted.includes('[Parent conversation context]'), 'string assistant content is summarized');
    assert.ok(submitted.includes('earlier assistant insight'), 'the assistant content is forwarded');
  });
});
