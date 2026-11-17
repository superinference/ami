import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

import {
  taskTool,
  resetAgentRegistry,
  selectExactForkToolPool,
} from '../src/tools/task';
import { createDefaultTools } from '../src/tools/index';
import type { ToolContext, EngineConfig, Message } from '../src/types';

// ===========================================================================
// Unit U011 / wave w95 — CACHE-IDENTICAL FORK PREFIX.
//
// Parity with Claude Code's forkSubagent (forkSubagent.ts:L44-L71) +
// resumeAgent (resumeAgent.ts:L116-L148,L183-L190): a `fork`-mode child must
// reuse the PARENT's already-rendered system prompt bytes and the parent's
// EXACT ordered tool pool so the API request prefix stays byte-identical to the
// parent's cached prefix (otherwise re-deriving the stance / rebuilding a
// general pool diverges and busts the provider prompt cache). On a fork RESUME
// the parent fork context must NOT be re-seeded (the child's reloaded transcript
// already holds it) to avoid duplicate tool_use IDs.
//
// Threaded into the child via ToolContext._parentSystemPrompt / _parentToolNames
// (populated by common/src/engine.ts).
// ===========================================================================

function gen(events: Array<Record<string, unknown>>) {
  return async function* (_p: string) {
    for (const e of events) yield e as never;
  };
}

/** Run the task tool capturing the submitted prompt AND the full sub-engine config. */
async function run(
  input: Record<string, unknown>,
  overrides: Partial<ToolContext> = {},
): Promise<{ isError?: boolean; output: string; submitted: string; cfg: EngineConfig }> {
  let submitted = '';
  let cfg = {} as EngineConfig;
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c: EngineConfig) => {
      cfg = c;
      return { submit: (p: string) => { submitted = p; return gen([{ type: 'text_delta', text: 'done' }])(p); } };
    },
    ...overrides,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, submitted, cfg };
}

const PARENT_MSGS: Message[] = [
  { role: 'user', content: 'original task' } as Message,
  { role: 'assistant', content: 'working on it' } as Message,
];

describe('selectExactForkToolPool — ordered exact parent tool pool', () => {
  const all = createDefaultTools(process.cwd()).getAll();

  it('keeps the parent tools in the parent order (byte-identical pool)', () => {
    const pool = selectExactForkToolPool(all, ['grep', 'file_read', 'bash']);
    assert.deepEqual(pool.map(t => t.name), ['grep', 'file_read', 'bash']);
  });

  it('skips a parent name the child registry does not have', () => {
    const pool = selectExactForkToolPool(all, ['bash', 'no_such_tool_xyz', 'grep']);
    assert.deepEqual(pool.map(t => t.name), ['bash', 'grep']);
  });

  it('de-dupes a repeated parent name', () => {
    const pool = selectExactForkToolPool(all, ['bash', 'bash', 'grep']);
    assert.deepEqual(pool.map(t => t.name), ['bash', 'grep']);
  });

  it('drops the nested-spawn task tool when the child is at/over the depth cap', () => {
    const atCap = selectExactForkToolPool(all, ['bash', 'task'], 5, 5);
    assert.deepEqual(atCap.map(t => t.name), ['bash'], 'task dropped at depth cap');
    const belowCap = selectExactForkToolPool(all, ['bash', 'task'], 0, 5);
    assert.ok(belowCap.some(t => t.name === 'task'), 'task kept below the depth cap');
  });
});

describe('taskTool fork — inherits the parent exact tool pool', () => {
  beforeEach(() => resetAgentRegistry());

  it('a fork child gets the parent exact ordered pool, not a rebuilt general pool', async () => {
    const { cfg, isError, output } = await run(
      { prompt: 'continue', mode: 'fork' },
      { _parentToolNames: ['grep', 'file_read', 'bash'], _parentMessages: PARENT_MSGS },
    );
    assert.ok(!isError, output);
    assert.deepEqual(cfg.tools.map(t => t.name), ['grep', 'file_read', 'bash']);
  });

  it('REGRESSION discriminator: without a threaded parent pool, fork rebuilds the full general pool', async () => {
    const { cfg } = await run({ prompt: 'continue', mode: 'fork' }, { _parentMessages: PARENT_MSGS });
    const names = cfg.tools.map(t => t.name);
    // The rebuilt general pool is far wider than a 3-tool parent pool and is NOT
    // limited to the parent's three tools in that order.
    assert.ok(names.length > 3, 'general rebuild is wider than the 3-tool parent pool');
    assert.ok(names.includes('file_edit'), 'general rebuild includes edit tools the parent pool omitted');
  });
});

describe('taskTool fork — threads the parent rendered system prompt verbatim', () => {
  beforeEach(() => resetAgentRegistry());

  it('a fork child reuses the parent already-rendered system prompt byte-for-byte', async () => {
    const PARENT_PROMPT = 'RENDERED-PARENT-STANCE-PROMPT-abc123';
    const { cfg, submitted } = await run(
      { prompt: 'keep going', mode: 'fork' },
      { _parentSystemPrompt: PARENT_PROMPT, _parentMessages: PARENT_MSGS },
    );
    assert.equal(cfg.systemPrompt, PARENT_PROMPT, 'subConfig.systemPrompt is the parent rendered prompt');
    // The user-message fork return/worker directive path is UNCHANGED (parity).
    assert.ok(submitted.includes('keep going'), 'task text preserved in the user message');
  });

  it('REGRESSION discriminator: without a threaded parent prompt, fork leaves systemPrompt undefined (child re-derives)', async () => {
    const { cfg } = await run({ prompt: 'keep going', mode: 'fork' }, { _parentMessages: PARENT_MSGS });
    assert.equal(cfg.systemPrompt, undefined, 'fork re-derives its stance when no parent prompt threaded');
  });

  it('a non-fork (general) spawn ignores the threaded parent prompt + pool', async () => {
    const PARENT_PROMPT = 'RENDERED-PARENT-STANCE-PROMPT-xyz';
    const { cfg } = await run(
      { prompt: 'do a thing', mode: 'general' },
      { _parentSystemPrompt: PARENT_PROMPT, _parentToolNames: ['grep'] },
    );
    assert.notEqual(cfg.systemPrompt, PARENT_PROMPT, 'general spawn uses its own lean prompt, not the parent stance');
    assert.ok(cfg.tools.length > 1, 'general spawn uses its own pool, not the 1-tool parent pool');
  });
});

describe('taskTool fork resume — no re-seeding of the parent fork context', () => {
  beforeEach(() => resetAgentRegistry());

  it('seeds parent fork context on the INITIAL spawn but NOT on resume (avoids duplicate tool_use)', async () => {
    const spawn = await run(
      { prompt: 'start', name: 'fk', mode: 'fork' },
      { _parentMessages: PARENT_MSGS, _parentSystemPrompt: 'P', _parentToolNames: ['bash', 'grep'] },
    );
    assert.ok(!spawn.isError, spawn.output);
    assert.ok(
      Array.isArray(spawn.cfg.initialMessages) && spawn.cfg.initialMessages.length > 0,
      'initial fork spawn seeds the parent transcript as initialMessages',
    );

    const resumed = await run(
      { prompt: 'keep going', resume: 'fk', mode: 'fork' },
      { _parentMessages: PARENT_MSGS, _parentSystemPrompt: 'P', _parentToolNames: ['bash', 'grep'] },
    );
    assert.ok(!resumed.isError, resumed.output);
    assert.equal(
      resumed.cfg.initialMessages,
      undefined,
      'fork resume does NOT re-seed the parent transcript (the reloaded session already holds it)',
    );
    // The resumed fork STILL inherits the cache-identical pool + prompt.
    assert.deepEqual(resumed.cfg.tools.map(t => t.name), ['bash', 'grep'], 'resumed fork keeps the exact parent pool');
    assert.equal(resumed.cfg.systemPrompt, 'P', 'resumed fork keeps the parent rendered prompt');
  });
});
