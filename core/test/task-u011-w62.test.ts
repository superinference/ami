import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  taskTool,
  resetAgentRegistry,
  stripIncompleteTrailingMessages,
  filterIncompleteToolCalls,
  isIncompleteAssistantMessage,
  selectFinalAnswer,
} from '../src/tools/task';
import type { ToolContext, Message } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

function gen(events: unknown[]) {
  return async function* () {
    for (const e of events) yield e as never;
  };
}

// ---------------------------------------------------------------------------
// Gap: whenToUse affordance hint for low-confidence searches
// Gap: Agent-type selection guidance (read-only vs full-capability)
// (generalPurposeAgent.ts:L27-28 / TeamCreateTool/prompt.ts:L14-22)
// ---------------------------------------------------------------------------

describe('task tool description — whenToUse + agent-type guidance', () => {
  it('names the low-confidence-search delegation trigger', () => {
    const d = taskTool.description.toLowerCase();
    assert.ok(
      d.includes('not confident') || d.includes('not sure'),
      'description must cue delegating uncertain/low-confidence searches',
    );
  });

  it('warns that read-only (explore) agents must not get implementation work', () => {
    const d = taskTool.description.toLowerCase();
    assert.ok(
      d.includes('read-only') && d.includes('implementation'),
      'description must guide picking read-only vs full-capability agents',
    );
  });
});

// ---------------------------------------------------------------------------
// Gap: Subagent-model env var as highest-priority forced override
// (utils/model/agent.ts:L43-45)
// ---------------------------------------------------------------------------

describe('taskTool — SUPERINFERENCE_SUBAGENT_MODEL is a forced override', () => {
  const ORIG = process.env.SUPERINFERENCE_SUBAGENT_MODEL;
  beforeEach(() => resetAgentRegistry());
  afterEach(() => {
    if (ORIG === undefined) delete process.env.SUPERINFERENCE_SUBAGENT_MODEL;
    else process.env.SUPERINFERENCE_SUBAGENT_MODEL = ORIG;
  });

  it('overrides an explicit per-call model arg', async () => {
    process.env.SUPERINFERENCE_SUBAGENT_MODEL = 'forced-model';
    let cfg: { provider?: { model?: string } } | null = null;
    await taskTool.execute(
      { prompt: 'go', mode: 'general', model: 'opus' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'parent-model' },
        _engineFactory: (c: unknown) => { cfg = c as { provider?: { model?: string } }; return { submit: gen([{ type: 'text_delta', text: 'ok' }]) }; },
      }),
    );
    assert.equal(cfg!.provider!.model, 'forced-model');
  });

  it('overrides an agent definition model', async () => {
    process.env.SUPERINFERENCE_SUBAGENT_MODEL = 'forced-model';
    let cfg: { provider?: { model?: string } } | null = null;
    const skillManager = {
      getAgent: () => ({ name: 'ag', systemPrompt: 'be an agent', model: 'agent-pinned-model' }),
      listAgents: () => [{ name: 'ag' }],
    };
    await taskTool.execute(
      { prompt: 'go', subagent_type: 'ag' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'parent-model' },
        _skillManager: skillManager as never,
        _engineFactory: (c: unknown) => { cfg = c as { provider?: { model?: string } }; return { submit: gen([{ type: 'text_delta', text: 'ok' }]) }; },
      }),
    );
    assert.equal(cfg!.provider!.model, 'forced-model');
  });
});

// ---------------------------------------------------------------------------
// Gap: Strip in-progress (stop_reason===null) trailing assistant message
// before forking conversation context (utils/queryContext.ts:L134-141)
// ---------------------------------------------------------------------------

describe('stripIncompleteTrailingMessages', () => {
  it('drops a trailing interrupted assistant message', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'do it' },
      { role: 'assistant', content: 'partial work\n\n[Response interrupted by server error]' },
    ];
    const out = stripIncompleteTrailingMessages(msgs);
    assert.equal(out.length, 1);
    assert.equal(out[0].role, 'user');
  });

  it('keeps a complete trailing assistant message', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'do it' },
      { role: 'assistant', content: 'done cleanly' },
    ];
    const out = stripIncompleteTrailingMessages(msgs);
    assert.equal(out.length, 2);
  });

  it('is a no-op on an empty list', () => {
    assert.deepEqual(stripIncompleteTrailingMessages([]), []);
  });

  it('isIncompleteAssistantMessage flags the tool-interrupt sentinel too', () => {
    assert.ok(isIncompleteAssistantMessage({ role: 'assistant', content: 'x [Tool call interrupted by user]' }));
    assert.ok(!isIncompleteAssistantMessage({ role: 'user', content: '[Response interrupted by server error]' }));
    assert.ok(!isIncompleteAssistantMessage({ role: 'assistant', content: 'clean' }));
  });

  it('seeds the forked child with real parent messages, minus the interrupted trailing turn', async () => {
    resetAgentRegistry();
    let seeded: Message[] | undefined;
    const parentMessages: Message[] = [
      { role: 'assistant', content: 'earlier solid reasoning here' },
      { role: 'user', content: 'keep going' },
      { role: 'assistant', content: 'half-written thought\n\n[Response interrupted by server error]' },
    ];
    await taskTool.execute(
      { prompt: 'continue the work', mode: 'fork' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _parentMessages: parentMessages,
        _engineFactory: (c: unknown) => {
          seeded = (c as { initialMessages?: Message[] }).initialMessages;
          return { submit: async function* () { yield { type: 'text_delta', text: 'ok' } as never; } };
        },
      }),
    );
    assert.ok(seeded, 'fork must seed the child engine with real parent messages');
    assert.equal(seeded!.length, 2, 'interrupted trailing turn must be stripped from the seed');
    assert.ok(
      seeded!.some(m => m.role === 'assistant' && m.content === 'earlier solid reasoning here'),
      'the complete assistant turn is threaded as a real message, not a text summary',
    );
    assert.ok(
      seeded!.some(m => m.role === 'user' && m.content === 'keep going'),
      'parent USER turns are preserved (not dropped like the old assistant-string-only summary)',
    );
    assert.ok(
      !seeded!.some(m => typeof m.content === 'string' && m.content.includes('half-written thought')),
      'interrupted trailing turn must be stripped',
    );
  });
});

// ---------------------------------------------------------------------------
// Gap: Thread the parent's REAL conversation into a fork child (not a lossy
// assistant-string-only text summary), dropping assistant turns whose tool_use
// has no matching tool_result so the seeded transcript is API-valid, and apply
// it in the BACKGROUND path too (runAgent.ts:L862-904 / forkSubagent.ts:L91-169)
// ---------------------------------------------------------------------------

describe('filterIncompleteToolCalls', () => {
  it('drops an assistant turn whose tool_call has no matching tool result', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'go' },
      { role: 'assistant', content: 'running', tool_calls: [{ id: 'tc1', type: 'function', function: { name: 'bash', arguments: '{}' } }] },
      // no tool message for tc1 -> the assistant turn is incomplete
    ];
    const out = filterIncompleteToolCalls(msgs);
    assert.equal(out.length, 1);
    assert.equal(out[0].role, 'user');
  });

  it('keeps an assistant turn whose tool_call IS answered', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'go' },
      { role: 'assistant', content: 'running', tool_calls: [{ id: 'tc1', type: 'function', function: { name: 'bash', arguments: '{}' } }] },
      { role: 'tool', tool_call_id: 'tc1', content: 'done' },
    ];
    const out = filterIncompleteToolCalls(msgs);
    assert.equal(out.length, 3);
  });

  it('drops a turn with ANY unanswered call even if others are answered', () => {
    const msgs: Message[] = [
      { role: 'assistant', content: 'two calls', tool_calls: [
        { id: 'a', type: 'function', function: { name: 'bash', arguments: '{}' } },
        { id: 'b', type: 'function', function: { name: 'bash', arguments: '{}' } },
      ] },
      { role: 'tool', tool_call_id: 'a', content: 'ok' },
      // 'b' unanswered
    ];
    const out = filterIncompleteToolCalls(msgs);
    assert.ok(!out.some(m => m.role === 'assistant'));
  });

  it('keeps assistant turns that carry no tool_calls and all non-assistant messages', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'hi' },
      { role: 'assistant', content: 'plain answer' },
      { role: 'system', content: 'note' },
    ];
    assert.deepEqual(filterIncompleteToolCalls(msgs), msgs);
  });
});

describe('fork mode — background path inherits parent context too', () => {
  beforeEach(() => resetAgentRegistry());

  it('a background fork seeds initialMessages (previously it inherited ZERO context)', async () => {
    let seeded: Message[] | undefined;
    const parentMessages: Message[] = [
      { role: 'user', content: 'analyze the repo' },
      { role: 'assistant', content: 'found three modules' },
    ];
    const r = await taskTool.execute(
      { prompt: 'keep analyzing', mode: 'fork', run_in_background: true },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _parentMessages: parentMessages,
        _engineFactory: (c: unknown) => {
          seeded = (c as { initialMessages?: Message[] }).initialMessages;
          return { submit: async function* () { yield { type: 'text_delta', text: 'bg' } as never; }, shutdown() {} };
        },
      }),
    );
    assert.ok(typeof r.output === 'string' && r.output.includes('Background agent started'));
    assert.ok(seeded, 'background fork must seed the child with the parent conversation');
    assert.equal(seeded!.length, 2);
    assert.ok(seeded!.some(m => m.role === 'assistant' && m.content === 'found three modules'));
  });
});

// ---------------------------------------------------------------------------
// Gap: Result = sub-agent's final assistant message text, with pure-tool_use
// fallback (agentToolUtils.ts:L297-317)
// ---------------------------------------------------------------------------

describe('selectFinalAnswer', () => {
  it('prefers the final assistant turn text over accumulated narration', () => {
    assert.equal(selectFinalAnswer('the answer', 'narration the answer'), 'the answer');
  });
  it('falls back to accumulated text when no turn carried text', () => {
    assert.equal(selectFinalAnswer(undefined, 'accumulated'), 'accumulated');
  });
});

describe('taskTool — returns the final assistant turn, not all narration', () => {
  beforeEach(() => resetAgentRegistry());

  it('returns only the last turn_complete text content', async () => {
    const events = [
      { type: 'text_delta', text: 'let me look... ' },
      { type: 'turn_complete', content: 'let me look...', toolCalls: [{}] },
      { type: 'text_delta', text: 'THE FINAL ANSWER' },
      { type: 'turn_complete', content: 'THE FINAL ANSWER', toolCalls: [] },
      { type: 'done', totalTurns: 2, exitReason: 'completed' },
    ];
    const result = await taskTool.execute(
      { prompt: 'solve it', mode: 'general' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({ submit: gen(events) }),
      }),
    );
    assert.ok(result.output.includes('THE FINAL ANSWER'));
    assert.ok(!result.output.includes('let me look'), `intermediate narration must be excluded, got: ${result.output}`);
  });

  it('walks back to the prior text turn when the final turn is pure tool_use', async () => {
    const events = [
      { type: 'turn_complete', content: 'earlier real answer', toolCalls: [] },
      { type: 'turn_complete', content: null, toolCalls: [{}] },
      { type: 'done', totalTurns: 2, exitReason: 'completed' },
    ];
    const result = await taskTool.execute(
      { prompt: 'solve it', mode: 'general' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({ submit: gen(events) }),
      }),
    );
    assert.ok(result.output.includes('earlier real answer'), `should fall back to prior text turn, got: ${result.output}`);
  });
});
