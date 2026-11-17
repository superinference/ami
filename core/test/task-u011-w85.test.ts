import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  taskTool,
  resetAgentRegistry,
  filterOrphanedToolResults,
  sanitizeTranscriptForReplay,
  isInterruptedFinalText,
  describeActivity,
  buildRecentActivityTrail,
  MAX_RECENT_ACTIVITIES,
  applyPlanReadOnlyBash,
  EXPLORE_BASH_WRITE_REFUSAL,
} from '../src/tools/task';
import { resetTeam } from '../src/tools/team';
import type { ToolContext, ToolDefinition, EngineConfig, Message, ToolCallContent } from '../src/types';

type Ev = Record<string, unknown>;

interface Harness {
  isError?: boolean;
  output: string;
  cfg: EngineConfig | null;
}

/** Run the task tool against a fake engine, capturing the sub-engine config. */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<Harness> {
  let cfg: EngineConfig | null = null;
  async function* fakeSubmit(_p: string) {
    for (const e of events) yield e as never;
  }
  const hookManager = {
    executeSubagentStart: async () => {},
    executeSubagentStop: async () => {},
    executeTeammateIdle: async () => {},
  };
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _emitChild: () => {},
    _hookManager: hookManager as unknown as ToolContext['_hookManager'],
    _engineFactory: (c) => { cfg = c; return { submit: fakeSubmit, shutdown: () => {} }; },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, cfg };
}

/** Build a minimal assistant tool_call block. */
function tc(id: string): ToolCallContent {
  return { id, type: 'function', function: { name: 'bash', arguments: '{}' } };
}

beforeEach(() => { resetAgentRegistry(); resetTeam(); });
afterEach(() => { resetAgentRegistry(); resetTeam(); });

// ---------------------------------------------------------------------------
// Gap U011/w85: filterOrphanedToolResults + sanitizeTranscriptForReplay
// (claudeRef tools/AgentTool/resumeAgent.ts:L70-74 — filterUnresolvedToolUses +
//  filterOrphanedToolResults sanitize a persisted transcript before replay)
describe('filterOrphanedToolResults — drop tool_results with no matching tool_use', () => {
  it('removes a tool message whose tool_call_id is unreferenced', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'hi' },
      { role: 'tool', tool_call_id: 'ghost', content: 'orphan' },
    ];
    const out = filterOrphanedToolResults(msgs);
    assert.equal(out.length, 1);
    assert.equal(out[0].role, 'user');
  });

  it('keeps a tool result whose tool_use is present', () => {
    const msgs: Message[] = [
      { role: 'assistant', content: null, tool_calls: [tc('A')] },
      { role: 'tool', tool_call_id: 'A', content: 'resA' },
    ];
    assert.deepEqual(filterOrphanedToolResults(msgs), msgs);
  });
});

describe('sanitizeTranscriptForReplay — fully reconciles a half-answered exchange', () => {
  it('drops the answered-call tool_result that filterIncompleteToolCalls orphans', () => {
    // Assistant called A (answered) AND B (unanswered). filterIncompleteToolCalls
    // drops the whole assistant turn, orphaning the tool_result for A — which the
    // full pipeline must then remove so the seed is API-valid in BOTH directions.
    const msgs: Message[] = [
      { role: 'user', content: 'hi' },
      { role: 'assistant', content: null, tool_calls: [tc('A'), tc('B')] },
      { role: 'tool', tool_call_id: 'A', content: 'resA' },
    ];
    const out = sanitizeTranscriptForReplay(msgs);
    assert.ok(!out.some(m => m.role === 'tool'), 'no orphaned tool_result survives');
    assert.deepEqual(out.map(m => m.role), ['user']);
  });
});

describe('task fork spawn — seeds a sanitized parent transcript (no orphaned tool_result)', () => {
  it('a mid-turn fork seed carries no orphaned tool_result', async () => {
    const parent: Message[] = [
      { role: 'user', content: 'hi' },
      { role: 'assistant', content: null, tool_calls: [tc('A'), tc('B')] },
      { role: 'tool', tool_call_id: 'A', content: 'resA' },
    ];
    const h = await runTask({ prompt: 'continue', mode: 'fork' }, {
      _parentMessages: parent,
    } as Partial<ToolContext>);
    assert.equal(h.isError, undefined);
    const seeded = (h.cfg?.initialMessages ?? []) as Message[];
    assert.ok(!seeded.some(m => m.role === 'tool'), 'fork seed has no orphaned tool_result');
  });
});

// ---------------------------------------------------------------------------
// Gap U011/w85: isInterruptedFinalText — a synthetic/interrupted final deliverable
// is a FAILURE (claudeRef tools/AgentTool/AgentTool.tsx:L1205-1218)
describe('isInterruptedFinalText — detect a synthetic/interrupted ending', () => {
  it('matches the server-error and tool-call interrupt sentinels', () => {
    assert.equal(isInterruptedFinalText('work [Response interrupted by server error]'), true);
    assert.equal(isInterruptedFinalText('[Tool call interrupted by user]'), true);
  });
  it('is false for a clean answer and for undefined', () => {
    assert.equal(isInterruptedFinalText('all done, here is the result'), false);
    assert.equal(isInterruptedFinalText(undefined), false);
  });
});

describe('task spawn — an interrupted final turn is returned as a failure', () => {
  it('marks the result isError when the final deliverable is interrupted', async () => {
    const h = await runTask({ prompt: 'do x' }, {}, [
      { type: 'turn_complete', content: 'partial work [Response interrupted by server error]' },
    ]);
    assert.equal(h.isError, true);
  });
  it('a clean final turn is not a failure', async () => {
    const h = await runTask({ prompt: 'do x' }, {}, [
      { type: 'turn_complete', content: 'all finished cleanly' },
    ]);
    assert.equal(h.isError, undefined);
  });
});

// ---------------------------------------------------------------------------
// Gap U011/w85: recent-activity trail (name+input) surfaced on a stalled subagent
// (claudeRef tasks/LocalMainSessionTask.ts:L421-468 — progress recentActivities)
describe('describeActivity — tool name + salient input hint', () => {
  it('appends a compact input hint when a salient field is present', () => {
    assert.equal(describeActivity('bash', { command: 'git status' }), 'bash(git status)');
    assert.equal(describeActivity('file_read', { file_path: '/a/b.ts' }), 'file_read(/a/b.ts)');
  });
  it('falls back to a bare label when no salient input is present', () => {
    assert.equal(describeActivity('todo_write', { todos: [] }), 'tool:todo_write');
    assert.equal(describeActivity('glob', undefined), 'tool:glob');
  });
  it('collapses whitespace and caps the hint length', () => {
    const long = 'x'.repeat(100);
    const out = describeActivity('bash', { command: `  multi\n  line   ${long}` });
    assert.ok(out.startsWith('bash('));
    assert.ok(!out.includes('\n'));
    assert.ok(out.length <= 'bash('.length + 40 + 1);
  });
});

describe('buildRecentActivityTrail — render the recent-activity window', () => {
  it('renders a comma trail of the recent activities', () => {
    assert.equal(
      buildRecentActivityTrail(['bash(ls)', 'file_read(/a)']),
      ' Recent activity: bash(ls), file_read(/a).',
    );
  });
  it('is empty when there is no activity', () => {
    assert.equal(buildRecentActivityTrail([]), '');
  });
  it('exposes a positive window size', () => {
    assert.ok(MAX_RECENT_ACTIVITIES > 0);
  });
});

// ---------------------------------------------------------------------------
// Gap U011/w85: applyPlanReadOnlyBash — structural read-only bash for plan agents
// (claudeRef tools/AgentTool/built-in/planAgent.ts:L47-48)
describe('applyPlanReadOnlyBash — structural read-only bash for a plan agent', () => {
  const bash: ToolDefinition = {
    name: 'bash',
    description: 'run shell',
    inputSchema: { type: 'object', properties: {} },
    execute: async () => ({ output: 'ran' }),
  };
  const read: ToolDefinition = {
    name: 'file_read',
    description: 'read',
    inputSchema: { type: 'object', properties: {} },
    execute: async () => ({ output: 'contents' }),
  };

  it('is a no-op when not plan-required', () => {
    assert.deepEqual(applyPlanReadOnlyBash([bash, read], false), [bash, read]);
  });

  it('wraps bash read-only and leaves other tools untouched', async () => {
    const out = applyPlanReadOnlyBash([bash, read], true);
    const wrappedBash = out.find(t => t.name === 'bash')!;
    assert.equal(wrappedBash.isReadOnly, true);
    assert.equal(out.find(t => t.name === 'file_read'), read);
    // A write command is structurally refused; a read-only one passes through.
    const writeAttempt = await wrappedBash.execute(
      { command: 'rm -rf /' },
      { cwd: process.cwd(), abortSignal: new AbortController().signal } as unknown as ToolContext,
    );
    assert.equal(writeAttempt.isError, true);
    assert.equal(writeAttempt.output, EXPLORE_BASH_WRITE_REFUSAL);
  });

  it('end-to-end: a plan_mode_required spawn runs bash read-only', async () => {
    const h = await runTask({ prompt: 'do x', plan_mode_required: true });
    assert.equal(h.isError, undefined);
    const tools = (h.cfg?.tools ?? []) as ToolDefinition[];
    const bashTool = tools.find(t => t.name === 'bash');
    assert.ok(bashTool, 'plan agent keeps bash');
    assert.equal(bashTool!.isReadOnly, true);
    const writeAttempt = await bashTool!.execute(
      { command: 'touch /tmp/evil' },
      { cwd: process.cwd(), abortSignal: new AbortController().signal } as unknown as ToolContext,
    );
    assert.equal(writeAttempt.isError, true);
  });
});
