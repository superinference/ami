import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { skillTool } from '../src/tools/skill';
import {
  validateEngineConfig,
  sanitizeEngineConfig,
  type ToolContext,
  type EngineEvent,
} from '../src/types';

// ---------------------------------------------------------------------------
// UNIT U006 / gap 8 — forked-skill live-progress cap (parity: Claude Code's
// SkillTool MAX_PROGRESS_MESSAGES_TO_SHOW + verbose flag), adapted to AMI's
// live-stream model: an OPT-IN `maxSkillProgressMessages` knob caps how many
// nested tool-use steps a forked skill surfaces live; the rest are suppressed
// from the live stream and summarized as "+K more tool use". Unset keeps AMI's
// full live streaming (verbose default — the AMI perk is preserved).
// ---------------------------------------------------------------------------

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp/u006-w114',
    abortSignal: new AbortController().signal,
    _engineAddSystemReminder: () => {},
    _engineSetActiveSkill: () => {},
    _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
    ...overrides,
  } as unknown as ToolContext;
}

function manager(skill: any) {
  return {
    getSkill: (n: string) => (n === skill.name ? skill : null),
    getSkillContent: () => 'do the thing',
    listSkills: () => [skill],
    getAgent: () => null,
    listAgents: () => [],
  } as any;
}

/** A forked sub-engine that emits `toolUses` nested tool-use steps (each a
 *  start→progress→result triple with a unique id) followed by a final text_delta. */
function factoryWith(toolUses: number) {
  return () => ({
    submit: async function* (): AsyncIterable<EngineEvent> {
      for (let i = 0; i < toolUses; i++) {
        const toolCallId = `call-${i}`;
        yield { type: 'tool_use_start', toolName: 'bash', toolCallId, input: {} };
        yield { type: 'tool_use_progress', toolCallId, data: `step ${i}` };
        yield {
          type: 'tool_use_result',
          toolCallId,
          toolName: 'bash',
          output: `out ${i}`,
          isError: false,
        };
      }
      yield { type: 'text_delta', text: 'done' };
    },
  });
}

const forkSkill = { name: 'sub', context: 'fork' as const };

async function runFork(cap: number | undefined, toolUses: number) {
  const emitted: EngineEvent[] = [];
  const r = await skillTool.execute(
    { skill: 'sub' },
    ctx({
      _skillManager: manager(forkSkill),
      _engineFactory: factoryWith(toolUses) as any,
      _emitChild: (e: EngineEvent) => emitted.push(e),
      ...(cap === undefined ? {} : { _maxSkillProgressMessages: cap }),
    }),
  );
  return { r, emitted };
}

describe('skillTool forked progress cap — maxSkillProgressMessages', () => {
  it('caps surfaced tool-use steps and summarizes the rest with "+K more tool uses"', async () => {
    const { r, emitted } = await runFork(2, 4);
    assert.equal(r.isError, false);
    // Only 2 tool_use_start steps surfaced; the matching progress/result rows for the
    // 2 hidden steps are suppressed too. 2 shown steps × 3 rows + final text_delta = 7.
    const starts = emitted.filter(e => e.type === 'tool_use_start');
    assert.equal(starts.length, 2);
    assert.equal(emitted.filter(e => e.type === 'tool_use_result').length, 2);
    assert.equal(emitted.filter(e => e.type === 'tool_use_progress').length, 2);
    assert.equal(emitted.filter(e => e.type === 'text_delta').length, 1);
    // No suppressed step's paired rows leaked through (ids call-2 / call-3).
    assert.ok(!emitted.some(e => 'toolCallId' in e && (e.toolCallId === 'call-2' || e.toolCallId === 'call-3')));
    assert.match(r.output, /\+2 more tool uses/);
    assert.equal((r.metadata as any)?.progressHidden, 2);
  });

  it('uses the singular "tool use" when exactly one step is hidden', async () => {
    const { r } = await runFork(2, 3);
    assert.match(r.output, /\+1 more tool use(?!s)/);
    assert.equal((r.metadata as any)?.progressHidden, 1);
  });

  it('cap of 0 surfaces the result only (every tool use summarized)', async () => {
    const { r, emitted } = await runFork(0, 3);
    assert.equal(emitted.filter(e => e.type === 'tool_use_start').length, 0);
    assert.equal(emitted.filter(e => e.type === 'tool_use_result').length, 0);
    // text_delta (model narration) is still surfaced — only tool-use rows are capped.
    assert.equal(emitted.filter(e => e.type === 'text_delta').length, 1);
    assert.match(r.output, /\+3 more tool uses/);
  });

  it('VERBOSE default (knob unset): every nested event is surfaced, no summary', async () => {
    const { r, emitted } = await runFork(undefined, 4);
    assert.equal(emitted.filter(e => e.type === 'tool_use_start').length, 4);
    assert.equal(emitted.filter(e => e.type === 'tool_use_result').length, 4);
    assert.doesNotMatch(r.output, /more tool use/);
    assert.equal((r.metadata as any)?.progressHidden, undefined);
  });

  it('a negative / non-finite cap is treated as verbose (full live streaming)', async () => {
    for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
      const { r, emitted } = await runFork(bad, 3);
      assert.equal(emitted.filter(e => e.type === 'tool_use_start').length, 3);
      assert.doesNotMatch(r.output, /more tool use/);
    }
  });

  it('does not summarize when the step count is within the cap', async () => {
    const { r, emitted } = await runFork(5, 3);
    assert.equal(emitted.filter(e => e.type === 'tool_use_start').length, 3);
    assert.doesNotMatch(r.output, /more tool use/);
    assert.equal((r.metadata as any)?.progressHidden, undefined);
  });
});

describe('maxSkillProgressMessages — engine config validation + sanitization', () => {
  it('accepts a non-negative integer', () => {
    assert.equal(validateEngineConfig({ maxSkillProgressMessages: 3 }).valid, true);
    assert.equal(validateEngineConfig({ maxSkillProgressMessages: 0 }).valid, true);
  });

  it('rejects a negative or fractional value', () => {
    assert.equal(validateEngineConfig({ maxSkillProgressMessages: -1 }).valid, false);
    assert.equal(validateEngineConfig({ maxSkillProgressMessages: 2.5 }).valid, false);
  });

  it('sanitizes to a non-negative rounded integer', () => {
    assert.equal(sanitizeEngineConfig({ maxSkillProgressMessages: 3.6 }).maxSkillProgressMessages, 4);
    assert.equal(sanitizeEngineConfig({ maxSkillProgressMessages: -4 }).maxSkillProgressMessages, 0);
  });
});
