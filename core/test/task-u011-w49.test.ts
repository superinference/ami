import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  taskTool,
  resetAgentRegistry,
  isBackgroundTasksDisabled,
  DISABLE_BACKGROUND_TASKS_ENV,
  SYSTEM_PROMPT_MODES,
  EXPLORE_THOROUGHNESS_LEVELS,
  TEAMMATE_PROMPT_ADDENDUM,
  buildThoroughnessDirective,
  SUBAGENT_SYSTEM_PROMPT,
  EXPLORE_SUBAGENT_SYSTEM_PROMPT,
  SUBAGENT_RETURN_DIRECTIVE,
} from '../src/tools/task';
import { restoreTeam, resetTeam } from '../src/tools/team';
import type { ToolContext, ToolDefinition } from '../src/types';

type Ev = Record<string, unknown>;

/** Run the task tool with a fake engine, capturing config + tool names + the
 *  exact prompt submitted to the sub-engine. */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<{
  isError?: boolean;
  output: string;
  cfg: { systemPrompt?: string; maxTurns?: number } | null;
  submittedPrompt: string | undefined;
  toolNames: string[];
}> {
  let cfg: { systemPrompt?: string; maxTurns?: number } | null = null;
  let submittedPrompt: string | undefined;
  let toolNames: string[] = [];
  async function* fakeSubmit(p: string) {
    submittedPrompt = p;
    for (const e of events) yield e as never;
  }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c) => {
      cfg = c as typeof cfg;
      toolNames = (c.tools as ToolDefinition[]).map(t => t.name);
      return { submit: fakeSubmit };
    },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, cfg, submittedPrompt, toolNames };
}

// ---------------------------------------------------------------------------
// gaps 110/135: CLAUDE_CODE_DISABLE_BACKGROUND_TASKS
// ---------------------------------------------------------------------------
describe('CLAUDE_CODE_DISABLE_BACKGROUND_TASKS knob', () => {
  const prior = process.env[DISABLE_BACKGROUND_TASKS_ENV];
  beforeEach(() => { resetAgentRegistry(); delete process.env[DISABLE_BACKGROUND_TASKS_ENV]; });
  afterEach(() => {
    if (prior === undefined) delete process.env[DISABLE_BACKGROUND_TASKS_ENV];
    else process.env[DISABLE_BACKGROUND_TASKS_ENV] = prior;
  });

  it('advertises run_in_background in the schema by default', () => {
    const props = taskTool.inputSchema.properties as Record<string, unknown>;
    assert.ok('run_in_background' in props, 'present when knob unset');
    assert.equal(isBackgroundTasksDisabled(), false);
  });

  it('omits run_in_background from the schema when the knob is truthy', () => {
    process.env[DISABLE_BACKGROUND_TASKS_ENV] = '1';
    assert.equal(isBackgroundTasksDisabled(), true);
    const props = taskTool.inputSchema.properties as Record<string, unknown>;
    assert.ok(!('run_in_background' in props), 'omitted when disabled');
    // Other fields remain.
    assert.ok('prompt' in props && 'mode' in props);
  });

  it('treats 0/false/off/empty as NOT disabled', () => {
    for (const v of ['0', 'false', 'off', '']) {
      process.env[DISABLE_BACKGROUND_TASKS_ENV] = v;
      assert.equal(isBackgroundTasksDisabled(), false, `"${v}" is falsy`);
      assert.ok('run_in_background' in (taskTool.inputSchema.properties as Record<string, unknown>));
    }
  });

  it('rejects a run_in_background spawn at runtime when disabled', async () => {
    process.env[DISABLE_BACKGROUND_TASKS_ENV] = 'true';
    const { isError, output, cfg } = await runTask({ prompt: 'go', run_in_background: true });
    assert.ok(isError, 'spawn rejected');
    assert.match(output, /background subagents are disabled/);
    assert.match(output, new RegExp(DISABLE_BACKGROUND_TASKS_ENV));
    assert.equal(cfg, null, 'no engine created');
  });

  it('still allows a foreground spawn when disabled', async () => {
    process.env[DISABLE_BACKGROUND_TASKS_ENV] = 'true';
    const { isError, cfg } = await runTask({ prompt: 'go', mode: 'general' });
    assert.ok(!isError);
    assert.ok(cfg, 'foreground engine created');
  });
});

// ---------------------------------------------------------------------------
// gaps 72/71/79: per-spawn system_prompt + system_prompt_mode
// ---------------------------------------------------------------------------
describe('per-spawn system_prompt + system_prompt_mode', () => {
  beforeEach(() => resetAgentRegistry());

  it('schema advertises system_prompt and system_prompt_mode', () => {
    const props = taskTool.inputSchema.properties as Record<string, { type?: string; enum?: string[] }>;
    assert.equal(props.system_prompt.type, 'string');
    assert.deepEqual(props.system_prompt_mode.enum, [...SYSTEM_PROMPT_MODES]);
  });

  it('replace (default) uses the custom prompt verbatim as the subagent system prompt', async () => {
    const { cfg, submittedPrompt } = await runTask({ prompt: 'task body', mode: 'general', system_prompt: 'You are Zed.' });
    assert.equal(cfg!.systemPrompt, 'You are Zed.');
    // Having an authoritative system prompt drops the user-message return directive.
    assert.ok(!submittedPrompt!.includes(SUBAGENT_RETURN_DIRECTIVE));
    assert.equal(submittedPrompt, 'task body');
  });

  it('explicit replace mode behaves like default', async () => {
    const { cfg } = await runTask({ prompt: 'b', mode: 'general', system_prompt: 'Persona', system_prompt_mode: 'replace' });
    assert.equal(cfg!.systemPrompt, 'Persona');
  });

  it('append layers the custom prompt on the mode-only base stance', async () => {
    const { cfg } = await runTask({ prompt: 'b', mode: 'general', system_prompt: 'Extra rule.', system_prompt_mode: 'append' });
    assert.equal(cfg!.systemPrompt, `${SUBAGENT_SYSTEM_PROMPT}\n\nExtra rule.`);
  });

  it('append layers on the explore base stance', async () => {
    const { cfg } = await runTask({ prompt: 'b', mode: 'explore', system_prompt: 'Extra.', system_prompt_mode: 'append' });
    assert.equal(cfg!.systemPrompt, `${EXPLORE_SUBAGENT_SYSTEM_PROMPT}\n\nExtra.`);
  });

  it('append layers on a named-agent base system prompt', async () => {
    const skillManager = {
      getAgent: () => ({ name: 'builder', systemPrompt: 'Agent base.', tools: ['file_read'] }),
      listAgents: () => [{ name: 'builder' }],
    };
    const { cfg } = await runTask(
      { prompt: 'b', subagent_type: 'builder', system_prompt: 'More.', system_prompt_mode: 'append' },
      { _skillManager: skillManager as unknown as ToolContext['_skillManager'] },
    );
    assert.equal(cfg!.systemPrompt, 'Agent base.\n\nMore.');
  });

  it('append is rejected for fork (no string base to append to)', async () => {
    const { isError, output, cfg } = await runTask({ prompt: 'b', mode: 'fork', system_prompt: 'X', system_prompt_mode: 'append' });
    assert.ok(isError);
    assert.match(output, /no base system prompt to append to/);
    assert.equal(cfg, null);
  });

  it('a custom replace system_prompt works even for fork', async () => {
    const { isError, cfg } = await runTask({ prompt: 'b', mode: 'fork', system_prompt: 'Forked persona', system_prompt_mode: 'replace' });
    assert.ok(!isError);
    assert.equal(cfg!.systemPrompt, 'Forked persona');
  });

  it('a blank/whitespace system_prompt is ignored (base stance preserved)', async () => {
    const { cfg } = await runTask({ prompt: 'b', mode: 'general', system_prompt: '   ' });
    assert.equal(cfg!.systemPrompt, SUBAGENT_SYSTEM_PROMPT);
  });

  it('rejects an invalid system_prompt_mode', async () => {
    const { isError, output, cfg } = await runTask({ prompt: 'b', mode: 'general', system_prompt: 'x', system_prompt_mode: 'bogus' });
    assert.ok(isError);
    assert.match(output, /invalid system_prompt_mode "bogus"/);
    assert.equal(cfg, null);
  });
});

// ---------------------------------------------------------------------------
// gap 125: explore thoroughness dial
// ---------------------------------------------------------------------------
describe('explore thoroughness dial', () => {
  beforeEach(() => resetAgentRegistry());

  it('schema advertises thoroughness with the three levels', () => {
    const props = taskTool.inputSchema.properties as Record<string, { enum?: string[] }>;
    assert.deepEqual(props.thoroughness.enum, [...EXPLORE_THOROUGHNESS_LEVELS]);
  });

  it('buildThoroughnessDirective scales the wording per level', () => {
    assert.match(buildThoroughnessDirective('quick'), /single-pass/);
    assert.match(buildThoroughnessDirective('very thorough'), /comprehensive sweep/);
    assert.match(buildThoroughnessDirective('medium'), /moderate exploration/);
    // Unknown value falls back to the medium directive (never throws).
    assert.match(buildThoroughnessDirective('whatever'), /moderate exploration/);
  });

  it('injects the thoroughness directive into an explore spawn prompt', async () => {
    const { submittedPrompt } = await runTask({ prompt: 'find X', mode: 'explore', thoroughness: 'very thorough' });
    assert.match(submittedPrompt!, /Thoroughness: very thorough/);
    assert.match(submittedPrompt!, /comprehensive sweep/);
    assert.match(submittedPrompt!, /find X/);
  });

  it('does NOT inject thoroughness for a non-explore spawn', async () => {
    const { submittedPrompt } = await runTask({ prompt: 'do', mode: 'general', thoroughness: 'quick' });
    assert.ok(!/Thoroughness:/.test(submittedPrompt!));
  });

  it('rejects an invalid thoroughness value', async () => {
    const { isError, output, cfg } = await runTask({ prompt: 'x', mode: 'explore', thoroughness: 'exhaustive' });
    assert.ok(isError);
    assert.match(output, /invalid thoroughness "exhaustive"/);
    assert.equal(cfg, null);
  });
});

// ---------------------------------------------------------------------------
// gap 87: teammate prompt addendum
// ---------------------------------------------------------------------------
describe('teammate prompt addendum', () => {
  let teamDir: string;
  beforeEach(() => {
    resetAgentRegistry();
    teamDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u011-team-'));
  });
  afterEach(() => {
    resetTeam();
    try { fs.rmSync(teamDir, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('exports an addendum carrying the four load-bearing hints', () => {
    assert.match(TEAMMATE_PROMPT_ADDENDUM, /running as an agent in a team/);
    assert.match(TEAMMATE_PROMPT_ADDENDUM, /send_message/);
    assert.match(TEAMMATE_PROMPT_ADDENDUM, /NOT visible to others on your team/);
    assert.match(TEAMMATE_PROMPT_ADDENDUM, /team lead/);
  });

  it('prepends the addendum when a named agent is spawned into an active team', async () => {
    restoreTeam({ name: 'alpha', leadAgentId: 'lead-1', members: [], createdAt: new Date().toISOString() }, teamDir);
    const { submittedPrompt } = await runTask({ prompt: 'work', mode: 'general', name: 'scout' });
    assert.match(submittedPrompt!, /running as an agent in a team/);
    assert.match(submittedPrompt!, /work/);
  });

  it('does NOT add the addendum when there is no active team', async () => {
    resetTeam();
    const { submittedPrompt } = await runTask({ prompt: 'work', mode: 'general', name: 'scout' });
    assert.ok(!/running as an agent in a team/.test(submittedPrompt!));
  });

  it('does NOT add the addendum for an unnamed agent even inside a team', async () => {
    restoreTeam({ name: 'alpha', leadAgentId: 'lead-1', members: [], createdAt: new Date().toISOString() }, teamDir);
    const { submittedPrompt } = await runTask({ prompt: 'work', mode: 'general' });
    assert.ok(!/running as an agent in a team/.test(submittedPrompt!));
  });
});
