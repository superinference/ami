import { describe, it, beforeEach, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  taskTool,
  resetAgentRegistry,
  // w80 additions under test — plan-required subagent spawns (parity: the reference
  // planModeRequired + CLAUDE_CODE_PLAN_MODE_REQUIRED).
  SUBAGENT_PLAN_REQUIRED_ENV,
  CLAUDE_PLAN_MODE_REQUIRED_ENV,
  isSubagentPlanRequiredByEnv,
  resolvePlanRequired,
  applyPlanRequirement,
  buildPlanRequiredNotice,
} from '../src/tools/task';
import type { ToolContext, ToolDefinition, EngineConfig } from '../src/types';

let dataDir: string;
before(() => {
  dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u011-w80-'));
  process.env.SUPERINFERENCE_DATA_DIR = dataDir;
});
after(() => {
  delete process.env.SUPERINFERENCE_DATA_DIR;
  delete process.env[SUBAGENT_PLAN_REQUIRED_ENV];
  delete process.env[CLAUDE_PLAN_MODE_REQUIRED_ENV];
  try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* best-effort */ }
});

type Ev = Record<string, unknown>;

/** Run task.execute with a fake engine, capturing the resolved EngineConfig AND the
 *  first-turn prompt handed to the sub-engine's submit(). */
async function runCapture(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
): Promise<{ result: { isError?: boolean; output: string }; captured: EngineConfig; submitPrompt: string }> {
  let captured!: EngineConfig;
  let submitPrompt = '';
  async function* fakeSubmit(p?: string) { submitPrompt = p ?? ''; yield { type: 'text_delta', text: 'ok' } as never; }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c: EngineConfig) => {
      captured = c;
      return { submit: fakeSubmit, shutdown() {} } as never;
    },
    ...ctx,
  } as unknown as ToolContext);
  return { result: result as { isError?: boolean; output: string }, captured, submitPrompt };
}

// ===========================================================================
// resolvePlanRequired — three-tier resolution (input > env > off)
// ===========================================================================
describe('resolvePlanRequired', () => {
  beforeEach(() => {
    delete process.env[SUBAGENT_PLAN_REQUIRED_ENV];
    delete process.env[CLAUDE_PLAN_MODE_REQUIRED_ENV];
  });

  it('an explicit truthy input flag wins (even with no env) — coerceBool semantics', () => {
    assert.equal(resolvePlanRequired(true), true);
    assert.equal(resolvePlanRequired('true'), true);
    assert.equal(resolvePlanRequired(' TRUE '), true);
  });

  it('an explicit falsy input flag wins OVER a truthy env default', () => {
    const env = { [SUBAGENT_PLAN_REQUIRED_ENV]: '1' } as NodeJS.ProcessEnv;
    assert.equal(resolvePlanRequired(false, env), false);
    // A non-"true" string is falsy under coerceBool — so an explicit (present) flag
    // that is not "true" still overrides the env (the input was provided).
    assert.equal(resolvePlanRequired('no', env), false);
  });

  it('falls back to the AMI env default when the input is absent', () => {
    assert.equal(resolvePlanRequired(undefined, { [SUBAGENT_PLAN_REQUIRED_ENV]: 'true' } as NodeJS.ProcessEnv), true);
    assert.equal(resolvePlanRequired(null, { [SUBAGENT_PLAN_REQUIRED_ENV]: 'true' } as NodeJS.ProcessEnv), true);
  });

  it('honors the Claude Code env spelling as a cross-framework alias', () => {
    assert.equal(resolvePlanRequired(undefined, { [CLAUDE_PLAN_MODE_REQUIRED_ENV]: '1' } as NodeJS.ProcessEnv), true);
  });

  it('defaults OFF when neither input nor env is set (additive — no change)', () => {
    assert.equal(resolvePlanRequired(undefined, {} as NodeJS.ProcessEnv), false);
  });
});

describe('isSubagentPlanRequiredByEnv', () => {
  it('is true for either env spelling and false otherwise', () => {
    assert.equal(isSubagentPlanRequiredByEnv({ [SUBAGENT_PLAN_REQUIRED_ENV]: '1' } as NodeJS.ProcessEnv), true);
    assert.equal(isSubagentPlanRequiredByEnv({ [CLAUDE_PLAN_MODE_REQUIRED_ENV]: 'true' } as NodeJS.ProcessEnv), true);
    assert.equal(isSubagentPlanRequiredByEnv({} as NodeJS.ProcessEnv), false);
    assert.equal(isSubagentPlanRequiredByEnv({ [SUBAGENT_PLAN_REQUIRED_ENV]: '0' } as NodeJS.ProcessEnv), false);
  });
});

// ===========================================================================
// applyPlanRequirement — clamp to the read-only "plan" floor without loosening
// ===========================================================================
describe('applyPlanRequirement', () => {
  it('is a no-op when plan is not required (every mode passes through)', () => {
    for (const m of ['ask', 'acceptEdits', 'auto-allow', 'bypassPermissions', 'deny-all', 'plan']) {
      assert.equal(applyPlanRequirement(m, false), m);
    }
  });

  it('clamps every more-permissive mode DOWN to plan', () => {
    assert.equal(applyPlanRequirement('ask', true), 'plan');
    assert.equal(applyPlanRequirement('acceptEdits', true), 'plan');
    assert.equal(applyPlanRequirement('auto-allow', true), 'plan');
    assert.equal(applyPlanRequirement('bypassPermissions', true), 'plan');
    assert.equal(applyPlanRequirement('plan', true), 'plan');
  });

  it('never LOOSENS a stricter deny-all (deny-all denies reads too)', () => {
    assert.equal(applyPlanRequirement('deny-all', true), 'deny-all');
  });
});

describe('buildPlanRequiredNotice', () => {
  it('tells the agent to return a plan and NOT to write code', () => {
    const n = buildPlanRequiredNotice();
    assert.match(n, /PLAN MODE REQUIRED/);
    assert.match(n, /plan/i);
    assert.match(n, /do not edit|not edit|denied/i);
  });
});

// ===========================================================================
// E2E through taskTool.execute — the whole wired spawn path
// ===========================================================================
describe('task.execute — plan_mode_required spawn', () => {
  beforeEach(() => {
    resetAgentRegistry();
    delete process.env[SUBAGENT_PLAN_REQUIRED_ENV];
    delete process.env[CLAUDE_PLAN_MODE_REQUIRED_ENV];
  });

  it('plan_mode_required:true forces the sub-engine into read-only plan mode', async () => {
    const { captured } = await runCapture({ prompt: 'implement X', plan_mode_required: true });
    assert.equal(captured.permissionMode, 'plan');
  });

  it('prepends a PLAN MODE REQUIRED notice to the subagent first turn', async () => {
    const { submitPrompt } = await runCapture({ prompt: 'implement X', plan_mode_required: true });
    assert.match(submitPrompt, /PLAN MODE REQUIRED/);
    assert.ok(submitPrompt.includes('implement X'), 'original task prompt is preserved after the notice');
  });

  it('default (flag absent, env unset) leaves the mode unchanged and injects NO notice', async () => {
    const { captured, submitPrompt } = await runCapture(
      { prompt: 'do X' },
      { _permissionPromptHandler: (() => {}) as unknown as ToolContext['_permissionPromptHandler'] },
    );
    assert.equal(captured.permissionMode, 'ask');
    assert.doesNotMatch(submitPrompt, /PLAN MODE REQUIRED/);
  });

  it('the env default makes an ordinary spawn plan-required without a per-call flag', async () => {
    process.env[SUBAGENT_PLAN_REQUIRED_ENV] = '1';
    const { captured, submitPrompt } = await runCapture({ prompt: 'do X' });
    assert.equal(captured.permissionMode, 'plan');
    assert.match(submitPrompt, /PLAN MODE REQUIRED/);
  });

  it('a per-call plan_mode_required:false overrides a truthy env default (opt out)', async () => {
    process.env[SUBAGENT_PLAN_REQUIRED_ENV] = '1';
    const { captured, submitPrompt } = await runCapture(
      { prompt: 'do X', plan_mode_required: false },
      { _permissionPromptHandler: (() => {}) as unknown as ToolContext['_permissionPromptHandler'] },
    );
    assert.equal(captured.permissionMode, 'ask');
    assert.doesNotMatch(submitPrompt, /PLAN MODE REQUIRED/);
  });

  it('advertises the plan_mode_required knob in the input schema', () => {
    const props = (taskTool.inputSchema as { properties: Record<string, { type?: string }> }).properties;
    assert.ok(props.plan_mode_required, 'plan_mode_required is an advertised task argument');
    assert.equal(props.plan_mode_required.type, 'boolean');
  });
});
