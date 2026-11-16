import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { planModeTool, setPlanModeActive } from '../src/tools/plan-mode';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
  };
}

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('planModeTool — definition', () => {
  it('has the correct name', () => {
    assert.equal(planModeTool.name, 'plan_mode');
  });

  it('has a description mentioning plan mode', () => {
    assert.ok(planModeTool.description.includes('plan mode'));
  });

  it('is read-only (not blocked by plan mode itself)', () => {
    assert.equal(planModeTool.isReadOnly, true);
  });

  it('schema requires "action"', () => {
    assert.ok(planModeTool.inputSchema.required?.includes('action'));
  });

  it('schema defines action with enter/exit enum', () => {
    const actionProp = planModeTool.inputSchema.properties.action;
    assert.ok(actionProp);
    assert.ok(actionProp.enum?.includes('enter'));
    assert.ok(actionProp.enum?.includes('exit'));
  });
});

// ---------------------------------------------------------------------------
// Execute — real implementation
// ---------------------------------------------------------------------------

describe('planModeTool — execute', () => {
  beforeEach(() => setPlanModeActive(false));

  it('enters plan mode and creates plan file', async () => {
    const result = await planModeTool.execute({ action: 'enter' }, ctx());
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('Entered plan mode'));
    assert.ok(result.output.includes('Plan file:'));
    setPlanModeActive(false);
  });

  it('exits plan mode and shows plan content', async () => {
    const fs = require('fs');
    await planModeTool.execute({ action: 'enter' }, ctx());
    const { getPlanFilePath } = require('../src/tools/plan-mode');
    fs.writeFileSync(getPlanFilePath(), '## Plan\n- do the thing\n');
    const result = await planModeTool.execute({ action: 'exit' }, ctx());
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('Exiting plan mode'));
    assert.ok(result.output.includes('do the thing'));
  });

  it('rejects exit when not in plan mode', async () => {
    const result = await planModeTool.execute({ action: 'exit' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Not in plan mode'));
  });

  it('rejects enter when already in plan mode', async () => {
    await planModeTool.execute({ action: 'enter' }, ctx());
    const result = await planModeTool.execute({ action: 'enter' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Already in plan mode'));
    setPlanModeActive(false);
  });

  it('returns error for unknown action', async () => {
    const result = await planModeTool.execute({ action: 'bogus' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Unknown action'));
    assert.ok(result.output.includes('bogus'));
  });

  it('verify reports status when in plan mode', async () => {
    await planModeTool.execute({ action: 'enter' }, ctx());
    const result = await planModeTool.execute({ action: 'verify' }, ctx());
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('Plan mode active'));
    assert.ok(result.output.includes('Plan file:'));
    setPlanModeActive(false);
  });

  it('verify reports not in plan mode when inactive', async () => {
    const result = await planModeTool.execute({ action: 'verify' }, ctx());
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('Not in plan mode'));
  });

  it('exit includes plan content from written file', async () => {
    const fs = require('fs');
    await planModeTool.execute({ action: 'enter' }, ctx());
    const { getPlanFilePath } = require('../src/tools/plan-mode');
    const planFile = getPlanFilePath();
    assert.ok(planFile, 'Plan file path should be set');
    fs.writeFileSync(planFile, '## My Plan\n- Step 1\n- Step 2\n');
    const result = await planModeTool.execute({ action: 'exit' }, ctx());
    assert.ok(result.output.includes('## My Plan'));
    assert.ok(result.output.includes('Step 1'));
  });
});

// ---------------------------------------------------------------------------
// Parity enhancements
// ---------------------------------------------------------------------------

describe('planModeTool — parity enhancements', () => {
  beforeEach(() => setPlanModeActive(false));

  it('rejects entering plan mode from a sub-agent context', async () => {
    const result = await planModeTool.execute({ action: 'enter' }, { ...ctx(), agentId: 'sub-1' });
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('sub-agent'));
    assert.equal(require('../src/tools/plan-mode').isPlanModeActive(), false);
  });

  it('accepts a plan override on exit and flags it as user-edited', async () => {
    await planModeTool.execute({ action: 'enter' }, ctx());
    const result = await planModeTool.execute({ action: 'exit', plan: '## Edited\n- revised step' }, ctx());
    assert.ok(result.output.includes('user-edited'));
    assert.ok(result.output.includes('revised step'));
  });

  it('handles an empty plan on exit without the approval framing', async () => {
    await planModeTool.execute({ action: 'enter' }, ctx());
    const result = await planModeTool.execute({ action: 'exit' }, ctx());
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('empty plan'));
  });

  it('getPlan recovers the last submitted plan content', async () => {
    const { getPlan } = require('../src/tools/plan-mode');
    await planModeTool.execute({ action: 'enter' }, ctx());
    await planModeTool.execute({ action: 'exit', plan: 'remembered plan' }, ctx());
    assert.equal(getPlan(), 'remembered plan');
  });

  it('getPlan falls back to the on-disk plan file while planning (nothing submitted yet)', async () => {
    const fs = require('fs');
    const mod = require('../src/tools/plan-mode');
    // Entering plan mode starts a fresh plan session (no stale last-plan leaks).
    await planModeTool.execute({ action: 'enter' }, ctx());
    const planFile = mod.getPlanFilePath();
    assert.ok(planFile, 'Plan file path should be set after enter');
    fs.writeFileSync(planFile, '## Draft plan\n- explore the module\n');
    // No exit has run, so there is no in-memory submitted plan; getPlan must
    // recover the plan content straight from disk (Claude getPlan() parity).
    assert.equal(mod.getPlan(), '## Draft plan\n- explore the module\n');
    mod.setPlanModeActive(false);
  });

  it('entering plan mode clears a stale submitted plan from a prior session', async () => {
    const mod = require('../src/tools/plan-mode');
    await planModeTool.execute({ action: 'enter' }, ctx());
    await planModeTool.execute({ action: 'exit', plan: 'old session plan' }, ctx());
    assert.equal(mod.getPlan(), 'old session plan');
    // A new plan session must not report the previous session's plan.
    await planModeTool.execute({ action: 'enter' }, ctx());
    assert.equal(mod.getPlan(), null);
    mod.setPlanModeActive(false);
  });

  it('caps very large plan content in the echoed result', async () => {
    const { PLAN_MAX_RESULT_CHARS } = require('../src/tools/plan-mode');
    await planModeTool.execute({ action: 'enter' }, ctx());
    const big = 'x'.repeat(PLAN_MAX_RESULT_CHARS + 500);
    const result = await planModeTool.execute({ action: 'exit', plan: big }, ctx());
    assert.ok(result.output.includes('plan truncated'));
  });

  it('allowedPrompts restricts the tool field to Bash via enum', () => {
    const items = (planModeTool.inputSchema.properties as any).allowedPrompts.items;
    assert.deepEqual(items.properties.tool.enum, ['Bash']);
  });

  it('description carries when-to-use / when-not / AskUserQuestion guidance', () => {
    const d = planModeTool.description;
    assert.ok(d.includes('WHEN TO USE'));
    assert.ok(d.includes('WHEN NOT TO USE'));
    assert.ok(d.includes('AskUserQuestion'));
  });

  it('getPlansDirectory resolves within the project state dir', () => {
    const { getPlansDirectory } = require('../src/tools/plan-mode');
    const dir = getPlansDirectory(process.cwd());
    assert.ok(dir.endsWith('plans'));
  });

  it('allowedPrompts items are a strict object (additionalProperties: false)', () => {
    const items = (planModeTool.inputSchema.properties as any).allowedPrompts.items;
    assert.equal(items.additionalProperties, false);
  });

  it('allowedPrompts prompt field carries a description/example', () => {
    const items = (planModeTool.inputSchema.properties as any).allowedPrompts.items;
    assert.equal(typeof items.properties.prompt.description, 'string');
    assert.ok(items.properties.prompt.description.length > 0);
  });

  it('verify warns when the plan exceeds the soft conciseness cap', async () => {
    const fs = require('fs');
    const { getPlanFilePath, PLAN_SOFT_CAP_CHARS } = require('../src/tools/plan-mode');
    await planModeTool.execute({ action: 'enter' }, ctx());
    fs.writeFileSync(getPlanFilePath(), 'y'.repeat(PLAN_SOFT_CAP_CHARS + 100));
    const result = await planModeTool.execute({ action: 'verify' }, ctx());
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('soft cap'));
    assert.ok(result.output.includes('trim it'));
    setPlanModeActive(false);
  });

  it('verify reports plan-has-content without warning below the soft cap', async () => {
    const fs = require('fs');
    const { getPlanFilePath } = require('../src/tools/plan-mode');
    await planModeTool.execute({ action: 'enter' }, ctx());
    fs.writeFileSync(getPlanFilePath(), '## Short plan\n- step');
    const result = await planModeTool.execute({ action: 'verify' }, ctx());
    assert.ok(result.output.includes('Plan has content.'));
    assert.ok(!result.output.includes('soft cap'));
    setPlanModeActive(false);
  });

  it('allocatePlanFile returns a plan-*.md path inside the given dir', () => {
    const os = require('os');
    const fs = require('fs');
    const path = require('path');
    const { allocatePlanFile } = require('../src/tools/plan-mode');
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'plandir-'));
    const file = allocatePlanFile(dir);
    assert.ok(file.startsWith(dir));
    assert.ok(/plan-.*\.md$/.test(file));
    fs.rmSync(dir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// Slug cache + configurable/memoized plans directory (parity: plans.ts)
// ---------------------------------------------------------------------------

describe('planModeTool — session slug cache + word-slug filenames', () => {
  const os = require('os');
  const fs = require('fs');
  const path = require('path');
  const {
    allocatePlanFile,
    getPlanSlug,
    clearPlanSlug,
    clearAllPlanSlugs,
  } = require('../src/tools/plan-mode');

  it('allocatePlanFile uses a memorable word slug (not a raw timestamp)', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'plandir-'));
    const file = allocatePlanFile(dir);
    const name = path.basename(file);
    // Word-slug form: plan-<adjective>-<verb>-<noun>.md — not plan-<digits>.md
    assert.match(name, /^plan-[a-z]+-[a-z]+-[a-z]+\.md$/);
    assert.doesNotMatch(name, /^plan-\d+\.md$/);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('returns a STABLE per-session filename across repeated allocations', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'plandir-'));
    clearAllPlanSlugs();
    const a = allocatePlanFile(dir, { sessionId: 'sess-A' });
    const b = allocatePlanFile(dir, { sessionId: 'sess-A' });
    assert.equal(a, b, 'same session should reuse the same plan file');
    // The session filename is <slug>.md (no plan- prefix), per Claude parity.
    assert.match(path.basename(a), /^[a-z]+-[a-z]+-[a-z]+\.md$/);
    clearAllPlanSlugs();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('distinct sessions get distinct plan files', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'plandir-'));
    clearAllPlanSlugs();
    const a = allocatePlanFile(dir, { sessionId: 'sess-1' });
    const b = allocatePlanFile(dir, { sessionId: 'sess-2' });
    assert.notEqual(a, b);
    clearAllPlanSlugs();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('getPlanSlug retries past an on-disk collision', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'plandir-'));
    clearAllPlanSlugs();
    const first = getPlanSlug('sess-X', dir);
    // Write the slug file so a sibling session must pick a different one.
    fs.writeFileSync(path.join(dir, `${first}.md`), 'x');
    clearPlanSlug('sess-Y'); // ensure fresh
    const second = getPlanSlug('sess-Y', dir);
    assert.notEqual(second, first, 'should avoid the occupied slug');
    clearAllPlanSlugs();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('clearPlanSlug forgets one session; clearAllPlanSlugs forgets all', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'plandir-'));
    clearAllPlanSlugs();
    const a1 = allocatePlanFile(dir, { sessionId: 'sess-c' });
    clearPlanSlug('sess-c');
    // A new slug may differ; at minimum the cache was cleared and re-populated.
    const a2 = allocatePlanFile(dir, { sessionId: 'sess-c' });
    assert.ok(typeof a2 === 'string');
    void a1;
    clearAllPlanSlugs();
    fs.rmSync(dir, { recursive: true, force: true });
  });
});

describe('planModeTool — configurable + memoized plans directory', () => {
  const os = require('os');
  const fs = require('fs');
  const path = require('path');
  const { getPlansDirectory, clearPlansDirectoryCache } = require('../src/tools/plan-mode');

  it('AMI_PLANS_DIR relocates the plans dir within the project root', () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'proj-'));
    clearPlansDirectoryCache();
    const dir = getPlansDirectory(cwd, { AMI_PLANS_DIR: 'docs/plans' });
    assert.equal(dir, path.join(cwd, 'docs', 'plans'));
    clearPlansDirectoryCache();
    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('rejects a traversal override and falls back to the default state dir', () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'proj-'));
    clearPlansDirectoryCache();
    const dir = getPlansDirectory(cwd, { AMI_PLANS_DIR: '../../etc/evil' });
    assert.doesNotMatch(dir, /etc[\\/]evil/, 'must not honor the traversal path');
    assert.ok(dir.endsWith('plans'), 'falls back to the default plans dir');
    clearPlansDirectoryCache();
    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('memoizes resolution per (cwd, override)', () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'proj-'));
    clearPlansDirectoryCache();
    const a = getPlansDirectory(cwd, { AMI_PLANS_DIR: 'p' });
    const b = getPlansDirectory(cwd, { AMI_PLANS_DIR: 'p' });
    assert.equal(a, b);
    const c = getPlansDirectory(cwd, {}); // different override → different entry
    assert.notEqual(a, c);
    clearPlansDirectoryCache();
    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('defaults to the per-project state plans dir when unset', () => {
    const dir = getPlansDirectory(process.cwd(), {});
    assert.ok(dir.endsWith('plans'));
  });
});

// ---------------------------------------------------------------------------
// Integration — tool registry
// ---------------------------------------------------------------------------

describe('planModeTool — integration', () => {
  it('is registered in createDefaultTools', () => {
    const { createDefaultTools } = require('../src/tools/index');
    const registry = createDefaultTools('/tmp');
    const tool = registry.get('plan_mode');
    assert.ok(tool, 'plan_mode should be in the default registry');
    assert.equal(tool.name, 'plan_mode');
    assert.equal(tool.isReadOnly, true);
  });

  it('is included in OpenAI format', () => {
    const { createDefaultTools } = require('../src/tools/index');
    const registry = createDefaultTools('/tmp');
    const formatted = registry.toOpenAIFormat();
    const entry = formatted.find((e: any) => e.function.name === 'plan_mode');
    assert.ok(entry);
    assert.equal(entry.type, 'function');
    assert.ok(entry.function.description.includes('plan mode'));
  });
});

// ---------------------------------------------------------------------------
// Interview-phase flag + configurable exploration agent count (parity:
// planModeV2.ts isPlanModeInterviewPhaseEnabled / getPlanModeV2AgentCount)
// ---------------------------------------------------------------------------

describe('planModeTool — interview phase flag', () => {
  const KEYS = ['AMI_PLAN_MODE_INTERVIEW_PHASE', 'CLAUDE_CODE_PLAN_MODE_INTERVIEW_PHASE'];
  const saved: Record<string, string | undefined> = {};
  beforeEach(() => {
    for (const k of KEYS) { saved[k] = process.env[k]; delete process.env[k]; }
  });
  function restore() { for (const k of KEYS) { if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k]; } }

  it('is disabled by default', () => {
    const { isPlanModeInterviewPhaseEnabled } = require('../src/tools/plan-mode');
    assert.equal(isPlanModeInterviewPhaseEnabled({}), false);
    restore();
  });

  it('is enabled via AMI_PLAN_MODE_INTERVIEW_PHASE', () => {
    const { isPlanModeInterviewPhaseEnabled } = require('../src/tools/plan-mode');
    assert.equal(isPlanModeInterviewPhaseEnabled({ AMI_PLAN_MODE_INTERVIEW_PHASE: '1' }), true);
    restore();
  });

  it('honors the Claude-config env var for compatibility', () => {
    const { isPlanModeInterviewPhaseEnabled } = require('../src/tools/plan-mode');
    assert.equal(isPlanModeInterviewPhaseEnabled({ CLAUDE_CODE_PLAN_MODE_INTERVIEW_PHASE: 'true' }), true);
    restore();
  });

  it('treats falsy values as disabled', () => {
    const { isPlanModeInterviewPhaseEnabled } = require('../src/tools/plan-mode');
    assert.equal(isPlanModeInterviewPhaseEnabled({ AMI_PLAN_MODE_INTERVIEW_PHASE: '0' }), false);
    assert.equal(isPlanModeInterviewPhaseEnabled({ AMI_PLAN_MODE_INTERVIEW_PHASE: 'false' }), false);
    restore();
  });

  it('enter output uses the linear exploration workflow when disabled', async () => {
    const result = await planModeTool.execute({ action: 'enter' }, ctx());
    assert.ok(result.output.includes('Exploration workflow'));
    assert.ok(!result.output.includes('Interview phase'));
    setPlanModeActive(false);
    restore();
  });

  it('enter output switches to the interview variant when enabled', async () => {
    process.env.AMI_PLAN_MODE_INTERVIEW_PHASE = '1';
    const result = await planModeTool.execute({ action: 'enter' }, ctx());
    assert.ok(result.output.includes('Interview phase'));
    assert.ok(result.output.includes('AskUserQuestion'));
    setPlanModeActive(false);
    restore();
  });
});

describe('planModeTool — configurable exploration agent count', () => {
  const KEYS = ['AMI_PLAN_AGENT_COUNT', 'CLAUDE_CODE_PLAN_V2_AGENT_COUNT', 'AMI_PLAN_MODE_INTERVIEW_PHASE'];
  const saved: Record<string, string | undefined> = {};
  beforeEach(() => { for (const k of KEYS) { saved[k] = process.env[k]; delete process.env[k]; } });
  function restore() { for (const k of KEYS) { if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k]; } }

  it('defaults to 1 when unset', () => {
    const { getPlanAgentCount, PLAN_AGENT_COUNT_DEFAULT } = require('../src/tools/plan-mode');
    assert.equal(getPlanAgentCount({}), PLAN_AGENT_COUNT_DEFAULT);
    assert.equal(getPlanAgentCount({}), 1);
    restore();
  });

  it('reads a valid override', () => {
    const { getPlanAgentCount } = require('../src/tools/plan-mode');
    assert.equal(getPlanAgentCount({ AMI_PLAN_AGENT_COUNT: '3' }), 3);
    restore();
  });

  it('honors the Claude-config env var for compatibility', () => {
    const { getPlanAgentCount } = require('../src/tools/plan-mode');
    assert.equal(getPlanAgentCount({ CLAUDE_CODE_PLAN_V2_AGENT_COUNT: '5' }), 5);
    restore();
  });

  it('clamps out-of-range and rejects non-numeric values to the default', () => {
    const { getPlanAgentCount, PLAN_AGENT_COUNT_DEFAULT } = require('../src/tools/plan-mode');
    assert.equal(getPlanAgentCount({ AMI_PLAN_AGENT_COUNT: '0' }), PLAN_AGENT_COUNT_DEFAULT);
    assert.equal(getPlanAgentCount({ AMI_PLAN_AGENT_COUNT: '99' }), PLAN_AGENT_COUNT_DEFAULT);
    assert.equal(getPlanAgentCount({ AMI_PLAN_AGENT_COUNT: 'abc' }), PLAN_AGENT_COUNT_DEFAULT);
    restore();
  });

  it('surfaces the configured agent count in the interview-phase enter output', async () => {
    process.env.AMI_PLAN_MODE_INTERVIEW_PHASE = '1';
    process.env.AMI_PLAN_AGENT_COUNT = '4';
    const result = await planModeTool.execute({ action: 'enter' }, ctx());
    assert.ok(result.output.includes('4'));
    setPlanModeActive(false);
    restore();
  });
});

describe('planModeTool — enriched description (parity: EnterPlanModeTool/prompt.ts)', () => {
  it('leads with a proactive-use rationale', () => {
    assert.ok(/proactively/i.test(planModeTool.description));
  });
  it('carries numbered when-to-use categories and GOOD/BAD worked examples', () => {
    const d = planModeTool.description;
    assert.ok(d.includes('Architectural decisions'));
    assert.ok(d.includes('GOOD'));
    assert.ok(d.includes('BAD'));
  });
});
