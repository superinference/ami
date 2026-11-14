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
