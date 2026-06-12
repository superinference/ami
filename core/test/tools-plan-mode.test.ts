import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { planModeTool } from '../src/tools/plan-mode';
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
// Execute — stub behavior
// ---------------------------------------------------------------------------

describe('planModeTool — execute', () => {
  it('returns stub message (engine handles actual logic)', async () => {
    const result = await planModeTool.execute({ action: 'enter' }, ctx());
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('handled by the engine'));
  });

  it('returns same stub for exit action', async () => {
    const result = await planModeTool.execute({ action: 'exit' }, ctx());
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('handled by the engine'));
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
