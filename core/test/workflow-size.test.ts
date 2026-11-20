import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  WORKFLOW_SIZE_ENV, WORKFLOW_SIZES, DEFAULT_WORKFLOW_SIZE,
  normalizeWorkflowSize, resolveWorkflowSize, getWorkflowSizeGuideline,
  workflowSizeGuidelineText, isLargeWorkflow,
} from '../src/tools/workflow-size';
import { applyConfigWorkflowSize } from '../src/config';
import { workflowTool } from '../src/tools/workflow';

// U4 — Dynamic workflow size: the soft agent-count guideline + "Large workflow"
// warning threshold, its env knob, the config->env bridge, and the guideline
// injected into the workflow tool description.

describe('workflow-size helper', () => {
  afterEach(() => { delete process.env[WORKFLOW_SIZE_ENV]; });

  it('normalizeWorkflowSize accepts the four sizes (case-insensitive), rejects others', () => {
    for (const s of WORKFLOW_SIZES) assert.equal(normalizeWorkflowSize(s.toUpperCase()), s);
    assert.equal(normalizeWorkflowSize('huge'), undefined);
    assert.equal(normalizeWorkflowSize(5), undefined);
    assert.equal(normalizeWorkflowSize(undefined), undefined);
  });

  it('resolveWorkflowSize defaults to medium, reads env, falls back on garbage', () => {
    assert.equal(resolveWorkflowSize({}), DEFAULT_WORKFLOW_SIZE);
    assert.equal(resolveWorkflowSize({ [WORKFLOW_SIZE_ENV]: 'large' }), 'large');
    assert.equal(resolveWorkflowSize({ [WORKFLOW_SIZE_ENV]: 'nonsense' }), 'medium');
  });

  it('bands: soft cap + warn threshold increase with size; off disables both', () => {
    assert.deepEqual(
      (['small', 'medium', 'large'] as const).map(s => getWorkflowSizeGuideline(s).softMaxAgents),
      [5, 10, 25],
    );
    assert.deepEqual(
      (['small', 'medium', 'large'] as const).map(s => getWorkflowSizeGuideline(s).warnThreshold),
      [8, 16, 40],
    );
    const off = getWorkflowSizeGuideline('off');
    assert.equal(off.softMaxAgents, null);
    assert.equal(off.warnThreshold, Infinity);
    assert.equal(getWorkflowSizeGuideline('medium').label, 'medium size');
  });

  it('isLargeWorkflow fires above the per-size threshold; never when off', () => {
    assert.equal(isLargeWorkflow(9, 'small'), true);
    assert.equal(isLargeWorkflow(8, 'small'), false);
    assert.equal(isLargeWorkflow(17, 'medium'), true);
    assert.equal(isLargeWorkflow(16, 'medium'), false);
    assert.equal(isLargeWorkflow(10000, 'off'), false);
  });

  it('workflowSizeGuidelineText reflects the size and the off state', () => {
    assert.match(workflowSizeGuidelineText({ [WORKFLOW_SIZE_ENV]: 'large' }), /Dynamic workflow size is large/);
    assert.match(workflowSizeGuidelineText({ [WORKFLOW_SIZE_ENV]: 'large' }), /~25 agents/);
    assert.match(workflowSizeGuidelineText({ [WORKFLOW_SIZE_ENV]: 'off' }), /OFF/);
  });
});

describe('applyConfigWorkflowSize bridge', () => {
  it('seeds the env knob from a valid persisted size', () => {
    const env: NodeJS.ProcessEnv = {};
    applyConfigWorkflowSize({ dynamicWorkflowSize: 'large' }, env);
    assert.equal(env[WORKFLOW_SIZE_ENV], 'large');
  });
  it('an explicit env value wins; an invalid/absent config is a no-op', () => {
    const env: NodeJS.ProcessEnv = { [WORKFLOW_SIZE_ENV]: 'small' };
    applyConfigWorkflowSize({ dynamicWorkflowSize: 'large' }, env);
    assert.equal(env[WORKFLOW_SIZE_ENV], 'small');
    const env2: NodeJS.ProcessEnv = {};
    applyConfigWorkflowSize({ dynamicWorkflowSize: 'bogus' as any }, env2);
    assert.equal(env2[WORKFLOW_SIZE_ENV], undefined);
    const env3: NodeJS.ProcessEnv = {};
    applyConfigWorkflowSize(null, env3);
    assert.equal(env3[WORKFLOW_SIZE_ENV], undefined);
  });
});

describe('workflow tool description reflects the configured size', () => {
  afterEach(() => { delete process.env[WORKFLOW_SIZE_ENV]; });
  it('injects the live size guideline + the background/flows note', () => {
    process.env[WORKFLOW_SIZE_ENV] = 'large';
    const desc = workflowTool.description;
    assert.match(desc, /Dynamic workflow size is large/);
    assert.match(desc, /launches in the BACKGROUND by default/);
    assert.match(desc, /\/flows/);
  });
});
