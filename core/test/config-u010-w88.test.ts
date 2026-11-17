import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  applyConfigBackgroundTasks,
  applyConfigStartupKnobs,
  mergeConfigs,
  validateProjectConfigValues,
  findUnknownConfigKeys,
  getConfigJSONSchema,
  type ProjectConfig,
} from '../src/config';
import {
  DISABLE_BACKGROUND_TASKS_ENV,
  DISABLE_BACKGROUND_TASKS_ENV_COMPAT,
  isBackgroundTasksDisabled,
} from '../src/tools/tool-gates';
import { bashTool } from '../src/tools/bash';

// Helper: a fresh, isolated env object so a unit test never mutates the real process.env.
function env(init: Record<string, string> = {}): NodeJS.ProcessEnv {
  return { ...init } as NodeJS.ProcessEnv;
}

// ---------------------------------------------------------------------------
// applyConfigBackgroundTasks  (backgroundTasksEnabled -> AMI_DISABLE_BACKGROUND_TASKS)
// ---------------------------------------------------------------------------
describe('applyConfigBackgroundTasks', () => {
  it('backgroundTasksEnabled:false arms the disable env gate', () => {
    const t = env();
    applyConfigBackgroundTasks({ backgroundTasksEnabled: false }, t);
    assert.equal(t[DISABLE_BACKGROUND_TASKS_ENV], '1');
    assert.equal(isBackgroundTasksDisabled(t), true);
  });

  it('backgroundTasksEnabled:true leaves background tasks on (no env set)', () => {
    const t = env();
    applyConfigBackgroundTasks({ backgroundTasksEnabled: true }, t);
    assert.equal(t[DISABLE_BACKGROUND_TASKS_ENV], undefined);
    assert.equal(isBackgroundTasksDisabled(t), false);
  });

  it('is a no-op when the knob is unset', () => {
    const t = env();
    applyConfigBackgroundTasks({}, t);
    applyConfigBackgroundTasks(null, t);
    assert.equal(t[DISABLE_BACKGROUND_TASKS_ENV], undefined);
  });

  it('an explicit AMI_DISABLE_BACKGROUND_TASKS env wins (one-shot override)', () => {
    const t = env({ [DISABLE_BACKGROUND_TASKS_ENV]: '0' });
    applyConfigBackgroundTasks({ backgroundTasksEnabled: false }, t);
    assert.equal(t[DISABLE_BACKGROUND_TASKS_ENV], '0');
  });

  it('the Claude-compatible CLAUDE_CODE_DISABLE_BACKGROUND_TASKS alias also short-circuits', () => {
    const t = env({ [DISABLE_BACKGROUND_TASKS_ENV_COMPAT]: '1' });
    applyConfigBackgroundTasks({ backgroundTasksEnabled: false }, t);
    // The alias is already set + honored by isBackgroundTasksDisabled, so the primary
    // key must not be redundantly stamped.
    assert.equal(t[DISABLE_BACKGROUND_TASKS_ENV], undefined);
    assert.equal(isBackgroundTasksDisabled(t), true);
  });
});

// ---------------------------------------------------------------------------
// applyConfigStartupKnobs aggregates the new bridge
// ---------------------------------------------------------------------------
describe('applyConfigStartupKnobs — background tasks', () => {
  it('applies the background-task bridge', () => {
    const t = env();
    applyConfigStartupKnobs({ backgroundTasksEnabled: false }, t);
    assert.equal(t[DISABLE_BACKGROUND_TASKS_ENV], '1');
  });

  it('is a no-op for a config without the knob', () => {
    const t = env();
    applyConfigStartupKnobs({ backgroundTasksEnabled: true }, t);
    assert.equal(t[DISABLE_BACKGROUND_TASKS_ENV], undefined);
  });
});

// ---------------------------------------------------------------------------
// mergeConfigs copies the scalar (last-layer-wins)
// ---------------------------------------------------------------------------
describe('mergeConfigs — backgroundTasksEnabled', () => {
  it('copies the scalar with higher-priority-layer-wins', () => {
    const merged = mergeConfigs(
      {},
      {},
      { backgroundTasksEnabled: false },
      { backgroundTasksEnabled: true },
    );
    // projectConfig (arg 3, { false }) outranks globalConfig (arg 4, { true }).
    assert.equal(merged.backgroundTasksEnabled, false);
  });

  it('is absent when no layer sets it', () => {
    const merged = mergeConfigs({}, {}, {}, {});
    assert.equal(merged.backgroundTasksEnabled, undefined);
  });
});

// ---------------------------------------------------------------------------
// schema recognition + value validation (table-driven)
// ---------------------------------------------------------------------------
describe('backgroundTasksEnabled — schema recognition + validation', () => {
  it('is a recognized config key (no spurious unknown-key warning)', () => {
    assert.deepEqual(findUnknownConfigKeys({ backgroundTasksEnabled: false }), []);
  });

  it('flags a wrong value type', () => {
    const issues = validateProjectConfigValues({
      backgroundTasksEnabled: 'no' as unknown as boolean,
    });
    assert.deepEqual(issues.map(i => i.field), ['backgroundTasksEnabled']);
  });

  it('accepts a well-typed boolean', () => {
    assert.deepEqual(validateProjectConfigValues({ backgroundTasksEnabled: true }), []);
  });

  it('is advertised as a boolean in the config JSON schema', () => {
    const schema = getConfigJSONSchema();
    assert.equal(schema.properties.backgroundTasksEnabled?.type, 'boolean');
  });
});

// ---------------------------------------------------------------------------
// E2E: config -> applyConfigStartupKnobs -> process.env -> bash tool schema
// The bashTool.inputSchema getter reads isBackgroundTasksDisabled(process.env) at
// advertise-time, so a persisted backgroundTasksEnabled:false drops the
// run_in_background capability from the model-facing schema the FIRST turn.
// ---------------------------------------------------------------------------
describe('E2E: backgroundTasksEnabled gates the real bash tool schema', () => {
  let prevAmi: string | undefined;
  let prevCompat: string | undefined;
  beforeEach(() => {
    prevAmi = process.env[DISABLE_BACKGROUND_TASKS_ENV];
    prevCompat = process.env[DISABLE_BACKGROUND_TASKS_ENV_COMPAT];
    delete process.env[DISABLE_BACKGROUND_TASKS_ENV];
    delete process.env[DISABLE_BACKGROUND_TASKS_ENV_COMPAT];
  });
  afterEach(() => {
    if (prevAmi === undefined) delete process.env[DISABLE_BACKGROUND_TASKS_ENV];
    else process.env[DISABLE_BACKGROUND_TASKS_ENV] = prevAmi;
    if (prevCompat === undefined) delete process.env[DISABLE_BACKGROUND_TASKS_ENV_COMPAT];
    else process.env[DISABLE_BACKGROUND_TASKS_ENV_COMPAT] = prevCompat;
  });

  it('default (knob unset) keeps run_in_background advertised', () => {
    applyConfigStartupKnobs({}, process.env);
    const props = bashTool.inputSchema.properties as Record<string, unknown>;
    assert.ok('run_in_background' in props, 'run_in_background should be advertised by default');
  });

  it('backgroundTasksEnabled:false drops run_in_background from the advertised schema', () => {
    applyConfigStartupKnobs({ backgroundTasksEnabled: false }, process.env);
    assert.equal(isBackgroundTasksDisabled(), true);
    const props = bashTool.inputSchema.properties as Record<string, unknown>;
    assert.ok(!('run_in_background' in props), 'run_in_background must be omitted when disabled');
    // The description also drops its background guidance (same gate).
    assert.ok(!/run_in_background/.test(bashTool.description));
  });
});
