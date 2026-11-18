/**
 * U010 / w109 — `task_tracker` enablement knob (task-mode toggle).
 *
 * AMI ships BOTH the rich `task_tracker` and the lightweight `todo_write` and keeps
 * both ON by default — a protected superset over Claude Code, which exposes only one
 * at a time behind `isTodoV2Enabled()`. The MISSING capability was a knob to opt the
 * heavier task tool out for SDK/headless use, exactly as Claude's
 * `CLAUDE_CODE_ENABLE_TASKS` toggles task mode.
 *
 * This locks in {@link isTaskTrackerEnabled} (Claude-compatible + AMI-native + generic
 * feature-flag resolution, default ON), the `taskTrackerTool.isEnabled` predicate, and
 * `createDefaultTools` dropping `task_tracker` when the knob is off (while `todo_write`
 * and the essentials remain).
 *
 * RED (pre-fix, no `isEnabled` on `taskTrackerTool`): the predicate is `undefined` and
 * `task_tracker` is always registered → the type + drop assertions FAIL.
 */
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  createDefaultTools,
  taskTrackerTool,
  isTaskTrackerEnabled,
  TASK_TRACKER_ENV,
  TASK_TRACKER_FEATURE,
  CLAUDE_ENABLE_TASKS_ENV,
} from '../src/tools/index';

function toolNames(env: NodeJS.ProcessEnv): Set<string> {
  const saved = process.env;
  const savedArgv = process.argv;
  process.env = env;
  process.argv = ['node', 'test'];
  try {
    return new Set(createDefaultTools('/tmp').getAll().map((t) => t.name));
  } finally {
    process.env = saved;
    process.argv = savedArgv;
  }
}

describe('U010/w109 — isTaskTrackerEnabled gate', () => {
  it('exposes the env/feature-name constants', () => {
    assert.equal(TASK_TRACKER_ENV, 'AMI_TASK_TRACKER');
    assert.equal(TASK_TRACKER_FEATURE, 'ENABLE_TASKS');
    assert.equal(CLAUDE_ENABLE_TASKS_ENV, 'CLAUDE_CODE_ENABLE_TASKS');
  });

  it('defaults ON when nothing is set', () => {
    assert.equal(isTaskTrackerEnabled({}), true);
  });

  it('honors the Claude-compatible CLAUDE_CODE_ENABLE_TASKS knob (both directions)', () => {
    assert.equal(isTaskTrackerEnabled({ [CLAUDE_ENABLE_TASKS_ENV]: '0' }), false);
    assert.equal(isTaskTrackerEnabled({ [CLAUDE_ENABLE_TASKS_ENV]: 'false' }), false);
    assert.equal(isTaskTrackerEnabled({ [CLAUDE_ENABLE_TASKS_ENV]: '1' }), true);
  });

  it('honors the AMI-native AMI_TASK_TRACKER disable switch', () => {
    assert.equal(isTaskTrackerEnabled({ [TASK_TRACKER_ENV]: 'off' }), false);
    assert.equal(isTaskTrackerEnabled({ [TASK_TRACKER_ENV]: '1' }), true);
  });

  it('honors the generic ENABLE_TASKS feature flag', () => {
    assert.equal(isTaskTrackerEnabled({ SUPERINFERENCE_FEATURE_ENABLE_TASKS: 'off' }), false);
    assert.equal(isTaskTrackerEnabled({ CLAUDE_CODE_FEATURE_ENABLE_TASKS: 'off' }), false);
  });

  it('the Claude knob wins over the AMI-native + feature flag', () => {
    assert.equal(
      isTaskTrackerEnabled({
        [CLAUDE_ENABLE_TASKS_ENV]: '1',
        [TASK_TRACKER_ENV]: 'off',
        SUPERINFERENCE_FEATURE_ENABLE_TASKS: 'off',
      }),
      true,
    );
  });
});

describe('U010/w109 — taskTrackerTool declares the isEnabled predicate', () => {
  const saved = process.env;
  afterEach(() => { process.env = saved; });

  it('declares isEnabled and reports enabled by default', () => {
    assert.equal(typeof taskTrackerTool.isEnabled, 'function');
    process.env = {};
    assert.equal(taskTrackerTool.isEnabled!(), true);
  });

  it('reports disabled when the knob is off', () => {
    process.env = { [TASK_TRACKER_ENV]: 'off' };
    assert.equal(taskTrackerTool.isEnabled!(), false);
  });
});

describe('U010/w109 — createDefaultTools honors the task-tracker knob', () => {
  it('registers task_tracker (and todo_write) by default', () => {
    const names = toolNames({});
    assert.ok(names.has('task_tracker'), 'task_tracker registered by default');
    assert.ok(names.has('todo_write'), 'todo_write registered by default');
  });

  it('drops task_tracker when the knob is off, keeping todo_write + essentials', () => {
    const names = toolNames({ [TASK_TRACKER_ENV]: 'off' });
    assert.ok(!names.has('task_tracker'), 'task_tracker dropped when disabled');
    assert.ok(names.has('todo_write'), 'the lightweight todo_write remains');
    assert.ok(names.has('bash') && names.has('file_edit') && names.has('skill'));
  });

  it('drops task_tracker via the Claude-compatible knob too', () => {
    const names = toolNames({ [CLAUDE_ENABLE_TASKS_ENV]: '0' });
    assert.ok(!names.has('task_tracker'));
    assert.ok(names.has('todo_write'));
  });
});
