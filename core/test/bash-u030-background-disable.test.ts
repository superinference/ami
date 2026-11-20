import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  bashTool,
  buildBashDescription,
  buildBashInputSchema,
} from '../src/tools/bash';
import {
  isBackgroundTasksDisabled,
  DISABLE_BACKGROUND_TASKS_ENV,
  DISABLE_BACKGROUND_TASKS_ENV_COMPAT,
} from '../src/tools/tool-gates';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Unit U030 — kill-switch env knob to disable background tasks.
// Gaps: config knob to drop run_in_background from the schema; env knob to
// disable background tasks (removes run_in_background from the advertised
// schema); kill-switch env var to fully disable background tasks; and
// CLAUDE_CODE_DISABLE_BACKGROUND_TASKS config knob to gate background guidance.
// Parity: the reference's CLAUDE_CODE_DISABLE_BACKGROUND_TASKS.
// ---------------------------------------------------------------------------

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

const SAVE = {
  ami: process.env[DISABLE_BACKGROUND_TASKS_ENV],
  cc: process.env[DISABLE_BACKGROUND_TASKS_ENV_COMPAT],
};
function restoreEnv(): void {
  if (SAVE.ami === undefined) delete process.env[DISABLE_BACKGROUND_TASKS_ENV];
  else process.env[DISABLE_BACKGROUND_TASKS_ENV] = SAVE.ami;
  if (SAVE.cc === undefined) delete process.env[DISABLE_BACKGROUND_TASKS_ENV_COMPAT];
  else process.env[DISABLE_BACKGROUND_TASKS_ENV_COMPAT] = SAVE.cc;
}

describe('U030 isBackgroundTasksDisabled', () => {
  afterEach(restoreEnv);

  it('is false (background enabled) by default', () => {
    assert.equal(isBackgroundTasksDisabled({} as NodeJS.ProcessEnv), false);
  });

  it('is true when the AMI-native env var is truthy', () => {
    assert.equal(isBackgroundTasksDisabled({ [DISABLE_BACKGROUND_TASKS_ENV]: '1' } as any), true);
  });

  it('is true when the Claude Code-compatible env var is truthy', () => {
    assert.equal(
      isBackgroundTasksDisabled({ [DISABLE_BACKGROUND_TASKS_ENV_COMPAT]: 'true' } as any),
      true,
    );
  });

  it('treats an explicit falsey token as NOT disabled', () => {
    assert.equal(isBackgroundTasksDisabled({ [DISABLE_BACKGROUND_TASKS_ENV]: 'false' } as any), false);
    assert.equal(isBackgroundTasksDisabled({ [DISABLE_BACKGROUND_TASKS_ENV]: '0' } as any), false);
  });

  it('reads process.env by default', () => {
    process.env[DISABLE_BACKGROUND_TASKS_ENV] = 'yes';
    assert.equal(isBackgroundTasksDisabled(), true);
  });
});

describe('U030 buildBashInputSchema', () => {
  it('includes run_in_background when background tasks are enabled', () => {
    const schema = buildBashInputSchema(false);
    assert.ok('run_in_background' in schema.properties);
    // other fields still present
    assert.ok('command' in schema.properties);
    assert.ok('timeout' in schema.properties);
    assert.ok('description' in schema.properties);
    assert.ok('dangerouslyDisableSandbox' in schema.properties);
  });

  it('OMITS run_in_background when background tasks are disabled, keeping all other fields', () => {
    const schema = buildBashInputSchema(true);
    assert.ok(!('run_in_background' in schema.properties));
    assert.ok('command' in schema.properties);
    assert.ok('timeout' in schema.properties);
    assert.ok('description' in schema.properties);
    assert.ok('dangerouslyDisableSandbox' in schema.properties);
    assert.deepEqual(schema.required, ['command']);
  });
});

describe('U030 buildBashDescription', () => {
  it('mentions run_in_background guidance when enabled', () => {
    const d = buildBashDescription(false);
    assert.match(d, /run_in_background/);
    assert.match(d, /Do not sleep between commands\./);
  });

  it('drops run_in_background guidance when disabled but keeps the rest', () => {
    const d = buildBashDescription(true);
    assert.ok(!/run_in_background/.test(d));
    assert.match(d, /Do not sleep between commands\./);
    assert.match(d, /Execute a bash command/);
  });
});

describe('U030 bashTool getters reflect the env knob at advertise time', () => {
  afterEach(restoreEnv);

  it('advertises run_in_background by default', () => {
    delete process.env[DISABLE_BACKGROUND_TASKS_ENV];
    delete process.env[DISABLE_BACKGROUND_TASKS_ENV_COMPAT];
    assert.ok('run_in_background' in bashTool.inputSchema.properties);
    assert.match(bashTool.description, /run_in_background/);
  });

  it('drops run_in_background from the live schema + description when disabled', () => {
    process.env[DISABLE_BACKGROUND_TASKS_ENV] = '1';
    assert.ok(!('run_in_background' in bashTool.inputSchema.properties));
    assert.ok(!/run_in_background/.test(bashTool.description));
  });
});

// ---------------------------------------------------------------------------
// E2E: execute forces foreground when background is disabled, even if the model
// still passes run_in_background: true. Discriminating fixture — a processManager
// mock is wired so the background branch WOULD be taken if the knob were ignored.
// ---------------------------------------------------------------------------
describe('U030 execute — foreground is forced when background tasks are disabled', () => {
  afterEach(restoreEnv);

  function pmMock() {
    const calls: string[] = [];
    const pm = {
      spawn: (cmd: string, _opts: any) => {
        calls.push(cmd);
        return 'bg-task-xyz';
      },
      get: () => ({ pid: 4242 }),
    };
    return { pm, calls };
  }

  // Distinct commands per test so the read-only result cache from one test can
  // never short-circuit the other before the background branch is reached.
  it('runs foreground (no spawn, real output) when the knob is set', async () => {
    process.env[DISABLE_BACKGROUND_TASKS_ENV] = '1';
    const { pm, calls } = pmMock();
    const result = await bashTool.execute(
      { command: 'echo u030-disabled-fg', run_in_background: true },
      ctx({ processManager: pm } as any),
    );
    assert.equal(result.isError, false, result.output);
    assert.ok(!result.output.includes('[background: true]'), `expected foreground output, got: ${result.output}`);
    assert.match(result.output, /u030-disabled-fg/);
    assert.equal(calls.length, 0, 'processManager.spawn must NOT be called when background is disabled');
  });

  it('REVERT-GUARD: with the knob unset, a background-requested command goes to the background manager', async () => {
    delete process.env[DISABLE_BACKGROUND_TASKS_ENV];
    delete process.env[DISABLE_BACKGROUND_TASKS_ENV_COMPAT];
    assert.equal(isBackgroundTasksDisabled(), false);
    const { pm, calls } = pmMock();
    const result = await bashTool.execute(
      { command: 'echo u030-enabled-bg', run_in_background: true },
      ctx({ processManager: pm } as any),
    );
    assert.equal(result.isError, false, result.output);
    assert.match(result.output, /\[background: true\]/);
    assert.match(result.output, /bg-task-xyz/);
    assert.equal(calls.length, 1, 'background branch must spawn exactly once when enabled');
  });
});
