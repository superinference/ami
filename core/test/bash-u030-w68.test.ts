/**
 * U030 (wave 68) — bash.ts parity gaps.
 *
 * Covers, each as a red-green discriminator:
 *  - countCharInString (allocation-free char count) + capSuite dropped-line marker
 *  - AMI_DISABLE_SANDBOX_ESCAPE: forbid dangerouslyDisableSandbox (schema + runtime)
 *  - AMI_DISABLE_SANDBOX / sandbox.enabled:false: global host-sandbox off switch
 *  - preSpawnError discriminator (cwd-missing / spawn-failure) in result metadata
 *  - BASH_ASSISTANT_BLOCKING_BUDGET_MS: detached blocking budget clamps foreground timeout
 *  - assistantAutoBackgrounded metadata + formatTimeoutBackgroundOutput coaching/outputPath
 */

import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';

import { countCharInString } from '../src/utils/string';
import { compactTestLog } from '../src/tools/test-log';
import {
  isHostSandboxDisabled,
  isSandboxEscapeDisabled,
  DISABLE_HOST_SANDBOX_ENV,
  DISABLE_SANDBOX_ESCAPE_ENV,
} from '../src/tools/tool-gates';
import {
  bashTool,
  _bashHooks,
  buildBashInputSchema,
  resolveAssistantBlockingBudget,
  formatTimeoutBackgroundOutput,
} from '../src/tools/bash';
import { SPAWN_FAILURE_EXIT_CODE } from '../src/utils/shell';
import type { ToolContext } from '../src/types';

function baseCtx(overrides: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

function restoreEnv(name: string, prev: string | undefined): void {
  if (prev === undefined) delete process.env[name];
  else process.env[name] = prev;
}

// ---------------------------------------------------------------------------
describe('countCharInString', () => {
  it('counts single-char occurrences without allocating a split array', () => {
    assert.equal(countCharInString('a,b,c', ','), 2);
    assert.equal(countCharInString('no-separators', '\n'), 0);
    assert.equal(countCharInString('\n\n\n', '\n'), 3);
  });

  it('honors the start offset (counts only the tail past a boundary)', () => {
    // "abc\ndef\nghi" — only the newline(s) at/after index 4 are counted.
    assert.equal(countCharInString('abc\ndef\nghi', '\n', 4), 1);
    assert.equal(countCharInString('abc\ndef\nghi', '\n', 0), 2);
  });

  it('is structurally typed so a Buffer works', () => {
    assert.equal(countCharInString(Buffer.from('x\ny\nz'), '\n'), 2);
  });
});

describe('capSuite dropped-line marker (compactTestLog)', () => {
  it('quantifies how many lines were dropped past the cap', () => {
    const header = Array.from({ length: 8 }, (_, i) => `{"Action":"fail","Test":"T${i}"}`);
    const bulk = Array.from(
      { length: 3000 },
      (_, i) => `{"Action":"fail","Test":"Big${i}","Output":"FAIL ${i} ${'x'.repeat(40)}"}`,
    );
    const out = compactTestLog([...header, ...bulk].join('\n'));
    const m = out.match(/\.\.\.\[(\d+) lines of failing output truncated\]/);
    assert.ok(m, `expected a quantified truncation marker, got tail: ${out.slice(-120)}`);
    // The reported count must be a positive number (real dropped lines).
    assert.ok(Number(m![1]) > 0, `expected a positive dropped-line count, got ${m![1]}`);
  });
});

// ---------------------------------------------------------------------------
describe('tool-gates sandbox knobs', () => {
  it('isHostSandboxDisabled reads the env knob (default off)', () => {
    assert.equal(isHostSandboxDisabled({} as NodeJS.ProcessEnv), false);
    assert.equal(isHostSandboxDisabled({ [DISABLE_HOST_SANDBOX_ENV]: '1' } as NodeJS.ProcessEnv), true);
    assert.equal(isHostSandboxDisabled({ [DISABLE_HOST_SANDBOX_ENV]: 'false' } as NodeJS.ProcessEnv), false);
  });

  it('isSandboxEscapeDisabled reads the env knob (default off)', () => {
    assert.equal(isSandboxEscapeDisabled({} as NodeJS.ProcessEnv), false);
    assert.equal(isSandboxEscapeDisabled({ [DISABLE_SANDBOX_ESCAPE_ENV]: 'yes' } as NodeJS.ProcessEnv), true);
  });
});

describe('buildBashInputSchema — dangerouslyDisableSandbox omission', () => {
  it('advertises dangerouslyDisableSandbox by default', () => {
    const schema = buildBashInputSchema(false, false);
    assert.ok('dangerouslyDisableSandbox' in schema.properties);
  });

  it('omits dangerouslyDisableSandbox when the escape hatch is forbidden by policy', () => {
    const schema = buildBashInputSchema(false, true);
    assert.ok(!('dangerouslyDisableSandbox' in schema.properties));
    // command stays required/present — only the escape hatch is dropped.
    assert.ok('command' in schema.properties);
  });
});

describe('bashTool.inputSchema getter honors AMI_DISABLE_SANDBOX_ESCAPE', () => {
  afterEach(() => restoreEnv(DISABLE_SANDBOX_ESCAPE_ENV, undefined));

  it('drops the parameter from the live schema when the env knob is set', () => {
    const prev = process.env[DISABLE_SANDBOX_ESCAPE_ENV];
    try {
      delete process.env[DISABLE_SANDBOX_ESCAPE_ENV];
      assert.ok('dangerouslyDisableSandbox' in bashTool.inputSchema.properties);
      process.env[DISABLE_SANDBOX_ESCAPE_ENV] = '1';
      assert.ok(!('dangerouslyDisableSandbox' in bashTool.inputSchema.properties));
    } finally {
      restoreEnv(DISABLE_SANDBOX_ESCAPE_ENV, prev);
    }
  });
});

// ---------------------------------------------------------------------------
describe('bashTool.execute — sandbox escape / host-sandbox runtime enforcement', () => {
  const TRIGGER = 'curl https://example.com';

  afterEach(() => mock.restoreAll());

  async function captureExecuted(ctx: ToolContext, input: Record<string, unknown> = {}): Promise<string> {
    let captured = '';
    mock.method(_bashHooks, 'execCommand', async (cmd: string) => {
      captured = cmd;
      return { stdout: '', stderr: '', exitCode: 0 };
    });
    const result = await bashTool.execute({ command: TRIGGER, ...input }, ctx);
    assert.equal(result.isError, false, `execute should succeed: ${JSON.stringify(result)}`);
    return captured;
  }

  it('sanity: the trigger is sandboxed by default', async () => {
    const executed = await captureExecuted(baseCtx());
    assert.match(executed, /unshare|ulimit/, `expected a sandbox wrapper, got: ${executed}`);
  });

  it('AMI_DISABLE_SANDBOX_ESCAPE ignores dangerouslyDisableSandbox (stays sandboxed)', async () => {
    const prev = process.env[DISABLE_SANDBOX_ESCAPE_ENV];
    try {
      // Red-green: WITHOUT the knob, dangerouslyDisableSandbox runs raw.
      delete process.env[DISABLE_SANDBOX_ESCAPE_ENV];
      const unlocked = await captureExecuted(baseCtx(), { dangerouslyDisableSandbox: true });
      assert.equal(unlocked, TRIGGER, 'unlocked escape hatch must run raw');

      process.env[DISABLE_SANDBOX_ESCAPE_ENV] = '1';
      const locked = await captureExecuted(baseCtx(), { dangerouslyDisableSandbox: true });
      assert.match(locked, /unshare|ulimit/, `escape hatch must be ignored when forbidden, got: ${locked}`);
    } finally {
      restoreEnv(DISABLE_SANDBOX_ESCAPE_ENV, prev);
    }
  });

  it('AMI_DISABLE_SANDBOX disables the host auto-sandbox globally (runs raw)', async () => {
    const prev = process.env[DISABLE_HOST_SANDBOX_ENV];
    try {
      process.env[DISABLE_HOST_SANDBOX_ENV] = '1';
      const executed = await captureExecuted(baseCtx());
      assert.equal(executed, TRIGGER, `host sandbox off-switch must run raw, got: ${executed}`);
    } finally {
      restoreEnv(DISABLE_HOST_SANDBOX_ENV, prev);
    }
  });

  it('sandbox.enabled:false disables the host auto-sandbox for that config (runs raw)', async () => {
    const executed = await captureExecuted(baseCtx({ sandbox: { mode: 'off', enabled: false } } as Partial<ToolContext>));
    assert.equal(executed, TRIGGER, `sandbox.enabled:false must run raw, got: ${executed}`);
  });
});

// ---------------------------------------------------------------------------
describe('bashTool.execute — preSpawnError discriminator', () => {
  afterEach(() => mock.restoreAll());

  it('tags a deleted/unresolvable working directory as cwd-missing', async () => {
    mock.method(_bashHooks, 'realpathSync', () => {
      const e = new Error('ENOENT') as NodeJS.ErrnoException;
      e.code = 'ENOENT';
      throw e;
    });
    const res = await bashTool.execute({ command: 'echo hi' }, baseCtx({ cwd: '/nonexistent-cwd-xyz' }));
    assert.equal(res.isError, true);
    assert.equal((res.metadata as Record<string, unknown>)?.preSpawnError, 'cwd-missing');
  });

  it('tags a shell spawn failure (exit 126) as spawn-failure', async () => {
    mock.method(_bashHooks, 'execCommand', async () => ({
      stdout: '',
      stderr: 'spawn failed',
      exitCode: SPAWN_FAILURE_EXIT_CODE,
    }));
    const res = await bashTool.execute({ command: 'echo hi' }, baseCtx());
    assert.equal((res.metadata as Record<string, unknown>)?.preSpawnError, 'spawn-failure');
  });

  it('does NOT tag a normal non-zero exit as a pre-spawn error', async () => {
    mock.method(_bashHooks, 'execCommand', async () => ({ stdout: '', stderr: 'boom', exitCode: 1 }));
    const res = await bashTool.execute({ command: 'echo hi' }, baseCtx());
    assert.equal((res.metadata as Record<string, unknown>)?.preSpawnError, undefined);
  });
});

// ---------------------------------------------------------------------------
describe('assistant blocking budget', () => {
  afterEach(() => mock.restoreAll());

  it('resolveAssistantBlockingBudget reads BASH_ASSISTANT_BLOCKING_BUDGET_MS', () => {
    const prev = process.env.BASH_ASSISTANT_BLOCKING_BUDGET_MS;
    try {
      delete process.env.BASH_ASSISTANT_BLOCKING_BUDGET_MS;
      assert.equal(resolveAssistantBlockingBudget(), undefined);
      process.env.BASH_ASSISTANT_BLOCKING_BUDGET_MS = '15000';
      assert.equal(resolveAssistantBlockingBudget(), 15000);
    } finally {
      restoreEnv('BASH_ASSISTANT_BLOCKING_BUDGET_MS', prev);
    }
  });

  async function captureTimeout(cmd: string, ctx: ToolContext, input: Record<string, unknown>): Promise<number | undefined> {
    let captured: number | undefined;
    mock.method(_bashHooks, 'execCommand', async (_cmd: string, opts: { timeout?: number }) => {
      captured = opts.timeout;
      return { stdout: '', stderr: '', exitCode: 0 };
    });
    await bashTool.execute({ command: cmd, ...input }, ctx);
    return captured;
  }

  it('clamps the foreground timeout to the budget in detached mode', async () => {
    const prev = process.env.BASH_ASSISTANT_BLOCKING_BUDGET_MS;
    try {
      process.env.BASH_ASSISTANT_BLOCKING_BUDGET_MS = '5000';
      // Distinct commands avoid the read-only result cache returning early.
      const detached = await captureTimeout('printf detached', baseCtx({ detachedMode: true }), { timeout: 120000 });
      assert.equal(detached, 5000, 'detached run must be clamped to the budget');

      // Red-green: a NON-detached run ignores the budget (keeps its full timeout).
      const interactive = await captureTimeout('printf interactive', baseCtx(), { timeout: 120000 });
      assert.equal(interactive, 120000, 'interactive run must keep its full timeout');
    } finally {
      restoreEnv('BASH_ASSISTANT_BLOCKING_BUDGET_MS', prev);
    }
  });
});

describe('assistantAutoBackgrounded metadata', () => {
  afterEach(() => mock.restoreAll());

  it('flags an auto-backgrounded timeout in detached mode', async () => {
    mock.method(_bashHooks, 'execCommand', async () => ({ stdout: '', stderr: '', exitCode: null }));
    const pm = { spawn: () => 'task-dd', get: () => ({ pid: 1, outputPath: '/tmp/task-dd.output' }) };
    const res = await bashTool.execute(
      { command: 'long-task' },
      baseCtx({ detachedMode: true, processManager: pm } as Partial<ToolContext>),
    );
    const md = res.metadata as Record<string, unknown>;
    assert.equal(md.backgrounded, true);
    assert.equal(md.assistantAutoBackgrounded, true);
    assert.equal(md.taskId, 'task-dd');
    // Coaching + output-path wording present in detached mode.
    assert.match(res.output, /delegate long-running work to a subagent/);
    assert.match(res.output, /\/tmp\/task-dd\.output/);
  });

  it('does NOT flag assistantAutoBackgrounded for an interactive auto-background', async () => {
    mock.method(_bashHooks, 'execCommand', async () => ({ stdout: '', stderr: '', exitCode: null }));
    const pm = { spawn: () => 'task-ii', get: () => ({ pid: 1, outputPath: '/tmp/task-ii.output' }) };
    const res = await bashTool.execute({ command: 'long-task' }, baseCtx({ processManager: pm } as Partial<ToolContext>));
    const md = res.metadata as Record<string, unknown>;
    assert.equal(md.backgrounded, true);
    assert.equal(md.assistantAutoBackgrounded, undefined);
    assert.doesNotMatch(res.output, /delegate long-running work to a subagent/);
  });
});

describe('formatTimeoutBackgroundOutput — coaching / output path', () => {
  it('appends the output path and assistant coaching when requested', () => {
    const out = formatTimeoutBackgroundOutput('', '', 15000, 't9', {
      detachedMode: true,
      outputPath: '/tmp/t9.output',
    });
    assert.match(out, /Output is being written to: \/tmp\/t9\.output\./);
    assert.match(out, /In assistant mode, delegate long-running work/);
  });

  it('stays unchanged (no coaching / path) when no opts are passed', () => {
    const out = formatTimeoutBackgroundOutput('', '', 15000, 't10');
    assert.doesNotMatch(out, /Output is being written to/);
    assert.doesNotMatch(out, /In assistant mode/);
    assert.match(out, /Auto-moved to background as task t10/);
  });
});
