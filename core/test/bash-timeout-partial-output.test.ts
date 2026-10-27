/**
 * Red-green reproducer for: a bash command that reaches its timeout and is
 * auto-moved to the background must surface the stdout/stderr captured before
 * the timeout (the "output so far"), not discard it.
 *
 * The timed-out exec is driven deterministically through the `_bashHooks`
 * execCommand DI seam (node:test `mock.method`, never a frozen-namespace
 * monkeypatch), returning exitCode: null with partial output. A fake
 * processManager drives the auto-background branch.
 *
 * RED (pre-fix): the auto-bg branch returns only
 *   "Command timed out ... Auto-moved to background as task X. Use task_output..."
 * so the "Step 1 complete" / stderr assertions fail, and
 * `formatTimeoutBackgroundOutput` does not exist.
 * GREEN (post-fix): the partial output is surfaced under "[output so far]".
 */

import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { bashTool, _bashHooks, formatTimeoutBackgroundOutput } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

describe('bashTool – timeout auto-background surfaces partial output', () => {
  afterEach(() => mock.restoreAll());

  it('includes the output captured before the timeout in the auto-bg result', async () => {
    mock.method(_bashHooks, 'execCommand', async () => ({
      stdout: 'Building project...\nStep 1 complete',
      stderr: 'warning: deprecated flag used',
      exitCode: null,
    }));

    let spawnedDesc = '';
    const pm = {
      spawn: (_cmd: string, opts: any) => { spawnedDesc = opts.description; return 'task-xyz'; },
      get: () => ({ pid: 999 }),
    };

    const res = await bashTool.execute(
      { command: 'long-running-build', timeout: 50 },
      ctx({ processManager: pm } as any),
    );

    // Still-running notice is preserved.
    assert.ok(res.output.includes('task-xyz'), res.output);
    assert.match(res.output, /still running in the background/i);
    assert.ok(spawnedDesc.startsWith('[auto-bg]'), `expected auto-bg spawn, got: ${spawnedDesc}`);

    // DISCRIMINATOR: the partial stdout/stderr captured before the timeout is
    // surfaced. Pre-fix this output was discarded.
    assert.ok(res.output.includes('[output so far]'), `expected output-so-far header, got: ${res.output}`);
    assert.ok(res.output.includes('Step 1 complete'), `expected partial stdout, got: ${res.output}`);
    assert.ok(res.output.includes('warning: deprecated flag used'), `expected partial stderr, got: ${res.output}`);
  });

  it('shows only the notice when nothing was captured before the timeout', async () => {
    mock.method(_bashHooks, 'execCommand', async () => ({
      stdout: '', stderr: '', exitCode: null,
    }));
    const pm = { spawn: () => 'task-empty', get: () => ({ pid: 1 }) };

    const res = await bashTool.execute(
      { command: 'silent-task', timeout: 50 },
      ctx({ processManager: pm } as any),
    );

    assert.ok(res.output.includes('task-empty'), res.output);
    assert.ok(!res.output.includes('[output so far]'), `no partial output expected, got: ${res.output}`);
  });
});

describe('formatTimeoutBackgroundOutput (pure helper)', () => {
  it('appends partial stdout/stderr under an [output so far] header', () => {
    const out = formatTimeoutBackgroundOutput('partial out', 'partial err', 1000, 't1');
    assert.match(out, /Auto-moved to background as task t1/);
    assert.match(out, /still running in the background/i);
    assert.ok(out.includes('[output so far]'));
    assert.ok(out.includes('partial out'));
    assert.ok(out.includes('partial err'));
  });

  it('omits the header when there is no captured output', () => {
    const out = formatTimeoutBackgroundOutput('', '', 2000, 't2');
    assert.match(out, /Auto-moved to background as task t2/);
    assert.ok(!out.includes('[output so far]'));
  });

  it('surfaces real output that is literally the "(no output)" sentinel string', () => {
    const out = formatTimeoutBackgroundOutput('(no output)', '', 3000, 't3');
    assert.ok(out.includes('[output so far]'), 'sentinel-looking real output must still be surfaced');
    assert.ok(out.includes('(no output)'));
  });

  it('omits the header for whitespace-only output', () => {
    const out = formatTimeoutBackgroundOutput('   \n\t', '', 3000, 't4');
    assert.ok(!out.includes('[output so far]'));
  });
});
