/**
 * Unit U030 — exclude `sleep` from auto-backgrounding on timeout.
 *
 * Gap: on a null-exitCode timeout AMI unconditionally auto-moved the command to
 * the background via processManager.spawn for ANY command — including a bare
 * `sleep`, which just keeps a pointless process alive as a background task.
 * Claude gates auto-backgrounding through `isAutobackgroundingAllowed`
 * (DISALLOWED_AUTO_BACKGROUND_COMMANDS = ['sleep']) so sleep is never
 * auto-backgrounded; it surfaces a plain timeout error instead.
 *
 * The timed-out exec is driven deterministically through the `_bashHooks`
 * execCommand DI seam (exitCode: null). A fake processManager records whether
 * the auto-background branch spawned anything.
 *
 * RED (pre-fix): `sleep` would be auto-backgrounded (pm.spawn called, result
 * NOT an error) exactly like a normal long-running command, so the
 * `spawnCalled === false` / `isError === true` assertions fail.
 * GREEN (post-fix): `sleep` falls through to the plain timeout error and never
 * spawns a background task, while a normal command still auto-backgrounds.
 */

import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { bashTool, isAutobackgroundingAllowed, _bashHooks } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

describe('U030 isAutobackgroundingAllowed (pure helper)', () => {
  it('disallows a bare sleep', () => {
    assert.equal(isAutobackgroundingAllowed('sleep 600'), false);
    assert.equal(isAutobackgroundingAllowed('sleep'), false);
    assert.equal(isAutobackgroundingAllowed('  sleep   30  '), false);
  });

  it('disallows a path-qualified / suffixed sleep via normalizeBaseCommand', () => {
    assert.equal(isAutobackgroundingAllowed('/bin/sleep 600'), false);
    assert.equal(isAutobackgroundingAllowed('sleep.exe 600'), false);
  });

  it('allows normal long-running commands', () => {
    assert.equal(isAutobackgroundingAllowed('npm run build'), true);
    assert.equal(isAutobackgroundingAllowed('node server.js'), true);
    assert.equal(isAutobackgroundingAllowed('make test'), true);
  });

  it('allows a compound command whose LEADING token is not sleep', () => {
    // Claude gates on the leading token only — `build && sleep 600` still
    // auto-backgrounds because the first command is `build`, not `sleep`.
    assert.equal(isAutobackgroundingAllowed('build && sleep 600'), true);
  });

  it('allows an empty / whitespace command (nothing to disallow)', () => {
    assert.equal(isAutobackgroundingAllowed(''), true);
    assert.equal(isAutobackgroundingAllowed('   '), true);
  });
});

describe('bashTool – sleep is NOT auto-backgrounded on timeout', () => {
  afterEach(() => mock.restoreAll());

  it('surfaces a plain timeout error and never spawns a background task for sleep', async () => {
    mock.method(_bashHooks, 'execCommand', async () => ({
      stdout: '', stderr: '', exitCode: null,
    }));

    let spawnCalled = false;
    const pm = {
      spawn: () => { spawnCalled = true; return 'task-sleep'; },
      get: () => ({ pid: 1 }),
    };

    // A custom timeout makes detectBlockedSleep return null (so the command is
    // allowed to run and reach the timeout branch), isolating the auto-bg gate.
    const res = await bashTool.execute(
      { command: 'sleep 600', timeout: 50 },
      ctx({ processManager: pm } as any),
    );

    assert.equal(spawnCalled, false, 'sleep must not be auto-backgrounded');
    assert.equal(res.isError, true, 'sleep timeout must be a plain error result');
    assert.match(res.output, /timed out/i);
    assert.ok(!res.output.includes('task-sleep'), 'no background task id should be surfaced');
  });

  it('still auto-backgrounds a normal command that times out (control)', async () => {
    mock.method(_bashHooks, 'execCommand', async () => ({
      stdout: 'partial', stderr: '', exitCode: null,
    }));

    let spawnedDesc = '';
    const pm = {
      spawn: (_cmd: string, opts: any) => { spawnedDesc = opts.description; return 'task-build'; },
      get: () => ({ pid: 2 }),
    };

    const res = await bashTool.execute(
      { command: 'long-running-build', timeout: 50 },
      ctx({ processManager: pm } as any),
    );

    assert.ok(spawnedDesc.startsWith('[auto-bg]'), `expected auto-bg spawn, got: ${spawnedDesc}`);
    assert.ok(res.output.includes('task-build'), res.output);
    assert.notEqual(res.isError, true);
  });
});
