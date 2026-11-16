/**
 * U117 parity tests for the sandbox violation store:
 *  - SandboxViolationStore unit behavior (record/total/recent/subscribe/clear/cap)
 *  - ignoreViolations per-command suppression (Claude's sandbox.ignoreViolations)
 *  - producer wiring: annotateStderrWithSandboxFailures records detected violations
 *  - E2E: a container-mode bashTool failure records a violation into the store
 *
 * All tests mock the shell seams — no real container is ever started.
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  SandboxViolationStore,
  sandboxViolationStore,
  SANDBOX_VIOLATION_CAP,
  type SandboxViolationEvent,
} from '../src/tools/sandbox-violations';
import {
  annotateStderrWithSandboxFailures,
  _resetDockerAvailableCache,
} from '../src/tools/docker-sandbox';
import { shellHooks } from '../src/utils/shell';
import { bashTool, _bashHooks } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

describe('SandboxViolationStore — unit', () => {
  it('records a violation, stamps a timestamp, and increments the total', () => {
    const store = new SandboxViolationStore();
    const before = store.getTotalCount();
    const stored = store.record({ kind: 'network', command: 'curl x', detail: 'blocked' });
    assert.equal(stored, true);
    assert.equal(store.getTotalCount(), before + 1);
    const all = store.getAll();
    assert.equal(all.length, 1);
    assert.equal(all[0].kind, 'network');
    assert.equal(all[0].command, 'curl x');
    assert.equal(all[0].detail, 'blocked');
    assert.equal(typeof all[0].timestamp, 'number');
  });

  it('getRecent returns the newest-last tail and clamps to <=0 as empty', () => {
    const store = new SandboxViolationStore();
    for (let i = 0; i < 15; i++) store.record({ kind: 'filesystem', command: `c${i}`, detail: `d${i}` });
    const recent = store.getRecent(10);
    assert.equal(recent.length, 10);
    assert.equal(recent[recent.length - 1].command, 'c14');
    assert.equal(recent[0].command, 'c5');
    assert.deepEqual(store.getRecent(0), []);
    assert.deepEqual(store.getRecent(-3), []);
  });

  it('evicts the oldest past the cap but keeps the running total intact', () => {
    const store = new SandboxViolationStore(3);
    for (let i = 0; i < 5; i++) store.record({ kind: 'network', command: `c${i}`, detail: `d${i}` });
    assert.equal(store.getAll().length, 3, 'buffer stays bounded at the cap');
    assert.equal(store.getTotalCount(), 5, 'total survives eviction');
    assert.equal(store.getAll()[0].command, 'c2', 'oldest dropped');
  });

  it('falls back to the default cap for a non-positive constructor arg', () => {
    const store = new SandboxViolationStore(0);
    for (let i = 0; i < SANDBOX_VIOLATION_CAP + 5; i++) {
      store.record({ kind: 'network', command: 'c', detail: `d${i}` });
    }
    assert.equal(store.getAll().length, SANDBOX_VIOLATION_CAP);
  });

  it('notifies subscribers on record and clear, and unsubscribe stops delivery', () => {
    const store = new SandboxViolationStore();
    const seen: number[] = [];
    const unsub = store.subscribe((v: readonly SandboxViolationEvent[]) => seen.push(v.length));
    store.record({ kind: 'network', command: 'a', detail: 'd' });
    store.record({ kind: 'network', command: 'b', detail: 'd' });
    assert.deepEqual(seen, [1, 2]);
    store.clear();
    assert.deepEqual(seen, [1, 2, 0], 'clear fires listeners with the emptied list');
    assert.equal(store.getTotalCount(), 0);
    unsub();
    store.record({ kind: 'network', command: 'c', detail: 'd' });
    assert.deepEqual(seen, [1, 2, 0], 'no delivery after unsubscribe');
  });

  it('suppresses a violation whose detail matches the per-command ignore list', () => {
    const store = new SandboxViolationStore();
    store.setIgnoreViolations({ 'npm install': ['network blocked'] });
    const suppressed = store.record({ kind: 'network', command: 'npm install', detail: 'network blocked for host' });
    assert.equal(suppressed, false, 'matched violation is not stored');
    assert.equal(store.getTotalCount(), 0);
    // A different command is NOT covered by that entry.
    const stored = store.record({ kind: 'network', command: 'curl x', detail: 'network blocked for host' });
    assert.equal(stored, true);
    assert.equal(store.getTotalCount(), 1);
  });

  it("honors a '*' wildcard entry and a '*' wildcard command key", () => {
    const starEntry = new SandboxViolationStore();
    starEntry.setIgnoreViolations({ 'git push': ['*'] });
    assert.equal(starEntry.record({ kind: 'network', command: 'git push', detail: 'anything at all' }), false);

    const starCmd = new SandboxViolationStore();
    starCmd.setIgnoreViolations({ '*': ['blocked'] });
    assert.equal(starCmd.record({ kind: 'network', command: 'whatever', detail: 'was blocked' }), false);
    assert.equal(starCmd.record({ kind: 'network', command: 'whatever', detail: 'unrelated' }), true);
  });

  it('clearing the ignore map re-enables recording', () => {
    const store = new SandboxViolationStore();
    store.setIgnoreViolations({ 'c': ['x'] });
    assert.equal(store.record({ kind: 'network', command: 'c', detail: 'xy' }), false);
    store.setIgnoreViolations(undefined);
    assert.equal(store.record({ kind: 'network', command: 'c', detail: 'xy' }), true);
  });
});

describe('annotateStderrWithSandboxFailures — records into the shared store', () => {
  beforeEach(() => {
    sandboxViolationStore.clear();
    sandboxViolationStore.setIgnoreViolations(undefined);
  });
  afterEach(() => {
    sandboxViolationStore.clear();
    sandboxViolationStore.setIgnoreViolations(undefined);
  });

  it('records a network violation when network is off and stderr shows a DNS failure', () => {
    const out = annotateStderrWithSandboxFailures(
      'curl https://x',
      'curl: (6) Could not resolve host: x',
      { allowNetwork: false, projectDir: '/proj' },
    );
    assert.match(out, /network is disabled/);
    assert.equal(sandboxViolationStore.getTotalCount(), 1);
    assert.equal(sandboxViolationStore.getAll()[0].kind, 'network');
  });

  it('records a filesystem violation when an unmounted path is reported missing', () => {
    const out = annotateStderrWithSandboxFailures(
      'cat /etc/shadow',
      'cat: /etc/shadow: No such file or directory',
      { allowNetwork: true, projectDir: '/proj' },
    );
    assert.match(out, /only the project directory/);
    assert.equal(sandboxViolationStore.getTotalCount(), 1);
    assert.equal(sandboxViolationStore.getAll()[0].kind, 'filesystem');
  });

  it('does not record when nothing matches, and returns stderr unchanged', () => {
    const out = annotateStderrWithSandboxFailures('ls', 'ls: cannot access', { allowNetwork: true, projectDir: '/proj' });
    assert.equal(out, 'ls: cannot access');
    assert.equal(sandboxViolationStore.getTotalCount(), 0);
  });

  it('suppresses recording for a command/detail matched by ignoreViolations', () => {
    const out = annotateStderrWithSandboxFailures(
      'curl https://x',
      'curl: (6) Could not resolve host: x',
      { allowNetwork: false, projectDir: '/proj', ignoreViolations: { 'curl https://x': ['outbound network'] } },
    );
    // The model-facing annotation is still produced…
    assert.match(out, /network is disabled/);
    // …but the violation is NOT recorded.
    assert.equal(sandboxViolationStore.getTotalCount(), 0);
  });
});

describe('bashTool container-mode — E2E records a sandbox violation', () => {
  let tmpProject: string;
  beforeEach(() => {
    _resetDockerAvailableCache();
    sandboxViolationStore.clear();
    sandboxViolationStore.setIgnoreViolations(undefined);
    tmpProject = fs.mkdtempSync(path.join(os.tmpdir(), 'si-sbviol-'));
  });
  afterEach(() => {
    mock.restoreAll();
    _resetDockerAvailableCache();
    sandboxViolationStore.clear();
    sandboxViolationStore.setIgnoreViolations(undefined);
    fs.rmSync(tmpProject, { recursive: true, force: true });
  });

  function ctx(overrides: Partial<ToolContext> = {}): ToolContext {
    return {
      cwd: tmpProject,
      abortSignal: new AbortController().signal,
      detachedMode: false,
      ...overrides,
    } as unknown as ToolContext;
  }

  it('records a network violation for a blocked container command', async () => {
    mock.method(shellHooks, 'execSync', () => Buffer.from('29.9.3')); // docker available
    mock.method(_bashHooks, 'execCommand', async () => ({
      stdout: '',
      stderr: 'curl: (6) Could not resolve host: example.com',
      exitCode: 6,
    }));
    assert.equal(sandboxViolationStore.getTotalCount(), 0);
    const result = await bashTool.execute(
      { command: 'curl https://example.com' },
      ctx({ sandbox: { mode: 'container', projectDir: tmpProject, allowNetwork: false } }),
    );
    assert.equal(result.isError, true);
    assert.equal(sandboxViolationStore.getTotalCount(), 1, 'the blocked op was recorded');
    assert.equal(sandboxViolationStore.getRecent(1)[0].kind, 'network');
  });

  it('does NOT record when the operator allowlisted the violation for that command', async () => {
    mock.method(shellHooks, 'execSync', () => Buffer.from('29.9.3'));
    mock.method(_bashHooks, 'execCommand', async () => ({
      stdout: '',
      stderr: 'curl: (6) Could not resolve host: example.com',
      exitCode: 6,
    }));
    await bashTool.execute(
      { command: 'curl https://example.com' },
      ctx({
        sandbox: {
          mode: 'container',
          projectDir: tmpProject,
          allowNetwork: false,
          ignoreViolations: { 'curl https://example.com': ['outbound network'] },
        },
      }),
    );
    assert.equal(sandboxViolationStore.getTotalCount(), 0, 'allowlisted violation suppressed');
  });
});
