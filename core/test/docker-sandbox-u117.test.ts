/**
 * U117 parity tests for the Docker sandbox backend:
 *  - cause-specific availability diagnostics (getDockerAvailability + describe)
 *  - cancellation-aware / overridable-timeout probe
 *  - annotateStderrWithSandboxFailures (network-off / unmounted-path hints)
 *  - TMPDIR pinned into the container tmpfs by wrapWithDocker
 *
 * All tests mock the shell seams — no real container is ever started.
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  getDockerAvailability,
  isDockerAvailable,
  describeDockerUnavailable,
  annotateStderrWithSandboxFailures,
  wrapWithDocker,
  _resetDockerAvailableCache,
  DOCKER_PROBE_TIMEOUT_MS,
  DOCKER_SANDBOX_TMPDIR,
} from '../src/tools/docker-sandbox';
import { shellHooks } from '../src/utils/shell';
import { bashTool, _bashHooks } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

let tmpProject: string;

beforeEach(() => {
  _resetDockerAvailableCache();
  tmpProject = fs.mkdtempSync(path.join(os.tmpdir(), 'si-docker-u117-'));
});

afterEach(() => {
  mock.restoreAll();
  _resetDockerAvailableCache();
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

describe('getDockerAvailability — cause classification', () => {
  it('reports available with no cause when the daemon answers', () => {
    mock.method(shellHooks, 'execSync', () => Buffer.from('29.9.3'));
    const a = getDockerAvailability();
    assert.deepEqual(a, { available: true });
  });

  it('classifies a missing CLI (ENOENT) as cli-missing', () => {
    mock.method(shellHooks, 'execSync', () => {
      throw Object.assign(new Error('spawnSync docker ENOENT'), { code: 'ENOENT' });
    });
    const a = getDockerAvailability();
    assert.equal(a.available, false);
    assert.equal(a.cause, 'cli-missing');
    assert.match(a.detail || '', /ENOENT/);
  });

  it('classifies a permission error (EACCES) as permission-denied', () => {
    mock.method(shellHooks, 'execSync', () => {
      throw Object.assign(new Error('connect EACCES /var/run/docker.sock'), { code: 'EACCES' });
    });
    assert.equal(getDockerAvailability().cause, 'permission-denied');
  });

  it('classifies a "permission denied" message with no code as permission-denied', () => {
    mock.method(shellHooks, 'execSync', () => {
      throw new Error('Got permission denied while trying to connect to the Docker daemon socket');
    });
    assert.equal(getDockerAvailability().cause, 'permission-denied');
  });

  it('classifies a timed-out probe (killed/SIGTERM) as daemon-slow', () => {
    mock.method(shellHooks, 'execSync', () => {
      throw Object.assign(new Error('Command timed out'), { killed: true, signal: 'SIGTERM' });
    });
    assert.equal(getDockerAvailability().cause, 'daemon-slow');
  });

  it('classifies an ETIMEDOUT code as daemon-slow', () => {
    mock.method(shellHooks, 'execSync', () => {
      throw Object.assign(new Error('timeout'), { code: 'ETIMEDOUT' });
    });
    assert.equal(getDockerAvailability().cause, 'daemon-slow');
  });

  it('classifies any other failure as daemon-unreachable', () => {
    mock.method(shellHooks, 'execSync', () => { throw new Error('Cannot connect to the Docker daemon'); });
    assert.equal(getDockerAvailability().cause, 'daemon-unreachable');
  });

  it('caches the classified cause within the negative-probe TTL', () => {
    let calls = 0;
    mock.method(shellHooks, 'execSync', () => { calls++; throw Object.assign(new Error('x ENOENT'), { code: 'ENOENT' }); });
    assert.equal(getDockerAvailability().cause, 'cli-missing');
    assert.equal(getDockerAvailability().cause, 'cli-missing'); // from cache
    assert.equal(calls, 1, 'negative cause is cached, not re-probed within the TTL');
  });
});

describe('getDockerAvailability — cancellation + overridable timeout', () => {
  it('fails fast with cause "aborted" and does NOT shell out when the signal is already aborted', () => {
    let calls = 0;
    mock.method(shellHooks, 'execSync', () => { calls++; return Buffer.from('ok'); });
    const ac = new AbortController();
    ac.abort();
    const a = getDockerAvailability({ signal: ac.signal });
    assert.equal(a.available, false);
    assert.equal(a.cause, 'aborted');
    assert.equal(calls, 0, 'an aborted probe must not block on a (possibly hung) docker version');
  });

  it('passes the default probe timeout when none is overridden', () => {
    let seen: number | undefined;
    mock.method(shellHooks, 'execSync', (_cmd: string, opts: { timeout?: number }) => {
      seen = opts.timeout;
      return Buffer.from('ok');
    });
    getDockerAvailability();
    assert.equal(seen, DOCKER_PROBE_TIMEOUT_MS);
  });

  it('honors an overridden probe timeout', () => {
    let seen: number | undefined;
    mock.method(shellHooks, 'execSync', (_cmd: string, opts: { timeout?: number }) => {
      seen = opts.timeout;
      return Buffer.from('ok');
    });
    getDockerAvailability({ timeoutMs: 1234 });
    assert.equal(seen, 1234);
  });
});

describe('isDockerAvailable — boolean accessor', () => {
  it('delegates to getDockerAvailability and returns a boolean', () => {
    mock.method(shellHooks, 'execSync', () => Buffer.from('ok'));
    assert.equal(isDockerAvailable(), true);
  });

  it('threads probe options through (already-aborted => false)', () => {
    let calls = 0;
    mock.method(shellHooks, 'execSync', () => { calls++; return Buffer.from('ok'); });
    const ac = new AbortController();
    ac.abort();
    assert.equal(isDockerAvailable({ signal: ac.signal }), false);
    assert.equal(calls, 0);
  });
});

describe('describeDockerUnavailable', () => {
  it('returns a distinct, actionable message per cause', () => {
    assert.match(describeDockerUnavailable({ available: false, cause: 'cli-missing' }), /not on PATH/);
    assert.match(describeDockerUnavailable({ available: false, cause: 'permission-denied' }), /permission denied/);
    assert.match(describeDockerUnavailable({ available: false, cause: 'daemon-slow' }), /did not respond/);
    assert.match(describeDockerUnavailable({ available: false, cause: 'aborted' }), /cancelled/);
    assert.match(describeDockerUnavailable({ available: false, cause: 'daemon-unreachable' }), /start Docker/);
    assert.match(describeDockerUnavailable({ available: false }), /start Docker/); // undefined -> default
  });
});

describe('annotateStderrWithSandboxFailures', () => {
  const cfgOff = { allowNetwork: false, projectDir: '/proj' };
  const cfgOn = { allowNetwork: true, projectDir: '/proj' };

  it('returns an empty stderr unchanged', () => {
    assert.equal(annotateStderrWithSandboxFailures('x', '', cfgOff), '');
  });

  it('appends a network note when network is off and stderr shows a network failure', () => {
    const out = annotateStderrWithSandboxFailures('curl x', 'curl: (6) Could not resolve host: example.com', cfgOff);
    assert.match(out, /network is disabled/);
    assert.match(out, /dangerouslyDisableSandbox: true/);
  });

  it('does NOT append a network note when network is allowed', () => {
    const out = annotateStderrWithSandboxFailures('curl x', 'Could not resolve host: example.com', cfgOn);
    assert.equal(out, 'Could not resolve host: example.com');
  });

  it('appends a mount note for a "No such file" on a path outside the project mount', () => {
    const out = annotateStderrWithSandboxFailures('cat /etc/hosts', 'cat: /etc/hosts: No such file or directory', cfgOn);
    assert.match(out, /only the project directory/);
    assert.match(out, /sandbox isolation/);
  });

  it('does NOT append a mount note when the missing path is inside the project', () => {
    const out = annotateStderrWithSandboxFailures('cat a', 'cat: /proj/a: No such file or directory', cfgOn);
    assert.equal(out, 'cat: /proj/a: No such file or directory');
  });

  it('does NOT append a mount note for a missing path under a container tmpfs (/tmp)', () => {
    const out = annotateStderrWithSandboxFailures('cat t', 'cat: /tmp/x: No such file or directory', cfgOn);
    assert.equal(out, 'cat: /tmp/x: No such file or directory');
  });

  it('returns stderr unchanged when nothing matches', () => {
    assert.equal(annotateStderrWithSandboxFailures('echo', 'some unrelated error', cfgOff), 'some unrelated error');
  });

  it('can append BOTH notes at once', () => {
    const se = 'Could not resolve host: x\ncat: /etc/hosts: No such file or directory';
    const out = annotateStderrWithSandboxFailures('x', se, { allowNetwork: false, projectDir: '/proj' });
    assert.match(out, /network is disabled/);
    assert.match(out, /only the project directory/);
  });
});

describe('wrapWithDocker — TMPDIR pin', () => {
  it('pins TMPDIR to the container tmpfs as the last -e before the image', () => {
    const out = wrapWithDocker('echo hi', tmpProject, { image: 'node:26', projectDir: tmpProject, allowNetwork: true });
    assert.ok(out.includes(`-e 'TMPDIR=${DOCKER_SANDBOX_TMPDIR}'`), out);
    // The -e must come immediately before the image arg so it wins over image ENV.
    const idxEnv = out.indexOf(`-e 'TMPDIR=${DOCKER_SANDBOX_TMPDIR}'`);
    const idxImage = out.indexOf(`'node:26'`);
    assert.ok(idxEnv < idxImage && idxImage - idxEnv < 40, out);
  });
});

describe('bashTool container-mode — cause-specific + annotated E2E', () => {
  it('fail-closed message names the real cause (CLI missing) instead of a generic one', async () => {
    mock.method(shellHooks, 'execSync', () => { throw Object.assign(new Error('docker ENOENT'), { code: 'ENOENT' }); });
    const execSpy = mock.method(_bashHooks, 'execCommand', async () => ({ stdout: '', stderr: '', exitCode: 0 }));
    const result = await bashTool.execute(
      { command: 'echo nope' },
      ctx({ sandbox: { mode: 'container', projectDir: tmpProject } }),
    );
    assert.equal(result.isError, true);
    const output = typeof result.output === 'string' ? result.output : '';
    assert.match(output, /SANDBOX REQUIRED/);
    assert.match(output, /not on PATH/, `expected cause-specific hint, got: ${output}`);
    assert.equal(execSpy.mock.callCount(), 0, 'must not run on the host');
  });

  it('fail-closed respects an already-aborted signal without probing docker', async () => {
    const probeSpy = mock.method(shellHooks, 'execSync', () => Buffer.from('ok'));
    mock.method(_bashHooks, 'execCommand', async () => ({ stdout: '', stderr: '', exitCode: 0 }));
    const ac = new AbortController();
    ac.abort();
    const result = await bashTool.execute(
      { command: 'echo nope' },
      ctx({ sandbox: { mode: 'container', projectDir: tmpProject }, abortSignal: ac.signal }),
    );
    assert.equal(result.isError, true);
    const output = typeof result.output === 'string' ? result.output : '';
    assert.match(output, /cancelled/);
    assert.equal(probeSpy.mock.callCount(), 0, 'aborted probe must not shell out');
  });

  it('annotates a failed container command with a sandbox network-off explanation', async () => {
    mock.method(shellHooks, 'execSync', () => Buffer.from('29.9.3')); // docker available
    mock.method(_bashHooks, 'execCommand', async () => ({
      stdout: '',
      stderr: 'curl: (6) Could not resolve host: example.com',
      exitCode: 6,
    }));
    const result = await bashTool.execute(
      { command: 'curl https://example.com' },
      ctx({ sandbox: { mode: 'container', projectDir: tmpProject, allowNetwork: false } }),
    );
    assert.equal(result.isError, true);
    const output = typeof result.output === 'string' ? result.output : '';
    assert.match(output, /network is disabled/, `expected sandbox network hint, got: ${output}`);
  });

  it('does not annotate when the sandbox is off (host mode)', async () => {
    mock.method(_bashHooks, 'execCommand', async () => ({
      stdout: '',
      stderr: 'curl: (6) Could not resolve host: example.com',
      exitCode: 6,
    }));
    const result = await bashTool.execute({ command: 'curl https://example.com' }, ctx());
    const output = typeof result.output === 'string' ? result.output : '';
    assert.doesNotMatch(output, /network is disabled/);
  });
});
