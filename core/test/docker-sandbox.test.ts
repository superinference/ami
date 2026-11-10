/**
 * Tests for the Docker isolation backend and its integration into bashTool.
 *
 * These tests never start a real container — they mock the shell seams. A real
 * end-to-end containment test (actual `docker run`) lives in
 * docker-sandbox-containment.test.ts and is gated on Docker being available.
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  isDockerAvailable,
  wrapWithDocker,
  _resetDockerAvailableCache,
  DEFAULT_DOCKER_IMAGE,
} from '../src/tools/docker-sandbox';
import { shellHooks } from '../src/utils/shell';
import { bashTool, _bashHooks } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

let tmpProject: string;

beforeEach(() => {
  _resetDockerAvailableCache();
  tmpProject = fs.mkdtempSync(path.join(os.tmpdir(), 'si-docker-proj-'));
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

describe('isDockerAvailable', () => {
  it('returns true when docker version succeeds, and caches it', () => {
    let calls = 0;
    mock.method(shellHooks, 'execSync', () => { calls++; return Buffer.from('29.9.3'); });
    assert.equal(isDockerAvailable(), true);
    assert.equal(isDockerAvailable(), true); // cached
    assert.equal(calls, 1, 'probe must be cached after first call');
  });

  it('returns false when docker version throws', () => {
    mock.method(shellHooks, 'execSync', () => { throw new Error('daemon unreachable'); });
    assert.equal(isDockerAvailable(), false);
  });
});

describe('wrapWithDocker', () => {
  function cfg(over = {}) {
    return { image: 'node:26', projectDir: tmpProject, allowNetwork: true, ...over };
  }

  it('mounts only the project rw, tmpfs /tmp /root /home, and clamps workdir inside the project', () => {
    const sub = path.join(tmpProject, 'pkg');
    const out = wrapWithDocker('echo hi', sub, cfg());
    assert.ok(out.startsWith('docker run --rm -i'), out);
    assert.ok(out.includes('--user 0:0'), 'runs as container root (host user under rootless)');
    assert.ok(out.includes('--entrypoint bash'), 'overrides any image ENTRYPOINT with bash');
    assert.ok(out.includes(`-v '${tmpProject}:${tmpProject}'`), 'project must be the rw mount');
    assert.ok(out.includes(`-w '${sub}'`), 'workdir should be the (in-project) cwd');
    assert.ok(out.includes('--tmpfs /tmp'), out);
    assert.ok(out.includes('--tmpfs /root'), out);
    assert.ok(out.includes('--tmpfs /home'), out);
    assert.ok(out.endsWith("'node:26' -c 'echo hi'"), out);
  });

  it('clamps a cwd OUTSIDE the project back to the project root', () => {
    const out = wrapWithDocker('ls', '/etc', cfg());
    assert.ok(out.includes(`-w '${tmpProject}'`), 'cwd outside project must clamp to project root');
  });

  it('uses --network none when network is disallowed, bridge when allowed', () => {
    assert.ok(wrapWithDocker('x', tmpProject, cfg({ allowNetwork: false })).includes('--network none'));
    assert.ok(wrapWithDocker('x', tmpProject, cfg({ allowNetwork: true })).includes('--network bridge'));
  });

  it('adds --memory and --pids-limit only when positive', () => {
    const withLimits = wrapWithDocker('x', tmpProject, cfg({ maxMemoryMB: 512, maxProcesses: 256 }));
    assert.ok(withLimits.includes('--memory 512m'));
    assert.ok(withLimits.includes('--pids-limit 256'));
    const noLimits = wrapWithDocker('x', tmpProject, cfg({ maxMemoryMB: 0, maxProcesses: 0 }));
    assert.ok(!noLimits.includes('--memory'));
    assert.ok(!noLimits.includes('--pids-limit'));
  });

  it('single-quote-escapes a command containing a single quote', () => {
    const out = wrapWithDocker("echo 'hi there'", tmpProject, cfg());
    // the inner single quotes become '\'' sequences, passed as the bash -c arg
    assert.ok(out.includes(`-c 'echo '\\''hi there'\\'''`), out);
  });

  it('falls back to the project root when cwd is empty', () => {
    const out = wrapWithDocker('x', '', cfg());
    assert.ok(out.includes(`-w '${tmpProject}'`));
  });
});

describe('bashTool container-mode integration', () => {
  it('wraps the command in `docker run` when sandbox mode is container and Docker is available', async () => {
    mock.method(shellHooks, 'execSync', () => Buffer.from('29.9.3')); // docker available
    let captured = '';
    mock.method(_bashHooks, 'execCommand', async (cmd: string) => {
      captured = cmd;
      return { stdout: 'ok', stderr: '', exitCode: 0 };
    });
    const result = await bashTool.execute(
      { command: 'echo integration-test' },
      ctx({ sandbox: { mode: 'container', projectDir: tmpProject } }),
    );
    assert.equal(result.isError, false);
    assert.ok(captured.startsWith('docker run --rm -i'), `expected docker wrap, got: ${captured}`);
    assert.ok(captured.includes('--entrypoint bash') && captured.includes('--user 0:0'), captured);
    assert.ok(captured.includes(DEFAULT_DOCKER_IMAGE), 'defaults to the openshell-ami image');
    assert.ok(captured.includes("-c 'echo integration-test'"), captured);
  });

  it('FAILS CLOSED (refuses, never runs on host) when container mode is set but Docker is unavailable', async () => {
    mock.method(shellHooks, 'execSync', () => { throw new Error('no daemon'); }); // docker gone
    const execSpy = mock.method(_bashHooks, 'execCommand', async () => ({ stdout: '', stderr: '', exitCode: 0 }));
    const result = await bashTool.execute(
      { command: 'echo should-not-run' },
      ctx({ sandbox: { mode: 'container', projectDir: tmpProject } }),
    );
    assert.equal(result.isError, true);
    const output = typeof result.output === 'string' ? result.output : '';
    assert.ok(output.includes('SANDBOX REQUIRED'), output);
    assert.equal(execSpy.mock.callCount(), 0, 'must NOT execute anything on the host');
  });

  it('does not wrap in Docker when sandbox mode is off (runs on host as before)', async () => {
    let captured = '';
    mock.method(_bashHooks, 'execCommand', async (cmd: string) => {
      captured = cmd;
      return { stdout: 'hi', stderr: '', exitCode: 0 };
    });
    const result = await bashTool.execute({ command: 'echo plainhost' }, ctx());
    assert.equal(result.isError, false);
    assert.equal(captured, 'echo plainhost', 'host mode must pass the command through unwrapped');
  });
});
