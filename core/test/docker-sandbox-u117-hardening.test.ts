/**
 * U117 parity tests — additional Docker sandbox hardening layers:
 *  - bare-repo .git-file poisoning defense: ro-bind any existing bare-repo
 *    marker (HEAD/objects/refs/hooks/config) at the container cwd so a sandboxed
 *    command cannot tamper with a planted/pre-existing marker (e.g. a `config`
 *    with core.fsmonitor) that a later UNsandboxed host `git` would honor.
 *  - seccomp / privilege-escalation isolation layer: every container runs with
 *    `--security-opt no-new-privileges=true` (blocks setuid escalation) and keeps
 *    Docker's DEFAULT seccomp syscall filter (never `seccomp=unconfined`).
 *
 * wrapWithDocker builds a host command STRING (no container is started); markers
 * are driven off the real filesystem, so tests use fs.mkdtempSync temp dirs.
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  wrapWithDocker,
  bareGitRepoDenyPaths,
  DOCKER_SECURITY_OPTS,
  _resetDockerAvailableCache,
  type DockerSandboxConfig,
} from '../src/tools/docker-sandbox';
import { BARE_GIT_REPO_FILES } from '../src/tools/bash-sandbox';
import { shellHooks } from '../src/utils/shell';
import { bashTool, _bashHooks } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

let tmpProject: string;

function cfg(overrides: Partial<DockerSandboxConfig> = {}): DockerSandboxConfig {
  return {
    image: 'node:26',
    projectDir: tmpProject,
    allowNetwork: false,
    ...overrides,
  };
}

beforeEach(() => {
  _resetDockerAvailableCache();
  tmpProject = fs.mkdtempSync(path.join(os.tmpdir(), 'si-docker-hard-'));
});

afterEach(() => {
  mock.restoreAll();
  _resetDockerAvailableCache();
  fs.rmSync(tmpProject, { recursive: true, force: true });
});

describe('wrapWithDocker — seccomp / no-new-privileges isolation layer', () => {
  it('runs every container with no-new-privileges and keeps the default seccomp filter', () => {
    const out = wrapWithDocker('x', tmpProject, cfg());
    assert.ok(out.includes('--security-opt no-new-privileges=true'), out);
    // Default seccomp profile must stay applied — never disabled.
    assert.ok(!/seccomp=unconfined/.test(out), out);
    assert.ok(!/--privileged\b/.test(out), out);
  });

  it('exposes the applied security-opts for introspection', () => {
    assert.ok(DOCKER_SECURITY_OPTS.includes('no-new-privileges=true'));
    // Each advertised opt actually appears in the wrapped command.
    const out = wrapWithDocker('x', tmpProject, cfg());
    for (const opt of DOCKER_SECURITY_OPTS) {
      assert.ok(out.includes(`--security-opt ${opt}`), `${opt} missing: ${out}`);
    }
  });
});

describe('bareGitRepoDenyPaths — bare-repo .git-file poisoning defense', () => {
  it('shares the marker list with the unshare backend (no duplicate constant)', () => {
    assert.deepEqual([...BARE_GIT_REPO_FILES], ['HEAD', 'objects', 'refs', 'hooks', 'config']);
  });

  it('returns only the bare-repo markers that EXIST at the cwd root', () => {
    fs.writeFileSync(path.join(tmpProject, 'HEAD'), 'ref: refs/heads/main\n');
    fs.writeFileSync(path.join(tmpProject, 'config'), '[core]\n\tfsmonitor = /tmp/evil\n');
    // objects/refs/hooks are NOT planted → must be omitted (no host-side stub).
    const paths = bareGitRepoDenyPaths(tmpProject);
    assert.deepEqual(
      paths.sort(),
      [path.join(tmpProject, 'HEAD'), path.join(tmpProject, 'config')].sort(),
    );
  });

  it('returns [] for a normal repo (a .git directory, no root-level markers)', () => {
    fs.mkdirSync(path.join(tmpProject, '.git'));
    fs.writeFileSync(path.join(tmpProject, '.git', 'HEAD'), 'ref: refs/heads/main\n');
    assert.deepEqual(bareGitRepoDenyPaths(tmpProject), []);
  });
});

describe('wrapWithDocker — bare-repo markers are bound read-only', () => {
  it('ro-binds an existing planted HEAD/config at the project root', () => {
    const head = path.join(tmpProject, 'HEAD');
    const conf = path.join(tmpProject, 'config');
    fs.writeFileSync(head, 'ref: refs/heads/main\n');
    fs.writeFileSync(conf, '[core]\n\tfsmonitor = /tmp/evil\n');
    const out = wrapWithDocker('x', tmpProject, cfg());
    assert.ok(out.includes(`-v '${head}:${head}:ro'`), out);
    assert.ok(out.includes(`-v '${conf}:${conf}:ro'`), out);
  });

  it('adds no bare-repo mount for a normal repo', () => {
    fs.mkdirSync(path.join(tmpProject, '.git'));
    const out = wrapWithDocker('x', tmpProject, cfg());
    assert.ok(!out.includes(`-v '${path.join(tmpProject, 'HEAD')}`), out);
  });
});

describe('bashTool container-mode E2E — hardening threads into the wrapped command', () => {
  function toolCtx(extra: Partial<ToolContext>): ToolContext {
    return { cwd: tmpProject, ...extra } as ToolContext;
  }

  it('wraps a container command with no-new-privileges and ro-binds a planted marker', async () => {
    fs.writeFileSync(path.join(tmpProject, 'HEAD'), 'ref: refs/heads/main\n');
    mock.method(shellHooks, 'execSync', () => Buffer.from('29.9.3')); // docker available
    let captured = '';
    mock.method(_bashHooks, 'execCommand', async (command: string) => {
      captured = command;
      return { stdout: '', stderr: '', exitCode: 0 };
    });
    await bashTool.execute(
      { command: 'echo hi' },
      toolCtx({ sandbox: { mode: 'container', projectDir: tmpProject } } as Partial<ToolContext>),
    );
    assert.ok(captured.includes('--security-opt no-new-privileges=true'), captured);
    assert.ok(
      captured.includes(`-v '${path.join(tmpProject, 'HEAD')}:${path.join(tmpProject, 'HEAD')}:ro'`),
      captured,
    );
  });
});
