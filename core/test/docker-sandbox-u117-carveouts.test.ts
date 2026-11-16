/**
 * U117 parity tests for the Docker sandbox filesystem/network carve-outs:
 *  - denyWritePaths  → nested read-only bind on top of the project RW mount
 *  - default deny-write of privileged autoload paths (settings/skills/commands/agents)
 *  - allowWritePaths → extra writable mounts outside the project
 *  - git-worktree main-repo detection → auto writable mount of the shared .git root
 *  - denyReadPaths   → /dev/null bind over hidden files
 *  - allowUnixSockets allowlist → deny-all by default, explicit grants only
 *
 * wrapWithDocker builds a host command STRING (no container is started); carve-outs
 * are driven off the real filesystem, so tests use fs.mkdtempSync temp dirs.
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  wrapWithDocker,
  defaultDenyWritePaths,
  detectWorktreeMainRepoPath,
  PRIVILEGED_AUTOLOAD_RELPATHS,
  _resetDockerAvailableCache,
  type DockerSandboxConfig,
} from '../src/tools/docker-sandbox';
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
  tmpProject = fs.mkdtempSync(path.join(os.tmpdir(), 'si-docker-carve-'));
});

afterEach(() => {
  mock.restoreAll();
  _resetDockerAvailableCache();
  fs.rmSync(tmpProject, { recursive: true, force: true });
});

describe('wrapWithDocker — denyWritePaths (nested read-only carve-outs)', () => {
  it('bind-remounts an existing configured path read-only on top of the project mount', () => {
    const secret = path.join(tmpProject, '.env');
    fs.writeFileSync(secret, 'TOKEN=1');
    const out = wrapWithDocker('x', tmpProject, cfg({ denyWritePaths: [secret] }));
    assert.ok(out.includes(`-v '${secret}:${secret}:ro'`), out);
    // project RW mount remains, and the command/image still terminate the string.
    assert.ok(out.includes(`-v '${tmpProject}:${tmpProject}'`), out);
    assert.ok(out.endsWith("'node:26' -c 'x'"), out);
  });

  it('skips a non-existing denyWrite path so Docker does not auto-create a host source', () => {
    const missing = path.join(tmpProject, 'nope.env');
    const out = wrapWithDocker('x', tmpProject, cfg({ denyWritePaths: [missing] }));
    assert.ok(!out.includes(`${missing}:${missing}:ro`), out);
  });

  it('does not duplicate a path listed twice / also a default', () => {
    const skills = path.join(tmpProject, '.claude', 'skills');
    fs.mkdirSync(skills, { recursive: true });
    const out = wrapWithDocker('x', tmpProject, cfg({ denyWritePaths: [skills, skills] }));
    const occurrences = out.split(`-v '${skills}:${skills}:ro'`).length - 1;
    assert.equal(occurrences, 1, out);
  });
});

describe('wrapWithDocker — default privileged-autoload deny-write', () => {
  it('mounts existing settings/skills/commands/agents dirs read-only automatically', () => {
    const settings = path.join(tmpProject, '.claude', 'settings.json');
    const skills = path.join(tmpProject, '.ami', 'skills');
    fs.mkdirSync(path.dirname(settings), { recursive: true });
    fs.writeFileSync(settings, '{}');
    fs.mkdirSync(skills, { recursive: true });
    const out = wrapWithDocker('x', tmpProject, cfg());
    assert.ok(out.includes(`-v '${settings}:${settings}:ro'`), out);
    assert.ok(out.includes(`-v '${skills}:${skills}:ro'`), out);
  });

  it('omits defaults that do not exist (fresh project → no extra carve-out mounts)', () => {
    const out = wrapWithDocker('x', tmpProject, cfg());
    assert.ok(!out.includes(':ro'), out);
    assert.deepEqual(defaultDenyWritePaths(tmpProject), []);
  });

  it('defaultDenyWritePaths lists exactly the existing privileged paths', () => {
    const s = path.join(tmpProject, '.claude', 'settings.json');
    fs.mkdirSync(path.dirname(s), { recursive: true });
    fs.writeFileSync(s, '{}');
    assert.deepEqual(defaultDenyWritePaths(tmpProject), [s]);
    // every default is a known privileged relpath
    for (const p of defaultDenyWritePaths(tmpProject)) {
      assert.ok(PRIVILEGED_AUTOLOAD_RELPATHS.some((rel) => p.endsWith(rel.replace('/', path.sep))));
    }
  });
});

describe('wrapWithDocker — allowWritePaths (extra writable mounts)', () => {
  it('mounts an existing directory outside the project writable', () => {
    const extra = fs.mkdtempSync(path.join(os.tmpdir(), 'si-extra-'));
    try {
      const out = wrapWithDocker('x', tmpProject, cfg({ allowWritePaths: [extra] }));
      assert.ok(out.includes(`-v '${extra}:${extra}'`), out);
      assert.ok(!out.includes(`${extra}:${extra}:ro`), out);
    } finally {
      fs.rmSync(extra, { recursive: true, force: true });
    }
  });

  it('skips paths inside the project (already writable) and missing paths', () => {
    const inside = path.join(tmpProject, 'sub');
    fs.mkdirSync(inside, { recursive: true });
    const missing = path.join(os.tmpdir(), 'si-nope-' + Date.now());
    const out = wrapWithDocker('x', tmpProject, cfg({ allowWritePaths: [inside, missing] }));
    assert.ok(!out.includes(`-v '${inside}:${inside}'`), out);
    assert.ok(!out.includes(missing), out);
  });
});

describe('detectWorktreeMainRepoPath + auto writable mount', () => {
  it('recovers the main repo root from a worktree .git file and mounts it writable', () => {
    const mainRepo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-mainrepo-'));
    try {
      const gitdir = path.join(mainRepo, '.git', 'worktrees', 'mybranch');
      fs.writeFileSync(path.join(tmpProject, '.git'), `gitdir: ${gitdir}\n`);
      assert.equal(detectWorktreeMainRepoPath(tmpProject), mainRepo);
      const out = wrapWithDocker('git commit', tmpProject, cfg());
      assert.ok(out.includes(`-v '${mainRepo}:${mainRepo}'`), out);
    } finally {
      fs.rmSync(mainRepo, { recursive: true, force: true });
    }
  });

  it('uses lastIndexOf of the exact .git/worktrees marker (no false /.github match)', () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), 'si-gh-'));
    try {
      const mainRepo = path.join(base, '.github-projects', 'repo');
      fs.mkdirSync(mainRepo, { recursive: true });
      const gitdir = path.join(mainRepo, '.git', 'worktrees', 'wt');
      fs.writeFileSync(path.join(tmpProject, '.git'), `gitdir: ${gitdir}`);
      assert.equal(detectWorktreeMainRepoPath(tmpProject), mainRepo);
    } finally {
      fs.rmSync(base, { recursive: true, force: true });
    }
  });

  it('returns null for a real .git directory, a missing .git, and a non-worktree gitdir', () => {
    // real .git dir
    fs.mkdirSync(path.join(tmpProject, '.git'));
    assert.equal(detectWorktreeMainRepoPath(tmpProject), null);
    fs.rmSync(path.join(tmpProject, '.git'), { recursive: true });
    // missing .git
    assert.equal(detectWorktreeMainRepoPath(tmpProject), null);
    // .git file without a worktrees marker
    fs.writeFileSync(path.join(tmpProject, '.git'), 'gitdir: /somewhere/else');
    assert.equal(detectWorktreeMainRepoPath(tmpProject), null);
    // .git file without a gitdir: line at all
    fs.writeFileSync(path.join(tmpProject, '.git'), 'not a gitdir file');
    assert.equal(detectWorktreeMainRepoPath(tmpProject), null);
  });

  it('resolves a relative gitdir against cwd', () => {
    const rel = path.join('..', 'main', '.git', 'worktrees', 'x');
    fs.writeFileSync(path.join(tmpProject, '.git'), `gitdir: ${rel}`);
    const expected = path.resolve(tmpProject, path.join('..', 'main'));
    assert.equal(detectWorktreeMainRepoPath(tmpProject), expected);
  });
});

describe('wrapWithDocker — denyReadPaths (/dev/null bind)', () => {
  it('binds /dev/null over an existing regular file and skips a directory', () => {
    const file = path.join(tmpProject, 'creds');
    fs.writeFileSync(file, 'secret');
    const dir = path.join(tmpProject, 'adir');
    fs.mkdirSync(dir);
    const out = wrapWithDocker('x', tmpProject, cfg({ denyReadPaths: [file, dir] }));
    assert.ok(out.includes(`-v '/dev/null:${file}:ro'`), out);
    assert.ok(!out.includes(`/dev/null:${dir}:ro`), out);
  });
});

describe('bashTool container-mode E2E — SandboxPolicy carve-outs thread into the wrapped command', () => {
  function toolCtx(overrides: Partial<ToolContext> = {}): ToolContext {
    return {
      cwd: tmpProject,
      abortSignal: new AbortController().signal,
      detachedMode: false,
      ...overrides,
    } as unknown as ToolContext;
  }

  it('threads filesystem.denyWrite + network.allowUnixSockets from the policy into the docker command', async () => {
    const secret = path.join(tmpProject, '.env');
    fs.writeFileSync(secret, 'TOKEN=1');
    const net = require('net') as typeof import('net');
    const sockPath = path.join(tmpProject, 'app.sock');
    await new Promise<void>((resolve, reject) => {
      const server = net.createServer();
      server.listen(sockPath, async () => {
        try {
          mock.method(shellHooks, 'execSync', () => Buffer.from('29.9.3')); // docker available
          let captured = '';
          mock.method(_bashHooks, 'execCommand', async (command: string) => {
            captured = command;
            return { stdout: '', stderr: '', exitCode: 0 };
          });
          await bashTool.execute(
            { command: 'echo hi' },
            toolCtx({
              sandbox: {
                mode: 'container',
                projectDir: tmpProject,
                filesystem: { denyWrite: [secret] },
                network: { allowUnixSockets: [sockPath] },
              },
            } as Partial<ToolContext>),
          );
          assert.ok(captured.includes(`-v '${secret}:${secret}:ro'`), captured);
          assert.ok(captured.includes(`-v '${sockPath}:${sockPath}'`), captured);
          server.close(() => resolve());
        } catch (e) {
          server.close(() => reject(e));
        }
      });
      server.on('error', reject);
    });
  });
});

describe('wrapWithDocker — allowUnixSockets (deny-all default + allowlist)', () => {
  it('mounts no host socket by default', () => {
    const out = wrapWithDocker('x', tmpProject, cfg());
    assert.ok(!out.includes('docker.sock'), out);
  });

  it('bind-mounts only existing allowlisted sockets', () => {
    const net = require('net') as typeof import('net');
    const sockPath = path.join(tmpProject, 'app.sock');
    return new Promise<void>((resolve, reject) => {
      const server = net.createServer();
      server.listen(sockPath, () => {
        try {
          const missing = path.join(tmpProject, 'ghost.sock');
          const out = wrapWithDocker('x', tmpProject, cfg({ allowUnixSockets: [sockPath, missing] }));
          assert.ok(out.includes(`-v '${sockPath}:${sockPath}'`), out);
          assert.ok(!out.includes(missing), out);
          server.close(() => resolve());
        } catch (e) {
          server.close(() => reject(e));
        }
      });
      server.on('error', reject);
    });
  });
});
