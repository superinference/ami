/**
 * U006 — custom seccomp syscall-filter profile for the Docker sandbox.
 *
 * PARITY (the reference's `sandbox.seccomp.bpfPath` / `applyPath`): AMI sandboxes via
 * Docker, so the architecture-native analog is a Docker seccomp JSON profile
 * surfaced through `SandboxPolicy.seccomp.profilePath`. This suite proves the
 * knob is wired end to end: typed + validated + sanitized in types.ts, passed to
 * the container backend by {@link wrapWithDocker} as `--security-opt seccomp=…`,
 * and threaded from a `SandboxPolicy` through the real `bash` tool.
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  wrapWithDocker,
  _resetDockerAvailableCache,
  type DockerSandboxConfig,
} from '../src/tools/docker-sandbox';
import { shellHooks } from '../src/utils/shell';
import { bashTool, _bashHooks } from '../src/tools/bash';
import { validateEngineConfig, sanitizeEngineConfig } from '../src/types';
import type { ToolContext } from '../src/types';

let tmpProject: string;

function cfg(overrides: Partial<DockerSandboxConfig> = {}): DockerSandboxConfig {
  return { image: 'node:26', projectDir: tmpProject, allowNetwork: false, ...overrides };
}

beforeEach(() => {
  _resetDockerAvailableCache();
  tmpProject = fs.mkdtempSync(path.join(os.tmpdir(), 'si-seccomp-'));
});

afterEach(() => {
  mock.restoreAll();
  _resetDockerAvailableCache();
  fs.rmSync(tmpProject, { recursive: true, force: true });
});

describe('wrapWithDocker — custom seccomp profile', () => {
  it('emits --security-opt seccomp=<resolved path> when a profile is configured', () => {
    const profile = path.join(tmpProject, 'seccomp.json');
    const out = wrapWithDocker('echo hi', tmpProject, cfg({ seccompProfilePath: profile }));
    assert.ok(out.includes(`--security-opt 'seccomp=${profile}'`), out);
    // Hardened default is still applied alongside the custom profile.
    assert.ok(out.includes('--security-opt no-new-privileges=true'), out);
  });

  it('resolves a relative profile path to an absolute one', () => {
    const out = wrapWithDocker('x', tmpProject, cfg({ seccompProfilePath: 'relative/sec.json' }));
    assert.ok(out.includes(`--security-opt 'seccomp=${path.resolve('relative/sec.json')}'`), out);
  });

  it('trims surrounding whitespace in the profile path', () => {
    const profile = path.join(tmpProject, 'sec.json');
    const out = wrapWithDocker('x', tmpProject, cfg({ seccompProfilePath: `  ${profile}  ` }));
    assert.ok(out.includes(`--security-opt 'seccomp=${profile}'`), out);
  });

  it('omits the seccomp flag entirely when no profile is configured', () => {
    assert.ok(!wrapWithDocker('x', tmpProject, cfg()).includes('seccomp='));
  });

  it('omits the seccomp flag for a blank/whitespace-only profile path', () => {
    assert.ok(!wrapWithDocker('x', tmpProject, cfg({ seccompProfilePath: '   ' })).includes('seccomp='));
  });
});

describe('SandboxPolicy.seccomp — validation + sanitization', () => {
  it('accepts a valid seccomp.profilePath', () => {
    const r = validateEngineConfig({ sandbox: { mode: 'container', seccomp: { profilePath: '/etc/sec.json' } } });
    assert.equal(r.valid, true);
    assert.equal(r.warnings.length, 0);
  });

  it('rejects an empty seccomp.profilePath', () => {
    const r = validateEngineConfig({ sandbox: { mode: 'container', seccomp: { profilePath: '   ' } } });
    assert.equal(r.valid, false);
    assert.ok(r.warnings.some((w) => w.includes('sandbox.seccomp.profilePath')), r.warnings.join(','));
  });

  it('accepts an absent seccomp sub-object', () => {
    const r = validateEngineConfig({ sandbox: { mode: 'container' } });
    assert.equal(r.valid, true);
  });

  it('trims a configured profile path during sanitization', () => {
    const r = sanitizeEngineConfig({ sandbox: { mode: 'container', seccomp: { profilePath: '  /etc/sec.json  ' } } });
    assert.deepEqual(r.sandbox!.seccomp, { profilePath: '/etc/sec.json' });
  });

  it('drops a blank profile path to an empty seccomp object during sanitization', () => {
    const r = sanitizeEngineConfig({ sandbox: { mode: 'container', seccomp: { profilePath: '   ' } } });
    assert.deepEqual(r.sandbox!.seccomp, {});
  });
});

describe('bash tool — threads seccomp profile from the policy into the container', () => {
  function toolCtx(overrides: Partial<ToolContext> = {}): ToolContext {
    return { cwd: tmpProject, abortSignal: new AbortController().signal, ...overrides } as unknown as ToolContext;
  }

  it('passes SandboxPolicy.seccomp.profilePath through to the docker --security-opt flag', async () => {
    const profile = path.join(tmpProject, 'profile.json');
    fs.writeFileSync(profile, '{}');
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
          seccomp: { profilePath: profile },
        },
      } as Partial<ToolContext>),
    );
    assert.ok(captured.includes(`--security-opt 'seccomp=${profile}'`), captured);
  });
});
