/**
 * U117 parity tests — container network egress control, sandbox-violation
 * envelope, and the "sandbox enabled but can't run" reason helper:
 *  - wrapWithDocker: per-host deny blackhole (--add-host h:0.0.0.0) + proxy routing
 *    (HTTP/SOCKS proxy env via the host-gateway alias) — gap: per-host egress.
 *  - annotateStderrWithSandboxFailures wraps notes in a <sandbox_violations> block
 *    that removeSandboxViolationTags (the CLI stripper) removes — gap: violation
 *    envelope (model keeps, user display strips).
 *  - getSandboxUnavailableReason: proactive "enabled but unavailable" reason.
 *
 * All tests mock the shell seams — no real container is ever started.
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  wrapWithDocker,
  annotateStderrWithSandboxFailures,
  getSandboxUnavailableReason,
  _resetDockerAvailableCache,
  DOCKER_HOST_GATEWAY_ALIAS,
} from '../src/tools/docker-sandbox';
import {
  wrapSandboxViolations,
  removeSandboxViolationTags,
  SANDBOX_VIOLATIONS_TAG,
} from '../src/tools/sandbox-violations';
import { shellHooks } from '../src/utils/shell';
import { bashTool, _bashHooks } from '../src/tools/bash';
import type { ToolContext, SandboxPolicy } from '../src/types';

let tmpProject: string;

beforeEach(() => {
  _resetDockerAvailableCache();
  tmpProject = fs.mkdtempSync(path.join(os.tmpdir(), 'si-docker-u117-nr-'));
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

describe('wrapWithDocker — per-host network deny (blackhole)', () => {
  it('blackholes each denied host via --add-host <host>:0.0.0.0 when network is on', () => {
    const out = wrapWithDocker('curl x', tmpProject, {
      image: 'node:26',
      projectDir: tmpProject,
      allowNetwork: true,
      deniedHosts: ['evil.example.com', 'tracker.io'],
    });
    assert.ok(out.includes(`--add-host 'evil.example.com:0.0.0.0'`), out);
    assert.ok(out.includes(`--add-host 'tracker.io:0.0.0.0'`), out);
  });

  it('does NOT add deny blackholes when network is off (already blocked)', () => {
    const out = wrapWithDocker('curl x', tmpProject, {
      image: 'node:26',
      projectDir: tmpProject,
      allowNetwork: false,
      deniedHosts: ['evil.example.com'],
    });
    assert.ok(!out.includes('0.0.0.0'), out);
  });

  it('dedupes repeated hosts and skips blank entries', () => {
    const out = wrapWithDocker('curl x', tmpProject, {
      image: 'node:26',
      projectDir: tmpProject,
      allowNetwork: true,
      deniedHosts: ['dup.example', 'dup.example', '  ', ''],
    });
    const occurrences = out.split(`--add-host 'dup.example:0.0.0.0'`).length - 1;
    assert.equal(occurrences, 1, out);
    // Exactly one blackhole total — the blank entries added none.
    assert.equal(out.split(`:0.0.0.0'`).length - 1, 1, out);
  });
});

describe('wrapWithDocker — proxy-routed egress', () => {
  it('routes HTTP(S) through the host-gateway proxy when httpProxyPort is set', () => {
    const out = wrapWithDocker('curl x', tmpProject, {
      image: 'node:26',
      projectDir: tmpProject,
      allowNetwork: true,
      httpProxyPort: 8080,
    });
    assert.ok(out.includes(`--add-host '${DOCKER_HOST_GATEWAY_ALIAS}:host-gateway'`), out);
    assert.ok(out.includes(`-e 'HTTP_PROXY=http://${DOCKER_HOST_GATEWAY_ALIAS}:8080'`), out);
    assert.ok(out.includes(`-e 'HTTPS_PROXY=http://${DOCKER_HOST_GATEWAY_ALIAS}:8080'`), out);
  });

  it('routes through a SOCKS proxy when socksProxyPort is set', () => {
    const out = wrapWithDocker('curl x', tmpProject, {
      image: 'node:26',
      projectDir: tmpProject,
      allowNetwork: true,
      socksProxyPort: 1080,
    });
    assert.ok(out.includes(`-e 'ALL_PROXY=socks5://${DOCKER_HOST_GATEWAY_ALIAS}:1080'`), out);
  });

  it('ignores an invalid proxy port (no env, no host-gateway alias)', () => {
    const out = wrapWithDocker('curl x', tmpProject, {
      image: 'node:26',
      projectDir: tmpProject,
      allowNetwork: true,
      httpProxyPort: 0,
      socksProxyPort: 70000,
    });
    assert.ok(!out.includes('HTTP_PROXY'), out);
    assert.ok(!out.includes('ALL_PROXY'), out);
    assert.ok(!out.includes('host-gateway'), out);
  });

  it('does not add proxy env when network is off', () => {
    const out = wrapWithDocker('curl x', tmpProject, {
      image: 'node:26',
      projectDir: tmpProject,
      allowNetwork: false,
      httpProxyPort: 8080,
    });
    assert.ok(!out.includes('HTTP_PROXY'), out);
  });
});

describe('sandbox-violations envelope — wrap + strip round-trip', () => {
  it('wrapSandboxViolations wraps notes in a <sandbox_violations> block', () => {
    const body = wrapSandboxViolations(['[sandbox: net blocked]', '[sandbox: path missing]']);
    assert.ok(body.startsWith(`<${SANDBOX_VIOLATIONS_TAG}>`), body);
    assert.ok(body.endsWith(`</${SANDBOX_VIOLATIONS_TAG}>`), body);
    assert.ok(body.includes('net blocked') && body.includes('path missing'), body);
  });

  it('returns empty string for an empty / all-blank note list (no empty tag)', () => {
    assert.equal(wrapSandboxViolations([]), '');
    assert.equal(wrapSandboxViolations(['', '   ']), '');
  });

  it('removeSandboxViolationTags excises the block (model keeps it, user does not)', () => {
    const wrapped = `boom\n${wrapSandboxViolations(['[sandbox: net blocked]'])}`;
    const stripped = removeSandboxViolationTags(wrapped);
    assert.ok(!stripped.includes('sandbox_violations'), stripped);
    assert.ok(!stripped.includes('net blocked'), stripped);
    assert.ok(stripped.includes('boom'), stripped);
  });

  it('annotateStderrWithSandboxFailures emits the <sandbox_violations> envelope', () => {
    const out = annotateStderrWithSandboxFailures(
      'curl x',
      'curl: (6) Could not resolve host: example.com',
      { allowNetwork: false, projectDir: tmpProject },
    );
    assert.match(out, /<sandbox_violations>/);
    assert.match(out, /network is disabled/); // the note text survives inside the tag
    // The CLI stripper removes the whole envelope for the user display.
    assert.ok(!removeSandboxViolationTags(out).includes('network is disabled'));
  });
});

describe('getSandboxUnavailableReason', () => {
  it('returns null when no policy / mode is off (nothing enabled)', () => {
    assert.equal(getSandboxUnavailableReason(undefined), null);
    assert.equal(getSandboxUnavailableReason({ mode: 'off' } as SandboxPolicy), null);
  });

  it('warns when the current platform is excluded by enabledPlatforms', () => {
    const other = process.platform === 'linux' ? 'win32' : 'linux';
    const reason = getSandboxUnavailableReason({
      mode: 'container',
      enabledPlatforms: [other],
    } as SandboxPolicy);
    assert.ok(reason, 'expected a reason');
    assert.match(reason!, /enabledPlatforms/);
    assert.match(reason!, /NOT be sandboxed/);
  });

  it('warns (unsandboxed) when Docker is unavailable without failIfUnavailable', () => {
    mock.method(shellHooks, 'execSync', () => {
      throw Object.assign(new Error('docker ENOENT'), { code: 'ENOENT' });
    });
    const reason = getSandboxUnavailableReason({ mode: 'container' } as SandboxPolicy);
    assert.ok(reason, 'expected a reason');
    assert.match(reason!, /not on PATH/);
    assert.match(reason!, /UNsandboxed/);
  });

  it('warns (refused) when Docker is unavailable WITH failIfUnavailable', () => {
    mock.method(shellHooks, 'execSync', () => {
      throw Object.assign(new Error('docker ENOENT'), { code: 'ENOENT' });
    });
    const reason = getSandboxUnavailableReason({
      mode: 'container',
      failIfUnavailable: true,
    } as SandboxPolicy);
    assert.match(reason!, /REFUSED/);
  });

  it('returns null when container mode is enabled AND Docker is reachable', () => {
    mock.method(shellHooks, 'execSync', () => Buffer.from('29.9.3'));
    assert.equal(getSandboxUnavailableReason({ mode: 'container' } as SandboxPolicy), null);
  });
});

describe('bashTool container-mode E2E — network policy threads into the wrapped command', () => {
  it('threads network.deniedHosts + proxy ports from the policy into the docker command', async () => {
    mock.method(shellHooks, 'execSync', () => Buffer.from('29.9.3')); // docker available
    let captured = '';
    mock.method(_bashHooks, 'execCommand', async (cmd: string) => {
      captured = cmd;
      return { stdout: '', stderr: '', exitCode: 0 };
    });
    const result = await bashTool.execute(
      { command: 'curl https://example.com' },
      ctx({
        sandbox: {
          mode: 'container',
          projectDir: tmpProject,
          allowNetwork: true,
          network: { deniedHosts: ['blocked.example'], httpProxyPort: 8080 },
        },
      } as Partial<ToolContext>),
    );
    assert.equal(result.isError, false);
    assert.ok(captured.includes(`--add-host 'blocked.example:0.0.0.0'`), captured);
    assert.ok(captured.includes(`-e 'HTTP_PROXY=http://${DOCKER_HOST_GATEWAY_ALIAS}:8080'`), captured);
  });
});
