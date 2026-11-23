/**
 * U117 parity tests — per-domain network ALLOWLIST in container mode.
 *
 * Gap (entrypoints/sandboxTypes.ts / sandbox-adapter.ts): AMI's container network
 * policy was binary (`--network bridge|none`) plus a per-host DENY blackhole; it
 * could not express "network on, but ONLY to these domains". Claude's
 * `SandboxNetworkConfig.allowedDomains` can. This verifies `DockerSandboxConfig`
 * now honors `allowedHosts` by realizing the SAME iptables allowlist the host
 * (unshare) backend uses — ACCEPT loopback + DNS + each resolved allowed host,
 * DROP the rest — behind `--cap-add NET_ADMIN`, with deny winning over allow and
 * leader-granted hosts folded in.
 *
 * All tests inspect the generated command string / mock the shell seams — no real
 * container is ever started.
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { wrapWithDocker, _resetDockerAvailableCache } from '../src/tools/docker-sandbox';
import {
  recordSandboxHostGrant,
  clearSandboxPermissionGrants,
} from '../src/tools/sandbox-permission-registry';
import { shellHooks } from '../src/utils/shell';
import { bashTool, _bashHooks } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

import { isolateSandboxGrants } from './_isolate-sandbox-grants';

isolateSandboxGrants();

let tmpProject: string;

beforeEach(() => {
  _resetDockerAvailableCache();
  clearSandboxPermissionGrants();
  tmpProject = fs.mkdtempSync(path.join(os.tmpdir(), 'si-docker-u117-al-'));
});

afterEach(() => {
  mock.restoreAll();
  clearSandboxPermissionGrants();
  _resetDockerAvailableCache();
  fs.rmSync(tmpProject, { recursive: true, force: true });
});

function base() {
  return { image: 'img', projectDir: tmpProject, allowNetwork: true };
}

describe('wrapWithDocker — per-domain network allowlist', () => {
  it('adds --cap-add NET_ADMIN and an iptables allowlist that DROPs all but allowed hosts', () => {
    const out = wrapWithDocker('curl https://registry.example', tmpProject, {
      ...base(),
      allowedHosts: ['registry.example', 'pkg.example'],
    });
    assert.match(out, /--cap-add NET_ADMIN/);
    // Loopback + DNS are always allowed, each allowed host is resolved + ACCEPTed,
    // and everything else is dropped by a trailing default-DROP rule.
    assert.match(out, /iptables -A OUTPUT -o lo -j ACCEPT/);
    // Host names are inside the shell-escaped `-c` arg, so quotes render as `'\''`;
    // match up to the first pipe (the getent line is `getent ahosts <host> ... | awk`).
    assert.match(out, /getent ahosts [^|]*registry\.example/);
    assert.match(out, /getent ahosts [^|]*pkg\.example/);
    assert.match(out, /iptables -A OUTPUT -j DROP/);
    // The real user command is preserved, AFTER the allowlist setup.
    assert.ok(out.includes('curl https://registry.example'), out);
    const setupIdx = out.indexOf('iptables -A OUTPUT -o lo');
    const cmdIdx = out.indexOf('curl https://registry.example');
    assert.ok(setupIdx !== -1 && cmdIdx > setupIdx, 'setup must run before the command');
  });

  it('emits deny rules BEFORE the allowlist so deny wins over allow (first-match)', () => {
    const out = wrapWithDocker('curl x', tmpProject, {
      ...base(),
      allowedHosts: ['good.example'],
      deniedHosts: ['bad.example'],
    });
    // Denied host still blackholed at the DNS level too.
    assert.match(out, /--add-host 'bad\.example:0\.0\.0\.0'/);
    // iptables DROP rule for the denied host precedes the ACCEPT rule for the allowed one.
    const dropIdx = out.search(/getent ahosts [^|]*bad\.example/);
    const acceptIdx = out.search(/getent ahosts [^|]*good\.example/);
    assert.ok(dropIdx !== -1 && acceptIdx !== -1 && dropIdx < acceptIdx, out);
  });

  it('folds a leader-granted host into the allowlist (escalation flow works under allowlist)', () => {
    recordSandboxHostGrant('granted.example');
    const out = wrapWithDocker('curl x', tmpProject, {
      ...base(),
      allowedHosts: ['static.example'],
    });
    assert.match(out, /getent ahosts [^|]*granted\.example/);
    assert.match(out, /getent ahosts [^|]*static\.example/);
  });

  it('does NOT add an allowlist (or NET_ADMIN) when allowedHosts is empty/unset', () => {
    const out = wrapWithDocker('echo hi', tmpProject, { ...base() });
    assert.doesNotMatch(out, /NET_ADMIN/);
    assert.doesNotMatch(out, /iptables/);
    assert.ok(out.includes("-c 'echo hi'"), out);
  });

  it('does NOT apply the allowlist when network is off (--network none already blocks all)', () => {
    const out = wrapWithDocker('echo hi', tmpProject, {
      ...base(),
      allowNetwork: false,
      allowedHosts: ['x.example'],
    });
    assert.match(out, /--network none/);
    assert.doesNotMatch(out, /NET_ADMIN/);
    assert.doesNotMatch(out, /iptables/);
  });

  it('dedupes/trims allowed hosts and ignores blank entries', () => {
    const out = wrapWithDocker('echo hi', tmpProject, {
      ...base(),
      allowedHosts: ['  a.example ', 'a.example', '', '   '],
    });
    const matches = out.match(/getent ahosts [^|]*a\.example/g) ?? [];
    assert.equal(matches.length, 1, out);
  });
});

describe('bashTool container-mode E2E — allowlist threads from the policy', () => {
  function ctx(overrides: Partial<ToolContext> = {}): ToolContext {
    return {
      cwd: tmpProject,
      abortSignal: new AbortController().signal,
      detachedMode: false,
      ...overrides,
    } as unknown as ToolContext;
  }

  it('threads network.allowedHosts into the wrapped docker command', async () => {
    mock.method(shellHooks, 'execSync', () => Buffer.from('29.9.3')); // docker available
    let captured = '';
    mock.method(_bashHooks, 'execCommand', async (cmd: string) => {
      captured = cmd;
      return { stdout: '', stderr: '', exitCode: 0 };
    });
    const result = await bashTool.execute(
      { command: 'curl https://allowed.example' },
      ctx({
        sandbox: {
          mode: 'container',
          projectDir: tmpProject,
          allowNetwork: true,
          network: { allowedHosts: ['allowed.example'] },
        },
      } as Partial<ToolContext>),
    );
    assert.equal(result.isError, false);
    assert.ok(captured.includes('--cap-add NET_ADMIN'), captured);
    assert.match(captured, /getent ahosts [^|]*allowed\.example/);
  });
});
