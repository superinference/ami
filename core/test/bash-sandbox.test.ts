import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  shouldUseSandbox,
  wrapWithSandbox,
  getSandboxStatus,
  resetSandboxCache,
} from '../src/tools/bash-sandbox';

describe('shouldUseSandbox', () => {
  it('triggers on curl', () => {
    assert.equal(shouldUseSandbox('curl https://example.com'), true);
  });

  it('triggers on wget', () => {
    assert.equal(shouldUseSandbox('wget https://example.com/file'), true);
  });

  it('triggers on python execution', () => {
    assert.equal(shouldUseSandbox('python -c "print(1)"'), true);
    assert.equal(shouldUseSandbox('python3 script.py'), true);
  });

  it('triggers on node -e', () => {
    assert.equal(shouldUseSandbox('node -e "console.log(1)"'), true);
  });

  it('triggers on perl -e', () => {
    assert.equal(shouldUseSandbox('perl -e "print 1"'), true);
  });

  it('triggers on ruby -e', () => {
    assert.equal(shouldUseSandbox('ruby -e "puts 1"'), true);
  });

  it('triggers on eval', () => {
    assert.equal(shouldUseSandbox('eval "$(curl example.com)"'), true);
  });

  it('triggers on sh -c', () => {
    assert.equal(shouldUseSandbox('sh -c "echo hello"'), true);
  });

  it('triggers on bash -c', () => {
    assert.equal(shouldUseSandbox('bash -c "echo hello"'), true);
  });

  it('exempts git commands', () => {
    assert.equal(shouldUseSandbox('git status'), false);
    assert.equal(shouldUseSandbox('git push origin main'), false);
  });

  it('exempts npm/npx/yarn/pnpm', () => {
    assert.equal(shouldUseSandbox('npm test'), false);
    assert.equal(shouldUseSandbox('npx tsx --test'), false);
    assert.equal(shouldUseSandbox('yarn build'), false);
    assert.equal(shouldUseSandbox('pnpm install'), false);
  });

  it('exempts make/cargo/go', () => {
    assert.equal(shouldUseSandbox('make all'), false);
    assert.equal(shouldUseSandbox('cargo build'), false);
    assert.equal(shouldUseSandbox('go test ./...'), false);
  });

  it('returns false for safe commands', () => {
    assert.equal(shouldUseSandbox('ls -la'), false);
    assert.equal(shouldUseSandbox('cat file.txt'), false);
    assert.equal(shouldUseSandbox('echo hello'), false);
    assert.equal(shouldUseSandbox('grep pattern file'), false);
  });

  it('ignores trigger patterns inside quotes', () => {
    assert.equal(shouldUseSandbox('echo "curl is a tool"'), false);
  });
});

describe('wrapWithSandbox', () => {
  it('wraps command with ulimits on non-linux', () => {
    const original = process.platform;
    if (original !== 'linux') {
      const wrapped = wrapWithSandbox('curl https://example.com', '/tmp');
      assert.ok(wrapped.includes('ulimit'));
      assert.ok(wrapped.includes('curl'));
    }
  });

  it('includes resource limits', () => {
    const wrapped = wrapWithSandbox('echo test', '/tmp', {
      maxMemoryMB: 256,
      maxProcesses: 32,
    });
    assert.ok(wrapped.includes('ulimit'));
    assert.ok(wrapped.includes('echo test'));
  });

  it('wraps with unshare on linux when available', () => {
    if (process.platform === 'linux') {
      resetSandboxCache();
      const wrapped = wrapWithSandbox('curl https://example.com', '/tmp');
      if (wrapped.includes('unshare')) {
        assert.ok(wrapped.includes('--net'));
        assert.ok(wrapped.includes('--mount'));
        assert.ok(wrapped.includes('--pid'));
      } else {
        assert.ok(wrapped.includes('ulimit'));
      }
    }
  });

  it('allows network when configured', () => {
    if (process.platform === 'linux') {
      resetSandboxCache();
      const wrapped = wrapWithSandbox('curl https://example.com', '/tmp', {
        allowNetwork: true,
      });
      if (wrapped.includes('unshare')) {
        assert.ok(!wrapped.includes('--net'));
      }
    }
  });
});

describe('getSandboxStatus', () => {
  it('returns available status', () => {
    resetSandboxCache();
    const status = getSandboxStatus();
    assert.ok(typeof status.available === 'boolean');
    assert.ok(typeof status.method === 'string');
    assert.ok(['unshare', 'ulimits'].includes(status.method));
  });
});

describe('bash tool sandbox integration', () => {
  it('sandbox does not alter safe commands', async () => {
    const { bashTool } = await import('../src/tools/bash');
    const result = await bashTool.execute(
      { command: 'echo hello' },
      { cwd: '/tmp', abortSignal: new AbortController().signal },
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('hello'));
  });

  it('dangerouslyDisableSandbox is accepted without error', async () => {
    const { bashTool } = await import('../src/tools/bash');
    const result = await bashTool.execute(
      { command: 'echo safe', dangerouslyDisableSandbox: true },
      { cwd: '/tmp', abortSignal: new AbortController().signal },
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('safe'));
  });
});

describe('resetSandboxCache', () => {
  it('clears the unshare detection cache', () => {
    resetSandboxCache();
    const s1 = getSandboxStatus();
    resetSandboxCache();
    const s2 = getSandboxStatus();
    assert.equal(s1.method, s2.method);
  });
});

describe('sandbox maxProcesses — fork exhaustion regression', () => {
  it('BUG: ulimit -u 64 kills fork on any machine with >64 threads', () => {
    // ROOT CAUSE: The sandbox set maxProcesses:64, which translates to
    // `ulimit -u 64`. ulimit -u is a PER-USER limit counting ALL threads
    // across the entire UID — Chrome, Slack, Claude sessions, everything.
    // A typical desktop has 2000+ threads. Setting ulimit -u 64 causes
    // every fork() inside the sandbox to fail with EAGAIN (exit 254):
    //
    //   bash: fork: retry: Resource temporarily unavailable
    //   bash: fork: Resource temporarily unavailable
    //   [exit code: 254]
    //
    // FIX: Raised maxProcesses to 8192.

    resetSandboxCache();
    const wrapped = wrapWithSandbox('python3 test.py', '/home/user');

    const match = wrapped.match(/ulimit -u (\d+)/);
    assert.ok(match, 'sandbox must set ulimit -u');
    const nproc = parseInt(match[1], 10);

    assert.ok(nproc > 64,
      `ulimit -u ${nproc} is too low — causes fork exhaustion when user has >64 threads`);
    assert.ok(nproc >= 4096,
      `ulimit -u ${nproc} must be >= 4096 to handle typical desktop thread counts (2000+)`);
  });

  it('default maxProcesses allows sandboxed commands to fork on busy systems', () => {
    resetSandboxCache();
    // Commands that trigger the sandbox (python, curl, bash -c)
    // must be able to fork even when the user has thousands of threads
    for (const cmd of ['python3 test.py', 'curl https://example.com', 'bash -c "echo hi"']) {
      const wrapped = wrapWithSandbox(cmd, '/tmp');
      const match = wrapped.match(/ulimit -u (\d+)/);
      assert.ok(match, `sandbox for "${cmd}" must set ulimit -u`);
      const nproc = parseInt(match[1], 10);
      assert.ok(nproc >= 4096,
        `"${cmd}": ulimit -u ${nproc} too low — needs >= 4096`);
    }
  });

  it('custom maxProcesses overrides default', () => {
    resetSandboxCache();
    const wrapped = wrapWithSandbox('python3 test.py', '/tmp', { maxProcesses: 16384 });
    const match = wrapped.match(/ulimit -u (\d+)/);
    assert.ok(match);
    assert.equal(parseInt(match[1], 10), 16384);
  });
});
