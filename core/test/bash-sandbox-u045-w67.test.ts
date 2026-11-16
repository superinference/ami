import { describe, it, afterEach, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { homedir, tmpdir } from 'os';
import { resolve as resolvePath, join } from 'path';
import { mkdtempSync, rmSync } from 'fs';
import {
  buildWriteDenials,
  normalizeSandboxFsPath,
  hostSandboxConfigFromPolicy,
  wrapWithUnshare,
  type SandboxConfig,
} from '../src/tools/bash-sandbox';
import { bashTool, _bashHooks } from '../src/tools/bash';
import type { SandboxPolicy, ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// UNIT U045 (w67) — two genuinely-missing host-sandbox parity knobs:
//   1. buildWriteDenials allowWrite-over-denyWrite precedence (the write mirror
//      of the existing buildReadDenials allowRead-over-denyRead precedence) plus
//      hostSandboxConfigFromPolicy mapping of `sandbox.filesystem.allowWrite`
//      (previously honored ONLY by the container backend — the host unshare
//      backend silently dropped it). This is the host home of Claude's nested
//      `SandboxFilesystemConfig.allowWrite`/`denyWrite` carve-out model and the
//      `permissions.additionalDirectories`/`--add-dir` write grant.
//   2. normalizeSandboxFsPath — CC-specific sandbox fs path conventions
//      (`//abs` → `/abs`, `~`/`~/x` home expansion, relative → cwd-relative),
//      mirroring Claude's `resolveSandboxFilesystemPath`.
// ---------------------------------------------------------------------------

function baseCfg(overrides: Partial<SandboxConfig> = {}): SandboxConfig {
  return {
    allowNetwork: false,
    readOnlyPaths: [],
    writablePaths: ['/tmp'],
    maxMemoryMB: 512,
    maxProcesses: 8192,
    timeout: 30000,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// buildWriteDenials — allowWrite wins over denyWrite for matching paths
// ---------------------------------------------------------------------------
describe('buildWriteDenials — allowWrite precedence over denyWrite', () => {
  // Paths are shell-escaped for embedding in the single-quoted `bash -c` body
  // (`'\''…'\''`), so match the remount rule loosely on the path (as the existing
  // buildWriteDenials parity test does) rather than on an exact quoted substring.
  const remount = (p: string): RegExp =>
    new RegExp('remount,ro,bind[^;]*' + p.replace(/[.*+?^${}()|[\]\\/]/g, '\\$&'));

  it('denies every denyWritePaths entry when no allowWritePaths are set', () => {
    const out = buildWriteDenials(baseCfg({ denyWritePaths: ['/ro/a', '/ro/b'] }));
    assert.ok(remount('/ro/a').test(out));
    assert.ok(remount('/ro/b').test(out));
  });

  it('re-allows an exact denied path listed in allowWritePaths', () => {
    const out = buildWriteDenials(
      baseCfg({ denyWritePaths: ['/ro/a', '/ro/b'], allowWritePaths: ['/ro/a'] }),
    );
    assert.ok(!out.includes('/ro/a'), 'exact allow-write path is NOT re-mounted read-only');
    assert.ok(remount('/ro/b').test(out), 'other path still read-only');
  });

  it('re-allows a denied path NESTED under an allowWritePaths region', () => {
    const out = buildWriteDenials(
      baseCfg({
        denyWritePaths: ['/data/project/build', '/data/other'],
        allowWritePaths: ['/data/project'],
      }),
    );
    assert.ok(!out.includes('build'), 'nested path inside allowed region stays writable');
    assert.ok(remount('/data/other').test(out), 'sibling outside region still ro');
  });

  it('does NOT re-allow a sibling with a shared name prefix (segment-aware)', () => {
    const out = buildWriteDenials(
      baseCfg({ denyWritePaths: ['/data-x/build'], allowWritePaths: ['/data'] }),
    );
    assert.ok(remount('/data-x/build').test(out), '/data-x is not under /data');
  });

  it('returns empty string when every denied path is re-allowed', () => {
    assert.equal(
      buildWriteDenials(baseCfg({ denyWritePaths: ['/w/a'], allowWritePaths: ['/w'] })),
      '',
    );
  });

  it('ignores blank allowWritePaths entries', () => {
    const out = buildWriteDenials(baseCfg({ denyWritePaths: ['/w/a'], allowWritePaths: ['   ', ''] }));
    assert.ok(remount('/w/a').test(out));
  });

  it('is unchanged (full deny) when allowWritePaths is absent', () => {
    const a = buildWriteDenials(baseCfg({ denyWritePaths: ['/x/y'] }));
    const b = buildWriteDenials(baseCfg({ denyWritePaths: ['/x/y'], allowWritePaths: [] }));
    assert.equal(a, b);
  });
});

// ---------------------------------------------------------------------------
// normalizeSandboxFsPath — CC path conventions
// ---------------------------------------------------------------------------
describe('normalizeSandboxFsPath — CC sandbox fs path conventions', () => {
  it('returns an absolute path unchanged', () => {
    assert.equal(normalizeSandboxFsPath('/etc/secret', '/proj'), '/etc/secret');
  });

  it('strips the leading // escape (//abs → /abs)', () => {
    assert.equal(normalizeSandboxFsPath('//.aws/creds', '/proj'), '/.aws/creds');
  });

  it('expands bare ~ to the home directory', () => {
    assert.equal(normalizeSandboxFsPath('~'), homedir());
  });

  it('expands ~/path to a home-relative path', () => {
    assert.equal(normalizeSandboxFsPath('~/.ssh/id_rsa'), resolvePath(homedir(), '.ssh/id_rsa'));
  });

  it('resolves a relative path against cwd when provided', () => {
    assert.equal(normalizeSandboxFsPath('build/out', '/proj'), '/proj/build/out');
    assert.equal(normalizeSandboxFsPath('./dist', '/proj'), '/proj/dist');
  });

  it('leaves a relative path untouched when no cwd is supplied', () => {
    assert.equal(normalizeSandboxFsPath('build/out'), 'build/out');
  });

  it('returns a blank input unchanged (no spurious resolution)', () => {
    assert.equal(normalizeSandboxFsPath('   ', '/proj'), '   ');
    assert.equal(normalizeSandboxFsPath(''), '');
  });

  it('trims surrounding whitespace before classifying', () => {
    assert.equal(normalizeSandboxFsPath('  /abs  ', '/proj'), '/abs');
  });
});

// ---------------------------------------------------------------------------
// hostSandboxConfigFromPolicy — maps + normalizes filesystem.allowWrite
// ---------------------------------------------------------------------------
describe('hostSandboxConfigFromPolicy — allowWrite mapping + path normalization', () => {
  it('maps sandbox.filesystem.allowWrite to allowWritePaths', () => {
    const policy: SandboxPolicy = {
      mode: 'container',
      filesystem: { allowWrite: ['/data/writable'] },
    };
    assert.deepEqual(hostSandboxConfigFromPolicy(policy), {
      allowWritePaths: ['/data/writable'],
    });
  });

  it('normalizes ~/// / relative fs entries with cwd', () => {
    const policy: SandboxPolicy = {
      mode: 'container',
      filesystem: {
        allowWrite: ['dist', '~/.cache'],
        denyWrite: ['//.git/config'],
        denyRead: ['secrets.env'],
        allowRead: ['/abs/ok'],
      },
    };
    assert.deepEqual(hostSandboxConfigFromPolicy(policy, '/proj'), {
      allowWritePaths: ['/proj/dist', resolvePath(homedir(), '.cache')],
      denyWritePaths: ['/.git/config'],
      denyReadPaths: ['/proj/secrets.env'],
      allowReadPaths: ['/abs/ok'],
    });
  });

  it('an absolute-only policy maps identically with or without cwd (fully additive)', () => {
    const policy: SandboxPolicy = {
      mode: 'container',
      filesystem: { denyWrite: ['/ro'], allowWrite: ['/rw'], denyRead: ['/sec'], allowRead: ['/sec/ok'] },
    };
    const expected = {
      denyWritePaths: ['/ro'],
      allowWritePaths: ['/rw'],
      denyReadPaths: ['/sec'],
      allowReadPaths: ['/sec/ok'],
    };
    assert.deepEqual(hostSandboxConfigFromPolicy(policy), expected);
    assert.deepEqual(hostSandboxConfigFromPolicy(policy, '/proj'), expected);
  });

  it('omits allowWritePaths when filesystem.allowWrite is unset', () => {
    assert.deepEqual(
      hostSandboxConfigFromPolicy({ mode: 'container', filesystem: { denyWrite: ['/ro'] } }),
      { denyWritePaths: ['/ro'] },
    );
  });
});

// ---------------------------------------------------------------------------
// End-to-end: policy → hostSandboxConfigFromPolicy → wrapWithUnshare
// (the full host translation+wrap chain honors allowWrite precedence)
// ---------------------------------------------------------------------------
describe('wrapWithUnshare — allowWrite precedence end-to-end from a policy', () => {
  it('re-opens an allowWrite path nested in a denyWrite region, denies the rest', () => {
    const policy: SandboxPolicy = {
      mode: 'container',
      filesystem: {
        denyWrite: ['locked', 'locked/sub/keep'],
        allowWrite: ['locked/sub'],
      },
    };
    const cfg = hostSandboxConfigFromPolicy(policy, '/home/u/proj');
    const out = wrapWithUnshare('echo hi', '/home/u/proj', baseCfg(cfg));
    assert.ok(
      /remount,ro,bind[^;]*\/home\/u\/proj\/locked/.test(out),
      'the broad denyWrite region is still read-only',
    );
    assert.ok(
      !out.includes('/home/u/proj/locked/sub/keep'),
      'the denyWrite path nested in the allowWrite region is re-opened (not re-mounted ro)',
    );
    assert.ok(out.includes('cd /home/u/proj && echo hi'), 'command still runs');
  });
});

// ---------------------------------------------------------------------------
// Integration (bash tool wiring): bashTool.execute threads context.cwd into
// hostSandboxConfigFromPolicy so a relative/allowWrite carve-out actually
// reaches the host sandbox wrapper. Captured at the wrapWithSandbox seam so the
// assertion is deterministic regardless of unshare availability on the host.
// ---------------------------------------------------------------------------
describe('bashTool.execute — filesystem.allowWrite + cwd threading (host backend)', () => {
  // Use a REAL existing dir as cwd so bash's cwd-validation does not fall back to
  // process.cwd(); relative fs carve-outs then resolve against it deterministically.
  const realCwd = mkdtempSync(join(tmpdir(), 'u045-w67-'));
  afterEach(() => mock.restoreAll());
  after(() => rmSync(realCwd, { recursive: true, force: true }));

  function ctx(overrides: Partial<ToolContext> = {}): ToolContext {
    return {
      cwd: realCwd,
      abortSignal: new AbortController().signal,
      // mode 'off' → the host (non-container) branch, which reads the fs carve-outs.
      sandbox: {
        mode: 'off',
        filesystem: { denyWrite: ['locked'], allowWrite: ['locked/build'] },
      } as SandboxPolicy,
      ...overrides,
    } as ToolContext;
  }

  async function captureHostConfig(): Promise<Partial<SandboxConfig>> {
    let captured: Partial<SandboxConfig> = {};
    mock.method(_bashHooks, 'wrapWithSandbox', (cmd: string, _cwd: string, config: Partial<SandboxConfig>) => {
      captured = config;
      return cmd; // run raw so execCommand succeeds without a real sandbox
    });
    mock.method(_bashHooks, 'execCommand', async () => ({ stdout: '', stderr: '', exitCode: 0 }));
    const result = await bashTool.execute({ command: 'curl https://example.com' }, ctx());
    assert.equal(result.isError, false, `execute should succeed: ${JSON.stringify(result)}`);
    return captured;
  }

  it('builds a host SandboxConfig with cwd-resolved allowWrite/denyWrite carve-outs', async () => {
    const config = await captureHostConfig();
    // Discriminating: if bash.ts dropped filesystem.allowWrite (the pre-fix host
    // behavior) or failed to thread context.cwd, these cwd-resolved paths would be
    // absent / unresolved.
    assert.deepEqual(config.allowWritePaths, [resolvePath(realCwd, 'locked/build')]);
    assert.deepEqual(config.denyWritePaths, [resolvePath(realCwd, 'locked')]);
  });
});
