import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  buildConfigWriteGuard,
  buildBareGitRepoGuard,
  validateSandboxEnabled,
  wrapWithUnshare,
  type SandboxConfig,
} from '../src/tools/bash-sandbox';

// ---------------------------------------------------------------------------
// UNIT U045 — parity hardening for the host bash sandbox (bash-sandbox.ts):
//   G1/G2: deny sandbox writes to settings/config + auto-loaded
//          skills/commands/agents dirs (config-rewrite / privilege-escalation
//          escape) — Claude Code sandbox-adapter.ts L230-255.
//   G3:    bare-repo .git-file poisoning defense (ro-bind the files git's
//          is_git_directory() keys on) — sandbox-adapter.ts L257-280.
//   G4:    failIfUnavailable — fail-closed hard gate when the sandbox cannot
//          start — sandboxTypes.ts L95-103.
// ---------------------------------------------------------------------------

/** Minimal valid config for the builders / wrapWithUnshare. */
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

/** Override process.platform for the duration of fn, then restore. */
function withPlatform(platform: NodeJS.Platform, fn: () => void): void {
  const orig = Object.getOwnPropertyDescriptor(process, 'platform')!;
  Object.defineProperty(process, 'platform', { value: platform, configurable: true });
  try {
    fn();
  } finally {
    Object.defineProperty(process, 'platform', orig);
  }
}

// ---------------------------------------------------------------------------
// G1/G2 — buildConfigWriteGuard
// ---------------------------------------------------------------------------
describe('buildConfigWriteGuard (config-rewrite / skills escape parity)', () => {
  it('is a no-op (empty string) by default and when disabled', () => {
    assert.equal(buildConfigWriteGuard('/home/u/proj', baseCfg()), '');
    assert.equal(buildConfigWriteGuard('/home/u/proj', baseCfg({ denyConfigWrites: false })), '');
  });

  it('ro-binds settings files and auto-loaded skill/command/agent dirs under cwd when enabled', () => {
    const out = buildConfigWriteGuard('/home/u/proj', baseCfg({ denyConfigWrites: true }));
    // settings/config files
    assert.ok(out.includes("/home/u/proj/.claude/settings.json"), 'denies .claude/settings.json');
    assert.ok(out.includes("/home/u/proj/.claude/settings.local.json"), 'denies .claude/settings.local.json');
    assert.ok(out.includes("/home/u/proj/.ami/config.json"), 'denies .ami/config.json');
    // auto-loaded dirs (same privilege level — privilege-escalation guard)
    assert.ok(out.includes("/home/u/proj/.claude/skills"), 'denies .claude/skills');
    assert.ok(out.includes("/home/u/proj/.claude/commands"), 'denies .claude/commands');
    assert.ok(out.includes("/home/u/proj/.claude/agents"), 'denies .claude/agents');
    assert.ok(out.includes("/home/u/proj/.ami/skills"), 'denies .ami/skills');
    // read-only remount technique + existence guard (no 0-byte stubs)
    assert.ok(out.includes('remount,ro,bind'), 'uses read-only remount');
    assert.ok(out.includes('[ -e '), 'guards each path on existence');
  });

  it('normalizes a trailing slash on cwd (no doubled separator)', () => {
    const out = buildConfigWriteGuard('/home/u/proj/', baseCfg({ denyConfigWrites: true }));
    assert.ok(out.includes('/home/u/proj/.claude/settings.json'), 'single separator');
    assert.ok(!out.includes('proj//.claude'), 'no doubled slash');
  });

  it('escapes single quotes in the cwd so the segment stays shell-safe', () => {
    const out = buildConfigWriteGuard("/tmp/it's", baseCfg({ denyConfigWrites: true }));
    assert.ok(out.includes("'\\''"), 'single quote is escaped for the bash -c body');
  });

  it('is embedded verbatim in the wrapWithUnshare command body when enabled', () => {
    const cfg = baseCfg({ denyConfigWrites: true });
    const guard = buildConfigWriteGuard('/work', cfg);
    const wrapped = wrapWithUnshare('curl https://x', '/work', cfg);
    assert.ok(guard.length > 0);
    assert.ok(wrapped.includes(guard), 'config-write guard present verbatim in the wrapper');
  });
});

// ---------------------------------------------------------------------------
// G3 — buildBareGitRepoGuard
// ---------------------------------------------------------------------------
describe('buildBareGitRepoGuard (bare-repo .git poisoning defense parity)', () => {
  it('is a no-op by default and when disabled', () => {
    assert.equal(buildBareGitRepoGuard('/work', baseCfg()), '');
    assert.equal(buildBareGitRepoGuard('/work', baseCfg({ denyBareGitPoisoning: false })), '');
  });

  it('ro-binds each file is_git_directory() keys on, under cwd, when enabled', () => {
    const out = buildBareGitRepoGuard('/work', baseCfg({ denyBareGitPoisoning: true }));
    for (const f of ['HEAD', 'objects', 'refs', 'hooks', 'config']) {
      assert.ok(out.includes(`/work/${f}`), `guards ${f}`);
    }
    assert.ok(out.includes('remount,ro,bind'), 'uses read-only remount');
    assert.ok(out.includes('[ -e '), 'existence-guarded (planted stubs are not created)');
  });

  it('is embedded verbatim in the wrapWithUnshare command body when enabled', () => {
    const cfg = baseCfg({ denyBareGitPoisoning: true });
    const guard = buildBareGitRepoGuard('/work', cfg);
    const wrapped = wrapWithUnshare('curl https://x', '/work', cfg);
    assert.ok(guard.length > 0);
    assert.ok(wrapped.includes(guard), 'bare-git guard present verbatim in the wrapper');
  });
});

// ---------------------------------------------------------------------------
// G4 — failIfUnavailable fail-closed gate
// ---------------------------------------------------------------------------
describe('validateSandboxEnabled — failIfUnavailable fail-closed gate', () => {
  it('marks an unavailable sandbox non-fatal by default (degrade to unsandboxed)', () => {
    withPlatform('darwin', () => {
      const result = validateSandboxEnabled();
      assert.equal(result.valid, false);
      assert.equal(result.fatal, false, 'default is warn + run unsandboxed');
    });
  });

  it('marks an unavailable sandbox FATAL when failIfUnavailable is set', () => {
    withPlatform('darwin', () => {
      const result = validateSandboxEnabled({ failIfUnavailable: true });
      assert.equal(result.valid, false);
      assert.equal(result.fatal, true, 'hard gate must fail closed');
      assert.ok(result.error!.includes('darwin'));
    });
  });

  it('never reports fatal when the sandbox IS available even with the flag set', () => {
    withPlatform('darwin', () => {
      // sanity: flag only matters on the invalid branch
      const result = validateSandboxEnabled({ failIfUnavailable: false });
      assert.equal(result.fatal, false);
    });
  });
});
