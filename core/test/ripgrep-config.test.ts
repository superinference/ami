import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  resolveRipgrepConfig,
  getRipgrepStatus,
  probeRipgrep,
  resetRipgrepStatus,
  ripgrepHooks,
  USE_BUILTIN_RIPGREP_ENV,
  RIPGREP_PATH_ENV,
} from '../src/tools/ripgrep-config';

const savedProbe = ripgrepHooks.execFileSync;

afterEach(() => {
  resetRipgrepStatus();
  ripgrepHooks.execFileSync = savedProbe;
});

describe('resolveRipgrepConfig – USE_BUILTIN_RIPGREP / AMI_RIPGREP_PATH knob', () => {
  it('defaults to the bare name `rg` in system mode (no env)', () => {
    const c = resolveRipgrepConfig({});
    assert.equal(c.mode, 'system');
    assert.equal(c.command, 'rg');
  });

  it('prefers a configured AMI_RIPGREP_PATH as the builtin tier', () => {
    const c = resolveRipgrepConfig({ [RIPGREP_PATH_ENV]: '/opt/rg/bin/rg' });
    assert.equal(c.mode, 'builtin');
    assert.equal(c.command, '/opt/rg/bin/rg');
  });

  it('USE_BUILTIN_RIPGREP=false forces system `rg`, ignoring the configured path', () => {
    const c = resolveRipgrepConfig({ [USE_BUILTIN_RIPGREP_ENV]: 'false', [RIPGREP_PATH_ENV]: '/opt/rg/bin/rg' });
    assert.equal(c.mode, 'system');
    assert.equal(c.command, 'rg');
  });

  it('USE_BUILTIN_RIPGREP=0/no/off are all falsy (force system)', () => {
    for (const v of ['0', 'no', 'off', 'FALSE']) {
      const c = resolveRipgrepConfig({ [USE_BUILTIN_RIPGREP_ENV]: v, [RIPGREP_PATH_ENV]: '/x/rg' });
      assert.equal(c.command, 'rg', `value ${v} must force system`);
    }
  });

  it('never returns a bare relative path (PATH-hijack protection)', () => {
    // With no configured absolute path, we must fall back to the OS-resolved name `rg`.
    const c = resolveRipgrepConfig({ [USE_BUILTIN_RIPGREP_ENV]: 'true' });
    assert.equal(c.command, 'rg');
  });
});

describe('getRipgrepStatus / probeRipgrep – cached self-test', () => {
  it('reports working=null before any probe', () => {
    resetRipgrepStatus();
    const s = getRipgrepStatus({});
    assert.equal(s.working, null);
    assert.equal(s.mode, 'system');
    assert.equal(s.path, 'rg');
  });

  it('probe returns true and caches when rg reports a ripgrep version', () => {
    let calls = 0;
    ripgrepHooks.execFileSync = (() => { calls++; return 'ripgrep 14.1.0\n'; }) as typeof ripgrepHooks.execFileSync;
    assert.equal(probeRipgrep({}), true);
    assert.equal(getRipgrepStatus({}).working, true);
    // Second call served from cache (no second spawn).
    assert.equal(probeRipgrep({}), true);
    assert.equal(calls, 1);
  });

  it('probe returns false when the binary is missing (throws)', () => {
    ripgrepHooks.execFileSync = (() => { throw Object.assign(new Error('nope'), { code: 'ENOENT' }); }) as typeof ripgrepHooks.execFileSync;
    assert.equal(probeRipgrep({}), false);
    assert.equal(getRipgrepStatus({}).working, false);
  });

  it('probe returns false when a non-ripgrep binary exits 0 with wrong output', () => {
    ripgrepHooks.execFileSync = (() => 'GNU grep 3.7\n') as typeof ripgrepHooks.execFileSync;
    assert.equal(probeRipgrep({}), false);
  });

  it('re-probes when the resolved binary changes (cache keyed by command)', () => {
    let calls = 0;
    ripgrepHooks.execFileSync = (() => { calls++; return 'ripgrep 14.1.0\n'; }) as typeof ripgrepHooks.execFileSync;
    probeRipgrep({});
    // Different configured path → cache miss → second probe.
    probeRipgrep({ [RIPGREP_PATH_ENV]: '/opt/rg/bin/rg' });
    assert.equal(calls, 2);
    // Status for the new path reflects the fresh probe.
    const s = getRipgrepStatus({ [RIPGREP_PATH_ENV]: '/opt/rg/bin/rg' });
    assert.equal(s.mode, 'builtin');
    assert.equal(s.path, '/opt/rg/bin/rg');
    assert.equal(s.working, true);
  });

  it('getRipgrepStatus reports working=null for a path that differs from the cached probe', () => {
    ripgrepHooks.execFileSync = (() => 'ripgrep 14.1.0\n') as typeof ripgrepHooks.execFileSync;
    probeRipgrep({});
    // Cached status is for `rg`; asking about a different binary must not reuse it.
    assert.equal(getRipgrepStatus({ [RIPGREP_PATH_ENV]: '/other/rg' }).working, null);
  });
});
