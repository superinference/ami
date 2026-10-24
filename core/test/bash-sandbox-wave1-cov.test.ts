import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  shouldUseSandbox,
  wrapWithSandbox,
  getSandboxStatus,
  validateSandboxEnabled,
  resetSandboxCache,
} from '../src/tools/bash-sandbox';

// ---------------------------------------------------------------------------
// Platform + unshare-availability dependent branches.
//
// These branches are gated on `process.platform` and on whether the `unshare`
// binary succeeds. On a typical CI/dev host only ONE combination is naturally
// exercised (e.g. linux-without-usable-unshare), leaving the other branches
// uncovered. We force each combination deterministically:
//   - process.platform is overridden via Object.defineProperty.
//   - unshare availability is forced by prepending a temp dir containing a
//     fake `unshare` executable (exit 0 => "available") to PATH, then resetting
//     the detection cache. Removing the fake / restoring PATH forces the
//     "unavailable" result when unshare really is missing.
// Every override is restored in a finally block and the cache is reset so no
// state leaks to sibling tests.
// ---------------------------------------------------------------------------

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

// A temp dir holding a fake `unshare` that always exits 0. Used to force
// isUnshareAvailable() === true regardless of the host.
const fakeBinDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-fake-unshare-'));
fs.writeFileSync(path.join(fakeBinDir, 'unshare'), '#!/bin/sh\nexit 0\n', { mode: 0o755 });

const ORIG_PATH = process.env.PATH;

/** Run fn with unshare detection forced to "available". */
function withUnshareAvailable(fn: () => void): void {
  process.env.PATH = fakeBinDir + path.delimiter + (ORIG_PATH ?? '');
  resetSandboxCache();
  try {
    fn();
  } finally {
    process.env.PATH = ORIG_PATH;
    resetSandboxCache();
  }
}

after(() => {
  process.env.PATH = ORIG_PATH;
  resetSandboxCache();
  fs.rmSync(fakeBinDir, { recursive: true, force: true });
});

describe('shouldUseSandbox — exemption revealed only after quote-stripping', () => {
  it('exempts a command whose exempt keyword is split by quotes in the original', () => {
    // Original: `gi"x"t` is NOT a contiguous `git` token, so the first exempt
    // check (run on the raw command) does not match, and a `curl` trigger is
    // present. Quote-stripping removes "x", joining the fragments into `git`,
    // which the SECOND exempt check matches -> command must be exempted.
    // If the second exempt check were removed, the surviving `curl` trigger
    // would (wrongly) force the sandbox, so this test discriminates the branch.
    const cmd = 'gi"x"t pull && curl https://example.com';
    // Sanity: the first-layer exempt check (raw command) must NOT match, i.e.
    // there is no contiguous "git" token in the original.
    assert.ok(!/\bgit\b/.test(cmd), 'precondition: raw command has no git token');
    assert.equal(
      shouldUseSandbox(cmd),
      false,
      'quote-stripped exempt keyword must suppress the sandbox despite the curl trigger',
    );
  });
});

describe('wrapWithSandbox — non-linux platform path', () => {
  it('uses resource-limits-only wrapping on non-linux (no unshare, no --net)', () => {
    withPlatform('darwin', () => {
      const wrapped = wrapWithSandbox('curl https://example.com', '/tmp');
      // Must NOT attempt namespace isolation off-linux.
      assert.ok(!wrapped.includes('unshare'), 'non-linux must not use unshare');
      assert.ok(!wrapped.includes('--net'), 'non-linux has no network namespace');
      // Falls back to ulimits + the raw command.
      assert.ok(wrapped.includes('ulimit -v'), 'must still apply memory ulimit');
      assert.ok(wrapped.includes('curl https://example.com'), 'command preserved');
    });
  });

  it('non-linux resource-limits mode still scrubs hiddenEnvVars via unset', () => {
    withPlatform('win32', () => {
      const wrapped = wrapWithSandbox('python3 x.py', '/tmp', {
        hiddenEnvVars: ['OPENAI_API_KEY', 'AWS_SECRET_ACCESS_KEY'],
      });
      assert.ok(
        wrapped.includes('unset OPENAI_API_KEY AWS_SECRET_ACCESS_KEY; '),
        'resource-limits mode must unset hidden env vars',
      );
      // No mount-based file hiding is possible without a mount namespace.
      assert.ok(!wrapped.includes('mount --bind'), 'no mount hiding off-linux');
      assert.ok(wrapped.includes('python3 x.py'));
    });
  });

  it('non-linux resource-limits mode omits unset when no hiddenEnvVars', () => {
    withPlatform('darwin', () => {
      const wrapped = wrapWithSandbox('echo hi', '/tmp');
      assert.ok(!wrapped.includes('unset '), 'no unset when nothing hidden');
      assert.ok(wrapped.includes('echo hi'));
    });
  });

  it('non-linux resource-limits mode omits unset for an empty hiddenEnvVars array', () => {
    // Exercises the `hiddenEnvVars && hiddenEnvVars.length > 0` short-circuit
    // where the array is present (truthy) but empty (length === 0).
    withPlatform('darwin', () => {
      const wrapped = wrapWithSandbox('echo hi', '/tmp', { hiddenEnvVars: [] });
      assert.ok(!wrapped.includes('unset '), 'empty hiddenEnvVars must not emit unset');
      assert.ok(wrapped.includes('echo hi'));
    });
  });
});

describe('wrapWithSandbox — linux with unshare available', () => {
  it('routes to the unshare wrapper when unshare is available', () => {
    withPlatform('linux', () => {
      withUnshareAvailable(() => {
        const wrapped = wrapWithSandbox('curl https://example.com', '/tmp');
        assert.ok(wrapped.startsWith('unshare '), 'must wrap with unshare');
        assert.ok(wrapped.includes('--mount'));
        assert.ok(wrapped.includes('--pid'));
        assert.ok(wrapped.includes('--net'), 'network blocked by default');
        assert.ok(wrapped.includes('curl https://example.com'));
      });
    });
  });
});

describe('isUnshareAvailable — detection + caching', () => {
  it('caches a positive detection so repeated calls agree', () => {
    withPlatform('linux', () => {
      withUnshareAvailable(() => {
        // First call runs detection (fake unshare exits 0 -> available).
        const first = getSandboxStatus();
        // Second call must hit the cached value, not re-run detection.
        const second = getSandboxStatus();
        assert.equal(first.method, 'unshare');
        assert.equal(second.method, 'unshare');
      });
    });
  });

  it('reports unavailable when the unshare binary cannot be found', () => {
    withPlatform('linux', () => {
      const saved = process.env.PATH;
      // Point PATH at an empty dir so `unshare` resolution fails (catch branch).
      const emptyDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-empty-'));
      process.env.PATH = emptyDir;
      resetSandboxCache();
      try {
        const status = getSandboxStatus();
        assert.equal(status.method, 'ulimits', 'missing unshare => ulimits fallback');
        assert.equal(status.available, true);
      } finally {
        process.env.PATH = saved;
        resetSandboxCache();
        fs.rmSync(emptyDir, { recursive: true, force: true });
      }
    });
  });
});

describe('getSandboxStatus — platform/unshare matrix', () => {
  it('reports method=unshare on linux when unshare is available', () => {
    withPlatform('linux', () => {
      withUnshareAvailable(() => {
        const status = getSandboxStatus();
        assert.deepEqual(status, { available: true, method: 'unshare' });
      });
    });
  });

  it('reports method=ulimits on non-linux (short-circuits unshare check)', () => {
    withPlatform('darwin', () => {
      // Even with a fake unshare on PATH, non-linux must never claim unshare.
      withUnshareAvailable(() => {
        const status = getSandboxStatus();
        assert.deepEqual(status, { available: true, method: 'ulimits' });
      });
    });
  });
});

describe('validateSandboxEnabled — fail-closed matrix', () => {
  it('is valid on linux when unshare is available', () => {
    withPlatform('linux', () => {
      withUnshareAvailable(() => {
        const result = validateSandboxEnabled();
        assert.equal(result.valid, true);
        assert.equal(result.method, 'unshare');
        assert.equal(result.error, undefined);
      });
    });
  });

  it('is invalid on non-linux and names the current platform', () => {
    withPlatform('darwin', () => {
      const result = validateSandboxEnabled();
      assert.equal(result.valid, false);
      assert.ok(result.error!.includes('Linux'), 'error must mention Linux requirement');
      assert.ok(result.error!.includes('darwin'), 'error must name current platform');
      assert.equal(result.method, undefined);
    });
  });

  it('is invalid on linux when unshare is missing and mentions unshare', () => {
    withPlatform('linux', () => {
      const saved = process.env.PATH;
      const emptyDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-empty2-'));
      process.env.PATH = emptyDir;
      resetSandboxCache();
      try {
        const result = validateSandboxEnabled();
        assert.equal(result.valid, false);
        assert.ok(result.error!.includes('unshare'), 'error must mention unshare');
      } finally {
        process.env.PATH = saved;
        resetSandboxCache();
        fs.rmSync(emptyDir, { recursive: true, force: true });
      }
    });
  });
});
