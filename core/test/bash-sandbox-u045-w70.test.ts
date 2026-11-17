import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { release } from 'node:os';
import {
  detectWslVersion,
  getSandboxPlatformSupport,
  checkSandboxDependencies,
  resetSandboxCache,
  _sandboxPlatformHooks,
  type WslVersion,
} from '../src/tools/bash-sandbox';

// ---------------------------------------------------------------------------
// UNIT U045 (w70) — WSL1-vs-WSL2 sandbox platform-support gating (parity with
//   Claude Code's isSupportedPlatform() WSL special-casing:
//   commands/sandbox-toggle/sandbox-toggle.tsx L14-30). WSL2 runs a real Linux
//   kernel (user namespaces + unshare work → sandbox runs); WSL1 is a syscall-
//   translation shim with NO namespace support, so the host sandbox cannot run
//   there. AMI previously surfaced a confusing generic "unshare could not create
//   a user namespace" error on WSL1 instead of a targeted "requires WSL2" one.
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

afterEach(() => {
  _sandboxPlatformHooks.release = release;
  resetSandboxCache();
});

// ---------------------------------------------------------------------------
// detectWslVersion — pure kernel-string classifier
// ---------------------------------------------------------------------------
describe('detectWslVersion (pure WSL-generation classifier)', () => {
  it('returns "none" for a native Linux kernel', () => {
    assert.equal(detectWslVersion('5.15.0-91-generic'), 'none');
    assert.equal(detectWslVersion('6.1.0-18-amd64'), 'none');
    assert.equal(detectWslVersion(''), 'none');
  });

  it('returns "wsl2" for a WSL2 kernel string (microsoft-standard-WSL2)', () => {
    assert.equal(detectWslVersion('5.15.90.1-microsoft-standard-WSL2'), 'wsl2');
    assert.equal(detectWslVersion('5.10.102.1-microsoft-standard-WSL2+'), 'wsl2');
  });

  it('returns "wsl2" when only the microsoft-standard tag is present', () => {
    assert.equal(detectWslVersion('5.10.16.3-microsoft-standard'), 'wsl2');
  });

  it('returns "wsl1" for an older Microsoft kernel with no WSL2 tag', () => {
    assert.equal(detectWslVersion('4.4.0-19041-Microsoft'), 'wsl1');
    assert.equal(detectWslVersion('4.4.0-18362-Microsoft'), 'wsl1');
  });

  it('is case-insensitive on the marker', () => {
    assert.equal(detectWslVersion('4.4.0-MICROSOFT'), 'wsl1');
    assert.equal(detectWslVersion('X-WSL'), 'wsl1');
  });
});

// ---------------------------------------------------------------------------
// getSandboxPlatformSupport — injectable platform + WSL generation
// ---------------------------------------------------------------------------
describe('getSandboxPlatformSupport (platform/WSL gating)', () => {
  it('supports native Linux (no WSL)', () => {
    const s = getSandboxPlatformSupport('linux', 'none');
    assert.equal(s.supported, true);
    assert.equal(s.reason, undefined);
  });

  it('supports WSL2', () => {
    const s = getSandboxPlatformSupport('linux', 'wsl2');
    assert.equal(s.supported, true);
  });

  it('refuses WSL1 with a targeted "requires WSL2" reason (not a generic unshare error)', () => {
    const s = getSandboxPlatformSupport('linux', 'wsl1');
    assert.equal(s.supported, false);
    assert.ok(s.reason);
    assert.ok(/WSL2/.test(s.reason!), 'reason names the WSL2 requirement');
    assert.ok(/WSL1 is not supported/i.test(s.reason!));
    assert.ok(!/unshare/i.test(s.reason!), 'WSL1 message must NOT be the generic unshare failure');
  });

  it('refuses non-Linux platforms, naming the platform', () => {
    for (const plat of ['darwin', 'win32'] as const) {
      const s = getSandboxPlatformSupport(plat, 'none');
      assert.equal(s.supported, false);
      assert.ok(s.reason!.includes('Linux'));
      assert.ok(s.reason!.includes(plat));
    }
  });
});

// ---------------------------------------------------------------------------
// Integration: checkSandboxDependencies surfaces the WSL1 error FIRST (parity
// pre-flight), ahead of the generic unshare branch. os.release() is mocked so
// the branch is exercised deterministically on any host.
// ---------------------------------------------------------------------------
describe('checkSandboxDependencies — WSL1 pre-flight wiring', () => {
  function mockRelease(kernel: string): void {
    _sandboxPlatformHooks.release = () => kernel;
    resetSandboxCache();
  }

  it('emits the WSL2-requirement error on a WSL1 host (not the unshare message)', () => {
    withPlatform('linux', () => {
      mockRelease('4.4.0-19041-Microsoft');
      const r = checkSandboxDependencies();
      assert.ok(
        r.errors.some((e) => /WSL2/.test(e) && /WSL1 is not supported/i.test(e)),
        `expected a WSL2-requirement error, got: ${JSON.stringify(r.errors)}`,
      );
      // The confusing generic unshare failure must NOT be what the user sees.
      assert.ok(
        !r.errors.some((e) => /could not create a user namespace/i.test(e)),
        'WSL1 must not fall through to the generic unshare error',
      );
    });
  });

  it('does NOT emit a WSL error on a genuine WSL2 host', () => {
    withPlatform('linux', () => {
      mockRelease('5.15.90.1-microsoft-standard-WSL2');
      const r = checkSandboxDependencies();
      assert.ok(
        !r.errors.some((e) => /WSL1 is not supported/i.test(e)),
        'WSL2 must not be gated as WSL1',
      );
    });
  });

  it('checkSandboxDependencies delegates to getSandboxPlatformSupport on the live platform', () => {
    // Behavioral tie: whenever the live platform is unsupported, its reason is the
    // first error. (On a supported host this is vacuously true.)
    const support = getSandboxPlatformSupport();
    if (!support.supported) {
      const r = checkSandboxDependencies();
      assert.ok(r.errors.includes(support.reason!));
    }
  });
});

// Type-level: WslVersion is the exported union used by both helpers.
const _v: WslVersion = 'wsl2';
void _v;
