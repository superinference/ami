import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  checkSandboxDependencies,
  getSandboxStatus,
  validateSandboxEnabled,
  resetSandboxCache,
  type SandboxConfig,
} from '../src/tools/bash-sandbox';

// ---------------------------------------------------------------------------
// UNIT U045 (dependency-diagnostics cluster) — parity with the reference's
// SandboxManager.checkDependencies() two-tier { errors, warnings } contract:
//   - structured dependency check (errors vs warnings)                 [medium]
//   - warn when a requested control (allowedDomains) silently cannot be
//     enforced because iptables is missing (security footgun)          [HIGH]
//   - non-fatal degraded-capability warnings surfaced to the user      [medium]
//   - validateSandboxEnabled enumerates the SPECIFIC missing dependency [medium]
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

/** Run fn with PATH pointed at an empty dir so no external command resolves. */
function withEmptyPath(fn: () => void): void {
  const saved = process.env.PATH;
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-empty-path-'));
  process.env.PATH = dir;
  resetSandboxCache();
  try {
    fn();
  } finally {
    process.env.PATH = saved;
    resetSandboxCache();
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

/** Run fn with PATH containing a fake `unshare` that resolves but exits non-zero
 *  (simulating "unshare installed but user namespaces disabled"). */
function withBrokenUnshare(fn: () => void): void {
  const saved = process.env.PATH;
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-broken-unshare-'));
  const bin = path.join(dir, 'unshare');
  fs.writeFileSync(bin, '#!/bin/sh\nexit 1\n', { mode: 0o755 });
  // Keep `command -v` (POSIX builtin) + our fake on PATH; nothing else resolves.
  process.env.PATH = dir;
  resetSandboxCache();
  try {
    fn();
  } finally {
    process.env.PATH = saved;
    resetSandboxCache();
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

function cfg(overrides: Partial<SandboxConfig> = {}): Partial<SandboxConfig> {
  return { allowNetwork: false, ...overrides };
}

// ---------------------------------------------------------------------------
// checkSandboxDependencies — structured { errors, warnings }
// ---------------------------------------------------------------------------
describe('checkSandboxDependencies — two-tier errors/warnings (the reference parity)', () => {
  it('returns the documented shape (arrays)', () => {
    const r = checkSandboxDependencies();
    assert.ok(Array.isArray(r.errors));
    assert.ok(Array.isArray(r.warnings));
  });

  it('errors enumerate the platform on non-Linux, no config warnings', () => {
    withPlatform('darwin', () => {
      const r = checkSandboxDependencies();
      assert.equal(r.errors.length, 1);
      assert.ok(r.errors[0]!.includes('Linux'));
      assert.ok(r.errors[0]!.includes('darwin'));
      assert.deepEqual(r.warnings, []);
    });
  });

  it('names `unshare` as NOT INSTALLED when it is absent on Linux', () => {
    withPlatform('linux', () => {
      withEmptyPath(() => {
        const r = checkSandboxDependencies();
        assert.equal(r.errors.length, 1);
        assert.ok(r.errors[0]!.includes('unshare'));
        assert.ok(/not installed/i.test(r.errors[0]!), 'should say not installed');
      });
    });
  });

  it('distinguishes "present but user namespaces disabled" from not-installed', () => {
    withPlatform('linux', () => {
      withBrokenUnshare(() => {
        const r = checkSandboxDependencies();
        assert.equal(r.errors.length, 1);
        assert.ok(r.errors[0]!.includes('unshare'));
        assert.ok(/user namespace/i.test(r.errors[0]!), 'should cite user namespaces');
        assert.ok(!/not installed/i.test(r.errors[0]!), 'must NOT be the not-installed message');
      });
    });
  });

  it('HIGH: warns that allowedDomains is unenforced when iptables is missing', () => {
    withPlatform('linux', () => {
      withEmptyPath(() => {
        const r = checkSandboxDependencies(cfg({ allowedDomains: ['example.com'] }));
        const iptablesWarn = r.warnings.find((w) => w.includes('iptables'));
        assert.ok(iptablesWarn, 'must warn about missing iptables');
        assert.ok(
          /will NOT be enforced/i.test(iptablesWarn!),
          'must say the allowlist is not enforced (security footgun)',
        );
      });
    });
  });

  it('warns that getent is required to resolve an allow/deny domain list', () => {
    withPlatform('linux', () => {
      withEmptyPath(() => {
        const r = checkSandboxDependencies(cfg({ deniedDomains: ['evil.com'] }));
        assert.ok(r.warnings.some((w) => w.includes('getent')), 'must warn about getent');
      });
    });
  });

  it('emits NO degradation warning when no allow/deny list is requested', () => {
    withPlatform('linux', () => {
      withEmptyPath(() => {
        // No allowedDomains/deniedDomains → the network allowlist path is never
        // taken, so a missing iptables/getent is not a degradation.
        const r = checkSandboxDependencies(cfg());
        assert.ok(!r.warnings.some((w) => w.includes('iptables')));
        assert.ok(!r.warnings.some((w) => w.includes('getent')));
      });
    });
  });

  it('allowedDomains with allowNetwork:true is not treated as an allowlist', () => {
    withPlatform('linux', () => {
      withEmptyPath(() => {
        const r = checkSandboxDependencies({ allowNetwork: true, allowedDomains: ['example.com'] });
        assert.ok(!r.warnings.some((w) => w.includes('iptables')), 'allowlist only applies with allowNetwork:false');
      });
    });
  });

  it('reports no errors AND no warnings on a healthy Linux host with unshare', () => {
    if (getSandboxStatus().method !== 'unshare') return; // only when the real ns sandbox works
    const r = checkSandboxDependencies();
    assert.deepEqual(r.errors, []);
    assert.deepEqual(r.warnings, []);
  });
});

// ---------------------------------------------------------------------------
// getSandboxStatus — warnings channel
// ---------------------------------------------------------------------------
describe('getSandboxStatus — warnings channel', () => {
  it('always returns a warnings array', () => {
    const s = getSandboxStatus();
    assert.ok(Array.isArray(s.warnings));
  });

  it('folds the namespace-unavailable diagnostic into warnings on the ulimits fallback', () => {
    withPlatform('darwin', () => {
      const s = getSandboxStatus();
      assert.equal(s.method, 'ulimits');
      assert.ok(s.warnings.some((w) => w.includes('Linux')), 'degraded isolation must be surfaced');
    });
  });

  it('surfaces the allowedDomains-unenforced footgun through the status (end-to-end)', () => {
    withPlatform('linux', () => {
      withEmptyPath(() => {
        const s = getSandboxStatus(cfg({ allowedDomains: ['example.com'] }));
        assert.ok(s.warnings.some((w) => w.includes('iptables')));
      });
    });
  });
});

// ---------------------------------------------------------------------------
// validateSandboxEnabled — enumerates the specific missing dependency
// ---------------------------------------------------------------------------
describe('validateSandboxEnabled — specific-dependency enumeration', () => {
  it('names the platform (not a generic message) on non-Linux', () => {
    withPlatform('darwin', () => {
      const r = validateSandboxEnabled();
      assert.equal(r.valid, false);
      assert.ok(r.error!.includes('darwin'));
      assert.ok(r.error!.includes('Linux'));
    });
  });

  it('names `unshare` as not installed when absent on Linux', () => {
    withPlatform('linux', () => {
      withEmptyPath(() => {
        const r = validateSandboxEnabled();
        assert.equal(r.valid, false);
        assert.ok(r.error!.includes('unshare'));
        assert.ok(/not installed/i.test(r.error!));
      });
    });
  });

  it('carries the failIfUnavailable fatal flag onto the enumerated error', () => {
    withPlatform('linux', () => {
      withEmptyPath(() => {
        const r = validateSandboxEnabled({ failIfUnavailable: true });
        assert.equal(r.valid, false);
        assert.equal(r.fatal, true);
        assert.ok(r.error!.includes('unshare'));
      });
    });
  });
});
