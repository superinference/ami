import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  hostSandboxConfigFromPolicy,
  wrapWithUnshare,
  type SandboxConfig,
} from '../src/tools/bash-sandbox';
import {
  validateEngineConfig,
  sanitizeEngineConfig,
  type SandboxPolicy,
} from '../src/types';

import { isolateSandboxGrants } from './_isolate-sandbox-grants';

isolateSandboxGrants();

// ---------------------------------------------------------------------------
// UNIT U045 (w83) — ENTERPRISE managed-policy sandbox lockdown parity:
//   sandbox.network.allowManagedDomainsOnly + managedAllowedHosts
//     — forces the outbound allowlist to the managed list ALONE, ignoring any
//       user `allowedHosts` (incl. folded web_fetch-allow rules). Empty managed
//       list => no outbound host allowed (full egress block). deny still wins.
//       Mirrors the reference's shouldAllowManagedSandboxDomainsOnly().
//   sandbox.filesystem.allowManagedReadPathsOnly + managedAllowRead
//     — forces read RE-allow carve-outs to the managed list ALONE, ignoring user
//       `allowRead`. Mirrors the reference's allowManagedReadPathsOnly.
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

describe('hostSandboxConfigFromPolicy — managed-domains-only network lockdown', () => {
  it('uses ONLY managedAllowedHosts and ignores user allowedHosts when locked', () => {
    const policy: SandboxPolicy = {
      mode: 'off',
      network: {
        allowedHosts: ['evil.example.com', 'leak.test'],
        managedAllowedHosts: ['registry.corp.example'],
        allowManagedDomainsOnly: true,
      },
    };
    const cfg = hostSandboxConfigFromPolicy(policy);
    assert.deepEqual(cfg.allowedDomains, ['registry.corp.example']);
    assert.ok(!cfg.allowedDomains!.includes('evil.example.com'));
    assert.ok(!cfg.allowedDomains!.includes('leak.test'));
  });

  it('blocks ALL outbound (empty allowlist) when locked with no managed hosts', () => {
    const policy: SandboxPolicy = {
      mode: 'off',
      network: {
        allowedHosts: ['user.example.com'],
        allowManagedDomainsOnly: true,
      },
    };
    const cfg = hostSandboxConfigFromPolicy(policy);
    assert.deepEqual(cfg.allowedDomains, []);
  });

  it('still honors deniedHosts under the managed lockdown (deny wins)', () => {
    const policy: SandboxPolicy = {
      mode: 'off',
      network: {
        managedAllowedHosts: ['a.corp', 'b.corp'],
        deniedHosts: ['b.corp'],
        allowManagedDomainsOnly: true,
      },
    };
    const cfg = hostSandboxConfigFromPolicy(policy);
    assert.deepEqual(cfg.allowedDomains, ['a.corp', 'b.corp']);
    assert.deepEqual(cfg.deniedDomains, ['b.corp']);
  });

  it('does NOT return a managed copy that aliases the policy array', () => {
    const managed = ['x.corp'];
    const policy: SandboxPolicy = {
      mode: 'off',
      network: { managedAllowedHosts: managed, allowManagedDomainsOnly: true },
    };
    const cfg = hostSandboxConfigFromPolicy(policy);
    (cfg.allowedDomains as string[]).push('mutated');
    assert.deepEqual(managed, ['x.corp'], 'source array must not be mutated');
  });

  it('REGRESSION: without the flag, user allowedHosts flows through unchanged', () => {
    const policy: SandboxPolicy = {
      mode: 'off',
      network: {
        allowedHosts: ['user.example.com'],
        managedAllowedHosts: ['unused.corp'],
      },
    };
    const cfg = hostSandboxConfigFromPolicy(policy);
    assert.deepEqual(cfg.allowedDomains, ['user.example.com']);
  });
});

describe('hostSandboxConfigFromPolicy — managed-read-paths-only filesystem lockdown', () => {
  it('uses ONLY managedAllowRead and ignores user allowRead when locked', () => {
    const policy: SandboxPolicy = {
      mode: 'off',
      filesystem: {
        denyRead: ['/secret'],
        allowRead: ['/secret/user-opened'],
        managedAllowRead: ['/secret/allowed-by-admin'],
        allowManagedReadPathsOnly: true,
      },
    };
    const cfg = hostSandboxConfigFromPolicy(policy);
    assert.deepEqual(cfg.allowReadPaths, ['/secret/allowed-by-admin']);
    assert.ok(!cfg.allowReadPaths!.includes('/secret/user-opened'));
  });

  it('re-opens NOTHING (empty allowRead) when locked with no managed read paths', () => {
    const policy: SandboxPolicy = {
      mode: 'off',
      filesystem: {
        denyRead: ['/secret'],
        allowRead: ['/secret/user-opened'],
        allowManagedReadPathsOnly: true,
      },
    };
    const cfg = hostSandboxConfigFromPolicy(policy);
    assert.deepEqual(cfg.allowReadPaths, []);
  });

  it('REGRESSION: without the flag, user allowRead flows through unchanged', () => {
    const policy: SandboxPolicy = {
      mode: 'off',
      filesystem: {
        denyRead: ['/secret'],
        allowRead: ['/secret/user-opened'],
        managedAllowRead: ['/unused'],
      },
    };
    const cfg = hostSandboxConfigFromPolicy(policy);
    assert.deepEqual(cfg.allowReadPaths, ['/secret/user-opened']);
  });
});

describe('managed lockdown — end-to-end through the host unshare wrapper', () => {
  it('iptables allowlist contains the managed host and NOT the user host', () => {
    const cfg = hostSandboxConfigFromPolicy({
      mode: 'off',
      network: {
        allowedHosts: ['evil.example.com'],
        managedAllowedHosts: ['registry.corp.example'],
        allowManagedDomainsOnly: true,
      },
    });
    const wrapped = wrapWithUnshare('curl registry.corp.example', '/work', baseCfg(cfg));
    assert.ok(wrapped.includes("getent ahosts 'registry.corp.example'"));
    assert.ok(!wrapped.includes('evil.example.com'));
    // The allowlist is active, so the default-deny DROP rule is present.
    assert.ok(wrapped.includes('iptables -A OUTPUT -j DROP'));
  });

  it('builds NO iptables allowlist (isolated netns = deny-all) when managed list is empty', () => {
    const cfg = hostSandboxConfigFromPolicy({
      mode: 'off',
      network: { allowedHosts: ['evil.example.com'], allowManagedDomainsOnly: true },
    });
    const wrapped = wrapWithUnshare('true', '/work', baseCfg(cfg));
    assert.ok(!wrapped.includes('getent ahosts'));
    assert.ok(!wrapped.includes('evil.example.com'));
    // Own network namespace is still created (no outbound route => deny-all).
    assert.ok(wrapped.includes('--net'));
  });
});

describe('managed lockdown — config validation + sanitization', () => {
  it('rejects empty-string managed host / read-path entries', () => {
    const res = validateEngineConfig({
      sandbox: {
        mode: 'off',
        network: { managedAllowedHosts: ['ok.corp', '  '] },
        filesystem: { managedAllowRead: ['/ok', ''] },
      },
    });
    assert.equal(res.valid, false);
    assert.ok(res.warnings.some((w) => w.includes('managedAllowedHosts')));
    assert.ok(res.warnings.some((w) => w.includes('managedAllowRead')));
  });

  it('trims + dedupes managed lists and preserves the lockdown flags', () => {
    const out = sanitizeEngineConfig({
      sandbox: {
        mode: 'off',
        network: { managedAllowedHosts: [' a.corp ', 'a.corp', 'b.corp'], allowManagedDomainsOnly: true },
        filesystem: { managedAllowRead: [' /x ', '/x'], allowManagedReadPathsOnly: true },
      },
    });
    assert.deepEqual(out.sandbox!.network!.managedAllowedHosts, ['a.corp', 'b.corp']);
    assert.equal(out.sandbox!.network!.allowManagedDomainsOnly, true);
    assert.deepEqual(out.sandbox!.filesystem!.managedAllowRead, ['/x']);
    assert.equal(out.sandbox!.filesystem!.allowManagedReadPathsOnly, true);
  });
});
