/**
 * U030 — derive sandbox outbound-network allow/deny domains from web_fetch rules.
 *
 * Parity: the reference's sandbox-adapter folds WebFetch permission rules into the
 * sandbox network allow/deny lists so the same per-domain allowlist governs BOTH
 * the WebFetch tool and in-sandbox outbound traffic. AMI already enforces
 * `network.allowedHosts`/`deniedHosts` in the sandbox backend, but nothing ever
 * populated them from permissions — `getEffectiveSandboxPolicy` now does.
 *
 * RED on revert: without the web_fetch → network fold, the returned policy's
 * `network.allowedHosts`/`deniedHosts` stay undefined and the "same ref" / merge
 * assertions fail.
 *
 * Run: cd core/core && node --import tsx --test test/permissions-web-fetch-sandbox-domains-u030.test.ts
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PermissionManager } from '../src/permissions';
import type { SandboxPolicy, PermissionRule } from '../src/types';

function pmWith(rules: PermissionRule[]): PermissionManager {
  return new PermissionManager('ask', rules);
}

describe('getEffectiveSandboxPolicy — web_fetch domain fold (U030)', () => {
  it('folds web_fetch allow-domains into network.allowedHosts', () => {
    const pm = pmWith([
      { tool: 'web_fetch', pattern: 'github.com', action: 'allow' },
      { tool: 'web_fetch', pattern: '*.npmjs.org', action: 'allow' },
    ]);
    const base: SandboxPolicy = { mode: 'container', projectDir: '/proj' };
    const eff = pm.getEffectiveSandboxPolicy(base)!;
    assert.deepEqual(eff.network?.allowedHosts, ['github.com', '*.npmjs.org']);
    assert.equal(eff.network?.deniedHosts, undefined);
    // Base is not mutated.
    assert.equal(base.network, undefined);
  });

  it('folds web_fetch deny-domains into network.deniedHosts', () => {
    const pm = pmWith([
      { tool: 'web_fetch', pattern: 'evil.example', action: 'deny' },
    ]);
    const eff = pm.getEffectiveSandboxPolicy({ mode: 'container', projectDir: '/proj' })!;
    assert.deepEqual(eff.network?.deniedHosts, ['evil.example']);
    assert.equal(eff.network?.allowedHosts, undefined);
  });

  it('merges with and de-dupes caller-supplied network hosts', () => {
    const pm = pmWith([
      { tool: 'web_fetch', pattern: 'github.com', action: 'allow' },
      { tool: 'web_fetch', pattern: 'added.example', action: 'allow' },
    ]);
    const base: SandboxPolicy = {
      mode: 'container',
      projectDir: '/proj',
      network: { allowedHosts: ['github.com', 'pre.example'], httpProxyPort: 8080 },
    };
    const eff = pm.getEffectiveSandboxPolicy(base)!;
    assert.deepEqual(eff.network?.allowedHosts, ['github.com', 'pre.example', 'added.example']);
    // Other network fields are preserved.
    assert.equal(eff.network?.httpProxyPort, 8080);
  });

  it('skips tool-wide web_fetch rules (no specific domain) and non-web_fetch rules', () => {
    const pm = pmWith([
      { tool: 'web_fetch', action: 'allow' }, // no pattern → names no domain
      { tool: 'web_fetch', pattern: '*', action: 'allow' }, // wildcard → names no domain
      { tool: 'bash', pattern: 'npm:*', action: 'allow' },
    ]);
    const base: SandboxPolicy = { mode: 'container', projectDir: '/proj' };
    // Nothing to fold → base returned by identity.
    assert.equal(pm.getEffectiveSandboxPolicy(base), base);
  });

  it('normalizes the WebFetch tool-name alias', () => {
    const pm = pmWith([
      { tool: 'WebFetch', pattern: 'aliased.example', action: 'allow' },
    ]);
    const eff = pm.getEffectiveSandboxPolicy({ mode: 'container', projectDir: '/proj' })!;
    assert.deepEqual(eff.network?.allowedHosts, ['aliased.example']);
  });

  it('combines the domain fold with the allowed-read-root mount fold', () => {
    const pm = pmWith([
      { tool: 'web_fetch', pattern: 'github.com', action: 'allow' },
    ]);
    pm.registerAllowedRoot('/data/added');
    const eff = pm.getEffectiveSandboxPolicy({ mode: 'container', projectDir: '/proj' })!;
    assert.deepEqual(eff.network?.allowedHosts, ['github.com']);
    assert.ok(eff.filesystem?.allowWrite?.some((p) => p.endsWith('added')));
  });

  it('returns the base policy by identity when there is nothing to fold', () => {
    const pm = pmWith([{ tool: 'bash', pattern: 'git *', action: 'allow' }]);
    const base: SandboxPolicy = { mode: 'container', projectDir: '/proj' };
    assert.equal(pm.getEffectiveSandboxPolicy(base), base);
  });
});
