import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  mergeConfigs,
  validateProjectConfigValues,
  findUnknownConfigKeys,
  getConfigJSONSchema,
  type ProjectConfig,
} from '../src/config';

// ---------------------------------------------------------------------------
// Round-3 merge-drop / recognition fix: knobs that a `common/` resolver already
// reads off the MERGED project config (isPolicyAllowed, resolveMemoryCriticalRss,
// preferTmuxOverIterm2, resolvePolicyRefreshConfig) or off the RAW managed/global
// config (pluginTrustMessage, blockedMarketplaces, strictKnownMarketplaces), yet
// config.ts previously neither declared, value-validated, nor (for the merged-path
// keys) copied through mergeConfigs — so a value set in `.ami/config.json` was
// silently dropped AND flagged as an unknown-key typo.
// ---------------------------------------------------------------------------

describe('mergeConfigs — round-3 merged-path knobs are copied (not dropped)', () => {
  it('copies policyRestrictions from the project tier', () => {
    const project: ProjectConfig = { policyRestrictions: { computer_use: { allowed: false } } } as ProjectConfig;
    const merged = mergeConfigs({}, {}, project, null);
    assert.deepEqual(merged.policyRestrictions, { computer_use: { allowed: false } });
  });

  it('deep-merges policyRestrictions across layers (additive denies)', () => {
    const global: ProjectConfig = { policyRestrictions: { a: { allowed: false } } } as ProjectConfig;
    const project: ProjectConfig = { policyRestrictions: { b: { allowed: false } } } as ProjectConfig;
    const merged = mergeConfigs({}, {}, project, global);
    assert.deepEqual(merged.policyRestrictions, { a: { allowed: false }, b: { allowed: false } });
  });

  it('managed policyRestrictions is authoritative — a lower tier cannot relax a managed deny', () => {
    const managed: ProjectConfig = { policyRestrictions: { computer_use: { allowed: false } } } as ProjectConfig;
    // A project tries to re-allow the managed-denied policy.
    const project: ProjectConfig = { policyRestrictions: { computer_use: { allowed: true } } } as ProjectConfig;
    const merged = mergeConfigs({}, {}, project, null, null, managed);
    assert.equal(merged.policyRestrictions?.computer_use?.allowed, false, 'managed deny wins');
  });

  it('copies memoryCriticalRssBytes / preferTmuxOverIterm2 / policyRefresh scalars', () => {
    const project: ProjectConfig = {
      memoryCriticalRssBytes: 1_500_000_000,
      preferTmuxOverIterm2: true,
      policyRefresh: { pollIntervalMs: 90_000, maxRetries: 2 },
    } as ProjectConfig;
    const merged = mergeConfigs({}, {}, project, null);
    assert.equal(merged.memoryCriticalRssBytes, 1_500_000_000);
    assert.equal(merged.preferTmuxOverIterm2, true);
    assert.deepEqual(merged.policyRefresh, { pollIntervalMs: 90_000, maxRetries: 2 });
  });

  it('copies pluginTrustMessage and accumulates the marketplace policy lists', () => {
    const global: ProjectConfig = {
      pluginTrustMessage: 'See go/plugins',
      blockedMarketplaces: ['evil.example'],
      strictKnownMarketplaces: ['github.com'],
    } as ProjectConfig;
    const project: ProjectConfig = {
      blockedMarketplaces: ['bad.example'],
      strictKnownMarketplaces: ['gitlab.com'],
    } as ProjectConfig;
    const merged = mergeConfigs({}, {}, project, global);
    assert.equal(merged.pluginTrustMessage, 'See go/plugins');
    assert.deepEqual(merged.blockedMarketplaces, ['evil.example', 'bad.example']);
    assert.deepEqual(merged.strictKnownMarketplaces, ['github.com', 'gitlab.com']);
  });
});

describe('round-3 knobs are recognized config keys (no spurious unknown-key warning)', () => {
  it('findUnknownConfigKeys does not flag any of the round-3 knobs', () => {
    const config: ProjectConfig = {
      policyRestrictions: { x: { allowed: false } },
      pluginTrustMessage: 'hi',
      memoryCriticalRssBytes: 1,
      preferTmuxOverIterm2: false,
      policyRefresh: {},
      blockedMarketplaces: [],
      strictKnownMarketplaces: [],
    } as ProjectConfig;
    assert.deepEqual(findUnknownConfigKeys(config), []);
  });

  it('getConfigJSONSchema emits every round-3 knob', () => {
    const schema = getConfigJSONSchema();
    for (const key of ['policyRestrictions', 'pluginTrustMessage', 'memoryCriticalRssBytes', 'preferTmuxOverIterm2', 'policyRefresh', 'blockedMarketplaces', 'strictKnownMarketplaces']) {
      assert.ok(schema.properties[key], `schema missing ${key}`);
    }
  });
});

describe('validateProjectConfigValues — round-3 knob value validation', () => {
  it('flags a non-object policyRestrictions', () => {
    const issues = validateProjectConfigValues({ policyRestrictions: 'nope' } as unknown as ProjectConfig);
    assert.ok(issues.some(i => i.field === 'policyRestrictions'), 'should flag non-object policyRestrictions');
  });

  it('flags a policyRestrictions entry whose allowed is not a boolean', () => {
    const issues = validateProjectConfigValues({ policyRestrictions: { x: { allowed: 'yes' } } } as unknown as ProjectConfig);
    assert.ok(issues.some(i => i.field === 'policyRestrictions.x.allowed'), 'should flag non-boolean allowed');
  });

  it('accepts a well-formed policyRestrictions map', () => {
    const issues = validateProjectConfigValues({ policyRestrictions: { x: { allowed: false }, y: { allowed: true } } } as ProjectConfig);
    assert.equal(issues.filter(i => i.field.startsWith('policyRestrictions')).length, 0);
  });

  it('flags wrong base types for scalar/array round-3 knobs', () => {
    const issues = validateProjectConfigValues({
      pluginTrustMessage: 123,
      memoryCriticalRssBytes: 'big',
      preferTmuxOverIterm2: 'yes',
      blockedMarketplaces: 'evil.example',
      strictKnownMarketplaces: [1, 2],
    } as unknown as ProjectConfig);
    for (const f of ['pluginTrustMessage', 'memoryCriticalRssBytes', 'preferTmuxOverIterm2', 'blockedMarketplaces', 'strictKnownMarketplaces']) {
      assert.ok(issues.some(i => i.field === f), `should flag ${f}`);
    }
  });
});
