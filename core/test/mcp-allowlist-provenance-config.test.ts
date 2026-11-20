import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  mergeConfigs,
  validateProjectConfigValues,
  isMcpAllowedServerEntry,
  type ProjectConfig,
} from '../src/config';

after(() => { setTimeout(() => process.exit(0), 200); });

// ---------------------------------------------------------------------------
// U018 (w82) — the `allowedMcpServers` config knob now accepts provenance-scoped
// entries (the reference-parity channelAllowlist) in addition to name/URL strings, and
// those entries validate + accumulate across config layers. This is the
// user-facing end of the provenance allowlist capability (the enforcement end is
// McpManager.setAllowedServers / enforceAllowlist).
// ---------------------------------------------------------------------------

describe('isMcpAllowedServerEntry', () => {
  it('accepts strings and provenance objects, rejects malformed entries', () => {
    assert.equal(isMcpAllowedServerEntry('my-server'), true);
    assert.equal(isMcpAllowedServerEntry({ pluginSource: 'acme' }), true);
    assert.equal(isMcpAllowedServerEntry({ marketplace: 'official', plugin: 'acme' }), true);
    // malformed
    assert.equal(isMcpAllowedServerEntry({ marketplace: 'official' }), false);
    assert.equal(isMcpAllowedServerEntry({ plugin: 'acme' }), false);
    assert.equal(isMcpAllowedServerEntry({ pluginSource: 5 }), false);
    assert.equal(isMcpAllowedServerEntry(['nested']), false);
    assert.equal(isMcpAllowedServerEntry(null), false);
    assert.equal(isMcpAllowedServerEntry(42), false);
  });
});

describe('validateProjectConfigValues — allowedMcpServers provenance entries', () => {
  it('accepts a mixed array of strings and provenance objects', () => {
    const issues = validateProjectConfigValues({
      allowedMcpServers: ['by-name', { pluginSource: 'acme' }, { marketplace: 'official', plugin: 'acme' }],
    } as ProjectConfig);
    assert.equal(issues.filter((i) => i.field === 'allowedMcpServers').length, 0);
  });

  it('rejects a malformed entry with an actionable tip', () => {
    const issues = validateProjectConfigValues({
      allowedMcpServers: [{ marketplace: 'official' }] as unknown as ProjectConfig['allowedMcpServers'],
    } as ProjectConfig);
    const issue = issues.find((i) => i.field === 'allowedMcpServers');
    assert.ok(issue, 'expected a validation issue for the malformed allowlist entry');
    assert.match(issue!.tip ?? '', /provenance objects/);
  });
});

describe('mergeConfigs — allowedMcpServers provenance accumulation', () => {
  it('accumulates string + provenance entries across layers, de-duplicating by canonical form', () => {
    const global: ProjectConfig = { allowedMcpServers: ['shared', { pluginSource: 'acme' }] };
    const project: ProjectConfig = {
      allowedMcpServers: ['shared', { pluginSource: 'acme' }, { marketplace: 'official', plugin: 'beta' }],
    };
    const merged = mergeConfigs({}, {}, project, global);
    assert.deepEqual(merged.allowedMcpServers, [
      'shared',
      { pluginSource: 'acme' },
      { marketplace: 'official', plugin: 'beta' },
    ]);
  });
});
