import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  mergeConfigs,
  getConfigJSONSchema,
  validateProjectConfigValues,
  findUnknownConfigKeys,
  type ProjectConfig,
} from '../src/config';

// ---------------------------------------------------------------------------
// Gap U011/w87: the persisted `subagentDefaultModel` config key is a FIRST-CLASS
// knob — recognized (not flagged unknown), type-validated, merged across layers,
// and surfaced in the config JSON schema (→ `ami --introspect`). Previously
// EngineConfig.subagentDefaultModel existed but was never sourced from user config.
// ---------------------------------------------------------------------------
describe('subagentDefaultModel — recognized user config knob', () => {
  it('is a known key (never reported as unknown)', () => {
    const cfg: ProjectConfig = { subagentDefaultModel: 'haiku' };
    assert.ok(!findUnknownConfigKeys(cfg).includes('subagentDefaultModel'));
  });

  it('appears in the config JSON schema as a string property', () => {
    const schema = getConfigJSONSchema();
    const prop = schema.properties?.subagentDefaultModel as { type?: string } | undefined;
    assert.ok(prop, 'subagentDefaultModel must be in the config schema');
    assert.equal(prop?.type, 'string');
  });

  it('accepts a model string and rejects a non-string value', () => {
    assert.equal(
      validateProjectConfigValues({ subagentDefaultModel: 'inherit' }).some(i => i.field === 'subagentDefaultModel'),
      false,
    );
    const bad = validateProjectConfigValues({ subagentDefaultModel: 123 as unknown as string });
    assert.equal(bad.some(i => i.field === 'subagentDefaultModel'), true);
  });

  it('is carried through the config merge waterfall (cli layer wins over global)', () => {
    // mergeConfigs(cliArgs, envVars, projectConfig, globalConfig, ...) — cli is highest.
    const merged = mergeConfigs(
      { subagentDefaultModel: 'sonnet' },
      {},
      null,
      { subagentDefaultModel: 'haiku' },
    );
    assert.equal(merged.subagentDefaultModel, 'sonnet');
  });

  it('carries a single layer value through unchanged', () => {
    const merged = mergeConfigs({}, {}, { subagentDefaultModel: 'haiku' }, null);
    assert.equal(merged.subagentDefaultModel, 'haiku');
  });

  it('leaves subagentDefaultModel unset when no layer provides it', () => {
    const merged = mergeConfigs({ model: 'claude-opus-4-8' }, {}, null, null);
    assert.equal(merged.subagentDefaultModel, undefined);
  });
});
