import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  getSettingsJSONSchema,
  MANAGED_ONLY_KEYS,
  TIER_LOCKED_KEYS,
  DANGEROUS_SHELL_SETTINGS,
} from '../src/settings-guard';

// Parity: the reference's generateSettingsJSONSchema (utils/settings/schemaOutput.ts).
// AMI has no emitted JSON Schema for its settings file; this generator derives one
// from the settings-guard module's own authoritative key sets so the schema can
// never drift from the governed keys (no hardcoded key list — it self-updates).
describe('getSettingsJSONSchema', () => {
  it('is a well-formed draft-07 object schema with additionalProperties allowed', () => {
    const schema = getSettingsJSONSchema();
    assert.equal(schema.$schema, 'http://json-schema.org/draft-07/schema#');
    assert.equal(schema.type, 'object');
    assert.equal(typeof schema.title, 'string');
    assert.ok(schema.title.length > 0);
    // A partial schema for the governed keys: other settings keys must not be
    // rejected (mirrors the reference's non-strict settings schema).
    assert.equal(schema.additionalProperties, true);
    assert.equal(typeof schema.properties, 'object');
  });

  it('includes a property for EVERY managed-only key (self-updating denominator)', () => {
    const schema = getSettingsJSONSchema();
    for (const key of MANAGED_ONLY_KEYS) {
      assert.ok(
        Object.prototype.hasOwnProperty.call(schema.properties, key),
        `managed-only key "${key}" is missing from the generated schema`,
      );
      const prop = schema.properties[key];
      assert.ok(typeof prop.description === 'string' && prop.description.length > 0,
        `managed-only key "${key}" must carry a description`);
      assert.equal(prop.managed, true, `managed-only key "${key}" must be flagged managed`);
    }
  });

  it('includes a property for every tier-locked key, flagged tierLocked', () => {
    const schema = getSettingsJSONSchema();
    for (const key of TIER_LOCKED_KEYS) {
      const prop = schema.properties[key];
      assert.ok(prop, `tier-locked key "${key}" is missing from the generated schema`);
      assert.equal(prop.tierLocked, true, `tier-locked key "${key}" must be flagged tierLocked`);
    }
  });

  it('types numeric tier-locked budget keys as number', () => {
    const schema = getSettingsJSONSchema();
    for (const key of ['maxTurns', 'maxBudgetUsd', 'maxSessionMs']) {
      assert.equal(schema.properties[key]?.type, 'number', `${key} should be typed number`);
    }
    // permissionMode is an enum of the known permission modes (string).
    assert.equal(schema.properties.permissionMode?.type, 'string');
    assert.ok(Array.isArray(schema.properties.permissionMode?.enum));
    assert.ok((schema.properties.permissionMode!.enum as string[]).includes('ask'));
  });

  it('types dangerous shell settings as string with a danger note', () => {
    const schema = getSettingsJSONSchema();
    for (const key of DANGEROUS_SHELL_SETTINGS) {
      const prop = schema.properties[key];
      assert.ok(prop, `dangerous shell setting "${key}" missing`);
      assert.equal(prop.type, 'string');
      assert.match(prop.description ?? '', /shell|command|execut/i);
    }
  });

  it('classifies array vs boolean vs object managed keys', () => {
    const schema = getSettingsJSONSchema();
    // boolean kill-switch
    assert.equal(schema.properties.disableAllHooks?.type, 'boolean');
    // string-array policy list
    assert.equal(schema.properties.allowedMcpServers?.type, 'array');
    assert.equal(schema.properties.allowedMcpServers?.items?.type, 'string');
    // object map
    assert.equal(schema.properties.managedMcpServers?.type, 'object');
    // union key (boolean | array of surfaces) is left permissive (no single type)
    assert.ok(schema.properties.strictPluginOnlyCustomization);
  });

  it('is deterministic (stable JSON across calls)', () => {
    assert.equal(
      JSON.stringify(getSettingsJSONSchema()),
      JSON.stringify(getSettingsJSONSchema()),
    );
  });
});
