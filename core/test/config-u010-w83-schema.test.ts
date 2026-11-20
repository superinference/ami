import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  getConfigJSONSchema,
  CONFIG_SCHEMA_URL,
  KNOWN_CONFIG_KEYS,
  findUnknownConfigKeys,
  validateProjectConfigValues,
  type ProjectConfig,
} from '../src/config';

// U010 / w83: JSON Schema emission for the FULL project-config key set + `$schema`
// reference-key support (parity: the reference's generateSettingsJSONSchema +
// CLAUDE_CODE_SETTINGS_SCHEMA_URL / `$schema`). The schema is DERIVED from the
// authoritative validation tables so it self-updates and never drops a knob.

describe('getConfigJSONSchema — full project-config JSON Schema emission', () => {
  it('emits a draft-07 object schema identified by the published URL', () => {
    const schema = getConfigJSONSchema();
    assert.equal(schema.$schema, 'http://json-schema.org/draft-07/schema#');
    assert.equal(schema.$id, CONFIG_SCHEMA_URL);
    assert.equal(schema.type, 'object');
    // Non-strict: AMI never fails closed on an unrecognized config key (it warns).
    assert.equal(schema.additionalProperties, true);
    assert.ok(schema.title.includes('.ami/config.json'), 'title names the config file');
  });

  it('CONFIG_SCHEMA_URL is an https documentation URL', () => {
    assert.match(CONFIG_SCHEMA_URL, /^https:\/\/\S+\.json$/);
  });

  it('is deterministic (two calls are deep-equal)', () => {
    assert.deepEqual(getConfigJSONSchema(), getConfigJSONSchema());
    // Property key order is stable (sorted) across calls.
    assert.deepEqual(
      Object.keys(getConfigJSONSchema().properties),
      Object.keys(getConfigJSONSchema().properties),
    );
  });

  it('covers EVERY recognized config knob (dynamic denominator — no key dropped)', () => {
    const props = getConfigJSONSchema().properties;
    for (const key of KNOWN_CONFIG_KEYS) {
      // `$schema` is editor-only meta, deliberately NOT a schema property.
      if (key === '$schema') {
        assert.ok(!(key in props), '$schema meta key is not emitted as a property');
        continue;
      }
      assert.ok(key in props, `schema advertises the "${key}" knob`);
      assert.ok(props[key]!.description.length > 0, `"${key}" has a description`);
    }
  });

  it('maps each base field type to the right JSON-Schema type', () => {
    const props = getConfigJSONSchema().properties;
    assert.equal(props['model']!.type, 'string');
    assert.equal(props['maxTurns']!.type, 'number');
    assert.equal(props['safeMode']!.type, 'boolean');
    assert.equal(props['superinference']!.type, 'object');
    // string[] → array of string items
    assert.equal(props['enforceAvailableModels']!.type, 'array');
    assert.deepEqual(props['enforceAvailableModels']!.items, { type: 'string' });
    // mcpAllowlist union → array with a permissive item schema (both shapes validate)
    assert.equal(props['allowedMcpServers']!.type, 'array');
    assert.deepEqual(props['allowedMcpServers']!.items, {});
  });

  it('emits enum fields with their allowed value list', () => {
    const props = getConfigJSONSchema().properties;
    assert.equal(props['permissionMode']!.type, 'string');
    assert.deepEqual(props['permissionMode']!.enum, ['ask', 'auto-allow', 'deny-all']);
    assert.deepEqual(props['cacheTtl']!.enum, ['5m', '1h']);
  });

  it('leaves union/custom-shape keys permissive (no fixed type)', () => {
    const props = getConfigJSONSchema().properties;
    assert.equal(props['statusLine']!.type, undefined);
    assert.equal(props['strictPluginOnlyCustomization']!.type, undefined);
    assert.ok(props['statusLine']!.description.length > 0);
  });
});

describe('`$schema` reference-key support in a config file', () => {
  it('is a recognized key (no spurious unknown-key typo warning)', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('$schema'));
    const config = { $schema: CONFIG_SCHEMA_URL, model: 'gpt-5' } as unknown as ProjectConfig;
    assert.deepEqual(findUnknownConfigKeys(config), []);
  });

  it('is skipped by value validation (it is meta, not a typed knob)', () => {
    // A `$schema` string must never surface as a validation issue even though it is not
    // in the typed field tables.
    const config = { $schema: CONFIG_SCHEMA_URL } as unknown as ProjectConfig;
    const issues = validateProjectConfigValues(config);
    assert.deepEqual(issues, [], 'no issue raised for the `$schema` meta key');
  });

  it('still surfaces a genuinely-unknown key alongside an accepted `$schema`', () => {
    const config = { $schema: CONFIG_SCHEMA_URL, notAKnownKey: 1 } as unknown as ProjectConfig;
    assert.deepEqual(findUnknownConfigKeys(config), ['notAKnownKey']);
  });
});
