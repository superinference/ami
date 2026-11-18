// U251 (wave 113) — copyFullResponse is a recognized, type-checked, mergeable config
// key (parity: Claude Code's global-config `copyFullResponse` knob, copy.tsx).
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  findUnknownConfigKeys,
  mergeConfigs,
  getConfigJSONSchema,
  validateProjectConfigValues,
} from '../src/config';

describe('copyFullResponse config key', () => {
  it('is a RECOGNIZED key (not flagged as unknown)', () => {
    assert.deepEqual(findUnknownConfigKeys({ copyFullResponse: true }), []);
  });

  it('is surfaced in the JSON schema as a boolean (so /config + introspect list it)', () => {
    const schema = getConfigJSONSchema();
    assert.ok(schema.properties.copyFullResponse, 'copyFullResponse present in config schema');
    assert.equal(schema.properties.copyFullResponse.type, 'boolean');
  });

  it('merges across the waterfall (a later layer overrides an earlier one)', () => {
    const merged = mergeConfigs({}, {}, { copyFullResponse: true }, { copyFullResponse: false });
    // project layer wins over global here (project is applied after global).
    assert.equal(merged.copyFullResponse, true);
    const mergedGlobalOnly = mergeConfigs({}, {}, null, { copyFullResponse: true });
    assert.equal(mergedGlobalOnly.copyFullResponse, true);
  });

  it('rejects a non-boolean value via the typed field validator', () => {
    const issues = validateProjectConfigValues({ copyFullResponse: 'yes' as unknown as boolean });
    assert.ok(issues.some((i) => i.field === 'copyFullResponse'), 'a string value is a type issue');
    assert.equal(validateProjectConfigValues({ copyFullResponse: true }).length, 0);
  });
});
