import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { coerceBool } from '../src/utils/coerce';

describe('coerceBool', () => {
  it('real boolean true/false pass through', () => {
    assert.equal(coerceBool(true), true);
    assert.equal(coerceBool(false), false);
  });

  it('string "true" (the provider-serialized form) is true', () => {
    assert.equal(coerceBool('true'), true);
    assert.equal(coerceBool('TRUE'), true);
    assert.equal(coerceBool('  true  '), true);
  });

  it('string "false" is false (NOT truthy — the bash !! bug)', () => {
    assert.equal(coerceBool('false'), false);
    assert.equal(coerceBool('FALSE'), false);
  });

  it('other strings are false', () => {
    assert.equal(coerceBool('yes'), false);
    assert.equal(coerceBool('1'), false);
    assert.equal(coerceBool(''), false);
  });

  it('nullish / numbers / objects are false', () => {
    assert.equal(coerceBool(undefined), false);
    assert.equal(coerceBool(null), false);
    assert.equal(coerceBool(1), false);
    assert.equal(coerceBool(0), false);
    assert.equal(coerceBool({}), false);
  });
});
