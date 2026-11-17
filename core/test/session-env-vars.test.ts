import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isValidEnvVarName,
  getSessionEnvVars,
  sessionEnvOverrides,
  setSessionEnvVar,
  deleteSessionEnvVar,
  clearSessionEnvVars,
  parseEnvAssignment,
} from '../src/session-env-vars';

// Keep each test isolated: clear every bucket touched.
afterEach(() => {
  clearSessionEnvVars('s1');
  clearSessionEnvVars('s2');
  clearSessionEnvVars();
});

describe('session-env-vars — name validation', () => {
  it('accepts valid identifiers', () => {
    assert.equal(isValidEnvVarName('FOO'), true);
    assert.equal(isValidEnvVarName('_x'), true);
    assert.equal(isValidEnvVarName('A1_B2'), true);
  });
  it('rejects invalid identifiers', () => {
    assert.equal(isValidEnvVarName(''), false);
    assert.equal(isValidEnvVarName('1FOO'), false);
    assert.equal(isValidEnvVarName('FO-O'), false);
    assert.equal(isValidEnvVarName('FO O'), false);
    assert.equal(isValidEnvVarName('A=B'), false);
  });
});

describe('session-env-vars — set/get/delete/clear', () => {
  it('sets and reads back a var for a session', () => {
    const r = setSessionEnvVar('FOO', 'bar', 's1');
    assert.deepEqual(r, { ok: true, name: 'FOO', value: 'bar' });
    assert.equal(getSessionEnvVars('s1').get('FOO'), 'bar');
    assert.deepEqual(sessionEnvOverrides('s1'), { FOO: 'bar' });
  });

  it('rejects an invalid name without storing it', () => {
    const r = setSessionEnvVar('1BAD', 'x', 's1');
    assert.equal(r.ok, false);
    assert.match(r.error!, /invalid variable name/);
    assert.equal(getSessionEnvVars('s1').size, 0);
  });

  it('isolates vars between sessions', () => {
    setSessionEnvVar('FOO', 'one', 's1');
    setSessionEnvVar('FOO', 'two', 's2');
    assert.equal(getSessionEnvVars('s1').get('FOO'), 'one');
    assert.equal(getSessionEnvVars('s2').get('FOO'), 'two');
  });

  it('falls back to a shared default bucket for an absent session id', () => {
    setSessionEnvVar('FOO', 'def', undefined);
    assert.equal(getSessionEnvVars(undefined).get('FOO'), 'def');
    assert.equal(getSessionEnvVars('').get('FOO'), 'def');
    // but not a named session
    assert.equal(getSessionEnvVars('s1').has('FOO'), false);
  });

  it('returns an empty (not shared-mutable) map for an unknown session', () => {
    const m = getSessionEnvVars('nope');
    assert.equal(m.size, 0);
  });

  it('deletes a var and reports whether it existed', () => {
    setSessionEnvVar('FOO', 'bar', 's1');
    assert.equal(deleteSessionEnvVar('FOO', 's1'), true);
    assert.equal(deleteSessionEnvVar('FOO', 's1'), false);
    assert.equal(deleteSessionEnvVar('NEVER', 'nosession'), false);
  });

  it('clear returns the number removed and empties the session', () => {
    setSessionEnvVar('A', '1', 's1');
    setSessionEnvVar('B', '2', 's1');
    assert.equal(clearSessionEnvVars('s1'), 2);
    assert.equal(getSessionEnvVars('s1').size, 0);
    assert.equal(clearSessionEnvVars('s1'), 0);
  });

  it('sessionEnvOverrides returns an empty object when nothing is set', () => {
    assert.deepEqual(sessionEnvOverrides('empty'), {});
  });
});

describe('session-env-vars — parseEnvAssignment', () => {
  it('parses NAME=VALUE', () => {
    assert.deepEqual(parseEnvAssignment('FOO=bar'), { name: 'FOO', value: 'bar' });
  });
  it('keeps extra = in the value', () => {
    assert.deepEqual(parseEnvAssignment('FOO=a=b'), { name: 'FOO', value: 'a=b' });
  });
  it('allows an empty value (NAME=)', () => {
    assert.deepEqual(parseEnvAssignment('FOO='), { name: 'FOO', value: '' });
  });
  it('rejects a token with no = or a leading =', () => {
    assert.equal(parseEnvAssignment('FOO'), null);
    assert.equal(parseEnvAssignment('=bar'), null);
  });
  it('rejects an invalid name', () => {
    assert.equal(parseEnvAssignment('1FOO=bar'), null);
  });
});
