import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  coerceBool,
  coerceBoolStrict,
  parseOptionalBool,
  coerceNumber,
  coerceString,
  coerceArray,
  coerceStringArray,
} from '../src/utils/coerce';

// Parity hardening for generic tool-input value coercion. Each block pins
// behavior that discriminates the hardened helper from the naive inline pattern
// (`Number(x)` / `String(x)` / `as T[]`) it replaces — i.e. the assertion fails
// if the function is reverted to the naive form.

describe('coerceBool (regression guard — unchanged)', () => {
  it('string "true"/"false" and nullish still behave', () => {
    assert.equal(coerceBool('true'), true);
    assert.equal(coerceBool('false'), false);
    assert.equal(coerceBool(undefined), false);
  });
});

describe('coerceBoolStrict (distinguishes legit false from garbage)', () => {
  it('real booleans are recognized', () => {
    assert.deepEqual(coerceBoolStrict(true), { ok: true, value: true });
    assert.deepEqual(coerceBoolStrict(false), { ok: true, value: false });
  });

  it('string "true"/"false" spellings are recognized (case/space-insensitive)', () => {
    assert.deepEqual(coerceBoolStrict('true'), { ok: true, value: true });
    assert.deepEqual(coerceBoolStrict('  TRUE  '), { ok: true, value: true });
    assert.deepEqual(coerceBoolStrict('false'), { ok: true, value: false });
    assert.deepEqual(coerceBoolStrict(' False '), { ok: true, value: false });
  });

  // RED vs lenient coerceBool (which collapses ALL of these to a legit `false`):
  // strict must flag them as NOT a recognized boolean so callers can reject them.
  it('garbage inputs are NOT a recognized boolean (ok:false, raw preserved)', () => {
    for (const g of ['yes', 'no', '1', '0', '', 'maybe', 1, 0, {}, [], null, undefined]) {
      const r = coerceBoolStrict(g);
      assert.equal(r.ok, false, `expected ${JSON.stringify(g)} to be unrecognized`);
      if (!r.ok) assert.equal(r.raw, g);
    }
  });

  it('agrees with lenient coerceBool on every recognized input', () => {
    for (const v of [true, false, 'true', 'false', 'TRUE', '  false ']) {
      const r = coerceBoolStrict(v);
      assert.equal(r.ok, true);
      if (r.ok) assert.equal(r.value, coerceBool(v));
    }
  });
});

describe('parseOptionalBool (strict optional flag parse)', () => {
  it('absent (undefined/null) returns the fallback with no error', () => {
    assert.deepEqual(parseOptionalBool(undefined, 'flag'), { value: false });
    assert.deepEqual(parseOptionalBool(null, 'flag'), { value: false });
    assert.deepEqual(parseOptionalBool(undefined, 'flag', true), { value: true });
  });

  it('recognized spellings return the coerced value', () => {
    assert.deepEqual(parseOptionalBool('true', 'flag'), { value: true });
    assert.deepEqual(parseOptionalBool('false', 'flag', true), { value: false });
    assert.deepEqual(parseOptionalBool(false, 'flag', true), { value: false });
  });

  // RED vs lenient coerceBool: a present-but-garbage flag must ERROR, not silently
  // become false.
  it('present-but-unrecognized value returns a field-named error', () => {
    const r = parseOptionalBool('yes', 'run_in_background');
    assert.ok('error' in r);
    if ('error' in r) {
      assert.match(r.error, /run_in_background must be a boolean/);
      assert.match(r.error, /"yes"/);
    }
  });

  it('non-string garbage is reported with its JSON form', () => {
    const r = parseOptionalBool(1, 'flag');
    assert.ok('error' in r);
    if ('error' in r) assert.match(r.error, /received 1/);
  });
});

describe('coerceNumber', () => {
  it('real finite numbers pass through', () => {
    assert.equal(coerceNumber(42, 0), 42);
    assert.equal(coerceNumber(-7.5, 0), -7.5);
    assert.equal(coerceNumber(0, 99), 0);
  });

  it('numeric strings (provider-serialized) coerce; "1e6" supported', () => {
    assert.equal(coerceNumber('42', 0), 42);
    assert.equal(coerceNumber('  13  ', 0), 13);
    assert.equal(coerceNumber('1e6', 0), 1_000_000);
  });

  // RED vs naive `Number(x)`: Number('')===0, Number(null)===0, Number([])===0,
  // Number(false)===0, Number(true)===1 would all slip bogus values through.
  it('empty/garbage strings, booleans, null, undefined, objects, arrays → fallback', () => {
    assert.equal(coerceNumber('', 5), 5);
    assert.equal(coerceNumber('   ', 5), 5);
    assert.equal(coerceNumber('12px', 5), 5);
    assert.equal(coerceNumber(null, 5), 5);
    assert.equal(coerceNumber(undefined, 5), 5);
    assert.equal(coerceNumber(true, 5), 5);
    assert.equal(coerceNumber(false, 5), 5);
    assert.equal(coerceNumber([], 5), 5);
    assert.equal(coerceNumber([7], 5), 5);
    assert.equal(coerceNumber({}, 5), 5);
  });

  // RED vs naive: Number('NaN')/Number('Infinity') are non-finite.
  it('NaN / Infinity values → fallback', () => {
    assert.equal(coerceNumber(NaN, 5), 5);
    assert.equal(coerceNumber(Infinity, 5), 5);
    assert.equal(coerceNumber(-Infinity, 5), 5);
    assert.equal(coerceNumber('Infinity', 5), 5);
  });

  it('integer option truncates toward zero', () => {
    assert.equal(coerceNumber(3.9, 0, { integer: true }), 3);
    assert.equal(coerceNumber(-3.9, 0, { integer: true }), -3);
    assert.equal(coerceNumber('7.8', 0, { integer: true }), 7);
  });

  it('min/max clamp the coerced value', () => {
    assert.equal(coerceNumber(100, 0, { max: 50 }), 50);
    assert.equal(coerceNumber(-5, 0, { min: 1 }), 1);
    assert.equal(coerceNumber(25, 0, { min: 1, max: 50 }), 25);
    assert.equal(coerceNumber('999', 0, { integer: true, min: 1, max: 50 }), 50);
  });

  // RED: fallback must be returned verbatim, never clamped.
  it('fallback is returned verbatim and NOT clamped', () => {
    assert.equal(coerceNumber(undefined, -1, { min: 0, max: 10 }), -1);
    assert.equal(coerceNumber('nope', 999, { min: 0, max: 10 }), 999);
  });
});

describe('coerceString', () => {
  it('real strings pass through verbatim (no trim)', () => {
    assert.equal(coerceString('hello'), 'hello');
    assert.equal(coerceString('  spaced  '), '  spaced  ');
    assert.equal(coerceString(''), '');
  });

  it('finite numbers, booleans, bigint stringify', () => {
    assert.equal(coerceString(42), '42');
    assert.equal(coerceString(0), '0');
    assert.equal(coerceString(true), 'true');
    assert.equal(coerceString(false), 'false');
    assert.equal(coerceString(10n), '10');
  });

  // RED vs naive `String(x)`: String({})==="[object Object]",
  // String([1,2])==="1,2", String(null)==="null", String(NaN)==="NaN".
  it('objects, arrays, null, undefined, NaN/Infinity → fallback (not garbage)', () => {
    assert.equal(coerceString({}), '');
    assert.equal(coerceString({ a: 1 }), '');
    assert.equal(coerceString([1, 2]), '');
    assert.equal(coerceString(null), '');
    assert.equal(coerceString(undefined), '');
    assert.equal(coerceString(NaN), '');
    assert.equal(coerceString(Infinity), '');
  });

  it('custom fallback is honored', () => {
    assert.equal(coerceString(null, 'default'), 'default');
    assert.equal(coerceString({}, 'x'), 'x');
  });
});

describe('coerceArray', () => {
  it('arrays pass through (same reference)', () => {
    const a = [1, 2, 3];
    assert.equal(coerceArray(a), a);
    assert.deepEqual(coerceArray(['x']), ['x']);
  });

  // RED vs naive `x` passthrough: non-arrays must become the fallback.
  it('non-arrays → fallback (default [])', () => {
    assert.deepEqual(coerceArray(null), []);
    assert.deepEqual(coerceArray(undefined), []);
    assert.deepEqual(coerceArray('abc'), []);
    assert.deepEqual(coerceArray(42), []);
    assert.deepEqual(coerceArray({}), []);
    assert.deepEqual(coerceArray(null, [9]), [9]);
  });
});

describe('coerceStringArray', () => {
  it('array of strings passes through', () => {
    assert.deepEqual(coerceStringArray(['a', 'b']), ['a', 'b']);
  });

  // RED vs naive `x as string[]`: non-string elements must be dropped, not cast.
  it('drops non-string elements (number/null/object/nested array)', () => {
    assert.deepEqual(coerceStringArray(['a', 1, null, {}, ['b'], 'c']), ['a', 'c']);
  });

  // RED: a lone string wraps into a single-element array (string | string[] schemas).
  it('wraps a lone string into a single-element array', () => {
    assert.deepEqual(coerceStringArray('solo'), ['solo']);
  });

  it('non-array non-string → fallback (default [])', () => {
    assert.deepEqual(coerceStringArray(null), []);
    assert.deepEqual(coerceStringArray(undefined), []);
    assert.deepEqual(coerceStringArray(42), []);
    assert.deepEqual(coerceStringArray({}), []);
    assert.deepEqual(coerceStringArray(null, ['fb']), ['fb']);
  });
});
