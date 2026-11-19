import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  findUnsafePatternReason,
  isSafeRegex,
  safeCompileRegex,
  safeRegexSchema,
  DEFAULT_MAX_PATTERN_LENGTH,
} from '../src/safe-regex';

test('findUnsafePatternReason: accepts well-behaved patterns', () => {
  assert.equal(findUnsafePatternReason('^[a-z]+$'), null);
  assert.equal(findUnsafePatternReason('abc|def'), null);
  assert.equal(findUnsafePatternReason('(?:ab)+'), null); // non-capturing, fixed body
  assert.equal(findUnsafePatternReason('(?<year>\\d{4})'), null); // named group ≠ lookbehind
  assert.equal(findUnsafePatternReason('(?=ab)cd'), null); // lookahead is fine
  assert.equal(findUnsafePatternReason('a.*b.*c'), null); // no quantified group
});

test('findUnsafePatternReason: rejects a backreference', () => {
  assert.equal(findUnsafePatternReason('(a)\\1'), 'uses a backreference');
  assert.equal(findUnsafePatternReason('(?<g>a)\\k<g>'), 'uses a backreference');
});

test('findUnsafePatternReason: rejects a lookbehind (both polarities)', () => {
  assert.equal(findUnsafePatternReason('(?<=a)b'), 'uses a lookbehind');
  assert.equal(findUnsafePatternReason('(?<!a)b'), 'uses a lookbehind');
});

test('findUnsafePatternReason: rejects a repeated variable group (catastrophic)', () => {
  const reason = 'repeats a group that can match in more than one way';
  assert.equal(findUnsafePatternReason('(a+)+$'), reason);
  assert.equal(findUnsafePatternReason('(a?)*'), reason);
  assert.equal(findUnsafePatternReason('(a|aa)+'), reason);
  assert.equal(findUnsafePatternReason('(a+){2,3}'), reason); // {-quantifier counts
  assert.equal(findUnsafePatternReason('((a|b)+)+'), reason); // nested propagation
});

test('findUnsafePatternReason: a fixed-body repeated group is safe', () => {
  // (abc)+ repeats but its body matches at most one way → not exponential.
  assert.equal(findUnsafePatternReason('(abc)+'), null);
  assert.equal(findUnsafePatternReason('(ab)*cd'), null);
});

test('findUnsafePatternReason: a quantifier inside a character class is not a group varier', () => {
  // `[*+?]` are literals in a class; `\(` is an escaped literal paren.
  assert.equal(findUnsafePatternReason('[a*+?]'), null);
  assert.equal(findUnsafePatternReason('\\(a+\\)+'), null); // escaped parens: no real group
});

test('findUnsafePatternReason: rejects a non-compiling pattern', () => {
  assert.equal(findUnsafePatternReason('('), 'does not compile');
  assert.equal(findUnsafePatternReason('a{2,1}'), 'does not compile');
});

test('findUnsafePatternReason: enforces the length cap', () => {
  const long = 'a'.repeat(DEFAULT_MAX_PATTERN_LENGTH + 1);
  assert.equal(
    findUnsafePatternReason(long),
    `exceeds the maximum length of ${DEFAULT_MAX_PATTERN_LENGTH} characters`,
  );
  // Custom cap honored.
  assert.equal(findUnsafePatternReason('aaa', 2), 'exceeds the maximum length of 2 characters');
  assert.equal(findUnsafePatternReason('aa', 2), null);
});

test('findUnsafePatternReason: non-string input', () => {
  assert.equal(findUnsafePatternReason(42 as unknown as string), 'is not a string');
});

test('isSafeRegex mirrors findUnsafePatternReason', () => {
  assert.equal(isSafeRegex('^ok$'), true);
  assert.equal(isSafeRegex('(a+)+'), false);
  assert.equal(isSafeRegex('aaa', 2), false);
});

test('safeCompileRegex: returns a RegExp for a safe pattern and honors flags', () => {
  const re = safeCompileRegex('^abc$', 'i');
  assert.ok(re instanceof RegExp);
  assert.equal(re!.test('ABC'), true);
  assert.equal(re!.flags, 'i');
});

test('safeCompileRegex: returns null for an unsafe or uncompilable pattern', () => {
  assert.equal(safeCompileRegex('(a+)+$'), null);
  assert.equal(safeCompileRegex('(a)\\1'), null);
  assert.equal(safeCompileRegex('('), null);
  // Invalid flags throw inside the compile → null.
  assert.equal(safeCompileRegex('ok', 'zzz'), null);
});

test('safeRegexSchema: parses a safe pattern and rejects an unsafe one', () => {
  const schema = safeRegexSchema();
  assert.equal(schema.parse('^[a-z]+$'), '^[a-z]+$');

  const bad = schema.safeParse('(a+)+$');
  assert.equal(bad.success, false);
  if (!bad.success) {
    assert.match(bad.error.issues[0].message, /repeats a group/);
  }

  const backref = schema.safeParse('(a)\\1');
  assert.equal(backref.success, false);
  if (!backref.success) {
    assert.match(backref.error.issues[0].message, /backreference/);
  }
});

test('safeRegexSchema: honors a custom length cap', () => {
  const schema = safeRegexSchema(3);
  assert.equal(schema.safeParse('aa').success, true);
  assert.equal(schema.safeParse('aaaa').success, false);
});
