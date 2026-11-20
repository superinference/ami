/**
 * U415 — canonical string primitives in core/core/src/utils/string.ts.
 *
 * Covers the three helpers consolidated from a dozen+ open-coded call sites
 * (the `${n === 1 ? '' : 's'}` plural idiom, the `charAt(0).toUpperCase()`
 * capitalize idiom, and four private `escapeReg(Exp|ex)` RegExp-escape copies),
 * matching the reference's `src/utils/stringUtils.ts` behaviour exactly.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { plural, capitalize, escapeRegExp } from '../src/utils/string';

describe('plural', () => {
  it('returns the singular word for exactly 1', () => {
    assert.equal(plural(1, 'file'), 'file');
  });

  it('appends "s" by default for n !== 1 (incl. 0 and negatives)', () => {
    assert.equal(plural(0, 'file'), 'files');
    assert.equal(plural(3, 'file'), 'files');
    assert.equal(plural(-1, 'file'), 'files');
  });

  it('honours an explicit irregular plural form', () => {
    assert.equal(plural(2, 'entry', 'entries'), 'entries');
    assert.equal(plural(1, 'entry', 'entries'), 'entry');
  });

  it('returns only the word form (no leading count) so it composes in templates', () => {
    assert.equal(`${2} ${plural(2, 'image')} exceed`, '2 images exceed');
    assert.equal(`Ran ${1} stop ${plural(1, 'hook')}`, 'Ran 1 stop hook');
  });
});

describe('capitalize', () => {
  it('uppercases the first character only, leaving the rest untouched', () => {
    assert.equal(capitalize('read'), 'Read');
    assert.equal(capitalize('fooBar'), 'FooBar');
  });

  it('is a no-op on an empty string', () => {
    assert.equal(capitalize(''), '');
  });

  it('leaves an already-capitalized or non-alpha first char unchanged', () => {
    assert.equal(capitalize('Read'), 'Read');
    assert.equal(capitalize('1m'), '1m');
  });
});

describe('escapeRegExp', () => {
  it('escapes every regex metacharacter', () => {
    assert.equal(escapeRegExp('a.b*c'), 'a\\.b\\*c');
    assert.equal(
      escapeRegExp('.*+?^${}()|[]\\'),
      '\\.\\*\\+\\?\\^\\$\\{\\}\\(\\)\\|\\[\\]\\\\',
    );
  });

  it('leaves a metacharacter-free string unchanged', () => {
    assert.equal(escapeRegExp('plain-text_123'), 'plain-text_123');
  });

  it('produces a source that matches the literal when compiled', () => {
    const literal = 'v1.2 (beta)';
    const re = new RegExp(`^${escapeRegExp(literal)}$`);
    assert.ok(re.test(literal));
    assert.ok(!re.test('v1X2 (beta)'));
  });
});
