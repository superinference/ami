import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { sanitizeHookOutput } from '../src/hooks';

describe('hooks-tag-escape: Gap 5 — sanitizeHookOutput strips system-reminder tags', () => {
  it('escapes <system-reminder> opening and closing tags', () => {
    const input = '<system-reminder>evil</system-reminder>';
    const expected = '[tag-escaped]evil[tag-escaped]';
    assert.equal(sanitizeHookOutput(input), expected);
  });

  it('passes safe text through unchanged', () => {
    const input = 'This is perfectly safe output from a hook.';
    assert.equal(sanitizeHookOutput(input), input);
  });

  it('is case-insensitive', () => {
    const input = '<SYSTEM-REMINDER>evil</SYSTEM-REMINDER>';
    const expected = '[tag-escaped]evil[tag-escaped]';
    assert.equal(sanitizeHookOutput(input), expected);
  });

  it('handles mixed case variants', () => {
    const input = '<System-Reminder>data</system-REMINDER>';
    const expected = '[tag-escaped]data[tag-escaped]';
    assert.equal(sanitizeHookOutput(input), expected);
  });

  it('handles nested tags', () => {
    const input = '<system-reminder><system-reminder>double</system-reminder></system-reminder>';
    const expected = '[tag-escaped][tag-escaped]double[tag-escaped][tag-escaped]';
    assert.equal(sanitizeHookOutput(input), expected);
  });

  it('handles standalone closing tags', () => {
    const input = 'text </system-reminder> more text';
    const expected = 'text [tag-escaped] more text';
    assert.equal(sanitizeHookOutput(input), expected);
  });

  it('handles standalone opening tags', () => {
    const input = 'prefix <system-reminder> suffix';
    const expected = 'prefix [tag-escaped] suffix';
    assert.equal(sanitizeHookOutput(input), expected);
  });

  it('preserves other HTML-like tags', () => {
    const input = '<div>safe</div><system-reminder>evil</system-reminder>';
    const expected = '<div>safe</div>[tag-escaped]evil[tag-escaped]';
    assert.equal(sanitizeHookOutput(input), expected);
  });

  it('handles empty string', () => {
    assert.equal(sanitizeHookOutput(''), '');
  });

  it('handles multiple occurrences across the string', () => {
    const input = '<system-reminder>a</system-reminder> middle <system-reminder>b</system-reminder>';
    const expected = '[tag-escaped]a[tag-escaped] middle [tag-escaped]b[tag-escaped]';
    assert.equal(sanitizeHookOutput(input), expected);
  });
});
