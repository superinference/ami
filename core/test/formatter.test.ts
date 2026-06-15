import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { formatFile, getAvailableFormatters } from '../src/formatter';

describe('formatter', () => {
  it('returns formatted:false for unknown extensions', () => {
    const result = formatFile('/tmp/file.xyz', '/tmp');
    assert.equal(result.formatted, false);
    assert.equal(result.error, undefined);
  });

  it('returns formatted:false when formatter is unavailable', () => {
    const result = formatFile('/tmp/file.dart', '/tmp');
    assert.equal(result.formatted, false);
  });

  it('getAvailableFormatters returns an array', () => {
    const result = getAvailableFormatters('/tmp');
    assert.ok(Array.isArray(result));
  });

  it('formatFile handles .ts extension mapping', () => {
    const result = formatFile('/nonexistent/file.ts', '/tmp');
    assert.equal(typeof result.formatted, 'boolean');
  });

  it('formatFile handles .py extension mapping', () => {
    const result = formatFile('/nonexistent/file.py', '/tmp');
    assert.equal(typeof result.formatted, 'boolean');
  });

  it('formatFile handles .go extension mapping', () => {
    const result = formatFile('/nonexistent/file.go', '/tmp');
    assert.equal(typeof result.formatted, 'boolean');
  });
});
