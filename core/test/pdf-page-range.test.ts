import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { parsePageRange } from '../src/tools/file-read';

describe('parsePageRange', () => {
  it('parses single page', () => {
    const result = parsePageRange('5');
    assert.equal(result.start, 5);
    assert.equal(result.end, 5);
    assert.equal(result.error, undefined);
  });

  it('parses page range', () => {
    const result = parsePageRange('3-7');
    assert.equal(result.start, 3);
    assert.equal(result.end, 7);
    assert.equal(result.error, undefined);
  });

  it('rejects comma-separated page list with helpful error', () => {
    const result = parsePageRange('6,9,15');
    assert.ok(result.error);
    assert.ok(result.error.includes('not supported'));
    assert.ok(result.error.includes('separately'));
  });

  it('rejects comma-separated range list', () => {
    const result = parsePageRange('1-3,5-7');
    assert.ok(result.error);
    assert.ok(result.error.includes('not supported'));
  });

  it('rejects comma-separated with spaces', () => {
    const result = parsePageRange('6, 9, 15');
    assert.ok(result.error);
    assert.ok(result.error.includes('not supported'));
  });

  it('rejects invalid format', () => {
    const result = parsePageRange('abc');
    assert.ok(result.error);
    assert.ok(result.error.includes('Invalid page range'));
  });

  it('rejects reversed range', () => {
    const result = parsePageRange('10-5');
    assert.ok(result.error);
    assert.ok(result.error.includes('end >= start'));
  });

  it('rejects range over 20 pages', () => {
    const result = parsePageRange('1-25');
    assert.ok(result.error);
    assert.ok(result.error.includes('max 20'));
  });

  it('handles whitespace padding', () => {
    const result = parsePageRange('  3-5  ');
    assert.equal(result.start, 3);
    assert.equal(result.end, 5);
    assert.equal(result.error, undefined);
  });

  it('rejects page 0', () => {
    const result = parsePageRange('0');
    assert.ok(result.error);
    assert.ok(result.error.includes('>= 1'));
  });
});
