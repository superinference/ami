import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { parseEnvInt } from '../src/utils/env';

describe('parseEnvInt', () => {
  it('parses plain integers', () => {
    assert.equal(parseEnvInt('42', 0), 42);
    assert.equal(parseEnvInt('1000', 0), 1000);
    assert.equal(parseEnvInt('0', 5), 0);
  });

  it('parses scientific notation', () => {
    assert.equal(parseEnvInt('1e6', 0), 1_000_000);
    assert.equal(parseEnvInt('5e3', 0), 5000);
    assert.equal(parseEnvInt('2.5e2', 0), 250);
  });

  it('parses digit separators', () => {
    assert.equal(parseEnvInt('64_000', 0), 64000);
    assert.equal(parseEnvInt('1_000_000', 0), 1_000_000);
    assert.equal(parseEnvInt('10_00', 0), 1000);
  });

  it('truncates fractional values', () => {
    assert.equal(parseEnvInt('3.7', 0), 3);
    assert.equal(parseEnvInt('9.99', 0), 9);
  });

  it('returns fallback for undefined', () => {
    assert.equal(parseEnvInt(undefined, 42), 42);
  });

  it('returns fallback for empty string', () => {
    assert.equal(parseEnvInt('', 10), 10);
  });

  it('returns fallback for non-numeric string', () => {
    assert.equal(parseEnvInt('fast', 10), 10);
    assert.equal(parseEnvInt('abc', 5), 5);
  });

  it('returns fallback for NaN', () => {
    assert.equal(parseEnvInt('NaN', 7), 7);
  });

  it('returns fallback for Infinity', () => {
    assert.equal(parseEnvInt('Infinity', 8), 8);
    assert.equal(parseEnvInt('-Infinity', 8), 8);
  });

  it('handles negative values', () => {
    assert.equal(parseEnvInt('-5', 0), -5);
  });
});
