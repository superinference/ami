import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  sanitizeErrorMessage,
  CREDENTIAL_PATTERNS,
  parseRetryAfterHeader,
  MAX_RETRY_AFTER_MS,
} from '../../core/src/provider';

describe('sanitizeErrorMessage', () => {
  it('strips sk- API keys', () => {
    const msg = sanitizeErrorMessage('Error: invalid key sk-abcdefghijklmnopqrstuvwxyz123456');
    assert.ok(!msg.includes('sk-abcdefghijkl'));
    assert.ok(msg.includes('[REDACTED]'));
  });

  it('strips sk-ant- Anthropic keys', () => {
    const msg = sanitizeErrorMessage('Auth failed with sk-ant-api03-xyzabcdef1234567890');
    assert.ok(!msg.includes('sk-ant-api03'));
    assert.ok(msg.includes('[REDACTED]'));
  });

  it('strips Bearer tokens', () => {
    const msg = sanitizeErrorMessage('Header: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0');
    assert.ok(!msg.includes('eyJhbGciOiJ'));
    assert.ok(msg.includes('[REDACTED]'));
  });

  it('strips basic auth in URLs', () => {
    const msg = sanitizeErrorMessage('Connecting to admin:secretpassword@api.example.com');
    assert.ok(!msg.includes('secretpassword'));
    assert.ok(msg.includes('[REDACTED]'));
  });

  it('strips x-api-key header values', () => {
    const msg = sanitizeErrorMessage('Request headers: x-api-key: sk-super-secret-key-value-here');
    assert.ok(!msg.includes('sk-super-secret'));
    assert.ok(msg.includes('[REDACTED]'));
  });

  it('strips Google API keys', () => {
    const msg = sanitizeErrorMessage('Error with key AIzaSyABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789');
    assert.ok(!msg.includes('AIzaSyABCDEFGHIJ'));
    assert.ok(msg.includes('[REDACTED]'));
  });

  it('strips Groq API keys', () => {
    const msg = sanitizeErrorMessage('Auth failed: gsk_abcdefghijklmnopqrstuvwxyz');
    assert.ok(!msg.includes('gsk_abcdefghij'));
    assert.ok(msg.includes('[REDACTED]'));
  });

  it('strips HuggingFace tokens', () => {
    const msg = sanitizeErrorMessage('Token hf_abcdefghijklmnopqrstuvwxyz is invalid');
    assert.ok(!msg.includes('hf_abcdefghij'));
    assert.ok(msg.includes('[REDACTED]'));
  });

  it('preserves normal error messages', () => {
    const msg = sanitizeErrorMessage('Connection refused: ECONNREFUSED 127.0.0.1:8080');
    assert.equal(msg, 'Connection refused: ECONNREFUSED 127.0.0.1:8080');
  });

  it('handles Error objects', () => {
    const err = new Error('Failed with key sk-abcdefghijklmnopqrstuvwxyz123456');
    const msg = sanitizeErrorMessage(err);
    assert.ok(!msg.includes('sk-abcdefghijkl'));
    assert.ok(msg.includes('[REDACTED]'));
  });

  it('strips multiple credentials in one message', () => {
    const msg = sanitizeErrorMessage(
      'Tried sk-key1abcdefghijklmnop then Bearer eyJhbGciOiJIUzI1NiJ9.eyJmb28iOiJiYXIifQ'
    );
    assert.ok(!msg.includes('sk-key1'));
    assert.ok(!msg.includes('eyJhbGciOiJ'));
    assert.equal((msg.match(/\[REDACTED\]/g) || []).length, 2);
  });
});

describe('CREDENTIAL_PATTERNS', () => {
  it('has at least 10 patterns', () => {
    assert.ok(CREDENTIAL_PATTERNS.length >= 10);
  });

  it('patterns are all RegExp instances', () => {
    for (const p of CREDENTIAL_PATTERNS) {
      assert.ok(p instanceof RegExp);
    }
  });
});

describe('parseRetryAfterHeader', () => {
  it('parses numeric seconds', () => {
    const ms = parseRetryAfterHeader('30');
    assert.equal(ms, 30_000);
  });

  it('clamps low values to 1000ms', () => {
    const ms = parseRetryAfterHeader('0');
    assert.equal(ms, 1_000);
  });

  it('clamps negative values to 1000ms', () => {
    const ms = parseRetryAfterHeader('-5');
    assert.equal(ms, 1_000);
  });

  it('clamps high values to MAX_RETRY_AFTER_MS', () => {
    const ms = parseRetryAfterHeader('7200');
    assert.equal(ms, MAX_RETRY_AFTER_MS);
  });

  it('returns default for null', () => {
    const ms = parseRetryAfterHeader(null);
    assert.equal(ms, 60_000);
  });

  it('returns default for undefined', () => {
    const ms = parseRetryAfterHeader(undefined);
    assert.equal(ms, 60_000);
  });

  it('returns default for empty string', () => {
    const ms = parseRetryAfterHeader('');
    assert.equal(ms, 60_000);
  });

  it('returns default for whitespace-only string', () => {
    const ms = parseRetryAfterHeader('   ');
    assert.equal(ms, 60_000);
  });

  it('returns default for garbage', () => {
    const ms = parseRetryAfterHeader('not-a-number');
    assert.equal(ms, 60_000);
  });

  it('handles fractional seconds', () => {
    const ms = parseRetryAfterHeader('1.5');
    assert.equal(ms, 1_500);
  });
});

describe('MAX_RETRY_AFTER_MS', () => {
  it('equals 1 hour in milliseconds', () => {
    assert.equal(MAX_RETRY_AFTER_MS, 3_600_000);
  });
});
