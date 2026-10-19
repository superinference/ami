import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  sanitizeErrorMessage,
  CREDENTIAL_PATTERNS,
  parseRetryAfterHeader,
  MAX_RETRY_AFTER_MS,
  isNetworkError,
  isSslError,
  DEFAULT_REQUEST_TIMEOUT_MS,
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

describe('isNetworkError', () => {
  it('returns true for ECONNRESET', () => {
    assert.ok(isNetworkError(Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' })));
  });

  it('returns true for ECONNREFUSED', () => {
    assert.ok(isNetworkError(Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' })));
  });

  it('returns true for ETIMEDOUT', () => {
    assert.ok(isNetworkError(Object.assign(new Error('connect ETIMEDOUT'), { code: 'ETIMEDOUT' })));
  });

  it('returns true for EHOSTUNREACH', () => {
    assert.ok(isNetworkError(Object.assign(new Error(''), { code: 'EHOSTUNREACH' })));
  });

  it('returns true for ECONNABORTED', () => {
    assert.ok(isNetworkError(Object.assign(new Error('request aborted'), { code: 'ECONNABORTED' })));
  });

  it('returns true for EPIPE', () => {
    assert.ok(isNetworkError(Object.assign(new Error('write EPIPE'), { code: 'EPIPE' })));
  });

  it('returns true for ENOTFOUND', () => {
    assert.ok(isNetworkError(Object.assign(new Error('getaddrinfo ENOTFOUND'), { code: 'ENOTFOUND' })));
  });

  it('returns true for ENETUNREACH', () => {
    assert.ok(isNetworkError(Object.assign(new Error(''), { code: 'ENETUNREACH' })));
  });

  it('returns true for EAI_AGAIN', () => {
    assert.ok(isNetworkError(Object.assign(new Error('getaddrinfo EAI_AGAIN'), { code: 'EAI_AGAIN' })));
  });

  it('returns true for ECONNABORTED in message (no code)', () => {
    assert.ok(isNetworkError(new Error('ECONNABORTED: request aborted')));
  });

  it('returns true for socket hang up in message', () => {
    assert.ok(isNetworkError(new Error('socket hang up')));
  });

  it('returns false for auth errors', () => {
    assert.equal(isNetworkError(new Error('401 Unauthorized')), false);
  });

  it('returns false for non-Error values', () => {
    assert.equal(isNetworkError('string'), false);
    assert.equal(isNetworkError(null), false);
  });
});

describe('isSslError', () => {
  it('returns true for UNABLE_TO_VERIFY_LEAF_SIGNATURE code', () => {
    assert.ok(isSslError(Object.assign(new Error('unable to verify'), { code: 'UNABLE_TO_VERIFY_LEAF_SIGNATURE' })));
  });

  it('returns true for CERT_HAS_EXPIRED code', () => {
    assert.ok(isSslError(Object.assign(new Error('cert expired'), { code: 'CERT_HAS_EXPIRED' })));
  });

  it('returns true for DEPTH_ZERO_SELF_SIGNED_CERT code', () => {
    assert.ok(isSslError(Object.assign(new Error('self signed'), { code: 'DEPTH_ZERO_SELF_SIGNED_CERT' })));
  });

  it('returns true for "self signed certificate" message', () => {
    assert.ok(isSslError(new Error('self signed certificate in chain')));
  });

  it('returns false for ECONNRESET (network error, not SSL)', () => {
    assert.equal(isSslError(Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' })), false);
  });

  it('returns false for "401 Unauthorized" (auth error, not SSL)', () => {
    assert.equal(isSslError(new Error('401 Unauthorized')), false);
  });
});

describe('SUPERINFERENCE_API_TIMEOUT_MS env var', () => {
  it('DEFAULT_REQUEST_TIMEOUT_MS is 300000 (5 minutes)', () => {
    assert.equal(DEFAULT_REQUEST_TIMEOUT_MS, 300_000);
  });

  it('env var override is wired into streamChatCompletionWithRetry source', async () => {
    // Verify the source code contains the env var check pattern.
    // This is a structural test: the env var must be read and used in the
    // timeout resolution chain, falling between options.requestTimeoutMs
    // (highest priority) and DEFAULT_REQUEST_TIMEOUT_MS (lowest priority).
    const fs = await import('node:fs');
    const src = fs.readFileSync(
      new URL('../../core/src/provider.ts', import.meta.url),
      'utf-8',
    );
    // The env var must be read
    assert.ok(
      src.includes('process.env.SUPERINFERENCE_API_TIMEOUT_MS'),
      'source must read SUPERINFERENCE_API_TIMEOUT_MS from process.env',
    );
    // The env var must be converted to a number and checked > 0
    assert.ok(
      src.includes('envTimeout > 0'),
      'source must guard against non-positive env values',
    );
    // The precedence chain: options.requestTimeoutMs > envTimeout > DEFAULT
    assert.ok(
      src.includes('options?.requestTimeoutMs ?? (envTimeout > 0 ? envTimeout : DEFAULT_REQUEST_TIMEOUT_MS)'),
      'timeout resolution must follow options > env > default precedence',
    );
  });

  it('env var is only used in streamChatCompletionWithRetry (not the inner stream)', async () => {
    // The env var override must NOT appear in streamChatCompletion (which
    // receives the already-resolved timeout via AbortController), only in
    // the retry wrapper that resolves it.
    const fs = await import('node:fs');
    const src = fs.readFileSync(
      new URL('../../core/src/provider.ts', import.meta.url),
      'utf-8',
    );
    const envVarOccurrences = src.split('SUPERINFERENCE_API_TIMEOUT_MS').length - 1;
    // Expect exactly 1 occurrence: the single resolution line in streamChatCompletionWithRetry
    assert.equal(envVarOccurrences, 1, 'env var should appear exactly once in provider.ts');
  });
});
