import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  generateNonce,
  signRequest,
  createRequestSignature,
  verifyRequestSignature,
  NonceTracker,
} from '../src/request-signature';

describe('generateNonce', () => {
  it('returns a hex string', () => {
    const nonce = generateNonce();
    assert.match(nonce, /^[0-9a-f]+$/);
  });

  it('generates unique values', () => {
    const a = generateNonce();
    const b = generateNonce();
    assert.notEqual(a, b);
  });

  it('default length is 32 chars (16 bytes)', () => {
    const nonce = generateNonce();
    assert.equal(nonce.length, 32);
  });

  it('respects custom byte length', () => {
    const nonce = generateNonce(8);
    assert.equal(nonce.length, 16);
  });
});

describe('signRequest', () => {
  const secret = 'test-secret-key';

  it('produces consistent output for same inputs', () => {
    const a = signRequest('POST', '/api/v1/chat', '{"msg":"hi"}', 1000, secret);
    const b = signRequest('POST', '/api/v1/chat', '{"msg":"hi"}', 1000, secret);
    assert.equal(a, b);
  });

  it('produces different output for different bodies', () => {
    const a = signRequest('POST', '/api/v1/chat', '{"msg":"hi"}', 1000, secret);
    const b = signRequest('POST', '/api/v1/chat', '{"msg":"bye"}', 1000, secret);
    assert.notEqual(a, b);
  });

  it('is case-insensitive on method (uppercases)', () => {
    const a = signRequest('post', '/api', 'body', 1000, secret);
    const b = signRequest('POST', '/api', 'body', 1000, secret);
    assert.equal(a, b);
  });

  it('produces a 64-char hex string (SHA-256)', () => {
    const sig = signRequest('GET', '/', '', 0, secret);
    assert.equal(sig.length, 64);
    assert.match(sig, /^[0-9a-f]{64}$/);
  });
});

describe('createRequestSignature / verifyRequestSignature', () => {
  const secret = 'my-secret';

  it('createRequestSignature has nonce, timestamp, signature', () => {
    const sig = createRequestSignature('POST', '/chat', '{}', secret);
    assert.ok(sig.nonce);
    assert.ok(sig.timestamp > 0);
    assert.ok(sig.signature);
    assert.equal(sig.signature.length, 64);
  });

  it('round-trip: create and verify', () => {
    const sig = createRequestSignature('POST', '/chat', '{"x":1}', secret);
    const result = verifyRequestSignature(sig, 'POST', '/chat', '{"x":1}', secret);
    assert.equal(result.valid, true);
    assert.equal(result.reason, undefined);
  });

  it('verifyRequestSignature accepts valid signature', () => {
    const sig = createRequestSignature('GET', '/health', '', secret);
    const result = verifyRequestSignature(sig, 'GET', '/health', '', secret);
    assert.equal(result.valid, true);
  });

  it('verifyRequestSignature rejects expired signature', () => {
    const sig = createRequestSignature('POST', '/api', 'body', secret);
    sig.timestamp = Date.now() - 400_000; // 400s ago
    // Re-sign with the old timestamp to make a structurally valid but expired sig
    const result = verifyRequestSignature(sig, 'POST', '/api', 'body', secret);
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'signature expired');
  });

  it('verifyRequestSignature rejects future timestamp', () => {
    const sig = createRequestSignature('POST', '/api', 'body', secret);
    sig.timestamp = Date.now() + 120_000; // 2 min in future
    const result = verifyRequestSignature(sig, 'POST', '/api', 'body', secret);
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'signature timestamp in future');
  });

  it('verifyRequestSignature rejects wrong body', () => {
    const sig = createRequestSignature('POST', '/api', 'body-a', secret);
    const result = verifyRequestSignature(sig, 'POST', '/api', 'body-b', secret);
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'signature mismatch');
  });

  it('verifyRequestSignature rejects wrong secret', () => {
    const sig = createRequestSignature('POST', '/api', 'body', secret);
    const result = verifyRequestSignature(sig, 'POST', '/api', 'body', 'wrong-secret');
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'signature mismatch');
  });

  it('verifyRequestSignature rejects wrong method', () => {
    const sig = createRequestSignature('POST', '/api', 'body', secret);
    const result = verifyRequestSignature(sig, 'GET', '/api', 'body', secret);
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'signature mismatch');
  });

  it('verifyRequestSignature custom maxAgeMs works', () => {
    const sig = createRequestSignature('POST', '/api', 'body', secret);
    sig.timestamp = Date.now() - 5_000; // 5s ago
    // Re-create the correct signature for the old timestamp
    // (can't just change timestamp on existing sig — signature won't match)
    // Instead, use a maxAge shorter than the age
    const result = verifyRequestSignature(sig, 'POST', '/api', 'body', secret, 1_000);
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'signature expired');
  });
});

describe('NonceTracker', () => {
  it('isReplay returns false for new nonce', () => {
    const tracker = new NonceTracker();
    assert.equal(tracker.isReplay('abc'), false);
  });

  it('isReplay returns true for seen nonce', () => {
    const tracker = new NonceTracker();
    tracker.record('abc');
    assert.equal(tracker.isReplay('abc'), true);
  });

  it('record returns true for new nonce', () => {
    const tracker = new NonceTracker();
    assert.equal(tracker.record('abc'), true);
  });

  it('record returns false for duplicate', () => {
    const tracker = new NonceTracker();
    tracker.record('abc');
    assert.equal(tracker.record('abc'), false);
  });

  it('size tracks count', () => {
    const tracker = new NonceTracker();
    assert.equal(tracker.size(), 0);
    tracker.record('a');
    tracker.record('b');
    assert.equal(tracker.size(), 2);
  });

  it('evicts oldest when at capacity', () => {
    const tracker = new NonceTracker(3);
    tracker.record('first');
    tracker.record('second');
    tracker.record('third');
    assert.equal(tracker.size(), 3);
    tracker.record('fourth');
    assert.equal(tracker.size(), 3);
    // 'first' should have been evicted
    assert.equal(tracker.isReplay('first'), false);
    assert.equal(tracker.isReplay('fourth'), true);
  });

  it('clear resets', () => {
    const tracker = new NonceTracker();
    tracker.record('a');
    tracker.record('b');
    tracker.clear();
    assert.equal(tracker.size(), 0);
    assert.equal(tracker.isReplay('a'), false);
  });

  it('handles multiple evictions correctly', () => {
    const tracker = new NonceTracker(2);
    tracker.record('a');
    tracker.record('b');
    tracker.record('c');
    tracker.record('d');
    assert.equal(tracker.size(), 2);
    assert.equal(tracker.isReplay('a'), false);
    assert.equal(tracker.isReplay('b'), false);
    assert.equal(tracker.isReplay('c'), true);
    assert.equal(tracker.isReplay('d'), true);
  });
});
