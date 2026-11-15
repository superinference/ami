import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  signRequest,
  createRequestSignature,
  verifyRequestSignature,
  SUPPORTED_SIGNATURE_ALGORITHMS,
} from '../src/request-signature';

const secret = 'parity-secret-key';

/**
 * Parity hardening for request-signature. Each block is a red-green reproducer:
 * it FAILS against the pre-hardening implementation and PASSES with the fix.
 */

// --- G-CANON: canonicalization injection (ambiguous \n-joined fields) ---------
// OLD: payload = `${METHOD}\n${PATH}\n${TS}\n${NONCE}\n${BODY}`. Distinct field
// vectors could collapse to the same byte string and share one signature.
// NEW: every field is length-prefixed, making the encoding injective.
describe('G-CANON canonicalization injection', () => {
  it('signRequest: (method="POST\\nA", path="B") must NOT collide with (method="POST", path="A\\nB")', () => {
    // Under the old \n-join both produce `POST\nA\nB\n<ts>\n<body>` → identical.
    const a = signRequest('POST\nA', 'B', 'body', 1000, secret);
    const b = signRequest('POST', 'A\nB', 'body', 1000, secret);
    assert.notEqual(a, b);
  });

  it('verify: a signature cannot be replayed across a shifted nonce/body boundary', () => {
    // Attacker goal: a signature legitimately covering body="secretX\ndata" is
    // reused for a different body="data" by moving bytes into the (also
    // attacker-controlled) nonce field. Old \n-join: both canonicalize to
    // `...\n<ts>\nN\nsecretX\ndata`. Here we sign one request, then forge a
    // second with the boundary shifted and assert the forgery is rejected.
    const sig = createRequestSignature('POST', '/api', 'secretX\ndata', secret);
    const forged = { ...sig, nonce: `${sig.nonce}\nsecretX` };
    const result = verifyRequestSignature(forged, 'POST', '/api', 'data', secret);
    // OLD canonicalization would make canonical(forged,'data') ==
    // canonical(sig,'secretX\ndata') → valid:true. NEW rejects it.
    assert.equal(result.valid, false);
  });

  it('round-trip still succeeds under length-prefixed canonicalization', () => {
    const sig = createRequestSignature('POST', '/chat', '{"x":1}', secret);
    assert.equal(verifyRequestSignature(sig, 'POST', '/chat', '{"x":1}', secret).valid, true);
  });
});

// --- G-SECRET: empty / non-string HMAC key rejection --------------------------
describe('G-SECRET empty/invalid secret', () => {
  it('signRequest throws on an empty secret', () => {
    // OLD: silently produced an HMAC with a public (empty) key.
    assert.throws(() => signRequest('GET', '/', '', 0, ''), /non-empty string/);
  });

  it('createRequestSignature throws on an empty secret', () => {
    assert.throws(() => createRequestSignature('GET', '/', '', ''), /non-empty string/);
  });

  it('verifyRequestSignature reports invalid secret (does not verify against empty key)', () => {
    const sig = createRequestSignature('POST', '/api', 'body', secret);
    const result = verifyRequestSignature(sig, 'POST', '/api', 'body', '');
    // OLD: computed HMAC('') and returned reason 'signature mismatch'.
    // NEW: short-circuits with reason 'invalid secret'.
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'invalid secret');
  });
});

// --- G-TS: non-finite / non-integer / negative timestamp bypass ---------------
describe('G-TS timestamp validation', () => {
  it('rejects a NaN timestamp with reason "invalid timestamp" (freshness-window bypass)', () => {
    const sig = createRequestSignature('POST', '/api', 'body', secret);
    (sig as unknown as { timestamp: number }).timestamp = NaN;
    const result = verifyRequestSignature(sig, 'POST', '/api', 'body', secret);
    // OLD: NaN passed both freshness checks (NaN comparisons are false) and fell
    // through to the HMAC compare → reason 'signature mismatch'.
    // NEW: explicit rejection → reason 'invalid timestamp'.
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'invalid timestamp');
  });

  it('rejects a non-numeric (string) timestamp', () => {
    const sig = createRequestSignature('POST', '/api', 'body', secret);
    (sig as unknown as { timestamp: unknown }).timestamp = '1000';
    const result = verifyRequestSignature(sig, 'POST', '/api', 'body', secret);
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'invalid timestamp');
  });

  it('rejects a negative timestamp', () => {
    const sig = createRequestSignature('POST', '/api', 'body', secret);
    sig.timestamp = -1;
    const result = verifyRequestSignature(sig, 'POST', '/api', 'body', secret);
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'invalid timestamp');
  });
});

// --- G-NONCE: empty / non-string nonce rejection ------------------------------
describe('G-NONCE nonce validation', () => {
  it('rejects an empty nonce with reason "invalid nonce"', () => {
    const sig = createRequestSignature('POST', '/api', 'body', secret);
    sig.nonce = '';
    const result = verifyRequestSignature(sig, 'POST', '/api', 'body', secret);
    // OLD: empty nonce flowed into the canonical string and failed as a generic
    // 'signature mismatch', defeating NonceTracker tracking silently.
    // NEW: explicit 'invalid nonce'.
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'invalid nonce');
  });

  it('rejects a non-string nonce', () => {
    const sig = createRequestSignature('POST', '/api', 'body', secret);
    (sig as unknown as { nonce: unknown }).nonce = 42;
    const result = verifyRequestSignature(sig, 'POST', '/api', 'body', secret);
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'invalid nonce');
  });
});

// --- G-ALGO: algorithm agility with a strict allowlist ------------------------
describe('G-ALGO algorithm agility', () => {
  it('createRequestSignature with sha512 yields a 128-hex-char signature and round-trips', () => {
    const sig = createRequestSignature('POST', '/api', 'body', secret, 'sha512');
    // OLD: extra arg ignored → sha256 → 64 chars.
    assert.equal(sig.signature.length, 128);
    assert.equal(verifyRequestSignature(sig, 'POST', '/api', 'body', secret, 300_000, 'sha512').valid, true);
  });

  it('signRequest with sha384 yields a 96-hex-char signature', () => {
    const s = signRequest('GET', '/', '', 0, secret, 'sha384');
    assert.equal(s.length, 96);
  });

  it('a sha512 signature is rejected when verified under the default (sha256)', () => {
    const sig = createRequestSignature('POST', '/api', 'body', secret, 'sha512');
    // Length differs (128 vs 64) → malformed; proves the verifier does not
    // blindly accept an off-algorithm MAC (no downgrade / confusion).
    const result = verifyRequestSignature(sig, 'POST', '/api', 'body', secret);
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'signature malformed');
  });

  it('an unsupported algorithm throws (closed allowlist) on sign and verify', () => {
    const bad = 'md5' as unknown as (typeof SUPPORTED_SIGNATURE_ALGORITHMS)[number];
    assert.throws(() => signRequest('GET', '/', '', 0, secret, bad), /unsupported signature algorithm/);
    assert.throws(() => createRequestSignature('GET', '/', '', secret, bad), /unsupported signature algorithm/);
    const sig = createRequestSignature('GET', '/', '', secret);
    assert.throws(() => verifyRequestSignature(sig, 'GET', '/', '', secret, 300_000, bad), /unsupported signature algorithm/);
  });

  it('the allowlist is exactly sha256/sha384/sha512', () => {
    assert.deepEqual([...SUPPORTED_SIGNATURE_ALGORITHMS], ['sha256', 'sha384', 'sha512']);
  });
});
