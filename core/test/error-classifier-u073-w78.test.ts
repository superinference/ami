import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { detectPromptTooLong, categorizeLoadFailure } from '../src/error-classifier';

// ---------------------------------------------------------------------------
// U073 / w78 — detectPromptTooLong: deterministic 400 "prompt is too long"
// special-casing (do-not-retry, distinct outcome with parsed token counts).
// ---------------------------------------------------------------------------
describe('detectPromptTooLong', () => {
  it('returns parsed counts for a deterministic "prompt is too long: N > M" error (Error)', () => {
    const r = detectPromptTooLong(new Error('prompt is too long: 215000 tokens > 200000 maximum'));
    assert.deepEqual(r, { actualTokens: 215000, limitTokens: 200000 });
  });

  it('returns parsed counts from a raw string too', () => {
    const r = detectPromptTooLong('API Error: prompt is too long: 137500 tokens > 135000 maximum');
    assert.deepEqual(r, { actualTokens: 137500, limitTokens: 135000 });
  });

  it('matches the phrase even when token counts are not parseable (counts undefined)', () => {
    const r = detectPromptTooLong('prompt is too long for this model');
    assert.deepEqual(r, { actualTokens: undefined, limitTokens: undefined });
  });

  it('is case-insensitive on the trigger phrase', () => {
    const r = detectPromptTooLong('PROMPT IS TOO LONG: 10 tokens > 5 maximum');
    assert.deepEqual(r, { actualTokens: 10, limitTokens: 5 });
  });

  it('returns undefined for the recoverable max_tokens-exceed 400 (different message)', () => {
    // This form is fixed by shrinking max_tokens, so it must NOT be treated as the
    // deterministic do-not-retry variant.
    assert.equal(
      detectPromptTooLong('input length and `max_tokens` exceed context limit: 100 + 200 > 250'),
      undefined,
    );
  });

  it('returns undefined for unrelated errors', () => {
    assert.equal(detectPromptTooLong('HTTP 503: overloaded'), undefined);
    assert.equal(detectPromptTooLong('context window exceeded'), undefined);
  });

  it('returns undefined for non-string / non-Error values', () => {
    assert.equal(detectPromptTooLong(undefined), undefined);
    assert.equal(detectPromptTooLong(null), undefined);
    assert.equal(detectPromptTooLong(42), undefined);
    assert.equal(detectPromptTooLong({ message: 'prompt is too long' }), undefined);
  });
});

// ---------------------------------------------------------------------------
// U073 / w78 — categorizeLoadFailure: network / permissions / config / unknown
// triage with an actionable remediation hint (plugin / hooks load failures).
// ---------------------------------------------------------------------------
describe('categorizeLoadFailure', () => {
  it('categorizes a failed clone / network errno as network', () => {
    for (const msg of [
      'Failed to clone repository',
      'getaddrinfo ENOTFOUND github.com',
      'connect ETIMEDOUT 10.0.0.1:443',
      'network is unreachable',
      'read ECONNRESET',
    ]) {
      const r = categorizeLoadFailure(new Error(msg));
      assert.equal(r.category, 'network', `expected network for: ${msg}`);
      assert.match(r.guidance, /network/i);
    }
  });

  it('categorizes a filesystem permission failure as permissions', () => {
    for (const msg of ['EACCES: permission denied', 'operation not permitted, EPERM']) {
      const r = categorizeLoadFailure(new Error(msg));
      assert.equal(r.category, 'permissions', `expected permissions for: ${msg}`);
      assert.match(r.guidance, /permission/i);
    }
  });

  it('categorizes a malformed-JSON / schema failure as config', () => {
    for (const msg of [
      'Unexpected token ] in JSON at position 5',
      'Unexpected end of JSON input',
      'invalid hook schema',
      'could not parse settings',
    ]) {
      const r = categorizeLoadFailure(new Error(msg));
      assert.equal(r.category, 'config', `expected config for: ${msg}`);
      assert.match(r.guidance, /config/i);
    }
  });

  it('falls back to unknown with generic guidance for an unrecognized failure', () => {
    const r = categorizeLoadFailure(new Error('something unexpected happened'));
    assert.equal(r.category, 'unknown');
    assert.ok(r.guidance.length > 0);
  });

  it('accepts a non-Error value (string / null) without throwing', () => {
    assert.equal(categorizeLoadFailure('EACCES while reading').category, 'permissions');
    assert.equal(categorizeLoadFailure(null).category, 'unknown');
  });

  it('prioritizes network over config when both signals are present', () => {
    // A "Failed to clone" that also mentions JSON must still read as the root
    // network cause (clone failed before any parse could happen).
    const r = categorizeLoadFailure(new Error('Failed to clone: could not read JSON index'));
    assert.equal(r.category, 'network');
  });
});
