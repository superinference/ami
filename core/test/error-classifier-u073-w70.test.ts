import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  extractConnectionErrorDetails,
  connectionErrorDiagnostic,
  findErrorCode,
  friendlyErrorMessage,
  classifyError,
} from '../src/error-classifier';

// ---------------------------------------------------------------------------
// extractConnectionErrorDetails — structured root-cause view (code+message+ssl)
// ---------------------------------------------------------------------------

describe('extractConnectionErrorDetails', () => {
  it('returns null for a non-object / code-less error', () => {
    assert.equal(extractConnectionErrorDetails(null), null);
    assert.equal(extractConnectionErrorDetails('oops'), null);
    assert.equal(extractConnectionErrorDetails(new Error('no code here')), null);
  });

  it('finds the root errno nested under a code-less wrapper and flags non-SSL', () => {
    const root = Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' });
    const wrapper = Object.assign(new Error('Connection error.'), { cause: root });
    const d = extractConnectionErrorDetails(wrapper);
    assert.ok(d);
    assert.equal(d.code, 'ECONNRESET');
    assert.equal(d.message, 'read ECONNRESET');
    assert.equal(d.isSSLError, false);
  });

  it('flags a canonical OpenSSL/TLS code as an SSL error', () => {
    const root = Object.assign(new Error('certificate has expired'), { code: 'CERT_HAS_EXPIRED' });
    const wrapper = Object.assign(new Error('Connection error.'), { cause: root });
    const d = extractConnectionErrorDetails(wrapper);
    assert.ok(d);
    assert.equal(d.code, 'CERT_HAS_EXPIRED');
    assert.equal(d.isSSLError, true);
  });

  it('tolerates a non-string message at the code node', () => {
    const d = extractConnectionErrorDetails({ code: 'ETIMEDOUT' });
    assert.ok(d);
    assert.equal(d.code, 'ETIMEDOUT');
    assert.equal(d.message, '');
  });

  it('shares the cause-walk with findErrorCode (same code, cyclic-safe)', () => {
    const a: { code?: string; cause?: unknown } = { code: '' };
    const b = Object.assign(new Error('boom'), { code: 'EPIPE' });
    a.cause = b;
    assert.equal(findErrorCode(a), 'EPIPE');
    assert.equal(extractConnectionErrorDetails(a)?.code, 'EPIPE');
    // self-referential cause must not loop
    const loop: { cause?: unknown; code?: string } = {};
    loop.cause = loop;
    assert.equal(extractConnectionErrorDetails(loop), null);
  });
});

// ---------------------------------------------------------------------------
// connectionErrorDiagnostic — one-line debug-log string
// ---------------------------------------------------------------------------

describe('connectionErrorDiagnostic', () => {
  it('returns null when there is no structured code', () => {
    assert.equal(connectionErrorDiagnostic(new Error('plain')), null);
  });

  it('formats a non-SSL connection error without the SSL label', () => {
    const err = Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' });
    assert.equal(
      connectionErrorDiagnostic(err),
      'Connection error details: code=ECONNREFUSED, message=connect ECONNREFUSED',
    );
  });

  it('adds the (SSL error) label for a TLS code', () => {
    const root = Object.assign(new Error('self signed certificate'), { code: 'DEPTH_ZERO_SELF_SIGNED_CERT' });
    const wrapper = Object.assign(new Error('Connection error.'), { cause: root });
    assert.equal(
      connectionErrorDiagnostic(wrapper),
      'Connection error details: code=DEPTH_ZERO_SELF_SIGNED_CERT (SSL error), message=self signed certificate',
    );
  });
});

// ---------------------------------------------------------------------------
// friendlyErrorMessage — model-tier-aware rate-limit fallback suggestion
// ---------------------------------------------------------------------------

describe('friendlyErrorMessage — rate-limit model fallback', () => {
  const classified = classifyError('HTTP 429 rate limit exceeded');

  it('names a concrete lighter model when the current model has a sibling', () => {
    const msg = friendlyErrorMessage(classified, { model: 'claude-opus-4-6' });
    assert.match(msg, /Rate limited by the provider/);
    assert.match(msg, /\/model claude-opus-4-1 to switch to a model with separate limits/);
  });

  it('falls back to the generic rate-limit message when no sibling exists', () => {
    const msg = friendlyErrorMessage(classified, { model: 'gpt-4o' });
    assert.equal(msg, 'Rate limited by the provider — retrying after a delay.');
  });

  it('falls back to the generic message when no model is supplied', () => {
    const msg = friendlyErrorMessage(classified);
    assert.equal(msg, 'Rate limited by the provider — retrying after a delay.');
  });
});
