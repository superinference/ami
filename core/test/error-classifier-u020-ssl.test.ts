import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  classifyError,
  getSSLErrorHint,
  matchesSslErrorCode,
  SSL_ERROR_CODES,
} from '../src/error-classifier';

// U020 #11 — comprehensive OpenSSL/TLS error-code set with per-code guidance.
// The codes newly carried into classifyError's ssl branch (previously only a
// subset were matched).
const NEWLY_COVERED_CODES = [
  'CERT_SIGNATURE_FAILURE',
  'CERT_REJECTED',
  'CERT_UNTRUSTED',
  'CERT_CHAIN_TOO_LONG',
  'PATH_LENGTH_EXCEEDED',
  'HOSTNAME_MISMATCH',
  'ERR_TLS_HANDSHAKE_TIMEOUT',
  'ERR_SSL_WRONG_VERSION_NUMBER',
  'ERR_SSL_DECRYPTION_FAILED_OR_BAD_RECORD_MAC',
];

describe('SSL_ERROR_CODES — canonical set parity', () => {
  it('contains the full OpenSSL/TLS breadth Claude Code tracks', () => {
    for (const code of [
      'UNABLE_TO_VERIFY_LEAF_SIGNATURE', 'UNABLE_TO_GET_ISSUER_CERT', 'UNABLE_TO_GET_ISSUER_CERT_LOCALLY',
      'CERT_SIGNATURE_FAILURE', 'CERT_NOT_YET_VALID', 'CERT_HAS_EXPIRED', 'CERT_REVOKED', 'CERT_REJECTED',
      'CERT_UNTRUSTED', 'DEPTH_ZERO_SELF_SIGNED_CERT', 'SELF_SIGNED_CERT_IN_CHAIN', 'CERT_CHAIN_TOO_LONG',
      'PATH_LENGTH_EXCEEDED', 'ERR_TLS_CERT_ALTNAME_INVALID', 'HOSTNAME_MISMATCH', 'ERR_TLS_HANDSHAKE_TIMEOUT',
      'ERR_SSL_WRONG_VERSION_NUMBER', 'ERR_SSL_DECRYPTION_FAILED_OR_BAD_RECORD_MAC',
    ]) {
      assert.ok(SSL_ERROR_CODES.has(code), `${code} must be in SSL_ERROR_CODES`);
    }
  });
  it('matchesSslErrorCode is case-insensitive on the lowered message', () => {
    assert.equal(matchesSslErrorCode('tls: err_ssl_wrong_version_number at handshake'), true);
    assert.equal(matchesSslErrorCode('econnreset while reading'), false);
  });
});

describe('classifyError — ssl_error branch covers the expanded code set', () => {
  for (const code of NEWLY_COVERED_CODES) {
    it(`classifies ${code} as ssl_error (non-retryable)`, () => {
      const c = classifyError(`Error: TLS handshake failed: ${code}`);
      assert.equal(c.category, 'ssl_error', `${code} must classify as ssl_error`);
      assert.equal(c.retryable, false, 'SSL errors are configuration issues, not transient');
    });
  }
  it('still classifies a plain ECONNRESET as network_error, not ssl_error', () => {
    const c = classifyError('Error: socket write ECONNRESET');
    assert.equal(c.category, 'network_error');
  });
});

describe('getSSLErrorHint — per-code actionable guidance', () => {
  const cases: Array<[string, string]> = [
    ['CERT_SIGNATURE_FAILURE', 'signature'],
    ['CERT_REJECTED', 'rejected'],
    ['CERT_UNTRUSTED', 'untrusted'],
    ['CERT_CHAIN_TOO_LONG', 'chain is too long'],
    ['PATH_LENGTH_EXCEEDED', 'chain is too long'],
    ['HOSTNAME_MISMATCH', 'hostname mismatch'],
    ['ERR_TLS_HANDSHAKE_TIMEOUT', 'handshake timed out'],
    ['ERR_SSL_WRONG_VERSION_NUMBER', 'wrong version number'],
    ['ERR_SSL_DECRYPTION_FAILED_OR_BAD_RECORD_MAC', 'decryption failed'],
  ];
  for (const [code, needle] of cases) {
    it(`${code} → a specific, certificate-flavored hint mentioning "${needle}"`, () => {
      const hint = getSSLErrorHint(`boom: ${code}`);
      assert.ok(hint.toLowerCase().includes(needle.toLowerCase()), `hint should mention "${needle}": ${hint}`);
      assert.ok(/ssl|tls/i.test(hint), 'hint must name SSL/TLS');
    });
  }
  it('falls back to the generic SSL hint for an unrecognized SSL error', () => {
    const hint = getSSLErrorHint('some unspecified ssl failure');
    assert.ok(/ssl\/tls certificate error/i.test(hint));
  });
});
