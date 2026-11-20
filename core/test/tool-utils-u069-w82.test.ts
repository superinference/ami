import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { redactSecrets } from '../src/tools/tool-utils';
import { redactSecretValue, REDACT_MIN_LENGTH } from '../src/env-access-guard';

// ---------------------------------------------------------------------------
// U069 (w82) — JSON field-name-based secret redaction + partial masking parity.
//
// Parity with the reference bridge/debugUtils.ts SECRET_FIELD_NAMES: redactSecrets
// must scrub a secret by its JSON FIELD NAME regardless of the value's shape, so
// an opaque custom token (one that matches no provider prefix) sitting under a
// `"access_token"`/`"session_ingress_token"`/`"environment_secret"` key cannot
// leak on the /export or log path. The partial-mask (fingerprint) mode that keeps
// a correlatable prefix/suffix for debug contexts already lives in
// env-access-guard.ts redactSecretValue; this locks both behaviors.
// ---------------------------------------------------------------------------

describe('redactSecrets — JSON field-name redaction (opaque values, parity add)', () => {
  // These field names carry opaque values that match NO provider token shape, so
  // only field-name-based redaction can catch them. Each would have leaked before.
  const opaqueFieldCases: Array<[string, string]> = [
    ['access_token', 'opaqueCustomValue1234567890'],
    ['access_token', 'nOtApRoViDeRsHaPeAtAlL____'],
    ['session_ingress_token', 'abcdefghijklmnopqrstuvwxyz'],
    ['environment_secret', 'supersecretvalue123456'],
  ];

  for (const [field, value] of opaqueFieldCases) {
    it(`redacts opaque value under "${field}"`, () => {
      const input = `{"${field}":"${value}"}`;
      const out = redactSecrets(input);
      assert.ok(!out.includes(value), `${field} value must be redacted`);
      assert.ok(out.includes('[REDACTED-SECRET]'), 'placeholder present');
      // The field name itself stays so a reader still knows WHAT was redacted.
      assert.ok(out.includes(`"${field}"`), 'field name preserved');
    });
  }

  it('redacts accesstoken (no underscore) as well', () => {
    const out = redactSecrets('{"accesstoken":"opaqueCustomValue1234567890"}');
    assert.ok(!out.includes('opaqueCustomValue1234567890'));
    assert.ok(out.includes('[REDACTED-SECRET]'));
  });

  it('still redacts the pre-existing token/secret JSON fields (no regression)', () => {
    const out = redactSecrets('{"token":"opaqueTok1234567890abcd","secret":"opaqueSecret1234567890ab"}');
    assert.ok(!out.includes('opaqueTok1234567890abcd'), 'token redacted');
    assert.ok(!out.includes('opaqueSecret1234567890ab'), 'secret redacted');
  });

  it('does NOT redact a non-sensitive field name', () => {
    const input = '{"username":"alice_in_wonderland_123"}';
    assert.equal(redactSecrets(input), input);
  });
});

describe('partial-mask (fingerprint) redaction — redactSecretValue preview', () => {
  it('keeps a prefix(8)/suffix(4) fingerprint for long values', () => {
    const value = 'abcdefghIJKLMNOPqrstuvwx';
    const masked = redactSecretValue(value, 'api_key', true);
    assert.equal(masked, `${value.slice(0, 8)}...${value.slice(-4)}`);
    assert.ok(!masked.includes(value), 'full value never present');
  });

  it('fully masks values shorter than REDACT_MIN_LENGTH', () => {
    const short = 'x'.repeat(REDACT_MIN_LENGTH - 1);
    assert.equal(redactSecretValue(short, 'api_key', true), '[REDACTED_API_KEY]');
  });

  it('fully masks by default (preview off) regardless of length', () => {
    const value = 'abcdefghIJKLMNOPqrstuvwx';
    assert.equal(redactSecretValue(value, 'token', false), '[REDACTED_TOKEN]');
  });
});
