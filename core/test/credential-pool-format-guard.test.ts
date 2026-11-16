import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { CredentialPool } from '../src/credential-pool';

// Parity gap (F0162): addKey must reject malformed key material, not just
// empty/whitespace-only strings. A credential is later placed verbatim into an
// Authorization / x-api-key HTTP header; a value containing a newline, carriage
// return, tab, or other control character can never be a valid header value and
// is a header-injection / request-corruption risk. Claude treats API-key
// material with explicit shape awareness (the sk-ant recognizer); AMI must at
// minimum refuse structurally-impossible key values before admitting them to the
// secret store. Trimming handles *surrounding* whitespace; this guard catches
// *internal* whitespace and control characters that trimming cannot fix.
describe('CredentialPool format guard: reject structurally invalid keys', () => {
  it('rejects a key containing a newline (header-injection defense)', () => {
    const pool = new CredentialPool();
    assert.throws(() => pool.addKey('sk-good\nInjected-Header: x'), /invalid characters/);
    assert.equal(pool.size, 0);
  });

  it('rejects a key containing a carriage return', () => {
    const pool = new CredentialPool();
    assert.throws(() => pool.addKey('sk-good\rvalue'), /invalid characters/);
    assert.equal(pool.size, 0);
  });

  it('rejects a key containing internal whitespace', () => {
    const pool = new CredentialPool();
    assert.throws(() => pool.addKey('sk inner space'), /invalid characters/);
    assert.equal(pool.size, 0);
  });

  it('rejects a key containing a control character (NUL/DEL)', () => {
    const pool = new CredentialPool();
    assert.throws(() => pool.addKey('sk-good\u0000'), /invalid characters/);
    assert.throws(() => pool.addKey('sk-good\u007f'), /invalid characters/);
    assert.equal(pool.size, 0);
  });

  it('still accepts a well-formed key with surrounding whitespace trimmed', () => {
    const pool = new CredentialPool();
    pool.addKey('  sk-ant-api03_Valid-Key_123  ');
    assert.equal(pool.acquire()!.apiKey, 'sk-ant-api03_Valid-Key_123');
  });

  it('still accepts short legacy-style keys (no minimum-length regression)', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    assert.equal(pool.acquire()!.apiKey, 'sk-1');
  });
});
