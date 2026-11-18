import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  checkGcpCredentialsValid,
  GCP_CREDENTIALS_CHECK_TIMEOUT_MS,
} from '../src/gcp-credentials';

describe('checkGcpCredentialsValid', () => {
  it('exposes a short (5s) default timeout matching the GCE metadata-hang guard', () => {
    assert.equal(GCP_CREDENTIALS_CHECK_TIMEOUT_MS, 5_000);
  });

  it('returns true when the ADC token fetch resolves within the bound', async () => {
    const valid = await checkGcpCredentialsValid({
      fetchAccessToken: async () => {
        /* token obtained */
      },
      timeoutMs: 1_000,
    });
    assert.equal(valid, true);
  });

  it('returns false (never throws) when the ADC token fetch rejects', async () => {
    const valid = await checkGcpCredentialsValid({
      fetchAccessToken: async () => {
        throw new Error('invalid_grant: ADC not configured');
      },
      timeoutMs: 1_000,
    });
    assert.equal(valid, false);
  });

  it('returns false (never throws) when a missing google-auth-library import rejects', async () => {
    const valid = await checkGcpCredentialsValid({
      fetchAccessToken: async () => {
        // Simulate `await import('google-auth-library')` failing (never installed).
        throw Object.assign(new Error('Cannot find module'), { code: 'ERR_MODULE_NOT_FOUND' });
      },
      timeoutMs: 1_000,
    });
    assert.equal(valid, false);
  });

  it('times out fast instead of hanging ~12s on the GCE metadata server', async () => {
    // A probe with no local credential would (without the guard) stall on the GCE
    // metadata server. Here the fetch never settles; the timeout must win quickly.
    const started = Date.now();
    const valid = await checkGcpCredentialsValid({
      fetchAccessToken: () => new Promise<void>(() => {
        /* never resolves — models the ~12s metadata-server hang */
      }),
      timeoutMs: 40,
    });
    const elapsed = Date.now() - started;
    assert.equal(valid, false);
    // Proves the race returned control well before the simulated hang — far under
    // the ~12s metadata stall this guard exists to prevent.
    assert.ok(elapsed < 1_000, `probe should time out fast, took ${elapsed}ms`);
  });
});
