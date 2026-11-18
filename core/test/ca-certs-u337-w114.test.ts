// ===========================================================================
// ca-certs-u337-w114.test.ts
//
// Parity unit U337 (wave 114) — memoized CA resolution + runtime cache
// invalidation in core/core/src/ca-certs.ts. Mirrors Claude Code's
// getCACertificates / clearCACertsCache (utils/caCerts.ts):
//
//   - getCACertificates(): env-driven CA trust bundle (bundled roots +
//     NODE_EXTRA_CA_CERTS file contents), memoized by env signature; identical in
//     value to assembleCATrustStore(env, undefined).
//   - clearCACertsCache(): invalidate the memo so a mid-session trust change
//     (e.g. a settings/trust-apply path re-writing the pinned cert file) takes
//     effect without restarting the process.
//   - caCertsCacheSize(): diagnostic — number of memoized env signatures.
//
// ESM-coverage-safe: injected env, temp files via fs.mkdtempSync, no network.
// ===========================================================================
import { describe, it, before, after, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import * as tls from 'node:tls';

import {
  getCACertificates,
  clearCACertsCache,
  caCertsCacheSize,
} from '../src/ca-certs';
import { assembleCATrustStore, NODE_EXTRA_CA_CERTS_ENV, type CATrustDeps } from '../src/utils/mtls';

const E = (o: Record<string, string>) => o as unknown as NodeJS.ProcessEnv;

const CERT_A = '-----BEGIN CERTIFICATE-----\nEXTRA_A\n-----END CERTIFICATE-----\n';
const CERT_B = '-----BEGIN CERTIFICATE-----\nEXTRA_B\n-----END CERTIFICATE-----\n';

describe('getCACertificates (memoized env-only CA resolution)', () => {
  let dir: string;
  let certPath: string;

  before(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cacerts-'));
    certPath = path.join(dir, 'extra.pem');
    fs.writeFileSync(certPath, CERT_A);
  });

  after(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  beforeEach(() => {
    clearCACertsCache();
  });

  it('returns undefined when no custom CA is configured', () => {
    assert.equal(getCACertificates(E({})), undefined);
  });

  it('returns the SAME value as assembleCATrustStore(env, undefined)', () => {
    const env = E({ [NODE_EXTRA_CA_CERTS_ENV]: certPath });
    const direct = assembleCATrustStore(env, undefined);
    const viaCache = getCACertificates(env);
    assert.deepEqual(viaCache, direct);
    // includes bundled roots + the extra cert file contents
    assert.ok(viaCache!.includes(CERT_A));
    assert.ok(viaCache!.length > 1);
  });

  it('memoizes by env signature — a same-path content change is NOT seen until cleared', () => {
    const env = E({ [NODE_EXTRA_CA_CERTS_ENV]: certPath });
    const first = getCACertificates(env);
    assert.ok(first!.includes(CERT_A));

    // Change the pinned cert file contents in place (same path -> same signature).
    fs.writeFileSync(certPath, CERT_B);
    const second = getCACertificates(env);
    assert.ok(second!.includes(CERT_A), 'stale memo still returns the old bundle');
    assert.ok(!second!.includes(CERT_B));

    // The invalidation hook forces a fresh read.
    clearCACertsCache();
    const third = getCACertificates(env);
    assert.ok(third!.includes(CERT_B), 'after clear, the new cert contents are read');
    assert.ok(!third!.includes(CERT_A));

    // restore for other tests
    fs.writeFileSync(certPath, CERT_A);
  });

  it('caches undefined too (no recompute for the no-custom-CA path)', () => {
    clearCACertsCache();
    assert.equal(caCertsCacheSize(), 0);
    assert.equal(getCACertificates(E({})), undefined);
    assert.equal(caCertsCacheSize(), 1);
    // second call hits the cached-undefined entry, cache size unchanged
    assert.equal(getCACertificates(E({})), undefined);
    assert.equal(caCertsCacheSize(), 1);
  });

  it('keys distinct env signatures as distinct entries', () => {
    clearCACertsCache();
    getCACertificates(E({}));
    getCACertificates(E({ [NODE_EXTRA_CA_CERTS_ENV]: certPath }));
    assert.equal(caCertsCacheSize(), 2);
  });

  it('deps-injected resolution BYPASSES the cache (offline/test path)', () => {
    clearCACertsCache();
    const deps: CATrustDeps = {
      rootCertificates: ['ROOT'],
      getSystemCACertificates: () => [],
      execArgv: ['--use-system-ca'],
    };
    const out = getCACertificates(E({}), deps);
    // --use-system-ca with empty system store falls back to the injected roots.
    assert.deepEqual(out, ['ROOT']);
    // nothing was written to the shared memo
    assert.equal(caCertsCacheSize(), 0);
  });
});

describe('clearCACertsCache', () => {
  it('drops every memoized signature and is a no-op when already empty', () => {
    clearCACertsCache();
    getCACertificates(E({}));
    assert.ok(caCertsCacheSize() >= 1);
    clearCACertsCache();
    assert.equal(caCertsCacheSize(), 0);
    // idempotent: clearing an empty cache does not throw
    assert.doesNotThrow(() => clearCACertsCache());
    assert.equal(caCertsCacheSize(), 0);
  });
});

describe('getCACertificates default env', () => {
  it('reads process.env when no env argument is given', () => {
    clearCACertsCache();
    const saved = process.env[NODE_EXTRA_CA_CERTS_ENV];
    delete process.env[NODE_EXTRA_CA_CERTS_ENV];
    try {
      // with no NODE_EXTRA_CA_CERTS and no system-ca flag in this test runner,
      // the default resolution is "no custom CA".
      const out = getCACertificates();
      // out is undefined unless the test runner itself was launched with a
      // system-ca flag; assert it matches the direct assembler either way.
      assert.deepEqual(out, assembleCATrustStore(process.env, undefined));
      // tls.rootCertificates is available in this runtime (sanity for the module).
      assert.ok(Array.isArray(tls.rootCertificates));
    } finally {
      if (saved === undefined) delete process.env[NODE_EXTRA_CA_CERTS_ENV];
      else process.env[NODE_EXTRA_CA_CERTS_ENV] = saved;
      clearCACertsCache();
    }
  });
});
