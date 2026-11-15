import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  getMTLSConfig,
  getTLSConnectOptions,
  getSecureTLSConnectOptions,
  redactMTLSMaterial,
  MTLS_ENV,
  DEFAULT_MIN_TLS_VERSION,
} from '../src/utils/mtls';

const FAKE_CERT = '-----BEGIN CERTIFICATE-----\nFAKECERT\n-----END CERTIFICATE-----\n';
const FAKE_KEY = '-----BEGIN PRIVATE KEY-----\nFAKEKEY\n-----END PRIVATE KEY-----\n';
const FAKE_CA = '-----BEGIN CERTIFICATE-----\nFAKECA\n-----END CERTIFICATE-----\n';

const E = (o: Record<string, string>) => o as unknown as NodeJS.ProcessEnv;

describe('utils/mtls parity — CA bundle loading', () => {
  let dir: string;
  let caPath: string;
  before(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mtls-ca-'));
    caPath = path.join(dir, 'ca.pem');
    fs.writeFileSync(caPath, FAKE_CA);
  });
  after(() => fs.rmSync(dir, { recursive: true, force: true }));

  // RED on revert: without ca handling in getMTLSConfig, material.ca is undefined.
  it('loads a custom CA bundle from a file path', () => {
    const cfg = getMTLSConfig(E({ [MTLS_ENV.ca]: caPath }));
    assert.deepEqual(cfg, { ca: FAKE_CA });
  });

  // RED on revert: ca not forwarded into connect options.
  it('forwards the CA into TLS connect options (CA-only trust, no client cert)', () => {
    const opts = getTLSConnectOptions({ ca: FAKE_CA });
    assert.deepEqual(opts, { ca: FAKE_CA });
  });

  // Backward-compat guard: CA + cert + key + passphrase all present.
  it('combines cert/key/passphrase/ca', () => {
    const opts = getTLSConnectOptions({ cert: FAKE_CERT, key: FAKE_KEY, passphrase: 'x', ca: FAKE_CA });
    assert.deepEqual(opts, { cert: FAKE_CERT, key: FAKE_KEY, passphrase: 'x', ca: FAKE_CA });
  });
});

describe('utils/mtls parity — inline PEM via env', () => {
  // RED on revert: without inline detection, an inline PEM value is treated as a
  // file path and readFileSync throws (ENOENT), so this would throw instead of
  // returning the inline material.
  it('accepts inline cert/key PEM directly from env (no temp file)', () => {
    const cfg = getMTLSConfig(E({ [MTLS_ENV.cert]: FAKE_CERT, [MTLS_ENV.key]: FAKE_KEY }));
    assert.deepEqual(cfg, { cert: FAKE_CERT, key: FAKE_KEY });
  });

  it('accepts inline CA PEM directly from env', () => {
    const cfg = getMTLSConfig(E({ [MTLS_ENV.ca]: FAKE_CA }));
    assert.deepEqual(cfg, { ca: FAKE_CA });
  });
});

describe('utils/mtls parity — labeled missing-file error', () => {
  // RED on revert: the raw fs error ("ENOENT: no such file ...") does NOT name
  // the env var, so these assertions on the labeled message would fail.
  it('names the env var and path when a cert file is missing', () => {
    assert.throws(
      () => getMTLSConfig(E({ [MTLS_ENV.cert]: '/no/such/cert.pem' })),
      (err: Error) => {
        assert.match(err.message, /mTLS: failed to read/);
        assert.match(err.message, new RegExp(MTLS_ENV.cert));
        assert.match(err.message, /\/no\/such\/cert\.pem/);
        return true;
      },
    );
  });

  it('names the env var when a CA file is missing', () => {
    assert.throws(
      () => getMTLSConfig(E({ [MTLS_ENV.ca]: '/no/such/ca.pem' })),
      (err: Error) => {
        assert.match(err.message, new RegExp(MTLS_ENV.ca));
        return true;
      },
    );
  });
});

describe('utils/mtls parity — secure TLS min-version floor', () => {
  // RED on revert: getSecureTLSConnectOptions does not exist / lacks minVersion.
  it('pins a secure default min version with no client material', () => {
    const opts = getSecureTLSConnectOptions(undefined, E({}));
    assert.deepEqual(opts, { minVersion: DEFAULT_MIN_TLS_VERSION });
    assert.equal(DEFAULT_MIN_TLS_VERSION, 'TLSv1.2');
  });

  it('merges the min version onto client material', () => {
    const opts = getSecureTLSConnectOptions({ cert: FAKE_CERT, key: FAKE_KEY }, E({}));
    assert.deepEqual(opts, { cert: FAKE_CERT, key: FAKE_KEY, minVersion: 'TLSv1.2' });
  });

  // RED on revert: env override not honored.
  it('honors AMI_CLIENT_TLS_MIN_VERSION override', () => {
    const opts = getSecureTLSConnectOptions(undefined, E({ [MTLS_ENV.minVersion]: 'TLSv1.3' }));
    assert.equal(opts.minVersion, 'TLSv1.3');
  });

  // RED on revert: invalid version silently accepted instead of surfaced.
  it('throws on an invalid min version (surfaced misconfiguration)', () => {
    assert.throws(
      () => getSecureTLSConnectOptions(undefined, E({ [MTLS_ENV.minVersion]: 'SSLv3' })),
      (err: Error) => {
        assert.match(err.message, new RegExp(MTLS_ENV.minVersion));
        assert.match(err.message, /invalid/);
        return true;
      },
    );
  });
});

describe('utils/mtls parity — secret redaction', () => {
  // RED on revert: redactMTLSMaterial does not exist, so the key/passphrase would
  // never be masked for logging.
  it('masks private key and passphrase, summarizes public material', () => {
    const red = redactMTLSMaterial({ cert: FAKE_CERT, key: FAKE_KEY, passphrase: 'hunter2', ca: FAKE_CA });
    assert.equal(red!.key, '[redacted]');
    assert.equal(red!.passphrase, '[redacted]');
    assert.equal(red!.cert, `<cert:${FAKE_CERT.length}b>`);
    assert.equal(red!.ca, `<ca:${FAKE_CA.length}b>`);
    // The secret bytes must not appear anywhere in the redacted view.
    const serialized = JSON.stringify(red);
    assert.ok(!serialized.includes('FAKEKEY'));
    assert.ok(!serialized.includes('hunter2'));
  });

  it('returns undefined for undefined material', () => {
    assert.equal(redactMTLSMaterial(undefined), undefined);
  });
});
