import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { getMTLSConfig, getTLSConnectOptions, MTLS_ENV } from '../src/utils/mtls';

// Fake PEM contents — the loader only reads the files, it does not parse them.
const FAKE_CERT = '-----BEGIN CERTIFICATE-----\nFAKECERT\n-----END CERTIFICATE-----\n';
const FAKE_KEY = '-----BEGIN PRIVATE KEY-----\nFAKEKEY\n-----END PRIVATE KEY-----\n';

describe('getMTLSConfig', () => {
  let dir: string;
  let certPath: string;
  let keyPath: string;

  before(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mtls-'));
    certPath = path.join(dir, 'client.crt');
    keyPath = path.join(dir, 'client.key');
    fs.writeFileSync(certPath, FAKE_CERT);
    fs.writeFileSync(keyPath, FAKE_KEY);
  });

  after(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('returns undefined when no env knob is set', () => {
    assert.equal(getMTLSConfig({} as NodeJS.ProcessEnv), undefined);
  });

  it('loads cert + key PEM contents and passphrase', () => {
    const env = {
      [MTLS_ENV.cert]: certPath,
      [MTLS_ENV.key]: keyPath,
      [MTLS_ENV.passphrase]: 'hunter2',
    } as unknown as NodeJS.ProcessEnv;
    const cfg = getMTLSConfig(env);
    assert.deepEqual(cfg, { cert: FAKE_CERT, key: FAKE_KEY, passphrase: 'hunter2' });
  });

  it('loads cert only (no key / passphrase)', () => {
    const cfg = getMTLSConfig({ [MTLS_ENV.cert]: certPath } as unknown as NodeJS.ProcessEnv);
    assert.deepEqual(cfg, { cert: FAKE_CERT });
  });

  it('returns a material object even when only the passphrase is set', () => {
    const cfg = getMTLSConfig({ [MTLS_ENV.passphrase]: 'p' } as unknown as NodeJS.ProcessEnv);
    assert.deepEqual(cfg, { passphrase: 'p' });
  });

  it('throws on a missing certificate file (hard misconfiguration)', () => {
    assert.throws(() => getMTLSConfig({ [MTLS_ENV.cert]: path.join(dir, 'nope.crt') } as unknown as NodeJS.ProcessEnv));
  });
});

describe('getTLSConnectOptions', () => {
  it('returns undefined without material or with only a passphrase', () => {
    assert.equal(getTLSConnectOptions(undefined), undefined);
    assert.equal(getTLSConnectOptions({ passphrase: 'p' }), undefined);
  });

  it('builds undici connect options from cert/key/passphrase', () => {
    assert.deepEqual(
      getTLSConnectOptions({ cert: FAKE_CERT, key: FAKE_KEY, passphrase: 'x' }),
      { cert: FAKE_CERT, key: FAKE_KEY, passphrase: 'x' },
    );
  });

  it('omits the passphrase key when absent', () => {
    assert.deepEqual(getTLSConnectOptions({ cert: FAKE_CERT }), { cert: FAKE_CERT });
  });
});
