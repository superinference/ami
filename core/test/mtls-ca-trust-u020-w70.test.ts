// ===========================================================================
// mtls-ca-trust-u020-w70.test.ts
//
// Parity unit U020 (wave 70) — CA trust-store assembly for custom undici
// dispatchers in core/core/src/utils/mtls.ts. Mirrors the reference's
// getCACertificates / hasNodeOption (utils/caCerts.ts, utils/mtls.ts,
// utils/proxy.ts):
//
//   - hasNodeOption(): detect --use-system-ca / --use-openssl-ca in execArgv OR
//     NODE_OPTIONS (bare and =value forms).
//   - assembleCATrustStore(): merge bundled Mozilla roots (never replacing them)
//     + system store (when opted in) + NODE_EXTRA_CA_CERTS file + explicit
//     AMI_CLIENT_CA material; returns undefined when no custom trust is in play.
//   - describeCATrustStore(): log-safe summary (count + sources), no key material.
//
// ESM-coverage-safe: pure functions, injected TLS accessors + env, temp files via
// fs.mkdtempSync, no network.
// ===========================================================================
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  hasNodeOption,
  assembleCATrustStore,
  describeCATrustStore,
  NODE_EXTRA_CA_CERTS_ENV,
  SYSTEM_CA_NODE_OPTIONS,
  type CATrustDeps,
} from '../src/utils/mtls';

const E = (o: Record<string, string>) => o as unknown as NodeJS.ProcessEnv;

const ROOT_A = '-----BEGIN CERTIFICATE-----\nROOTA\n-----END CERTIFICATE-----\n';
const ROOT_B = '-----BEGIN CERTIFICATE-----\nROOTB\n-----END CERTIFICATE-----\n';
const SYS_C = '-----BEGIN CERTIFICATE-----\nSYSC\n-----END CERTIFICATE-----\n';
const EXTRA_D = '-----BEGIN CERTIFICATE-----\nEXTRAD\n-----END CERTIFICATE-----\n';
const CLIENT_CA = '-----BEGIN CERTIFICATE-----\nCLIENTCA\n-----END CERTIFICATE-----\n';

const DEPS: CATrustDeps = {
  rootCertificates: [ROOT_A, ROOT_B],
  getSystemCACertificates: () => [SYS_C],
  execArgv: [],
};

describe('hasNodeOption', () => {
  it('detects a bare flag in execArgv', () => {
    assert.equal(hasNodeOption('--use-system-ca', ['--use-system-ca'], undefined), true);
  });
  it('detects a flag=value form in execArgv', () => {
    assert.equal(hasNodeOption('--use-system-ca', ['--use-system-ca=1'], undefined), true);
  });
  it('detects a flag inside NODE_OPTIONS (space-separated)', () => {
    assert.equal(hasNodeOption('--use-openssl-ca', [], '--max-old-space-size=4096 --use-openssl-ca'), true);
  });
  it('returns false when neither execArgv nor NODE_OPTIONS carries the flag', () => {
    assert.equal(hasNodeOption('--use-system-ca', ['--other'], '--enable-source-maps'), false);
  });
  it('returns false when NODE_OPTIONS is undefined and execArgv lacks it', () => {
    assert.equal(hasNodeOption('--use-system-ca', [], undefined), false);
  });
  it('does not match a flag that is a prefix-without-boundary of another token', () => {
    // '--use-system-ca' must not match '--use-system-cache'
    assert.equal(hasNodeOption('--use-system-ca', ['--use-system-cache'], undefined), false);
  });
});

describe('assembleCATrustStore', () => {
  it('returns undefined when no system-ca, no NODE_EXTRA_CA_CERTS, no client CA (default path untouched)', () => {
    assert.equal(assembleCATrustStore(E({}), undefined, DEPS), undefined);
  });

  it('RED on revert: merges bundled roots with the client CA instead of replacing them', () => {
    // The whole point: setting `ca` on a dispatcher MUST NOT drop the default
    // roots. A naive `ca = clientCA` (the pre-fix behavior) would yield just
    // [CLIENT_CA]; the fix includes the bundled roots as the base.
    const bundle = assembleCATrustStore(E({}), CLIENT_CA, DEPS);
    assert.deepEqual(bundle, [ROOT_A, ROOT_B, CLIENT_CA]);
  });

  it('includes the OS/system store when --use-system-ca is set', () => {
    const bundle = assembleCATrustStore(E({}), undefined, { ...DEPS, execArgv: ['--use-system-ca'] });
    assert.deepEqual(bundle, [ROOT_A, ROOT_B, SYS_C]);
  });

  it('honors --use-openssl-ca via NODE_OPTIONS', () => {
    const bundle = assembleCATrustStore(E({ NODE_OPTIONS: '--use-openssl-ca' }), undefined, DEPS);
    assert.deepEqual(bundle, [ROOT_A, ROOT_B, SYS_C]);
  });

  it('merges NODE_EXTRA_CA_CERTS file contents', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-ca-'));
    const extra = path.join(dir, 'extra.pem');
    fs.writeFileSync(extra, EXTRA_D);
    try {
      const bundle = assembleCATrustStore(E({ [NODE_EXTRA_CA_CERTS_ENV]: extra }), undefined, DEPS);
      assert.deepEqual(bundle, [ROOT_A, ROOT_B, EXTRA_D]);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('skips an unreadable NODE_EXTRA_CA_CERTS best-effort (still returns bundled roots)', () => {
    const bundle = assembleCATrustStore(E({ [NODE_EXTRA_CA_CERTS_ENV]: '/no/such/ca-file.pem' }), undefined, DEPS);
    assert.deepEqual(bundle, [ROOT_A, ROOT_B]);
  });

  it('composes system + extra + client CA in order, deduplicating', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-ca-'));
    const extra = path.join(dir, 'extra.pem');
    fs.writeFileSync(extra, EXTRA_D);
    try {
      const bundle = assembleCATrustStore(
        E({ [NODE_EXTRA_CA_CERTS_ENV]: extra }),
        ROOT_A, // duplicate of a bundled root → must be deduped
        { ...DEPS, execArgv: ['--use-system-ca'] },
      );
      assert.deepEqual(bundle, [ROOT_A, ROOT_B, SYS_C, EXTRA_D]);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('falls back to bundled roots when the system-store accessor throws', () => {
    const bundle = assembleCATrustStore(E({}), undefined, {
      ...DEPS,
      execArgv: ['--use-system-ca'],
      getSystemCACertificates: () => { throw new Error('no system store'); },
    });
    assert.deepEqual(bundle, [ROOT_A, ROOT_B]);
  });

  it('accepts inline PEM for NODE_EXTRA_CA_CERTS (no temp file)', () => {
    const bundle = assembleCATrustStore(E({ [NODE_EXTRA_CA_CERTS_ENV]: EXTRA_D }), undefined, DEPS);
    assert.deepEqual(bundle, [ROOT_A, ROOT_B, EXTRA_D]);
  });
});

describe('describeCATrustStore', () => {
  it('returns undefined when no custom trust is assembled', () => {
    assert.equal(describeCATrustStore(E({}), undefined, DEPS), undefined);
  });
  it('summarizes count + sources without leaking cert bodies', () => {
    const desc = describeCATrustStore(E({ [NODE_EXTRA_CA_CERTS_ENV]: EXTRA_D }), CLIENT_CA, {
      ...DEPS,
      execArgv: ['--use-system-ca'],
    });
    assert.ok(desc);
    assert.match(desc!, /CA cert\(s\)/);
    assert.match(desc!, /bundled/);
    assert.match(desc!, /system/);
    assert.match(desc!, /extra-file/);
    assert.match(desc!, /client-ca/);
    // must not contain the actual PEM payloads
    assert.ok(!desc!.includes('CLIENTCA'));
    assert.ok(!desc!.includes('EXTRAD'));
  });
});

describe('SYSTEM_CA_NODE_OPTIONS', () => {
  it('lists the two Node system-CA opt-in flags', () => {
    assert.deepEqual([...SYSTEM_CA_NODE_OPTIONS], ['--use-system-ca', '--use-openssl-ca']);
  });
});
