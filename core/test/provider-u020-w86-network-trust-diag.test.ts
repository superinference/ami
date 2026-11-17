// ===========================================================================
// provider-u020-w86-network-trust-diag.test.ts
//
// Parity unit U020 (wave 86) — CA-cert & TLS-proxy RESOLUTION diagnostics trail
// in core/core/src/provider.ts.
//
// Gap: references/claude/claude-code-main/src/utils/caCertsConfig.ts:L41-L87
//   "Debug/introspection trail for CA-cert & TLS-proxy resolution" — Claude logs
//   each CA-cert / proxy resolution step via logForDebugging so an operator can
//   audit HOW the trust store / proxy routing was resolved. AMI previously only
//   emitted a human `log('provider', …)` line at dispatcher creation and had NO
//   record in its structured, machine-monitored diagnostics channel
//   (AMI_DIAGNOSTICS_FILE). This unit ADDS a PII-free structured
//   `network_trust_resolved` diagnostic at every HTTP-dispatcher creation site,
//   routed through recordDiagnostic → the diagnostics channel (the introspection
//   surface), so the resolution trail is auditable from the log.
//
// Includes an END-TO-END test driving the live withProxyDispatcher transport path
// with a proxy env + a client CA and a stubbed undici ProxyAgent, asserting the
// structured diagnostic lands in the configured AMI_DIAGNOSTICS_FILE — proving the
// whole wired path (resolution → record → channel), not just the unit.
//
// ESM-coverage-safe: env snapshotted/restored; temp files via mkdtemp; undici
// ProxyAgent stubbed via the proxyTransportHooks DI seam (no real network).
// ===========================================================================
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  recordNetworkTrustDiagnostics,
  NETWORK_TRUST_DIAGNOSTIC_EVENT,
  withProxyDispatcher,
  proxyTransportHooks,
  clearProxyAgentCache,
} from '../src/provider';
import { flushDiagnosticsWriters } from '../src/logger';

const E = (o: Record<string, string>) => o as unknown as NodeJS.ProcessEnv;
const CLIENT_CA = '-----BEGIN CERTIFICATE-----\nCLIENTCA\n-----END CERTIFICATE-----\n';

/** Make a fresh temp diagnostics file path (not created yet). */
function tmpDiagFile(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-netdiag-'));
  return path.join(dir, 'diag.jsonl');
}

/** Read + parse every JSONL record currently written to `file`. */
function readRecords(file: string): Array<{ level: string; event: string; data: Record<string, unknown> }> {
  flushDiagnosticsWriters();
  if (!fs.existsSync(file)) return [];
  return fs
    .readFileSync(file, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map(l => JSON.parse(l));
}

describe('recordNetworkTrustDiagnostics (CA-cert & TLS-proxy resolution trail)', () => {
  const savedEnv = { ...process.env };
  afterEach(() => {
    delete proxyTransportHooks.ProxyAgent;
    delete proxyTransportHooks.Agent;
    for (const k of Object.keys(process.env)) if (!(k in savedEnv)) delete process.env[k];
    Object.assign(process.env, savedEnv);
    clearProxyAgentCache();
  });

  it('records a PII-free structured event for a proxy dispatcher with a custom trust store', () => {
    const file = tmpDiagFile();
    process.env.AMI_DIAGNOSTICS_FILE = file;
    recordNetworkTrustDiagnostics('proxy', '3 CA cert(s) [bundled+client-ca]', {
      proxySideDns: true,
      clientCert: true,
    });
    const recs = readRecords(file).filter(r => r.event === NETWORK_TRUST_DIAGNOSTIC_EVENT);
    assert.equal(recs.length, 1);
    assert.equal(recs[0].level, 'info');
    assert.deepEqual(recs[0].data, {
      dispatcher: 'proxy',
      customTrustStore: true,
      trustStore: '3 CA cert(s) [bundled+client-ca]',
      proxySideDns: true,
      clientCert: true,
    });
  });

  it('omits the trustStore field and marks customTrustStore:false when no custom store was assembled', () => {
    const file = tmpDiagFile();
    process.env.AMI_DIAGNOSTICS_FILE = file;
    recordNetworkTrustDiagnostics('mtls', undefined, { clientCert: true });
    const rec = readRecords(file).find(r => r.event === NETWORK_TRUST_DIAGNOSTIC_EVENT)!;
    assert.equal(rec.data.dispatcher, 'mtls');
    assert.equal(rec.data.customTrustStore, false);
    assert.ok(!('trustStore' in rec.data), 'no trustStore key when there is no custom store');
    // posture defaults applied explicitly (never undefined in the record)
    assert.equal(rec.data.proxySideDns, false);
    assert.equal(rec.data.clientCert, true);
  });

  it('defaults posture flags to false when omitted', () => {
    const file = tmpDiagFile();
    process.env.AMI_DIAGNOSTICS_FILE = file;
    recordNetworkTrustDiagnostics('proxy', undefined);
    const rec = readRecords(file).find(r => r.event === NETWORK_TRUST_DIAGNOSTIC_EVENT)!;
    assert.equal(rec.data.proxySideDns, false);
    assert.equal(rec.data.clientCert, false);
  });

  it('is a zero-cost no-op when the diagnostics channel is unconfigured', () => {
    const file = tmpDiagFile();
    delete process.env.AMI_DIAGNOSTICS_FILE;
    delete process.env.CLAUDE_CODE_DIAGNOSTICS_FILE;
    recordNetworkTrustDiagnostics('proxy', '1 CA cert(s) [bundled]', { clientCert: true });
    assert.equal(fs.existsSync(file), false, 'nothing written when no diagnostics file is set');
  });

  it('END-TO-END: the live proxy transport path emits the resolution trail into the diagnostics channel', async () => {
    const file = tmpDiagFile();
    clearProxyAgentCache();

    // DI seam: a fake ProxyAgent ctor so no real proxy connection is attempted
    // (undici's namespace is frozen and cannot be monkeypatched).
    class FakeProxyAgent {
      constructor(_opts: unknown) {}
    }
    proxyTransportHooks.ProxyAgent = FakeProxyAgent as unknown as new (opts: unknown) => unknown;

    // The diagnostics channel is resolved from process.env by the logger, so it
    // must be set there (not only in the transport `env` arg).
    process.env.AMI_DIAGNOSTICS_FILE = file;
    const env = E({
      HTTPS_PROXY: 'http://proxy.local:8080',
      AMI_CLIENT_CA: CLIENT_CA,
      AMI_PROXY_RESOLVES_HOSTS: '1',
    });
    const base = (async () => new Response('ok')) as unknown as typeof fetch;

    const wrapped = withProxyDispatcher(base, env)!;
    assert.notEqual(wrapped, base);
    await wrapped('https://api.anthropic.com/v1/messages');

    const rec = readRecords(file).find(r => r.event === NETWORK_TRUST_DIAGNOSTIC_EVENT);
    assert.ok(rec, 'a network_trust_resolved record was written by the live transport path');
    assert.equal(rec!.data.dispatcher, 'proxy');
    assert.equal(rec!.data.customTrustStore, true);
    assert.equal(rec!.data.proxySideDns, true, 'proxy-side DNS posture captured from the resolved connect options');
    // PII-free: the recorded trust-store summary carries counts/sources only, never
    // the proxy URL, a hostname, or raw PEM/key material.
    const trustStore = String(rec!.data.trustStore ?? '');
    assert.match(trustStore, /CA cert\(s\) \[.*client-ca.*\]/);
    assert.doesNotMatch(trustStore, /proxy\.local|BEGIN CERTIFICATE|CLIENTCA/);
  });
});
