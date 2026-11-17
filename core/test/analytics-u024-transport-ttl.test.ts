import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  AnalyticsTracker,
  shouldBypassProxy,
  resolveOtlpProxyUrl,
  resolveOtlpTlsConfig,
  buildOtlpDispatcher,
  resolveOtlpTransport,
  resolveInMemoryEventTtlMs,
  DEFAULT_IN_MEMORY_EVENT_TTL_MS,
  postOtlpBatch,
  type TelemetryBatch,
} from '../src/analytics';

const batch = (): TelemetryBatch => ({ events: [{ type: 't', timestamp: 1, data: {} }], sessionId: 's', batchId: 1 });

// A fake undici module recording constructor args so the dispatcher wiring is
// observable without a real proxy / TLS handshake.
function fakeUndici() {
  const proxyCalls: Array<{ uri: string; requestTls?: unknown }> = [];
  const agentCalls: Array<{ connect?: unknown }> = [];
  class ProxyAgent {
    kind = 'proxy';
    constructor(public opts: { uri: string; requestTls?: unknown }) { proxyCalls.push(opts); }
  }
  class Agent {
    kind = 'agent';
    constructor(public opts: { connect?: unknown }) { agentCalls.push(opts); }
  }
  return { mod: { ProxyAgent, Agent }, proxyCalls, agentCalls };
}

const captureFetch = (sink: Array<{ dispatcher?: unknown }>): typeof fetch =>
  (async (_url: string, init?: { dispatcher?: unknown }) => {
    sink.push({ dispatcher: init?.dispatcher });
    return { ok: true, status: 200 } as Response;
  }) as unknown as typeof fetch;

describe('U024 OTLP transport — proxy bypass (NO_PROXY)', () => {
  it('no NO_PROXY → never bypass', () => {
    assert.equal(shouldBypassProxy('https://a.example.com', undefined), false);
    assert.equal(shouldBypassProxy('https://a.example.com', ''), false);
  });
  it('wildcard * bypasses everything', () => {
    assert.equal(shouldBypassProxy('https://any.host', '*'), true);
  });
  it('exact host, dot-suffix, *.suffix, and port-stripped entries match', () => {
    assert.equal(shouldBypassProxy('https://api.example.com', 'api.example.com'), true);
    assert.equal(shouldBypassProxy('https://api.example.com', '.example.com'), true);
    assert.equal(shouldBypassProxy('https://api.example.com', '*.example.com'), true);
    assert.equal(shouldBypassProxy('https://api.example.com:4318', 'example.com:4318'), true);
    assert.equal(shouldBypassProxy('https://api.example.com', 'other.com, example.com'), true);
  });
  it('non-matching entry and bad URL → false', () => {
    assert.equal(shouldBypassProxy('https://api.example.com', 'foo.com'), false);
    assert.equal(shouldBypassProxy('https://api.example.com', ', ,'), false);
    assert.equal(shouldBypassProxy('::not a url::', '*.example.com'), false);
  });
});

describe('U024 OTLP transport — proxy URL resolution', () => {
  it('bad target URL → undefined', () => {
    assert.equal(resolveOtlpProxyUrl('::nope::', { HTTPS_PROXY: 'http://p' }), undefined);
  });
  it('https target picks HTTPS_PROXY (and lower-case https_proxy)', () => {
    assert.equal(resolveOtlpProxyUrl('https://c/v1/logs', { HTTPS_PROXY: 'http://p:1' }), 'http://p:1');
    assert.equal(resolveOtlpProxyUrl('https://c/v1/logs', { https_proxy: 'http://p:2' }), 'http://p:2');
  });
  it('http target picks HTTP_PROXY', () => {
    assert.equal(resolveOtlpProxyUrl('http://c/v1/logs', { HTTP_PROXY: 'http://p:3' }), 'http://p:3');
  });
  it('OTEL_EXPORTER_OTLP_PROXY overrides, ALL_PROXY is the last fallback', () => {
    assert.equal(
      resolveOtlpProxyUrl('https://c/v1/logs', { OTEL_EXPORTER_OTLP_PROXY: 'http://explicit', HTTPS_PROXY: 'http://p' }),
      'http://explicit',
    );
    assert.equal(resolveOtlpProxyUrl('https://c/v1/logs', { ALL_PROXY: 'http://all' }), 'http://all');
  });
  it('no proxy configured → undefined; NO_PROXY match suppresses', () => {
    assert.equal(resolveOtlpProxyUrl('https://c/v1/logs', {}), undefined);
    assert.equal(
      resolveOtlpProxyUrl('https://c.internal/v1/logs', { HTTPS_PROXY: 'http://p', NO_PROXY: '.internal' }),
      undefined,
    );
  });
});

describe('U024 OTLP transport — mTLS/CA config resolution', () => {
  it('nothing set → undefined', () => {
    assert.equal(resolveOtlpTlsConfig({}), undefined);
  });
  it('reads cert/key/ca files, logs-specific overrides generic, and passphrase', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u024-tls-'));
    const certP = path.join(dir, 'cert.pem');
    const keyP = path.join(dir, 'key.pem');
    const caP = path.join(dir, 'ca.pem');
    const logsCertP = path.join(dir, 'logs-cert.pem');
    fs.writeFileSync(certP, 'CERT'); fs.writeFileSync(keyP, 'KEY');
    fs.writeFileSync(caP, 'CA'); fs.writeFileSync(logsCertP, 'LOGSCERT');
    const cfg = resolveOtlpTlsConfig({
      OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE: certP,
      OTEL_EXPORTER_OTLP_LOGS_CLIENT_CERTIFICATE: logsCertP,
      OTEL_EXPORTER_OTLP_CLIENT_KEY: keyP,
      OTEL_EXPORTER_OTLP_CERTIFICATE: caP,
      AMI_OTEL_CLIENT_KEY_PASSPHRASE: 'pw',
    });
    assert.deepEqual(cfg, { cert: 'LOGSCERT', key: 'KEY', ca: 'CA', passphrase: 'pw' });
    fs.rmSync(dir, { recursive: true, force: true });
  });
  it('passphrase only (no files) still yields a config', () => {
    assert.deepEqual(resolveOtlpTlsConfig({ AMI_OTEL_CLIENT_KEY_PASSPHRASE: 'pw' }), { passphrase: 'pw' });
  });
  it('unreadable PEM collapses to undefined (never throws)', () => {
    const cfg = resolveOtlpTlsConfig({ OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE: '/no/such/cert.pem' });
    assert.equal(cfg, undefined);
  });
});

describe('U024 OTLP transport — dispatcher construction', () => {
  it('no proxy and no TLS → undefined (undici never loaded)', () => {
    let loaded = false;
    const d = buildOtlpDispatcher({}, () => { loaded = true; return fakeUndici().mod; });
    assert.equal(d, undefined);
    assert.equal(loaded, false);
  });
  it('proxy only → ProxyAgent without requestTls', () => {
    const f = fakeUndici();
    const d = buildOtlpDispatcher({ proxyUrl: 'http://p:8080' }, () => f.mod) as { kind: string };
    assert.equal(d.kind, 'proxy');
    assert.deepEqual(f.proxyCalls, [{ uri: 'http://p:8080' }]);
  });
  it('proxy + TLS → ProxyAgent carrying requestTls', () => {
    const f = fakeUndici();
    buildOtlpDispatcher({ proxyUrl: 'http://p', tls: { cert: 'C', key: 'K' } }, () => f.mod);
    assert.deepEqual(f.proxyCalls[0], { uri: 'http://p', requestTls: { cert: 'C', key: 'K' } });
  });
  it('TLS only → Agent with connect material', () => {
    const f = fakeUndici();
    const d = buildOtlpDispatcher({ tls: { ca: 'CA' } }, () => f.mod) as { kind: string };
    assert.equal(d.kind, 'agent');
    assert.deepEqual(f.agentCalls, [{ connect: { ca: 'CA' } }]);
  });
  it('default loader resolves the real undici ProxyAgent (construction only, no connect)', () => {
    const d = buildOtlpDispatcher({ proxyUrl: 'http://127.0.0.1:1' });
    assert.ok(d, 'real undici dispatcher constructed');
    // Construct again to exercise the module cache branch.
    assert.ok(buildOtlpDispatcher({ proxyUrl: 'http://127.0.0.1:1' }));
  });
  it('resolveOtlpTransport combines proxy + TLS, and yields undefined when nothing is set', () => {
    const f = fakeUndici();
    const d = resolveOtlpTransport('https://c/v1/logs', { HTTPS_PROXY: 'http://p' }, { undiciLoader: () => f.mod });
    assert.ok(d);
    assert.equal(f.proxyCalls.length, 1);
    assert.equal(resolveOtlpTransport('https://c/v1/logs', {}, { undiciLoader: () => fakeUndici().mod }), undefined);
  });
});

describe('U024 OTLP transport — postOtlpBatch + AnalyticsTracker wiring', () => {
  it('postOtlpBatch attaches an injected dispatcher to the fetch init', async () => {
    const seen: Array<{ dispatcher?: unknown }> = [];
    const sentinel = { id: 'disp' };
    await postOtlpBatch('https://c/v1/logs', batch(), { fetchImpl: captureFetch(seen), dispatcher: sentinel });
    assert.equal(seen[0].dispatcher, sentinel);
  });
  it('postOtlpBatch omits dispatcher when none given', async () => {
    const seen: Array<{ dispatcher?: unknown }> = [];
    await postOtlpBatch('https://c/v1/logs', batch(), { fetchImpl: captureFetch(seen) });
    assert.equal(seen[0].dispatcher, undefined);
  });
  it('AnalyticsTracker auto-resolves a proxy dispatcher from env and threads it into every export', async () => {
    const seen: Array<{ dispatcher?: unknown }> = [];
    const f = fakeUndici();
    const t = new AnalyticsTracker(undefined, {
      otlpEndpoint: 'https://collector.example/v1/logs',
      otlpFetch: captureFetch(seen),
      env: { HTTPS_PROXY: 'http://corp-proxy:8080' } as NodeJS.ProcessEnv,
      undiciLoader: () => f.mod,
    });
    t.setSessionId('sess');
    t.log('event_one', { a: 1 });
    await t.flushOtlp();
    assert.equal(seen.length, 1);
    assert.equal((seen[0].dispatcher as { kind: string }).kind, 'proxy');
    assert.deepEqual(f.proxyCalls, [{ uri: 'http://corp-proxy:8080' }]);
  });
  it('an explicit otlpDispatcher opt wins over env auto-resolution', async () => {
    const seen: Array<{ dispatcher?: unknown }> = [];
    const sentinel = { id: 'explicit' };
    const t = new AnalyticsTracker(undefined, {
      otlpEndpoint: 'https://collector.example/v1/logs',
      otlpFetch: captureFetch(seen),
      env: { HTTPS_PROXY: 'http://corp-proxy:8080' } as NodeJS.ProcessEnv,
      otlpDispatcher: sentinel,
    });
    t.setSessionId('sess');
    t.log('event_one');
    await t.flushOtlp();
    assert.equal(seen[0].dispatcher, sentinel);
  });
  it('no proxy/TLS configured → no dispatcher attached', async () => {
    const seen: Array<{ dispatcher?: unknown }> = [];
    const t = new AnalyticsTracker(undefined, {
      otlpEndpoint: 'https://collector.example/v1/logs',
      otlpFetch: captureFetch(seen),
      env: {} as NodeJS.ProcessEnv,
    });
    t.setSessionId('sess');
    t.log('event_one');
    await t.flushOtlp();
    assert.equal(seen[0].dispatcher, undefined);
  });
});

describe('U024 in-memory event TTL resolution', () => {
  it('explicit opt: positive → that value; <=0 → disabled', () => {
    assert.equal(resolveInMemoryEventTtlMs({}, 5000), 5000);
    assert.equal(resolveInMemoryEventTtlMs({}, 0), undefined);
    assert.equal(resolveInMemoryEventTtlMs({}, -1), undefined);
  });
  it('env numeric positive wins; <=0 disabled; unset → undefined', () => {
    assert.equal(resolveInMemoryEventTtlMs({ AMI_ANALYTICS_EVENT_TTL_MS: '1500' }), 1500);
    assert.equal(resolveInMemoryEventTtlMs({ AMI_ANALYTICS_EVENT_TTL_MS: '0' }), undefined);
    assert.equal(resolveInMemoryEventTtlMs({ AMI_ANALYTICS_EVENT_TTL_MS: '-5' }), undefined);
    assert.equal(resolveInMemoryEventTtlMs({}), undefined);
  });
  it('truthy non-numeric env → the default TTL; falsey → disabled', () => {
    assert.equal(resolveInMemoryEventTtlMs({ AMI_ANALYTICS_EVENT_TTL_MS: 'on' }), DEFAULT_IN_MEMORY_EVENT_TTL_MS);
    assert.equal(resolveInMemoryEventTtlMs({ AMI_ANALYTICS_EVENT_TTL_MS: 'false' }), undefined);
  });
});

describe('U024 in-memory event TTL eviction', () => {
  it('sweeps events older than the TTL on read, keeps recent ones', () => {
    let clock = 0;
    const t = new AnalyticsTracker(undefined, { inMemoryEventTtlMs: 100, now: () => clock, env: {} as NodeJS.ProcessEnv });
    clock = 0; t.log('a');
    clock = 80; t.log('b');
    // At t=150 the cutoff is 50: 'a' (recorded at 0) expired, 'b' (at 80) survives.
    clock = 150;
    const events = t.getEvents();
    assert.deepEqual(events.map(e => e.type), ['b']);
    assert.deepEqual(t.getSummary(), { b: 1 });
    assert.deepEqual(t.getEventsByType('a'), []);
  });
  it('a new log() also sweeps expired entries before appending', () => {
    let clock = 0;
    const t = new AnalyticsTracker(undefined, { inMemoryEventTtlMs: 100, now: () => clock, env: {} as NodeJS.ProcessEnv });
    clock = 0; t.log('old');
    clock = 500; t.log('fresh');
    assert.deepEqual(t.getEvents().map(e => e.type), ['fresh']);
  });
  it('disabled (default) retains all events regardless of age', () => {
    let clock = 0;
    const t = new AnalyticsTracker(undefined, { now: () => clock, env: {} as NodeJS.ProcessEnv });
    clock = 0; t.log('a');
    clock = 10_000_000; t.log('b');
    assert.equal(t.getEvents().length, 2);
  });
  it('reset clears the parallel time index so new events are not mis-aged', () => {
    let clock = 0;
    const t = new AnalyticsTracker(undefined, { inMemoryEventTtlMs: 100, now: () => clock, env: {} as NodeJS.ProcessEnv });
    clock = 0; t.log('a');
    t.reset();
    clock = 50; t.log('b');
    clock = 120; // cutoff 20: 'b' at 50 survives
    assert.deepEqual(t.getEvents().map(e => e.type), ['b']);
  });
});
