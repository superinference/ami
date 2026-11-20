// ===========================================================================
// provider-u020-w70.test.ts
//
// Parity unit U020 (wave 70) — custom-dispatcher trust + proxy-side DNS +
// dispatcher-cache lifecycle in core/core/src/provider.ts. Mirrors the reference:
//
//   - proxyResolvesHosts() / getAddressFamily() / proxy-side DNS lookup
//     [ref: utils/proxy.ts CLAUDE_CODE_PROXY_RESOLVES_HOSTS, getAddressFamily]
//   - buildProxyConnectOptions(): merge the CA trust store onto the dispatcher so
//     a custom ca never REPLACES the default roots, and install the proxy-side
//     DNS lookup when opted in [ref: utils/proxy.ts getCACertificates injection]
//   - clearProxyAgentCache() + proxyAgentCacheSize(): drop cached dispatchers so a
//     mid-session env change is honored [ref: utils/managedEnv.ts
//     configureGlobalAgents + cache clears]
//   - NETWORK_ENV_FLAGS: introspection manifest entries.
//
// Includes an end-to-end test that drives withProxyDispatcher (the live
// transport path) with a proxy env set + a client CA, capturing the undici
// ProxyAgent construction and asserting the merged trust store rides the
// dispatcher — proving the whole wired path, not just a unit.
//
// ESM-coverage-safe: env snapshotted + restored; undici ProxyAgent/Agent
// constructors stubbed via mock.method on the dynamically-imported module so no
// real network/proxy is touched.
// ===========================================================================
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  proxyResolvesHosts,
  getAddressFamily,
  buildProxyConnectOptions,
  clearProxyAgentCache,
  proxyAgentCacheSize,
  withProxyDispatcher,
  proxyTransportHooks,
  NETWORK_ENV_FLAGS,
} from '../src/provider';

const E = (o: Record<string, string>) => o as unknown as NodeJS.ProcessEnv;
const CLIENT_CA = '-----BEGIN CERTIFICATE-----\nCLIENTCA\n-----END CERTIFICATE-----\n';

describe('getAddressFamily', () => {
  it('maps 4 / IPv4 → 4', () => {
    assert.equal(getAddressFamily(4), 4);
    assert.equal(getAddressFamily('IPv4'), 4);
  });
  it('maps 6 / IPv6 → 6', () => {
    assert.equal(getAddressFamily(6), 6);
    assert.equal(getAddressFamily('IPv6'), 6);
  });
  it('maps undefined / unknown → 0 (unspecified)', () => {
    assert.equal(getAddressFamily(undefined), 0);
    assert.equal(getAddressFamily(0), 0);
    assert.equal(getAddressFamily('weird'), 0);
  });
});

describe('proxyResolvesHosts', () => {
  it('is off by default', () => {
    assert.equal(proxyResolvesHosts(E({})), false);
  });
  it('honors AMI_PROXY_RESOLVES_HOSTS', () => {
    assert.equal(proxyResolvesHosts(E({ AMI_PROXY_RESOLVES_HOSTS: '1' })), true);
  });
  it('honors the reference name for parity', () => {
    assert.equal(proxyResolvesHosts(E({ CLAUDE_CODE_PROXY_RESOLVES_HOSTS: 'true' })), true);
  });
  it('is off for a falsy value', () => {
    assert.equal(proxyResolvesHosts(E({ AMI_PROXY_RESOLVES_HOSTS: '0' })), false);
  });
});

describe('buildProxyConnectOptions', () => {
  it('returns undefined when nothing to apply (no mTLS, no custom CA, no proxy DNS)', () => {
    assert.equal(buildProxyConnectOptions(E({}), undefined), undefined);
  });

  it('merges the bundled roots with a client CA so defaults are preserved', () => {
    const opts = buildProxyConnectOptions(E({}), { ca: CLIENT_CA });
    assert.ok(opts);
    assert.ok(Array.isArray(opts!.ca));
    // bundled roots present (many) + the client CA at the end
    assert.ok((opts!.ca as string[]).length > 1);
    assert.equal((opts!.ca as string[])[(opts!.ca as string[]).length - 1], CLIENT_CA);
  });

  it('passes through client cert/key/passphrase', () => {
    const opts = buildProxyConnectOptions(E({}), { cert: 'C', key: 'K', passphrase: 'P' });
    assert.ok(opts);
    assert.equal(opts!.cert, 'C');
    assert.equal(opts!.key, 'K');
    assert.equal(opts!.passphrase, 'P');
  });

  it('installs a proxy-side DNS lookup when AMI_PROXY_RESOLVES_HOSTS is on', () => {
    const opts = buildProxyConnectOptions(E({ AMI_PROXY_RESOLVES_HOSTS: '1' }), undefined);
    assert.ok(opts);
    assert.equal(typeof opts!.lookup, 'function');
  });

  it('the proxy lookup returns the raw hostname (skips local DNS)', () => {
    const opts = buildProxyConnectOptions(E({ AMI_PROXY_RESOLVES_HOSTS: '1' }), undefined)!;
    let captured: unknown;
    opts.lookup!('internal.corp', { family: 4 }, (_err, addr) => { captured = addr; });
    assert.deepEqual(captured, [{ address: 'internal.corp', family: 4 }]);
  });

  it('defaults the lookup family to 4 when unspecified', () => {
    const opts = buildProxyConnectOptions(E({ AMI_PROXY_RESOLVES_HOSTS: '1' }), undefined)!;
    let captured: unknown;
    opts.lookup!('internal.corp', {}, (_err, addr) => { captured = addr; });
    assert.deepEqual(captured, [{ address: 'internal.corp', family: 4 }]);
  });
});

describe('clearProxyAgentCache + proxyAgentCacheSize (dispatcher lifecycle)', () => {
  const savedEnv = { ...process.env };
  afterEach(() => {
    mock.restoreAll();
    delete proxyTransportHooks.ProxyAgent;
    delete proxyTransportHooks.Agent;
    for (const k of Object.keys(process.env)) if (!(k in savedEnv)) delete process.env[k];
    Object.assign(process.env, savedEnv);
    clearProxyAgentCache();
  });

  it('starts empty, grows as dispatchers are built, and clears on reconfigure', async () => {
    clearProxyAgentCache();
    assert.equal(proxyAgentCacheSize(), 0);

    // Inject a fake ProxyAgent ctor via the DI seam so no real proxy connection
    // is attempted (undici's namespace is frozen and cannot be monkeypatched).
    const connectOpts: unknown[] = [];
    class FakeProxyAgent {
      constructor(opts: unknown) { connectOpts.push(opts); }
    }
    proxyTransportHooks.ProxyAgent = FakeProxyAgent as unknown as new (opts: unknown) => unknown;

    const env = E({ HTTPS_PROXY: 'http://proxy.local:8080', AMI_CLIENT_CA: CLIENT_CA });
    let dispatcherSeen: any; // eslint-disable-line @typescript-eslint/no-explicit-any
    const base = (async (_input: unknown, init?: { dispatcher?: unknown }) => {
      dispatcherSeen = init?.dispatcher;
      return new Response('ok');
    }) as unknown as typeof fetch;

    const wrapped = withProxyDispatcher(base, env)!;
    assert.notEqual(wrapped, base); // a wrapper was installed
    await wrapped('https://api.anthropic.com/v1/messages');

    // the dispatcher was built and cached
    assert.ok(dispatcherSeen instanceof FakeProxyAgent);
    assert.equal(proxyAgentCacheSize(), 1);

    // END-TO-END: the merged trust store rode the ProxyAgent construction.
    const built = connectOpts[0] as { uri: string; requestTls?: { ca?: string[] }; connect?: { ca?: string[] } };
    assert.equal(built.uri, 'http://proxy.local:8080');
    assert.ok(Array.isArray(built.requestTls?.ca));
    assert.equal(built.requestTls!.ca!.at(-1), CLIENT_CA);
    assert.ok((built.requestTls!.ca as string[]).length > 1, 'bundled roots preserved alongside the client CA');

    // reconfiguration drops the cached dispatcher
    clearProxyAgentCache();
    assert.equal(proxyAgentCacheSize(), 0);
  });
});

describe('NETWORK_ENV_FLAGS (introspection manifest)', () => {
  it('surfaces the proxy-DNS and TLS-trust knobs', () => {
    const flags = NETWORK_ENV_FLAGS.map(f => f.flag);
    assert.ok(flags.includes('AMI_PROXY_RESOLVES_HOSTS'));
    assert.ok(flags.includes('AMI_CLIENT_CA'));
    assert.ok(flags.includes('NODE_EXTRA_CA_CERTS'));
    // every entry has affects + description
    for (const f of NETWORK_ENV_FLAGS) {
      assert.ok(f.affects.length > 0);
      assert.ok(f.description.length > 0);
    }
  });
});
