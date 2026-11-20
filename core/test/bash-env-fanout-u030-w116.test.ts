/**
 * U030 / w116 — subprocess env augmentation seams for running behind a relay /
 * MITM proxy, plus the persisted-session-environment introspection surface.
 *
 * 1. `applyProxyFanout` fans a single configured proxy var out to its other-case
 *    sibling so a child curl (lower-case) and gh/python (upper-case) share ONE
 *    relay — parity with the reference upstreamproxy `getUpstreamProxyEnv`, mirroring
 *    AMI's existing `applyCaBundleFanout`.
 * 2. `describeSubprocessEnvIsolation` advertises the CA-bundle + proxy fan-out
 *    vars so the seam is discoverable via `ami --introspect`.
 * 3. `describeSessionEnvironment` advertises the session-env ASSEMBLY policy
 *    (what is sourced) — the provenance/affordance surface a user needs to know
 *    why a venv/conda activation did or did not persist across bash calls.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  applyProxyFanout,
  describeSubprocessEnvIsolation,
  bashTool,
} from '../src/tools/bash';
import {
  describeSessionEnvironment,
  _sessionEnvHooks,
} from '../src/session-environment';
import type { ToolContext } from '../src/types';

describe('U030 w116: applyProxyFanout — one proxy var → all tool variants', () => {
  it('fans an UPPER-case proxy out to its lower-case sibling (curl honors lower)', () => {
    // RED on revert: before the fan-out, a child curl (which reads lower-case
    // `https_proxy` first) silently bypasses a relay the operator exported only
    // as `HTTPS_PROXY`.
    const env: NodeJS.ProcessEnv = { HTTPS_PROXY: 'http://relay:8080' };
    applyProxyFanout(env);
    assert.equal(env.https_proxy, 'http://relay:8080');
    assert.equal(env.HTTPS_PROXY, 'http://relay:8080');
  });

  it('fans a LOWER-case proxy out to its upper-case sibling (gh/python honor upper)', () => {
    const env: NodeJS.ProcessEnv = { http_proxy: 'http://relay:3128' };
    applyProxyFanout(env);
    assert.equal(env.HTTP_PROXY, 'http://relay:3128');
    assert.equal(env.http_proxy, 'http://relay:3128');
  });

  it('covers ALL_PROXY and NO_PROXY groups too', () => {
    const env: NodeJS.ProcessEnv = {
      ALL_PROXY: 'socks5://relay:1080',
      no_proxy: 'localhost,127.0.0.1',
    };
    applyProxyFanout(env);
    assert.equal(env.all_proxy, 'socks5://relay:1080');
    assert.equal(env.NO_PROXY, 'localhost,127.0.0.1');
  });

  it('never overwrites an explicitly-set sibling (per-runtime override wins)', () => {
    const env: NodeJS.ProcessEnv = {
      HTTPS_PROXY: 'http://relay:8080',
      https_proxy: 'http://custom:9999',
    };
    applyProxyFanout(env);
    assert.equal(env.https_proxy, 'http://custom:9999'); // explicit wins
    assert.equal(env.HTTPS_PROXY, 'http://relay:8080');
  });

  it('is a no-op when no proxy is configured', () => {
    const env: NodeJS.ProcessEnv = { PATH: '/usr/bin' };
    applyProxyFanout(env);
    assert.equal(env.HTTPS_PROXY, undefined);
    assert.equal(env.https_proxy, undefined);
    assert.equal(env.HTTP_PROXY, undefined);
    assert.equal(env.no_proxy, undefined);
  });

  it('returns the same env object (mutates in place)', () => {
    const env: NodeJS.ProcessEnv = { HTTPS_PROXY: 'http://relay:8080' };
    assert.equal(applyProxyFanout(env), env);
  });
});

describe('U030 w116: describeSubprocessEnvIsolation advertises the fan-out seams', () => {
  it('lists the CA-bundle fan-out target vars', () => {
    const iso = describeSubprocessEnvIsolation();
    assert.deepEqual(iso.caBundleFanoutVars, ['SSL_CERT_FILE', 'REQUESTS_CA_BUNDLE', 'CURL_CA_BUNDLE']);
  });

  it('lists the proxy fan-out vars (both cases of each group)', () => {
    const iso = describeSubprocessEnvIsolation();
    for (const v of ['HTTPS_PROXY', 'https_proxy', 'HTTP_PROXY', 'http_proxy', 'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy']) {
      assert.ok(iso.proxyFanoutVars.includes(v), `proxyFanoutVars must include ${v}`);
    }
  });
});

describe('U030 w116: describeSessionEnvironment — assembly provenance introspection', () => {
  it('reports the honored env-file vars in precedence order', () => {
    const d = describeSessionEnvironment({});
    assert.deepEqual(d.envFileVars, ['AMI_ENV_FILE', 'CLAUDE_ENV_FILE']);
    assert.equal(d.envFileConfigured, null);
  });

  it('resolves which env-file var is currently configured (AMI wins)', () => {
    assert.equal(describeSessionEnvironment({ CLAUDE_ENV_FILE: '/a.sh' }).envFileConfigured, 'CLAUDE_ENV_FILE');
    assert.equal(
      describeSessionEnvironment({ AMI_ENV_FILE: '/a.sh', CLAUDE_ENV_FILE: '/b.sh' }).envFileConfigured,
      'AMI_ENV_FILE',
    );
    // whitespace-only is treated as unset
    assert.equal(describeSessionEnvironment({ AMI_ENV_FILE: '   ' }).envFileConfigured, null);
  });

  it('lists the ingested hook events in load (priority) order', () => {
    const d = describeSessionEnvironment({});
    assert.deepEqual(d.hookEvents, ['setup', 'sessionstart', 'cwdchanged', 'filechanged']);
    assert.equal(d.fragmentNamePattern, '<event>-hook-<index>.sh');
    assert.deepEqual(d.cacheStates, ['unloaded', 'empty', 'loaded']);
  });

  it('reports the session-env base dir and a non-empty note', () => {
    const d = describeSessionEnvironment({ SUPERINFERENCE_DATA_DIR: '/tmp/x' });
    assert.ok(d.sessionEnvBaseDir.endsWith('session-env'));
    assert.ok(typeof d.note === 'string' && d.note.length > 0);
  });

  it('reflects Windows-unsupported via the platform DI seam', () => {
    const saved = _sessionEnvHooks.platform;
    try {
      _sessionEnvHooks.platform = 'win32';
      assert.equal(describeSessionEnvironment({}).supported, false);
      _sessionEnvHooks.platform = 'linux';
      assert.equal(describeSessionEnvironment({}).supported, true);
    } finally {
      _sessionEnvHooks.platform = saved;
    }
  });
});

describe('U030 w116: proxy fan-out — end-to-end through the bash tool', () => {
  const saved: Record<string, string | undefined> = {};
  function setEnv(k: string, v: string | undefined): void {
    if (!(k in saved)) saved[k] = process.env[k];
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
  beforeEach(() => setEnv('AMI_SANDBOX', 'off')); // non-container determinism (AGENTS.md)
  afterEach(() => {
    for (const [k, v] of Object.entries(saved)) setEnv(k, v);
    for (const k of Object.keys(saved)) delete saved[k];
  });
  function ctx(): ToolContext {
    return { cwd: process.cwd(), abortSignal: new AbortController().signal, sessionId: 'proxy-e2e' };
  }

  it('a parent-only UPPER-case HTTPS_PROXY reaches a child curl via lower-case https_proxy', async () => {
    // RED on revert: without applyProxyFanout in scrubEnv, a spawned tool that
    // reads the lower-case variant (curl) sees an empty value and bypasses the relay.
    setEnv('HTTPS_PROXY', 'http://relay.example:8080');
    delete process.env.https_proxy;
    const r = await bashTool.execute({ command: 'echo "lower=$https_proxy"' }, ctx());
    assert.ok(!r.isError, r.output);
    assert.match(r.output, /lower=http:\/\/relay\.example:8080/);
  });

  it('a parent-only lower-case http_proxy reaches a child that reads upper-case HTTP_PROXY', async () => {
    setEnv('http_proxy', 'http://relay.example:3128');
    delete process.env.HTTP_PROXY;
    const r = await bashTool.execute({ command: 'echo "upper=$HTTP_PROXY"' }, ctx());
    assert.ok(!r.isError, r.output);
    assert.match(r.output, /upper=http:\/\/relay\.example:3128/);
  });
});
