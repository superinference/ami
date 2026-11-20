import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  resolveProxyForUrl,
  getVertexRegionForModel,
  envApiTimeoutMs,
  formatTimeoutError,
} from '../src/provider';

// ---------------------------------------------------------------------------
// NO_PROXY parity: port-specific entries + CIDR / IP-range entries.
// Mirrors the reference's shouldBypassProxy (utils/proxy.ts L83-124) and the
// CIDR forms carried by NO_PROXY (upstreamproxy.ts L34-63). Exercised through
// the real consumer resolveProxyForUrl (no network).
// ---------------------------------------------------------------------------
describe('resolveProxyForUrl — port-specific NO_PROXY', () => {
  it('bypasses the proxy ONLY for the matching host:port', () => {
    const env = { HTTPS_PROXY: 'http://p:1', NO_PROXY: 'example.com:8080' } as NodeJS.ProcessEnv;
    // Same host, matching explicit port -> bypass (undefined).
    assert.equal(resolveProxyForUrl(env, 'https://example.com:8080/v1'), undefined);
    // Same host, different port -> proxied.
    assert.equal(resolveProxyForUrl(env, 'https://example.com:9090/v1'), 'http://p:1');
    // Same host, default https port (443) -> proxied (entry demanded 8080).
    assert.equal(resolveProxyForUrl(env, 'https://example.com/v1'), 'http://p:1');
  });

  it('matches the protocol-default port against a host:port entry', () => {
    const env = { HTTPS_PROXY: 'http://p:1', NO_PROXY: 'example.com:443' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://example.com/v1'), undefined);
    const httpEnv = { HTTP_PROXY: 'http://p:1', NO_PROXY: 'example.com:80' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(httpEnv, 'http://example.com/v1'), undefined);
  });
});

describe('resolveProxyForUrl — CIDR / IP-range NO_PROXY', () => {
  it('bypasses the proxy for an IPv4 host inside a CIDR range', () => {
    const env = { HTTPS_PROXY: 'http://p:1', NO_PROXY: '10.0.0.0/8' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://10.1.2.3/v1'), undefined);
    // Outside the range -> proxied.
    assert.equal(resolveProxyForUrl(env, 'https://11.0.0.1/v1'), 'http://p:1');
  });

  it('honors the prefix length precisely (link-local /16)', () => {
    const env = { HTTPS_PROXY: 'http://p:1', NO_PROXY: '169.254.0.0/16' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://169.254.169.254/latest'), undefined);
    assert.equal(resolveProxyForUrl(env, 'https://169.255.0.1/x'), 'http://p:1');
  });

  it('matches an IPv6 host inside a CIDR range', () => {
    const env = { HTTPS_PROXY: 'http://p:1', NO_PROXY: 'fd00::/8' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://[fd00::1]/v1'), undefined);
    assert.equal(resolveProxyForUrl(env, 'https://[fe00::1]/v1'), 'http://p:1');
  });

  it('does not match a domain host against a CIDR entry, and ignores malformed CIDRs', () => {
    const env = { HTTPS_PROXY: 'http://p:1', NO_PROXY: '10.0.0.0/8' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://api.anthropic.com/v1'), 'http://p:1');
    const bad = { HTTPS_PROXY: 'http://p:1', NO_PROXY: 'nota/cidr, 10.0.0.0/99' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(bad, 'https://10.1.2.3/v1'), 'http://p:1');
  });

  it('treats malformed IPv4/IPv6 CIDR bases as non-matching', () => {
    // Octet > 255 in the CIDR base.
    const big = { HTTPS_PROXY: 'http://p:1', NO_PROXY: '999.0.0.0/8' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(big, 'https://10.1.2.3/v1'), 'http://p:1');
    // Non-digit octet in a length-4 base.
    const nondigit = { HTTPS_PROXY: 'http://p:1', NO_PROXY: '10.0.x.0/8' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(nondigit, 'https://10.1.2.3/v1'), 'http://p:1');
    // Over-compressed IPv6 (two '::') and non-hex group — v6 path, no match.
    const badv6 = { HTTPS_PROXY: 'http://p:1', NO_PROXY: 'fd00::1::2/8' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(badv6, 'https://[fd00::1]/v1'), 'http://p:1');
    const nonhex = { HTTPS_PROXY: 'http://p:1', NO_PROXY: 'xyzz::/8' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(nonhex, 'https://[fd00::1]/v1'), 'http://p:1');
    // Prefix out of range for the family.
    const bigPrefix = { HTTPS_PROXY: 'http://p:1', NO_PROXY: 'fd00::/200' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(bigPrefix, 'https://[fd00::1]/v1'), 'http://p:1');
  });

  it('matches a /0 CIDR (whole IPv4 space) and an exact IPv6 literal entry', () => {
    const all = { HTTPS_PROXY: 'http://p:1', NO_PROXY: '0.0.0.0/0' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(all, 'https://8.8.8.8/v1'), undefined);
    const loop = { HTTPS_PROXY: 'http://p:1', NO_PROXY: '::1' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(loop, 'https://[::1]:8443/v1'), undefined);
  });

  it('still honors plain host, suffix, and wildcard entries', () => {
    const exact = { HTTPS_PROXY: 'http://p', NO_PROXY: 'api.anthropic.com' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(exact, 'https://api.anthropic.com/v1'), undefined);
    const wild = { HTTPS_PROXY: 'http://p', NO_PROXY: '*' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(wild, 'https://anything.example/v1'), undefined);
  });
});

// ---------------------------------------------------------------------------
// Per-model Vertex region override (Claude envUtils getVertexRegionForModel).
// ---------------------------------------------------------------------------
describe('getVertexRegionForModel', () => {
  const touched = [
    'VERTEX_REGION_CLAUDE_HAIKU_4_5',
    'VERTEX_REGION_CLAUDE_4_5_SONNET',
    'VERTEX_REGION_CLAUDE_4_1_OPUS',
    'CLOUD_ML_REGION',
    'GOOGLE_VERTEX_LOCATION',
  ];
  afterEach(() => { for (const k of touched) delete process.env[k]; });

  it('returns the model-specific override env var when set', () => {
    process.env.VERTEX_REGION_CLAUDE_HAIKU_4_5 = 'europe-west1';
    assert.equal(getVertexRegionForModel('claude-haiku-4-5'), 'europe-west1');
  });

  it('prefers the more-specific prefix (opus-4-1 over opus-4)', () => {
    process.env.VERTEX_REGION_CLAUDE_4_1_OPUS = 'us-west4';
    assert.equal(getVertexRegionForModel('claude-opus-4-1-20250805'), 'us-west4');
  });

  it('falls back to the default region waterfall when no override is set', () => {
    process.env.CLOUD_ML_REGION = 'asia-southeast1';
    assert.equal(getVertexRegionForModel('claude-sonnet-4-5'), 'asia-southeast1');
  });

  it('falls back to GOOGLE_VERTEX_LOCATION then us-east5', () => {
    process.env.GOOGLE_VERTEX_LOCATION = 'us-central1';
    assert.equal(getVertexRegionForModel('gemini-2.0'), 'us-central1');
    delete process.env.GOOGLE_VERTEX_LOCATION;
    assert.equal(getVertexRegionForModel(undefined), 'us-east5');
  });

  it('uses the default waterfall when the matched override env is empty', () => {
    process.env.VERTEX_REGION_CLAUDE_4_5_SONNET = '';
    process.env.CLOUD_ML_REGION = 'northamerica-northeast1';
    assert.equal(getVertexRegionForModel('claude-sonnet-4-5'), 'northamerica-northeast1');
  });
});

// ---------------------------------------------------------------------------
// API_TIMEOUT_MS env-var name parity (the reference client.ts L144).
// ---------------------------------------------------------------------------
describe('envApiTimeoutMs', () => {
  it('reads SUPERINFERENCE_API_TIMEOUT_MS', () => {
    assert.equal(envApiTimeoutMs({ SUPERINFERENCE_API_TIMEOUT_MS: '1234' } as NodeJS.ProcessEnv), 1234);
  });

  it('falls back to API_TIMEOUT_MS for the reference-name parity', () => {
    assert.equal(envApiTimeoutMs({ API_TIMEOUT_MS: '5678' } as NodeJS.ProcessEnv), 5678);
  });

  it('prefers SUPERINFERENCE_API_TIMEOUT_MS when both are set', () => {
    assert.equal(
      envApiTimeoutMs({ SUPERINFERENCE_API_TIMEOUT_MS: '100', API_TIMEOUT_MS: '200' } as NodeJS.ProcessEnv),
      100,
    );
  });

  it('returns NaN-safe 0-or-less sentinel when unset or non-numeric', () => {
    assert.ok(!(envApiTimeoutMs({} as NodeJS.ProcessEnv) > 0));
    assert.ok(!(envApiTimeoutMs({ API_TIMEOUT_MS: 'abc' } as NodeJS.ProcessEnv) > 0));
  });
});

// ---------------------------------------------------------------------------
// Actionable timeout error hint (Claude AssistantTextMessage.tsx L156-166).
// ---------------------------------------------------------------------------
describe('formatTimeoutError', () => {
  it('names the configurable env var and the current limit', () => {
    const msg = formatTimeoutError(600000, false);
    assert.match(msg, /Request timed out after 600000ms/);
    assert.match(msg, /SUPERINFERENCE_API_TIMEOUT_MS=600000ms/);
    assert.match(msg, /try increasing it/);
  });

  it('marks partial-content timeouts while keeping the hint', () => {
    const msg = formatTimeoutError(42, true);
    assert.match(msg, /partial content received/);
    assert.match(msg, /SUPERINFERENCE_API_TIMEOUT_MS=42ms/);
  });
});
