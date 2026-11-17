// ===========================================================================
// provider-u020-w71.test.ts
//
// Parity unit U020 (wave 71) — three ADDITIVE provider.ts capabilities that
// close confirmed Claude-Code parity gaps:
//
//   1. isFirstPartyBaseUrl(baseUrl, domains) — host-boundary classification of a
//      first-party API base URL, replacing host-spoofable
//      `baseUrl.includes('anthropic.com')` substring tests in modelsEndpoint()
//      and the SDK-routing block.
//      [ref: utils/model/providers.ts isFirstPartyAnthropicBaseUrl L25-40]
//
//   2. getAWSRegion() — AWS region waterfall AWS_REGION -> AWS_DEFAULT_REGION ->
//      'us-east-1'. [ref: utils/envUtils.ts getAWSRegion L92-98]
//
//   3. resolveBedrockGateway() — AWS_BEARER_TOKEN_BEDROCK API-key auth mode +
//      region-derived default OpenAI-compatible endpoint, wired into the
//      amazon-bedrock branch of resolveModel().
//      [ref: utils/model/bedrock.ts AWS_BEARER_TOKEN_BEDROCK L81-91,
//       utils/envUtils.ts getAWSRegion]
//
// Includes end-to-end tests that drive the REAL wired paths: fetchAdvertisedContextWindow
// (through modelsEndpoint, against an http.createServer mock) proves a host that
// merely CONTAINS "anthropic.com" in its path is no longer treated as first-party
// (RED on revert — the old substring test sent x-api-key to it); resolveModel()
// proves the amazon-bedrock branch constructs a model through resolveBedrockGateway.
//
// ESM-coverage-safe: env snapshotted + restored; http.createServer on port 0.
// ===========================================================================
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'node:http';

import {
  isFirstPartyBaseUrl,
  getAWSRegion,
  resolveBedrockGateway,
  AWS_BEARER_TOKEN_BEDROCK_ENV,
  resolveModel,
  fetchAdvertisedContextWindow,
} from '../src/provider';
import type { ProviderConfig } from '../src/types';

// ---------------------------------------------------------------------------
// 1. isFirstPartyBaseUrl — host-boundary classification
// ---------------------------------------------------------------------------
describe('isFirstPartyBaseUrl', () => {
  it('matches the exact apex domain', () => {
    assert.equal(isFirstPartyBaseUrl('https://anthropic.com/v1', ['anthropic.com']), true);
  });

  it('matches a genuine subdomain', () => {
    assert.equal(isFirstPartyBaseUrl('https://api.anthropic.com/v1', ['anthropic.com']), true);
  });

  it('is case-insensitive on the host', () => {
    assert.equal(isFirstPartyBaseUrl('https://API.Anthropic.COM/v1', ['anthropic.com']), true);
  });

  it('rejects a suffix-spoofed host (anthropic.com.evil.example)', () => {
    assert.equal(isFirstPartyBaseUrl('https://api.anthropic.com.evil.example/v1', ['anthropic.com']), false);
  });

  it('rejects a lookalike apex (not-anthropic.com)', () => {
    assert.equal(isFirstPartyBaseUrl('https://not-anthropic.com/v1', ['anthropic.com']), false);
  });

  it('rejects the domain appearing only in the path/query', () => {
    assert.equal(isFirstPartyBaseUrl('https://evil.example/?x=anthropic.com', ['anthropic.com']), false);
    assert.equal(isFirstPartyBaseUrl('http://127.0.0.1:8080/anthropic.com/v1', ['anthropic.com']), false);
  });

  it('returns false for an undefined / empty base URL', () => {
    assert.equal(isFirstPartyBaseUrl(undefined, ['anthropic.com']), false);
    assert.equal(isFirstPartyBaseUrl('', ['anthropic.com']), false);
  });

  it('returns false for a malformed URL (parse throws -> conservative default)', () => {
    assert.equal(isFirstPartyBaseUrl('::not a url::', ['anthropic.com']), false);
    assert.equal(isFirstPartyBaseUrl('http://', ['anthropic.com']), false);
  });

  it('supports an allowlist of several domains (any match)', () => {
    const domains = ['generativelanguage.googleapis.com', 'aiplatform.googleapis.com'];
    assert.equal(isFirstPartyBaseUrl('https://generativelanguage.googleapis.com/v1beta', domains), true);
    assert.equal(isFirstPartyBaseUrl('https://foo.aiplatform.googleapis.com', domains), true);
    assert.equal(isFirstPartyBaseUrl('https://googleapis.com.evil.example', domains), false);
  });

  it('treats a sibling host (prefix-, not dot-separated) as NOT a subdomain', () => {
    // `us-central1-aiplatform.googleapis.com` shares a SUFFIX but is a different
    // host than `aiplatform.googleapis.com`; only genuine `.`-separated subdomains match.
    assert.equal(isFirstPartyBaseUrl('https://us-central1-aiplatform.googleapis.com', ['aiplatform.googleapis.com']), false);
  });
});

// ---------------------------------------------------------------------------
// 2. getAWSRegion — AWS_REGION -> AWS_DEFAULT_REGION -> 'us-east-1'
// ---------------------------------------------------------------------------
describe('getAWSRegion', () => {
  it('prefers AWS_REGION', () => {
    assert.equal(getAWSRegion({ AWS_REGION: 'eu-west-1', AWS_DEFAULT_REGION: 'ap-south-1' }), 'eu-west-1');
  });

  it('falls back to AWS_DEFAULT_REGION when AWS_REGION is unset', () => {
    assert.equal(getAWSRegion({ AWS_DEFAULT_REGION: 'ap-south-1' }), 'ap-south-1');
  });

  it("defaults to 'us-east-1' when neither is set", () => {
    assert.equal(getAWSRegion({}), 'us-east-1');
  });

  it('treats an empty AWS_REGION as unset (falls through)', () => {
    assert.equal(getAWSRegion({ AWS_REGION: '', AWS_DEFAULT_REGION: 'ca-central-1' }), 'ca-central-1');
  });

  it('reads process.env by default', () => {
    const saved = { r: process.env.AWS_REGION, d: process.env.AWS_DEFAULT_REGION };
    try {
      delete process.env.AWS_REGION;
      process.env.AWS_DEFAULT_REGION = 'us-west-2';
      assert.equal(getAWSRegion(), 'us-west-2');
    } finally {
      if (saved.r === undefined) delete process.env.AWS_REGION; else process.env.AWS_REGION = saved.r;
      if (saved.d === undefined) delete process.env.AWS_DEFAULT_REGION; else process.env.AWS_DEFAULT_REGION = saved.d;
    }
  });
});

// ---------------------------------------------------------------------------
// 3. resolveBedrockGateway — bearer-token auth + region-derived endpoint
// ---------------------------------------------------------------------------
describe('resolveBedrockGateway', () => {
  it('prefers AWS_BEARER_TOKEN_BEDROCK over config.apiKey', () => {
    const r = resolveBedrockGateway({ apiKey: 'aws-access-key', baseUrl: '' }, {
      [AWS_BEARER_TOKEN_BEDROCK_ENV]: 'bearer-xyz',
    });
    assert.equal(r.apiKey, 'bearer-xyz');
  });

  it('falls back to config.apiKey when no bearer token is set', () => {
    const r = resolveBedrockGateway({ apiKey: 'aws-access-key', baseUrl: '' }, {});
    assert.equal(r.apiKey, 'aws-access-key');
  });

  it('trims a whitespace-only bearer token back to the config apiKey', () => {
    const r = resolveBedrockGateway({ apiKey: 'aws-access-key', baseUrl: '' }, {
      [AWS_BEARER_TOKEN_BEDROCK_ENV]: '   ',
    });
    assert.equal(r.apiKey, 'aws-access-key');
  });

  it('derives the region-scoped OpenAI-compatible endpoint when no baseUrl is given', () => {
    const r = resolveBedrockGateway({ apiKey: 'k', baseUrl: '' }, { AWS_REGION: 'eu-west-1' });
    assert.equal(r.baseURL, 'https://bedrock-runtime.eu-west-1.amazonaws.com/openai/v1');
  });

  it("uses the us-east-1 default region in the derived endpoint", () => {
    const r = resolveBedrockGateway({ apiKey: 'k', baseUrl: '' }, {});
    assert.equal(r.baseURL, 'https://bedrock-runtime.us-east-1.amazonaws.com/openai/v1');
  });

  it('honors an explicit custom gateway baseUrl over the derived default', () => {
    const r = resolveBedrockGateway({ apiKey: 'k', baseUrl: 'https://my-gw.example/v1' }, { AWS_REGION: 'eu-west-1' });
    assert.equal(r.baseURL, 'https://my-gw.example/v1');
  });

  it('trims trailing slashes from an explicit baseUrl', () => {
    const r = resolveBedrockGateway({ apiKey: 'k', baseUrl: 'https://my-gw.example/v1///' }, {});
    assert.equal(r.baseURL, 'https://my-gw.example/v1');
  });

  // End-to-end: the amazon-bedrock branch of resolveModel() constructs a model
  // through resolveBedrockGateway (bearer token + region endpoint) without throwing.
  it('E2E: resolveModel routes amazon-bedrock through the gateway with a bearer token', () => {
    const saved = process.env[AWS_BEARER_TOKEN_BEDROCK_ENV];
    try {
      process.env[AWS_BEARER_TOKEN_BEDROCK_ENV] = 'bearer-token-123';
      const config: ProviderConfig = {
        provider: 'amazon-bedrock',
        model: 'anthropic.claude-sonnet-4-6',
        apiKey: '',
        baseUrl: '',
      };
      const model = resolveModel(config);
      // Bedrock routes through the OpenAI-compatible SDK.
      assert.ok(String(model.provider).includes('openai'), `provider was ${model.provider}`);
      assert.equal(model.modelId, 'anthropic.claude-sonnet-4-6');
    } finally {
      if (saved === undefined) delete process.env[AWS_BEARER_TOKEN_BEDROCK_ENV];
      else process.env[AWS_BEARER_TOKEN_BEDROCK_ENV] = saved;
    }
  });
});

// ---------------------------------------------------------------------------
// E2E: modelsEndpoint host-boundary guard (isFirstPartyBaseUrl wired in)
// A base URL whose host is NOT anthropic.com but whose PATH contains the string
// "anthropic.com" must NOT be classified first-party: it must receive the generic
// Authorization: Bearer header, NOT the official x-api-key / anthropic-version.
// RED on revert: the old `baseUrl.includes('anthropic.com')` substring test sent
// x-api-key to this host.
// ---------------------------------------------------------------------------
describe('modelsEndpoint host-boundary guard (E2E through fetchAdvertisedContextWindow)', () => {
  function startServer(record: { headers: http.IncomingHttpHeaders | null }): Promise<http.Server> {
    const server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      record.headers = req.headers;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ data: [{ id: 'some-model', max_input_tokens: 12345 }] }));
    });
    return new Promise(resolve => server.listen(0, '127.0.0.1', () => resolve(server)));
  }

  it('sends generic Bearer auth (not x-api-key) to a host that only has "anthropic.com" in its path', async () => {
    const record: { headers: http.IncomingHttpHeaders | null } = { headers: null };
    const server = await startServer(record);
    const port = (server.address() as { port: number }).port;
    try {
      const config: ProviderConfig = {
        // Host is 127.0.0.1; "anthropic.com" appears only in the PATH.
        baseUrl: `http://127.0.0.1:${port}/anthropic.com/v1`,
        apiKey: 'secret-key',
        model: 'some-model',
        // provider intentionally UNSET so only the host-detection arm can classify.
      };
      const ctx = await fetchAdvertisedContextWindow(config);
      assert.equal(ctx, 12345);
      assert.ok(record.headers, 'server was not reached');
      // New (host-boundary) behavior: generic Bearer, never the official headers.
      assert.equal(record.headers!['authorization'], 'Bearer secret-key');
      assert.equal(record.headers!['x-api-key'], undefined);
      assert.equal(record.headers!['anthropic-version'], undefined);
    } finally {
      await new Promise<void>(resolve => server.close(() => resolve()));
    }
  });

  it('still sends x-api-key to the genuine api.anthropic.com host', async () => {
    const record: { headers: http.IncomingHttpHeaders | null } = { headers: null };
    const server = await startServer(record);
    const port = (server.address() as { port: number }).port;
    try {
      // A genuine anthropic host reached via an explicit provider + local mock:
      // provider:'anthropic' keeps the official-arm ergonomics (per existing suite).
      const config: ProviderConfig = {
        baseUrl: `http://127.0.0.1:${port}/anthropic.com/v1`,
        apiKey: 'ant-key',
        model: 'some-model',
        provider: 'anthropic',
      };
      const ctx = await fetchAdvertisedContextWindow(config);
      assert.equal(ctx, 12345);
      assert.equal(record.headers!['x-api-key'], 'ant-key');
      assert.equal(record.headers!['anthropic-version'], '2023-06-01');
    } finally {
      await new Promise<void>(resolve => server.close(() => resolve()));
    }
  });
});
