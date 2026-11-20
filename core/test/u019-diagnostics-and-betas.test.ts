import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  captureApiRequest,
  getLastApiRequest,
  getLastApiRequests,
  clearApiRequestCache,
  MAX_CACHED_API_REQUESTS,
  logApiCall,
} from '../src/logger';
import { describeAnthropicBetas } from '../src/provider';
import {
  ONE_MILLION_CONTEXT_BETA,
  EXTENDED_CACHE_TTL_BETA,
} from '../src/provider';

// U019 G6: bounded ring buffer of the last N API requests (parity: the reference's
// dumpPrompts MAX_CACHED_REQUESTS cache). AMI previously kept only the single
// latest request; this keeps the most recent MAX_CACHED_API_REQUESTS for a
// bug-report / diagnostic surface.
describe('U019 G6: recent-API-request ring buffer', () => {
  beforeEach(() => clearApiRequestCache());
  afterEach(() => clearApiRequestCache());

  it('starts empty', () => {
    assert.equal(getLastApiRequest(), null);
    assert.deepEqual(getLastApiRequests(), []);
  });

  it('captures requests oldest-first and exposes the most recent', () => {
    captureApiRequest({ model: 'a' });
    captureApiRequest({ model: 'b' });
    const all = getLastApiRequests();
    assert.equal(all.length, 2);
    assert.equal(all[0].params.model, 'a');
    assert.equal(all[1].params.model, 'b');
    assert.equal(getLastApiRequest()!.params.model, 'b', 'last() is the most recent');
    assert.ok(typeof all[0].timestamp === 'string' && all[0].timestamp.length > 0);
  });

  it('is bounded: never holds more than MAX_CACHED_API_REQUESTS, dropping the oldest', () => {
    for (let i = 0; i < MAX_CACHED_API_REQUESTS + 3; i++) captureApiRequest({ model: `m${i}` });
    const all = getLastApiRequests();
    assert.equal(all.length, MAX_CACHED_API_REQUESTS);
    // Oldest three (m0,m1,m2) were shifted out; the window ends at the newest.
    assert.equal(all[0].params.model, 'm3');
    assert.equal(all[all.length - 1].params.model, `m${MAX_CACHED_API_REQUESTS + 2}`);
  });

  it('redacts credential-like string values on capture', () => {
    captureApiRequest({ auth: 'Bearer sk-ant-0123456789abcdefghij0123456789' });
    const [entry] = getLastApiRequests();
    assert.ok(!String(entry.params.auth).includes('sk-ant-0123456789abcdefghij'),
      'a credential-looking value must be redacted in the captured request');
  });

  it('returns defensive copies (mutating the result cannot corrupt the ring)', () => {
    captureApiRequest({ model: 'x' });
    const copy = getLastApiRequests();
    copy[0].params.model = 'TAMPERED';
    assert.equal(getLastApiRequests()[0].params.model, 'x');
  });

  it('is fed by logApiCall so the ring populates on every real API call', () => {
    logApiCall('claude-opus-4-8', 7, 3, true);
    const [entry] = getLastApiRequests();
    assert.equal(entry.params.model, 'claude-opus-4-8');
    assert.equal(entry.params.messageCount, 7);
    assert.equal(entry.params.toolCount, 3);
    assert.equal(entry.params.thinkingEnabled, true);
  });
});

// U019 G12: active/available Anthropic betas surfaced as introspectable metadata
// (parity: the reference's getSdkBetas session field).
describe('U019 G12: describeAnthropicBetas', () => {
  const saved = process.env.ANTHROPIC_BETAS;
  afterEach(() => {
    if (saved === undefined) delete process.env.ANTHROPIC_BETAS;
    else process.env.ANTHROPIC_BETAS = saved;
  });

  it('exposes a catalog of beta tokens each with an activation condition', () => {
    delete process.env.ANTHROPIC_BETAS;
    const { catalog, operatorBetas } = describeAnthropicBetas();
    assert.ok(Array.isArray(catalog) && catalog.length >= 2);
    for (const entry of catalog) {
      assert.ok(typeof entry.token === 'string' && entry.token.length > 0);
      assert.ok(typeof entry.condition === 'string' && entry.condition.length > 0);
    }
    const tokens = catalog.map((c) => c.token);
    assert.ok(tokens.includes(ONE_MILLION_CONTEXT_BETA), 'catalog must include the 1M-context beta');
    assert.ok(tokens.includes(EXTENDED_CACHE_TTL_BETA), 'catalog must include the extended-cache-ttl beta');
    assert.deepEqual(operatorBetas, [], 'no operator betas when ANTHROPIC_BETAS is unset');
  });

  it('reflects the ANTHROPIC_BETAS operator escape hatch', () => {
    process.env.ANTHROPIC_BETAS = 'my-custom-beta-2026-01-01, another-beta';
    const { operatorBetas } = describeAnthropicBetas();
    assert.deepEqual(operatorBetas, ['my-custom-beta-2026-01-01', 'another-beta']);
  });
});
