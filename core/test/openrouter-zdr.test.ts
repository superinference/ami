import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseZdrModelIds, isZdrEligible, filterZdrModels, classifyZdrError,
  withZdrProviderExtraBody, openRouterZdrAutoConfig, OPENROUTER_AUTO_MODEL,
  fetchZdrModelIds, OPENROUTER_ZDR_ENDPOINT, OPENROUTER_PRIVACY_URL,
} from '../src/openrouter-zdr';
import type { ProviderConfig } from '../src/types';

describe('parseZdrModelIds', () => {
  it('extracts model_id slugs from the /endpoints/zdr shape', () => {
    const json = { data: [
      { model_id: 'google/gemini-2.5-flash', provider_name: 'Google', safety_retention: false },
      { model_id: 'anthropic/claude-sonnet-5.5', provider_name: 'Anthropic' },
      { model_id: 'google/gemini-2.5-flash' }, // duplicate → deduped by the Set
    ] };
    const set = parseZdrModelIds(json);
    assert.equal(set.size, 2);
    assert.ok(set.has('google/gemini-2.5-flash'));
    assert.ok(set.has('anthropic/claude-sonnet-5.5'));
  });
  it('tolerates malformed shapes (null, no data, non-array, bad entries) → empty set', () => {
    assert.equal(parseZdrModelIds(null).size, 0);
    assert.equal(parseZdrModelIds(undefined).size, 0);
    assert.equal(parseZdrModelIds({}).size, 0);
    assert.equal(parseZdrModelIds({ data: 'nope' }).size, 0);
    assert.equal(parseZdrModelIds({ data: [{}, { model_id: 42 }, { model_id: '' }] }).size, 0);
  });
});

describe('isZdrEligible / filterZdrModels', () => {
  const zdr = new Set(['google/gemini-2.5-flash', 'anthropic/claude-sonnet-5.5']);
  it('matches an AMI-namespaced openrouter id by its stripped slug', () => {
    assert.equal(isZdrEligible('openrouter/google/gemini-2.5-flash', zdr), true);
    assert.equal(isZdrEligible('openrouter/openai/gpt-4o-mini', zdr), false);
  });
  it('treats native single-token routers as not individually ZDR-listed', () => {
    // openrouter/auto strips to itself (no bare slug) → not in the set
    assert.equal(isZdrEligible('openrouter/auto', zdr), false);
  });
  it('an empty/unknown ZDR set does not filter anything out (fail-open)', () => {
    assert.equal(isZdrEligible('openrouter/anything', new Set()), true);
    assert.deepEqual(filterZdrModels(['openrouter/a/b', 'openrouter/c/d'], new Set()), ['openrouter/a/b', 'openrouter/c/d']);
  });
  it('filters a list to ZDR-eligible ids', () => {
    const models = ['openrouter/google/gemini-2.5-flash', 'openrouter/openai/gpt-4o-mini', 'openrouter/anthropic/claude-sonnet-5.5'];
    assert.deepEqual(filterZdrModels(models, zdr), ['openrouter/google/gemini-2.5-flash', 'openrouter/anthropic/claude-sonnet-5.5']);
  });
});

describe('classifyZdrError', () => {
  it('detects the ZDR-violation 404 and returns guidance with both links', () => {
    const msg = 'HTTP 404: 0 endpoints out of 1 requested are available matching your guardrail restrictions and data policy. ZDR violation (account settings): 1 endpoint excluded; configurable at https://openrouter.ai/settings/privacy';
    const info = classifyZdrError(msg);
    assert.equal(info.isZdr, true);
    assert.ok(info.guidance!.includes(OPENROUTER_PRIVACY_URL));
    assert.ok(info.guidance!.includes(OPENROUTER_ZDR_ENDPOINT));
    assert.ok(/account setting/i.test(info.guidance!), 'clarifies it is not a bad model name');
  });
  it('does NOT misclassify a generic 404 / model-not-found', () => {
    assert.equal(classifyZdrError('HTTP 404: model not found').isZdr, false);
    assert.equal(classifyZdrError('No endpoints found for model foo').isZdr, false); // ZDR words absent
    assert.equal(classifyZdrError('').isZdr, false);
  });
});

describe('withZdrProviderExtraBody', () => {
  it('adds provider.zdr to an empty/undefined extraBody', () => {
    assert.deepEqual(withZdrProviderExtraBody(undefined), { provider: { zdr: true } });
    assert.deepEqual(withZdrProviderExtraBody({}), { provider: { zdr: true } });
  });
  it('preserves other extraBody fields and other provider.* routing keys', () => {
    assert.deepEqual(
      withZdrProviderExtraBody({ temperature_override: 1, provider: { sort: 'price' } }),
      { temperature_override: 1, provider: { sort: 'price', zdr: true } },
    );
  });
  it('ignores a non-object provider value (treats it as empty)', () => {
    assert.deepEqual(withZdrProviderExtraBody({ provider: 'nope' }), { provider: { zdr: true } });
  });
});

describe('openRouterZdrAutoConfig', () => {
  it('switches the model to the ZDR-forced auto-router, keeping creds + merging zdr', () => {
    const base: ProviderConfig = {
      baseUrl: 'https://openrouter.ai/api/v1', apiKey: 'sk-or-x',
      model: 'openrouter/google/gemini-2.5-flash', provider: 'openrouter',
      extraBody: { provider: { sort: 'price' } },
    };
    const out = openRouterZdrAutoConfig(base);
    assert.equal(out.model, OPENROUTER_AUTO_MODEL);
    assert.equal(out.model, 'openrouter/auto');
    assert.equal(out.apiKey, 'sk-or-x');
    assert.equal(out.baseUrl, base.baseUrl);
    assert.deepEqual(out.extraBody, { provider: { sort: 'price', zdr: true } });
  });
});

describe('fetchZdrModelIds', () => {
  it('fetches + parses via the injected fetch', async () => {
    const fakeFetch = (async () => ({
      ok: true,
      json: async () => ({ data: [{ model_id: 'google/gemini-2.5-flash' }] }),
    })) as unknown as typeof fetch;
    const set = await fetchZdrModelIds({ fetchImpl: fakeFetch });
    assert.ok(set.has('google/gemini-2.5-flash'));
  });
  it('sends the Authorization header when an apiKey is given', async () => {
    let seenAuth: string | undefined;
    const fakeFetch = (async (_url: string, init: { headers?: Record<string, string> }) => {
      seenAuth = init?.headers?.Authorization;
      return { ok: true, json: async () => ({ data: [] }) };
    }) as unknown as typeof fetch;
    await fetchZdrModelIds({ apiKey: 'sk-or-xyz', fetchImpl: fakeFetch });
    assert.equal(seenAuth, 'Bearer sk-or-xyz');
  });
  it('returns an empty set on a non-ok response', async () => {
    const fakeFetch = (async () => ({ ok: false, json: async () => ({}) })) as unknown as typeof fetch;
    assert.equal((await fetchZdrModelIds({ fetchImpl: fakeFetch })).size, 0);
  });
  it('returns an empty set on a thrown/network error (never throws)', async () => {
    const fakeFetch = (async () => { throw new Error('network down'); }) as unknown as typeof fetch;
    assert.equal((await fetchZdrModelIds({ fetchImpl: fakeFetch })).size, 0);
  });
  it('falls back to the global fetch and default opts when none are provided', async () => {
    const realFetch = globalThis.fetch;
    globalThis.fetch = (async () => ({ ok: true, json: async () => ({ data: [{ model_id: 'google/gemini-2.5-flash' }] }) })) as unknown as typeof fetch;
    try {
      const set = await fetchZdrModelIds(); // no args → default opts {} + global fetch
      assert.ok(set.has('google/gemini-2.5-flash'));
    } finally {
      globalThis.fetch = realFetch;
    }
  });
  it('honours an explicit timeoutMs', async () => {
    let seenSignal = false;
    const fakeFetch = (async (_url: string, init: { signal?: AbortSignal }) => {
      seenSignal = init?.signal instanceof AbortSignal;
      return { ok: true, json: async () => ({ data: [] }) };
    }) as unknown as typeof fetch;
    await fetchZdrModelIds({ fetchImpl: fakeFetch, timeoutMs: 1234 });
    assert.ok(seenSignal, 'an AbortSignal (from the timeout) is passed to fetch');
  });
});
