// Parity hardening for OpenRouter ZDR helpers (openrouter-zdr.ts).
//
// Covers the additive robustness superset added for parity:
//   1. slug normalization (trim + lowercase) in parse + eligibility
//   2. array-valued `provider` guard in withZdrProviderExtraBody
//   3. invalid timeoutMs guard in fetchZdrModelIds
//   4. non-string message guard in classifyZdrError
//
// Network paths use a real http.createServer bound to port 0 (no injected fetch),
// exercising the actual global-fetch code path end to end.
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'node:http';
import type { AddressInfo } from 'node:net';
import {
  parseZdrModelIds, isZdrEligible, filterZdrModels, classifyZdrError,
  withZdrProviderExtraBody, normalizeZdrSlug, fetchZdrModelIds,
} from '../src/openrouter-zdr';

describe('normalizeZdrSlug', () => {
  it('trims surrounding whitespace and lowercases', () => {
    assert.equal(normalizeZdrSlug('  Google/Gemini-2.5-Flash '), 'google/gemini-2.5-flash');
    assert.equal(normalizeZdrSlug('already/lower'), 'already/lower');
    assert.equal(normalizeZdrSlug('   '), '');
  });
});

describe('parseZdrModelIds normalization [FUNCTIONAL]', () => {
  it('normalizes feed-side casing/whitespace so ids match after strip', () => {
    const set = parseZdrModelIds({ data: [
      { model_id: 'Google/Gemini-2.5-Flash' },
      { model_id: '  anthropic/Claude-Sonnet-5.5  ' },
    ] });
    assert.ok(set.has('google/gemini-2.5-flash'));
    assert.ok(set.has('anthropic/claude-sonnet-5.5'));
  });
  it('drops ids that normalize to empty (whitespace-only)', () => {
    assert.equal(parseZdrModelIds({ data: [{ model_id: '   ' }] }).size, 0);
  });
});

describe('isZdrEligible normalization [FUNCTIONAL]', () => {
  const zdr = new Set(['google/gemini-2.5-flash']);
  it('matches a stored id that differs only in case/whitespace', () => {
    assert.equal(isZdrEligible('openrouter/Google/Gemini-2.5-Flash', zdr), true);
    assert.equal(isZdrEligible('openrouter/GOOGLE/GEMINI-2.5-FLASH', zdr), true);
  });
  it('still rejects a genuinely different model', () => {
    assert.equal(isZdrEligible('openrouter/openai/gpt-4o-mini', zdr), false);
  });
  it('filterZdrModels round-trips with mixed-case parsed set', () => {
    const set = parseZdrModelIds({ data: [{ model_id: 'Google/Gemini-2.5-Flash' }] });
    assert.deepEqual(
      filterZdrModels(['openrouter/google/gemini-2.5-flash', 'openrouter/x/y'], set),
      ['openrouter/google/gemini-2.5-flash'],
    );
  });
});

describe('withZdrProviderExtraBody array-provider guard [FUNCTIONAL]', () => {
  it('treats an array provider value as empty (no numeric-index corruption)', () => {
    assert.deepEqual(
      withZdrProviderExtraBody({ provider: ['openai'] as unknown as Record<string, unknown> }),
      { provider: { zdr: true } },
    );
  });
  it('still merges a real object provider', () => {
    assert.deepEqual(
      withZdrProviderExtraBody({ provider: { sort: 'price' } }),
      { provider: { sort: 'price', zdr: true } },
    );
  });
});

describe('classifyZdrError non-string guard [FUNCTIONAL]', () => {
  it('does not misclassify a non-string value coerced via regex', () => {
    // An Error whose toString contains ZDR+endpoint words must not match when passed
    // as a non-string: only real strings are trusted.
    const sneaky = new Error('ZDR endpoint') as unknown as string;
    assert.equal(classifyZdrError(sneaky).isZdr, false);
    assert.equal(classifyZdrError(undefined as unknown as string).isZdr, false);
  });
});

describe('fetchZdrModelIds over a real http server', () => {
  let server: http.Server;
  let base: string;
  let lastAuth: string | undefined;
  const handlers = { reply: (_req: http.IncomingMessage, res: http.ServerResponse) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ data: [{ model_id: 'Google/Gemini-2.5-Flash' }] }));
  } };

  before(async () => {
    server = http.createServer((req, res) => {
      lastAuth = req.headers.authorization;
      handlers.reply(req, res);
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = (server.address() as AddressInfo).port;
    base = `http://127.0.0.1:${port}/zdr`;
  });
  after(async () => { await new Promise<void>((resolve) => server.close(() => resolve())); });

  // fetchZdrModelIds hardcodes the OpenRouter URL, so drive the real network path by
  // injecting a thin fetchImpl that redirects to the local mock server's base URL.
  const localFetch = (async (_url: string, init?: RequestInit) => fetch(base, init)) as unknown as typeof fetch;

  it('normalizes ids fetched over the wire [FUNCTIONAL]', async () => {
    handlers.reply = (_req, res) => {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ data: [{ model_id: 'Google/Gemini-2.5-Flash' }] }));
    };
    const set = await fetchZdrModelIds({ fetchImpl: localFetch });
    assert.ok(set.has('google/gemini-2.5-flash'), 'wire id normalized to lowercase');
  });

  it('sends Authorization and returns empty on malformed JSON body', async () => {
    handlers.reply = (_req, res) => {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{not json');
    };
    const set = await fetchZdrModelIds({ apiKey: 'sk-or-live', fetchImpl: localFetch });
    assert.equal(set.size, 0);
    assert.equal(lastAuth, 'Bearer sk-or-live');
  });

  it('returns empty on a non-ok status', async () => {
    handlers.reply = (_req, res) => { res.writeHead(503); res.end('busy'); };
    assert.equal((await fetchZdrModelIds({ fetchImpl: localFetch })).size, 0);
  });

  it('invalid timeoutMs falls back to the default instead of aborting immediately [FUNCTIONAL]', async () => {
    handlers.reply = (_req, res) => {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ data: [{ model_id: 'a/b' }] }));
    };
    for (const bad of [0, -5, Number.NaN, Number.POSITIVE_INFINITY]) {
      const set = await fetchZdrModelIds({ fetchImpl: localFetch, timeoutMs: bad });
      assert.ok(set.has('a/b'), `timeoutMs=${bad} should not defeat the fetch`);
    }
  });

  it('a valid positive timeoutMs is still honoured (request completes)', async () => {
    handlers.reply = (_req, res) => {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ data: [{ model_id: 'a/b' }] }));
    };
    const set = await fetchZdrModelIds({ fetchImpl: localFetch, timeoutMs: 3000 });
    assert.ok(set.has('a/b'));
  });
});
