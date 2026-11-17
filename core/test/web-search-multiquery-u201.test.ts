import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  webSearchTool,
  resolveQueryList,
  MAX_QUERIES_PER_CALL,
  type WebSearchHooks,
} from '../src/tools/web-search';
import { httpGet, validateUrlSafety } from '../src/tools/web-utils';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

const okValidate = (async () => ({ resolvedIP: '127.0.0.1' })) as typeof validateUrlSafety;

// A search URL that carries the query so per-query routing is possible.
function searchUrl(query: string): string {
  return `mock://search?q=${encodeURIComponent(query)}`;
}

function ddgFor(title: string): string {
  return `
    <div class="result web-result">
      <a class="result__a" href="https://docs.example.com/${encodeURIComponent(title)}">${title}</a>
      <a class="result__snippet">${title} snippet.</a>
    </div>`;
}

/**
 * Mock httpGet where each search query can be routed to a canned response or a
 * thrown error / HTTP status, so partial-failure across searches is testable.
 */
function mockHooks(
  perQuery: Record<string, { body?: string; statusCode?: number; throws?: string }>,
): WebSearchHooks {
  const get = (async (url: string) => {
    // search requests carry ?q=; content fetches are everything else.
    const m = url.match(/^mock:\/\/search\?q=(.*)$/);
    if (m) {
      const q = decodeURIComponent(m[1]);
      const route = perQuery[q];
      if (!route) throw new Error(`no route for query ${q}`);
      if (route.throws) throw new Error(route.throws);
      return { body: route.body ?? '', statusCode: route.statusCode ?? 200, headers: {}, url };
    }
    return { body: '<p>content body</p>', statusCode: 200, headers: {}, url };
  }) as unknown as typeof httpGet;
  return { httpGet: get, validateUrlSafety: okValidate, searchUrl };
}

// ---------------------------------------------------------------------------
// GAP: resolveQueryList — primary + additional queries, deduped & capped
// ---------------------------------------------------------------------------
describe('resolveQueryList — multi-query assembly', () => {
  it('returns just the primary query when no extras are given', () => {
    assert.deepEqual(resolveQueryList('alpha', undefined), ['alpha']);
    assert.deepEqual(resolveQueryList('alpha', []), ['alpha']);
  });

  it('appends additional string queries in order', () => {
    assert.deepEqual(resolveQueryList('alpha', ['beta', 'gamma']), ['alpha', 'beta', 'gamma']);
  });

  it('de-duplicates against the primary and among extras', () => {
    assert.deepEqual(resolveQueryList('alpha', ['alpha', 'beta', 'beta']), ['alpha', 'beta']);
  });

  it('trims and drops too-short / non-string extras', () => {
    assert.deepEqual(resolveQueryList('alpha', [' beta ', 'a', 1, null, 'gamma']), ['alpha', 'beta', 'gamma']);
  });

  it(`caps the total at ${MAX_QUERIES_PER_CALL}`, () => {
    const extras = Array.from({ length: 20 }, (_, i) => `q${i}`);
    const list = resolveQueryList('primary', extras);
    assert.equal(list.length, MAX_QUERIES_PER_CALL);
    assert.equal(list[0], 'primary');
  });
});

// ---------------------------------------------------------------------------
// GAP: Multi-query agentic search in one call (max_uses: 8) + "Did N searches"
// ---------------------------------------------------------------------------
describe('webSearchTool.execute — multi-query in one call', () => {
  it('runs every query and summarizes "Did N searches" with per-query results', async () => {
    const result = await webSearchTool.execute(
      { query: 'alpha', queries: ['beta'] },
      ctx({
        _webSearchHooks: mockHooks({
          alpha: { body: ddgFor('Alpha') },
          beta: { body: ddgFor('Beta') },
        }),
      }),
    );
    assert.equal(result.isError, undefined);
    assert.match(result.output, /Did 2 searches \(2 queries\)/);
    assert.match(result.output, /Search results for: "alpha"/);
    assert.match(result.output, /Search results for: "beta"/);
    assert.match(result.output, /Alpha/);
    assert.match(result.output, /Beta/);
    // single Sources directive appended once for the whole multi-result output
    assert.equal(result.output.match(/MANDATORY/g)?.length, 1);
  });

  it('emits a "Did N searches" progress affordance', async () => {
    const progress: string[] = [];
    await webSearchTool.execute(
      { query: 'alpha', queries: ['beta'] },
      ctx({
        _webSearchHooks: mockHooks({ alpha: { body: ddgFor('Alpha') }, beta: { body: ddgFor('Beta') } }),
        onProgress: (d) => progress.push(d),
      }),
    );
    assert.ok(progress.some((p) => /Searching: "alpha"/.test(p)));
    assert.ok(progress.some((p) => /Searching: "beta"/.test(p)));
    assert.ok(progress.some((p) => /Did 2 searches of 2/.test(p)), JSON.stringify(progress));
  });
});

// ---------------------------------------------------------------------------
// GAP: Per-search partial-failure tolerance (one failed search does not fail all)
// ---------------------------------------------------------------------------
describe('webSearchTool.execute — partial-failure tolerance', () => {
  it('a single thrown search degrades to an error entry; others still return', async () => {
    const result = await webSearchTool.execute(
      { query: 'good', queries: ['bad'] },
      ctx({
        _webSearchHooks: mockHooks({
          good: { body: ddgFor('Good') },
          bad: { throws: 'ECONNRESET' },
        }),
      }),
    );
    assert.equal(result.isError, undefined); // partial success is NOT a whole-call error
    assert.match(result.output, /Did 1 search \(2 queries\)/);
    assert.match(result.output, /Search results for: "good"/);
    assert.match(result.output, /Web search error for "bad": ECONNRESET/);
  });

  it('an HTTP>=400 for one query degrades that query only', async () => {
    const result = await webSearchTool.execute(
      { query: 'good', queries: ['boom'] },
      ctx({
        _webSearchHooks: mockHooks({
          good: { body: ddgFor('Good') },
          boom: { statusCode: 503 },
        }),
      }),
    );
    assert.equal(result.isError, undefined);
    assert.match(result.output, /Web search error for "boom": DuckDuckGo returned HTTP 503/);
    assert.match(result.output, /Search results for: "good"/);
  });

  it('a query with zero results is reported, not an error', async () => {
    const result = await webSearchTool.execute(
      { query: 'good', queries: ['empty'] },
      ctx({
        _webSearchHooks: mockHooks({
          good: { body: ddgFor('Good') },
          empty: { body: '<html><body>nothing</body></html>' },
        }),
      }),
    );
    assert.equal(result.isError, undefined);
    assert.match(result.output, /No results found for "empty"/);
    assert.match(result.output, /Did 2 searches/); // an empty-result search still "succeeded"
  });

  it('fails the whole call ONLY when every search fails', async () => {
    const result = await webSearchTool.execute(
      { query: 'a1', queries: ['a2'] },
      ctx({
        _webSearchHooks: mockHooks({
          a1: { throws: 'down' },
          a2: { statusCode: 500 },
        }),
      }),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /Did 0 searches/);
    assert.match(result.output, /Web search error for "a1"/);
    assert.match(result.output, /Web search error for "a2"/);
  });
});

// ---------------------------------------------------------------------------
// GAP: multi-query charges the budget per search
// ---------------------------------------------------------------------------
describe('webSearchTool.execute — multi-query budget accounting', () => {
  it('charges one budget unit per query in the call', async () => {
    const budget = { count: 0, max: 100 };
    await webSearchTool.execute(
      { query: 'aa', queries: ['bb', 'cc'] },
      ctx({
        _webSearchBudget: budget,
        _webSearchHooks: mockHooks({
          aa: { body: ddgFor('A') }, bb: { body: ddgFor('B') }, cc: { body: ddgFor('C') },
        }),
      }),
    );
    assert.equal(budget.count, 3);
  });

  it('blocks the entire call (no partial run) when the charge exceeds the cap', async () => {
    const budget = { count: 0, max: 2 }; // 3 queries would push to 3 > 2
    const result = await webSearchTool.execute(
      { query: 'aa', queries: ['bb', 'cc'] },
      ctx({
        _webSearchBudget: budget,
        _webSearchHooks: mockHooks({ aa: { body: ddgFor('A') }, bb: { body: ddgFor('B') }, cc: { body: ddgFor('C') } }),
      }),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /web search limit reached/);
  });
});

// ---------------------------------------------------------------------------
// Regression: single-query behavior unchanged when no extra queries given
// ---------------------------------------------------------------------------
describe('webSearchTool.execute — single-query path preserved', () => {
  it('does not emit the multi-query summary for a lone query', async () => {
    const result = await webSearchTool.execute(
      { query: 'alpha' },
      ctx({ _webSearchHooks: mockHooks({ alpha: { body: ddgFor('Alpha') } }) }),
    );
    assert.equal(result.isError, undefined);
    assert.doesNotMatch(result.output, /Did \d+ search/);
    assert.match(result.output, /Search results for: "alpha"/);
  });
});
