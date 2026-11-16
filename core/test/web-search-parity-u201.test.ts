import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  webSearchTool,
  parseDuckDuckGoResults,
  capResultSize,
  MAX_RESULT_SIZE_CHARS,
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

// Canned HTTP getter so the full success path runs without any network.
function mockHttpGet(
  routes: Record<string, { body: string; statusCode: number }>,
): typeof httpGet {
  return (async (url: string) => {
    const hit = routes[url] ?? routes['*'];
    if (!hit) throw new Error(`no route for ${url}`);
    return { body: hit.body, statusCode: hit.statusCode, headers: {}, url };
  }) as unknown as typeof httpGet;
}

const SEARCH = 'mock://search';
const okValidate = (async () => ({ resolvedIP: '127.0.0.1' })) as typeof validateUrlSafety;
const ddg = `
  <div class="result web-result">
    <a class="result__a" href="https://docs.example.com/guide">Alpha Guide</a>
    <a class="result__snippet">Alpha snippet.</a>
  </div>
  <div class="result web-result">
    <a class="result__a" href="https://docs.example.com/other">Beta Guide</a>
    <a class="result__snippet">Beta snippet.</a>
  </div>`;

function hooks(partial: Partial<WebSearchHooks> = {}): WebSearchHooks {
  return {
    httpGet: mockHttpGet({
      [SEARCH]: { body: ddg, statusCode: 200 },
      '*': { body: '<p>content body</p>', statusCode: 200 },
    }),
    validateUrlSafety: okValidate,
    searchUrl: () => SEARCH,
    ...partial,
  };
}

// ---------------------------------------------------------------------------
// GAP: Graceful (never-throw) decode of untrusted percent-encoded redirect URL
// (references/.../LSPTool/formatters.ts :: graceful never throw decode)
// ---------------------------------------------------------------------------
describe('parseDuckDuckGoResults — malformed uddg redirect', () => {
  it('does not throw on a malformed percent-encoded uddg value', () => {
    const malformed = `
      <div class="result web-result">
        <a class="result__a" href="/l/?uddg=%ZZbad%">Broken</a>
        <a class="result__snippet">Broken snippet.</a>
      </div>`;
    let results: ReturnType<typeof parseDuckDuckGoResults> = [];
    assert.doesNotThrow(() => {
      results = parseDuckDuckGoResults(malformed);
    });
    // The single bad result degrades (raw fallback) instead of failing parse.
    assert.equal(results.length, 1);
    assert.equal(results[0].title, 'Broken');
    assert.equal(results[0].url, '%ZZbad%');
  });

  it('still decodes a well-formed uddg value', () => {
    const good = `
      <div class="result web-result">
        <a class="result__a" href="/l/?uddg=https%3A%2F%2Fok.example.com%2Fp">OK</a>
        <a class="result__snippet">ok.</a>
      </div>`;
    const results = parseDuckDuckGoResults(good);
    assert.equal(results[0].url, 'https://ok.example.com/p');
  });
});

// ---------------------------------------------------------------------------
// GAP: Live progress hints (query_update / search_results_received)
// (references/.../WebSearchTool/UI.tsx + WebSearchTool.ts :: progress)
// ---------------------------------------------------------------------------
describe('webSearchTool.execute — live progress affordances', () => {
  it('emits a "Searching:" hint before the fetch and a "Found N results" hint after parse', async () => {
    const progress: string[] = [];
    const result = await webSearchTool.execute(
      { query: 'alpha docs' },
      ctx({
        _webSearchHooks: hooks(),
        onProgress: (d) => progress.push(d),
      }),
    );
    assert.equal(result.isError, undefined);
    assert.ok(
      progress.some((p) => /Searching:\s*"alpha docs"/.test(p)),
      `expected a Searching hint, got: ${JSON.stringify(progress)}`,
    );
    assert.ok(
      progress.some((p) => /Found 2 results for "alpha docs"/.test(p)),
      `expected a Found-results hint, got: ${JSON.stringify(progress)}`,
    );
    // The Searching hint must precede the Found hint.
    const si = progress.findIndex((p) => p.startsWith('Searching:'));
    const fi = progress.findIndex((p) => p.startsWith('Found'));
    assert.ok(si >= 0 && fi > si);
  });

  it('does not require onProgress to be present (optional channel)', async () => {
    const result = await webSearchTool.execute(
      { query: 'alpha docs' },
      ctx({ _webSearchHooks: hooks() }),
    );
    assert.equal(result.isError, undefined);
  });
});

// ---------------------------------------------------------------------------
// GAP: Descriptive tool-purpose bullets (when to use web search)
// (references/.../WebSearchTool/prompt.ts :: descriptive tool purpose bullets)
// ---------------------------------------------------------------------------
describe('webSearchTool — description guidance', () => {
  it('explains WHEN to use it (current events + beyond knowledge cutoff)', () => {
    const d = webSearchTool.description.toLowerCase();
    assert.match(d, /current events|up-to-date/);
    assert.match(d, /knowledge cutoff/);
  });
});

// ---------------------------------------------------------------------------
// GAP: Mandatory structured "Sources:" section with format spec + example
// (references/.../WebSearchTool/prompt.ts :: mandatory sources section)
// ---------------------------------------------------------------------------
describe('webSearchTool.execute — mandatory Sources directive', () => {
  it('appends a structured Sources directive with [Title](URL) format + example', async () => {
    const result = await webSearchTool.execute(
      { query: 'alpha docs' },
      ctx({ _webSearchHooks: hooks() }),
    );
    assert.match(result.output, /Sources:/);
    assert.match(result.output, /\[Title\]\(URL\)/);
    assert.match(result.output, /MANDATORY/i);
    // includes a concrete example block
    assert.match(result.output, /\[Source Title 1\]\(https:\/\/example\.com\/1\)/);
  });
});

// ---------------------------------------------------------------------------
// GAP: maxResultSizeChars overall output-size cap (100000)
// (references/.../WebSearchTool/WebSearchTool.ts :: maxResultSizeChars knob)
// ---------------------------------------------------------------------------
describe('capResultSize — overall output-size cap', () => {
  it('exposes the 100000-char cap constant mirroring Claude maxResultSizeChars', () => {
    assert.equal(MAX_RESULT_SIZE_CHARS, 100000);
  });

  it('returns the output unchanged when under the cap', () => {
    assert.equal(capResultSize('short', 100), 'short');
  });

  it('truncates and annotates output that exceeds the cap', () => {
    const big = 'x'.repeat(50);
    const capped = capResultSize(big, 20);
    assert.ok(capped.length < big.length + 100);
    assert.match(capped, /truncated/i);
    assert.ok(capped.startsWith('x'.repeat(20)));
  });

  it('applies the cap to a real execute() result', async () => {
    // 10 results each with a ~15k-char snippet => ~150k total, exceeding the cap.
    const bigSnippet = 'z'.repeat(15000);
    const blocks = Array.from({ length: 10 }, (_, i) => `
      <div class="result web-result">
        <a class="result__a" href="https://docs.example.com/p${i}">Res ${i}</a>
        <a class="result__snippet">${bigSnippet}</a>
      </div>`).join('\n');
    const result = await webSearchTool.execute(
      { query: 'alpha docs' },
      ctx({
        _webSearchHooks: hooks({
          httpGet: mockHttpGet({
            [SEARCH]: { body: blocks, statusCode: 200 },
            '*': { body: '<p>c</p>', statusCode: 200 },
          }),
        }),
      }),
    );
    assert.ok(result.output.length <= MAX_RESULT_SIZE_CHARS + 100);
    assert.match(result.output, /truncated/i);
  });
});
