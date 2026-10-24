import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { webSearchTool, parseDuckDuckGoResults } from '../src/tools/web-search';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// ESM-safe strategy note
// ---------------------------------------------------------------------------
// web-search issues its HTTP request to a hard-coded DuckDuckGo URL via the
// `httpGet` named import from web-utils. There is no injection seam for the
// search response, and ESM rules forbid monkeypatching the imported binding.
// So the deterministic, network-free branches we CAN drive end-to-end are:
//   * the exported pure parser `parseDuckDuckGoResults` (all skip/decode arms)
//   * the execute() error funnel — reached by handing execute an AbortSignal
//     whose `aborted` getter throws. That throw surfaces inside httpGet's
//     promise executor (before any socket is opened), so execute's try/catch
//     runs with a value WE choose. This exercises both arms of the
//     `err instanceof Error ? err.message : String(err)` ternary and the
//     `message === 'Aborted'` discriminator with zero network dependency.
// None of this reads source text, patches a namespace binding, or couples to
// __dirname — so it runs identically under tsx and the native-ESM harness.

/**
 * Build an AbortSignal-shaped object whose `.aborted` access throws `val`.
 * execute() -> httpGet() -> `if (signal.aborted)` throws synchronously inside
 * the promise executor, rejecting with `val` before any request is made.
 */
function throwingSignal(val: unknown): AbortSignal {
  return {
    get aborted(): boolean {
      throw val;
    },
    addEventListener(): void {},
    removeEventListener(): void {},
  } as unknown as AbortSignal;
}

function ctx(signal: AbortSignal): ToolContext {
  return { cwd: process.cwd(), abortSignal: signal };
}

// ---------------------------------------------------------------------------
// execute() — error funnel (lines 219-229): both ternary arms + Aborted split
// ---------------------------------------------------------------------------

describe('webSearchTool.execute — error funnel', () => {
  it('maps an Error thrown during fetch to the generic web_fetch hint (instanceof Error arm)', async () => {
    const res = await webSearchTool.execute(
      { query: 'quantum computing' },
      ctx(throwingSignal(new Error('boom-net-failure'))),
    );
    assert.equal(res.isError, true);
    // message = err.message, embedded verbatim in the output
    assert.ok(
      res.output.includes('boom-net-failure'),
      `Error message must be surfaced, got: ${res.output}`,
    );
    assert.ok(
      res.output.includes('web_fetch'),
      'generic failure must steer the model toward web_fetch',
    );
    assert.ok(
      res.output.includes('quantum computing'),
      'the failing query must be echoed back',
    );
  });

  it('stringifies a non-Error rejection value (String(err) arm)', async () => {
    const res = await webSearchTool.execute(
      { query: 'rust lifetimes' },
      ctx(throwingSignal('raw-string-rejection')),
    );
    assert.equal(res.isError, true);
    // Hits the FALSE arm of `err instanceof Error` -> String(err)
    assert.ok(
      res.output.includes('raw-string-rejection'),
      `String(err) must be embedded, got: ${res.output}`,
    );
    assert.ok(res.output.includes('web_fetch'));
  });

  it('returns the dedicated abort message when the error is exactly "Aborted"', async () => {
    const res = await webSearchTool.execute(
      { query: 'anything' },
      ctx(throwingSignal(new Error('Aborted'))),
    );
    assert.equal(res.isError, true);
    assert.equal(res.output, 'Search aborted.');
    // Must NOT fall through to the generic hint — proves the discriminator works.
    assert.ok(!res.output.includes('web_fetch'), 'abort path must not emit the generic hint');
  });

  it('a near-miss message ("Aborted!") still takes the generic path, not the abort path', async () => {
    // Guards the exact-equality check: only "Aborted" is special.
    const res = await webSearchTool.execute(
      { query: 'edge' },
      ctx(throwingSignal(new Error('Aborted!'))),
    );
    assert.equal(res.isError, true);
    assert.notEqual(res.output, 'Search aborted.');
    assert.ok(res.output.includes('Aborted!'));
    assert.ok(res.output.includes('web_fetch'));
  });
});

// ---------------------------------------------------------------------------
// parseDuckDuckGoResults — skip arms of `if (title && href)` and uddg decode
// ---------------------------------------------------------------------------

describe('parseDuckDuckGoResults — push guard (title && href)', () => {
  it('skips a result whose title is empty (falsy title arm)', () => {
    // linkMatch succeeds (href present) but the anchor has no text -> title ''.
    const html =
      '<div class="result x"><a class="result__a" href="https://z.com"></a>' +
      '<a class="result__snippet">snip</a></div>';
    assert.deepEqual(parseDuckDuckGoResults(html), []);
  });

  it('skips a result whose href is empty (falsy href arm)', () => {
    // href is "" -> no uddg -> href stays "" -> guard rejects even with a title.
    const html =
      '<div class="result x"><a class="result__a" href="">Has Title</a>' +
      '<a class="result__snippet">snip</a></div>';
    assert.deepEqual(parseDuckDuckGoResults(html), []);
  });

  it('keeps a valid result adjacent to a skipped (title-less) one', () => {
    const html =
      '<div class="result a"><a class="result__a" href="https://skip.com"></a></div>' +
      '<div class="result b"><a class="result__a" href="https://keep.com">Keep Me</a>' +
      '<span class="result__snippet">kept snippet</span></div>';
    const out = parseDuckDuckGoResults(html);
    assert.equal(out.length, 1);
    assert.equal(out[0].url, 'https://keep.com');
    assert.equal(out[0].title, 'Keep Me');
    assert.equal(out[0].snippet, 'kept snippet');
  });

  it('decodes only the uddg redirect payload, ignoring trailing tracking params', () => {
    const encoded = encodeURIComponent('https://real.example/article?id=7');
    const html =
      `<div class="result x"><a class="result__a" href="/l/?uddg=${encoded}&rut=abc123">T</a>` +
      '<a class="result__snippet">s</a></div>';
    const out = parseDuckDuckGoResults(html);
    assert.equal(out.length, 1);
    assert.equal(out[0].url, 'https://real.example/article?id=7');
  });
});
