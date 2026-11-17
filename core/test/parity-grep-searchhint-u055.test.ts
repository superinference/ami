import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { grepTool } from '../src/tools/grep';
import { toolSearchTool, setSearchableTools } from '../src/tools/tool-search';
import type { ToolContext } from '../src/types';

/**
 * U055 — grep must advertise a curated `searchHint` so `tool_search` can surface it
 * by capability synonyms NOT already in the name/description (e.g. "contents"), the
 * same discovery affordance every other first-class tool already carries. Parity:
 * Claude Code's GrepTool `searchHint: 'search file contents with regex (ripgrep)'`.
 *
 * RED (before the fix): grep has no `searchHint`, and the standalone word "contents"
 * appears nowhere in its name/description, so a `+contents` required-term query
 * filters grep OUT (score 0) → grep is absent from the matches. GREEN: the hint adds
 * "contents" to the required-term match surface, so grep is returned.
 */

const ctx: ToolContext = { cwd: '/tmp', abortSignal: new AbortController().signal };

describe('grep tool — tool_search discovery hint (U055)', () => {
  beforeEach(() => { setSearchableTools([grepTool]); });
  afterEach(() => { setSearchableTools([]); });

  it('exposes the ripgrep-content searchHint', () => {
    assert.equal(grepTool.searchHint, 'search file contents with regex (ripgrep)');
  });

  it('is discoverable via a required term that lives ONLY in the searchHint', async () => {
    const result = await toolSearchTool.execute({ query: '+contents' }, ctx);
    assert.equal(result.isError, false);
    const matches = (result.metadata as { matches?: string[] } | undefined)?.matches ?? [];
    assert.ok(matches.includes('grep'), `expected grep in matches, got ${JSON.stringify(matches)}`);
    assert.ok(result.output.includes('grep'));
  });

  it('does NOT falsely match the searchHint term inside the description alone', () => {
    // Guard the RED discriminator: "contents" must be absent (word-boundary) from the
    // name + description so the +contents hit above is attributable to the hint.
    const haystack = `${grepTool.name}\n${grepTool.description}`;
    assert.ok(!/\bcontents\b/i.test(haystack));
  });
});
