// Split from web-search.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { validateUrlSafety, httpGet } from './web-utils';

export const FETCH_TIMEOUT_MS = 10000;

export const MAX_CONTENT_PER_RESULT = 3000;

export const TOP_RESULTS_TO_FETCH = 3;

export const MIN_QUERY_LENGTH = 2;

/**
 * Upper bound on the number of searches performed in a single tool call.
 * Mirrors the reference's WebSearch `max_uses: 8` — the model may pass a primary
 * `query` plus a bounded list of additional `queries` and all are searched
 * within one invocation (with per-search partial-failure tolerance).
 */
export const MAX_QUERIES_PER_CALL = 8;

/**
 * Overall serialized tool-result size cap. Mirrors the reference's framework-level
 * `maxResultSizeChars: 100_000` knob on the WebSearch tool: the per-result
 * (MAX_CONTENT_PER_RESULT) and top-N (TOP_RESULTS_TO_FETCH) bounds limit each
 * piece, but a large number of results/snippets can still accumulate, so the
 * whole formatted output is capped as a last line of defense.
 */
export const MAX_RESULT_SIZE_CHARS = 100000;

/**
 * Cap the total serialized output to {@link MAX_RESULT_SIZE_CHARS}, appending a
 * truncation marker when the limit is exceeded. Mirrors the reference's
 * maxResultSizeChars. Exported for direct unit testing.
 */
export function capResultSize(output: string, max = MAX_RESULT_SIZE_CHARS): string {
  if (output.length <= max) return output;
  return (
    output.slice(0, max) +
    `\n\n[Results truncated: output exceeded the ${max} character limit]`
  );
}

export interface SearchResult {
  title: string;
  url: string;
  snippet: string;
  content?: string;
}

/**
 * Dependency seam for tests (mirrors the _createClient / fsHooks pattern used
 * elsewhere). Production code never sets `context._webSearchHooks`, so the
 * default object below — the real `httpGet`, `validateUrlSafety`, and the
 * hard-coded DuckDuckGo URL builder — is used unchanged. A test may supply a
 * mock HTTP getter / SSRF validator / search-URL builder to drive the
 * HTTP-success path against a local mock server without touching the network.
 */
export interface WebSearchHooks {
  httpGet: typeof httpGet;
  validateUrlSafety: typeof validateUrlSafety;
  searchUrl: (query: string) => string;
}

export function defaultSearchUrl(query: string): string {
  return `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
}
