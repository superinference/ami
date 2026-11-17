import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validateUrlSafety, stripHtml, httpGet } from './web-utils';
import { extractQuery } from './tool-utils';

const FETCH_TIMEOUT_MS = 10000;
const MAX_CONTENT_PER_RESULT = 3000;
const TOP_RESULTS_TO_FETCH = 3;
const MIN_QUERY_LENGTH = 2;

/**
 * Upper bound on the number of searches performed in a single tool call.
 * Mirrors Claude Code's WebSearch `max_uses: 8` — the model may pass a primary
 * `query` plus a bounded list of additional `queries` and all are searched
 * within one invocation (with per-search partial-failure tolerance).
 */
export const MAX_QUERIES_PER_CALL = 8;

/**
 * Overall serialized tool-result size cap. Mirrors Claude Code's framework-level
 * `maxResultSizeChars: 100_000` knob on the WebSearch tool: the per-result
 * (MAX_CONTENT_PER_RESULT) and top-N (TOP_RESULTS_TO_FETCH) bounds limit each
 * piece, but a large number of results/snippets can still accumulate, so the
 * whole formatted output is capped as a last line of defense.
 */
export const MAX_RESULT_SIZE_CHARS = 100000;

/**
 * Cap the total serialized output to {@link MAX_RESULT_SIZE_CHARS}, appending a
 * truncation marker when the limit is exceeded. Mirrors Claude's
 * maxResultSizeChars. Exported for direct unit testing.
 */
export function capResultSize(output: string, max = MAX_RESULT_SIZE_CHARS): string {
  if (output.length <= max) return output;
  return (
    output.slice(0, max) +
    `\n\n[Results truncated: output exceeded the ${max} character limit]`
  );
}

/**
 * Current month/year injected into the tool description so the model grounds
 * recency-sensitive queries in the present year instead of a stale
 * training-cutoff year. Computed once at module load (the process is a single
 * session). Mirrors Claude's getLocalMonthYear() temporal guidance.
 */
const CURRENT_YEAR = new Date().getFullYear();
const CURRENT_MONTH_YEAR = new Date().toLocaleString('en-US', {
  month: 'long',
  year: 'numeric',
});

interface SearchResult {
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

function defaultSearchUrl(query: string): string {
  return `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
}

export function parseDuckDuckGoResults(html: string): SearchResult[] {
  const results: SearchResult[] = [];

  const splitRegex = /<div[^>]*class="[^"]*\bresult\s[^"]*"[^>]*>/gi;
  const positions: number[] = [];
  let match: RegExpExecArray | null;
  while ((match = splitRegex.exec(html)) !== null) {
    positions.push(match.index);
  }

  const blocks: string[] = [];
  for (let i = 0; i < positions.length; i++) {
    const start = positions[i];
    const end = i + 1 < positions.length ? positions[i + 1] : html.length;
    blocks.push(html.slice(start, end));
  }

  for (const block of blocks) {
    const linkMatch = block.match(
      /<a[^>]*class="result__a"[^>]*href="([^"]*)"[^>]*>([\s\S]*?)<\/a>/i,
    );
    if (!linkMatch) continue;

    let href = linkMatch[1];
    const titleHtml = linkMatch[2];

    const uddgMatch = href.match(/[?&]uddg=([^&]+)/);
    if (uddgMatch) {
      // Untrusted upstream HTML: a malformed percent-encoding (lone '%' / '%ZZ')
      // makes decodeURIComponent throw URIError. Mirror Claude's formatUri
      // never-throw guard — degrade this ONE result to its raw href instead of
      // letting the throw abort parsing of the entire search response.
      try {
        href = decodeURIComponent(uddgMatch[1]);
      } catch {
        href = uddgMatch[1];
      }
    }

    const title = titleHtml.replace(/<[^>]+>/g, '').trim();

    const snippetMatch = block.match(
      /<a[^>]*class="result__snippet"[^>]*>([\s\S]*?)<\/a>/i,
    ) || block.match(
      /<span[^>]*class="result__snippet"[^>]*>([\s\S]*?)<\/span>/i,
    ) || block.match(
      /<div[^>]*class="result__snippet"[^>]*>([\s\S]*?)<\/div>/i,
    );

    const snippet = snippetMatch
      ? snippetMatch[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim()
      : '';

    if (title && href) {
      results.push({ title, url: href, snippet });
    }

    if (results.length >= 10) break;
  }

  return results;
}

async function fetchResultContent(
  result: SearchResult,
  signal: AbortSignal,
  hooks: WebSearchHooks,
): Promise<string> {
  try {
    const ssrfResult = await hooks.validateUrlSafety(result.url);
    if ('error' in ssrfResult) return '';
    const { body, statusCode } = await hooks.httpGet(result.url, signal, { timeoutMs: FETCH_TIMEOUT_MS, resolvedIP: ssrfResult.resolvedIP });
    if (statusCode >= 400) return '';
    const text = stripHtml(body);
    return text.substring(0, MAX_CONTENT_PER_RESULT);
  } catch {
    return '';
  }
}

/**
 * Mandatory, structured citation directive appended to every successful search
 * result. Mirrors Claude's WebSearch prompt: a MANDATORY "Sources:" section with
 * an explicit [Title](URL) format spec and a concrete example, instead of a
 * single terse reminder line — so the model's citation output is consistent.
 */
const SOURCES_DIRECTIVE =
  '\n\nCRITICAL REQUIREMENT — You MUST follow this:\n' +
  '- After answering the user\'s question, you MUST include a "Sources:" section at the end of your response.\n' +
  '- In the Sources section, list all relevant URLs from these results as markdown hyperlinks: [Title](URL).\n' +
  '- This is MANDATORY — never skip including sources in your response.\n' +
  '- Example format:\n\n' +
  '    [Your answer here]\n\n' +
  '    Sources:\n' +
  '    - [Source Title 1](https://example.com/1)\n' +
  '    - [Source Title 2](https://example.com/2)';

function formatResults(query: string, results: SearchResult[]): string {
  const lines: string[] = [];
  lines.push(`Search results for: "${query}"\n`);

  for (let i = 0; i < results.length; i++) {
    const r = results[i];
    lines.push(`${i + 1}. [${r.title}](${r.url})`);
    if (r.snippet) {
      lines.push(`   ${r.snippet}`);
    }
    if (r.content) {
      lines.push('');
      lines.push(`   --- Content from ${r.url} ---`);
      lines.push(`   ${r.content.replace(/\n/g, '\n   ')}`);
      lines.push('   --- End ---');
    }
    lines.push('');
  }

  return lines.join('\n').trim();
}

type WebSearchBudget = NonNullable<ToolContext['_webSearchBudget']>;

/**
 * Charge `cost` searches against the session-wide runaway budget (token-bucket
 * refill). Returns a terminal error ToolResult when the charge pushes the count
 * over the cap, or `null` when the call may proceed. Centralizes the budget
 * accounting so the single-query and multi-query paths share one implementation
 * (no semantic duplication).
 */
function applyBudget(budget: WebSearchBudget, cost: number): ToolResult | null {
  if (budget.lastRefillTime && budget.refillRate) {
    const now = Date.now();
    const elapsedHours = (now - budget.lastRefillTime) / 3_600_000;
    const refilled = Math.floor(elapsedHours * budget.refillRate);
    if (refilled > 0) {
      budget.count = Math.max(0, budget.count - refilled);
      budget.lastRefillTime = now;
    }
  }
  budget.count += cost;
  if (budget.count > budget.max) {
    return {
      output: `Error: web search limit reached (${budget.max} per session). Do not attempt alternative search methods (web_fetch to search engines, bash curl to search URLs, or similar workarounds) — they will also fail or be blocked. Work with the information you already have.`,
      isError: true,
    };
  }
  return null;
}

/**
 * Outcome of a single DuckDuckGo search. Mirrors Claude's heterogeneous
 * `(SearchResult | string | null)[]` tolerance: a search either yields a list
 * of results (possibly empty) or degrades to one of the error variants, which
 * in the multi-query path becomes an error entry rather than failing the whole
 * tool call.
 */
interface SingleSearchOutcome {
  query: string;
  results?: SearchResult[];
  httpError?: number;
  aborted?: boolean;
  errorMessage?: string;
}

/**
 * Run exactly one DuckDuckGo search: emit the live "Searching"/"Found N" progress
 * affordances, fetch + parse results, apply domain filters, and enrich the top
 * results with page content. Never throws — transient failures are returned as
 * structured outcome variants so callers decide whether to fail (single-query)
 * or degrade to a per-search error entry (multi-query).
 */
async function runSingleSearch(
  query: string,
  allowedDomains: string[] | undefined,
  blockedDomains: string[] | undefined,
  context: ToolContext,
  hooks: WebSearchHooks,
): Promise<SingleSearchOutcome> {
  const searchUrl = hooks.searchUrl(query);

  // Live progress affordance (parity with Claude's WebSearch 'query_update'):
  // surface what is being searched before the (potentially multi-second) fetch
  // so the TUI/CLI shows activity instead of going silent until completion.
  context.onProgress?.(`Searching: "${query}"`);

  try {
    const { body, statusCode } = await hooks.httpGet(searchUrl, context.abortSignal);
    if (statusCode >= 400) return { query, httpError: statusCode };

    let results = parseDuckDuckGoResults(body);

    if (allowedDomains && allowedDomains.length > 0) {
      results = results.filter(r => {
        try {
          const host = new URL(r.url).hostname;
          return allowedDomains.some(d => host === d || host.endsWith('.' + d));
        } catch { return false; }
      });
    }

    if (blockedDomains && blockedDomains.length > 0) {
      results = results.filter(r => {
        try {
          const host = new URL(r.url).hostname;
          return !blockedDomains.some(d => host === d || host.endsWith('.' + d));
        } catch { return true; }
      });
    }

    // Live progress affordance (parity with Claude's 'search_results_received'):
    // report how many results came back for this query.
    context.onProgress?.(`Found ${results.length} results for "${query}"`);

    if (results.length === 0) return { query, results: [] };

    // Fetch content from top results in parallel for richer context.
    const topResults = results.slice(0, TOP_RESULTS_TO_FETCH);
    const contents = await Promise.all(
      topResults.map(r => fetchResultContent(r, context.abortSignal, hooks)),
    );
    for (let i = 0; i < topResults.length; i++) {
      if (contents[i]) topResults[i].content = contents[i];
    }

    return { query, results };
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    if (message === 'Aborted') return { query, aborted: true };
    return { query, errorMessage: message };
  }
}

/**
 * Resolve the ordered, de-duplicated list of queries for one tool call: the
 * required primary `query` first, then any additional `queries` (trimmed, >=
 * MIN_QUERY_LENGTH, de-duped), capped at {@link MAX_QUERIES_PER_CALL}. Mirrors
 * Claude's multi-query-in-one-call (max_uses: 8).
 */
export function resolveQueryList(primary: string, extra: unknown): string[] {
  const list = [primary];
  if (Array.isArray(extra)) {
    for (const raw of extra) {
      if (typeof raw !== 'string') continue;
      const q = raw.trim();
      if (q.length < MIN_QUERY_LENGTH) continue;
      if (list.includes(q)) continue;
      list.push(q);
      if (list.length >= MAX_QUERIES_PER_CALL) break;
    }
  }
  return list;
}

/**
 * Format a multi-query result: a "Did N searches" summary with timing (parity
 * with Claude's getSearchSummary / "Did {searchCount} searches in {time}"),
 * followed by each query's results or a degraded per-search error line, then the
 * mandatory Sources directive. A single failed search does not fail the call.
 */
function formatMultiResults(outcomes: SingleSearchOutcome[], durationMs: number): string {
  const succeeded = outcomes.filter(o => o.results !== undefined).length;
  const timeDisplay = durationMs >= 1000
    ? `${Math.round(durationMs / 1000)}s`
    : `${Math.round(durationMs)}ms`;

  const sections: string[] = [];
  sections.push(
    `Did ${succeeded} search${succeeded === 1 ? '' : 'es'} ` +
    `(${outcomes.length} ${outcomes.length === 1 ? 'query' : 'queries'}) in ${timeDisplay}`,
  );

  for (const o of outcomes) {
    sections.push('');
    if (o.results !== undefined) {
      if (o.results.length === 0) {
        sections.push(`No results found for "${o.query}".`);
      } else {
        sections.push(formatResults(o.query, o.results));
      }
    } else if (o.aborted) {
      sections.push(`Web search error for "${o.query}": search aborted.`);
    } else if (o.httpError !== undefined) {
      sections.push(`Web search error for "${o.query}": DuckDuckGo returned HTTP ${o.httpError}.`);
    } else {
      sections.push(`Web search error for "${o.query}": ${o.errorMessage}.`);
    }
  }

  return sections.join('\n').trim();
}

export const webSearchTool: ToolDefinition = {
  name: 'web_search',
  description:
    'Search the web and return results with article content. ' +
    'Provides up-to-date information for current events and recent data. ' +
    "Use this tool for accessing information beyond your knowledge cutoff. " +
    'Returns search results with titles, URLs, snippets, and the actual page content from the top results, formatted with markdown hyperlinks. ' +
    'Searches are performed automatically within a single call. ' +
    `You may issue up to ${MAX_QUERIES_PER_CALL} searches in one call by passing additional "queries"; each is searched independently and a single failed search does not fail the others. ` +
    `The current month is ${CURRENT_MONTH_YEAR}. You MUST use the current year (${CURRENT_YEAR}) when searching for recent information, documentation, or current events — e.g. for "latest React docs", search "React documentation ${CURRENT_YEAR}", NOT last year.`,
  inputSchema: {
    type: 'object',
    properties: {
      query: {
        type: 'string',
        minLength: MIN_QUERY_LENGTH,
        description: 'The search query',
      },
      queries: {
        type: 'array',
        items: { type: 'string', minLength: MIN_QUERY_LENGTH },
        description:
          `Optional additional search queries to run in the same call (agentic multi-query search, up to ${MAX_QUERIES_PER_CALL} total including "query"). Each is searched independently with per-search partial-failure tolerance.`,
      },
      allowed_domains: {
        type: 'array',
        items: { type: 'string' },
        description: 'Only include results from these domains.',
      },
      blocked_domains: {
        type: 'array',
        items: { type: 'string' },
        description: 'Exclude results from these domains.',
      },
    },
    required: ['query'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,
  // Read-only yet OPEN-WORLD: queries a live search backend, so results depend on
  // external state and are non-reproducible across runs (parity: Claude's isOpenWorld).
  isOpenWorld: true,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const q = extractQuery(input);
    if (q.error) return q.error;
    const query = q.query;

    // Reject degenerate 1-character / whitespace-trimmed queries before any
    // budget spend or network call (mirrors Claude's z.string().min(2)).
    if (query.trim().length < MIN_QUERY_LENGTH) {
      return {
        output: `Error: query must be at least ${MIN_QUERY_LENGTH} characters.`,
        isError: true,
      };
    }

    // allowed_domains and blocked_domains are mutually exclusive: supplying both
    // is contradictory (the allow-list already excludes everything else), so
    // reject with a corrective message instead of silently chaining filters.
    const allowedDomains = input.allowed_domains as string[] | undefined;
    const blockedDomains = input.blocked_domains as string[] | undefined;
    if (
      allowedDomains && allowedDomains.length > 0 &&
      blockedDomains && blockedDomains.length > 0
    ) {
      return {
        output: 'Error: Cannot specify both allowed_domains and blocked_domains in the same request',
        isError: true,
      };
    }

    // Resolve the ordered query list (primary + any additional queries). When no
    // extra queries are supplied this is exactly [query] and the single-query
    // path below runs unchanged.
    const queryList = resolveQueryList(query, input.queries);

    // Session-wide runaway cap with token-bucket refill: each query in the call
    // is one search, charged together so the whole call is blocked (not partially
    // run) once the budget is exceeded.
    const budget = context._webSearchBudget;
    if (budget) {
      const blocked = applyBudget(budget, queryList.length);
      if (blocked) return blocked;
    }

    const hooks: WebSearchHooks = (context._webSearchHooks as WebSearchHooks | undefined) ?? { httpGet, validateUrlSafety, searchUrl: defaultSearchUrl };

    // ---- Multi-query path (parity with Claude's max_uses:8 agentic search) ----
    // Several searches in one call with per-search partial-failure tolerance:
    // an individual search that errors degrades to a per-search error entry
    // instead of failing the whole tool call, and a "Did N searches" summary
    // reports how many succeeded.
    if (queryList.length > 1) {
      const start = Date.now();
      const outcomes = await Promise.all(
        queryList.map(qq => runSingleSearch(qq, allowedDomains, blockedDomains, context, hooks)),
      );
      const succeeded = outcomes.filter(o => o.results !== undefined).length;
      context.onProgress?.(`Did ${succeeded} search${succeeded === 1 ? '' : 'es'} of ${queryList.length}`);
      const output = capResultSize(
        formatMultiResults(outcomes, Date.now() - start) + SOURCES_DIRECTIVE,
      );
      // Only fail the whole call when EVERY search failed (no partial data).
      return succeeded === 0 ? { output, isError: true } : { output };
    }

    // ---- Single-query path (unchanged behavior) ----
    const outcome = await runSingleSearch(query, allowedDomains, blockedDomains, context, hooks);

    if (outcome.httpError !== undefined) {
      return {
        output: `Error: DuckDuckGo returned HTTP ${outcome.httpError}. Try using web_fetch with a specific URL instead.`,
        isError: true,
      };
    }
    if (outcome.aborted) {
      return { output: 'Search aborted.', isError: true };
    }
    if (outcome.errorMessage !== undefined) {
      return {
        output: `Error searching for "${query}": ${outcome.errorMessage}. Try using web_fetch with a specific URL instead.`,
        isError: true,
      };
    }
    if (outcome.results!.length === 0) {
      return {
        output: `No results found for "${query}". Try using web_fetch with a specific URL if you know where the information is.`,
      };
    }

    const output = capResultSize(formatResults(query, outcome.results!) + SOURCES_DIRECTIVE);
    return { output };
  },
};
