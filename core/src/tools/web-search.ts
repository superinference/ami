import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validateUrlSafety, stripHtml, httpGet } from './web-utils';
import { extractQuery } from './tool-utils';

const FETCH_TIMEOUT_MS = 10000;
const MAX_CONTENT_PER_RESULT = 3000;
const TOP_RESULTS_TO_FETCH = 3;
const MIN_QUERY_LENGTH = 2;

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

export const webSearchTool: ToolDefinition = {
  name: 'web_search',
  description:
    'Search the web and return results with article content. ' +
    'Provides up-to-date information for current events and recent data. ' +
    "Use this tool for accessing information beyond your knowledge cutoff. " +
    'Returns search results with titles, URLs, snippets, and the actual page content from the top results, formatted with markdown hyperlinks. ' +
    'Searches are performed automatically within a single call. ' +
    `The current month is ${CURRENT_MONTH_YEAR}. You MUST use the current year (${CURRENT_YEAR}) when searching for recent information, documentation, or current events — e.g. for "latest React docs", search "React documentation ${CURRENT_YEAR}", NOT last year.`,
  inputSchema: {
    type: 'object',
    properties: {
      query: {
        type: 'string',
        minLength: MIN_QUERY_LENGTH,
        description: 'The search query',
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

    // Session-wide runaway cap with token-bucket refill.
    const budget = context._webSearchBudget;
    if (budget) {
      if (budget.lastRefillTime && budget.refillRate) {
        const now = Date.now();
        const elapsedHours = (now - budget.lastRefillTime) / 3_600_000;
        const refilled = Math.floor(elapsedHours * budget.refillRate);
        if (refilled > 0) {
          budget.count = Math.max(0, budget.count - refilled);
          budget.lastRefillTime = now;
        }
      }
      budget.count++;
      if (budget.count > budget.max) {
        return { output: `Error: web search limit reached (${budget.max} per session). Do not attempt alternative search methods (web_fetch to search engines, bash curl to search URLs, or similar workarounds) — they will also fail or be blocked. Work with the information you already have.`, isError: true };
      }
    }

    const hooks: WebSearchHooks = (context._webSearchHooks as WebSearchHooks | undefined) ?? { httpGet, validateUrlSafety, searchUrl: defaultSearchUrl };
    const searchUrl = hooks.searchUrl(query);

    // Live progress affordance (parity with Claude's WebSearch 'query_update'):
    // surface what is being searched before the (potentially multi-second) fetch
    // so the TUI/CLI shows activity instead of going silent until completion.
    context.onProgress?.(`Searching: "${query}"`);

    try {
      const { body, statusCode } = await hooks.httpGet(
        searchUrl,
        context.abortSignal,
      );

      if (statusCode >= 400) {
        return {
          output: `Error: DuckDuckGo returned HTTP ${statusCode}. Try using web_fetch with a specific URL instead.`,
          isError: true,
        };
      }

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

      if (results.length === 0) {
        return {
          output: `No results found for "${query}". Try using web_fetch with a specific URL if you know where the information is.`,
        };
      }

      // Fetch content from top results in parallel for richer context
      const topResults = results.slice(0, TOP_RESULTS_TO_FETCH);
      const contentPromises = topResults.map(r => fetchResultContent(r, context.abortSignal, hooks));
      const contents = await Promise.all(contentPromises);

      for (let i = 0; i < topResults.length; i++) {
        if (contents[i]) {
          topResults[i].content = contents[i];
        }
      }

      const output = capResultSize(formatResults(query, results) + SOURCES_DIRECTIVE);
      return { output };
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);

      if (message === 'Aborted') {
        return { output: 'Search aborted.', isError: true };
      }

      return {
        output: `Error searching for "${query}": ${message}. Try using web_fetch with a specific URL instead.`,
        isError: true,
      };
    }
  },
};
