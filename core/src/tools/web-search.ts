import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validateUrlSafety, stripHtml, httpGet } from './web-utils';
import { extractQuery } from './tool-utils';

const FETCH_TIMEOUT_MS = 10000;
const MAX_CONTENT_PER_RESULT = 3000;
const TOP_RESULTS_TO_FETCH = 3;

interface SearchResult {
  title: string;
  url: string;
  snippet: string;
  content?: string;
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
      href = decodeURIComponent(uddgMatch[1]);
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
): Promise<string> {
  try {
    const ssrfResult = await validateUrlSafety(result.url);
    if ('error' in ssrfResult) return '';
    const { body, statusCode } = await httpGet(result.url, signal, { timeoutMs: FETCH_TIMEOUT_MS, resolvedIP: ssrfResult.resolvedIP });
    if (statusCode >= 400) return '';
    const text = stripHtml(body);
    return text.substring(0, MAX_CONTENT_PER_RESULT);
  } catch {
    return '';
  }
}

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
    'Search the web and return results with article content. Returns search results with titles, URLs, snippets, and the actual page content from the top results.',
  inputSchema: {
    type: 'object',
    properties: {
      query: {
        type: 'string',
        description: 'The search query',
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

    const searchUrl = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;

    try {
      const { body, statusCode } = await httpGet(
        searchUrl,
        context.abortSignal,
      );

      if (statusCode >= 400) {
        return {
          output: `Error: DuckDuckGo returned HTTP ${statusCode}. Try using web_fetch with a specific URL instead.`,
          isError: true,
        };
      }

      const results = parseDuckDuckGoResults(body);

      if (results.length === 0) {
        return {
          output: `No results found for "${query}". Try using web_fetch with a specific URL if you know where the information is.`,
        };
      }

      // Fetch content from top results in parallel for richer context
      const topResults = results.slice(0, TOP_RESULTS_TO_FETCH);
      const contentPromises = topResults.map(r => fetchResultContent(r, context.abortSignal));
      const contents = await Promise.all(contentPromises);

      for (let i = 0; i < topResults.length; i++) {
        if (contents[i]) {
          topResults[i].content = contents[i];
        }
      }

      return { output: formatResults(query, results) };
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
