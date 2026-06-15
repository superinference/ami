import * as fs from 'fs';
import * as path from 'path';
import { URL } from 'url';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validateUrlSafety, stripHtml, httpGet } from './web-utils';

const MAX_RESPONSE_LENGTH = 50000;
const MAX_URL_LENGTH = 2000;

// --- Byte-limited LRU cache ---
const CACHE_MAX_BYTES = 50 * 1024 * 1024; // 50MB
const CACHE_TTL_MS = 15 * 60 * 1000; // 15 minutes

interface CacheEntry {
  content: string;
  timestamp: number;
  sizeBytes: number;
}

const fetchCache = new Map<string, CacheEntry>();
let cacheTotalBytes = 0;

function getCached(url: string): string | null {
  const entry = fetchCache.get(url);
  if (!entry) return null;
  if (Date.now() - entry.timestamp > CACHE_TTL_MS) {
    fetchCache.delete(url);
    cacheTotalBytes -= entry.sizeBytes;
    return null;
  }
  return entry.content;
}

function setCache(url: string, content: string): void {
  // If already cached, remove old entry's size first
  const existing = fetchCache.get(url);
  if (existing) {
    cacheTotalBytes -= existing.sizeBytes;
    fetchCache.delete(url);
  }
  const sizeBytes = Buffer.byteLength(content, 'utf-8');
  // Evict oldest entries if over limit
  while (cacheTotalBytes + sizeBytes > CACHE_MAX_BYTES && fetchCache.size > 0) {
    const oldest = fetchCache.keys().next().value;
    if (oldest === undefined) break;
    const oldEntry = fetchCache.get(oldest)!;
    cacheTotalBytes -= oldEntry.sizeBytes;
    fetchCache.delete(oldest);
  }
  fetchCache.set(url, { content, timestamp: Date.now(), sizeBytes });
  cacheTotalBytes += sizeBytes;
}

function htmlToMarkdown(html: string): string {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<h([1-6])[^>]*>(.*?)<\/h\1>/gi, (_, level, text) => '#'.repeat(parseInt(level)) + ' ' + text.trim() + '\n')
    .replace(/<p[^>]*>(.*?)<\/p>/gi, '$1\n\n')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<a[^>]*href="([^"]*)"[^>]*>(.*?)<\/a>/gi, '[$2]($1)')
    .replace(/<li[^>]*>(.*?)<\/li>/gi, '- $1\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

export const webFetchTool: ToolDefinition = {
  name: 'web_fetch',
  description:
    'Fetch content from a URL as text (HTML is auto-stripped). Use for reading documentation, articles, APIs, and web pages. NOT for binary files (zip, tar, images) — use bash with curl -O for binary downloads.',
  inputSchema: {
    type: 'object',
    properties: {
      url: {
        type: 'string',
        description: 'The URL to fetch',
      },
      prompt: {
        type: 'string',
        description: 'Required. The question or instruction to answer using the fetched content.',
      },
      allowed_domains: {
        type: 'array',
        items: { type: 'string' },
        description: 'Only allow fetching from these domains.',
      },
      blocked_domains: {
        type: 'array',
        items: { type: 'string' },
        description: 'Block fetching from these domains.',
      },
    },
    required: ['url'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    let url = input.url as string;
    const prompt = (input.prompt as string) ?? '';

    if (!url || url.trim().length === 0) {
      return { output: 'Error: url must not be empty.', isError: true };
    }

    // Auto-upgrade HTTP to HTTPS (skip localhost/127.0.0.1 for local dev servers)
    if (url.startsWith('http://') && !/^http:\/\/(localhost|127\.0\.0\.1)(:|\/|$)/.test(url)) {
      url = url.replace('http://', 'https://');
    }

    // Reject excessively long URLs
    if (url.length > MAX_URL_LENGTH) {
      return { output: `Error: URL exceeds maximum length of ${MAX_URL_LENGTH} characters.`, isError: true };
    }

    // Validate URL
    let parsedUrl: URL;
    try {
      parsedUrl = new URL(url);
    } catch {
      return {
        output: `Error: Invalid URL "${url}". The URL could not be parsed.`,
        isError: true,
      };
    }

    // Strip credentials from URL before including in output to prevent leaking
    // user:password into the LLM conversation context.
    let sanitizedUrl = url;
    if (parsedUrl.username || parsedUrl.password) {
      parsedUrl.username = '';
      parsedUrl.password = '';
      sanitizedUrl = parsedUrl.toString();
    }

    // Domain filtering
    const allowedDomains = input.allowed_domains as string[] | undefined;
    const blockedDomains = input.blocked_domains as string[] | undefined;
    const host = parsedUrl.hostname;

    if (allowedDomains && allowedDomains.length > 0) {
      if (!allowedDomains.some(d => host === d || host.endsWith('.' + d))) {
        return { output: `Error: Domain "${host}" is not in the allowed domains list.`, isError: true };
      }
    }

    if (blockedDomains && blockedDomains.length > 0) {
      if (blockedDomains.some(d => host === d || host.endsWith('.' + d))) {
        return { output: `Error: Domain "${host}" is blocked.`, isError: true };
      }
    }

    // Only allow http/https
    if (
      parsedUrl.protocol !== 'http:' &&
      parsedUrl.protocol !== 'https:'
    ) {
      return {
        output: `Error: Unsupported protocol "${parsedUrl.protocol}". Only http and https are supported.`,
        isError: true,
      };
    }

    // SSRF protection: block private/metadata hostnames and IPs (before cache check)
    let pinnedIP: string | undefined;
    if (!context._allowLocalhostForTesting) {
      const ssrfResult = await validateUrlSafety(url);
      if ('error' in ssrfResult) {
        return { output: `Error: ${ssrfResult.error}`, isError: true };
      }
      pinnedIP = ssrfResult.resolvedIP;
    }

    // Response cache check (after SSRF to prevent cache bypass)
    const cacheKey = prompt ? `${url}\0${prompt}` : url;
    const cached = getCached(cacheKey);
    if (cached !== null) {
      return { output: cached };
    }

    try {
      const { body, statusCode, contentType, finalUrl } = await httpGet(
        url,
        context.abortSignal,
        { resolvedIP: pinnedIP },
      );

      if (statusCode >= 400) {
        return {
          output: `Error: HTTP ${statusCode} when fetching ${sanitizedUrl}`,
          isError: true,
        };
      }

      // Cross-host redirect detection
      const originalHost = new URL(url).hostname;
      const finalHost = new URL(finalUrl).hostname;
      if (originalHost !== finalHost) {
        return { output: `Redirect detected: ${url} → ${finalUrl}\nThe URL redirected to a different host. Re-fetch with the new URL if needed.` };
      }

      // Handle PDF responses — extract text from the PDF
      if (contentType.includes('application/pdf') || body.startsWith('%PDF')) {
        try {
          const pdfParse = require('pdf-parse');
          const pdfBuffer = Buffer.from(body, 'binary');
          const pdf = await pdfParse(pdfBuffer);
          const pdfText = (pdf.text || '').trim();
          if (pdfText.length > 100) {
            const truncated = pdfText.length > MAX_RESPONSE_LENGTH
              ? pdfText.slice(0, MAX_RESPONSE_LENGTH) + '\n\n[PDF content truncated]'
              : pdfText;
            return {
              output: `URL: ${sanitizedUrl}\nType: PDF (${pdf.numpages} pages)\n\n${truncated}`,
              isError: false,
            };
          }
        } catch {}
        // Fallback: try HTML version
        const htmlUrl = url.replace('/pdf/', '/html/');
        if (htmlUrl !== url) {
          try {
            const htmlResult = await httpGet(htmlUrl, context.abortSignal);
            if (htmlResult.statusCode < 400 && htmlResult.contentType.includes('text/html')) {
              const htmlContent = stripHtml(htmlResult.body);
              if (htmlContent.length > 200) {
                return {
                  output: `URL: ${htmlUrl} (HTML version of PDF)\nStatus: ${htmlResult.statusCode}\n\n${htmlContent.slice(0, MAX_RESPONSE_LENGTH)}`,
                  isError: false,
                };
              }
            }
          } catch {}
        }
        return {
          output: `Error: Could not extract text from PDF at "${sanitizedUrl}".`,
          isError: true,
        };
      }

      const binaryTypes = ['image/', 'application/zip', 'application/octet-stream'];
      if (binaryTypes.some(t => contentType?.includes(t))) {
        const ext = contentType?.split('/').pop()?.split(';')[0] || 'bin';
        const spillDir = path.join(context.cwd, '.superinference', 'tool-results');
        fs.mkdirSync(spillDir, { recursive: true });
        const spillFile = path.join(spillDir, `fetch-${Date.now()}.${ext}`);
        fs.writeFileSync(spillFile, body);
        return { output: `[Binary content (${contentType}, ${body.length} bytes) saved to ${spillFile}]` };
      }

      // Convert HTML to markdown, leave other content types as-is
      let content: string;
      if (contentType.includes('text/html')) {
        content = htmlToMarkdown(body);
      } else {
        content = body;
      }

      // Truncate if necessary
      if (content.length > MAX_RESPONSE_LENGTH) {
        content =
          content.slice(0, MAX_RESPONSE_LENGTH) +
          '\n\n[Content truncated at 50000 characters]';
      }

      // Build the result
      const parts: string[] = [];
      parts.push(`URL: ${sanitizedUrl}`);
      parts.push(`Status: ${statusCode}`);
      parts.push(`Content-Type: ${contentType}`);
      if (prompt) {
        parts.push(`Prompt: ${prompt}`);
      }
      parts.push('');
      parts.push(content);

      let output = parts.join('\n');

      // Apply prompt to content via LLM summarization (like Claude's applyPromptToMarkdown)
      if (input.prompt && typeof input.prompt === 'string' && context._engineFactory) {
        try {
          const summarizeEngine = context._engineFactory({
            provider: context._providerConfig!,
            cwd: context.cwd,
            permissionMode: 'auto-allow',
            maxTurns: 1,
            tokenBudget: 8000,
          });
          let summary = '';
          const summarizePrompt = `Given this web page content, answer the following question/instruction:\n\nQuestion: ${input.prompt}\n\nContent:\n${output.slice(0, 30000)}`;
          for await (const event of summarizeEngine.submit(summarizePrompt)) {
            if (event.type === 'text_delta') summary += event.text;
          }
          summarizeEngine.shutdown?.();
          if (summary.trim()) output = summary.trim();
        } catch {}
      }

      // Store in cache
      setCache(cacheKey, output);

      return { output };
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);

      if (message === 'Aborted') {
        return { output: 'Fetch aborted.', isError: true };
      }

      return {
        output: `Error fetching ${sanitizedUrl}: ${message}`,
        isError: true,
      };
    }
  },
};
