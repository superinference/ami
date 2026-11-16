import * as fs from 'fs';
import { projectStateDir } from '../reference-journal';
import * as path from 'path';
import { URL } from 'url';
import { createRequire } from 'module';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validateUrlSafety, stripHtml, httpGet, MAX_HTTP_BODY_BYTES } from './web-utils';
import { spillToolText, formatSize, spillErrorReason } from './spill-output';

const MAX_RESPONSE_LENGTH = 50000;
export const MAX_FETCH_DEADLINE_MS = 300_000;

/** Head and tail for a summary prompt. A head slice hides the end of the page. */
export function summaryWindow(text: string, limit = 30_000): string {
  if (text.length <= limit) return text;
  const marker = '\n\n[... middle omitted from the summary prompt; the tail follows ...]\n\n';
  const head = Math.floor(limit * 0.67);
  const tail = Math.max(0, limit - head - marker.length);
  return text.slice(0, head) + marker + text.slice(text.length - tail);
}
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

// Exported for direct unit testing of the byte-limited LRU cache (TTL expiry,
// re-set, eviction) — paths the public execute() flow cannot reach because it
// always consults getCached (which deletes expired/absent keys) before setCache.
export function getCached(url: string): string | null {
  const entry = fetchCache.get(url);
  if (!entry) return null;
  if (Date.now() - entry.timestamp > CACHE_TTL_MS) {
    fetchCache.delete(url);
    cacheTotalBytes -= entry.sizeBytes;
    return null;
  }
  return entry.content;
}

export function setCache(url: string, content: string): void {
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

/**
 * Map a server-supplied Content-Type to a safe, fixed-vocabulary file extension.
 * The subtype of an attacker-controlled Content-Type header must NEVER be used
 * verbatim to name a file on disk — it can carry path separators, '..', or other
 * garbage that corrupts the spill filename. This whitelist returns a known-safe
 * extension and falls back to 'bin' for anything unrecognized (parity with
 * Claude's extensionForMimeType).
 */
export function extensionForMimeType(mimeType: string | undefined): string {
  if (!mimeType) return 'bin';
  const mt = (mimeType.split(';')[0] ?? '').trim().toLowerCase();
  const MAP: Record<string, string> = {
    'application/pdf': 'pdf',
    'application/json': 'json',
    'text/csv': 'csv',
    'text/plain': 'txt',
    'text/html': 'html',
    'text/markdown': 'md',
    'application/zip': 'zip',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'pptx',
    'application/msword': 'doc',
    'application/vnd.ms-excel': 'xls',
    'audio/mpeg': 'mp3',
    'audio/wav': 'wav',
    'audio/ogg': 'ogg',
    'video/mp4': 'mp4',
    'video/webm': 'webm',
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'image/gif': 'gif',
    'image/webp': 'webp',
    'image/svg+xml': 'svg',
  };
  return MAP[mt] ?? 'bin';
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
      offset: {
        type: 'number',
        description: 'Character offset into the page text to start reading from. Use when a previous fetch reported truncated content.',
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
    const textOffset = (input.offset as number) ?? 0;
    // Test-only dependency seam (defaults to the real httpGet). Lets a test drive
    // the cross-host-redirect branch and other response shapes deterministically.
    const doHttpGet = (context._webFetchHttpGet as typeof httpGet | undefined) ?? httpGet;

    // Operator kill-switch (Claude Code parity): disable outbound web_fetch.
    if (process.env.CLAUDE_CODE_DISABLE_WEB_FETCH === '1' || process.env.SUPERINFERENCE_DISABLE_WEB_FETCH === '1') {
      return { output: 'Error: web_fetch is disabled (CLAUDE_CODE_DISABLE_WEB_FETCH / SUPERINFERENCE_DISABLE_WEB_FETCH).', isError: true };
    }

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

    // Strip credentials from the URL before it is used anywhere: not only to keep
    // user:password out of the LLM conversation context, but so the credentials are
    // never transmitted on the wire (Node turns a userinfo-bearing URL into a Basic
    // Authorization header). We fetch, cache, validate, and display the stripped URL.
    let sanitizedUrl = url;
    if (parsedUrl.username || parsedUrl.password) {
      parsedUrl.username = '';
      parsedUrl.password = '';
      sanitizedUrl = parsedUrl.toString();
      url = sanitizedUrl;
    }

    // Domain filtering
    const allowedDomains = input.allowed_domains as string[] | undefined;
    const blockedDomains = input.blocked_domains as string[] | undefined;
    const host = parsedUrl.hostname;

    // Contradictory filters are almost always a mistake and the intent is ambiguous
    // (allow-list and deny-list at once). Reject rather than silently pick one.
    if (allowedDomains && allowedDomains.length > 0 && blockedDomains && blockedDomains.length > 0) {
      return { output: 'Error: Cannot specify both allowed_domains and blocked_domains in the same request.', isError: true };
    }

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
    const cacheKey = `${url}\0${prompt}\0${textOffset}`;
    const cached = getCached(cacheKey);
    if (cached !== null) {
      return { output: cached };
    }

    const deadlineMs = (context as any)._fetchDeadlineMs ?? MAX_FETCH_DEADLINE_MS;
    const deadlineSignal = AbortSignal.timeout(deadlineMs);
    const fetchSignal = context.abortSignal
      ? AbortSignal.any([context.abortSignal, deadlineSignal])
      : deadlineSignal;

    try {
      const { body, bodyBuffer, statusCode, contentType, finalUrl, truncated } = await doHttpGet(
        url,
        fetchSignal,
        { resolvedIP: pinnedIP, raw: true },
      );

      if (statusCode >= 500) {
        return {
          output: `Error: HTTP ${statusCode} (server error) when fetching ${sanitizedUrl}`,
          isError: true,
        };
      }

      // Redirect-boundary detection. A redirect that crosses an origin boundary
      // (host, protocol-downgrade, or port) is surfaced for re-approval instead of
      // transparently returning content fetched from somewhere the caller did not
      // ask for. Hostnames are www-normalized so a bare `www.` add/remove on the
      // same registrable host is treated as the same origin and the content is kept.
      const originalUrl = new URL(url);
      const finalParsed = new URL(finalUrl);
      const normHost = (h: string): string => h.replace(/^www\./i, '');
      const effectivePort = (u: URL): string =>
        u.port || (u.protocol === 'https:' ? '443' : u.protocol === 'http:' ? '80' : '');
      const hostChanged = normHost(originalUrl.hostname) !== normHost(finalParsed.hostname);
      const protocolDowngrade = originalUrl.protocol === 'https:' && finalParsed.protocol === 'http:';
      const portChanged = effectivePort(originalUrl) !== effectivePort(finalParsed);
      if (hostChanged || protocolDowngrade || portChanged) {
        const reason = hostChanged
          ? 'a different host'
          : protocolDowngrade
            ? 'an insecure (http) protocol'
            : 'a different port';
        const refetch = prompt
          ? `Re-fetch with url="${finalUrl}" and the same prompt="${prompt}" if you still want this content.`
          : `Re-fetch with url="${finalUrl}" if you still want this content.`;
        return {
          output: `Redirect detected: ${sanitizedUrl} → ${finalUrl}\nThe URL redirected to ${reason}; the fetched content was not returned. ${refetch}`,
        };
      }

      // Handle PDF responses — extract text from the PDF
      if (contentType.includes('application/pdf') || body.startsWith('%PDF')) {
        try {
          // Resolve pdf-parse from OUR OWN install location, never from the
          // (untrusted) process.cwd(): a cloned repo could ship a malicious
          // node_modules/pdf-parse that would otherwise execute the instant
          // web_fetch retrieves any PDF (arbitrary code execution).
          const ownRequire = createRequire(import.meta.url);
          const { PDFParse } = ownRequire('pdf-parse');
          const pdfBuffer = bodyBuffer ?? Buffer.from(body, 'binary');
          const parser = new PDFParse({ data: pdfBuffer });
          const pdf = await parser.getText();
          await parser.destroy();
          const pdfText = (pdf.text || '').trim();
          if (pdfText.length > 100) {
            const truncated = spillToolText(context.cwd, 'pdf', pdfText, MAX_RESPONSE_LENGTH);
            return {
              output: `URL: ${sanitizedUrl}\nType: PDF (${pdf.total} pages)\n\n${truncated}`,
              isError: false,
            };
          }
        } catch {}
        // Fallback: try HTML version
        const htmlUrl = url.replace('/pdf/', '/html/');
        if (htmlUrl !== url) {
          try {
            let fallbackIP: string | undefined;
            if (!context._allowLocalhostForTesting) {
              const fallbackSsrf = await validateUrlSafety(htmlUrl);
              if ('error' in fallbackSsrf) throw new Error(fallbackSsrf.error);
              fallbackIP = fallbackSsrf.resolvedIP;
            }
            const htmlResult = await doHttpGet(htmlUrl, fetchSignal, { resolvedIP: fallbackIP });
            if (htmlResult.statusCode < 400 && htmlResult.contentType.includes('text/html')) {
              const htmlContent = stripHtml(htmlResult.body);
              if (htmlContent.length > 200) {
                const page = spillToolText(context.cwd, 'web-fetch', htmlContent, MAX_RESPONSE_LENGTH);
                return {
                  output: `URL: ${htmlUrl} (HTML version of PDF)\nStatus: ${htmlResult.statusCode}\n\n${page}`,
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
        // Derive the extension from a fixed-vocabulary whitelist so a hostile
        // Content-Type header can never inject path separators / garbage into
        // the on-disk filename.
        const ext = extensionForMimeType(contentType);
        // Write the raw bytes, not the lossy UTF-8 string, so the saved file is intact.
        const outBytes = bodyBuffer ?? Buffer.from(body, 'binary');
        try {
          const spillDir = projectStateDir(context.cwd, 'tool-results');
          fs.mkdirSync(spillDir, { recursive: true });
          const spillFile = path.join(spillDir, `fetch-${Date.now()}.${ext}`);
          fs.writeFileSync(spillFile, outBytes);
          return { output: `[Binary content (${contentType}, ${formatSize(outBytes.length)}) saved to ${spillFile}]` };
        } catch (writeErr: unknown) {
          // A disk/permission failure persisting the bytes is surfaced as a clear
          // tool error instead of a generic fetch failure, so the model knows the
          // content was retrieved but could not be saved.
          const reason = spillErrorReason((writeErr as NodeJS.ErrnoException)?.code);
          return {
            output: `Error: could not save binary content (${contentType}, ${formatSize(outBytes.length)}) to disk: ${reason}.`,
            isError: true,
          };
        }
      }

      // Convert HTML to markdown, leave other content types as-is
      let content: string;
      if (contentType.includes('text/html')) {
        content = htmlToMarkdown(body);
      } else {
        content = body;
      }

      const totalLength = content.length;
      if (textOffset > 0) {
        if (textOffset >= totalLength) {
          return { output: `No content at offset ${textOffset} (total length: ${totalLength} characters).` };
        }
        content = content.slice(textOffset);
      }
      content = spillToolText(context.cwd, 'web-fetch', content, MAX_RESPONSE_LENGTH);
      const endOffset = textOffset + Math.min(content.length, MAX_RESPONSE_LENGTH);
      const unread = totalLength - endOffset;
      if (unread > 0) {
        content += `\n\n[${unread.toLocaleString()} characters unread. Use offset=${endOffset} to continue reading.]`;
      }
      if (truncated) {
        content += `\n\n[Download stopped at ${MAX_HTTP_BODY_BYTES} bytes; the rest of the response was not read.]`;
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
          // The summarizer processes UNTRUSTED fetched page content. It only needs to
          // emit text, so it runs under deny-all: a prompt-injection payload in the
          // page cannot coerce it into executing a tool (it never gets auto-allowed).
          const summarizeEngine = context._engineFactory({
            provider: context._providerConfig!,
            cwd: context.cwd,
            permissionMode: 'deny-all',
            maxTurns: 1,
            tokenBudget: 8000,
          });
          let summary = '';
          const summarizePrompt = `Given this web page content, answer the following question/instruction:\n\nQuestion: ${input.prompt}\n\nContent:\n${summaryWindow(output)}`;
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

      if (deadlineSignal.aborted) {
        return {
          output: `Error: request exceeded ${Math.round(deadlineMs / 1000)}s overall deadline`,
          isError: true,
        };
      }

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
