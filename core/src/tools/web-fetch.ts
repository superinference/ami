import * as fs from 'fs';
import { projectStateDir } from '../reference-journal';
import * as path from 'path';
import { URL } from 'url';
import { createRequire } from 'module';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validateUrlSafety, stripHtml, httpGet, MAX_HTTP_BODY_BYTES, EgressBlockedError } from './web-utils';
import { spillToolText, formatSize, spillErrorReason } from './spill-output';
import { isPreapprovedHost } from './web-preapproved';

const MAX_RESPONSE_LENGTH = 50000;
export const MAX_FETCH_DEADLINE_MS = 300_000;

/**
 * Upper bound on content handed to the secondary summarizer / returned verbatim
 * from a preapproved trusted-docs host (parity with Claude's MAX_MARKDOWN_LENGTH).
 * Preapproved docs under this size are returned raw (no lossy summarization pass).
 */
export const MAX_MARKDOWN_LENGTH = 100_000;

/**
 * Map an HTTP status code to its canonical reason phrase (e.g. 200 -> "OK",
 * 404 -> "Not Found"), so fetch results can surface a human-readable status
 * alongside the numeric code (parity with Claude's codeText). Returns '' for an
 * unknown code so callers can omit a trailing phrase.
 */
export function httpStatusText(code: number): string {
  const MAP: Record<number, string> = {
    200: 'OK',
    201: 'Created',
    202: 'Accepted',
    204: 'No Content',
    206: 'Partial Content',
    301: 'Moved Permanently',
    302: 'Found',
    303: 'See Other',
    304: 'Not Modified',
    307: 'Temporary Redirect',
    308: 'Permanent Redirect',
    400: 'Bad Request',
    401: 'Unauthorized',
    403: 'Forbidden',
    404: 'Not Found',
    405: 'Method Not Allowed',
    406: 'Not Acceptable',
    408: 'Request Timeout',
    409: 'Conflict',
    410: 'Gone',
    429: 'Too Many Requests',
    500: 'Internal Server Error',
    501: 'Not Implemented',
    502: 'Bad Gateway',
    503: 'Service Unavailable',
    504: 'Gateway Timeout',
  };
  return MAP[code] ?? '';
}

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
 * Evict ALL cached fetched web-page content and reset the byte accounting.
 * Wired into the /clear session-reset handler (Claude parity: clearSessionCaches)
 * so a fresh conversation never reuses stale fetched pages — a bounded-memory
 * boundary and a cross-session data-retention hygiene guard.
 */
export function clearWebFetchCache(): void {
  fetchCache.clear();
  cacheTotalBytes = 0;
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

/**
 * Deny-by-default binary content-type detection: treat a fetched response as
 * BINARY (persist raw bytes to disk) UNLESS it is provably textual. Only a small
 * whitelist of text-ish types is considered safe to decode into the model's
 * context — `text/*`, the structured-text `+json`/`+xml` suffixes (and their
 * bare `application/json` / `application/xml` forms), `application/javascript`,
 * and form-urlencoded. Everything else — including UNKNOWN or novel types
 * (`font/woff2`, `video/mp4`, `application/wasm`, …) — is binary. The previous
 * allow-list (image/* + zip + octet-stream) silently leaked the raw bytes of any
 * unrecognized binary type into the conversation. Parity with Claude's
 * `isBinaryContentType` (mcpOutputStorage.ts). An empty/absent header is treated
 * as text so a server that omits Content-Type still gets the inline path.
 */
export function isBinaryContentType(contentType: string | undefined): boolean {
  if (!contentType) return false;
  const mt = (contentType.split(';')[0] ?? '').trim().toLowerCase();
  if (mt.startsWith('text/')) return false;
  // Structured text delivered under an application/ type. Suffix or exact match
  // (not substring) so 'openxmlformats' office types stay binary.
  if (mt.endsWith('+json') || mt === 'application/json') return false;
  if (mt.endsWith('+xml') || mt === 'application/xml') return false;
  if (mt.startsWith('application/javascript')) return false;
  if (mt === 'application/x-www-form-urlencoded') return false;
  return true;
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

/**
 * Build the secondary (summarizer) model prompt for UNTRUSTED fetched content.
 * Mirrors Claude's makeSecondaryModelPrompt: a preapproved/trusted domain (one the
 * caller explicitly allow-listed) gets a permissive "include code/doc excerpts"
 * instruction, while an untrusted domain gets strict copyright/quote guardrails
 * (125-char quote cap, no verbatim reproduction, no song lyrics, no legal
 * commentary) so prompt-injected content on a hostile page cannot coax the
 * summarizer into reproducing large copyrighted passages.
 */
export function makeSecondaryModelPrompt(
  content: string,
  prompt: string,
  isPreapprovedDomain: boolean,
): string {
  const guidelines = isPreapprovedDomain
    ? 'Provide a concise response based on the content above. Include relevant details, code examples, and documentation excerpts as needed.'
    : [
        'Provide a concise response based only on the content above. In your response:',
        ' - Enforce a strict 125-character maximum for quotes from any source document. Open Source Software is ok as long as we respect the license.',
        ' - Use quotation marks for exact language from articles; any language outside of the quotation should never be word-for-word the same.',
        ' - You are not a lawyer and never comment on the legality of your own prompts and responses.',
        ' - Never produce or reproduce exact song lyrics.',
      ].join('\n');
  return `Given this web page content, answer the following question/instruction:\n\nQuestion: ${prompt}\n\nContent:\n${content}\n\n${guidelines}`;
}

export const webFetchTool: ToolDefinition = {
  name: 'web_fetch',
  description:
    'IMPORTANT: web_fetch WILL FAIL for authenticated or private URLs. Before using it, check whether the URL points to an authenticated service (e.g. Google Docs, Confluence, Jira, GitHub); if so, prefer a specialized MCP tool that provides authenticated access.\n' +
    'Fetch content from a URL as text (HTML is auto-stripped) and, when a prompt is given, answer it using a small, fast model.\n' +
    'Usage notes:\n' +
    '  - If an MCP-provided web-fetch tool is available, prefer it over this one — it may have fewer restrictions.\n' +
    '  - HTTP URLs are automatically upgraded to HTTPS.\n' +
    '  - A self-cleaning 15-minute cache speeds up repeated fetches of the same URL.\n' +
    '  - Large pages are summarized against the prompt by a small model; the raw content may not be returned verbatim.\n' +
    '  - If a URL redirects to a different host, protocol, or port, the tool reports the redirect instead of returning the content; re-fetch with the redirect URL to continue.\n' +
    '  - For GitHub URLs, prefer the gh CLI via Bash (e.g. gh pr view, gh issue view, gh api) instead of fetching.\n' +
    '  - NOT for binary files (zip, tar, images) — use bash with curl -O for binary downloads.',
  // Intent synonyms NOT present in the name/description, so ToolSearch can surface
  // web_fetch when the model searches by capability rather than exact wording
  // (parity with Claude's Tool.searchHint).
  searchHint: 'scrape crawl retrieve webpage website visit open link read article rest api endpoint',
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
    required: ['url', 'prompt'],
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

    // Reject non-publicly-resolvable single-label hostnames (e.g. "intranet",
    // "router") BEFORE any DNS lookup. A hostname with no dot is almost always an
    // internal short name, never a public site, so refuse it structurally (parity
    // with Claude's validateURL `parts.length < 2` guard). `localhost` is left to
    // the SSRF layer (which reports it as a blocked private host), and the whole
    // check is skipped when localhost is explicitly permitted for testing.
    if (!context._allowLocalhostForTesting && host !== 'localhost' && !host.includes('.')) {
      return {
        output: `Error: Hostname "${host}" is not a publicly-resolvable domain (hostname must contain a dot).`,
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

      // Deny-by-default: anything not provably text is persisted as raw bytes so
      // non-text payloads never leak into the model context (see isBinaryContentType).
      if (isBinaryContentType(contentType)) {
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

      // Build the result. The status line carries the human-readable reason
      // phrase (e.g. "200 OK") and a separate "Received <size>" line reports the
      // bytes actually fetched, so a fetch result is self-describing at a glance
      // (parity with Claude's "Received 12.3 KB (200 OK)" confirmation).
      const statusText = httpStatusText(statusCode);
      const receivedBytes = bodyBuffer ? bodyBuffer.length : Buffer.byteLength(body);
      const parts: string[] = [];
      parts.push(`URL: ${sanitizedUrl}`);
      parts.push(`Status: ${statusCode}${statusText ? ' ' + statusText : ''}`);
      parts.push(`Received: ${formatSize(receivedBytes)}`);
      parts.push(`Content-Type: ${contentType}`);
      if (prompt) {
        parts.push(`Prompt: ${prompt}`);
      }
      parts.push('');
      parts.push(content);

      let output = parts.join('\n');

      // A fetch to a curated trusted code/docs host (the built-in PREAPPROVED_HOSTS
      // knob) is treated as preapproved: its content is returned VERBATIM without a
      // lossy small-model summarization pass, as long as it fits under the markdown
      // ceiling (parity with Claude's isPreapprovedUrl raw-markdown passthrough).
      const preapprovedHost = isPreapprovedHost(host, parsedUrl.pathname);
      const rawPassthrough = preapprovedHost && totalLength < MAX_MARKDOWN_LENGTH;

      // Apply prompt to content via LLM summarization (like Claude's applyPromptToMarkdown)
      if (input.prompt && typeof input.prompt === 'string' && context._engineFactory && !rawPassthrough) {
        // A domain the caller explicitly allow-listed — OR one on the built-in
        // preapproved trusted-docs list — is treated as preapproved/trusted, which
        // relaxes the summarizer's copyright/quote guardrails (code-doc excerpts
        // are fine). Everything else gets the strict guidelines.
        const isPreapprovedDomain = preapprovedHost || !!(
          allowedDomains &&
          allowedDomains.length > 0 &&
          allowedDomains.some(d => host === d || host.endsWith('.' + d))
        );
        let summary = '';
        let summarizeFailed = false;
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
          const summarizePrompt = makeSecondaryModelPrompt(
            summaryWindow(output),
            input.prompt,
            isPreapprovedDomain,
          );
          for await (const event of summarizeEngine.submit(summarizePrompt)) {
            // Honor cancellation/deadline mid-stream: stop consuming the moment the
            // fetch signal aborts rather than accumulating a partial summary.
            if (fetchSignal.aborted) break;
            if (event.type === 'text_delta') summary += event.text;
          }
          summarizeEngine.shutdown?.();
        } catch {
          // An engine construction/stream failure must not lose the already-fetched
          // raw output — fall back to it. Cancellation is surfaced explicitly below.
          summarizeFailed = true;
        }
        // Surface cancellation/deadline during summarization as an error instead of
        // silently returning the un-summarized raw output (parity with Claude's
        // applyPromptToMarkdown, which re-checks signal.aborted after the model call
        // and throws rather than returning partial content).
        if (fetchSignal.aborted) {
          if (deadlineSignal.aborted) {
            return {
              output: `Error: request exceeded ${Math.round(deadlineMs / 1000)}s overall deadline`,
              isError: true,
            };
          }
          return { output: 'Fetch aborted.', isError: true };
        }
        if (!summarizeFailed && summary.trim()) output = summary.trim();
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

      // An egress-proxy allow-list block is not an origin error — the request
      // never left the network boundary. Report it distinctly with the actionable
      // remedy (allow-list the domain in the egress policy) instead of a generic
      // "Error fetching …" that reads like the site itself failed.
      if (err instanceof EgressBlockedError) {
        return {
          output: `Error: fetching "${sanitizedUrl}" was blocked by the network egress proxy — the domain "${err.domain}" is not on the egress allow-list. Add it to the egress policy to permit this fetch.`,
          isError: true,
        };
      }

      return {
        output: `Error fetching ${sanitizedUrl}: ${message}`,
        isError: true,
      };
    }
  },
};
