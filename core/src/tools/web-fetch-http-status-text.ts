// Split from web-fetch.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { IncomingHttpHeaders } from 'http';
import { errorMessage } from '../credential-guard';

export const MAX_RESPONSE_LENGTH = 50000;

export const MAX_FETCH_DEADLINE_MS = 300_000;

/**
 * Hard upper bound on the in-process PDF text-extraction step. A malformed or
 * adversarial PDF (deeply nested objects, a decompression bomb, pathological
 * streams) can make the parser spin CPU or hang unboundedly, blocking the agent.
 * the reference time-bounds every external PDF operation (pdfinfo 10s, pdftoppm
 * 5s/120s); AMI parses in-process, so we race getText() against this deadline and
 * always release the native parser afterward (parity: bounded PDF parsing).
 */
export const PDF_PARSE_TIMEOUT_MS = 30_000;

/**
 * Minimal surface of the pdf-parse `PDFParse` instance this module uses. Declared
 * so {@link parsePdfWithTimeout} is unit-testable with a fake parser without
 * pulling in the native dependency.
 */
export interface PdfParserLike<T> {
  getText(): Promise<T>;
  destroy(): Promise<void> | void;
}

/**
 * Run `parser.getText()` with a hard timeout, ALWAYS releasing the parser's native
 * resources (`destroy()`) afterward — on success, on parse error, and on timeout.
 * Without the timeout a hostile PDF could hang the extraction forever; without the
 * guaranteed destroy, a timed-out parse would leak the native handle. On timeout it
 * rejects with a clear deadline error that the caller's PDF `catch` turns into the
 * HTML-fallback / "could not extract" path. ADDITIVE.
 */
export async function parsePdfWithTimeout<T>(
  parser: PdfParserLike<T>,
  timeoutMs: number,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`PDF parsing exceeded ${timeoutMs}ms deadline`)),
      timeoutMs,
    );
  });
  try {
    return await Promise.race([parser.getText(), timeout]);
  } finally {
    if (timer) clearTimeout(timer);
    try {
      await parser.destroy();
    } catch {
      /* best-effort release; a destroy failure must not mask the parse result */
    }
  }
}

/**
 * Compile a caller-supplied regex `filter` and keep only the page-text lines that
 * match it, so a verbose fetched page can be reduced to the relevant lines before
 * it is summarized/returned (parity with the reference's regex content-filter knob on
 * verbose-output tools; same contract as the bash/task-output `filter`). Returns
 * `{ error }` for an invalid pattern so the tool can surface it instead of silently
 * matching nothing. An empty/whitespace pattern is treated as "no filter".
 */
export function filterTextByPattern(
  text: string,
  pattern: string,
): { text: string } | { error: string } {
  let re: RegExp;
  try {
    // eslint-disable-next-line security/detect-non-literal-regexp -- caller-supplied content filter, same contract as task-output's `filter`; no network/eval side effect
    re = new RegExp(pattern);
  } catch (e) {
    const msg = errorMessage(e);
    return { error: `invalid filter regex ${JSON.stringify(pattern)}: ${msg}` };
  }
  const kept = text.split('\n').filter((line) => re.test(line));
  return { text: kept.join('\n') };
}

/**
 * Upper bound on content handed to the secondary summarizer / returned verbatim
 * from a preapproved trusted-docs host (parity with the reference's MAX_MARKDOWN_LENGTH).
 * Preapproved docs under this size are returned raw (no lossy summarization pass).
 */
export const MAX_MARKDOWN_LENGTH = 100_000;

/**
 * Map an HTTP status code to its canonical reason phrase (e.g. 200 -> "OK",
 * 404 -> "Not Found"), so fetch results can surface a human-readable status
 * alongside the numeric code (parity with the reference's codeText). Returns '' for an
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

export const MAX_URL_LENGTH = 2000;

export interface CacheEntry {
  content: string;
  timestamp: number;
  sizeBytes: number;
}

/**
 * Map a server-supplied Content-Type to a safe, fixed-vocabulary file extension.
 * The subtype of an attacker-controlled Content-Type header must NEVER be used
 * verbatim to name a file on disk — it can carry path separators, '..', or other
 * garbage that corrupts the spill filename. This whitelist returns a known-safe
 * extension and falls back to 'bin' for anything unrecognized (parity with
 * the reference's extensionForMimeType).
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
 * unrecognized binary type into the conversation. Parity with the reference's
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

export function htmlToMarkdown(html: string): string {
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
 * Mirrors the reference's makeSecondaryModelPrompt: a preapproved/trusted domain (one the
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

/**
 * Detect an enterprise network egress/allowlist proxy block and, if present,
 * build a machine-readable structured error for it. Many corporate egress
 * proxies answer a disallowed destination with HTTP 403 plus a proxy-origin
 * header (e.g. `X-Proxy-Error: blocked-by-allowlist`, Squid's `X-Squid-Error`),
 * which is indistinguishable from an ordinary site 403 unless the header is
 * inspected. When detected we return a JSON payload (`error_type: EGRESS_BLOCKED`)
 * so the model/caller can tell "your network blocked this" apart from "the site
 * forbade you" and react accordingly (parity with the reference's EgressBlockedError).
 * Returns null when the response is not an egress-proxy block.
 */
export function detectEgressBlock(
  statusCode: number,
  headers: IncomingHttpHeaders | undefined,
  domain: string,
): string | null {
  if (statusCode !== 403 || !headers) return null;
  const proxyError = headers['x-proxy-error'] ?? headers['x-squid-error'];
  if (proxyError === undefined) return null;
  return JSON.stringify({
    error_type: 'EGRESS_BLOCKED',
    domain,
    message: `Access to ${domain} is blocked by the network egress proxy.`,
  });
}
