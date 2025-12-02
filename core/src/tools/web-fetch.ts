import { URL } from 'url';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validateUrlSafety, stripHtml, httpGet } from './web-utils';

const MAX_RESPONSE_LENGTH = 50000;

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
        description: 'What to extract from the page (optional)',
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
    const url = input.url as string;
    const prompt = (input.prompt as string) ?? '';

    if (!url || url.trim().length === 0) {
      return { output: 'Error: url must not be empty.', isError: true };
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

    // SSRF protection: block private/metadata hostnames and IPs
    if (!context._allowLocalhostForTesting) {
      const ssrfError = await validateUrlSafety(url);
      if (ssrfError) {
        return { output: `Error: ${ssrfError}`, isError: true };
      }
    }

    try {
      const { body, statusCode, contentType } = await httpGet(
        url,
        context.abortSignal,
      );

      if (statusCode >= 400) {
        return {
          output: `Error: HTTP ${statusCode} when fetching ${sanitizedUrl}`,
          isError: true,
        };
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

      // Convert HTML to text, leave other content types as-is
      let content: string;
      if (contentType.includes('text/html')) {
        content = stripHtml(body);
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

      return { output: parts.join('\n') };
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
