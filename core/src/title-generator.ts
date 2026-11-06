import type { ProviderConfig } from './types';
import { streamChatCompletion } from './provider';

/**
 * A session title is rendered on a single-line separator/tab. Raw model output (or
 * the raw user-message fallback) can carry newlines — which wrap into and corrupt
 * the separator bar — and markdown markers (`# heading`, fences, `- list`, emphasis)
 * which read as noise on one line. Flatten to a clean single line: strip the common
 * markdown markers, then collapse every whitespace run (incl. newlines/tabs) to a
 * single space. Order matters — line-anchored markers are removed BEFORE newlines
 * collapse, or the `^` anchors would no longer match.
 */
export function sanitizeTitle(s: string): string {
  return s
    .replace(/```+[a-z0-9]*/gi, ' ')             // fenced code markers (+ optional lang)
    .replace(/`/g, '')                            // inline code backticks
    .replace(/^\s*#{1,6}\s+/gm, '')               // ATX headings (# .. ######)
    .replace(/^\s*>\s?/gm, '')                    // blockquote markers
    .replace(/^\s*(?:[-*+]|\d+\.)\s+/gm, '')      // unordered/ordered list markers
    .replace(/[*_]{1,3}(\S[^*_]*?)[*_]{1,3}/g, '$1') // bold/italic emphasis
    .replace(/\s+/g, ' ')                         // collapse all whitespace incl newlines
    .trim();
}

export async function generateTitle(
  userMessage: string,
  provider: ProviderConfig,
  compactionModel?: string,
  // Test seam (behaviour-preserving): defaults to the real streamer so callers
  // are unaffected; tests inject a deterministic async generator.
  _streamChatCompletion: typeof streamChatCompletion = streamChatCompletion,
): Promise<string> {
  const titleProvider: ProviderConfig = {
    ...provider,
    model: compactionModel || provider.model,
    maxTokens: 50,
  };

  const messages = [{
    role: 'user' as const,
    content: `Generate a concise title (max 50 chars, single line, no quotes, same language as the text) for this conversation:\n\n"${userMessage.slice(0, 500)}"`,
  }];

  let title = '';
  try {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), 30000);
    try {
      for await (const chunk of _streamChatCompletion(titleProvider, messages, [], ac.signal)) {
        if (chunk.type === 'content_delta' && chunk.text) title += chunk.text;
        if (chunk.type === 'done' || chunk.type === 'error') break;
      }
    } finally {
      clearTimeout(timer);
    }
  } catch {}

  // Clean up: remove quotes, thinking tags (paired and unclosed), then flatten to a
  // single markdown-free line (see sanitizeTitle) so it never wraps/corrupts the
  // separator bar or leaks raw markdown into the tab/title chrome.
  title = title.replace(/<think>[\s\S]*?<\/think>/g, '').replace(/<think>[\s\S]*$/g, '').replace(/^["']|["']$/g, '').trim();
  title = sanitizeTitle(title);
  return title.slice(0, 50) || sanitizeTitle(userMessage).slice(0, 50);
}
