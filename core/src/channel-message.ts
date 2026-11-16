/**
 * Channel-message framing — serialize/parse the `<channel source="…" …>…</channel>`
 * wrapper an external channel (chat bridge, MCP notification server, plugin
 * channel) prepends to an inbound user message so the model (and the transcript)
 * can see the message's provenance.
 *
 * Parity: Claude Code's channel schema —
 *   - `services/mcp/channelNotification.ts` `wrapChannelMessage()` (producer)
 *   - `components/messages/UserChannelMessage.tsx` `CHANNEL_RE` / `USER_ATTR_RE`
 *     / `displayServerName()` (consumer).
 *
 * Self-contained core building block: it only depends on the shared XML-escape
 * helpers so a single escaping/round-trip implementation is reused rather than
 * re-written (AGENTS.md No-Duplication Rule).
 */

import type { ChannelMessageMeta } from './types';
import { escapeXmlAttr, unescapeXml } from './xml-escape';

/** The XML tag name of a channel wrapper. Parity: Claude's `CHANNEL_TAG`. */
export const CHANNEL_TAG = 'channel';

/**
 * Meta keys become XML attribute NAMES, so a crafted key like `x="" injected="y`
 * would break out of the attribute structure. Only accept keys that look like
 * plain identifiers — stricter than the XML spec (which also allows `:`/`.`/`-`),
 * but channel servers only send `chat_id`, `user`, `thread_ts`, `message_id` in
 * practice. Parity: Claude's `SAFE_META_KEY`.
 */
export const SAFE_CHANNEL_META_KEY = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

/**
 * Serialize a channel message. `source` is always written first; every safe meta
 * key is appended as an escaped attribute (unsafe keys are dropped). The body is
 * framed with one newline on each side so the parser can strip exactly that frame.
 * Parity: Claude's `wrapChannelMessage`.
 */
export function wrapChannelMessage(
  source: string,
  content: string,
  meta?: Record<string, string>,
): string {
  const attrs = Object.entries(meta ?? {})
    .filter(([k]) => SAFE_CHANNEL_META_KEY.test(k))
    .map(([k, v]) => ` ${k}="${escapeXmlAttr(v)}"`)
    .join('');
  return `<${CHANNEL_TAG} source="${escapeXmlAttr(source)}"${attrs}>\n${content}\n</${CHANNEL_TAG}>`;
}

// `source` is always first (the producer writes it first); the trailing group
// captures any remaining attributes verbatim for a second pass. The body allows
// an optional single framing newline on each side (matching `wrapChannelMessage`).
const CHANNEL_RE = new RegExp(
  `<${CHANNEL_TAG}\\s+source="([^"]*)"([^>]*)>\\n?([\\s\\S]*?)\\n?</${CHANNEL_TAG}>`,
);
// Parse one `key="value"` attribute pair at a time from the remaining-attrs tail.
const ATTR_RE = /([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"/g;

/**
 * Parse a channel wrapper out of `text`. Returns the structured
 * {@link ChannelMessageMeta} (source + decoded attributes + body), or `null` when
 * `text` is not a channel message. Attribute values are XML-unescaped so they
 * round-trip through {@link wrapChannelMessage}. Parity: Claude's `CHANNEL_RE` +
 * `USER_ATTR_RE` extraction in `UserChannelMessage`.
 */
export function parseChannelMessage(text: string): ChannelMessageMeta | null {
  const m = CHANNEL_RE.exec(text);
  if (!m) return null;
  const source = unescapeXml(m[1]);
  const attrs: Record<string, string> = {};
  ATTR_RE.lastIndex = 0;
  let a: RegExpExecArray | null;
  while ((a = ATTR_RE.exec(m[2])) !== null) {
    attrs[a[1]] = unescapeXml(a[2]);
  }
  return {
    source,
    user: attrs.user,
    attrs,
    content: m[3],
  };
}

/**
 * Plugin-provided channel servers get names like `plugin:slack-channel:slack`;
 * show just the leaf segment after the last `:`. Parity: Claude's
 * `displayServerName`.
 */
export function displayChannelServerName(name: string): string {
  const i = name.lastIndexOf(':');
  return i === -1 ? name : name.slice(i + 1);
}

/** True when `text` begins a channel wrapper (cheap guard before a full parse). */
export function isChannelMessage(text: string): boolean {
  return CHANNEL_RE.test(text);
}
