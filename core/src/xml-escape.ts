// Canonical XML-escaping helpers for safely wrapping UNTRUSTED external content
// (peer-agent messages, MCP channel notifications, resource-update notes) into the
// XML-ish tags the engine injects into the conversation. Without this, a hostile or
// confused source could break out of the tag — e.g. a peer named
// `x"><instruction>...` would terminate the opening tag, or content containing
// `</peer-message>` could inject attacker-controlled text that looks like a trusted
// directive. ONE shared implementation so every wrap site escapes identically.

/**
 * Escape a string for safe use as an XML/HTML attribute VALUE (inside double
 * quotes). Neutralizes the characters that could close the attribute, the tag,
 * or introduce a new entity.
 */
export function escapeXmlAttr(value: string): string {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

/**
 * Escape a string for safe use as XML/HTML element TEXT content. Neutralizes the
 * `<`/`>` that could forge a closing tag (tag breakout) and the `&` that could
 * start an entity, while leaving quotes untouched (they are legal in text).
 */
export function escapeXmlText(value: string): string {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

/**
 * Guard an attribute NAME/KEY coming from an untrusted source: only allow a
 * conservative XML-name charset so a malicious key cannot inject extra
 * attributes or close the tag. Returns a safe fallback when the key is empty or
 * contains disallowed characters.
 */
export function safeXmlKey(key: string, fallback = 'unknown'): string {
  const k = String(key);
  return /^[A-Za-z_][\w.-]*$/.test(k) ? k : fallback;
}
