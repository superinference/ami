// Canonical XML-escaping helpers for safely wrapping UNTRUSTED external content
// (peer-agent messages, MCP channel notifications, resource-update notes) into the
// XML-ish tags the engine injects into the conversation. Without this, a hostile or
// confused source could break out of the tag — e.g. a peer named
// `x"><instruction>...` would terminate the opening tag, or content containing
// `</peer-message>` could inject attacker-controlled text that looks like a trusted
// directive. ONE shared implementation so every wrap site escapes identically.

// The five predefined XML entities, mapped to their literal characters. Used by the
// round-trip unescaper. (These are the only named entities defined by XML 1.0 itself;
// anything else would need a DTD, which untrusted prompt content must never carry.)
const XML_NAMED_ENTITIES: Record<string, string> = {
  amp: '&',
  lt: '<',
  gt: '>',
  quot: '"',
  apos: "'",
};

// Characters that are ILLEGAL in an XML 1.0 document and cannot be represented even
// as numeric character references: the C0 control range except TAB (#x9), LF (#xA)
// and CR (#xD), plus the two permanently-unassigned noncharacters U+FFFE / U+FFFF.
// If untrusted content carries e.g. a NUL byte, embedding it raw (or as `&#0;`) yields
// a document a conforming parser must reject — a denial-of-service / smuggling vector.
// We drop these outright before escaping. (DEL and U+0080-U+009F are legal in XML 1.0,
// so they are preserved.)
const INVALID_XML_CHARS = /[\u0000-\u0008\u000B\u000C\u000E-\u001F\uFFFE\uFFFF]/g;

// A lone (unpaired) UTF-16 surrogate code unit — a high surrogate not followed by a
// low one, or a low surrogate not preceded by a high one. These are not valid Unicode
// scalar values and make an XML document ill-formed, so they are stripped while
// well-formed surrogate PAIRS (real astral characters, e.g. emoji) are preserved.
const LONE_SURROGATE =
  /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g;

/**
 * [FUNCTIONAL] Remove characters that cannot legally appear in an XML 1.0 document
 * (disallowed C0 control chars, the U+FFFE/U+FFFF noncharacters, and unpaired
 * surrogates). Escaping alone is insufficient for these — `&#0;` is still an invalid
 * XML character reference — so untrusted content must have them stripped. Valid
 * whitespace (TAB/LF/CR) and well-formed astral characters are preserved.
 */
export function stripInvalidXmlChars(value: string): string {
  return String(value).replace(INVALID_XML_CHARS, '').replace(LONE_SURROGATE, '');
}

/**
 * Escape a string for safe use as an XML/HTML attribute VALUE (inside double
 * quotes). Neutralizes the characters that could close the attribute, the tag,
 * or introduce a new entity.
 *
 * [FUNCTIONAL] Also (a) strips XML-illegal characters via {@link stripInvalidXmlChars}
 * and (b) escapes TAB/LF/CR as numeric character references. Per the XML
 * attribute-value normalization rules, a raw TAB/newline/CR in an attribute is
 * silently folded to a space by the parser — which both corrupts round-tripped data
 * and can mask an injected payload — so they are preserved explicitly as `&#9;`,
 * `&#10;`, `&#13;`.
 */
export function escapeXmlAttr(value: string): string {
  return stripInvalidXmlChars(String(value))
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;')
    .replace(/\t/g, '&#9;')
    .replace(/\n/g, '&#10;')
    .replace(/\r/g, '&#13;');
}

/**
 * Escape a string for safe use as XML/HTML element TEXT content. Neutralizes the
 * `<`/`>` that could forge a closing tag (tag breakout) and the `&` that could
 * start an entity, while leaving quotes untouched (they are legal in text).
 *
 * [FUNCTIONAL] Also strips XML-illegal characters via {@link stripInvalidXmlChars}
 * so a NUL byte or lone surrogate in untrusted content cannot produce a document a
 * conforming parser must reject.
 */
export function escapeXmlText(value: string): string {
  return stripInvalidXmlChars(String(value))
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

/**
 * [FUNCTIONAL] Reverse {@link escapeXmlText} / {@link escapeXmlAttr}: decode the five
 * predefined XML entities and decimal (`&#60;`) / hexadecimal (`&#x3C;`) numeric
 * character references back to their literal characters. Enables a round trip
 * (`unescapeXml(escapeXmlAttr(s)) === s` for any `s` with no XML-illegal chars).
 *
 * Unknown or malformed entities are left verbatim (an unterminated `&foo` or an
 * out-of-range code point like `&#x110000;` is NOT decoded), so this never invents
 * characters that were not encoded. A single left-to-right pass means an escaped
 * ampersand (`&amp;lt;`) correctly decodes to the literal text `&lt;`, not to `<`.
 */
export function unescapeXml(value: string): string {
  return String(value).replace(
    /&(#[Xx][0-9A-Fa-f]+|#[0-9]+|amp|lt|gt|quot|apos);/g,
    (match, body: string) => {
      if (body[0] === '#') {
        const codePoint =
          body[1] === 'x' || body[1] === 'X'
            ? parseInt(body.slice(2), 16)
            : parseInt(body.slice(1), 10);
        if (!Number.isFinite(codePoint) || codePoint < 0 || codePoint > 0x10ffff) {
          return match;
        }
        try {
          return String.fromCodePoint(codePoint);
        } catch {
          return match;
        }
      }
      return XML_NAMED_ENTITIES[body] ?? match;
    },
  );
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
