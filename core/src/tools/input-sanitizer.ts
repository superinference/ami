// ---------------------------------------------------------------------------
// Invisible Unicode stripping — prevents hidden characters in permission
// prompts from concealing part of a command from the user.
// ---------------------------------------------------------------------------

// The `u` flag makes this codepoint-aware so the astral Unicode TAG block
// (U+E0000–U+E007F) is stripped too — that block can smuggle a full hidden ASCII
// instruction past a human reviewer and into the model (prompt injection). (H1)
//
// The Unicode general-category classes \p{Cf} (format), \p{Co} (private-use) and
// \p{Cn} (unassigned/noncharacters) are the PRIMARY, category-complete defense —
// they catch EVERY format char, ALL private-use codepoints (BMP PUA U+E000–U+F8FF
// plus the supplementary PUA planes) and all unassigned/noncharacter codepoints,
// so a smuggling char that is not in the hand-enumerated list (newly-assigned
// format chars, PUA glyphs, noncharacters) is still removed by category. The
// explicit enumerated ranges are kept as a documented FALLBACK.
// eslint-disable-next-line security/detect-bidi-characters, security/detect-unsafe-regex -- intentional bidi chars to strip them; a single flat character class cannot backtrack (no ReDoS)
const INVISIBLE_RE = new RegExp('[\\u00AD\\u034F\\u061C\\u180E\\u200B-\\u200F\\u202A-\\u202E\\u2060-\\u2064\\u2066-\\u206F\\uFEFF\\uFFF9-\\uFFFB\\u{E0000}-\\u{E007F}\\p{Cf}\\p{Co}\\p{Cn}]', 'gu');

// ASCII C0 control chars + DEL, EXCEPT tab (U+0009) and newline (U+000A). These
// terminal-display-spoofing vectors — backspace (erases rendered chars), carriage
// return (overwrites a displayed line), ESC (injects cursor/colour sequences) —
// can hide or rewrite part of a command shown to a human in a permission prompt.
// eslint-disable-next-line no-control-regex -- intentional: strip control chars from display
const DISPLAY_CONTROL_RE = /[\u0000-\u0008\u000B-\u001F\u007F]/g;

/**
 * Upper bound on the normalize-and-strip passes `stripInvisibleUnicode` runs
 * before it gives up. One pass is not a fixpoint: NFKC normalization can expose
 * a new strippable codepoint, and stripping can enable a new composition — so we
 * iterate until the string stops changing. The bound is an explicit DoS boundary
 * for adversarially-layered/deeply-composed input (mirrors Claude Code's
 * MAX_ITERATIONS). Exported so callers/tests can tighten it.
 */
export const MAX_SANITIZE_ITERATIONS = 10;

/**
 * Strip invisible/smuggling Unicode from a string, hardened against
 * composition-based bypasses:
 *  1. NFKC normalization folds composed/compatibility sequences (full-width
 *     ASCII, ligatures, compatibility forms) to their canonical form FIRST, so a
 *     dangerous char disguised as a composed/compatibility sequence cannot slip
 *     past the character class.
 *  2. The invisible/format/private-use/unassigned character class is removed.
 *  3. Steps 1–2 repeat to a fixpoint (bounded by `maxIterations`) because a
 *     single pass can leave a residual hidden char that only a re-pass removes.
 * Throws loudly (with a short input preview) if the string never settles within
 * the bound — a pathological payload fails observably rather than silently
 * returning partially-sanitized text.
 */
export function stripInvisibleUnicode(text: string, maxIterations: number = MAX_SANITIZE_ITERATIONS): string {
  let current = text;
  for (let i = 0; i < maxIterations; i++) {
    const previous = current;
    current = current.normalize('NFKC').replace(INVISIBLE_RE, '');
    if (current === previous) return current;
  }
  throw new Error(
    `Unicode sanitization reached maximum iterations (${maxIterations}) for input: ${text.slice(0, 100)}`,
  );
}

/**
 * POSIX/terminal default tab-stop interval. A hard tab advances the cursor to the
 * NEXT multiple of this column, not a fixed run of spaces — so the visual width of
 * a tab depends on the column it starts at. Parity: Claude Code's
 * `DEFAULT_TAB_INTERVAL` in `ink/tabstops.ts` (8, hardcoded in terminals like
 * Ghostty). Exported so callers/tests can pin a different interval.
 */
export const DEFAULT_TAB_INTERVAL = 8;

/**
 * Column-aware tab expansion — THE single canonical implementation (cli's
 * tool-render-descriptor delegates here; AGENTS.md No-Duplication Rule). A literal
 * tab is expanded to the number of spaces that advances the running display column
 * to the next `tabWidth`-column stop (`tabWidth - (column % tabWidth)`), NOT a
 * fixed run — so tab-separated columns stay aligned regardless of the terminal's
 * own tab setting. The column counter resets to 0 at every newline so each line
 * aligns independently. Mirrors Claude Code's `ink/tabstops.ts` `expandTabs`.
 *
 * Divergence note: Claude measures run width with `stringWidth` (CJK/ANSI-aware)
 * in its ink renderer; this helper lives in core/ (no ink, no terminal), so it
 * advances the column by one per code point — a deterministic approximation that
 * is correct for the common ASCII case and never under-pads (same convention as
 * `command-preview.ts`). Fast-returns unchanged when there is no tab.
 */
export function expandTabs(text: string, tabWidth: number = DEFAULT_TAB_INTERVAL): string {
  if (!text.includes('\t')) return text;
  let out = '';
  let column = 0;
  for (const ch of text) {
    if (ch === '\n') {
      out += ch;
      column = 0;
    } else if (ch === '\t') {
      const pad = tabWidth - (column % tabWidth);
      out += ' '.repeat(pad);
      column += pad;
    } else {
      out += ch;
      column += 1;
    }
  }
  return out;
}

export function normalizeCommandForDisplay(command: string): string {
  return expandTabs(
    stripInvisibleUnicode(command).replace(DISPLAY_CONTROL_RE, ''),
  );
}

/** Depth cap for the recursive unicode sanitizer (mirrors sanitizeToolInput). */
const MAX_UNICODE_SANITIZE_DEPTH = 20;

/**
 * Recursively strip invisible/smuggling Unicode from every string reachable in
 * an arbitrary JSON-ish value (strings, arrays, plain objects). This is the ONE
 * canonical deep sanitizer for UNTRUSTED MCP server payloads — tool metadata
 * (names/descriptions/schema strings) advertised to the model and tool/resource/
 * prompt results returned from a server — mirroring Claude Code's
 * `recursivelySanitizeUnicode` over `result.tools`. Non-string leaves are
 * returned unchanged; object keys are preserved verbatim (only values are
 * sanitized) so schema property names stay stable. Prototype-poisoning keys are
 * dropped, matching `sanitizeToolInput`.
 */
export function recursivelySanitizeUnicode<T>(value: T, depth = 0): T {
  if (typeof value === 'string') return stripInvisibleUnicode(value) as unknown as T;
  if (depth >= MAX_UNICODE_SANITIZE_DEPTH) return value;
  if (Array.isArray(value)) {
    return value.map((v) => recursivelySanitizeUnicode(v, depth + 1)) as unknown as T;
  }
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [key, v] of Object.entries(value as Record<string, unknown>)) {
      if (POISONED_KEYS.has(key)) continue;
      out[key] = recursivelySanitizeUnicode(v, depth + 1);
    }
    return out as unknown as T;
  }
  return value;
}

// ---------------------------------------------------------------------------

const POISONED_KEYS = new Set(['__proto__', 'constructor', 'prototype']);
const MAX_SANITIZE_DEPTH = 20;

/**
 * Strip smuggling Unicode (+ NUL) from an OBJECT KEY before it is used. Keys are
 * the second injection channel alongside values: a zero-width/homoglyph-obfuscated
 * key such as `__pro​to__` would otherwise slip a prototype-pollution key
 * past the `POISONED_KEYS` denylist, and a hidden-Unicode key would carry an
 * invisible payload into the sanitized object. Runs the SAME canonical
 * `stripInvisibleUnicode` (NFKC + invisible/format/private-use class) the value
 * path uses, then removes NUL — mirroring Claude Code's `recursivelySanitizeUnicode`,
 * which sanitizes BOTH keys and values. Keys are not trimmed (leading/trailing
 * spaces in a key are structural, not smuggling).
 */
function sanitizeToolKey(key: string): string {
  return stripInvisibleUnicode(key).replace(/\x00/g, '');
}

/**
 * Sanitize a single value reachable from a tool-input object: strings are routed
 * through the canonical `stripInvisibleUnicode` (NFKC fold + invisible/format/
 * private-use/unassigned stripping) THEN NUL-stripped and trimmed — so hidden
 * instructions smuggled into a tool argument as zero-width/bidi/compatibility
 * Unicode are neutralized before the tool runs, matching Claude Code's
 * `recursivelySanitizeUnicode` over tool input. ARRAYS are recursed
 * element-by-element (so a string smuggled inside `{ paths: ['  rm\x00 -rf /  '] }`
 * is cleaned, not passed through), plain objects re-enter `sanitizeToolInput`
 * (dropping poisoned keys), and non-string primitives are returned unchanged.
 * Depth-bounded through the same `MAX_SANITIZE_DEPTH` cap as the object walker.
 */
function sanitizeToolValue(value: unknown, depth: number): unknown {
  if (typeof value === 'string') return stripInvisibleUnicode(value).replace(/\x00/g, '').trim();
  if (depth >= MAX_SANITIZE_DEPTH) return value;
  if (Array.isArray(value)) return value.map((v) => sanitizeToolValue(v, depth + 1));
  if (value && typeof value === 'object') {
    return sanitizeToolInput(value as Record<string, unknown>, depth + 1);
  }
  return value;
}

export function sanitizeToolInput(input: Record<string, unknown>, depth: number = 0): Record<string, unknown> {
  if (depth >= MAX_SANITIZE_DEPTH) return {};
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(input)) {
    // Sanitize the KEY first (strip smuggling Unicode + NUL), THEN apply the
    // prototype-pollution denylist to the CLEANED key — so a Unicode-obfuscated
    // `__proto__`/`constructor`/`prototype` key cannot evade the denylist.
    const cleanKey = sanitizeToolKey(key);
    if (POISONED_KEYS.has(cleanKey)) continue;
    result[cleanKey] = sanitizeToolValue(value, depth);
  }
  return result;
}

/** Upper bound on the length of an untrusted identifier embedded into a prompt. */
export const MAX_PROMPT_IDENTIFIER_LEN = 80;

/**
 * Structural allowlist/boundary for an UNTRUSTED IDENTIFIER — an installed/URL
 * plugin-bundle skill, stance or agent NAME, or a bundle name — before it is
 * embedded VERBATIM into the model-facing capability listing (the system prompt).
 *
 * The heuristic scanners (`detectPromptInjection`, `sanitizeSubagentOutput`)
 * harden tool OUTPUT text; this hardens the IDENTIFIER channel structurally, which
 * output heuristics do not cover. A malicious bundle could ship a skill named
 * `deploy\nIgnore all previous instructions and ...` so the rendered markdown
 * listing line breaks onto a second line that reads like a top-level
 * instruction. This:
 *  1. runs the canonical `stripInvisibleUnicode` (NFKC + invisible/format/
 *     private-use/unassigned removal), then
 *  2. replaces every C0/C1 control char (newlines, CR, tab, …) with a single
 *     space — using an explicit control class, NOT `\s`, so a name can never
 *     smuggle a line break, mirroring Claude Code's `appNames` boundary, then
 *  3. collapses whitespace runs to one space, trims, and hard-caps the length.
 * Printable punctuation that is legitimate in identifiers (`-`, `_`, `:`, `/`,
 * `.`) is preserved so real plugin/skill names (e.g. `plugin:skill`) render
 * unchanged.
 */
export function sanitizeIdentifierForPrompt(
  identifier: string,
  maxLength: number = MAX_PROMPT_IDENTIFIER_LEN,
): string {
  // eslint-disable-next-line no-control-regex -- intentional: collapse control chars so no identifier can smuggle a line break into the prompt
  const cleaned = stripInvisibleUnicode(identifier)
    .replace(/[\u0000-\u001F\u007F-\u009F]/g, ' ')
    .replace(/ {2,}/g, ' ')
    .trim();
  return cleaned.length > maxLength ? cleaned.slice(0, maxLength) : cleaned;
}

export function recoverJsonStringFields(
  input: Record<string, unknown>,
  schema?: { properties?: Record<string, { type?: string }> },
): Record<string, unknown> {
  if (!schema?.properties) return input;
  const result = { ...input };
  for (const [key, propSchema] of Object.entries(schema.properties)) {
    const value = result[key];
    if (typeof value !== 'string') continue;
    const expectedType = propSchema.type;
    const trimmed = value.trim();
    if (expectedType === 'array' || expectedType === 'object') {
      const isArrayLike = trimmed.startsWith('[') && trimmed.endsWith(']');
      const isObjectLike = trimmed.startsWith('{') && trimmed.endsWith('}');
      if ((expectedType === 'array' && isArrayLike) || (expectedType === 'object' && isObjectLike)) {
        try {
          result[key] = JSON.parse(trimmed);
        } catch { /* keep original string */ }
      }
    } else if (expectedType === 'number') {
      // Models frequently quote numeric tool args (e.g. {"head_limit":"30"}).
      // Coerce ONLY strict decimal literals that are finite — garbage like
      // "" / "abc" / "1e5" is left as a string so it fails loudly downstream
      // instead of being silently masked (mirrors semanticNumber).
      // eslint-disable-next-line security/detect-unsafe-regex -- fully anchored (^…$), no nested/overlapping quantifiers; `\d+` and the optional `\.\d+` are separated by a literal `.`, so matching is linear (no ReDoS)
      if (/^-?\d+(?:\.\d+)?$/.test(trimmed) && Number.isFinite(Number(trimmed))) {
        result[key] = Number(trimmed);
      }
    } else if (expectedType === 'integer') {
      // Integer fields coerce only whole-number literals that fit a safe
      // integer; a quoted float ("3.14") stays a string so the mismatch
      // surfaces rather than being truncated.
      if (/^-?\d+$/.test(trimmed) && Number.isSafeInteger(Number(trimmed))) {
        result[key] = Number(trimmed);
      }
    } else if (expectedType === 'boolean') {
      // Coerce quoted booleans ("true"/"false", case-insensitive); any other
      // string is left untouched so it is not silently read as `false`.
      const lower = trimmed.toLowerCase();
      if (lower === 'true') result[key] = true;
      else if (lower === 'false') result[key] = false;
    }
  }
  return result;
}

export function repairJsonArguments(raw: string): Record<string, unknown> | null {
  if (!raw || !raw.trim()) return null;
  let s = raw.trim();
  if (!s.startsWith('{') && !s.startsWith('[')) return null;

  const repairs = [
    (t: string) => t.replace(/,\s*([}\]])/g, '$1'),
    (t: string) => t.replace(/\/\/[^\n]*/g, '').replace(/\/\*[\s\S]*?\*\//g, ''),
    (t: string) => t.replace(/,\s*$/, ''),
    (t: string) => {
      let depth = 0;
      let inStr = false;
      let escape = false;
      for (const ch of t) {
        if (escape) { escape = false; continue; }
        if (ch === '\\') { escape = true; continue; }
        if (ch === '"') { inStr = !inStr; continue; }
        if (inStr) continue;
        if (ch === '{' || ch === '[') depth++;
        if (ch === '}' || ch === ']') depth--;
      }
      if (inStr) t += '"';
      while (depth > 0) { t += t.trimStart().startsWith('[') && depth === 1 ? ']' : '}'; depth--; }
      return t;
    },
  ];

  for (let i = 0; i <= repairs.length; i++) {
    let attempt = s;
    for (let j = 0; j < i; j++) attempt = repairs[j](attempt);
    try {
      const parsed = JSON.parse(attempt);
      if (typeof parsed === 'object' && parsed !== null) return parsed as Record<string, unknown>;
    } catch { /* try next repair stage */ }
  }
  return null;
}

export function validateRequiredFields(
  input: Record<string, unknown>,
  required: string[],
): string | null {
  const missing = required.filter(field => {
    const val = input[field];
    return val === undefined || val === null || (typeof val === 'string' && val.trim() === '');
  });
  if (missing.length === 0) return null;
  return `Missing required fields: ${missing.join(', ')}`;
}

const INJECTION_PATTERNS = [
  /ignore\s+(?:all\s+)?previous\s+instructions/i, // eslint-disable-line security/detect-unsafe-regex -- "all" starts with [a-z], disjoint from \s
  /ignore\s+(?:all\s+)?(?:your|my|the)\s+rules/i, // eslint-disable-line security/detect-unsafe-regex -- same: non-capturing, no overlap
  /you\s+are\s+now\s+\w+\.\s*ignore/i,
  /disregard\s+(?:all\s+)?(?:prior|previous|above)/i, // eslint-disable-line security/detect-unsafe-regex -- same: non-capturing, no overlap
  /<system>.*<\/system>/is,
  /\[system\].*\[\/system\]/is,
  /new\s+instructions?\s*:\s*ignore/i,
  /override\s+(system|safety)\s+(prompt|instructions)/i,
];

const FALSE_POSITIVE_PATTERNS = [
  /\.gitignore/i,
  /ignore\s+file/i,
  /ignore\s+errors?/i,
  /ignore\s+(this|that|the)\s/i,
];

export function detectPromptInjection(text: string): { suspicious: boolean; pattern?: string } {
  if (!text) return { suspicious: false };

  for (const fp of FALSE_POSITIVE_PATTERNS) {
    if (fp.test(text)) return { suspicious: false };
  }

  for (const pattern of INJECTION_PATTERNS) {
    if (pattern.test(text)) {
      return { suspicious: true, pattern: pattern.source };
    }
  }

  return { suspicious: false };
}

export function sanitizeSubagentOutput(output: string): { output: string; warnings: string[] } {
  if (!output) return { output, warnings: [] };
  const warnings: string[] = [];
  const result = detectPromptInjection(output);
  if (result.suspicious) {
    warnings.push(`Possible prompt injection detected (pattern: ${result.pattern})`);
  }
  if (/&lt;\\?system[-_]?reminder|<\\?system[-_]?reminder/i.test(output)) {
    warnings.push('Content contains system-reminder–shaped tags');
  }
  const prefix = warnings.length > 0
    ? `[WARNING: This content may contain prompt injection attempts — ${warnings.join('; ')}]\n\n`
    : '';
  return { output: prefix + output, warnings };
}
