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

export function normalizeCommandForDisplay(command: string): string {
  return stripInvisibleUnicode(command)
    .replace(DISPLAY_CONTROL_RE, '')
    .replace(/\t/g, '    ');
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
 * Sanitize a single value reachable from a tool-input object: strings are
 * NUL-stripped and trimmed, ARRAYS are recursed element-by-element (so a string
 * smuggled inside `{ paths: ['  rm\x00 -rf /  '] }` is cleaned, not passed
 * through), plain objects re-enter `sanitizeToolInput` (dropping poisoned keys),
 * and non-string primitives are returned unchanged. Depth-bounded through the
 * same `MAX_SANITIZE_DEPTH` cap as the object walker.
 */
function sanitizeToolValue(value: unknown, depth: number): unknown {
  if (typeof value === 'string') return value.replace(/\x00/g, '').trim();
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
    if (POISONED_KEYS.has(key)) continue;
    result[key] = sanitizeToolValue(value, depth);
  }
  return result;
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
