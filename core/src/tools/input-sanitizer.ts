// ---------------------------------------------------------------------------
// Invisible Unicode stripping — prevents hidden characters in permission
// prompts from concealing part of a command from the user.
// ---------------------------------------------------------------------------

// eslint-disable-next-line security/detect-bidi-characters -- intentional: this regex MUST contain bidi chars to strip them
const INVISIBLE_RE = new RegExp('[\\u00AD\\u034F\\u061C\\u180E\\u200B-\\u200F\\u202A-\\u202E\\u2060-\\u2064\\u2066-\\u206F\\uFEFF\\uFFF9-\\uFFFB]', 'g');

export function stripInvisibleUnicode(text: string): string {
  return text.replace(INVISIBLE_RE, '');
}

export function normalizeCommandForDisplay(command: string): string {
  return stripInvisibleUnicode(command).replace(/\t/g, '    ');
}

// ---------------------------------------------------------------------------

const POISONED_KEYS = new Set(['__proto__', 'constructor', 'prototype']);
const MAX_SANITIZE_DEPTH = 20;

export function sanitizeToolInput(input: Record<string, unknown>, depth: number = 0): Record<string, unknown> {
  if (depth >= MAX_SANITIZE_DEPTH) return {};
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(input)) {
    if (POISONED_KEYS.has(key)) continue;
    if (typeof value === 'string') {
      result[key] = value.replace(/\x00/g, '').trim();
    } else if (value && typeof value === 'object' && !Array.isArray(value)) {
      result[key] = sanitizeToolInput(value as Record<string, unknown>, depth + 1);
    } else {
      result[key] = value;
    }
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
    if (expectedType !== 'array' && expectedType !== 'object') continue;
    const trimmed = value.trim();
    const isArrayLike = trimmed.startsWith('[') && trimmed.endsWith(']');
    const isObjectLike = trimmed.startsWith('{') && trimmed.endsWith('}');
    if ((expectedType === 'array' && isArrayLike) || (expectedType === 'object' && isObjectLike)) {
      try {
        result[key] = JSON.parse(trimmed);
      } catch { /* keep original string */ }
    }
  }
  return result;
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
