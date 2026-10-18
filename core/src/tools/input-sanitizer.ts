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
  /ignore\s+(all\s+)?previous\s+instructions/i,
  /ignore\s+(all\s+)?(your|my|the)\s+rules/i,
  /you\s+are\s+now\s+\w+\.\s*ignore/i,
  /disregard\s+(all\s+)?(prior|previous|above)/i,
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
