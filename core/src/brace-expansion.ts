const DEFAULT_BUDGET = 1000;
const ESTIMATE_CAP = 1_000_000;

/**
 * Expand shell-style brace groups in a pattern string with a budget cap.
 * Prevents combinatorial explosion from malicious patterns like
 * `{a,b}{c,d}{e,f}...` that would OOM or stall at startup.
 *
 * Supports both brace forms:
 *   - comma alternation: `{a,b,c}` -> a, b, c
 *   - sequence/range:    `{1..5}` -> 1..5, `{a..e}` -> a..e,
 *     with an optional step `{1..10..2}` -> 1,3,5,7,9 and zero-padding
 *     preserved (`{01..03}` -> 01,02,03). Mixed-type ranges (`{1..a}`) and
 *     multi-character non-numeric endpoints are left unexpanded.
 */
export function expandBraces(pattern: string, budget: number = DEFAULT_BUDGET): string[] {
  if (!pattern.includes('{')) return [pattern];

  const estimated = estimateBraceExpansions(pattern);
  if (estimated > budget) {
    throw new Error(
      `Brace expansion exceeded budget: ${estimated} expansions (max ${budget}). ` +
      `Simplify the pattern or increase the budget.`
    );
  }
  return expandBracesRecursive(pattern);
}

/**
 * Estimate the number of expansions a brace pattern would produce without
 * actually expanding, as an upper bound. Throws early (via expandBraces) if
 * over budget.
 *
 * The estimate multiplies the alternative count of EVERY brace group, at every
 * nesting depth — not just top-level groups. A flat, top-level-only count
 * undercounts patterns whose alternatives themselves contain braces
 * (e.g. `{{a,b}p,{a,b}q}` really expands to 4, not 2), which would let a crafted
 * nested pattern slip past the budget and still cause the combinatorial blow-up
 * the budget exists to stop. Multiplying every group's cardinality is a safe
 * upper bound on the true expansion count (nested groups only ever add factors
 * >= 1), so the guard never undercounts. Sequence/range groups contribute their
 * enumerated length so large ranges (e.g. `{1..1000000}`) are also capped.
 */
export function estimateBraceExpansions(pattern: string): number {
  if (!pattern.includes('{')) return 1;

  let total = 1;
  const stack: Array<{ commas: number; start: number }> = [];

  for (let i = 0; i < pattern.length; i++) {
    const ch = pattern[i];
    if (ch === '\\' && i + 1 < pattern.length) {
      i++;
      continue;
    }
    if (ch === '{') {
      stack.push({ commas: 0, start: i + 1 });
    } else if (ch === '}') {
      const group = stack.pop();
      if (!group) continue;
      let alternatives: number;
      if (group.commas > 0) {
        alternatives = group.commas + 1;
      } else {
        const rc = rangeCardinality(pattern.slice(group.start, i));
        alternatives = rc !== null ? rc : 1;
      }
      total *= alternatives;
      if (total > ESTIMATE_CAP) return total;
    } else if (ch === ',' && stack.length > 0) {
      stack[stack.length - 1].commas++;
    }
  }
  return total;
}

function expandBracesRecursive(pattern: string): string[] {
  const braceStart = findTopLevelBrace(pattern);
  if (braceStart === -1) return [pattern];

  const braceEnd = findMatchingBrace(pattern, braceStart);
  if (braceEnd === -1) return [pattern];

  const prefix = pattern.slice(0, braceStart);
  const suffix = pattern.slice(braceEnd + 1);
  const inner = pattern.slice(braceStart + 1, braceEnd);

  let alternatives = splitTopLevel(inner);

  if (alternatives.length === 1 && alternatives[0] === inner) {
    // No top-level comma: try a sequence/range form ({N..M} / {a..z}).
    const range = expandRange(inner);
    if (range === null) return [pattern];
    alternatives = range;
  }

  const results: string[] = [];
  for (const alt of alternatives) {
    const expanded = expandBracesRecursive(prefix + alt + suffix);
    for (const e of expanded) {
      results.push(e);
    }
  }
  return results;
}

function findTopLevelBrace(pattern: string): number {
  for (let i = 0; i < pattern.length; i++) {
    if (pattern[i] === '\\' && i + 1 < pattern.length) {
      i++;
      continue;
    }
    if (pattern[i] === '{') return i;
  }
  return -1;
}

function findMatchingBrace(pattern: string, start: number): number {
  let depth = 0;
  for (let i = start; i < pattern.length; i++) {
    if (pattern[i] === '\\' && i + 1 < pattern.length) {
      i++;
      continue;
    }
    if (pattern[i] === '{') depth++;
    else if (pattern[i] === '}') {
      depth--;
      if (depth === 0) return i;
    }
  }
  return -1;
}

function splitTopLevel(inner: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let current = '';

  for (let i = 0; i < inner.length; i++) {
    const ch = inner[i];
    if (ch === '\\' && i + 1 < inner.length) {
      current += ch + inner[i + 1];
      i++;
      continue;
    }
    if (ch === '{') {
      depth++;
      current += ch;
    } else if (ch === '}') {
      depth--;
      current += ch;
    } else if (ch === ',' && depth === 0) {
      parts.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  parts.push(current);
  return parts;
}

// ---------------------------------------------------------------------------
// Sequence / range brace expansion: {N..M}, {a..z}, optional step {N..M..S}.
// ---------------------------------------------------------------------------

type NumRange = { kind: 'num'; start: number; end: number; step: number; width: number };
type CharRange = { kind: 'char'; start: number; end: number; step: number };
type RangeDesc = NumRange | CharRange;

const NUM_RE = /^-?\d+$/;
const LETTER_RE = /^[A-Za-z]$/;

/**
 * Parse a brace-sequence body (`N..M`, `a..z`, or `N..M..S`) into a descriptor.
 * Returns null when the body is not a valid range so the caller treats it as a
 * literal. Mirrors bash/Claude type validation: both endpoints must be numeric
 * OR both single letters; mixed-type and multi-character non-numeric endpoints
 * are rejected, as is a zero or non-integer step.
 */
function parseRange(inner: string): RangeDesc | null {
  const firstSep = inner.indexOf('..');
  if (firstSep === -1) return null;

  const startS = inner.slice(0, firstSep);
  const afterStart = inner.slice(firstSep + 2);
  let endS = afterStart;
  let stepS: string | null = null;
  const secondSep = afterStart.indexOf('..');
  if (secondSep !== -1) {
    endS = afterStart.slice(0, secondSep);
    stepS = afterStart.slice(secondSep + 2);
  }

  let step = 1;
  if (stepS !== null) {
    if (!NUM_RE.test(stepS)) return null;
    step = parseInt(stepS, 10);
    if (step === 0) return null;
  }
  step = Math.abs(step);

  if (NUM_RE.test(startS) && NUM_RE.test(endS)) {
    return {
      kind: 'num',
      start: parseInt(startS, 10),
      end: parseInt(endS, 10),
      step,
      width: rangePadWidth(startS, endS),
    };
  }

  if (LETTER_RE.test(startS) && LETTER_RE.test(endS)) {
    return { kind: 'char', start: startS.charCodeAt(0), end: endS.charCodeAt(0), step };
  }

  return null;
}

/**
 * Zero-pad width for a numeric range: bash pads every produced number to the
 * widest endpoint when either endpoint carries a leading zero (e.g. `01..100`).
 */
function rangePadWidth(startS: string, endS: string): number {
  const startDigits = startS.replace(/^-/, '');
  const endDigits = endS.replace(/^-/, '');
  const hasLeadingZero =
    (startDigits.length > 1 && startDigits[0] === '0') ||
    (endDigits.length > 1 && endDigits[0] === '0');
  if (!hasLeadingZero) return 0;
  return Math.max(startDigits.length, endDigits.length);
}

function formatNum(n: number, width: number): string {
  if (width <= 0) return String(n);
  const sign = n < 0 ? '-' : '';
  return sign + String(Math.abs(n)).padStart(width, '0');
}

/** Enumerate a sequence/range body, or null if it is not a valid range. */
function expandRange(inner: string): string[] | null {
  const desc = parseRange(inner);
  if (desc === null) return null;

  const out: string[] = [];
  const dir = desc.end >= desc.start ? 1 : -1;
  const step = desc.step * dir;

  if (desc.kind === 'num') {
    for (let n = desc.start; dir > 0 ? n <= desc.end : n >= desc.end; n += step) {
      out.push(formatNum(n, desc.width));
    }
  } else {
    for (let c = desc.start; dir > 0 ? c <= desc.end : c >= desc.end; c += step) {
      out.push(String.fromCharCode(c));
    }
  }
  return out;
}

/** Number of values a sequence/range body would produce, or null if not a range. */
function rangeCardinality(inner: string): number | null {
  const desc = parseRange(inner);
  if (desc === null) return null;
  return Math.floor(Math.abs(desc.end - desc.start) / desc.step) + 1;
}
