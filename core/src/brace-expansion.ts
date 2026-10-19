const DEFAULT_BUDGET = 1000;

/**
 * Expand shell-style brace groups in a pattern string with a budget cap.
 * Prevents combinatorial explosion from malicious patterns like
 * `{a,b}{c,d}{e,f}...` that would OOM or stall at startup.
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
 * Estimate the number of expansions a brace pattern would produce
 * without actually expanding. Throws early if over budget.
 */
export function estimateBraceExpansions(pattern: string): number {
  if (!pattern.includes('{')) return 1;
  const groups = extractBraceGroups(pattern);
  let total = 1;
  for (const g of groups) {
    total *= g;
    if (total > 1_000_000) return total;
  }
  return total;
}

function extractBraceGroups(pattern: string): number[] {
  const counts: number[] = [];
  let depth = 0;
  let alternatives = 1;

  for (let i = 0; i < pattern.length; i++) {
    const ch = pattern[i];
    if (ch === '\\' && i + 1 < pattern.length) {
      i++;
      continue;
    }
    if (ch === '{') {
      if (depth === 0) alternatives = 1;
      depth++;
    } else if (ch === '}') {
      depth--;
      if (depth === 0) counts.push(alternatives);
    } else if (ch === ',' && depth === 1) {
      alternatives++;
    }
  }
  return counts;
}

function expandBracesRecursive(pattern: string): string[] {
  const braceStart = findTopLevelBrace(pattern);
  if (braceStart === -1) return [pattern];

  const braceEnd = findMatchingBrace(pattern, braceStart);
  if (braceEnd === -1) return [pattern];

  const prefix = pattern.slice(0, braceStart);
  const suffix = pattern.slice(braceEnd + 1);
  const inner = pattern.slice(braceStart + 1, braceEnd);

  const alternatives = splitTopLevel(inner);

  if (alternatives.length === 1 && alternatives[0] === inner) {
    return [pattern];
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
