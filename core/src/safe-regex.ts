// Structural ReDoS pre-validator for model-supplied / untrusted regexes.
//
// AMI compiles regexes from untrusted sources — a JSON-Schema `pattern`, a
// skill catalog, permission/hook matchers — then runs them over model- or
// provider-controlled strings on the SINGLE engine thread. A compile-time
// `try/catch` only rejects a pattern that does not parse; a pattern that
// parses but backtracks catastrophically (`(a+)+$`, a backreference, a
// lookbehind) can wedge the whole process for seconds on a crafted input.
//
// This module rejects the dangerous SHAPES up front — once, at parse/load —
// rather than paying the cost at match time:
//   - a backreference (`\1`, `\k<name>`) — forces a backtracking engine,
//   - a lookbehind (`(?<=…)`, `(?<!…)`) — ditto,
//   - a quantified group whose body can match the same input more than one
//     way (`(a+)*`, `(a?)+`, `(a|aa)+`) — the classic exponential split,
//   - a pattern longer than a sane length cap.
//
// Ported faithfully from Orca's `findUnsafePatternReason`
// (references/orca/.../agent-state-rules/agent-state-rule-pattern-safety.ts)
// and its `SafeRegex` zod `superRefine`, generalized with a configurable
// length cap and a `safeCompileRegex` helper so every untrusted compile site
// shares one guard. core/ keeps the building block; consumers call it.

import { z } from 'zod';

/**
 * Default upper bound on an untrusted pattern's length. Orca caps its
 * agent-state-rule patterns at 200; a JSON-Schema `pattern` can legitimately
 * be longer, so the shared default is more generous while still refusing a
 * pathologically long pattern whose sheer size is itself a compile/DoS risk.
 */
export const DEFAULT_MAX_PATTERN_LENGTH = 1000;

/**
 * Return a human-readable reason the pattern is unsafe to compile from an
 * untrusted source, or `null` when it is safe. Mirrors Orca's
 * `findUnsafePatternReason`, with an added length cap (`maxLength`, default
 * {@link DEFAULT_MAX_PATTERN_LENGTH}).
 *
 * The order matters: length and compile checks run first (cheap, and a
 * non-compiling pattern cannot be further analysed), then the structural
 * checks for backreferences, lookbehind, and variably-matching quantified
 * groups.
 */
export function findUnsafePatternReason(
  pattern: string,
  maxLength: number = DEFAULT_MAX_PATTERN_LENGTH,
): string | null {
  if (typeof pattern !== 'string') return 'is not a string';
  if (pattern.length > maxLength) {
    return `exceeds the maximum length of ${maxLength} characters`;
  }
  try {
    new RegExp(pattern);
  } catch {
    return 'does not compile';
  }
  // Backreferences (`\1`…`\9`, `\k<name>`) force a backtracking matcher.
  if (/\\[1-9]|\\k</.test(pattern)) {
    return 'uses a backreference';
  }
  // Lookbehind (`(?<=…)` / `(?<!…)`) likewise. The negative/positive marker
  // distinguishes it from a named group `(?<name>…)`, which is fine.
  if (/\(\?<[=!]/.test(pattern)) {
    return 'uses a lookbehind';
  }
  return repeatsAVariableGroup(pattern)
    ? 'repeats a group that can match in more than one way'
    : null;
}

/** A `*`, `+`, or `{n,m}` quantifier immediately follows (so `(…)` is repeated). */
function isRepeatingQuantifier(char: string | undefined): boolean {
  return char === '*' || char === '+' || char === '{';
}

/**
 * True when the pattern repeats a group whose body can match a given input in
 * more than one way — the structural signature of catastrophic backtracking
 * (`(a+)*`, `(a?)+`, `(a|aa)+`). A single-pass scanner that tracks, per open
 * group, whether its body contains a quantifier or alternation ("varies"); on
 * the closing paren, if that group varies AND is itself quantified, the pattern
 * is exponential. Variability also propagates outward to the enclosing group.
 * Character classes and escapes are skipped so a `\(` or a `[*]` is not
 * misread as a group opener / quantifier.
 */
function repeatsAVariableGroup(pattern: string): boolean {
  const groupVaries: boolean[] = [];
  let inClass = false;
  for (let index = 0; index < pattern.length; index += 1) {
    const char = pattern[index];
    if (char === '\\') {
      index += 1;
    } else if (inClass) {
      inClass = char !== ']';
    } else if (char === '[') {
      inClass = true;
    } else if (char === '(') {
      groupVaries.push(false);
      // The `?` that opens a group modifier (`(?:`, `(?=`, `(?<name>`) is not a
      // quantifier — skip it so it does not mark the body as variable.
      if (pattern[index + 1] === '?') {
        index += 1;
      }
    } else if (char === ')') {
      const bodyVaries = groupVaries.pop() ?? false;
      if (bodyVaries && isRepeatingQuantifier(pattern[index + 1])) {
        return true;
      }
      if (bodyVaries && groupVaries.length > 0) {
        groupVaries[groupVaries.length - 1] = true;
      }
    } else if (
      (isRepeatingQuantifier(char) || char === '?' || char === '|') &&
      groupVaries.length > 0
    ) {
      groupVaries[groupVaries.length - 1] = true;
    }
  }
  return false;
}

/**
 * True when the pattern is safe to compile from an untrusted source (no
 * backreference, lookbehind, variably-matching quantified group, over-length,
 * or compile error). Thin boolean wrapper over {@link findUnsafePatternReason}.
 */
export function isSafeRegex(pattern: string, maxLength?: number): boolean {
  return findUnsafePatternReason(pattern, maxLength) === null;
}

/**
 * Compile an untrusted pattern to a `RegExp`, or return `null` when the pattern
 * is structurally unsafe ({@link findUnsafePatternReason}) or does not compile.
 * This is the canonical safe-compile helper for every untrusted-regex site: a
 * caller that previously did `new RegExp(p)` inside a `try/catch` should call
 * this instead so a catastrophic pattern is refused before it can run.
 *
 * `flags` are passed through to `RegExp`; an invalid flag string makes the
 * final compile throw, which is caught and surfaced as `null`.
 */
export function safeCompileRegex(
  pattern: string,
  flags?: string,
  maxLength?: number,
): RegExp | null {
  if (findUnsafePatternReason(pattern, maxLength) !== null) return null;
  try {
    return new RegExp(pattern, flags);
  } catch {
    return null;
  }
}

/**
 * A zod string schema that refuses an unsafe regex at parse time, mirroring
 * Orca's `SafeRegex = Literal.superRefine(...)`. Use in any zod schema that
 * accepts a regex from an untrusted source so the rejection happens once, at
 * validation, with a descriptive message (`pattern uses a backreference`, …).
 *
 * `maxLength` caps the string before the structural check (default
 * {@link DEFAULT_MAX_PATTERN_LENGTH}).
 */
export function safeRegexSchema(maxLength: number = DEFAULT_MAX_PATTERN_LENGTH) {
  return z.string().superRefine((pattern, ctx) => {
    const reason = findUnsafePatternReason(pattern, maxLength);
    if (reason) {
      ctx.addIssue({ code: 'custom', message: `pattern ${reason}` });
    }
  });
}
