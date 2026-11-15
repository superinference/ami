/**
 * Coerce a tool-call argument to a boolean.
 *
 * Models/providers frequently serialize JSON booleans as the STRING "true" /
 * "false" in tool-call arguments. A naive `=== true` silently drops `"true"`
 * (treating it as false), while a naive truthy check (`!!v`) treats the string
 * `"false"` as TRUE (any non-empty string is truthy). Both are real bugs we hit
 * (run_in_background on task and bash). Only a real `true` or the string "true"
 * (case/space-insensitive) count as true; everything else is false.
 */
export function coerceBool(v: unknown): boolean {
  if (v === true) return true;
  if (typeof v === 'string') return v.trim().toLowerCase() === 'true';
  return false;
}

/** Options for {@link coerceNumber}. */
export interface CoerceNumberOptions {
  /** Inclusive lower bound; the result is clamped up to this value. */
  min?: number;
  /** Inclusive upper bound; the result is clamped down to this value. */
  max?: number;
  /**
   * Truncate toward zero to an integer (via `Math.trunc`) before clamping.
   * Mirrors the `parseEnvInt` convention used elsewhere in core.
   */
  integer?: boolean;
}

/**
 * Coerce a tool-call argument to a finite number, with a safe fallback.
 *
 * FUNCTIONAL CHANGE (additive): new helper. Tools hand-rolled
 * `const n = Number(input.x); if (!Number.isFinite(n)) ...` plus ad-hoc
 * `Math.min(Math.max(...))` clamping (agentic-fetch, scout, glob, tool-search).
 * A bare `Number()` is a trap on untrusted tool input:
 *   - `Number('')` / `Number(null)` / `Number([])` / `Number(false)` → `0`
 *   - `Number(true)` / `Number([5])` → `1` / `5`
 *   - `Number('12px')` → `NaN`
 * Each of those silently injects a bogus numeric value instead of the fallback.
 * This helper accepts ONLY a real finite `number` or a non-empty numeric
 * STRING (trimmed); every other shape (boolean, null, undefined, NaN/Infinity,
 * object, array, empty/garbage string) returns `fallback` unchanged. Optional
 * `integer`/`min`/`max` apply (in that order) to a successfully-coerced value;
 * the fallback is returned verbatim and is NOT clamped (the caller supplies a
 * sane fallback).
 */
export function coerceNumber(v: unknown, fallback: number, opts?: CoerceNumberOptions): number {
  let n: number;
  if (typeof v === 'number') {
    if (!Number.isFinite(v)) return fallback;
    n = v;
  } else if (typeof v === 'string') {
    const trimmed = v.trim();
    if (trimmed === '') return fallback;
    const parsed = Number(trimmed);
    if (!Number.isFinite(parsed)) return fallback;
    n = parsed;
  } else {
    // boolean, null, undefined, object, array, bigint, symbol, function
    return fallback;
  }
  if (opts?.integer) n = Math.trunc(n);
  if (opts?.min !== undefined && n < opts.min) n = opts.min;
  if (opts?.max !== undefined && n > opts.max) n = opts.max;
  return n;
}

/**
 * Coerce a tool-call argument to a string, with a safe fallback.
 *
 * FUNCTIONAL CHANGE (additive): new helper. A bare `String(input.x)` is a trap:
 * `String({})` → `"[object Object]"`, `String([1,2])` → `"1,2"`,
 * `String(null)` → `"null"`, `String(undefined)` → `"undefined"`,
 * `String(NaN)` → `"NaN"` — each produces a plausible-looking but garbage value.
 * This accepts an actual `string` verbatim, stringifies a finite `number`,
 * `boolean`, or `bigint` (the JSON-scalar shapes a provider may serialize), and
 * returns `fallback` (default `''`) for everything else (null, undefined,
 * non-finite numbers, objects, arrays, symbols, functions).
 */
export function coerceString(v: unknown, fallback = ''): string {
  if (typeof v === 'string') return v;
  if (typeof v === 'number') return Number.isFinite(v) ? String(v) : fallback;
  if (typeof v === 'boolean' || typeof v === 'bigint') return String(v);
  return fallback;
}

/**
 * Coerce a tool-call argument to an array, with a safe fallback.
 *
 * FUNCTIONAL CHANGE (additive): new helper. Callers wrote
 * `Array.isArray(x) ? (x as T[]) : undefined`. This centralizes that so a
 * non-array (null/undefined/object/scalar) yields `fallback` (default `[]`)
 * rather than leaking a non-iterable value downstream.
 */
export function coerceArray<T = unknown>(v: unknown, fallback: T[] = []): T[] {
  return Array.isArray(v) ? (v as T[]) : fallback;
}

/**
 * Coerce a tool-call argument to an array of strings.
 *
 * FUNCTIONAL CHANGE (additive): new helper. The pattern
 * `Array.isArray(input.team_allowed_paths) ? (input.team_allowed_paths as string[]) : undefined`
 * casts WITHOUT validating elements — a model emitting `["ok", 3, null, {}]`
 * smuggles non-strings past the `as string[]` into code that assumes strings.
 * This:
 *   - wraps a lone `string` into a single-element array (providers routinely
 *     send a scalar where the schema allows `string | string[]`),
 *   - for an array, KEEPS only the genuine string elements (dropping numbers,
 *     null, objects, nested arrays),
 *   - returns `fallback` (default `[]`) for any other shape.
 */
export function coerceStringArray(v: unknown, fallback: string[] = []): string[] {
  if (typeof v === 'string') return [v];
  if (!Array.isArray(v)) return fallback;
  return v.filter((e): e is string => typeof e === 'string');
}
