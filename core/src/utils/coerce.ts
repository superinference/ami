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
