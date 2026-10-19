/**
 * Parse an environment variable as an integer, accepting:
 * - Plain integers: "42", "1000"
 * - Scientific notation: "1e6", "5e3"
 * - Digit separators: "64_000", "1_000_000"
 * Returns the parsed integer or the fallback value.
 */
export function parseEnvInt(envVar: string | undefined, fallback: number): number {
  if (!envVar) return fallback;
  const cleaned = envVar.replace(/_/g, '');
  const n = Number(cleaned);
  if (!Number.isFinite(n)) return fallback;
  return Math.trunc(n);
}
