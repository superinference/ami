/**
 * PARITY (configurable Read limits) — core/core/src/tools/file-read-limits.ts
 *
 * The Read tool enforces two independent caps on every text read:
 *
 *   | cap          | default | checks                      | on overflow          |
 *   |--------------|---------|-----------------------------|----------------------|
 *   | maxSizeBytes | 256 KB  | TOTAL FILE SIZE (pre-read)  | partial view / error |
 *   | maxTokens    | 25 000  | estimated OUTPUT tokens     | content truncated    |
 *
 * Both were previously hardcoded inside file-read.ts. This module centralizes
 * the defaults, an operator env-var override, and a per-call/per-context
 * override, with STRICT per-field validation so no malformed value (zero,
 * negative, NaN, Infinity, a non-number, or a non-numeric env string) can ever
 * disable or corrupt a cap — an invalid value always falls through to the next
 * precedence tier and ultimately to the hardcoded default. There is NO path to
 * a cap of 0/NaN/Infinity.
 *
 * Precedence (highest first): env var > caller/context override > default.
 *
 * Mirrors Claude Code's getDefaultFileReadingLimits / getEnvMaxTokens
 * (tools/FileReadTool/limits.ts) but AMI-named, synchronous, and
 * side-effect-free (no memoize, no experiment infra) so the resolved cap is
 * deterministic per call and fully testable. The env knobs are surfaced in
 * `ami --introspect` (experimentalEnvFlags).
 */

/** Default OUTPUT-token budget for a single Read (post-read, estimated). */
export const DEFAULT_FILE_READ_MAX_TOKENS = 25_000;
/** Default pre-read TOTAL-FILE-SIZE gate (256 KB). */
export const DEFAULT_FILE_READ_MAX_SIZE_BYTES = 256 * 1024;

/** Operator override for the output-token budget (positive integer). */
export const FILE_READ_MAX_TOKENS_ENV = 'AMI_FILE_READ_MAX_OUTPUT_TOKENS';
/** Operator override for the pre-read size gate in bytes (positive integer). */
export const FILE_READ_MAX_SIZE_BYTES_ENV = 'AMI_FILE_READ_MAX_SIZE_BYTES';

export interface FileReadingLimits {
  /** Max estimated output tokens before the read content is truncated. */
  maxTokens: number;
  /** Max total file size (bytes) before the pre-read size gate trips. */
  maxSizeBytes: number;
}

/**
 * A usable cap: a positive, finite number. Rejects 0, negatives, NaN, Infinity,
 * and non-numbers. Guarantees no route to a zero/NaN/Infinity cap.
 */
function isValidCap(v: unknown): v is number {
  return typeof v === 'number' && Number.isFinite(v) && v > 0;
}

/**
 * Parse an operator env-var override into a positive integer cap, or `undefined`
 * when unset/malformed (empty, non-numeric, zero, negative, NaN) so the caller
 * falls through to the next precedence tier. parseInt + isNaN + >0, mirroring
 * Claude Code's getEnvMaxTokens validation.
 */
function parseEnvCap(raw: string | undefined): number | undefined {
  if (!raw) return undefined;
  const parsed = parseInt(raw, 10);
  if (!isNaN(parsed) && parsed > 0) return parsed;
  return undefined;
}

/**
 * Resolve the effective {@link FileReadingLimits} for a Read call.
 *
 * Each field is resolved independently: env override (if a valid positive
 * integer) wins, else the caller/context override (if a valid positive finite
 * number), else the hardcoded default. Every tier is validated, so a malformed
 * value at any tier is skipped rather than producing a 0/NaN/Infinity cap.
 */
export function getFileReadingLimits(
  overrides?: Partial<FileReadingLimits>,
  env: NodeJS.ProcessEnv = process.env,
): FileReadingLimits {
  const maxTokens =
    parseEnvCap(env[FILE_READ_MAX_TOKENS_ENV]) ??
    (isValidCap(overrides?.maxTokens)
      ? (overrides.maxTokens as number)
      : DEFAULT_FILE_READ_MAX_TOKENS);

  const maxSizeBytes =
    parseEnvCap(env[FILE_READ_MAX_SIZE_BYTES_ENV]) ??
    (isValidCap(overrides?.maxSizeBytes)
      ? (overrides.maxSizeBytes as number)
      : DEFAULT_FILE_READ_MAX_SIZE_BYTES);

  return { maxTokens, maxSizeBytes };
}
