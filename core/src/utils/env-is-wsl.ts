// Split from env.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { existsSync } from 'node:fs';
import * as os from 'node:os';

/**
 * The binfmt_misc interop handler WSL registers so Windows `.exe`s can run from
 * inside the distro — the single most reliable on-disk WSL marker (present on
 * every WSL1/WSL2 distro, absent on native Linux), independent of whether the
 * interop env vars happen to be exported in the current shell. Parity with the reference
 * Code's `isWslEnvironment` (`utils/env.ts`), which keys solely off this file.
 */
export const WSL_INTEROP_MARKER = '/proc/sys/fs/binfmt_misc/WSLInterop';

/**
 * True when running under Windows Subsystem for Linux — detected via the
 * WSL-specific env vars (`WSL_DISTRO_NAME`/`WSL_INTEROP`), the presence of the
 * WSL interop binfmt marker ({@link WSL_INTEROP_MARKER}, the reliable file-system
 * signal the reference's `isWslEnvironment` uses — catches a WSL shell that did not
 * export the interop env vars), or the `microsoft`/`wsl` marker in the kernel
 * release string. The single canonical WSL detector shared by every consumer
 * (ripgrep timeout tuning, cross-OS lock-reclaim guards, the Windows-npm PATH
 * diagnostic) so the check can never drift between call sites (§0b.6 — no
 * duplicated platform probe). The on-disk probe is injectable so the file-signal
 * branch is unit-testable without a real WSL host.
 */
export function isWSL(
  env: NodeJS.ProcessEnv = process.env,
  fileExists: (p: string) => boolean = existsSync,
): boolean {
  if (env.WSL_DISTRO_NAME || env.WSL_INTEROP) return true;
  try { if (fileExists(WSL_INTEROP_MARKER)) return true; } catch { /* probe failed → fall through */ }
  try { return /microsoft|wsl/i.test(os.release()); } catch { return false; }
}

/**
 * Coerce an env-var string to a finite truncated integer, or `null` when it is
 * undefined/empty/non-finite. The single parse used by {@link parseEnvInt} and
 * {@link validateBoundedEnvInt} so the accepted numeric grammar (plain integers,
 * scientific notation, `_` digit separators) can never drift between the two.
 */
export function coerceEnvInt(envVar: string | undefined): number | null {
  if (!envVar) return null;
  const cleaned = envVar.replace(/_/g, '');
  const n = Number(cleaned);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

/** Inclusive bounds for {@link parseEnvInt}. An out-of-range value falls back. */
export interface EnvIntBounds {
  /** Inclusive lower bound; a parsed value below it returns the fallback. */
  min?: number;
  /** Inclusive upper bound; a parsed value above it returns the fallback. */
  max?: number;
}

/**
 * Parse an environment variable as an integer, accepting:
 * - Plain integers: "42", "1000"
 * - Scientific notation: "1e6", "5e3"
 * - Digit separators: "64_000", "1_000_000"
 *
 * Returns the parsed integer, or the fallback for undefined/empty/non-finite
 * input. With optional `bounds`, a value outside `[min, max]` (inclusive) is
 * REJECTED back to the fallback — so a misconfigured negative/zero/absurd
 * override (e.g. a `0` concurrency that would deadlock, or a huge timeout from a
 * typo) can never take effect. See {@link validateBoundedEnvInt} for the richer
 * clamp-and-report variant that distinguishes "invalid" from "capped".
 */
export function parseEnvInt(envVar: string | undefined, fallback: number, bounds?: EnvIntBounds): number {
  const n = coerceEnvInt(envVar);
  if (n === null) return fallback;
  if (bounds?.min !== undefined && n < bounds.min) return fallback;
  if (bounds?.max !== undefined && n > bounds.max) return fallback;
  return n;
}

/** Outcome classification for {@link validateBoundedEnvInt}. */
export const ENV_INT_STATUS = ['valid', 'capped', 'invalid'] as const;
