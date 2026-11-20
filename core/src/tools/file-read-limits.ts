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
 * Mirrors the reference's getDefaultFileReadingLimits / getEnvMaxTokens
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

/**
 * Operator toggle for whether the Read tool's SELF-DESCRIPTION surfaces the
 * concrete size cap (the `256KB size limit` phrase in the `allow_large` schema
 * description). Set to a falsy token (0/false/no/off) to drop the specific cap
 * figure from the prompt. Mirrors the reference's `includeMaxSizeInPrompt` limit
 * (tools/FileReadTool/limits.ts).
 */
export const FILE_READ_SHOW_SIZE_CAP_ENV = 'AMI_FILE_READ_SHOW_SIZE_CAP';
/**
 * Operator toggle for whether the Read tool's schema NUDGES the model toward
 * targeted (offset/limit) range reads. Set to a falsy token (0/false/no/off) to
 * drop the "prefer whole file / use offset+limit for large files" guidance from
 * the offset/limit parameter descriptions. Mirrors the reference's
 * `targetedRangeNudge` limit (tools/FileReadTool/limits.ts).
 */
export const FILE_READ_RANGE_NUDGE_ENV = 'AMI_FILE_READ_RANGE_NUDGE';

/**
 * Operator toggle (DEFAULT OFF) selecting the over-budget behavior of a Read whose
 * content exceeds the per-read token budget ({@link FileReadingLimits.maxTokens}):
 *
 *   - unset / falsy (default) — AMI's behavior: the content is silently TRUNCATED
 *     to the budget with a `[... truncated]` note (keeps partial content flowing);
 *   - truthy (1/true/yes/on) — the reference-parity behavior: the read ERRORS with an
 *     actionable "use offset/limit, or search instead" message
 *     ({@link module:token-estimator.fileReadTokenBudgetMessage}) so the model
 *     never receives a file that was silently cut mid-structure and is nudged to
 *     a targeted re-read.
 *
 * Both behaviors are kept (AMI's truncation differentiator is the default; the
 * the reference hard-error is opt-in). Surfaced in `ami --introspect` (experimentalEnvFlags).
 * Mirrors the reference's MaxFileReadTokenExceededError throw (FileReadTool.ts).
 */
export const FILE_READ_ERROR_ON_BUDGET_ENV = 'AMI_FILE_READ_ERROR_ON_TOKEN_BUDGET';

/**
 * Prompt-shaping flags that control which optional guidance the Read tool's
 * static description/schema surfaces. Independent of the numeric caps above —
 * these only affect the text the model sees, never the enforced limits.
 */
export interface FileReadPromptFlags {
  /** Surface the concrete `256KB size limit` figure in the allow_large schema. */
  includeMaxSizeInPrompt: boolean;
  /** Surface the offset/limit targeted-range-read nudge in the schema. */
  targetedRangeNudge: boolean;
}

/**
 * Interpret an env toggle that DEFAULTS ON: only an explicitly falsy token
 * (0/false/no/off, case-insensitive) turns it off; unset/empty/any other value
 * keeps it on. Mirrors the default-on env gates elsewhere in the tool layer.
 */
function isOnByDefault(raw: string | undefined): boolean {
  if (raw === undefined) return true;
  return !/^(0|false|no|off)$/i.test(raw.trim());
}

/**
 * Interpret an env toggle that DEFAULTS OFF: only an explicitly truthy token
 * (1/true/yes/on, case-insensitive) turns it on; unset/empty/any other value
 * keeps it off. The inverse of {@link isOnByDefault}.
 */
function isOffByDefault(raw: string | undefined): boolean {
  if (raw === undefined) return false;
  return /^(1|true|yes|on)$/i.test(raw.trim());
}

/**
 * Resolve the {@link FILE_READ_ERROR_ON_BUDGET_ENV} toggle. Defaults OFF, so a
 * clean environment keeps AMI's silent-truncation behavior; only an explicitly
 * truthy value opts into the reference-parity hard-error-with-nudge behavior.
 */
export function getFileReadErrorOnBudget(
  env: NodeJS.ProcessEnv = process.env,
): boolean {
  return isOffByDefault(env[FILE_READ_ERROR_ON_BUDGET_ENV]);
}

/**
 * Resolve the {@link FileReadPromptFlags} from the environment. Both default ON,
 * so an operator can only ever REMOVE guidance, never silently add misleading
 * text. Surfaced in `ami --introspect` (experimentalEnvFlags).
 */
export function getFileReadPromptFlags(
  env: NodeJS.ProcessEnv = process.env,
): FileReadPromptFlags {
  return {
    includeMaxSizeInPrompt: isOnByDefault(env[FILE_READ_SHOW_SIZE_CAP_ENV]),
    targetedRangeNudge: isOnByDefault(env[FILE_READ_RANGE_NUDGE_ENV]),
  };
}

export interface FileReadingLimits {
  /** Max estimated output tokens before the read content is truncated. */
  maxTokens: number;
  /** Max total file size (bytes) before the pre-read size gate trips. */
  maxSizeBytes: number;
}

/**
 * How the Read tool prefixes each returned line with its line number:
 * - `compact` — `N\t<content>` (a bare number + TAB). The default; it is the
 *   least-overhead format and the one AMI/the reference prefer.
 * - `arrow` — `     N→<content>` (the number right-padded to width 6, then a `→`
 *   arrow). The legacy, more-verbose format some operators/tools expect.
 *
 * The round-trip inverse (stripping EITHER prefix back to the bare source line)
 * already lives in {@link module:file-cache-seed.stripLineNumberPrefix}. Mirrors
 * the reference's `isCompactLinePrefixEnabled()` line-prefix selector (utils/file.ts).
 */
export type LineNumberStyle = 'compact' | 'arrow';

/** Default line-number prefix style (compact `N\t`), matching the historical output. */
export const DEFAULT_LINE_NUMBER_STYLE: LineNumberStyle = 'compact';

/**
 * Operator knob selecting the Read tool's line-number prefix format. Set to
 * `arrow` (case-insensitive) for the padded `     N→` format; any other/unset
 * value keeps the default compact `N\t`. The active format is reflected in the
 * file_edit tool description so the model knows exactly which prefix to strip
 * from old_string/new_string. Surfaced in `ami --introspect` (experimentalEnvFlags).
 * Mirrors the reference's `tengu_compact_line_prefix_killswitch` (utils/file.ts).
 */
export const LINE_NUMBER_STYLE_ENV = 'AMI_LINE_NUMBER_STYLE';

/**
 * Resolve the effective {@link LineNumberStyle} from the environment. Only an
 * explicit `arrow` token selects the padded-arrow format; unset/empty/any other
 * value falls back to the default compact style, so a malformed value can never
 * silently corrupt the read output format.
 */
export function getLineNumberStyle(
  env: NodeJS.ProcessEnv = process.env,
): LineNumberStyle {
  return /^arrow$/i.test((env[LINE_NUMBER_STYLE_ENV] ?? '').trim())
    ? 'arrow'
    : DEFAULT_LINE_NUMBER_STYLE;
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
 * the reference's getEnvMaxTokens validation.
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
