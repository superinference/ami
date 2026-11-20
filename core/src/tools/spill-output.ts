import * as fs from 'fs';
import { createHash } from 'crypto';
import { projectStateDir } from '../reference-journal';
import { validateBoundedIntEnvVar } from '../config';
import * as path from 'path';

/**
 * Built-in default ceiling for a subagent/task transcript (parity: the reference's
 * TASK_MAX_OUTPUT_DEFAULT). Applied EVEN WHEN no output budget is configured, so a
 * chatty subagent can never return an unbounded transcript into the parent context.
 */
export const DEFAULT_TASK_MAX_CHARS = 32_000;

/**
 * Enforced upper bound for the task output cap (parity: the reference's
 * TASK_MAX_OUTPUT_UPPER_LIMIT). A misconfigured or hostile override cannot push the
 * cap above this, so the "don't blow up the parent context" guard can't be disabled.
 */
export const TASK_MAX_OUTPUT_UPPER_LIMIT = 160_000;

/**
 * Resolve the effective character cap for a subagent/task transcript. The cap is
 * ALWAYS finite (mirrors the reference's getMaxTaskOutputLength): an explicit per-run
 * budget wins (clamped to the upper bound), else the `TASK_MAX_OUTPUT_LENGTH` env
 * knob (validated + bounded), else the built-in {@link DEFAULT_TASK_MAX_CHARS}. This
 * guarantees subagent output is bounded out of the box — no env var, no project
 * config required — closing the "uncapped transcript floods the parent" hole.
 */
export function resolveTaskMaxChars(configured?: number): number {
  if (configured !== undefined && Number.isFinite(configured) && configured > 0) {
    return Math.min(configured, TASK_MAX_OUTPUT_UPPER_LIMIT);
  }
  return validateBoundedIntEnvVar('TASK_MAX_OUTPUT_LENGTH', process.env.TASK_MAX_OUTPUT_LENGTH, {
    default: DEFAULT_TASK_MAX_CHARS,
    min: 1,
    max: TASK_MAX_OUTPUT_UPPER_LIMIT,
  }).effective;
}

/**
 * Human-readable size for a character/byte count (e.g. 4906 -> "4.8 KB").
 * Used in spill banners so the model sees a legible magnitude, not a raw count.
 */
export function formatSize(count: number): string {
  if (count < 1024) return `${count} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = count / 1024;
  let i = 0;
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024;
    i++;
  }
  return `${value.toFixed(1)} ${units[i]}`;
}

/**
 * Machine-delineated markers wrapping a persisted-output banner region.
 *
 * These tags are **model-facing signaling**: they let a downstream display layer
 * detect (and optionally strip) the read-back guidance for the user while the
 * model keeps the full instructions. Exposed as shared constants so every site
 * that emits or detects a persisted-output region uses the same, stable sentinel
 * (one canonical tag, not an ad-hoc per-call string).
 */
export const PERSISTED_OUTPUT_OPEN = '<persisted-output>';
export const PERSISTED_OUTPUT_CLOSE = '</persisted-output>';

/**
 * True when `text` is already the product of a prior spill (it contains a
 * persisted-output banner). Callers use this to avoid re-spilling — and
 * re-wrapping — the same content on replay. The check is specific (marker AND
 * the exact banner signature) so ordinary content that merely mentions the tag
 * (e.g. reading this source file) is not misclassified.
 */
export function isAlreadySpilled(text: string): boolean {
  return (
    text.includes(PERSISTED_OUTPUT_OPEN) &&
    /\[\.\.\. \d+ chars persisted to .+ — truncated/.test(text)
  );
}

/**
 * Options controlling the read-back guidance embedded in a spill banner.
 * When any field is set (and the budget affords it), the banner is enriched
 * with a sequential-read protocol so the model knows how to consume the file.
 */
export interface SpillOptions {
  /** Human description of the spilled payload's format (e.g. 'JSON',
   *  'Plain text', or 'JSON with schema: …'). Tells the model how to parse. */
  formatDescription?: string;
  /** Per-read character cap when reading the spilled file back (e.g. a tool's
   *  output budget). Tells the model its safe chunk size. */
  maxReadLength?: number;
}

/**
 * Derive a format description for a structured tool result, mirroring the reference
 * Code's getFormatDescription. Used when spilling MCP results so the banner can
 * tell the model whether the persisted file is plain text, JSON, or a JSON
 * array — optionally with a compact schema signature for jq-friendly querying.
 */
export function getFormatDescription(
  type: 'toolResult' | 'structuredContent' | 'contentArray',
  schema?: string,
): string {
  switch (type) {
    case 'toolResult':
      return 'Plain text';
    case 'structuredContent':
      return schema ? `JSON with schema: ${schema}` : 'JSON';
    case 'contentArray':
      return schema ? `JSON array with schema: ${schema}` : 'JSON array';
  }
}

/**
 * Build the sequential-read protocol appended to a spill banner. Mirrors
 * the reference's getLargeOutputInstructions: format line, offset/limit + jq guidance,
 * a 100%-read requirement, truncation-retry guidance (with the per-read cap when
 * known), and a completeness-attestation requirement. Returns '' when no
 * read-back options were provided (caller falls back to the compact banner).
 */
function buildReadBackInstructions(spillFile: string, opts?: SpillOptions): string {
  if (!opts || (opts.formatDescription === undefined && opts.maxReadLength === undefined)) {
    return '';
  }
  const lines: string[] = [];
  if (opts.formatDescription) lines.push(`Format: ${opts.formatDescription}`);
  lines.push(
    'Use file_read with offset and limit to read specific portions, search within the file, and jq to make structured queries.',
  );
  lines.push('REQUIREMENTS FOR SUMMARIZATION/ANALYSIS/REVIEW:');
  lines.push(
    `- You MUST read the content from ${spillFile} in sequential chunks until 100% of it has been read.`,
  );
  lines.push(
    opts.maxReadLength !== undefined
      ? `- If you receive truncation warnings when reading the file, reduce the chunk size until you have read 100% of the content without truncation. Each read is bounded to ${opts.maxReadLength.toLocaleString()} chars.`
      : '- If you receive truncation warnings when reading the file, reduce the chunk size until you have read 100% of the content without truncation.',
  );
  lines.push(
    '- Before producing ANY summary or analysis, you MUST explicitly describe what portion of the content you have read. If you did not read the entire content, you MUST explicitly state this.',
  );
  return lines.join('\n');
}

/** Map a filesystem error code to a short, human reason (kept alongside the
 *  stable "could not persist to disk" phrase so callers/tests keep working). */
export function spillErrorReason(errCode?: string): string {
  switch (errCode) {
    case 'ENOSPC': return 'disk full — ENOSPC';
    case 'EACCES':
    case 'EPERM': return 'permission denied';
    case 'EROFS': return 'read-only filesystem';
    case 'EDQUOT': return 'disk quota exceeded';
    case 'EMFILE':
    case 'ENFILE': return 'too many open files';
    case 'EEXIST': return 'file already exists';
    case 'ENOTDIR':
    case 'ENOENT': return 'invalid spill path';
    default: return errCode ? `error ${errCode}` : 'unknown error';
  }
}

/**
 * Build the middle banner for truncated spill output. Exported for testing.
 * On success it reports the omitted slice AND the total original size in
 * human-readable units; on failure it gives an errno-specific reason while
 * preserving the stable "could not persist to disk" / "truncated" phrasing.
 */
export function buildSpillBanner(
  omitted: number,
  spillFile: string | null,
  errCode?: string,
  total?: number,
  opts?: SpillOptions,
): string {
  if (spillFile) {
    const sizePart = total !== undefined ? ` (${formatSize(omitted)} of ${formatSize(total)})` : ` (${formatSize(omitted)})`;
    const headline = `[... ${omitted} chars persisted to ${spillFile}${sizePart} — truncated, use file_read to view the rest ...]`;
    const instructions = buildReadBackInstructions(spillFile, opts);
    // When read-back guidance is requested, wrap the whole region in the stable
    // persisted-output sentinel so a display layer can delineate (and strip) it.
    if (instructions) {
      return `\n\n${PERSISTED_OUTPUT_OPEN}\n${headline}\n${instructions}\n${PERSISTED_OUTPUT_CLOSE}\n\n`;
    }
    return `\n\n${headline}\n\n`;
  }
  // ENOSPC gets a distinct, self-explanatory banner (not the generic phrasing).
  if (errCode === 'ENOSPC') {
    return `\n\n[... ${omitted} chars omitted (disk full — ENOSPC) — output truncated ...]\n\n`;
  }
  return `\n\n[... ${omitted} chars omitted — could not persist to disk (${spillErrorReason(errCode)}), output truncated ...]\n\n`;
}

/**
 * Structured outcome of a spill. `text` is the head+tail+banner string the model
 * sees (identical to {@link spillToolText}'s return). When the full payload was
 * persisted to disk, {@link SpillResult.outputFilePath} and
 * {@link SpillResult.outputFileSize} point a programmatic caller at the on-disk
 * file WITHOUT re-parsing the banner — so a tool can carry them in its
 * a ToolResult's `metadata`. They are both undefined when the output was
 * returned verbatim (under the limit / non-finite limit) or the spill FAILED
 * (disk full, permission denied, …), in which case only the banner records it.
 *
 * Parity: the reference's ExecResult `{ outputFilePath, outputFileSize }` structured
 * fields (ShellCommand.ts) set when stdout overflows inline into a file on disk.
 */
export interface SpillResult {
  /** Head + banner + tail — the bounded text the model receives. */
  text: string;
  /** Absolute path of the persisted full-output file (set only on a real spill). */
  outputFilePath?: string;
  /** Size in bytes of the persisted full-output file (set with outputFilePath). */
  outputFileSize?: number;
}

/**
 * Keep a head and a tail of oversized tool text and write the full text
 * beside the workspace, returning BOTH the bounded display text and structured
 * spill metadata ({@link SpillResult}). Callers that only need the text use the
 * thin {@link spillToolText} wrapper; callers that want to surface the on-disk
 * path/size in a ToolResult's `metadata` (parity: the reference's ExecResult
 * outputFilePath/outputFileSize) use this directly.
 *
 * The spill filename is deterministic (content hash) and writes are idempotent
 * (skip if the identical file already exists), so replaying the same output does
 * not create duplicate spill files.
 *
 * A non-finite `limit` (Infinity / NaN) is a hard opt-out: such output is already
 * self-bounded by its producer, so it is passed through verbatim and never spilled.
 */
export function spillToolTextWithMeta(cwd: string, prefix: string, text: string, limit: number, opts?: SpillOptions): SpillResult {
  // Non-finite limit = hard opt-out (self-bounded tools must never be spilled and
  // re-read circularly). Finite-and-under-limit output is returned unchanged.
  if (!Number.isFinite(limit) || text.length <= limit) return { text };

  let spillFile: string | null = null;
  let errCode: string | undefined;
  try {
    const spillDir = projectStateDir(cwd || process.cwd(), 'tool-results');
    fs.mkdirSync(spillDir, { recursive: true });
    const hash = createHash('sha256').update(text).digest('hex').slice(0, 16);
    // Sanitise the caller-supplied prefix to a single safe path segment before
    // interpolating it — a value containing `/` or `..` would otherwise escape
    // spillDir and write the full (untruncated) output to an arbitrary location.
    const safePrefix = prefix.replace(/[^A-Za-z0-9._-]/g, '_') || 'tool';
    spillFile = path.join(spillDir, `${safePrefix}-${hash}.txt`);
    // Exclusive ('wx' = O_CREAT|O_EXCL) write: race-free vs a stat-then-write
    // TOCTOU and refuses to create through a pre-existing symlink at the
    // deterministic target, defeating a symlink-swap arbitrary-file-overwrite.
    try {
      fs.writeFileSync(spillFile, text, { encoding: 'utf-8', flag: 'wx' });
    } catch (e: unknown) {
      if ((e as { code?: string }).code === 'EEXIST') {
        // Deterministic (content-hashed) name already present. Use lstat (never
        // follows a symlink) to confirm it is a regular file of the right size —
        // identical content is an idempotent replay, nothing to do. Anything else
        // (a planted symlink, or a partial/corrupt prior write) is unlinked — which
        // removes the symlink itself, not its target — and rewritten with O_EXCL.
        const st = fs.lstatSync(spillFile);
        if (!st.isFile() || st.size !== Buffer.byteLength(text)) {
          fs.unlinkSync(spillFile);
          fs.writeFileSync(spillFile, text, { encoding: 'utf-8', flag: 'wx' });
        }
      } else {
        throw e;
      }
    }
  } catch (err: unknown) {
    spillFile = null;
    if (err && typeof err === 'object' && 'code' in err) {
      errCode = (err as { code?: string }).code;
    }
  }

  // Decide whether the enriched read-back instructions fit the budget. For small
  // limits the instruction block would crowd out the head/tail preview, so fall
  // back to the compact banner. Computed deterministically (and reused below) so
  // the reserve length matches what is actually rendered.
  let effectiveOpts = opts;
  if (spillFile && opts && (opts.formatDescription !== undefined || opts.maxReadLength !== undefined)) {
    const richReserve = buildSpillBanner(text.length, spillFile, errCode, text.length, opts).length;
    const MIN_PREVIEW = 200;
    if (limit - richReserve < Math.max(MIN_PREVIEW, Math.floor(limit * 0.25))) {
      effectiveOpts = undefined;
    }
  }

  // Reserve space for the banner (its length depends on the spill path) so the
  // head + tail + banner can never exceed `limit`. Upper-bound the banner length
  // by rendering it with the maximum-digit omitted/total values.
  const bannerReserve = buildSpillBanner(text.length, spillFile, errCode, text.length, effectiveOpts).length;
  const budget = Math.max(0, limit - bannerReserve);
  let headLen = Math.floor(budget * 0.67);
  const tailLen = Math.max(0, budget - headLen);

  // Avoid splitting a UTF-16 surrogate pair at the head boundary.
  if (headLen > 0 && headLen < text.length) {
    const code = text.charCodeAt(headLen - 1);
    if (code >= 0xD800 && code <= 0xDBFF) headLen--; // back off lone high surrogate
  }
  // Prefer a newline boundary for the head so the preview is not mid-line.
  const headNl = text.lastIndexOf('\n', headLen);
  if (headNl > headLen - 120 && headNl > 0) headLen = headNl + 1;

  // Avoid splitting a UTF-16 surrogate pair at the tail boundary.
  let tailStart = tailLen > 0 ? text.length - tailLen : text.length;
  if (tailStart > 0 && tailStart < text.length) {
    const code = text.charCodeAt(tailStart);
    if (code >= 0xDC00 && code <= 0xDFFF) tailStart++; // skip lone low surrogate
  }
  // Prefer a newline boundary for the tail start.
  if (tailStart > 0 && tailStart < text.length) {
    const tailNl = text.indexOf('\n', tailStart);
    if (tailNl !== -1 && tailNl < tailStart + 120) tailStart = tailNl + 1;
  }

  const omitted = Math.max(0, text.length - headLen - (text.length - tailStart));
  const middle = buildSpillBanner(omitted, spillFile, errCode, text.length, effectiveOpts);
  const display = text.slice(0, headLen) + middle + text.slice(tailStart);
  // Only a SUCCESSFUL spill (spillFile !== null) exposes the on-disk path/size; a
  // failed spill (errCode set, spillFile null) records nothing structured — the
  // banner already says the output could not be persisted.
  return spillFile
    ? { text: display, outputFilePath: spillFile, outputFileSize: Buffer.byteLength(text) }
    : { text: display };
}

/**
 * Keep a head and a tail of oversized tool text and persist the full text to disk.
 * Thin wrapper over {@link spillToolTextWithMeta} for callers that need only the
 * bounded display text (file_read, web-fetch, mcp, pdf). The structured on-disk
 * path/size is available via {@link spillToolTextWithMeta} — do not re-derive it
 * by parsing the banner.
 */
export function spillToolText(cwd: string, prefix: string, text: string, limit: number, opts?: SpillOptions): string {
  return spillToolTextWithMeta(cwd, prefix, text, limit, opts).text;
}
