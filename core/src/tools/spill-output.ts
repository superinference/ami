import * as fs from 'fs';
import { createHash } from 'crypto';
import { projectStateDir } from '../reference-journal';
import * as path from 'path';

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

/** Map a filesystem error code to a short, human reason (kept alongside the
 *  stable "could not persist to disk" phrase so callers/tests keep working). */
export function spillErrorReason(errCode?: string): string {
  switch (errCode) {
    case 'ENOSPC': return 'disk full — ENOSPC';
    case 'EACCES':
    case 'EPERM': return 'permission denied';
    case 'EROFS': return 'read-only filesystem';
    case 'EDQUOT': return 'disk quota exceeded';
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
): string {
  if (spillFile) {
    const sizePart = total !== undefined ? ` (${formatSize(omitted)} of ${formatSize(total)})` : ` (${formatSize(omitted)})`;
    return `\n\n[... ${omitted} chars persisted to ${spillFile}${sizePart} — truncated, use file_read to view the rest ...]\n\n`;
  }
  // ENOSPC gets a distinct, self-explanatory banner (not the generic phrasing).
  if (errCode === 'ENOSPC') {
    return `\n\n[... ${omitted} chars omitted (disk full — ENOSPC) — output truncated ...]\n\n`;
  }
  return `\n\n[... ${omitted} chars omitted — could not persist to disk (${spillErrorReason(errCode)}), output truncated ...]\n\n`;
}

/**
 * Keep a head and a tail of oversized tool text and write the full text
 * beside the workspace. Callers must not return a silent head slice.
 *
 * The spill filename is deterministic (content hash) and writes are idempotent
 * (skip if the identical file already exists), so replaying the same output does
 * not create duplicate spill files.
 */
export function spillToolText(cwd: string, prefix: string, text: string, limit: number): string {
  if (text.length <= limit) return text;

  let spillFile: string | null = null;
  let errCode: string | undefined;
  try {
    const spillDir = projectStateDir(cwd || process.cwd(), 'tool-results');
    fs.mkdirSync(spillDir, { recursive: true });
    const hash = createHash('sha256').update(text).digest('hex').slice(0, 16);
    spillFile = path.join(spillDir, `${prefix}-${hash}.txt`);
    // Idempotent: only write when absent (or size differs from a partial write).
    if (!fs.existsSync(spillFile) || fs.statSync(spillFile).size !== Buffer.byteLength(text)) {
      fs.writeFileSync(spillFile, text, 'utf-8');
    }
  } catch (err: unknown) {
    spillFile = null;
    if (err && typeof err === 'object' && 'code' in err) {
      errCode = (err as { code?: string }).code;
    }
  }

  // Reserve space for the banner (its length depends on the spill path) so the
  // head + tail + banner can never exceed `limit`. Upper-bound the banner length
  // by rendering it with the maximum-digit omitted/total values.
  const bannerReserve = buildSpillBanner(text.length, spillFile, errCode, text.length).length;
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
  const middle = buildSpillBanner(omitted, spillFile, errCode, text.length);
  return text.slice(0, headLen) + middle + text.slice(tailStart);
}
