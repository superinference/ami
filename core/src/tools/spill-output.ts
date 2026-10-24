import * as fs from 'fs';
import * as path from 'path';

/**
 * Build the middle banner for truncated spill output. Exported for testing.
 */
export function buildSpillBanner(omitted: number, spillFile: string | null, errCode?: string): string {
  if (spillFile) {
    return `\n\n[... ${omitted} chars persisted to ${spillFile} — truncated, use file_read to view the rest ...]\n\n`;
  }
  if (errCode === 'ENOSPC') {
    return `\n\n[... ${omitted} chars omitted (disk full — ENOSPC) ...]\n\n`;
  }
  return `\n\n[... ${omitted} chars omitted (could not persist to disk) — output truncated ...]\n\n`;
}

/**
 * Keep a head and a tail of oversized tool text and write the full text
 * beside the workspace. Callers must not return a silent head slice.
 */
export function spillToolText(cwd: string, prefix: string, text: string, limit: number): string {
  if (text.length <= limit) return text;

  let spillFile: string | null = null;
  let errCode: string | undefined;
  try {
    const spillDir = path.join(cwd || process.cwd(), '.superinference', 'tool-results');
    fs.mkdirSync(spillDir, { recursive: true });
    spillFile = path.join(spillDir, `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}.txt`);
    fs.writeFileSync(spillFile, text, 'utf-8');
  } catch (err: unknown) {
    spillFile = null;
    if (err && typeof err === 'object' && 'code' in err) {
      errCode = (err as { code?: string }).code;
    }
  }

  let headLen = Math.floor(limit * 0.67);
  const tailLen = Math.max(0, limit - headLen - 200);

  // Avoid splitting a UTF-16 surrogate pair at the head boundary.
  if (headLen > 0 && headLen < text.length) {
    const code = text.charCodeAt(headLen - 1);
    if (code >= 0xD800 && code <= 0xDBFF) headLen--; // back off lone high surrogate
  }

  // Avoid splitting a UTF-16 surrogate pair at the tail boundary.
  let tailStart = tailLen > 0 ? text.length - tailLen : text.length;
  if (tailStart > 0 && tailStart < text.length) {
    const code = text.charCodeAt(tailStart);
    if (code >= 0xDC00 && code <= 0xDFFF) tailStart++; // skip lone low surrogate
  }

  const omitted = Math.max(0, text.length - headLen - (text.length - tailStart));
  const middle = buildSpillBanner(omitted, spillFile, errCode);
  return text.slice(0, headLen) + middle + text.slice(tailStart);
}
