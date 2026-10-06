import * as fs from 'fs';
import * as path from 'path';

/**
 * Keep a head and a tail of oversized tool text and write the full text
 * beside the workspace. Callers must not return a silent head slice.
 */
export function spillToolText(cwd: string, prefix: string, text: string, limit: number): string {
  if (text.length <= limit) return text;
  const spillDir = path.join(cwd || process.cwd(), '.superinference', 'tool-results');
  fs.mkdirSync(spillDir, { recursive: true });
  const spillFile = path.join(spillDir, `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}.txt`);
  fs.writeFileSync(spillFile, text, 'utf-8');
  const headLen = Math.floor(limit * 0.67);
  const tailLen = Math.max(0, limit - headLen - 200);
  const omitted = Math.max(0, text.length - headLen - tailLen);
  return text.slice(0, headLen)
    + `\n\n[... ${omitted} chars persisted to ${spillFile} — truncated, use file_read to view the rest ...]\n\n`
    + text.slice(-tailLen);
}
