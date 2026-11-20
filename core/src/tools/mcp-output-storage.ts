import * as fs from 'fs';
import * as path from 'path';
import { createHash } from 'crypto';
import { projectStateDir } from '../reference-journal';
import { formatSize } from './spill-output';
// Reuse the single canonical mime→extension vocabulary (web-fetch) so MCP
// binary-blob persistence and web-fetch share ONE map (no parallel copy).
// Mirrors the reference's extensionForMimeType (utils/mcpOutputStorage.ts).
import { extensionForMimeType } from './web-fetch';

/**
 * Build the model-facing message describing where a decoded binary blob was
 * saved, mirroring the reference's `getBinaryBlobSavedMessage`
 * (utils/mcpOutputStorage.ts:L181-189):
 *
 *   `<sourceDescription>Binary content (<mime>, <human size>) saved to <path>`
 *
 * The leading source description lets the model re-open the file with the Read
 * tool and know which server/resource it came from. The size is rendered
 * human-readably (via {@link formatSize}) rather than as a raw byte count.
 */
export function getBinaryBlobSavedMessage(
  filepath: string,
  mimeType: string | undefined,
  size: number,
  sourceDescription: string,
): string {
  return `${sourceDescription}Binary content (${mimeType || 'unknown type'}, ${formatSize(size)}) saved to ${filepath}`;
}

/** Result of persisting a binary blob: the written path + size, or an error. */
export type PersistBinaryResult =
  | { filepath: string; size: number; ext: string }
  | { error: string };

/**
 * Write raw binary bytes to the project's `tool-results` directory with a
 * mime-derived extension, so a PDF/xlsx/image returned by an MCP tool can
 * later be opened by the Read tool (which dispatches on the extension) instead
 * of having its base64 dumped into — and bloating — the model context. Mirrors
 * the reference's `persistBinaryContent` (utils/mcpOutputStorage.ts:L138-174).
 *
 * The filename stem is a content hash (deterministic + collision-resistant) so
 * replaying the same blob is idempotent and never leaks the raw bytes inline.
 * Any filesystem failure degrades to `{ error }` rather than throwing, so the
 * caller can fall back to a size-only placeholder.
 */
export function persistBinaryContent(
  cwd: string,
  bytes: Buffer,
  mimeType: string | undefined,
): PersistBinaryResult {
  try {
    const dir = projectStateDir(cwd || process.cwd(), 'tool-results');
    fs.mkdirSync(dir, { recursive: true });
    const ext = extensionForMimeType(mimeType);
    const stem = createHash('sha256').update(bytes).digest('hex').slice(0, 16);
    const filepath = path.join(dir, `mcp-blob-${stem}.${ext}`);
    fs.writeFileSync(filepath, bytes);
    return { filepath, size: bytes.length, ext };
  } catch (err) {
    return { error: err instanceof Error ? err.message : String(err) };
  }
}
