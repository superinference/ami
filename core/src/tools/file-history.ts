import * as fs from 'fs';
import * as path from 'path';

/**
 * Maximum number of pre-edit snapshots kept per file. Older `.bak` snapshots
 * beyond this are pruned so a long editing session cannot grow the history dir
 * without bound. Parity: Claude Code's pre-edit file-history recovery buffer.
 */
export const FILE_HISTORY_MAX_SNAPSHOTS = 20;

/**
 * Write a pre-edit snapshot of a file's original content to
 * `<cwd>/.ami/file-history/<name>.<timestamp>.bak` BEFORE a destructive write,
 * so a bad edit can be recovered/undone. Best-effort: any failure (permissions,
 * missing dir) is swallowed — the snapshot is a safety net and must never fail
 * the edit itself.
 *
 * This is the single canonical implementation shared by every mutating file tool
 * (`file_edit`, `notebook_edit`) — do not re-implement per tool.
 */
export async function trackFileHistory(
  filePath: string,
  originalContent: string,
  cwd: string,
): Promise<void> {
  try {
    const historyDir = path.join(cwd, '.ami', 'file-history');
    await fs.promises.mkdir(historyDir, { recursive: true });

    const timestamp = Date.now();
    const safeName = path.basename(filePath).replace(/[^a-zA-Z0-9.-]/g, '_');
    const historyFile = path.join(historyDir, `${safeName}.${timestamp}.bak`);

    await fs.promises.writeFile(historyFile, originalContent, 'utf-8');

    const prefix = safeName + '.';
    const entries = await fs.promises.readdir(historyDir);
    const matches = entries.filter(e => e.startsWith(prefix) && e.endsWith('.bak')).sort();
    if (matches.length > FILE_HISTORY_MAX_SNAPSHOTS) {
      for (const old of matches.slice(0, matches.length - FILE_HISTORY_MAX_SNAPSHOTS)) {
        await fs.promises.unlink(path.join(historyDir, old)).catch(() => {});
      }
    }
  } catch { /* non-critical, don't fail the edit */ }
}
