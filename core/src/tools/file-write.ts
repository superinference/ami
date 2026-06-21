import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { detectLineEnding, convertToLineEnding, resolveFilePath, scanForSecrets } from './tool-utils';
import { getFileCache } from '../file-cache';

async function trackFileHistory(filePath: string, originalContent: string, cwd: string): Promise<void> {
  try {
    const historyDir = path.join(cwd, '.superinference', 'file-history');
    await fs.promises.mkdir(historyDir, { recursive: true });

    const timestamp = Date.now();
    const safeName = path.basename(filePath).replace(/[^a-zA-Z0-9.-]/g, '_');
    const historyFile = path.join(historyDir, `${safeName}.${timestamp}.bak`);

    await fs.promises.writeFile(historyFile, originalContent, 'utf-8');

    // Keep only last 20 backups per file
    const prefix = safeName + '.';
    const entries = await fs.promises.readdir(historyDir);
    const matches = entries.filter(e => e.startsWith(prefix) && e.endsWith('.bak')).sort();
    if (matches.length > 20) {
      for (const old of matches.slice(0, matches.length - 20)) {
        await fs.promises.unlink(path.join(historyDir, old)).catch(() => {});
      }
    }
  } catch { /* non-critical, don't fail the edit */ }
}


export const fileWriteTool: ToolDefinition = {
  name: 'file_write',
  description:
    'Write content to a file. Creates the file and any parent directories if they don\'t exist. Overwrites existing content.',
  inputSchema: {
    type: 'object',
    properties: {
      file_path: {
        type: 'string',
        description: 'The absolute or relative path to the file to write.',
      },
      content: {
        type: 'string',
        description: 'The content to write to the file.',
      },
    },
    required: ['file_path', 'content'],
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const filePath = input.file_path as string;
    const content = input.content as string;

    if (!filePath || filePath.trim().length === 0) {
      return { output: 'Error: file_path must not be empty.', isError: true };
    }

    if (content === undefined || content === null) {
      return { output: 'Error: content must be provided.', isError: true };
    }

    const { resolved, error: pathError } = resolveFilePath(filePath, context.cwd);
    if (pathError) return pathError;

    try {
      // Read existing content for diff (if file exists)
      let oldContent = '';
      let fileExists = false;
      try {
        oldContent = await fs.promises.readFile(resolved, 'utf-8');
        fileExists = true;
      } catch {}
      const isNew = !fileExists || oldContent.length === 0;

      if (fileExists && context.filesRead && !context.filesRead.has(resolved)) {
        return {
          output: `Error: You must read ${resolved} with file_read before overwriting it. This prevents accidental data loss.`,
          isError: true,
        };
      }

      if (fileExists && context.filesRead?.has(resolved)) {
        if (getFileCache(context.cwd).hasChanged(resolved)) {
          getFileCache(context.cwd).delete(resolved);
          return { output: 'Error: File has been modified since you last read it. Read the file again before overwriting.', isError: true };
        }
      }

      const secrets = scanForSecrets(content);
      if (secrets.length > 0) {
        return { output: `Warning: Potential secrets detected in content: ${secrets.join(', ')}. Remove secrets before writing.`, isError: true };
      }

      // Preserve original line endings when overwriting
      let finalContent = content;
      if (fileExists && oldContent.length > 0) {
        const originalEnding = detectLineEnding(oldContent);
        finalContent = convertToLineEnding(content, originalEnding);
      }

      // Create parent directories if they don't exist
      const dir = path.dirname(resolved);
      await fs.promises.mkdir(dir, { recursive: true });

      // Back up existing content before overwriting
      if (fileExists && oldContent.length > 0) {
        await trackFileHistory(resolved, oldContent, context.cwd);
      }

      // Write file content
      await fs.promises.writeFile(resolved, finalContent, 'utf-8');

      // Update the unified file cache so subsequent reads return "unchanged"
      // and edits don't trigger stale-mtime errors
      const stat = fs.statSync(resolved);
      getFileCache(context.cwd).set(resolved, finalContent, stat.mtimeMs);

      // Track as known — the model wrote this content, so it can overwrite later
      context.filesRead?.add(resolved);

      if (!context.detachedMode) {
        try {
          const { getLSPClient } = require('../lsp');
          const lsp = getLSPClient();
          lsp.notifyDidChange(resolved, finalContent, context.cwd).catch(() => {});
          lsp.notifyDidSave(resolved, context.cwd).catch(() => {});
        } catch {}
      }

      // Build diff output
      const lines = content.split('\n');
      const lineCount = lines.length;
      if (isNew) {
        // New file: show all lines as added
        const preview = lines.slice(0, 20);
        const diffLines = preview.map(l => `+${l}`);
        const truncNote = lineCount > 20 ? `\n... (+${lineCount - 20} more lines)` : '';
        return { output: `Successfully wrote ${resolved} (new file, ${lineCount} lines)\n\n--- /dev/null\n+++ ${resolved}\n@@ -0,0 +1,${lineCount} @@\n${diffLines.join('\n')}${truncNote}` };
      } else {
        // Existing file: show unified diff
        const oldLines = oldContent.split('\n');
        const diffParts: string[] = [`--- ${resolved}`, `+++ ${resolved}`];
        // Simple diff: show removed old and added new (first 20 lines each)
        const maxShow = 20;
        diffParts.push(`@@ -1,${Math.min(oldLines.length, maxShow)} +1,${Math.min(lineCount, maxShow)} @@`);
        for (const l of oldLines.slice(0, maxShow)) diffParts.push(`-${l}`);
        if (oldLines.length > maxShow) diffParts.push(`... (-${oldLines.length - maxShow} more removed)`);
        for (const l of lines.slice(0, maxShow)) diffParts.push(`+${l}`);
        if (lineCount > maxShow) diffParts.push(`... (+${lineCount - maxShow} more added)`);
        return { output: `Successfully wrote ${resolved} (${lineCount} lines)\n\n${diffParts.join('\n')}` };
      }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      return {
        output: `Error writing file: ${message}`,
        isError: true,
      };
    }
  },
};
