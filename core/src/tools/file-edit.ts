import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { fuzzyFindAndReplace, findClosestLines } from './fuzzy-match';
import { detectLineEnding, normalizeToLf, convertToLineEnding, resolveFilePath, scanForSecrets } from './tool-utils';
import { getFileCache } from '../file-cache';

const CONTEXT_LINES = 3;
const MAX_EDIT_FILE_SIZE = 1_073_741_824; // 1 GiB

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


export const fileEditTool: ToolDefinition = {
  name: 'file_edit',
  description:
    'Edit a file by replacing an exact string with new content. You MUST file_read the file first. The old_string must match file content exactly including whitespace and indentation. Use the smallest old_string that uniquely identifies the target — usually 2-4 adjacent lines. Avoid large old_strings (10+ lines). If match fails, re-read the file with file_read and retry with the exact text. Do not include line numbers. For new files, use file_write instead.',
  inputSchema: {
    type: 'object',
    properties: {
      file_path: {
        type: 'string',
        description: 'The absolute or relative path to the file to edit.',
      },
      old_string: {
        type: 'string',
        description:
          'The exact string to find and replace. Must match file content exactly, including whitespace and indentation.',
      },
      new_string: {
        type: 'string',
        description: 'The replacement string.',
      },
      replace_all: {
        type: 'boolean',
        description: 'Replace all occurrences of old_string instead of requiring uniqueness. Default false.',
        default: false,
      },
    },
    required: ['file_path', 'old_string', 'new_string'],
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const filePath = input.file_path as string;
    const oldString = input.old_string as string;
    const newString = input.new_string as string;
    const replaceAll = (input.replace_all as boolean) ?? false;

    if (!filePath || filePath.trim().length === 0) {
      return { output: 'Error: file_path must not be empty.', isError: true };
    }

    if (oldString === undefined || oldString === null) {
      return { output: 'Error: old_string must be provided.', isError: true };
    }

    if (newString === undefined || newString === null) {
      return { output: 'Error: new_string must be provided.', isError: true };
    }

    if (oldString === newString) {
      return {
        output: 'Error: old_string and new_string are identical. No changes needed.',
        isError: true,
      };
    }

    const { resolved, error: pathError } = resolveFilePath(filePath, context.cwd);
    if (pathError) return pathError;

    if (resolved.endsWith('.ipynb')) {
      return {
        output: `Error: Cannot edit Jupyter notebooks (.ipynb) with file_edit. Use notebook_edit instead, which understands cell structure and properly updates execution state.`,
        isError: true,
      };
    }

    if (!oldString) {
      const secrets = scanForSecrets(newString);
      if (secrets.length > 0) {
        return { output: `Error: Potential secrets detected: ${secrets.join(', ')}. Remove them before writing.`, isError: true };
      }
      if (!fs.existsSync(resolved)) {
        fs.mkdirSync(path.dirname(resolved), { recursive: true });
        fs.writeFileSync(resolved, newString, 'utf-8');
        context.filesRead?.add(resolved);
        return { output: `Created new file: ${resolved}\n\n${newString.slice(0, 500)}${newString.length > 500 ? '...' : ''}` };
      }
      const existing = await fs.promises.readFile(resolved, 'utf-8');
      if (existing.length === 0) {
        fs.writeFileSync(resolved, newString, 'utf-8');
        return { output: `Populated empty file: ${resolved}` };
      }
      return { output: 'Error: old_string is empty but file has content. Provide the text to replace.', isError: true };
    }

    if (context.filesRead && !context.filesRead.has(resolved)) {
      return {
        output: `Error: You must read ${resolved} with file_read before editing it. This prevents edits based on stale content.`,
        isError: true,
      };
    }

    const fileCache = getFileCache(context.cwd);
    if (fileCache.hasChanged(resolved)) {
      fileCache.delete(resolved);
      return { output: 'Error: File has been modified since you last read it. Please read the file again before editing.', isError: true };
    }

    try {
      const stat = fs.statSync(resolved);
      if (stat.size > MAX_EDIT_FILE_SIZE) {
        return { output: `Error: File size (${(stat.size / 1_073_741_824).toFixed(2)} GiB) exceeds 1 GiB limit. Large files cannot be edited.`, isError: true };
      }
    } catch {
      // File doesn't exist yet — will be caught by readFile below
    }

    let rawContent: string;
    try {
      rawContent = await fs.promises.readFile(resolved, 'utf-8');
    } catch (err: unknown) {
      if ((err as { code?: string })?.code === 'ENOENT') {
        const dir = path.dirname(resolved);
        const base = path.basename(resolved);
        let suggestions = '';
        try {
          const files = fs.readdirSync(dir).filter(f => f.includes(base.slice(0, 3)) || base.includes(f.slice(0, 3)));
          if (files.length > 0) suggestions = `\nDid you mean: ${files.slice(0, 5).join(', ')}?`;
        } catch {}
        return { output: `Error: File not found: ${resolved}${suggestions}`, isError: true };
      }
      const message = err instanceof Error ? err.message : String(err);
      return {
        output: `Error reading file: ${message}`,
        isError: true,
      };
    }

    const originalEnding = detectLineEnding(rawContent);
    const content = normalizeToLf(rawContent);
    const normalizedOld = normalizeToLf(oldString);
    const normalizedNew = normalizeToLf(newString);

    // Graduated fuzzy matching: try exact first, then progressively looser strategies
    const result = fuzzyFindAndReplace(content, normalizedOld, normalizedNew);

    if (result.error) {
      if (result.matchCount === 0) {
        fileCache.delete(resolved);

        // No match — provide "did you mean?" hints
        const searchLines = oldString.split('\n');
        const hints = findClosestLines(content, searchLines, 3);
        let hintText = '';
        if (hints.length > 0) {
          hintText = '\n\nDid you mean one of these lines?\n' +
            hints.map(h => `  Line ${h.lineNumber}: ${h.line}`).join('\n');
        }
        const lines = content.split('\n');
        const preview = lines.slice(0, 30).map((l, i) => `${i + 1}: ${l}`).join('\n');
        const truncNote = lines.length > 30 ? `\n... (${lines.length - 30} more lines)` : '';
        return {
          output: `Error: old_string not found in ${resolved}. The string must match the file content EXACTLY. Re-read the file with file_read before retrying.${hintText}\n\nCurrent file content (first 30 lines):\n${preview}${truncNote}`,
          isError: true,
        };
      }
      // Multiple matches — replace all if flag is set, otherwise error
      if (replaceAll) {
        const replaced = content.split(normalizedOld).join(normalizedNew);
        const finalContent = convertToLineEnding(replaced, originalEnding);
        fs.mkdirSync(path.dirname(resolved), { recursive: true });
        await trackFileHistory(resolved, rawContent, context.cwd);
        try {
          await fs.promises.writeFile(resolved, finalContent, 'utf-8');
        } catch (err: unknown) {
          const message = err instanceof Error ? err.message : String(err);
          return { output: `Error writing file: ${message}`, isError: true };
        }
        fileCache.set(resolved, normalizeToLf(finalContent), fs.statSync(resolved).mtimeMs);
        const diff = buildUnifiedDiff(content, replaced, resolved);
        return {
          output: `Successfully replaced ${result.matchCount} occurrences in ${resolved}\n\n${diff}`,
        };
      }
      return {
        output: `Error: ${result.error}`,
        isError: true,
      };
    }

    const newContent = convertToLineEnding(result.newContent!, originalEnding);
    const strategy = result.strategy!;

    if (resolved.endsWith('config.json') || resolved.endsWith('settings.json') || resolved.endsWith('tsconfig.json') || resolved.endsWith('package.json')) {
      try { JSON.parse(newContent); } catch (e) {
        return { output: `Warning: Edit would create invalid JSON in ${path.basename(resolved)}. Check syntax.\n${(e as Error).message}`, isError: true };
      }
    }

    const secrets = scanForSecrets(newContent);
    if (secrets.length > 0) {
      return { output: `Warning: Potential secrets detected in content: ${secrets.join(', ')}. Remove secrets before writing.`, isError: true };
    }

    fs.mkdirSync(path.dirname(resolved), { recursive: true });
    await trackFileHistory(resolved, rawContent, context.cwd);
    try {
      await fs.promises.writeFile(resolved, newContent, 'utf-8');
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      return {
        output: `Error writing file: ${message}`,
        isError: true,
      };
    }
    fileCache.set(resolved, normalizeToLf(newContent), fs.statSync(resolved).mtimeMs);

    // Build a unified diff showing old vs new (use LF-normalized for clean display)
    const diff = buildUnifiedDiff(content, normalizeToLf(newContent), resolved);

    if (!context.detachedMode) {
      try {
        const { getLSPClient } = require('../lsp');
        const lsp = getLSPClient();
        lsp.notifyDidChange(resolved, newContent, context.cwd).catch(() => {});
        lsp.notifyDidSave(resolved, context.cwd).catch(() => {});
      } catch {}
    }

    const strategyNote = strategy !== 'exact' ? ` (matched via ${strategy} strategy)` : '';
    const diffLines = diff.split('\n').filter(l => l.startsWith('+') || l.startsWith('-')).length;
    const patchInfo = `[edit: ${path.basename(resolved)}, ${diffLines} lines changed]`;
    return {
      output: `${patchInfo}\nSuccessfully edited ${resolved}${strategyNote}\n\n${diff}`,
    };
  },
};

function buildUnifiedDiff(oldContent: string, newContent: string, filePath: string): string {
  const oldLines = oldContent.split('\n');
  const newLines = newContent.split('\n');

  // Find first and last differing lines
  let firstDiff = 0;
  while (firstDiff < oldLines.length && firstDiff < newLines.length && oldLines[firstDiff] === newLines[firstDiff]) {
    firstDiff++;
  }

  let oldEnd = oldLines.length - 1;
  let newEnd = newLines.length - 1;
  while (oldEnd > firstDiff && newEnd > firstDiff && oldLines[oldEnd] === newLines[newEnd]) {
    oldEnd--;
    newEnd--;
  }

  if (firstDiff > oldEnd && firstDiff > newEnd) {
    return '(no changes detected)';
  }

  // Context window
  const ctxStart = Math.max(0, firstDiff - CONTEXT_LINES);
  const ctxOldEnd = Math.min(oldLines.length - 1, oldEnd + CONTEXT_LINES);
  const ctxNewEnd = Math.min(newLines.length - 1, newEnd + CONTEXT_LINES);

  const result: string[] = [];
  result.push(`--- ${filePath}`);
  result.push(`+++ ${filePath}`);
  result.push(`@@ -${ctxStart + 1},${ctxOldEnd - ctxStart + 1} +${ctxStart + 1},${ctxNewEnd - ctxStart + 1} @@`);

  // Context before
  for (let i = ctxStart; i < firstDiff; i++) {
    result.push(` ${oldLines[i]}`);
  }

  // Removed lines
  for (let i = firstDiff; i <= oldEnd; i++) {
    result.push(`-${oldLines[i]}`);
  }

  // Added lines
  for (let i = firstDiff; i <= newEnd; i++) {
    result.push(`+${newLines[i]}`);
  }

  // Context after
  const afterStart = oldEnd + 1;
  const afterEnd = Math.min(oldLines.length - 1, oldEnd + CONTEXT_LINES);
  for (let i = afterStart; i <= afterEnd; i++) {
    result.push(` ${oldLines[i]}`);
  }

  return result.join('\n');
}
