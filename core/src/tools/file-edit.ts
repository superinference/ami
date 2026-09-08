import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { fuzzyFindAndReplace, findClosestLines } from './fuzzy-match';
import { detectLineEnding, normalizeToLf, convertToLineEnding, resolveFilePath, scanForSecrets } from './tool-utils';
import { getFileCache } from '../file-cache';

const CONTEXT_LINES = 3;
const MAX_EDIT_FILE_SIZE = 1_073_741_824; // 1 GiB

interface SequentialEdit {
  old_string: string;
  new_string: string;
  replace_all?: boolean;
}

async function trackFileHistory(filePath: string, originalContent: string, cwd: string): Promise<void> {
  try {
    const historyDir = path.join(cwd, '.superinference', 'file-history');
    await fs.promises.mkdir(historyDir, { recursive: true });

    const timestamp = Date.now();
    const safeName = path.basename(filePath).replace(/[^a-zA-Z0-9.-]/g, '_');
    const historyFile = path.join(historyDir, `${safeName}.${timestamp}.bak`);

    await fs.promises.writeFile(historyFile, originalContent, 'utf-8');

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

function notifyLsp(resolved: string, content: string, context: ToolContext): void {
  if (context.detachedMode) return;
  try {
    const { getLSPClient } = require('../lsp');
    const lsp = getLSPClient();
    lsp.notifyDidChange(resolved, content, context.cwd).catch(() => {});
    lsp.notifyDidSave(resolved, context.cwd).catch(() => {});
  } catch {}
}

function commitWrite(
  resolved: string,
  rawToWrite: string,
  cacheContent: string,
  context: ToolContext,
): string | undefined {
  try {
    fs.mkdirSync(path.dirname(resolved), { recursive: true });
    fs.writeFileSync(resolved, rawToWrite, 'utf-8');
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    return `Error writing file: ${message}`;
  }
  getFileCache(context.cwd).setWritten(resolved, cacheContent);
  context.filesRead?.add(resolved);
  notifyLsp(resolved, rawToWrite, context);
  return undefined;
}

async function writeEntireFile(
  resolved: string,
  content: string,
  context: ToolContext,
): Promise<ToolResult> {
  let oldContent = '';
  let fileExists = false;
  try {
    oldContent = await fs.promises.readFile(resolved, 'utf-8');
    fileExists = true;
  } catch {}
  const isNew = !fileExists || oldContent.length === 0;

  if (fileExists && oldContent.length > 0 && context.filesRead && !context.filesRead.has(resolved)) {
    return {
      output: `Error: You must read ${resolved} with file_read before overwriting it. This prevents accidental data loss.`,
      isError: true,
    };
  }

  if (fileExists && oldContent.length > 0 && context.filesRead?.has(resolved)) {
    const fileCache = getFileCache(context.cwd);
    if (fileCache.hasChanged(resolved)) {
      fileCache.delete(resolved);
      return { output: 'Error: File has been modified since you last read it. Read the file again before overwriting.', isError: true };
    }
  }

  const secrets = scanForSecrets(content);
  if (secrets.length > 0) {
    return { output: `Warning: Potential secrets detected in content: ${secrets.join(', ')}. Remove secrets before writing.`, isError: true };
  }

  let finalContent = content;
  if (fileExists && oldContent.length > 0) {
    finalContent = convertToLineEnding(content, detectLineEnding(oldContent));
  }

  if (fileExists && oldContent.length > 0) {
    await trackFileHistory(resolved, oldContent, context.cwd);
  }

  const writeErr = commitWrite(resolved, finalContent, content, context);
  if (writeErr) return { output: writeErr, isError: true };

  const lines = content.split('\n');
  const lineCount = lines.length;
  if (isNew) {
    const preview = lines.slice(0, 20);
    const diffLines = preview.map(l => `+${l}`);
    const truncNote = lineCount > 20 ? `\n... (+${lineCount - 20} more lines)` : '';
    const createdLabel = !fileExists ? 'Created new file' : 'Populated empty file';
    return {
      output: `${createdLabel}: ${resolved}\nSuccessfully wrote ${resolved} (new file, ${lineCount} lines)\n\n--- /dev/null\n+++ ${resolved}\n@@ -0,0 +1,${lineCount} @@\n${diffLines.join('\n')}${truncNote}`,
    };
  }

  const oldLines = oldContent.split('\n');
  const diffParts: string[] = [`--- ${resolved}`, `+++ ${resolved}`];
  const maxShow = 20;
  diffParts.push(`@@ -1,${Math.min(oldLines.length, maxShow)} +1,${Math.min(lineCount, maxShow)} @@`);
  for (const l of oldLines.slice(0, maxShow)) diffParts.push(`-${l}`);
  if (oldLines.length > maxShow) diffParts.push(`... (-${oldLines.length - maxShow} more removed)`);
  for (const l of lines.slice(0, maxShow)) diffParts.push(`+${l}`);
  if (lineCount > maxShow) diffParts.push(`... (+${lineCount - maxShow} more added)`);
  return { output: `Successfully wrote ${resolved} (${lineCount} lines)\n\n${diffParts.join('\n')}` };
}

async function applySequentialEdits(
  resolved: string,
  edits: SequentialEdit[],
  context: ToolContext,
): Promise<ToolResult> {
  if (context.filesRead && !context.filesRead.has(resolved)) {
    return {
      output: `Error: You must read ${resolved} with file_read before editing it. This prevents edits based on stale content.`,
      isError: true,
    };
  }

  const fileCache = getFileCache(context.cwd);
  if (fileCache.hasChanged(resolved)) {
    fileCache.delete(resolved);
    return { output: 'Error: File has been modified since you last read it. Read the file again before editing.', isError: true };
  }

  let rawContent: string;
  try {
    rawContent = await fs.promises.readFile(resolved, 'utf-8');
  } catch (err) {
    return {
      output: `Error: Cannot read file "${resolved}": ${err instanceof Error ? err.message : String(err)}`,
      isError: true,
    };
  }

  const originalEnding = detectLineEnding(rawContent);
  let content = normalizeToLf(rawContent);

  const applied: string[] = [];
  const failed: string[] = [];

  for (let i = 0; i < edits.length; i++) {
    const { old_string, new_string, replace_all: replaceAll } = edits[i];

    if (!old_string || old_string.length === 0) {
      failed.push(`Edit ${i + 1}: old_string must not be empty`);
      continue;
    }

    if (old_string === new_string) {
      failed.push(`Edit ${i + 1}: old_string and new_string are identical`);
      continue;
    }

    const normalizedOld = normalizeToLf(old_string);
    const normalizedNew = normalizeToLf(new_string ?? '');
    const result = fuzzyFindAndReplace(content, normalizedOld, normalizedNew);

    if (result.error) {
      if (result.matchCount === 0) {
        const searchLines = old_string.split('\n');
        const hints = findClosestLines(content, searchLines, 2);
        let hintText = '';
        if (hints.length > 0) {
          hintText = ' — did you mean line ' + hints.map(h => h.lineNumber).join(' or ') + '?';
        }
        failed.push(`Edit ${i + 1}: old_string not found in file${hintText}`);
        continue;
      }
      if (replaceAll && result.matchCount > 1) {
        content = content.split(normalizedOld).join(normalizedNew);
        applied.push(`Edit ${i + 1}: replaced ${result.matchCount} occurrences`);
        continue;
      }
      failed.push(`Edit ${i + 1}: ${result.error}`);
      continue;
    }

    content = result.newContent!;
    const strategyNote = result.strategy !== 'exact' ? ` (${result.strategy})` : '';
    applied.push(`Edit ${i + 1}: applied${strategyNote}`);
  }

  if (applied.length === 0) {
    return {
      output: `No edits applied.\n${failed.join('\n')}`,
      isError: true,
    };
  }

  const secrets = scanForSecrets(content);
  if (secrets.length > 0) {
    return { output: `Warning: Potential secrets detected in edited content: ${secrets.join(', ')}. Remove secrets before writing.`, isError: true };
  }

  const finalContent = convertToLineEnding(content, originalEnding);
  await trackFileHistory(resolved, rawContent, context.cwd);
  const writeErr = commitWrite(resolved, finalContent, content, context);
  if (writeErr) return { output: writeErr, isError: true };

  const summary = [
    `Successfully edited ${resolved}`,
    `${applied.length}/${edits.length} edits applied.`,
  ];
  if (failed.length > 0) {
    summary.push(`\nFailed edits:\n${failed.join('\n')}`);
  }

  return { output: summary.join('\n'), isError: false };
}

export const fileEditTool: ToolDefinition = {
  name: 'file_edit',
  description:
    'Create or edit a file. You MUST file_read an existing file before changing it. Modes: (1) Replace — file_path + old_string + new_string; old_string must match exactly (use the smallest unique 2–4 line span). Set replace_all to replace every occurrence. (2) Multiple replaces — file_path + edits: [{old_string, new_string, replace_all?}]; applied in order. (3) Write/create — file_path + new_string (or content) with old_string omitted or empty: creates the file or replaces its entire contents. Do not include line numbers. Never use bash sed/awk to edit files.',
  inputSchema: {
    type: 'object',
    properties: {
      file_path: {
        type: 'string',
        description: 'The absolute or relative path to the file to create or edit.',
      },
      old_string: {
        type: 'string',
        description:
          'Exact text to replace. Omit or pass empty string to write/create the entire file using new_string or content.',
      },
      new_string: {
        type: 'string',
        description: 'Replacement text, or the full file contents when old_string is empty/omitted.',
      },
      content: {
        type: 'string',
        description: 'Alias for new_string when writing or creating a file (old_string empty/omitted).',
      },
      replace_all: {
        type: 'boolean',
        description: 'Replace all occurrences of old_string instead of requiring uniqueness. Default false.',
        default: false,
      },
      edits: {
        type: 'array',
        description: 'Sequential search-and-replace edits applied in order to one file.',
        items: {
          type: 'object',
          properties: {
            old_string: { type: 'string', description: 'The exact string to find.' },
            new_string: { type: 'string', description: 'The replacement string.' },
            replace_all: { type: 'boolean', description: 'Replace every occurrence of this old_string.' },
          },
          required: ['old_string', 'new_string'],
        },
      },
    },
    required: ['file_path'],
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const filePath = input.file_path as string;
    const replaceAll = (input.replace_all as boolean) ?? false;
    const editsInput = input.edits;

    if (!filePath || filePath.trim().length === 0) {
      return { output: 'Error: file_path must not be empty.', isError: true };
    }

    const { resolved, error: pathError } = resolveFilePath(filePath, context.cwd);
    if (pathError) return pathError;

    if (resolved.endsWith('.ipynb')) {
      return {
        output: `Error: Cannot edit Jupyter notebooks (.ipynb) with file_edit. Use notebook_edit instead, which understands cell structure and properly updates execution state.`,
        isError: true,
      };
    }

    if (editsInput !== undefined) {
      if (!Array.isArray(editsInput) || editsInput.length === 0) {
        return { output: 'Error: edits must be a non-empty array.', isError: true };
      }
      const edits: SequentialEdit[] = [];
      for (const raw of editsInput) {
        if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
          return { output: 'Error: each edit must be an object with old_string and new_string.', isError: true };
        }
        const e = raw as Record<string, unknown>;
        edits.push({
          old_string: String(e.old_string ?? ''),
          new_string: String(e.new_string ?? ''),
          replace_all: Boolean(e.replace_all),
        });
      }
      return applySequentialEdits(resolved, edits, context);
    }

    const newString = (input.new_string ?? input.content) as string | undefined;
    const oldString = input.old_string as string | undefined;
    const writeMode = oldString === undefined || oldString === null || oldString === '';

    if (writeMode) {
      if (newString === undefined || newString === null) {
        return { output: 'Error: new_string (or content) must be provided to create or overwrite a file.', isError: true };
      }
      return writeEntireFile(resolved, newString, context);
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

    const result = fuzzyFindAndReplace(content, normalizedOld, normalizedNew);

    if (result.error) {
      if (result.matchCount === 0) {
        fileCache.delete(resolved);

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
      if (replaceAll) {
        const replaced = content.split(normalizedOld).join(normalizedNew);
        const finalContent = convertToLineEnding(replaced, originalEnding);
        await trackFileHistory(resolved, rawContent, context.cwd);
        const writeErr = commitWrite(resolved, finalContent, normalizeToLf(finalContent), context);
        if (writeErr) return { output: writeErr, isError: true };
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

    await trackFileHistory(resolved, rawContent, context.cwd);
    const writeErr = commitWrite(resolved, newContent, normalizeToLf(newContent), context);
    if (writeErr) return { output: writeErr, isError: true };

    const diff = buildUnifiedDiff(content, normalizeToLf(newContent), resolved);

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

  const ctxStart = Math.max(0, firstDiff - CONTEXT_LINES);
  const ctxOldEnd = Math.min(oldLines.length - 1, oldEnd + CONTEXT_LINES);
  const ctxNewEnd = Math.min(newLines.length - 1, newEnd + CONTEXT_LINES);

  const result: string[] = [];
  result.push(`--- ${filePath}`);
  result.push(`+++ ${filePath}`);
  result.push(`@@ -${ctxStart + 1},${ctxOldEnd - ctxStart + 1} +${ctxStart + 1},${ctxNewEnd - ctxStart + 1} @@`);

  for (let i = ctxStart; i < firstDiff; i++) {
    result.push(` ${oldLines[i]}`);
  }

  for (let i = firstDiff; i <= oldEnd; i++) {
    result.push(`-${oldLines[i]}`);
  }

  for (let i = firstDiff; i <= newEnd; i++) {
    result.push(`+${newLines[i]}`);
  }

  const afterStart = oldEnd + 1;
  const afterEnd = Math.min(oldLines.length - 1, oldEnd + CONTEXT_LINES);
  for (let i = afterStart; i <= afterEnd; i++) {
    result.push(` ${oldLines[i]}`);
  }

  return result.join('\n');
}
