import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { fuzzyFindAndReplace, findClosestLines } from './fuzzy-match';
import { detectLineEnding, normalizeToLf, convertToLineEnding, resolveFilePath } from './tool-utils';

const CONTEXT_LINES = 3;

export const fileEditTool: ToolDefinition = {
  name: 'file_edit',
  description:
    'Edit a file by replacing an exact string with new content. You MUST file_read the file first. The old_string must match file content exactly including whitespace and indentation. If match fails, re-read the file and retry with corrected text. Do not include line numbers in old_string or new_string. For new files, use file_write instead.',
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

    if (!filePath || filePath.trim().length === 0) {
      return { output: 'Error: file_path must not be empty.', isError: true };
    }

    if (oldString === undefined || oldString === null) {
      return { output: 'Error: old_string must be provided.', isError: true };
    }

    if (oldString.length === 0) {
      return { output: 'Error: old_string must not be empty.', isError: true };
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

    if (context.filesRead && !context.filesRead.has(resolved)) {
      return {
        output: `Error: You must read ${resolved} with file_read before editing it. This prevents edits based on stale content.`,
        isError: true,
      };
    }

    let rawContent: string;
    try {
      rawContent = await fs.promises.readFile(resolved, 'utf-8');
    } catch (err: unknown) {
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
      // Multiple matches
      return {
        output: `Error: ${result.error}`,
        isError: true,
      };
    }

    const newContent = convertToLineEnding(result.newContent!, originalEnding);
    const strategy = result.strategy!;

    try {
      await fs.promises.writeFile(resolved, newContent, 'utf-8');
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      return {
        output: `Error writing file: ${message}`,
        isError: true,
      };
    }

    // Build a unified diff showing old vs new (use LF-normalized for clean display)
    const diff = buildUnifiedDiff(content, normalizeToLf(newContent), resolved);

    const strategyNote = strategy !== 'exact' ? ` (matched via ${strategy} strategy)` : '';
    return {
      output: `Successfully edited ${resolved}${strategyNote}\n\n${diff}`,
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
