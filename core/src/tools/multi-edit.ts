import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { fuzzyFindAndReplace, findClosestLines } from './fuzzy-match';
import { resolveFilePath, detectLineEnding, normalizeToLf, convertToLineEnding, scanForSecrets } from './tool-utils';
import { getFileCache } from '../file-cache';

export const multiEditTool: ToolDefinition = {
  name: 'multi_edit',
  description:
    'Apply multiple search-and-replace edits to a single file in one operation. Each edit replaces an exact string match. Edits are applied sequentially, so later edits see the result of earlier ones.',
  inputSchema: {
    type: 'object',
    properties: {
      file_path: {
        type: 'string',
        description: 'The absolute or relative path to the file to edit.',
      },
      edits: {
        type: 'array',
        description: 'Array of edits to apply in order.',
        items: {
          type: 'object',
          properties: {
            old_string: {
              type: 'string',
              description: 'The exact string to find.',
            },
            new_string: {
              type: 'string',
              description: 'The replacement string.',
            },
          },
          required: ['old_string', 'new_string'],
        },
      },
    },
    required: ['file_path', 'edits'],
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const filePath = input.file_path as string;
    const edits = input.edits as Array<{ old_string: string; new_string: string }>;

    if (!filePath || filePath.trim().length === 0) {
      return { output: 'Error: file_path must not be empty.', isError: true };
    }

    if (!Array.isArray(edits) || edits.length === 0) {
      return { output: 'Error: edits must be a non-empty array.', isError: true };
    }

    const { resolved, error: pathError } = resolveFilePath(filePath, context.cwd);
    if (pathError) return pathError;

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
      const { old_string, new_string } = edits[i];

      if (!old_string || old_string.length === 0) {
        failed.push(`Edit ${i + 1}: old_string must not be empty`);
        continue;
      }

      if (old_string === new_string) {
        failed.push(`Edit ${i + 1}: old_string and new_string are identical`);
        continue;
      }

      const normalizedOld = normalizeToLf(old_string);
      const normalizedNew = normalizeToLf(new_string);
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
        } else {
          failed.push(`Edit ${i + 1}: ${result.error}`);
        }
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

    try {
      const finalContent = convertToLineEnding(content, originalEnding);
      await fs.promises.writeFile(resolved, finalContent, 'utf-8');
      const fileCache = getFileCache(context.cwd);
      const newStat = await fs.promises.stat(resolved);
      fileCache.set(resolved, content, newStat.mtimeMs);
    } catch (err) {
      return {
        output: `Error writing file: ${err instanceof Error ? err.message : String(err)}`,
        isError: true,
      };
    }

    const summary = [
      `Successfully edited ${resolved}`,
      `${applied.length}/${edits.length} edits applied.`,
    ];
    if (failed.length > 0) {
      summary.push(`\nFailed edits:\n${failed.join('\n')}`);
    }

    return { output: summary.join('\n'), isError: false };
  },
};
