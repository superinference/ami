import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';

const MAX_ENTRIES = 1000;

export const listDirTool: ToolDefinition = {
  name: 'list_dir',
  description:
    'List contents of a directory. Returns entries with type indicators (file/directory).',
  inputSchema: {
    type: 'object',
    properties: {
      path: {
        type: 'string',
        description: 'The directory path to list.',
      },
    },
    required: ['path'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const dirPath = input.path as string;

    if (!dirPath || dirPath.trim().length === 0) {
      return { output: 'Error: path must not be empty.', isError: true };
    }

    const resolved = path.isAbsolute(dirPath)
      ? dirPath
      : path.resolve(context.cwd, dirPath);

    // Check if the path exists and is a directory
    let stat: fs.Stats;
    try {
      stat = await fs.promises.stat(resolved);
    } catch {
      return {
        output: `Error: Directory not found: ${resolved}`,
        isError: true,
      };
    }

    if (!stat.isDirectory()) {
      return {
        output: `Error: ${resolved} is not a directory.`,
        isError: true,
      };
    }

    // Read directory entries with file type information
    let entries: fs.Dirent[];
    try {
      entries = await fs.promises.readdir(resolved, { withFileTypes: true });
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      return {
        output: `Error reading directory: ${message}`,
        isError: true,
      };
    }

    // Separate directories and files
    const dirs: string[] = [];
    const files: string[] = [];

    for (const entry of entries) {
      if (entry.isDirectory()) {
        dirs.push(entry.name);
      } else {
        files.push(entry.name);
      }
    }

    // Sort each group alphabetically
    dirs.sort((a, b) => a.localeCompare(b));
    files.sort((a, b) => a.localeCompare(b));

    // Format entries: directories first, then files
    const formatted: string[] = [];

    for (const name of dirs) {
      formatted.push(`[dir]  ${name}/`);
    }

    for (const name of files) {
      formatted.push(`[file] ${name}`);
    }

    // Apply limit
    const totalCount = formatted.length;
    const limited = formatted.slice(0, MAX_ENTRIES);

    const header = `Contents of ${resolved}: (${totalCount} entries)`;
    const parts = [header];

    if (totalCount > MAX_ENTRIES) {
      parts.push(`Showing first ${MAX_ENTRIES} of ${totalCount} entries`);
    }

    parts.push('');
    parts.push(limited.join('\n'));

    return { output: parts.join('\n') };
  },
};
