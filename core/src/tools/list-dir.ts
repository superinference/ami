import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { isWithinWorkspaceRoots, isUNCPath } from './tool-utils';
import { suggestSimilarPaths } from './path-suggest';
import { matchesReadIgnore } from './read-ignore';

const MAX_ENTRIES = 1000;

export const listDirTool: ToolDefinition = {
  name: 'list_dir',
  description:
    'List the contents of a known directory, returning each entry with a type ' +
    'indicator ([dir]/[file]). Use it to discover what exists in a specific ' +
    'directory when you do not yet know exact filenames; reach for glob instead to ' +
    'match files by name pattern across a tree, and grep to search file contents. ' +
    'Entries the user has deny-listed for reading are omitted. Results are sorted ' +
    'directories-first then alphabetically, and capped at ' +
    `${MAX_ENTRIES} entries — if a directory holds more, only the first ${MAX_ENTRIES} ` +
    'are shown and the full total is reported.',
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

    // Honor engine/turn cancellation BEFORE any filesystem work: a listing on a
    // slow/network-mounted or very large directory must not keep running after the
    // turn is aborted (mirrors grep/glob consuming context.abortSignal).
    if (context.abortSignal?.aborted) {
      return { output: 'Error: directory listing was cancelled.', isError: true };
    }

    // UNC-path guard: short-circuit BEFORE any realpath/stat/readdir to avoid an
    // outbound SMB/NTLM credential leak on Windows (mirrors glob/file-read).
    if (isUNCPath(dirPath)) {
      return { output: `Error: UNC paths are not allowed (potential NTLM credential leak): ${dirPath}`, isError: true };
    }

    const resolved = path.isAbsolute(dirPath)
      ? dirPath
      : path.resolve(context.cwd, dirPath);

    const abs = path.resolve(resolved);
    // In-bounds = launch cwd OR a permission-granted read root (so a skill loaded from an
    // external folder can list its own bundle). The ungranted default is cwd-only.
    if (!isWithinWorkspaceRoots(abs, context.cwd, context.allowedReadRoots)) {
      return { output: `Error: path "${dirPath}" is outside the workspace directory.`, isError: true };
    }
    // Symlink-escape guard: a symlinked dir inside the bounds that resolves outside both
    // cwd and every granted root is rejected (a pure prefix check would miss it).
    try {
      const real = fs.realpathSync(abs);
      if (!isWithinWorkspaceRoots(real, context.cwd, context.allowedReadRoots)) {
        return { output: `Error: path "${dirPath}" resolves outside the workspace via symlink.`, isError: true };
      }
    } catch {
      // Path doesn't exist yet — no symlink to follow
    }

    // Check if the path exists and is a directory
    let stat: fs.Stats;
    try {
      stat = await fs.promises.stat(resolved);
    } catch {
      // Self-correcting error: name the cwd and offer the closest matching sibling
      // so the model can recover from a typo without a second blind guess.
      const suggestions = suggestSimilarPaths(resolved, context.cwd);
      const hint = suggestions.length > 0 ? ` Did you mean ${suggestions[0]}?` : '';
      return {
        output: `Error: Directory not found: ${resolved}. Your current working directory is ${context.cwd}.${hint}`,
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

    // File-READ deny globs from the permission layer: never ENUMERATE an entry the
    // user forbade reading — its name alone leaks the existence of a sensitive file
    // (e.g. `.env`, a credential dir) via the listing. Mirrors how grep/glob exclude
    // these from search output, reusing the shared matcher so the exclusion matches
    // exactly what a read of the entry would be denied for. Parity: the reference feeds
    // getFileReadIgnorePatterns into its directory/file enumeration too.
    const readIgnore = context.readIgnorePatterns ?? [];

    for (const entry of entries) {
      if (readIgnore.length > 0) {
        // Prefer the cwd-relative path so an anchored pattern (e.g. `subdir/*.key`)
        // applies; fall back to the bare name for entries outside cwd (an allowed
        // read root) — matchesReadIgnore tests the basename too, so any-depth and
        // bare patterns still match. Mirrors grep's `relative(root, p) || name`.
        const rel = path.relative(context.cwd, path.join(resolved, entry.name));
        const matchPath = rel && !rel.startsWith('..') ? rel : entry.name;
        if (matchesReadIgnore(matchPath, readIgnore)) continue;
      }
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
