// fast-glob pulls braces (GHSA-vfj7-8cjw-p6xm), which has no patched release.
// expandDirectories stays off so a directory pattern is not rewritten.
import { glob } from 'tinyglobby';
import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validatePatternAndPath, assertReadableTarget, rejectEscapingPattern } from './tool-utils';

const MAX_RESULTS = 100;
// Cap the total joined output so a huge match set can't blow the context window.
const MAX_OUTPUT_CHARS = 50_000;

/**
 * Indirection seam for the underlying glob implementation. Production uses
 * tinyglobby's `glob`; tests swap it to exercise the error branch (tinyglobby
 * itself rarely throws). Behavior is unchanged in production.
 */
export const globHooks: { glob: typeof glob } = { glob };

export const globTool: ToolDefinition = {
  name: 'glob',
  description:
    'Find files matching a glob pattern. Returns matching file paths relative to the working directory.',
  inputSchema: {
    type: 'object',
    properties: {
      pattern: {
        type: 'string',
        description: 'The glob pattern to match, e.g. "**/*.ts" or "src/**/*.json".',
      },
      path: {
        type: 'string',
        description:
          'Base directory to search in. Defaults to the current working directory.',
      },
    },
    required: ['pattern'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const v = validatePatternAndPath(input.pattern, input.path as string | undefined, context.cwd);
    if (v.error) return v.error;
    const { pattern, resolved } = v;
    // The glob pattern itself must not traverse above the (already-confined) search root:
    // a pattern like "../../**" would walk out of the workspace even though `resolved` is
    // inside it. (grep's `pattern` is a REGEX, not a path, so this guard is glob-only.)
    const patternEscape = rejectEscapingPattern(pattern);
    if (patternEscape) return patternEscape;
    const unreadable = assertReadableTarget(resolved);
    if (unreadable) return unreadable;

    // The search root must be a directory. A `path` that points at an existing
    // file would otherwise silently yield "No files found"; fail loud instead.
    // (Nonexistent/unreadable roots are already handled by assertReadableTarget.)
    // The search root must be a directory. A `path` that points at an existing
    // file would otherwise silently yield "No files found"; fail loud instead.
    // (Nonexistent/unreadable roots are already handled by assertReadableTarget.)
    try {
      if (!fs.statSync(resolved).isDirectory()) {
        return {
          output: `Error: "${input.path as string}" is not a directory`,
          isError: true,
        };
      }
    } catch {
      /* stat failed (e.g. race) — let the glob below surface any real error */
    }

    // node_modules / .git are excluded by default, but if the user's pattern
    // explicitly references one of them they clearly want to glob into it, so
    // drop that specific ignore (keep the other).
    const ignore: string[] = [];
    if (!pattern.includes('node_modules')) ignore.push('**/node_modules/**');
    if (!pattern.includes('.git')) ignore.push('**/.git/**');

    try {
      const entries = await globHooks.glob(pattern, {
        cwd: resolved,
        ignore,
        dot: true,
        onlyFiles: true,
        absolute: false,
        expandDirectories: false,
      });

      if (entries.length === 0) {
        return { output: `No files found matching pattern: ${pattern}` };
      }

      // Sort by mtime descending (newest first), matching Claude Code. Stat each
      // entry relative to the search root; a stat failure is treated as mtime 0.
      // Ties fall back to alphabetical so the ordering stays deterministic.
      const mtimes = new Map<string, number>();
      for (const entry of entries) {
        try {
          mtimes.set(entry, fs.statSync(path.resolve(resolved, entry)).mtimeMs);
        } catch {
          mtimes.set(entry, 0);
        }
      }
      entries.sort((a, b) => {
        const diff = (mtimes.get(b) ?? 0) - (mtimes.get(a) ?? 0);
        return diff !== 0 ? diff : a.localeCompare(b);
      });

      const total = entries.length;
      // First apply the count cap, then shrink further if the joined list would
      // exceed the character cap.
      const shown = entries.slice(0, MAX_RESULTS);
      let truncated = total > MAX_RESULTS;
      while (shown.length > 0 && shown.join('\n').length > MAX_OUTPUT_CHARS) {
        shown.pop();
        truncated = true;
      }

      const summary = `[${total} files${truncated ? ' (truncated)' : ''}]`;
      let output = `${summary}\n${shown.join('\n')}`;
      if (truncated) {
        output += `\n\n... truncated (showing ${shown.length} of ${total} matches)`;
      }

      return { output };
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      return {
        output: `Error searching for files: ${message}`,
        isError: true,
      };
    }
  },
};
