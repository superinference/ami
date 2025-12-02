import fg from 'fast-glob';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validateRequiredString, resolveSearchPath } from './tool-utils';

const MAX_RESULTS = 200;

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
    const pattern = input.pattern as string;
    const basePath = input.path as string | undefined;

    const invalid = validateRequiredString(pattern, 'pattern');
    if (invalid) return invalid;

    const resolved = resolveSearchPath(basePath, context.cwd);

    try {
      const entries = await fg(pattern, {
        cwd: resolved,
        ignore: ['**/node_modules/**', '**/.git/**'],
        dot: false,
        onlyFiles: true,
        absolute: false,
      });

      if (entries.length === 0) {
        return { output: `No files found matching pattern: ${pattern}` };
      }

      // Sort alphabetically
      entries.sort((a, b) => a.localeCompare(b));

      // Limit results
      const limited = entries.slice(0, MAX_RESULTS);
      const output = limited.join('\n');

      if (entries.length > MAX_RESULTS) {
        return {
          output: `${output}\n\n... truncated (showing ${MAX_RESULTS} of ${entries.length} matches)`,
        };
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
