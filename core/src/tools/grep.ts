import { spawn } from 'child_process';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validateRequiredString, resolveSearchPath } from './tool-utils';

const MAX_LINES = 200;

export const grepTool: ToolDefinition = {
  name: 'grep',
  description:
    'Search for a pattern in files. Uses regular expressions. Returns matching lines with file paths and line numbers.',
  inputSchema: {
    type: 'object',
    properties: {
      pattern: {
        type: 'string',
        description: 'The regex pattern to search for.',
      },
      path: {
        type: 'string',
        description:
          'Directory to search in. Defaults to the current working directory.',
      },
      include: {
        type: 'string',
        description:
          'Glob pattern to filter files, e.g. "*.ts" or "*.py".',
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
    const searchPath = input.path as string | undefined;
    const include = input.include as string | undefined;

    const invalid = validateRequiredString(pattern, 'pattern');
    if (invalid) return invalid;

    const { resolved, error: pathError } = resolveSearchPath(searchPath, context.cwd);
    if (pathError) return { output: pathError, isError: true };

    // Try ripgrep first, fall back to grep
    const rgResult = await runSearch(
      buildRgArgs(pattern, resolved, include),
      'rg',
      context,
    );

    if (rgResult !== null) {
      return formatResult(rgResult, pattern);
    }

    // Fallback to grep
    const grepResult = await runSearch(
      buildGrepArgs(pattern, resolved, include),
      'grep',
      context,
    );

    if (grepResult !== null) {
      return formatResult(grepResult, pattern);
    }

    return {
      output: 'Error: Neither rg (ripgrep) nor grep is available on this system.',
      isError: true,
    };
  },
};

function buildRgArgs(
  pattern: string,
  searchPath: string,
  include: string | undefined,
): string[] {
  const args = [
    '--line-number',
    '--no-heading',
    '--color',
    'never',
    '--max-count',
    '200',
  ];

  if (include) {
    args.push('--glob', include);
  }

  args.push('--', pattern, searchPath);
  return args;
}

function buildGrepArgs(
  pattern: string,
  searchPath: string,
  include: string | undefined,
): string[] {
  const args = ['-rn'];

  if (include) {
    args.push(`--include=${include}`);
  }

  args.push('--', pattern, searchPath);
  return args;
}

function runSearch(
  args: string[],
  command: string,
  context: ToolContext,
): Promise<string | null> {
  return new Promise<string | null>((resolve) => {
    let stdout = '';
    let stderr = '';

    let child: ReturnType<typeof spawn>;
    try {
      child = spawn(command, args, {
        cwd: context.cwd,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
    } catch {
      resolve(null);
      return;
    }

    const onAbort = () => {
      child.kill('SIGKILL');
    };

    if (context.abortSignal) {
      if (context.abortSignal.aborted) {
        child.kill('SIGKILL');
        resolve('');
        return;
      }
      context.abortSignal.addEventListener('abort', onAbort, { once: true });
    }

    child.stdout!.on('data', (data: Buffer) => {
      stdout += data.toString();
    });

    child.stderr!.on('data', (data: Buffer) => {
      stderr += data.toString();
    });

    child.on('error', () => {
      if (context.abortSignal) {
        context.abortSignal.removeEventListener('abort', onAbort);
      }
      // Command not available
      resolve(null);
    });

    child.on('close', (code: number | null) => {
      if (context.abortSignal) {
        context.abortSignal.removeEventListener('abort', onAbort);
      }

      // Exit code 1 for grep/rg means no matches (not an error)
      if (code === null || (code !== 0 && code !== 1)) {
        // If stderr indicates the binary doesn't exist, return null
        if (stderr.includes('not found') || stderr.includes('No such file')) {
          resolve(null);
          return;
        }
      }

      resolve(stdout);
    });
  });
}

function formatResult(output: string, pattern: string): ToolResult {
  const trimmed = output.trim();

  if (trimmed.length === 0) {
    return { output: `No matches found for pattern: ${pattern}` };
  }

  const lines = trimmed.split('\n');

  if (lines.length > MAX_LINES) {
    const truncated = lines.slice(0, MAX_LINES).join('\n');
    return {
      output: `${truncated}\n\n... truncated (showing ${MAX_LINES} of ${lines.length} matches)`,
    };
  }

  return { output: trimmed };
}
