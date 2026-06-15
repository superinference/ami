import { spawn } from 'child_process';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validatePatternAndPath } from './tool-utils';

const MAX_LINES = 250;
const RG_TIMEOUT = 20_000;

interface SearchOutcome {
  output: string | null;
  timedOut?: boolean;
  eagain?: boolean;
}

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
      output_mode: {
        type: 'string',
        enum: ['content', 'files_with_matches', 'count'],
        description: 'Output format: content (default, matching lines with context), files_with_matches (file names only — more token-efficient), count (match counts per file).',
        default: 'content',
      },
      case_insensitive: {
        type: 'boolean',
        description: 'Perform case-insensitive matching. Default false.',
        default: false,
      },
      context_lines: {
        type: 'number',
        description: 'Number of context lines before and after each match. Only used in content mode.',
      },
      multiline: {
        type: 'boolean',
        description: 'Enable multiline matching (dot matches newlines). Only supported with ripgrep.',
        default: false,
      },
      line_numbers: {
        type: 'boolean',
        description: 'Show line numbers in content mode. Default true.',
      },
      head_limit: {
        type: 'number',
        description: 'Maximum number of result lines to return. Default 250.',
      },
      offset: {
        type: 'number',
        description: 'Number of result lines to skip from the beginning.',
      },
      type: {
        type: 'string',
        description: 'File type filter for ripgrep (e.g., "js", "py", "rust", "go", "java"). Maps to rg --type.',
      },
      before_context: {
        type: 'number',
        description: 'Lines of context before each match (rg -B). Overrides context_lines for before.',
      },
      after_context: {
        type: 'number',
        description: 'Lines of context after each match (rg -A). Overrides context_lines for after.',
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
    const include = input.include as string | undefined;
    const outputMode = (input.output_mode as string) || 'content';
    const lineNumbers = input.line_numbers as boolean | undefined;
    const caseInsensitive = (input.case_insensitive as boolean) ?? false;
    const contextLines = input.context_lines as number | undefined;
    const multiline = (input.multiline as boolean) ?? false;
    const headLimit = (input.head_limit as number) ?? MAX_LINES;
    const offset = (input.offset as number) ?? 0;
    const fileType = input.type as string | undefined;
    const beforeContext = input.before_context as number | undefined;
    const afterContext = input.after_context as number | undefined;
    const v = validatePatternAndPath(input.pattern, input.path as string | undefined, context.cwd);
    if (v.error) return v.error;
    const { pattern, resolved } = v;
    if (!pattern || pattern.length < 1) return { output: 'Error: pattern is required.', isError: true };

    // Try ripgrep first, fall back to grep
    const rgArgs = buildRgArgs(pattern, resolved, include, outputMode, caseInsensitive, contextLines, multiline, fileType, beforeContext, afterContext, headLimit, lineNumbers);
    let rgOutcome = await runSearch(rgArgs, 'rg', context, RG_TIMEOUT);

    if (rgOutcome.eagain) {
      rgArgs.push('-j', '1');
      rgOutcome = await runSearch(rgArgs, 'rg', context, RG_TIMEOUT);
    }

    if (rgOutcome.timedOut) {
      return { output: `Error: Search timed out after ${RG_TIMEOUT / 1000}s. Try a more specific pattern or path.`, isError: true };
    }

    if (rgOutcome.output !== null) {
      const cleaned = rgOutcome.output.replace(new RegExp(escapeRegex(resolved) + '/', 'g'), '');
      return formatResult(cleaned, pattern, headLimit, offset);
    }

    // Fallback to grep
    const grepOutcome = await runSearch(
      buildGrepArgs(pattern, resolved, include, outputMode, caseInsensitive, contextLines),
      'grep',
      context,
      RG_TIMEOUT,
    );

    if (grepOutcome.timedOut) {
      return { output: `Error: Search timed out after ${RG_TIMEOUT / 1000}s. Try a more specific pattern or path.`, isError: true };
    }

    if (grepOutcome.output !== null) {
      const cleaned = grepOutcome.output.replace(new RegExp(escapeRegex(resolved) + '/', 'g'), '');
      return formatResult(cleaned, pattern, headLimit, offset);
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
  outputMode: string,
  caseInsensitive: boolean,
  contextLines?: number,
  multiline?: boolean,
  fileType?: string,
  beforeContext?: number,
  afterContext?: number,
  headLimit?: number,
  lineNumbers?: boolean,
): string[] {
  const args = ['--color', 'never', '--max-columns', '500'];

  args.push('--hidden');
  args.push('--glob', '!.git', '--glob', '!.svn', '--glob', '!.hg', '--glob', '!.bzr', '--glob', '!.jj');

  if (outputMode === 'files_with_matches') {
    args.push('--files-with-matches');
  } else if (outputMode === 'count') {
    args.push('--count');
  } else {
    if (lineNumbers !== false) args.push('--line-number');
    args.push('--no-heading');
    if (headLimit === 0) { /* unlimited */ }
    else if (headLimit) args.push('--max-count', String(headLimit));
    if (contextLines && contextLines > 0) {
      args.push('-C', String(contextLines));
    }
  }

  if (caseInsensitive) {
    args.push('-i');
  }

  if (multiline) {
    args.push('-U', '--multiline-dotall');
  }

  args.push('--sortr', 'modified');

  if (include) {
    const globs = (include as string).split(',').map(g => g.trim());
    for (const g of globs) args.push('--glob', g);
  }

  if (fileType) args.push('--type', fileType);
  if (beforeContext != null) args.push('-B', String(beforeContext));
  if (afterContext != null) args.push('-A', String(afterContext));

  if (pattern.startsWith('-')) args.push('-e', pattern);
  else args.push(pattern);
  args.push(searchPath);
  return args;
}

function buildGrepArgs(
  pattern: string,
  searchPath: string,
  include: string | undefined,
  outputMode: string,
  caseInsensitive: boolean,
  contextLines?: number,
): string[] {
  const args: string[] = ['-r'];

  if (outputMode === 'files_with_matches') {
    args.push('-l');
  } else if (outputMode === 'count') {
    args.push('-c');
  } else {
    args.push('-n');
    if (contextLines && contextLines > 0) {
      args.push(`-C`, String(contextLines));
    }
  }

  if (caseInsensitive) {
    args.push('-i');
  }

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
  timeout?: number,
): Promise<SearchOutcome> {
  return new Promise<SearchOutcome>((resolve) => {
    let stdout = '';
    let stderr = '';
    let timedOut = false;

    let child: ReturnType<typeof spawn>;
    try {
      child = spawn(command, args, {
        cwd: context.cwd,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
    } catch {
      resolve({ output: null });
      return;
    }

    let timer: ReturnType<typeof setTimeout> | null = null;
    if (timeout && timeout > 0) {
      timer = setTimeout(() => {
        timedOut = true;
        child.kill('SIGKILL');
      }, timeout);
    }

    const onAbort = () => {
      if (timer) clearTimeout(timer);
      child.kill('SIGKILL');
    };

    if (context.abortSignal) {
      if (context.abortSignal.aborted) {
        if (timer) clearTimeout(timer);
        child.kill('SIGKILL');
        resolve({ output: '' });
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

    child.on('error', (err: any) => {
      if (timer) clearTimeout(timer);
      if (context.abortSignal) {
        context.abortSignal.removeEventListener('abort', onAbort);
      }
      if (err.code === 'EAGAIN') {
        resolve({ output: null, eagain: true });
        return;
      }
      resolve({ output: null });
    });

    child.on('close', (code: number | null) => {
      if (timer) clearTimeout(timer);
      if (context.abortSignal) {
        context.abortSignal.removeEventListener('abort', onAbort);
      }

      if (timedOut) {
        resolve({ output: stdout || null, timedOut: true });
        return;
      }

      // Exit code 1 for grep/rg means no matches (not an error)
      if (code === null || (code !== 0 && code !== 1)) {
        if (stderr.includes('not found') || stderr.includes('No such file')) {
          resolve({ output: null });
          return;
        }
      }

      resolve({ output: stdout });
    });
  });
}

function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function formatResult(output: string, pattern: string, limit: number = MAX_LINES, offset: number = 0): ToolResult {
  const trimmed = output.trim();

  if (trimmed.length === 0) {
    return { output: `No matches found for pattern: ${pattern}` };
  }

  const allLines = trimmed.split('\n');
  const totalLines = allLines.length;
  const effectiveLimit = limit === 0 ? totalLines : limit;
  const sliced = allLines.slice(offset, offset + effectiveLimit);

  if (sliced.length === 0) {
    return { output: `No matches in range (offset ${offset}, total ${totalLines})` };
  }

  const result = sliced.join('\n');
  const fileSet = new Set(allLines.map(l => l.split(':')[0]));
  const fileCount = fileSet.size;
  const matchCount = totalLines;
  const truncated = sliced.length < totalLines;
  const summary = `[${matchCount} matches in ${fileCount} files${truncated ? ' (truncated)' : ''}]`;

  if (offset > 0 || truncated) {
    return {
      output: `${summary}\n${result}\n\n... showing lines ${offset + 1}-${offset + sliced.length} of ${totalLines}`,
    };
  }

  return { output: `${summary}\n${result}` };
}
