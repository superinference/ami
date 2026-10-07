import { spawn } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
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
    // Ask for one past the page so formatResult can say the list was cut.
    // --max-count equal to head_limit hid later matches and dropped offset.
    const fetchLimit = headLimit === 0 ? 0 : offset + Math.max(headLimit, 1) + 1;
    const rgArgs = buildRgArgs(pattern, resolved, include, outputMode, caseInsensitive, contextLines, multiline, fileType, beforeContext, afterContext, fetchLimit, lineNumbers);
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

    // Slim eval images often have neither binary. Search in-process so the
    // agent can still locate the failing symbol.
    try {
      const fallback = searchFilesForPattern(resolved, pattern, {
        include,
        caseInsensitive,
        headLimit,
        offset,
      });
      return formatResult(fallback, pattern, headLimit, offset);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return { output: `Error: Search failed (${message}). Neither rg nor grep is available.`, isError: true };
    }
  },
};

const FALLBACK_SKIP_DIRS = new Set(['node_modules', '.git', 'vendor', 'dist', 'build', '.svn']);
const FALLBACK_MAX_FILES = 4000;
const FALLBACK_MAX_FILE_BYTES = 1_000_000;

/** Walk the tree when rg and grep are not installed. Output matches `path:line:text`. */
export function searchFilesForPattern(
  root: string,
  pattern: string,
  opts: { include?: string; caseInsensitive?: boolean; headLimit?: number; offset?: number },
): string {
  let re: RegExp;
  try {
    re = new RegExp(pattern, opts.caseInsensitive ? 'i' : '');
  } catch (err) {
    throw new Error(err instanceof Error ? err.message : String(err));
  }
  const includeTail = opts.include
    ? opts.include.slice(opts.include.lastIndexOf('*') + 1)
    : '';
  const matches: string[] = [];
  const page = opts.headLimit && opts.headLimit > 0 ? opts.headLimit : MAX_LINES;
  const cap = opts.headLimit === 0 ? Number.POSITIVE_INFINITY : (opts.offset ?? 0) + page + 1;
  let seen = 0;

  const walk = (dir: string): void => {
    if (matches.length >= cap || seen >= FALLBACK_MAX_FILES) return;
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      if (matches.length >= cap || seen >= FALLBACK_MAX_FILES) return;
      if (entry.isDirectory()) {
        if (FALLBACK_SKIP_DIRS.has(entry.name)) continue;
        walk(path.join(dir, entry.name));
        continue;
      }
      if (!entry.isFile()) continue;
      if (includeTail && !entry.name.endsWith(includeTail) && entry.name !== opts.include) {
        continue;
      }
      const filePath = path.join(dir, entry.name);
      seen++;
      let text: string;
      try {
        const stat = fs.statSync(filePath);
        if (stat.size > FALLBACK_MAX_FILE_BYTES) continue;
        text = fs.readFileSync(filePath, 'utf8');
      } catch {
        continue;
      }
      const rel = path.relative(root, filePath) || entry.name;
      const fileLines = text.split('\n');
      for (let i = 0; i < fileLines.length; i++) {
        if (matches.length >= cap) return;
        if (re.test(fileLines[i])) {
          matches.push(`${rel}:${i + 1}:${fileLines[i]}`);
        }
      }
    }
  };

  let rootStat: fs.Stats;
  try {
    rootStat = fs.statSync(root);
  } catch {
    return '';
  }
  if (rootStat.isFile()) {
    const text = fs.readFileSync(root, 'utf8');
    const rel = path.basename(root);
    text.split('\n').forEach((line, i) => {
      if (re.test(line)) matches.push(`${rel}:${i + 1}:${line}`);
    });
    return matches.join('\n');
  }
  walk(root);
  return matches.join('\n');
}

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
  // -E so `|` is alternation. Basic grep treats it as a literal, which made
  // `BLP|Bell` and `blp_model|blp_policy` report no matches in the testbed.
  const args: string[] = ['-r', '-E'];

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

      // Exit code 1 means no matches. Any other failure with no stdout
      // (missing binary, invalid regex) tries the next searcher.
      if (code !== 0 && code !== 1 && !stdout.trim()) {
        resolve({ output: null });
        return;
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
