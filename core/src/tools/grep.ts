import { spawn } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validatePatternAndPath, assertReadableTarget, rejectEscapingPattern, isUNCPath } from './tool-utils';
import { suggestSimilarPaths } from './path-suggest';

const MAX_LINES = 250;
const RG_TIMEOUT = 20_000;
// Hard ceiling on bytes accumulated from a spawned searcher's stdout. A pattern
// matching an enormous tree could otherwise stream gigabytes into memory before
// the result is ever truncated to MAX_LINES (OOM / DoS). Once exceeded we stop
// buffering, kill the child, and flag the output truncated.
export const MAX_SEARCH_OUTPUT_BYTES = 10 * 1024 * 1024; // 10 MB
// After SIGKILL on timeout/abort, a wedged process may never emit 'close' (seen on
// Windows and for uninterruptible I/O). Resolve anyway after this grace period so
// the tool can never hang forever.
const KILL_GRACE_MS = 2_000;

/**
 * Indirection seam for process spawning and the search timeout. Tests swap
 * `spawn` for a fake child and shrink `timeoutMs` to exercise the timeout,
 * abort, EAGAIN and signal branches deterministically. Production code uses the
 * real `child_process.spawn` and the 20s default, so behavior is unchanged.
 */
export const grepHooks: {
  spawn: typeof spawn;
  timeoutMs: number;
  killGraceMs: number;
  searchFilesForPattern: typeof searchFilesForPattern;
} = {
  spawn,
  timeoutMs: RG_TIMEOUT,
  killGraceMs: KILL_GRACE_MS,
  // Hoisted function declaration (defined later in this module).
  searchFilesForPattern,
};

// eslint-disable-next-line security/detect-unsafe-regex -- [lcrniw] and \s are disjoint sets, no overlap
const GREP_FLAG_RE = /^(-[lcrniw]+\s+)+/;

export function stripGrepFlags(pattern: string): string {
  const stripped = pattern.replace(GREP_FLAG_RE, '');
  return stripped || pattern;
}

interface SearchOutcome {
  output: string | null;
  timedOut?: boolean;
  eagain?: boolean;
  /** True when stdout hit MAX_SEARCH_OUTPUT_BYTES and buffering was cut short. */
  outputTruncated?: boolean;
  /** Non-null when the search process exited with an error (code > 1) or was killed by a signal. */
  searchError?: string;
}

/**
 * Returns true when the exit code means "no matches" (exit code 1 for grep/rg/ag).
 * Exit code 0 means matches were found; any code > 1 signals an actual error
 * (2 = invalid regex or permission error, 127 = command not found, etc.).
 */
export function isGrepNoMatchExit(exitCode: number): boolean {
  return exitCode === 1;
}

/**
 * ripgrep in a resource-constrained container (Docker/CI with a low thread/RLIMIT
 * budget) cannot spawn its worker threads: it exits NON-zero and writes 'os error 11'
 * / 'Resource temporarily unavailable' to STDERR. The Node `spawn` itself succeeds, so
 * no `error` event with code 'EAGAIN' fires — the only signal is rg's own stderr.
 * Detecting it lets grep retry once single-threaded (`-j 1`).
 */
export function isEagainStderr(stderr: string): boolean {
  return /os error 11\b|Resource temporarily unavailable/i.test(stderr);
}

/**
 * Split a grep `include` file-filter into individual rg `--glob` patterns, splitting
 * on whitespace but keeping brace groups intact: "*.{ts,tsx}" stays whole (a single
 * rg glob), while a brace-free "*.ts,*.js" is comma-split into two globs. A naive
 * split(',') would shred "*.{ts,tsx}" into "*.{ts" and "tsx}" and match nothing.
 */
export function splitGlobFilter(include: string): string[] {
  const out: string[] = [];
  for (const frag of include.split(/\s+/).filter(Boolean)) {
    if (frag.includes('{') || frag.includes('}')) {
      out.push(frag);
    } else {
      for (const part of frag.split(',')) {
        const t = part.trim();
        if (t) out.push(t);
      }
    }
  }
  return out;
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
          'Glob pattern to filter files, e.g. "*.ts" or "*.py". Brace groups like "*.{ts,tsx}" are kept intact; space/comma-separate multiple globs.',
      },
      glob: {
        type: 'string',
        description:
          'Alias for include (Claude Code canonical name). Glob pattern to filter files, e.g. "*.{ts,tsx}".',
      },
      fixed_strings: {
        type: 'boolean',
        description: 'Match the pattern as a literal fixed string instead of a regex (ripgrep -F). Default false.',
        default: false,
      },
      output_mode: {
        type: 'string',
        enum: ['content', 'files_with_matches', 'count'],
        description: 'Output format: files_with_matches (default, file names only — more token-efficient), content (matching lines with context), count (match counts per file).',
        default: 'files_with_matches',
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
        description: 'Maximum number of result lines to return. Defaults to 250 when unspecified. Pass 0 for unlimited (use sparingly — large result sets waste context).',
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
    // `glob` is Claude Code's canonical name for the file-filter param; accept both.
    const include = (input.include ?? input.glob) as string | undefined;
    const outputMode = (input.output_mode as string) || 'files_with_matches';
    const fixedStrings = (input.fixed_strings as boolean) ?? false;
    const lineNumbers = input.line_numbers as boolean | undefined;
    const caseInsensitive = (input.case_insensitive as boolean) ?? false;
    const contextLines = input.context_lines as number | undefined;
    const multiline = (input.multiline as boolean) ?? false;
    const headLimit = (input.head_limit as number) ?? MAX_LINES;
    const offset = (input.offset as number) ?? 0;
    const fileType = input.type as string | undefined;
    const beforeContext = input.before_context as number | undefined;
    const afterContext = input.after_context as number | undefined;
    const searchPath = (input.path ?? input.file_path) as string | undefined;
    // UNC-path guard: short-circuit BEFORE any filesystem access (NTLM leak risk).
    if (typeof searchPath === 'string' && isUNCPath(searchPath)) {
      return { output: `Error: UNC paths are not allowed (potential NTLM credential leak): ${searchPath}`, isError: true };
    }
    const v = validatePatternAndPath(input.pattern, searchPath, context.cwd, context.allowedReadRoots);
    if (v.error) return v.error;
    const { pattern, resolved } = v;
    // The file-FILTER glob (`include`) must not traverse above the search root — a filter
    // like "../../**" would widen the search out of the workspace. (grep's `pattern` is a
    // REGEX and is intentionally NOT path-checked.)
    if (typeof include === 'string') {
      const includeEscape = rejectEscapingPattern(include);
      if (includeEscape) return includeEscape;
    }
    // Self-correcting error when the given search path does not exist.
    if (typeof searchPath === 'string' && !fs.existsSync(resolved)) {
      const suggestions = suggestSimilarPaths(resolved, context.cwd);
      const hint = suggestions.length > 0 ? ` Did you mean ${suggestions[0]}?` : '';
      return { output: `Error: Path does not exist: ${resolved}. Your current working directory is ${context.cwd}.${hint}`, isError: true };
    }
    const unreadable = assertReadableTarget(resolved);
    if (unreadable) return unreadable;
    if (!pattern || pattern.length < 1) return { output: 'Error: pattern is required.', isError: true };

    // Strip common grep flags the model sometimes embeds in the pattern string
    // (e.g. "-l pattern" or "-r -c pattern"). We handle these via our own args.
    const cleanedPattern = stripGrepFlags(pattern);

    // Try ripgrep first, fall back to grep
    // Ask for one past the page so formatResult can say the list was cut.
    // --max-count equal to head_limit hid later matches and dropped offset.
    const fetchLimit = headLimit === 0 ? 0 : offset + Math.max(headLimit, 1) + 1;
    const rgArgs = buildRgArgs(cleanedPattern, resolved, include, outputMode, caseInsensitive, contextLines, multiline, fileType, beforeContext, afterContext, fetchLimit, lineNumbers, fixedStrings);
    let rgOutcome = await runSearch(rgArgs, 'rg', context, grepHooks.timeoutMs);

    if (rgOutcome.eagain) {
      rgArgs.push('-j', '1');
      rgOutcome = await runSearch(rgArgs, 'rg', context, grepHooks.timeoutMs);
    }

    if (rgOutcome.timedOut) {
      return { output: `Error: Search timed out after ${RG_TIMEOUT / 1000}s. Try a more specific pattern or path.`, isError: true };
    }

    if (rgOutcome.output !== null) {
      const cleaned = rgOutcome.output.replace(new RegExp(escapeRegex(resolved) + '/', 'g'), '');
      const result = formatResult(cleaned, pattern, headLimit, offset, outputMode);
      if (rgOutcome.outputTruncated && !result.isError) {
        result.output += `\n\n[Warning: search output exceeded ${MAX_SEARCH_OUTPUT_BYTES / 1024 / 1024}MB and was cut short — narrow the pattern or path for complete results.]`;
      }
      if (rgOutcome.searchError && !result.isError) {
        result.output += `\n\n[Warning: search exited with error: ${rgOutcome.searchError}]`;
      }
      return result;
    }

    // Track search errors for reporting when all searchers fail
    const searchErrors: string[] = [];
    if (rgOutcome.searchError) searchErrors.push(`rg: ${rgOutcome.searchError}`);

    // Fallback to grep
    const grepOutcome = await runSearch(
      buildGrepArgs(cleanedPattern, resolved, include, outputMode, caseInsensitive, contextLines, fixedStrings),
      'grep',
      context,
      grepHooks.timeoutMs,
    );

    if (grepOutcome.timedOut) {
      return { output: `Error: Search timed out after ${RG_TIMEOUT / 1000}s. Try a more specific pattern or path.`, isError: true };
    }

    if (grepOutcome.output !== null) {
      const cleaned = grepOutcome.output.replace(new RegExp(escapeRegex(resolved) + '/', 'g'), '');
      const result = formatResult(cleaned, pattern, headLimit, offset, outputMode);
      if (grepOutcome.outputTruncated && !result.isError) {
        result.output += `\n\n[Warning: search output exceeded ${MAX_SEARCH_OUTPUT_BYTES / 1024 / 1024}MB and was cut short — narrow the pattern or path for complete results.]`;
      }
      if (grepOutcome.searchError && !result.isError) {
        result.output += `\n\n[Warning: search exited with error: ${grepOutcome.searchError}]`;
      }
      return result;
    }
    if (grepOutcome.searchError) searchErrors.push(`grep: ${grepOutcome.searchError}`);

    // Slim eval images often have neither binary. Search in-process so the
    // agent can still locate the failing symbol (and get a better error for
    // invalid regex patterns than the raw process stderr).
    try {
      const fallback = grepHooks.searchFilesForPattern(resolved, cleanedPattern, {
        include,
        caseInsensitive,
        headLimit,
        offset,
      });
      return formatResult(fallback, pattern, headLimit, offset, outputMode);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (/invalid regular expression|unterminated|invalid group/i.test(message)) {
        return {
          output: `Error: Invalid regular expression (${message}). Escape parentheses and other regex metacharacters, or search for a literal.`,
          isError: true,
        };
      }
      // Surface the external searcher errors when the in-process fallback also fails
      if (searchErrors.length > 0) {
        return {
          output: `Error: Search failed.\n${searchErrors.join('\n')}`,
          isError: true,
        };
      }
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
  fixedStrings?: boolean,
): string[] {
  const args = ['--color', 'never', '--max-columns', '500'];
  // Literal search: match the query as a fixed string, not a regex.
  if (fixedStrings) args.push('-F');

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
    for (const g of splitGlobFilter(include)) args.push('--glob', g);
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
  fixedStrings?: boolean,
): string[] {
  // -E so `|` is alternation. Basic grep treats it as a literal, which made
  // `BLP|Bell` and `blp_model|blp_policy` report no matches in the testbed.
  // With fixedStrings, use -F (literal) instead so metacharacters match verbatim.
  const args: string[] = fixedStrings ? ['-r', '-F'] : ['-r', '-E'];

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

export function runSearch(
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
      child = grepHooks.spawn(command, args, {
        cwd: context.cwd,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
    } catch {
      resolve({ output: null });
      return;
    }

    let timer: ReturnType<typeof setTimeout> | null = null;
    let graceTimer: ReturnType<typeof setTimeout> | null = null;
    let settled = false;
    // Resolve exactly once; clear timers and the abort listener. 'close', 'error',
    // timeout and abort all route through here so none can double-resolve or leak.
    const onAbort = () => { if (timer) clearTimeout(timer); killThenSettle({ output: '' }); };
    const settle = (outcome: SearchOutcome) => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      if (graceTimer) clearTimeout(graceTimer);
      if (context.abortSignal) context.abortSignal.removeEventListener('abort', onAbort);
      resolve(outcome);
    };
    // SIGKILL, then settle after a grace period even if 'close' never fires (a
    // wedged process may not emit it — the "search never returns" hang).
    const killThenSettle = (outcome: SearchOutcome) => {
      try { child.kill('SIGKILL'); } catch { /* already gone */ }
      graceTimer = setTimeout(() => settle(outcome), grepHooks.killGraceMs);
    };
    if (timeout && timeout > 0) {
      timer = setTimeout(() => {
        timedOut = true;
        killThenSettle({ output: stdout || null, timedOut: true });
      }, timeout);
    }

    if (context.abortSignal) {
      if (context.abortSignal.aborted) {
        if (timer) clearTimeout(timer);
        try { child.kill('SIGKILL'); } catch { /* already gone */ }
        settle({ output: '' });
        return;
      }
      context.abortSignal.addEventListener('abort', onAbort, { once: true });
    }

    let outputTruncated = false;
    child.stdout!.on('data', (data: Buffer) => {
      if (outputTruncated) return;
      stdout += data.toString();
      if (Buffer.byteLength(stdout) > MAX_SEARCH_OUTPUT_BYTES) {
        // Stop buffering and kill the child so it can't keep streaming into memory.
        outputTruncated = true;
        killThenSettle({ output: stdout, outputTruncated: true });
      }
    });

    child.stderr!.on('data', (data: Buffer) => {
      stderr += data.toString();
    });

    child.on('error', (err: any) => {
      if (err.code === 'EAGAIN') {
        settle({ output: null, eagain: true });
        return;
      }
      settle({ output: null });
    });

    child.on('close', (code: number | null, signal: string | null) => {
      if (timedOut) {
        settle({ output: stdout || null, timedOut: true });
        return;
      }

      // Killed by a signal (SIGTERM, SIGKILL, etc.) — not a normal exit.
      if (signal) {
        settle({ output: stdout || null, searchError: `Search was terminated by signal ${signal}` });
        return;
      }

      // Exit code 1 means no matches — a normal result, not an error.
      if (code === 0 || isGrepNoMatchExit(code ?? 0)) {
        settle({ output: stdout });
        return;
      }

      // Non-zero exit caused by rg failing to spawn worker threads (EAGAIN) — detected
      // from rg's own stderr, since no 'error' event fires in this mode. Retry single-threaded.
      if (command === 'rg' && isEagainStderr(stderr)) {
        settle({ output: null, eagain: true });
        return;
      }

      // Exit code > 1: actual error (invalid regex, permission denied, etc.).
      // If we got some stdout, return it but note the error. If no stdout,
      // signal that this searcher failed so we try the next one.
      const errMsg = stderr.trim() || `Search process exited with code ${code}`;
      if (!stdout.trim()) {
        settle({ output: null, searchError: errMsg });
        return;
      }
      settle({ output: stdout, searchError: errMsg });
    });
  });
}

function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function isSuiteLogLine(line: string): boolean {
  return /"Action"\s*:/.test(line) && /"(?:Test|Package|Elapsed)"/.test(line);
}

function formatResult(output: string, pattern: string, limit: number = MAX_LINES, offset: number = 0, outputMode: string = 'content'): ToolResult {
  const kept = output.split('\n').filter(line => line.trim().length > 0 && !isSuiteLogLine(line));
  const trimmed = kept.join('\n').trim();

  if (trimmed.length === 0) {
    return { output: `No matches found for pattern: ${pattern}` };
  }

  // Count mode: aggregate each `path:N` line into a natural-language summary with
  // correct singular/plural grammar, preserving the per-file breakdown below it.
  if (outputMode === 'count') {
    const lines = trimmed.split('\n');
    let totalMatches = 0;
    let fileCount = 0;
    for (const line of lines) {
      const m = line.match(/:(\d+)\s*$/);
      if (m) { totalMatches += Number(m[1]); fileCount++; }
    }
    if (fileCount > 0) {
      const occ = totalMatches === 1 ? 'occurrence' : 'occurrences';
      const fileWord = fileCount === 1 ? 'file' : 'files';
      return { output: `Found ${totalMatches} total ${occ} across ${fileCount} ${fileWord}.\n${lines.join('\n')}` };
    }
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
  const truncNote = truncated ? ' (truncated)' : '';
  // files_with_matches lists one file per line (no per-match lines), so the count
  // IS the file count — word it accordingly with singular/plural grammar. Content
  // mode counts matches across files.
  const summary = outputMode === 'files_with_matches'
    ? `[Found ${fileCount} ${fileCount === 1 ? 'file' : 'files'}${truncNote}]`
    : `[${matchCount} ${matchCount === 1 ? 'match' : 'matches'} in ${fileCount} ${fileCount === 1 ? 'file' : 'files'}${truncNote}]`;

  if (offset > 0 || truncated) {
    return {
      output: `${summary}\n${result}\n\n... showing lines ${offset + 1}-${offset + sliced.length} of ${totalLines}`,
    };
  }

  return { output: `${summary}\n${result}` };
}
