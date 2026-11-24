import { spawn } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { isWSL } from '../utils/env';
import { validatePatternAndPath, assertReadableTarget, rejectEscapingPattern, isUNCPath, ORPHANED_PLUGIN_HOLD_DIRNAME } from './tool-utils';
import { suggestSimilarPaths } from './path-suggest';
import { coerceBool } from '../utils/coerce';
import { resolveRipgrepConfig } from './ripgrep-config';
import { matchesReadIgnore } from './read-ignore';
import { escapeRegExp } from '../utils/string';
import { FALLBACK_MAX_FILES, FALLBACK_MAX_FILE_BYTES, KILL_GRACE_MS, MAX_LINES, SIGKILL_ESCALATION_MS, buildGrepArgs, buildRgArgs, formatResult, grepTimeoutMs, isEagainStderr, isGrepNoMatchExit, optionalBool, optionalNumber, sortFilesWithMatchesByMtime, splitStreamLines, stripGrepFlags, stripTrailingCR } from './grep-sort-files-with-matches-by-mtime';
import type { SearchOutcome } from './grep-sort-files-with-matches-by-mtime';
export { grepLineFilePath, grepTimeoutMs, isEagainStderr, isGrepNoMatchExit, sortFilesWithMatchesByMtime, splitGlobFilter, splitStreamLines, stripGrepFlags, stripTrailingCR } from './grep-sort-files-with-matches-by-mtime';

/** True when running under WSL (slow /mnt bridge). Re-exported from the canonical
 *  {@link isWSL} in utils/env so the ripgrep timeout and the session-lock cross-OS
 *  reclaim guard share ONE detector (§0b.6 — no duplicated platform probe). */
export { isWSL };
// Hard ceiling on bytes accumulated from a spawned searcher's stdout. A pattern
// matching an enormous tree could otherwise stream gigabytes into memory before
// the result is ever truncated to MAX_LINES (OOM / DoS). Once exceeded we stop
// buffering, kill the child, and flag the output truncated.
export const MAX_SEARCH_OUTPUT_BYTES = 10 * 1024 * 1024; // 10 MB

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
  sigkillEscalationMs: number;
  platform: NodeJS.Platform;
  searchFilesForPattern: typeof searchFilesForPattern;
  inProcessBudgetMs: number;
} = {
  spawn,
  timeoutMs: grepTimeoutMs(),
  killGraceMs: KILL_GRACE_MS,
  sigkillEscalationMs: SIGKILL_ESCALATION_MS,
  platform: process.platform,
  // Hoisted function declaration (defined later in this module).
  searchFilesForPattern,
  // Wall-clock budget for the in-process fallback walk. A catastrophic-backtracking
  // (ReDoS) pattern against a huge tree can't hang the process forever: the walk
  // aborts once this elapses. Mirrors the subprocess RG_TIMEOUT for the no-rg/no-grep
  // path. Tests shrink it to exercise the deadline branch deterministically.
  inProcessBudgetMs: grepTimeoutMs(),
};

export const grepTool: ToolDefinition = {
  name: 'grep',
  description:
    `A powerful search tool built on ripgrep (with graceful grep and in-process fallbacks).

Usage:
- Supports full regex syntax (e.g. "log.*Error", "function\\s+\\w+").
- Filter files with the \`glob\`/\`include\` parameter (e.g. "*.js", "**/*.tsx") or the \`type\` parameter (e.g. "js", "py", "rust", "go", "java") — \`type\` is more efficient for standard file types.
- Output modes: "content" shows matching lines, "files_with_matches" shows only file paths (default, most token-efficient), "count" shows match counts per file.
- Pattern syntax uses ripgrep (not basic grep): literal braces need escaping (use "interface\\{\\}" to find "interface{}" in Go code).
- Multiline matching: by default patterns match within a single line only. For cross-line patterns like "struct \\{[\\s\\S]*?field", pass multiline: true.
- Prefer this tool over invoking \`grep\`/\`rg\` through the bash tool: it enforces read permissions, VCS exclusions and result caps, and returns matching lines with file paths and line numbers.
- For an open-ended search that needs multiple rounds of grep/glob (you are not confident a single pattern will find the right match), delegate to the \`task\` tool (subagent) instead of running many searches yourself.`,
  inputSchema: {
    type: 'object',
    properties: {
      pattern: {
        type: 'string',
        description: 'The regex pattern to search for, using ripgrep regex syntax — e.g. "log.*Error" or "function\\s+\\w+". Escape literal regex metacharacters: use "interface\\{\\}" to match the text "interface{}". For patterns that span multiple lines (e.g. "struct \\{[\\s\\S]*?field"), set multiline: true. To match a literal string with no regex interpretation, set fixed_strings: true instead.',
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
          'Alias for include (the reference canonical name). Glob pattern to filter files, e.g. "*.{ts,tsx}".',
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
        description: 'Number of context lines before and after each match (rg -C). Requires output_mode: "content", ignored otherwise.',
      },
      multiline: {
        type: 'boolean',
        description: 'Enable multiline matching (dot matches newlines). Only supported with ripgrep.',
        default: false,
      },
      line_numbers: {
        type: 'boolean',
        description: 'Show line numbers in output (rg -n). Requires output_mode: "content", ignored otherwise. Default true.',
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
        description: 'Lines of context before each match (rg -B). Requires output_mode: "content", ignored otherwise. Overrides context_lines for before.',
      },
      after_context: {
        type: 'number',
        description: 'Lines of context after each match (rg -A). Requires output_mode: "content", ignored otherwise. Overrides context_lines for after.',
      },
      '-A': {
        type: 'number',
        description: 'Alias for after_context (the reference canonical name). Lines of context after each match (rg -A). Requires output_mode: "content".',
      },
      '-B': {
        type: 'number',
        description: 'Alias for before_context (the reference canonical name). Lines of context before each match (rg -B). Requires output_mode: "content".',
      },
      '-C': {
        type: 'number',
        description: 'Alias for context_lines (the reference canonical name). Lines of context before and after each match (rg -C). Requires output_mode: "content".',
      },
      '-n': {
        type: 'boolean',
        description: 'Alias for line_numbers (the reference canonical name). Show line numbers (rg -n). Requires output_mode: "content".',
      },
      '-i': {
        type: 'boolean',
        description: 'Alias for case_insensitive (the reference canonical name). Case-insensitive matching (rg -i).',
      },
    },
    required: ['pattern'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,
  // Curated discovery phrase weighted by `tool_search` above the prose description:
  // surfaces capability synonyms NOT already in the name/description ("contents",
  // "regex") so the model can find grep by intent even when it is deferred. Parity:
  // the reference's GrepTool `searchHint: 'search file contents with regex (ripgrep)'`.
  searchHint: 'search file contents with regex (ripgrep)',

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    // `glob` is the reference's canonical name for the file-filter param; accept both.
    const include = (input.include ?? input.glob) as string | undefined;
    const outputMode = (input.output_mode as string) || 'files_with_matches';
    // Numeric/boolean params are coerced from the string literals providers often
    // serialize (`"true"`, `"5"`) — a model emitting `"head_limit":"5"` must behave
    // like `5`, and `"case_insensitive":"false"` must be false (not truthy). Mirrors
    // the reference's semanticNumber/semanticBoolean coercion.
    // Accept the reference's canonical single-letter aliases (-A/-B/-C/-n/-i) as
    // synonyms for AMI's descriptive names, so a tool call authored against the
    // the reference schema runs unchanged. The descriptive name wins when both are given.
    const fixedStrings = coerceBool(input.fixed_strings);
    const lineNumbers = optionalBool(input.line_numbers ?? input['-n']);
    const caseInsensitive = coerceBool(input.case_insensitive ?? input['-i']);
    const contextLines = optionalNumber(input.context_lines ?? input['-C']);
    const multiline = coerceBool(input.multiline);
    const headLimit = optionalNumber(input.head_limit) ?? MAX_LINES;
    const offset = optionalNumber(input.offset) ?? 0;
    const fileType = input.type as string | undefined;
    const beforeContext = optionalNumber(input.before_context ?? input['-B']);
    const afterContext = optionalNumber(input.after_context ?? input['-A']);
    const searchPath = (input.path ?? input.file_path) as string | undefined;
    // UNC-path guard: short-circuit BEFORE any filesystem access (NTLM leak risk).
    if (typeof searchPath === 'string' && isUNCPath(searchPath)) {
      return { output: `Error: UNC paths are not allowed (potential NTLM credential leak): ${searchPath}`, isError: true };
    }
    const v = validatePatternAndPath(input.pattern, searchPath, context.cwd, context.allowedReadRoots, 'search');
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
    // Resolve which ripgrep binary to spawn. A config-level sandbox.ripgrep override wins
    // over the USE_BUILTIN_RIPGREP / AMI_RIPGREP_PATH env waterfall (shared with the glob
    // tool). Falls back to the bare name `rg`.
    const rgConfig = resolveRipgrepConfig(process.env, context.sandbox?.ripgrep);
    const rgCommand = rgConfig.command;
    // File-READ deny globs the permission layer wants excluded from search output, so a
    // file the user forbade reading never surfaces (its path OR its matching lines) in any
    // output mode. Threaded into every search path (rg, grep, in-process). Parity: the reference's
    // getFileReadIgnorePatterns → `--glob !pattern` injection.
    const readIgnore = context.readIgnorePatterns ?? [];
    const rgArgs = buildRgArgs(cleanedPattern, resolved, include, outputMode, caseInsensitive, contextLines, multiline, fileType, beforeContext, afterContext, fetchLimit, lineNumbers, fixedStrings, readIgnore);
    // Prepend any config-supplied extra ripgrep flags (sandbox.ripgrep.args).
    if (rgConfig.args?.length) rgArgs.unshift(...rgConfig.args);
    let rgOutcome = await runSearch(rgArgs, rgCommand, context, grepHooks.timeoutMs, rgConfig.argv0);

    if (rgOutcome.eagain) {
      rgArgs.push('-j', '1');
      rgOutcome = await runSearch(rgArgs, rgCommand, context, grepHooks.timeoutMs, rgConfig.argv0);
    }

    if (rgOutcome.timedOut) {
      const partial = partialOnTimeout(rgOutcome.output, resolved, pattern, headLimit, offset, outputMode);
      if (partial) return partial;
      return { output: `Error: Search timed out after ${grepHooks.timeoutMs / 1000}s. Try a more specific pattern or path.`, isError: true };
    }

    if (rgOutcome.output !== null) {
      // files_with_matches is recency-ordered in JS (stat sort) rather than via
      // ripgrep's `--sortr`, so rg itself ran multi-threaded (see buildRgArgs).
      const ordered = outputMode === 'files_with_matches'
        ? sortFilesWithMatchesByMtime(rgOutcome.output)
        : rgOutcome.output;
      const cleaned = ordered.replace(new RegExp(escapeRegExp(resolved) + '/', 'g'), '');
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
      buildGrepArgs(cleanedPattern, resolved, include, outputMode, caseInsensitive, contextLines, fixedStrings, readIgnore),
      'grep',
      context,
      grepHooks.timeoutMs,
    );

    if (grepOutcome.timedOut) {
      const partial = partialOnTimeout(grepOutcome.output, resolved, pattern, headLimit, offset, outputMode);
      if (partial) return partial;
      return { output: `Error: Search timed out after ${grepHooks.timeoutMs / 1000}s. Try a more specific pattern or path.`, isError: true };
    }

    if (grepOutcome.output !== null) {
      // Same JS-side recency ordering for the grep-binary fallback, which otherwise
      // returns files in filesystem/readdir order (gap: inconsistent with the rg path).
      const ordered = outputMode === 'files_with_matches'
        ? sortFilesWithMatchesByMtime(grepOutcome.output)
        : grepOutcome.output;
      const cleaned = ordered.replace(new RegExp(escapeRegExp(resolved) + '/', 'g'), '');
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
        readIgnore,
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

// Keep the VCS metadata set aligned with buildRgArgs' --glob exclusions (git, svn, hg,
// bzr, jj, sl) so the in-process fallback never searches an internal object store that
// the ripgrep path skips.
const FALLBACK_SKIP_DIRS = new Set(['node_modules', '.git', 'vendor', 'dist', 'build', '.svn', '.hg', '.bzr', '.jj', '.sl', ORPHANED_PLUGIN_HOLD_DIRNAME]);

/** Walk the tree when rg and grep are not installed. Output matches `path:line:text`. */
export function searchFilesForPattern(
  root: string,
  pattern: string,
  opts: { include?: string; caseInsensitive?: boolean; headLimit?: number; offset?: number; readIgnore?: readonly string[] },
): string {
  const readIgnore = opts.readIgnore ?? [];
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
  // Wall-clock deadline: a pathological (ReDoS) pattern or an enormous tree can't
  // pin the process — the walk and per-line matching bail once this elapses.
  const deadline = Date.now() + Math.max(grepHooks.inProcessBudgetMs, 0);
  // `>=` so a zero budget aborts within the same millisecond (and a real multi-second
  // budget only trips once the elapsed time actually reaches it).
  const expired = (): boolean => Date.now() >= deadline;

  // Phase 1: collect candidate files (respecting VCS/skip dirs, the include filter,
  // and the file-count ceiling), capturing each file's mtime so matches can be
  // returned newest-first — mirroring the ripgrep path's `--sortr modified` ordering
  // (a bare directory walk otherwise returns readdir order, which varies by OS/FS).
  const candidates: { filePath: string; mtimeMs: number; size: number }[] = [];
  const collect = (dir: string): void => {
    if (candidates.length >= FALLBACK_MAX_FILES || expired()) return;
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      if (candidates.length >= FALLBACK_MAX_FILES || expired()) return;
      if (entry.isDirectory()) {
        if (FALLBACK_SKIP_DIRS.has(entry.name)) continue;
        collect(path.join(dir, entry.name));
        continue;
      }
      if (!entry.isFile()) continue;
      if (includeTail && !entry.name.endsWith(includeTail) && entry.name !== opts.include) {
        continue;
      }
      const filePath = path.join(dir, entry.name);
      // Drop files the permission layer forbids reading BEFORE they are stat'd/read, so a
      // denied file never surfaces (path or contents) on the in-process fallback path.
      if (readIgnore.length > 0 && matchesReadIgnore(path.relative(root, filePath) || entry.name, readIgnore)) {
        continue;
      }
      try {
        // TOCTOU guard: `lstat` (not `stat`) so a regular file swapped for a symlink
        // between readdir and now is detected and skipped — we never follow a symlink
        // planted mid-walk out of the tree. Mirrors outputsScanner's post-readdir
        // lstat re-check and AMI's other file tools.
        const stat = fs.lstatSync(filePath);
        if (stat.isSymbolicLink() || !stat.isFile()) continue;
        candidates.push({ filePath, mtimeMs: stat.mtimeMs, size: stat.size });
      } catch {
        // A stat failure (deleted between readdir and stat, permission) sorts last.
        candidates.push({ filePath, mtimeMs: 0, size: 0 });
      }
    }
  };

  const searchFile = (filePath: string, size: number): void => {
    if (size > FALLBACK_MAX_FILE_BYTES) return;
    let text: string;
    try {
      // Re-check immediately before reading: close the collect→read window where the
      // candidate could be replaced by a symlink pointing outside the tree (TOCTOU).
      if (fs.lstatSync(filePath).isSymbolicLink()) return;
      text = fs.readFileSync(filePath, 'utf8');
    } catch {
      return;
    }
    const rel = path.relative(root, filePath) || path.basename(filePath);
    const fileLines = text.split('\n');
    for (let i = 0; i < fileLines.length; i++) {
      if (matches.length >= cap || expired()) return;
      if (re.test(fileLines[i])) {
        matches.push(`${rel}:${i + 1}:${fileLines[i]}`);
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
    // A single denied file passed directly as the search path yields nothing.
    if (readIgnore.length > 0 && matchesReadIgnore(path.basename(root), readIgnore)) return '';
    const text = fs.readFileSync(root, 'utf8');
    const rel = path.basename(root);
    text.split('\n').forEach((line, i) => {
      if (re.test(line)) matches.push(`${rel}:${i + 1}:${line}`);
    });
    return matches.join('\n');
  }
  collect(root);
  // Phase 2: newest-first by mtime, with a filename tiebreak so equal-mtime files
  // (and the test harness, where files are written in the same instant) are ordered
  // deterministically.
  candidates.sort((a, b) => (b.mtimeMs - a.mtimeMs) || a.filePath.localeCompare(b.filePath));
  for (const c of candidates) {
    if (matches.length >= cap || expired()) break;
    searchFile(c.filePath, c.size);
  }
  return matches.join('\n');
}

export function runSearch(
  args: string[],
  command: string,
  context: ToolContext,
  timeout?: number,
  argv0?: string,
): Promise<SearchOutcome> {
  return new Promise<SearchOutcome>((resolve) => {
    let stdout = '';
    let stderr = '';
    let timedOut = false;
    // Early-result streaming: when the caller provides a progress sink, forward
    // COMPLETE result lines as each stdout chunk arrives (incremental paint) while
    // still accumulating the authoritative buffered `stdout` the final result is
    // parsed from. A partial trailing line is carried in `progressRemainder` across
    // chunk boundaries and flushed only on a clean close — never a torn tail from a
    // killed/aborted/timed-out process. Mirrors the reference's `ripGrepStream`.
    const onProgress = context.onProgress;
    let progressRemainder = '';
    const streamProgress = (chunk: string): void => {
      if (!onProgress) return;
      const split = splitStreamLines(progressRemainder, chunk);
      progressRemainder = split.remainder;
      for (const line of split.lines) onProgress(line);
    };

    let child: ReturnType<typeof spawn>;
    try {
      // Strip RIPGREP_CONFIG_PATH from the child env so a planted config file can't
      // inject flags into the search (paired with the `--no-config` rg arg).
      const childEnv = { ...process.env };
      delete childEnv.RIPGREP_CONFIG_PATH;
      child = grepHooks.spawn(command, args, {
        cwd: context.cwd,
        stdio: ['ignore', 'pipe', 'pipe'],
        env: childEnv,
        // Explicit argv[0] for a multi-call ripgrep binary (sandbox.ripgrep.argv0);
        // omitted → the OS default (argv[0] === the spawned path).
        ...(argv0 ? { argv0 } : {}),
      });
    } catch {
      resolve({ output: null });
      return;
    }

    let timer: ReturnType<typeof setTimeout> | null = null;
    let graceTimer: ReturnType<typeof setTimeout> | null = null;
    let killTimer: ReturnType<typeof setTimeout> | null = null;
    let settled = false;
    // Resolve exactly once; clear timers and the abort listener. 'close', 'error',
    // timeout and abort all route through here so none can double-resolve or leak.
    const onAbort = () => { if (timer) clearTimeout(timer); killThenSettle({ output: '' }); };
    const settle = (outcome: SearchOutcome) => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      if (graceTimer) clearTimeout(graceTimer);
      if (killTimer) clearTimeout(killTimer);
      if (context.abortSignal) context.abortSignal.removeEventListener('abort', onAbort);
      resolve(outcome);
    };
    // Graceful SIGTERM first (so the searcher can flush/clean up), escalating to the
    // uncatchable SIGKILL if it is still alive after sigkillEscalationMs — SIGTERM
    // alone may not kill ripgrep when it is blocked in uninterruptible I/O. On
    // Windows SIGTERM throws, so use the default signal there. Either way we settle
    // after a grace period even if 'close' never fires (a wedged process may not emit
    // it — the "search never returns" hang). Mirrors the reference's escalation.
    const killThenSettle = (outcome: SearchOutcome) => {
      try {
        if (grepHooks.platform === 'win32') {
          child.kill();
        } else {
          child.kill('SIGTERM');
          killTimer = setTimeout(() => { try { child.kill('SIGKILL'); } catch { /* already gone */ } }, grepHooks.sigkillEscalationMs);
        }
      } catch { /* already gone */ }
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
      const text = data.toString();
      stdout += text;
      streamProgress(text);
      if (Buffer.byteLength(stdout) > MAX_SEARCH_OUTPUT_BYTES) {
        // Stop buffering and kill the child so it can't keep streaming into memory.
        outputTruncated = true;
        killThenSettle({ output: stdout, outputTruncated: true });
      }
    });

    let stderrTruncated = false;
    child.stderr!.on('data', (data: Buffer) => {
      if (stderrTruncated) return;
      stderr += data.toString();
      if (Buffer.byteLength(stderr) > MAX_SEARCH_OUTPUT_BYTES) {
        // Bound stderr the same way as stdout: a search that spews warnings to
        // stderr (e.g. permission-denied noise over a huge tree) must not grow
        // memory without limit. Keep the first MAX_SEARCH_OUTPUT_BYTES and stop
        // accumulating. Unlike the stdout cap we do NOT kill the child — stderr
        // is diagnostic, not the result, and the search may still complete with
        // useful stdout. Mirrors the reference's dual stdout/stderr buffer cap.
        stderr = stderr.slice(0, MAX_SEARCH_OUTPUT_BYTES);
        stderrTruncated = true;
      }
    });

    child.on('error', (err: any) => {
      if (err.code === 'EAGAIN') {
        settle({ output: null, eagain: true });
        return;
      }
      // Critical errors mean the searcher is broken, not "binary missing" — surface
      // them instead of silently falling through to the next searcher (which would
      // hide a real permission problem). ENOENT (binary not installed) still falls
      // back. Mirrors the reference's CRITICAL_ERROR_CODES distinction.
      if (err.code === 'EACCES' || err.code === 'EPERM') {
        settle({ output: null, searchError: `${command}: ${err.code} ${err.message ?? ''}`.trim() });
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
        // Clean close: flush the last partial line (no trailing newline) that was
        // held back as the remainder. Only here — a torn tail from a timed-out /
        // signalled / aborted process is never flushed.
        if (onProgress && progressRemainder) onProgress(stripTrailingCR(progressRemainder));
        settle({ output: stdout });
        return;
      }

      // Non-zero exit caused by rg failing to spawn worker threads (EAGAIN) — detected
      // from rg's own stderr, since no 'error' event fires in this mode. Retry single-threaded.
      // `command !== 'grep'` identifies the ripgrep invocation whether it is the bare
      // name `rg` or an operator-configured AMI_RIPGREP_PATH.
      if (command !== 'grep' && isEagainStderr(stderr)) {
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

/**
 * On timeout, salvage any already-collected matches instead of discarding them:
 * drop the last (possibly truncated) line, format the rest, and append a timeout
 * note. Returns null when nothing usable was collected (caller errors out instead).
 * Mirrors the reference's return-partial-results-on-timeout behavior.
 */
function partialOnTimeout(
  output: string | null,
  resolved: string,
  pattern: string,
  headLimit: number,
  offset: number,
  outputMode: string,
): ToolResult | null {
  if (!output) return null;
  // Drop the last line (the process was killed mid-write, so it may be incomplete).
  const lastNl = output.lastIndexOf('\n');
  const complete = lastNl >= 0 ? output.slice(0, lastNl) : '';
  if (!complete.trim()) return null;
  const cleaned = complete.replace(new RegExp(escapeRegExp(resolved) + '/', 'g'), '');
  const result = formatResult(cleaned, pattern, headLimit, offset, outputMode);
  if (!result.isError) {
    result.output += `\n\n[Warning: search timed out after ${grepHooks.timeoutMs / 1000}s — partial results shown. Narrow the pattern or path for complete results.]`;
  }
  return result;
}
