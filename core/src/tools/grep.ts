import { spawn } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validatePatternAndPath, assertReadableTarget, rejectEscapingPattern, isUNCPath } from './tool-utils';
import { suggestSimilarPaths } from './path-suggest';
import { coerceBool, coerceNumber } from '../utils/coerce';
import { resolveRipgrepConfig } from './ripgrep-config';
import { toRgIgnoreGlob, toGrepExcludeGlob, matchesReadIgnore } from './read-ignore';

/**
 * Optional numeric tool-arg: tolerate a model-quoted number ("5") via the shared
 * `coerceNumber`, but preserve "not supplied" as `undefined` (an absent context /
 * head-limit knob must stay absent, not collapse to a default). Garbage coerces to
 * `undefined` rather than injecting a bogus value. Mirrors Claude Code's
 * semanticNumber tolerance without masking bad input via a bare Number().
 */
function optionalNumber(v: unknown): number | undefined {
  if (v === undefined || v === null) return undefined;
  const n = coerceNumber(v, Number.NaN);
  return Number.isNaN(n) ? undefined : n;
}

/**
 * Optional boolean tool-arg: tolerate the string literals "true"/"false" (what
 * providers often serialize) while keeping "not supplied" distinguishable as
 * `undefined` — `line_numbers` defaults to true ONLY when absent, so it must not
 * collapse to false. Mirrors Claude Code's semanticBoolean.
 */
function optionalBool(v: unknown): boolean | undefined {
  if (v === undefined || v === null) return undefined;
  return coerceBool(v);
}

const MAX_LINES = 250;
const RG_TIMEOUT = 20_000;
/** On WSL, filesystem I/O over the /mnt bridge is far slower — bump the default. */
const WSL_RG_TIMEOUT = 60_000;
/** Env override for the search timeout, in seconds. Mirrors Claude Code's CLAUDE_CODE_*. */
const GREP_TIMEOUT_ENV = 'AMI_GREP_TIMEOUT_SECONDS';

/** True when running under WSL (slow /mnt bridge), via distro env or kernel release. */
export function isWSL(env: NodeJS.ProcessEnv = process.env): boolean {
  if (env.WSL_DISTRO_NAME || env.WSL_INTEROP) return true;
  try { return /microsoft|wsl/i.test(os.release()); } catch { return false; }
}

/**
 * Resolve the search timeout in ms: an explicit AMI_GREP_TIMEOUT_SECONDS override
 * wins, else 60s on WSL (slow /mnt), else the 20s default. Mirrors Claude Code's
 * platform-aware, env-overridable ripgrep timeout.
 */
export function grepTimeoutMs(env: NodeJS.ProcessEnv = process.env): number {
  const raw = Number((env[GREP_TIMEOUT_ENV] ?? '').trim());
  if (Number.isFinite(raw) && raw > 0) return raw * 1000;
  return isWSL(env) ? WSL_RG_TIMEOUT : RG_TIMEOUT;
}
// Hard ceiling on bytes accumulated from a spawned searcher's stdout. A pattern
// matching an enormous tree could otherwise stream gigabytes into memory before
// the result is ever truncated to MAX_LINES (OOM / DoS). Once exceeded we stop
// buffering, kill the child, and flag the output truncated.
export const MAX_SEARCH_OUTPUT_BYTES = 10 * 1024 * 1024; // 10 MB
// After SIGKILL on timeout/abort, a wedged process may never emit 'close' (seen on
// Windows and for uninterruptible I/O). Resolve anyway after this grace period so
// the tool can never hang forever.
const KILL_GRACE_MS = 2_000;
// On timeout/abort we first ask the searcher to stop with SIGTERM (which it can
// flush/clean up on), then escalate to the uncatchable SIGKILL if it is still
// alive after this delay. SIGTERM alone may not kill ripgrep when it is blocked in
// uninterruptible I/O (deep filesystem traversal). Kept below KILL_GRACE_MS so the
// forced kill is attempted before we give up and resolve. Mirrors Claude Code's
// SIGTERM-before-SIGKILL escalation.
const SIGKILL_ESCALATION_MS = 1_000;

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
        description: 'Alias for after_context (Claude Code canonical name). Lines of context after each match (rg -A). Requires output_mode: "content".',
      },
      '-B': {
        type: 'number',
        description: 'Alias for before_context (Claude Code canonical name). Lines of context before each match (rg -B). Requires output_mode: "content".',
      },
      '-C': {
        type: 'number',
        description: 'Alias for context_lines (Claude Code canonical name). Lines of context before and after each match (rg -C). Requires output_mode: "content".',
      },
      '-n': {
        type: 'boolean',
        description: 'Alias for line_numbers (Claude Code canonical name). Show line numbers (rg -n). Requires output_mode: "content".',
      },
      '-i': {
        type: 'boolean',
        description: 'Alias for case_insensitive (Claude Code canonical name). Case-insensitive matching (rg -i).',
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
    // Numeric/boolean params are coerced from the string literals providers often
    // serialize (`"true"`, `"5"`) — a model emitting `"head_limit":"5"` must behave
    // like `5`, and `"case_insensitive":"false"` must be false (not truthy). Mirrors
    // Claude Code's semanticNumber/semanticBoolean coercion.
    // Accept Claude Code's canonical single-letter aliases (-A/-B/-C/-n/-i) as
    // synonyms for AMI's descriptive names, so a tool call authored against the
    // Claude schema runs unchanged. The descriptive name wins when both are given.
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
    // Resolve which ripgrep binary to spawn (honors USE_BUILTIN_RIPGREP /
    // AMI_RIPGREP_PATH, shared with the glob tool). Falls back to the bare name `rg`.
    const rgCommand = resolveRipgrepConfig().command;
    // File-READ deny globs the permission layer wants excluded from search output, so a
    // file the user forbade reading never surfaces (its path OR its matching lines) in any
    // output mode. Threaded into every search path (rg, grep, in-process). Parity: Claude's
    // getFileReadIgnorePatterns → `--glob !pattern` injection.
    const readIgnore = context.readIgnorePatterns ?? [];
    const rgArgs = buildRgArgs(cleanedPattern, resolved, include, outputMode, caseInsensitive, contextLines, multiline, fileType, beforeContext, afterContext, fetchLimit, lineNumbers, fixedStrings, readIgnore);
    let rgOutcome = await runSearch(rgArgs, rgCommand, context, grepHooks.timeoutMs);

    if (rgOutcome.eagain) {
      rgArgs.push('-j', '1');
      rgOutcome = await runSearch(rgArgs, rgCommand, context, grepHooks.timeoutMs);
    }

    if (rgOutcome.timedOut) {
      const partial = partialOnTimeout(rgOutcome.output, resolved, pattern, headLimit, offset, outputMode);
      if (partial) return partial;
      return { output: `Error: Search timed out after ${grepHooks.timeoutMs / 1000}s. Try a more specific pattern or path.`, isError: true };
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
const FALLBACK_SKIP_DIRS = new Set(['node_modules', '.git', 'vendor', 'dist', 'build', '.svn', '.hg', '.bzr', '.jj', '.sl']);
const FALLBACK_MAX_FILES = 4000;
const FALLBACK_MAX_FILE_BYTES = 1_000_000;

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
  readIgnore?: readonly string[],
): string[] {
  // `--no-config` neutralizes ripgrep config injection: a planted RIPGREP_CONFIG_PATH
  // (or ~/.ripgreprc) could otherwise inject flags (e.g. extra include globs) into
  // every search. The child env also drops RIPGREP_CONFIG_PATH (see runSearch).
  const args = ['--no-config', '--color', 'never', '--max-columns', '500'];
  // Literal search: match the query as a fixed string, not a regex.
  if (fixedStrings) args.push('-F');

  args.push('--hidden');
  // Exclude VCS metadata stores from searches. `.sl` is Sapling — without it a grep
  // over a Sapling checkout would walk the internal object store (noise + possible
  // leakage of packed content). Mirrors Claude Code's full VCS exclusion set.
  args.push('--glob', '!.git', '--glob', '!.svn', '--glob', '!.hg', '--glob', '!.bzr', '--glob', '!.jj', '--glob', '!.sl');
  // Read-deny exclusions: hide files the permission layer forbids reading from EVERY
  // output mode (including content, which would otherwise print their lines). Negated
  // globs anchored per ripgrep's cwd-relative ignore rule. Parity: Claude GrepTool.
  if (readIgnore) {
    for (const p of readIgnore) args.push('--glob', toRgIgnoreGlob(p));
  }

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
  // context_lines (-C) takes precedence: when a symmetric context is set, the
  // independent before/after knobs are ignored so the two can't fight over the
  // same window. Mirrors Claude Code's GrepTool precedence.
  if (!(contextLines && contextLines > 0)) {
    if (beforeContext != null) args.push('-B', String(beforeContext));
    if (afterContext != null) args.push('-A', String(afterContext));
  }

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
  readIgnore?: readonly string[],
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

  // Read-deny exclusions (best-effort basename globs — GNU grep's --exclude matches the
  // final path component only). Keeps a forbidden file (e.g. `.env`) out of grep output.
  if (readIgnore) {
    for (const p of readIgnore) {
      const g = toGrepExcludeGlob(p);
      if (g) args.push(`--exclude=${g}`);
    }
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
      // Strip RIPGREP_CONFIG_PATH from the child env so a planted config file can't
      // inject flags into the search (paired with the `--no-config` rg arg).
      const childEnv = { ...process.env };
      delete childEnv.RIPGREP_CONFIG_PATH;
      child = grepHooks.spawn(command, args, {
        cwd: context.cwd,
        stdio: ['ignore', 'pipe', 'pipe'],
        env: childEnv,
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
    // it — the "search never returns" hang). Mirrors Claude Code's escalation.
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
      // Critical errors mean the searcher is broken, not "binary missing" — surface
      // them instead of silently falling through to the next searcher (which would
      // hide a real permission problem). ENOENT (binary not installed) still falls
      // back. Mirrors Claude Code's CRITICAL_ERROR_CODES distinction.
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

function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * On timeout, salvage any already-collected matches instead of discarding them:
 * drop the last (possibly truncated) line, format the rest, and append a timeout
 * note. Returns null when nothing usable was collected (caller errors out instead).
 * Mirrors Claude Code's return-partial-results-on-timeout behavior.
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
  const cleaned = complete.replace(new RegExp(escapeRegex(resolved) + '/', 'g'), '');
  const result = formatResult(cleaned, pattern, headLimit, offset, outputMode);
  if (!result.isError) {
    result.output += `\n\n[Warning: search timed out after ${grepHooks.timeoutMs / 1000}s — partial results shown. Narrow the pattern or path for complete results.]`;
  }
  return result;
}

/**
 * Extract the file-path portion from a ripgrep/grep output line. Content lines look
 * like `path:LINENO:text` and count lines like `path:COUNT`; both are split on the
 * first `:<digits>` boundary. A naive `line.split(':')[0]` mis-parses Windows
 * drive-letter paths (`C:\dir\file.ts:5:text` → `C`), inflating the per-file count.
 * files_with_matches mode emits bare paths with no `:N`, so the whole line is the
 * path. Mirrors Claude Code's drive-safe `^(.*?):(\d+):` parse (GlobalSearchDialog).
 */
export function grepLineFilePath(line: string): string {
  const m = line.match(/^(.*?):\d+(?::|$)/);
  return m ? m[1] : line;
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
  const fileSet = new Set(allLines.map(grepLineFilePath));
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
