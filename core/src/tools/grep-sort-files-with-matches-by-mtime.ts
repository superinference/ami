// Split from grep.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import { ToolResult } from '../types';
import { isWSL } from '../utils/env';
import { ORPHANED_PLUGIN_HOLD_DIRNAME } from './tool-utils';
import { coerceBool, coerceNumber } from '../utils/coerce';
import { toRgIgnoreGlob, toGrepExcludeGlob } from './read-ignore';

/**
 * Optional numeric tool-arg: tolerate a model-quoted number ("5") via the shared
 * `coerceNumber`, but preserve "not supplied" as `undefined` (an absent context /
 * head-limit knob must stay absent, not collapse to a default). Garbage coerces to
 * `undefined` rather than injecting a bogus value. Mirrors the reference's
 * semanticNumber tolerance without masking bad input via a bare Number().
 */
export function optionalNumber(v: unknown): number | undefined {
  if (v === undefined || v === null) return undefined;
  const n = coerceNumber(v, Number.NaN);
  return Number.isNaN(n) ? undefined : n;
}

/**
 * Optional boolean tool-arg: tolerate the string literals "true"/"false" (what
 * providers often serialize) while keeping "not supplied" distinguishable as
 * `undefined` — `line_numbers` defaults to true ONLY when absent, so it must not
 * collapse to false. Mirrors the reference's semanticBoolean.
 */
export function optionalBool(v: unknown): boolean | undefined {
  if (v === undefined || v === null) return undefined;
  return coerceBool(v);
}

export const MAX_LINES = 250;

export const RG_TIMEOUT = 20_000;

/** On WSL, filesystem I/O over the /mnt bridge is far slower — bump the default. */
export const WSL_RG_TIMEOUT = 60_000;

/** Env override for the search timeout, in seconds. Mirrors the reference's CLAUDE_CODE_*. */
export const GREP_TIMEOUT_ENV = 'AMI_GREP_TIMEOUT_SECONDS';

/**
 * Resolve the search timeout in ms: an explicit AMI_GREP_TIMEOUT_SECONDS override
 * wins, else 60s on WSL (slow /mnt), else the 20s default. Mirrors the reference's
 * platform-aware, env-overridable ripgrep timeout.
 */
export function grepTimeoutMs(env: NodeJS.ProcessEnv = process.env): number {
  const raw = Number((env[GREP_TIMEOUT_ENV] ?? '').trim());
  if (Number.isFinite(raw) && raw > 0) return raw * 1000;
  return isWSL(env) ? WSL_RG_TIMEOUT : RG_TIMEOUT;
}

// After SIGKILL on timeout/abort, a wedged process may never emit 'close' (seen on
// Windows and for uninterruptible I/O). Resolve anyway after this grace period so
// the tool can never hang forever.
export const KILL_GRACE_MS = 2_000;

// On timeout/abort we first ask the searcher to stop with SIGTERM (which it can
// flush/clean up on), then escalate to the uncatchable SIGKILL if it is still
// alive after this delay. SIGTERM alone may not kill ripgrep when it is blocked in
// uninterruptible I/O (deep filesystem traversal). Kept below KILL_GRACE_MS so the
// forced kill is attempted before we give up and resolve. Mirrors the reference's
// SIGTERM-before-SIGKILL escalation.
export const SIGKILL_ESCALATION_MS = 1_000;

// eslint-disable-next-line security/detect-unsafe-regex -- [lcrniw] and \s are disjoint sets, no overlap
export const GREP_FLAG_RE = /^(-[lcrniw]+\s+)+/;

export function stripGrepFlags(pattern: string): string {
  const stripped = pattern.replace(GREP_FLAG_RE, '');
  return stripped || pattern;
}

export interface SearchOutcome {
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

export const FALLBACK_MAX_FILES = 4000;

export const FALLBACK_MAX_FILE_BYTES = 1_000_000;

export function buildRgArgs(
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
  // leakage of packed content). Mirrors the reference's full VCS exclusion set.
  args.push('--glob', '!.git', '--glob', '!.svn', '--glob', '!.hg', '--glob', '!.bzr', '--glob', '!.jj', '--glob', '!.sl');
  // Superseded plugin versions relocated to the hidden `.orphaned/` hold dir
  // (common's plugin-orphan-cache) hold stale SKILL.md/source until reclaimed;
  // never return matches from them (parity: the reference's getGlobExclusionsForPluginCache).
  args.push('--glob', `!${ORPHANED_PLUGIN_HOLD_DIRNAME}`);
  // Read-deny exclusions: hide files the permission layer forbids reading from EVERY
  // output mode (including content, which would otherwise print their lines). Negated
  // globs anchored per ripgrep's cwd-relative ignore rule. Parity: the reference GrepTool.
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

  // NOTE (functional change): do NOT pass `--sortr modified`. ripgrep disables its
  // multi-threaded engine whenever a sort is requested, so sorting in rg slowed
  // every search AND reordered content/count output. Instead, content/count keep
  // ripgrep's natural (parallel) order and files_with_matches is sorted newest-first
  // in JS via sortFilesWithMatchesByMtime (stat + mtime). Mirrors the reference's
  // GrepTool, which runs rg parallel and does the mtime sort in JS.

  if (include) {
    for (const g of splitGlobFilter(include)) args.push('--glob', g);
  }

  if (fileType) args.push('--type', fileType);
  // context_lines (-C) takes precedence: when a symmetric context is set, the
  // independent before/after knobs are ignored so the two can't fight over the
  // same window. Mirrors the reference's GrepTool precedence.
  if (!(contextLines && contextLines > 0)) {
    if (beforeContext != null) args.push('-B', String(beforeContext));
    if (afterContext != null) args.push('-A', String(afterContext));
  }

  if (pattern.startsWith('-')) args.push('-e', pattern);
  else args.push(pattern);
  args.push(searchPath);
  return args;
}

export function buildGrepArgs(
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

/**
 * Strip a single trailing CR so CRLF-terminated searcher output (Windows, or a
 * grepped CRLF file) yields clean lines. Shared by the streaming splitter and the
 * final-remainder flush so both treat line endings identically.
 */
export function stripTrailingCR(line: string): string {
  return line.endsWith('\r') ? line.slice(0, -1) : line;
}

/**
 * Split a raw stdout chunk into COMPLETE lines while carrying the partial trailing
 * line forward as a remainder across chunk boundaries. Mirrors the reference's
 * `ripGrepStream` chunk handling (ripgrep.ts): prepend the prior remainder, split
 * on `\n`, pop the last (possibly partial) segment back into `remainder`, and strip
 * a trailing CR from each complete line. A searcher writes result lines as they are
 * found, but a single write may land mid-line — this lets a caller flush finished
 * lines early (incremental paint) without ever emitting a torn partial line.
 *
 * The returned `remainder` is NOT CR-stripped (it is an incomplete line); callers
 * flush it with {@link stripTrailingCR} only on a clean close, never on abort.
 */
export function splitStreamLines(
  remainder: string,
  chunk: string,
): { lines: string[]; remainder: string } {
  const data = remainder + chunk;
  const parts = data.split('\n');
  const nextRemainder = parts.pop() ?? '';
  return { lines: parts.map(stripTrailingCR), remainder: nextRemainder };
}

/**
 * Order a `files_with_matches` result (one file path per line) newest-first by
 * modification time, with a filename tiebreaker, doing the stat sort in JS so
 * ripgrep itself runs WITHOUT `--sort`/`--sortr` (ripgrep disables its
 * multi-threaded engine whenever a sort is requested — see buildRgArgs). This
 * keeps content/count output in ripgrep's natural parallel order while still
 * giving the agent recency-ordered file lists on EVERY backend (rg, grep, and —
 * via its own candidate sort — the in-process walk). A failed `stat` (file
 * deleted/denied between the search and the stat) sorts as mtime 0 (last). Under
 * `NODE_ENV==='test'` the order is filename-only so fixtures written in the same
 * instant are deterministic. Mirrors the reference's GrepTool mtime sort.
 */
export function sortFilesWithMatchesByMtime(
  output: string,
  testMode: boolean = process.env.NODE_ENV === 'test',
): string {
  const paths = output.split('\n').filter(line => line.length > 0);
  if (paths.length <= 1) return output;
  const withMtime = paths.map((p) => {
    let mtimeMs = 0;
    try {
      mtimeMs = fs.statSync(p).mtimeMs;
    } catch {
      // Deleted between the search and this stat, or unreadable — sorts last.
    }
    return { p, mtimeMs };
  });
  withMtime.sort((a, b) => {
    if (testMode) return a.p.localeCompare(b.p);
    const byTime = b.mtimeMs - a.mtimeMs;
    return byTime !== 0 ? byTime : a.p.localeCompare(b.p);
  });
  return withMtime.map((x) => x.p).join('\n');
}

/**
 * Extract the file-path portion from a ripgrep/grep output line. Content lines look
 * like `path:LINENO:text` and count lines like `path:COUNT`; both are split on the
 * first `:<digits>` boundary. A naive `line.split(':')[0]` mis-parses Windows
 * drive-letter paths (`C:\dir\file.ts:5:text` → `C`), inflating the per-file count.
 * files_with_matches mode emits bare paths with no `:N`, so the whole line is the
 * path. Mirrors the reference's drive-safe `^(.*?):(\d+):` parse (GlobalSearchDialog).
 */
export function grepLineFilePath(line: string): string {
  const m = line.match(/^(.*?):\d+(?::|$)/);
  return m ? m[1] : line;
}

export function isSuiteLogLine(line: string): boolean {
  return /"Action"\s*:/.test(line) && /"(?:Test|Package|Elapsed)"/.test(line);
}

export function formatResult(output: string, pattern: string, limit: number = MAX_LINES, offset: number = 0, outputMode: string = 'content'): ToolResult {
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
