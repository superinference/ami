// fast-glob pulls braces (GHSA-vfj7-8cjw-p6xm), which has no patched release.
// expandDirectories stays off so a directory pattern is not rewritten.
import { glob } from 'tinyglobby';
import { execFile } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { validatePatternAndPath, assertReadableTarget, rejectEscapingPattern, isUNCPath, ORPHANED_PLUGIN_HOLD_DIRNAME } from './tool-utils';
import { suggestSimilarPaths } from './path-suggest';
import { isEagainStderr } from './grep';
import { resolveRipgrepConfig, type RipgrepOverride } from './ripgrep-config';
import { toAnyDepthGlob } from './read-ignore';
import { collectGitignoreGlobs } from '../workspace-indexer';

const MAX_RESULTS = 100;
// Cap the total joined output so a huge match set can't blow the context window.
const MAX_OUTPUT_CHARS = 50_000;
// Default glob timeout. The in-process tinyglobby walk is otherwise unbounded — a
// pathological pattern over a huge tree could hang the turn forever. Env-configurable
// (AMI_GLOB_TIMEOUT_SECONDS) to mirror Claude Code's CLAUDE_CODE_GLOB_TIMEOUT_SECONDS.
export const GLOB_TIMEOUT_ENV = 'AMI_GLOB_TIMEOUT_SECONDS';
const DEFAULT_GLOB_TIMEOUT_MS = 20_000;

/** Resolve the glob timeout in ms from the env flag (seconds), falling back to the default. */
export function globTimeoutMs(env: NodeJS.ProcessEnv = process.env): number {
  const raw = Number((env[GLOB_TIMEOUT_ENV] ?? '').trim());
  return Number.isFinite(raw) && raw > 0 ? raw * 1000 : DEFAULT_GLOB_TIMEOUT_MS;
}

/** Env knob for the glob result-count cap (per page), mirroring Claude Code's
 *  globLimits.maxResults. Falls back to the built-in default. */
export const GLOB_MAX_RESULTS_ENV = 'AMI_GLOB_MAX_RESULTS';
export function globMaxResults(env: NodeJS.ProcessEnv = process.env): number {
  const raw = Number((env[GLOB_MAX_RESULTS_ENV] ?? '').trim());
  return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : MAX_RESULTS;
}

/** Sentinel the glob walk races against its timeout with. */
const GLOB_TIMEOUT = Symbol('glob-timeout');

/**
 * G5: opt-in env flag to route glob through `rg --files` (ripgrep's parallel walker,
 * much faster on very large trees) instead of the in-process tinyglobby walk. OFF by
 * default — `rg`'s `-g` uses gitignore-glob semantics (a pattern without `/` matches at
 * ANY depth), which differs from tinyglobby's glob semantics (`*.ts` = top level only),
 * so enabling it is a deliberate choice, not a silent default. When set but `rg` is
 * missing or errors, we fall back to tinyglobby so results are always produced.
 */
export const GLOB_RIPGREP_ENV = 'AMI_GLOB_RG';

/** True when the G5 ripgrep route is opted in via the env flag. */
export function isRipgrepGlobEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const v = (env[GLOB_RIPGREP_ENV] ?? '').trim().toLowerCase();
  return v !== '' && v !== '0' && v !== 'false' && v !== 'no' && v !== 'off';
}

/**
 * Parse an env flag whose default is TRUE: unset or empty → true; only an
 * explicit falsey token ('0'/'false'/'no'/'off') flips it to false. Mirrors
 * Claude Code's `isEnvTruthy(env || 'true')` convention for its glob toggles
 * (so an empty string counts as unset, not as "false").
 */
function envFlagDefaultTrue(raw: string | undefined): boolean {
  const v = (raw ?? '').trim().toLowerCase();
  if (v === '') return true;
  return v !== '0' && v !== 'false' && v !== 'no' && v !== 'off';
}

/**
 * Env toggle for including hidden (dot) files in glob results. Defaults ON
 * (matching the prior always-`dot:true` behavior and Claude Code's default).
 * Set AMI_GLOB_HIDDEN=false to exclude dotfiles. Mirrors CLAUDE_CODE_GLOB_HIDDEN.
 */
export const GLOB_HIDDEN_ENV = 'AMI_GLOB_HIDDEN';
export function globIncludesHidden(env: NodeJS.ProcessEnv = process.env): boolean {
  return envFlagDefaultTrue(env[GLOB_HIDDEN_ENV]);
}

/**
 * Env toggle controlling whether glob RESPECTS the project's .gitignore. The
 * flag is AMI_GLOB_NO_IGNORE and defaults ON (true = do NOT respect .gitignore),
 * preserving the prior behavior and matching Claude Code's CLAUDE_CODE_GLOB_NO_IGNORE
 * default. Set AMI_GLOB_NO_IGNORE=false to honor .gitignore (nested + .git/info/exclude).
 * Returns true when results should be filtered through .gitignore.
 */
export const GLOB_NO_IGNORE_ENV = 'AMI_GLOB_NO_IGNORE';
export function globRespectsGitignore(env: NodeJS.ProcessEnv = process.env): boolean {
  return !envFlagDefaultTrue(env[GLOB_NO_IGNORE_ENV]);
}

/** Top-level keys the glob tool accepts. Any other key is rejected (strict schema,
 *  parity with Claude Code's `z.strictObject`), so a hallucinated/typo'd parameter
 *  fails loud instead of being silently ignored. */
const GLOB_ALLOWED_KEYS = new Set(['pattern', 'path', 'offset', 'limit']);

/**
 * Split an absolute glob pattern into its static base directory (everything before
 * the first glob metacharacter `* ? [ {`) and the remaining relative pattern, so the
 * walk can be rooted at a real directory and the glob applied relative to it. Mirrors
 * Claude Code's `extractGlobBaseDirectory` (utils/glob.ts), including the filesystem-
 * root (`/*.txt` → base `/`) and Windows-drive-root (`C:/*.txt` → base `C:\`) special
 * cases. Returns an empty `baseDir` when the first glob char precedes any path
 * separator (the pattern is relative to cwd and needs no re-rooting).
 */
export function extractGlobBaseDirectory(pattern: string): { baseDir: string; relativePattern: string } {
  const match = pattern.match(/[*?[{]/);
  if (!match || match.index === undefined) {
    // No glob metachar — a literal path: split dir / filename.
    return { baseDir: path.dirname(pattern), relativePattern: path.basename(pattern) };
  }
  const staticPrefix = pattern.slice(0, match.index);
  const lastSep = Math.max(staticPrefix.lastIndexOf('/'), staticPrefix.lastIndexOf(path.sep));
  if (lastSep === -1) {
    // Glob char before any separator — pattern is relative to cwd.
    return { baseDir: '', relativePattern: pattern };
  }
  let baseDir = staticPrefix.slice(0, lastSep);
  const relativePattern = pattern.slice(lastSep + 1);
  // Root pattern (e.g. "/*.txt"): lastSep===0 leaves baseDir empty — use the root.
  if (baseDir === '' && lastSep === 0) baseDir = '/';
  // Windows drive root ("C:/*.txt"): "C:" is the drive-relative dir, not the root.
  if (process.platform === 'win32' && /^[A-Za-z]:$/.test(baseDir)) baseDir += path.sep;
  return { baseDir, relativePattern };
}

/**
 * List files matching `pattern` under `cwd` via `rg --files`, honoring `ignore` as
 * `-g '!…'` excludes. Resolves to the relative paths, `[]` when rg ran but matched
 * nothing, or `null` when rg is unavailable / errored (signal the caller to fall back).
 * Injectable via {@link globHooks.rgFiles} for tests (no real ripgrep needed).
 */
/** One `rg --files` run; resolves {result, eagain}. result is null on any non-empty error. */
function runRipgrepFiles(args: string[], cwd: string, signal?: AbortSignal, override?: RipgrepOverride): Promise<{ result: string[] | null; eagain: boolean }> {
  return new Promise((resolve) => {
    const timeoutMs = globHooks.timeoutMs;
    let killTimer: ReturnType<typeof setTimeout> | undefined;
    let done = false;
    const finish = (result: string[] | null, eagain: boolean) => {
      if (done) return;
      done = true;
      if (killTimer) clearTimeout(killTimer);
      resolve({ result, eagain });
    };
    // Env-configurable timeout (AMI_GLOB_TIMEOUT_SECONDS via globHooks.timeoutMs): on
    // expiry execFile sends SIGTERM (killSignal). Forward the turn's AbortSignal too so
    // a cancelled turn (Esc / engine abort) kills the running rg child.
    // A config-level sandbox.ripgrep override wins over the env waterfall, same as grep.
    const rgConfig = resolveRipgrepConfig(process.env, override);
    const child = globHooks.execFileImpl(
      rgConfig.command,
      rgConfig.args?.length ? [...rgConfig.args, ...args] : args,
      { cwd, maxBuffer: 64 * 1024 * 1024, timeout: timeoutMs, killSignal: 'SIGTERM', signal },
      (err, stdout, stderr) => {
        if (err) {
          // rg exits 1 when it matched no files (valid empty result); any other error
          // (ENOENT = not installed, non-zero crash, SIGTERM/SIGKILL on timeout) means
          // "can't use rg" → fall back. execFile sets `code` to the numeric exit code,
          // or a string like 'ENOENT' when the binary is missing. A thread-spawn EAGAIN
          // (os error 11, common in constrained Docker/CI) is retried single-threaded.
          const code = (err as { code?: number | string }).code;
          finish(code === 1 ? [] : null, isEagainStderr(String(stderr ?? '')));
          return;
        }
        finish(stdout.split('\n').map(l => l.trim()).filter(Boolean), false);
      },
    );
    // Escalate SIGTERM → SIGKILL: a wedged rg can ignore SIGTERM, so force-kill after a
    // short grace period past the timeout to guarantee the child dies (mirrors grep.ts).
    if (!done && timeoutMs > 0 && child && typeof child.kill === 'function') {
      killTimer = setTimeout(() => {
        try { child.kill('SIGKILL'); } catch { /* already gone */ }
      }, timeoutMs + globHooks.sigkillGraceMs);
      if (typeof killTimer.unref === 'function') killTimer.unref();
    }
  });
}

async function ripgrepFiles(pattern: string, cwd: string, ignore: string[], signal?: AbortSignal, override?: RipgrepOverride): Promise<string[] | null> {
  // --no-ignore: don't respect .gitignore (default; AMI_GLOB_NO_IGNORE=false lets
  // ripgrep honor it natively). --hidden: include dotfiles (AMI_GLOB_HIDDEN=false
  // excludes them). Mirrors Claude Code's rg flag assembly.
  const args = ['--files'];
  if (!globRespectsGitignore()) args.push('--no-ignore');
  if (globIncludesHidden()) args.push('--hidden');
  // Order the walk newest-first in ripgrep itself (`--sortr=modified`) so the match
  // set arrives pre-sorted and we skip the per-file `statSync` pass on a potentially
  // huge result list — a bounded, non-blocking mtime sort (parity: Claude uses rg's
  // native `--sort=modified`). Keeps AMI's newest-first ordering (`--sortr`, reverse).
  args.push('--sortr=modified');
  args.push('-g', pattern);
  for (const ig of ignore) { args.push('-g', `!${ig}`); }
  let r = await runRipgrepFiles(args, cwd, signal, override);
  // Thread-spawn exhaustion (EAGAIN): retry once single-threaded before giving up.
  if (r.result === null && r.eagain) {
    r = await runRipgrepFiles([...args, '-j', '1'], cwd, signal, override);
  }
  return r.result;
}

/**
 * Indirection seam for the underlying glob implementation. Production uses
 * tinyglobby's `glob`; tests swap it to exercise the error branch (tinyglobby
 * itself rarely throws). `rgFiles` is the G5 ripgrep walker; `execFileImpl` is the
 * child-process seam it uses (injectable so the rg branches are unit-testable
 * without a real ripgrep binary).
 */
export const globHooks: {
  glob: typeof glob;
  rgFiles: (pattern: string, cwd: string, ignore: string[], signal?: AbortSignal, override?: RipgrepOverride) => Promise<string[] | null>;
  execFileImpl: typeof execFile;
  timeoutMs: number;
  /** Grace period after the SIGTERM timeout before escalating to SIGKILL on the rg child. */
  sigkillGraceMs: number;
} = { glob, rgFiles: ripgrepFiles, execFileImpl: execFile, timeoutMs: globTimeoutMs(), sigkillGraceMs: 2_000 };

export const globTool: ToolDefinition = {
  name: 'glob',
  description:
    'Find files matching a glob pattern. Works with any codebase size. ' +
    'Supports glob patterns like "**/*.ts" or "src/**/*.json". ' +
    'Returns matching file paths relative to the working directory, ' +
    'sorted by modification time (most recently modified first). ' +
    'When you are doing an open-ended search that may require multiple rounds of globbing and grepping, ' +
    'use the `task` tool to delegate it to a sub-agent instead.',
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
          'The directory to search in. If not specified, the current working directory is used. ' +
          'IMPORTANT: Omit this field to use the default directory. Do NOT enter "undefined" or "null" - ' +
          'simply omit it for the default behavior. Must be a valid directory path if provided.',
      },
      offset: {
        type: 'number',
        description: 'Skip this many matches (0-based) before listing. Use with limit to page through a large match set.',
      },
      limit: {
        type: 'number',
        description: 'Maximum number of matches to return in this page (capped at the result-count limit).',
      },
    },
    required: ['pattern'],
    // Strict input object: reject unknown top-level keys (parity with Claude's
    // z.strictObject) so a hallucinated/typo'd parameter fails loud. Enforced at
    // runtime in execute() against GLOB_ALLOWED_KEYS.
    additionalProperties: false,
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    if (context.abortSignal?.aborted) {
      return { output: 'Error: glob search was cancelled.', isError: true };
    }
    // Strict input object: reject unknown/typo'd top-level keys instead of silently
    // ignoring them (parity with Claude's z.strictObject — a hallucinated `recursive`
    // or misspelled `paht` fails loud). Declared on inputSchema.additionalProperties.
    const unknownKeys = Object.keys(input).filter(k => !GLOB_ALLOWED_KEYS.has(k));
    if (unknownKeys.length > 0) {
      return { output: `Error: Unknown parameter(s): ${unknownKeys.join(', ')}. Allowed: pattern, path, offset, limit.`, isError: true };
    }
    // Absolute glob pattern: Claude lets `pattern` itself be a fully absolute path.
    // Extract its static base directory and apply the remainder relative to it, so the
    // walk is rooted at a real dir (tinyglobby/rg -g both want a relative pattern). The
    // derived base is confined to the workspace / allowed roots by validatePatternAndPath
    // below, so an absolute pattern can never escape the sandbox.
    let patternArg = input.pattern as unknown;
    let searchPathArg = typeof input.path === 'string' ? input.path : undefined;
    if (typeof patternArg === 'string' && path.isAbsolute(patternArg)) {
      const { baseDir, relativePattern } = extractGlobBaseDirectory(patternArg);
      if (baseDir) {
        searchPathArg = baseDir;
        patternArg = relativePattern;
      }
    }
    // UNC-path guard: short-circuit BEFORE any filesystem access to avoid an outbound
    // SMB/NTLM credential leak on Windows. Covers an explicit `path` AND an absolute
    // pattern whose extracted base directory resolves to a UNC share.
    if (typeof searchPathArg === 'string' && isUNCPath(searchPathArg)) {
      return { output: `Error: UNC paths are not allowed (potential NTLM credential leak): ${searchPathArg}`, isError: true };
    }
    const v = validatePatternAndPath(patternArg, searchPathArg, context.cwd, context.allowedReadRoots, 'glob');
    if (v.error) return v.error;
    const { pattern, resolved } = v;
    // The glob pattern itself must not traverse above the (already-confined) search root:
    // a pattern like "../../**" would walk out of the workspace even though `resolved` is
    // inside it. (grep's `pattern` is a REGEX, not a path, so this guard is glob-only.)
    const patternEscape = rejectEscapingPattern(pattern);
    if (patternEscape) return patternEscape;
    // Self-correcting error when the given base directory does not exist: name the
    // cwd and the closest sibling so the model can recover from a typo.
    if (typeof searchPathArg === 'string' && !fs.existsSync(resolved)) {
      const suggestions = suggestSimilarPaths(resolved, context.cwd);
      const hint = suggestions.length > 0 ? ` Did you mean ${suggestions[0]}?` : '';
      return { output: `Error: Directory does not exist: ${resolved}. Your current working directory is ${context.cwd}.${hint}`, isError: true };
    }
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
          output: `Error: "${searchPathArg}" is not a directory`,
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
    // Superseded plugin versions are relocated into a hidden `.orphaned/` hold dir
    // (common's plugin-orphan-cache) and kept for a grace window; never enumerate
    // their stale files. Dropped if the pattern explicitly targets the hold dir, so
    // a deliberate audit of orphaned versions still works (same escape hatch as
    // node_modules/.git above). Parity: Claude's getGlobExclusionsForPluginCache.
    if (!pattern.includes(ORPHANED_PLUGIN_HOLD_DIRNAME)) ignore.push(`**/${ORPHANED_PLUGIN_HOLD_DIRNAME}/**`);

    // .gitignore respect (opt-in via AMI_GLOB_NO_IGNORE=false). Translate the
    // project's ignore sources into tinyglobby excludes so deliberately-excluded
    // files (secrets, build output) are never enumerated. The ripgrep route also
    // honors them natively (it drops --no-ignore); these explicit excludes cover
    // the in-process tinyglobby walk. Best-effort — a missing .gitignore adds none.
    if (globRespectsGitignore()) {
      ignore.push(...await collectGitignoreGlobs(resolved, ignore));
    }

    // File-READ deny globs from the permission layer: never ENUMERATE a file the user
    // forbade reading (its path alone leaks via a directory listing). Applied to BOTH
    // the ripgrep route (negated to `-g !glob`) and the in-process tinyglobby `ignore`.
    // Parity: Claude feeds getFileReadIgnorePatterns into glob/ls exclusions too.
    for (const p of context.readIgnorePatterns ?? []) ignore.push(toAnyDepthGlob(p));

    try {
      // G5: opt-in ripgrep route (fast parallel walk); falls back to tinyglobby when
      // rg is unavailable/errors so results are always produced.
      let entries: string[] | null = null;
      // When the rg route produces results it has already ordered them newest-first via
      // `--sortr=modified`, so the per-file stat sort below is skipped for that path.
      let sortedByRipgrep = false;
      if (isRipgrepGlobEnabled()) {
        entries = await globHooks.rgFiles(pattern, resolved, ignore, context.abortSignal, context.sandbox?.ripgrep);
        if (entries !== null) sortedByRipgrep = true;
      }
      if (entries === null) {
        // Bound the in-process tinyglobby walk: race it against a timeout and the
        // turn's abort so a pathological pattern on a huge tree can never hang forever.
        let timer: ReturnType<typeof setTimeout> | undefined;
        const timeoutMs = globHooks.timeoutMs;
        const timeout = new Promise<typeof GLOB_TIMEOUT>((res) => {
          timer = setTimeout(() => res(GLOB_TIMEOUT), timeoutMs);
        });
        const walked = globHooks.glob(pattern, {
          cwd: resolved,
          ignore,
          // Hidden (dot) files are included by default; AMI_GLOB_HIDDEN=false excludes them.
          dot: globIncludesHidden(),
          onlyFiles: true,
          absolute: false,
          expandDirectories: false,
          // Forward the turn's AbortSignal so a cancelled turn (Esc / engine
          // abort) stops the in-process crawl immediately instead of walking
          // the whole tree before the timeout fires (mirrors the rg route,
          // which already forwards the signal to its child process).
          signal: context.abortSignal,
        });
        const raced = await Promise.race([walked, timeout]);
        if (timer) clearTimeout(timer);
        if (raced === GLOB_TIMEOUT) {
          return { output: `Error: glob timed out after ${timeoutMs / 1000}s. Use a more specific pattern or a narrower path.`, isError: true };
        }
        entries = raced;
      }

      // Cancellation: tinyglobby resolves to [] (it does NOT throw) when its
      // signal aborts mid-walk, and an aborted rg route likewise falls through
      // to an empty tinyglobby result. Either would masquerade as a definitive
      // "No files found" below. Surface the cancellation explicitly so the model
      // doesn't treat an interrupted search as a real empty result.
      if (context.abortSignal?.aborted) {
        return { output: 'Error: glob search was cancelled.', isError: true };
      }

      if (entries.length === 0) {
        return { output: `No files found matching pattern: ${pattern}` };
      }

      // Sort by mtime descending (newest first), matching Claude Code. The ripgrep
      // route already ordered the walk via `--sortr=modified`, so only the in-process
      // tinyglobby route needs the per-file stat pass (parallel, non-blocking, bounded
      // by the match set). Stat each entry relative to the search root; a stat failure
      // is treated as mtime 0. Ties fall back to alphabetical for a deterministic order.
      if (!sortedByRipgrep) {
        const mtimes = new Map<string, number>();
        await Promise.all(entries.map(async (entry) => {
          try {
            mtimes.set(entry, (await fs.promises.stat(path.resolve(resolved, entry))).mtimeMs);
          } catch {
            mtimes.set(entry, 0);
          }
        }));
        entries.sort((a, b) => {
          const diff = (mtimes.get(b) ?? 0) - (mtimes.get(a) ?? 0);
          return diff !== 0 ? diff : a.localeCompare(b);
        });
      }

      const total = entries.length;
      // Pagination: skip `offset` matches, then take up to `limit` (capped by the
      // configurable result-count knob). Defaults (offset 0, limit = maxResults)
      // preserve the original single-page behavior.
      const maxResults = globMaxResults();
      const offsetRaw = Number(input.offset);
      const offset = Number.isFinite(offsetRaw) && offsetRaw > 0 ? Math.floor(offsetRaw) : 0;
      const limitRaw = Number(input.limit);
      const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(Math.floor(limitRaw), maxResults) : maxResults;

      if (offset >= total && total > 0) {
        return { output: `No files in range (offset ${offset}, ${total} total matches).` };
      }

      // First apply the page window, then shrink further if the joined list would
      // exceed the character cap.
      const shown = entries.slice(offset, offset + limit);
      let truncated = total > offset + shown.length;
      while (shown.length > 0 && shown.join('\n').length > MAX_OUTPUT_CHARS) {
        shown.pop();
        truncated = true;
      }

      const summary = `[${total} files${truncated ? ' (truncated)' : ''}]`;
      let output = `${summary}\n${shown.join('\n')}`;
      // When the result set was cut off, tell the model HOW to recover (narrow the
      // search) rather than just that it was truncated — mirrors Claude Code's
      // "Consider using a more specific path or pattern." affordance.
      const narrowHint = ' Consider using a more specific path or pattern to narrow the search.';
      if (offset > 0) {
        output += `\n\n... showing ${offset + 1}-${offset + shown.length} of ${total} matches`;
        if (truncated) output += `.${narrowHint}`;
      } else if (truncated) {
        output += `\n\n... truncated (showing ${shown.length} of ${total} matches).${narrowHint}`;
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
