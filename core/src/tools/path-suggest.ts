import * as fs from 'fs';
import * as path from 'path';
import { logError } from '../logger';
import { isWithinWorkspaceRoots } from './tool-utils';
import { matchesReadIgnore } from './read-ignore';
import { findProjectRoot, gitignoreToIgnoreGlobs } from '../workspace-indexer';

// PARITY (error observability): a directory scan that fails with ENOENT is the
// expected "parent does not exist" case and is swallowed quietly. Any OTHER
// failure (EACCES permission-denied, ENOTDIR, EMFILE, …) is unexpected and was
// previously discarded silently, making path-suggestion gaps undiagnosable.
// Mirrors the reference's findSimilarFile (`if (!isENOENT(error)) logError(...)`).
function isENOENT(err: unknown): boolean {
  return (
    typeof err === 'object' &&
    err !== null &&
    (err as NodeJS.ErrnoException).code === 'ENOENT'
  );
}

function logScanError(component: string, dir: string, err: unknown): void {
  if (isENOENT(err)) return;
  const msg = err instanceof Error ? err.message : String(err);
  logError(component, `directory scan failed: ${dir}: ${msg}`);
}

export function levenshteinDistance(a: string, b: string): number {
  if (a.length === 0) return b.length;
  if (b.length === 0) return a.length;

  const matrix: number[][] = [];
  for (let i = 0; i <= a.length; i++) {
    matrix[i] = [i];
  }
  for (let j = 0; j <= b.length; j++) {
    matrix[0][j] = j;
  }

  for (let i = 1; i <= a.length; i++) {
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      matrix[i][j] = Math.min(
        matrix[i - 1][j] + 1,
        matrix[i][j - 1] + 1,
        matrix[i - 1][j - 1] + cost,
      );
    }
  }
  return matrix[a.length][b.length];
}

// PARITY (bounds hardening): clamp a caller-supplied count/budget to a safe,
// non-negative integer. Guards against negative values (e.g. a negative
// maxResults would turn `slice(0, n)` into a from-the-end slice and leak
// unintended entries) and non-finite inputs. ADDITIVE: positive integers pass
// through unchanged, so existing callers keep identical behavior.
function clampCount(n: number): number {
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.floor(n));
}

// PARITY (secret-leak hardening): a directory-entry predicate that answers "may
// this entry be surfaced as a 'did you mean' suggestion?". the reference's
// suggestion pipeline filters ignored files out of results so secret/ignored
// filenames are never disclosed (hooks/fileSuggestions.ts:289-295); AMI's
// readdir-and-score loop previously scored EVERY entry verbatim, so a near-miss
// of `.env` (or a gitignored `secret-config.json`) could leak the existence of a
// credential file. This builds a synchronous ignore matcher for `scanDir` that:
//   1. drops dotfiles/hidden entries (the dominant secret vector: .env, keys), and
//   2. drops gitignored entries, reusing the single canonical gitignore→glob
//      translator (`gitignoreToIgnoreGlobs`) + glob matcher (`matchesReadIgnore`)
//      rather than re-implementing ignore parsing.
// Patterns are gathered from the project root's `.git/info/exclude` and every
// `.gitignore` from the root down to `scanDir` (each anchored at its own dir).
function buildSuggestionIgnore(scanDir: string): (entry: string) => boolean {
  const root = findProjectRoot(scanDir);
  const globs: string[] = [];
  if (root) {
    try {
      globs.push(
        ...gitignoreToIgnoreGlobs(
          fs.readFileSync(path.join(root, '.git', 'info', 'exclude'), 'utf-8'),
          '',
        ),
      );
    } catch {
      // absent/unreadable exclude file — nothing to contribute.
    }
    const rel = path.relative(root, scanDir);
    const segs = rel ? rel.split(path.sep).filter(Boolean) : [];
    let cur = root;
    for (let i = 0; ; i++) {
      const baseRel = path.relative(root, cur).split(path.sep).join('/');
      try {
        const content = fs.readFileSync(path.join(cur, '.gitignore'), 'utf-8');
        globs.push(...gitignoreToIgnoreGlobs(content, baseRel));
      } catch {
        // no .gitignore at this level — skip.
      }
      if (i >= segs.length) break;
      cur = path.join(cur, segs[i]);
    }
  }
  return (entry: string): boolean => {
    // Hidden/dotfiles are always withheld (credential stores, VCS metadata).
    if (entry.startsWith('.')) return true;
    if (!root || globs.length === 0) return false;
    const relFromRoot = path
      .relative(root, path.join(scanDir, entry))
      .split(path.sep)
      .join('/');
    return matchesReadIgnore(relFromRoot, globs);
  };
}

export function suggestSimilarPaths(
  missingPath: string,
  cwd: string,
  maxResults = 3,
  maxDistance = 3,
): string[] {
  const limit = clampCount(maxResults);
  const budget = clampCount(maxDistance);
  const abs = path.isAbsolute(missingPath) ? missingPath : path.resolve(cwd, missingPath);

  if (fs.existsSync(abs)) return [];

  const dir = path.dirname(abs);
  const base = path.basename(abs);
  // PARITY (same-stem/different-extension): the basename WITHOUT its extension.
  const stemLower = path.basename(base, path.extname(base)).toLowerCase();

  // PARITY (workspace-boundary containment): never enumerate a directory that
  // lives OUTSIDE the caller's workspace (cwd). An out-of-tree target would
  // otherwise disclose sibling-filesystem contents via the suggestion list.
  // Mirrors the reference's cwd-scoped suggestion enumeration
  // (hooks/fileSuggestions.ts:243-316). Reuses the single canonical containment
  // helper so the check matches the rest of the tool layer exactly.
  if (!isWithinWorkspaceRoots(dir, cwd)) return [];

  let entries: string[];
  try {
    entries = fs.readdirSync(dir);
  } catch (err) {
    logScanError('suggestSimilarPaths', dir, err);
    return [];
  }

  // Secret-leak hardening: withhold dotfiles and gitignored entries before they
  // are scored (see buildSuggestionIgnore).
  const isIgnored = buildSuggestionIgnore(dir);

  const scored: Array<{ name: string; distance: number; stem: boolean }> = [];
  for (const entry of entries) {
    if (isIgnored(entry)) continue;
    const dist = levenshteinDistance(base.toLowerCase(), entry.toLowerCase());
    // PARITY (same-stem suggestion, findSimilarFile): an entry whose basename
    // stripped of its extension equals the requested stem (e.g. `config.ts`
    // requested, `config.toml` present) is always a strong "did you mean" — even
    // when the extensions differ by more than `maxDistance` characters. The old
    // levenshtein-only filter dropped these. ADDITIVE: surface same-stem matches
    // regardless of the edit-distance budget, ranked ahead of plain typo hits.
    const sameStem =
      entry !== base &&
      path.basename(entry, path.extname(entry)).toLowerCase() === stemLower;
    if (dist === 0) {
      // PARITY (case correction): a case-only difference (e.g. `README.md`
      // requested as `readme.md` on a case-sensitive filesystem) is one of the
      // strongest "did you mean" signals. The previous implementation dropped
      // these entirely via the `dist > 0` filter. ADDITIVE: surface them with
      // the lowest distance so they rank ahead of typo corrections. (An entry
      // whose raw name equals `base` would mean the file exists and we would
      // have returned above, so `entry !== base` is always a true case mismatch.)
      if (entry !== base) scored.push({ name: entry, distance: 0, stem: false });
    } else if (dist <= budget || sameStem) {
      scored.push({ name: entry, distance: dist, stem: sameStem });
    }
  }

  scored.sort((a, b) => {
    // Exact-case corrections (distance 0) always rank first.
    if ((a.distance === 0) !== (b.distance === 0)) return a.distance === 0 ? -1 : 1;
    // Then same-stem/different-extension matches, independent of edit distance.
    if (a.stem !== b.stem) return a.stem ? -1 : 1;
    // Then by edit distance (nearest typo first).
    return a.distance - b.distance;
  });
  return scored.slice(0, limit).map(s => path.join(dir, s.name));
}

// ---------------------------------------------------------------------------
// PARITY (rich did-you-mean): report WHY a path was corrected and recover from
// a misspelled *directory* component, not just a misspelled basename.
// ---------------------------------------------------------------------------

export type PathSuggestionKind = 'exact-case' | 'typo' | 'missing-dir' | 'none';

export interface PathCorrectionOptions {
  maxResults?: number;
  maxDistance?: number;
}

export interface PathCorrectionResult {
  kind: PathSuggestionKind;
  suggestions: string[];
}

/**
 * PARITY: a structured "did you mean" correction for a missing path.
 *
 * Unlike {@link suggestSimilarPaths} (which only ever returns bare strings and
 * only inspects the immediate parent directory), this reports a `kind` so a
 * caller can tailor the message, and it recovers from a misspelled *directory*
 * component by walking up to the nearest existing ancestor and correcting the
 * missing segment there.
 *
 * - `exact-case`  — a sibling differs only by letter case.
 * - `typo`        — a sibling is within the edit-distance budget.
 * - `missing-dir` — the parent directory itself is misspelled; the corrected
 *                   directory (and the target inside it) is suggested.
 * - `none`        — nothing close enough, or the path already exists.
 */
export function suggestPathCorrection(
  missingPath: string,
  cwd: string,
  opts: PathCorrectionOptions = {},
): PathCorrectionResult {
  const maxResults = clampCount(opts.maxResults ?? 3);
  const maxDistance = clampCount(opts.maxDistance ?? 3);
  const abs = path.isAbsolute(missingPath) ? missingPath : path.resolve(cwd, missingPath);

  if (fs.existsSync(abs)) return { kind: 'none', suggestions: [] };

  const dir = path.dirname(abs);
  const base = path.basename(abs);

  let entries: string[] | null;
  try {
    entries = fs.readdirSync(dir);
  } catch {
    entries = null;
  }

  if (entries) {
    // Apply the same secret-leak filter as suggestSimilarPaths so a case-variant
    // of a dotfile/gitignored name (e.g. `.ENV` → `.env`) is never disclosed.
    const isIgnored = buildSuggestionIgnore(dir);
    const caseMatches = entries.filter(
      e => e !== base && e.toLowerCase() === base.toLowerCase() && !isIgnored(e),
    );
    if (caseMatches.length > 0) {
      return {
        kind: 'exact-case',
        suggestions: caseMatches.slice(0, maxResults).map(e => path.join(dir, e)),
      };
    }
    const typo = suggestSimilarPaths(abs, cwd, maxResults, maxDistance);
    if (typo.length > 0) return { kind: 'typo', suggestions: typo };
    return { kind: 'none', suggestions: [] };
  }

  // Parent directory does not exist / is unreadable — try to correct the
  // misspelled directory component against its own parent.
  const parent = path.dirname(dir);
  const dirBase = path.basename(dir);
  let siblings: string[];
  try {
    siblings = fs.readdirSync(parent);
  } catch {
    return { kind: 'none', suggestions: [] };
  }

  const dirCandidates = siblings
    .filter(s => {
      try {
        return fs.statSync(path.join(parent, s)).isDirectory();
      } catch {
        return false;
      }
    })
    .map(s => ({ s, d: levenshteinDistance(dirBase.toLowerCase(), s.toLowerCase()) }))
    .filter(e => e.d <= maxDistance)
    .sort((a, b) => a.d - b.d);

  const out: string[] = [];
  for (const c of dirCandidates) {
    const correctedDir = path.join(parent, c.s);
    const candidate = path.join(correctedDir, base);
    if (fs.existsSync(candidate)) {
      out.push(candidate);
    } else {
      for (const sim of suggestSimilarPaths(candidate, correctedDir, maxResults, maxDistance)) {
        out.push(sim);
      }
    }
    if (out.length >= maxResults) break;
  }

  if (out.length > 0) {
    return { kind: 'missing-dir', suggestions: out.slice(0, maxResults) };
  }
  return { kind: 'none', suggestions: [] };
}

// ---------------------------------------------------------------------------
// PARITY (basename-wide search): find a file by its basename elsewhere in the
// tree, not just in the directory the caller guessed. Covers the common case of
// a file that was moved/renamed into another folder.
// ---------------------------------------------------------------------------

export interface BasenameSearchOptions {
  maxResults?: number;
  maxDistance?: number;
  maxDepth?: number;
}

const BASENAME_SKIP_DIRS = new Set(['node_modules', '.git']);

/**
 * PARITY: suggest real paths whose *basename* matches (or is close to) the
 * basename of `missingPath`, searching the tree rooted at `searchRoot` up to a
 * bounded depth. Results are ranked nearest-first (edit distance, then shorter
 * path), de-duplicated, and capped at `maxResults`. `node_modules`/`.git` are
 * skipped; symlinked directories are not traversed (loop-safe).
 */
export function suggestByBasename(
  missingPath: string,
  searchRoot: string,
  opts: BasenameSearchOptions = {},
): string[] {
  const maxResults = clampCount(opts.maxResults ?? 5);
  const maxDistance = clampCount(opts.maxDistance ?? 2);
  const maxDepth = clampCount(opts.maxDepth ?? 5);

  const baseLower = path.basename(missingPath).toLowerCase();
  const matches: Array<{ p: string; d: number }> = [];

  const walk = (dir: string, depth: number): void => {
    if (depth > maxDepth) return;
    let ents: fs.Dirent[];
    try {
      ents = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of ents) {
      const full = path.join(dir, e.name);
      if (e.isDirectory()) {
        if (!BASENAME_SKIP_DIRS.has(e.name)) walk(full, depth + 1);
      } else if (e.isFile()) {
        const d = levenshteinDistance(baseLower, e.name.toLowerCase());
        if (d <= maxDistance) matches.push({ p: full, d });
      }
    }
  };

  walk(searchRoot, 0);

  matches.sort((a, b) => a.d - b.d || a.p.length - b.p.length);

  const seen = new Set<string>();
  const out: string[] = [];
  for (const m of matches) {
    if (seen.has(m.p)) continue;
    seen.add(m.p);
    out.push(m.p);
    if (out.length >= maxResults) break;
  }
  return out;
}

// ---------------------------------------------------------------------------
// PARITY (suggestPathUnderCwd): recover from the common "dropped repo folder"
// model error where an absolute path is built as a SIBLING of the project root
// rather than inside it. Mirrors the reference's suggestPathUnderCwd
// (references/.../utils/file.ts:L228-267) but synchronous — matching this
// module's fs usage — and takes `cwd` explicitly for testability.
// ---------------------------------------------------------------------------

/**
 * PARITY: suggest a corrected path under `cwd` when an absolute path was not
 * found and matches the "dropped repo folder" pattern.
 *
 * Example:
 *   cwd           = /u/src/currentRepo
 *   requestedPath = /u/src/foobar            (does not exist)
 *   returns         /u/src/currentRepo/foobar (when it exists under cwd)
 *
 * The requested path's parent is realpath-resolved first (so `/tmp` → `/private/tmp`
 * style symlinks line up with a realpath-resolved cwd). Only paths that live
 * under cwd's PARENT but NOT under cwd itself are candidates — anything already
 * under cwd, equal to cwd, or outside the parent is left alone. Returns the
 * corrected absolute path if it exists, otherwise `undefined`.
 */
export function suggestPathUnderCwd(
  requestedPath: string,
  cwd: string,
): string | undefined {
  let resolvedCwd = cwd;
  try {
    resolvedCwd = fs.realpathSync(cwd);
  } catch {
    // cwd not resolvable — fall back to the raw value for the prefix check.
  }
  const cwdParent = path.dirname(resolvedCwd);

  // Resolve symlinks in the requested path's PARENT so the prefix comparison
  // lines up with the realpath-resolved cwd.
  let resolvedPath = requestedPath;
  try {
    const resolvedDir = fs.realpathSync(path.dirname(requestedPath));
    resolvedPath = path.join(resolvedDir, path.basename(requestedPath));
  } catch {
    // Parent directory does not exist — keep the original path.
  }

  // Only act when the requested path is under cwd's parent but NOT under cwd
  // itself. When cwdParent is the filesystem root, use it directly to avoid a
  // double-separator prefix that could never match.
  const cwdParentPrefix = cwdParent === path.sep ? path.sep : cwdParent + path.sep;
  if (
    !resolvedPath.startsWith(cwdParentPrefix) ||
    resolvedPath.startsWith(resolvedCwd + path.sep) ||
    resolvedPath === resolvedCwd
  ) {
    return undefined;
  }

  const relFromParent = path.relative(cwdParent, resolvedPath);
  const correctedPath = path.join(resolvedCwd, relFromParent);
  if (fs.existsSync(correctedPath)) return correctedPath;
  return undefined;
}
