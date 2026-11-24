// Split from workspace-indexer.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
/* eslint-disable security/detect-unsafe-regex -- Symbol extraction regexes are applied to controlled file content, not user input */
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { SCORE_MATCH, BONUS_CONSECUTIVE, BONUS_FIRST_CHAR, PENALTY_GAP_START, PENALTY_GAP_EXTENSION, boundaryBonus } from './fuzzy-score';

/**
 * Tunable knobs for the chunked async index build (mirrors the reference's
 * file-index `CHUNK_MS`). The build parses files in a tight await-loop but
 * yields to the event loop on a WALL-CLOCK time budget — not a fixed file
 * count — because a count that is ~2ms on fast hardware can be 15ms+ on older
 * machines, which would stall the main thread. `chunkMs` is the budget; the
 * elapsed check runs only every `chunkCheckInterval` files to amortize the
 * `performance.now()` overhead. Exposed as a mutable object so the cadence is
 * tunable and deterministically testable.
 */
export const indexBuildConfig: { chunkMs: number; chunkCheckInterval: number } = {
  chunkMs: 4,
  chunkCheckInterval: 256,
};

// ---------------------------------------------------------------------------
// Fuzzy (subsequence) symbol-name scoring
//
// Mirrors the reference's nucleo/fzf-v2-style scorer (references
// native-ts/file-index/index.ts) so symbol search ranks by MATCH QUALITY
// (boundary / camelCase / consecutive bonuses minus gap penalties) and can
// match non-contiguous queries (e.g. 'wspidx' → 'WorkspaceIndexer'), instead
// of the old case-insensitive substring test + coarse 3-tier alpha sort.
// ---------------------------------------------------------------------------

// Nucleo/fzf-v2 scoring constants + the boundary/camel classifier are shared
// with the file-path index via ./fuzzy-score (one canonical match-quality
// definition — see No-Duplication Rule). This module layers tier bonuses +
// a test-path penalty on top for symbol-name ranking.

// Tier bonuses layered on the fuzzy score so the historical
// exact > starts-with > subsequence ordering is preserved while still ranking
// by quality within each tier. Large enough to dominate any fuzzy delta.
export const BONUS_EXACT = 100_000;

export const BONUS_PREFIX = 10_000;

/**
 * Deliberate down-rank of test / spec / fixture paths so a test file's symbol
 * does not outrank the real implementation on an otherwise-equal match. Mirrors
 * the reference's 1.05× test-path penalty, applied here as a small score nudge
 * (ties only — never overrides a genuinely better match).
 */
export const TEST_PATH_PENALTY = 2;

export const TEST_PATH_RE =
  /(^|[\\/])(__tests__|__mocks__|tests?|spec|fixtures?)([\\/]|$)|\.(test|spec)\./i;

/**
 * Score how well `needle` (already length-clamped) fuzzy-matches `name`.
 * Returns null when `needle` is not a subsequence of `name`. Higher is better.
 * Boundary/camel/consecutive bonuses + gap penalties mirror the reference's
 * file-index scorer; a shorter-name bonus favors tighter matches among
 * equal-quality candidates.
 *
 * Smart case: when `caseSensitive` is false (the default), `needle` must be
 * lowercased by the caller and the match is case-insensitive (the haystack is
 * lowercased here). When `caseSensitive` is true, `needle` keeps its original
 * casing and the match is exact-case — mirroring the reference's
 * `caseSensitive = query !== query.toLowerCase()` per-query derivation. The
 * lowercase transform preserves character positions, so boundary/camel bonuses
 * are always computed against the original-case `name`.
 */
export function fuzzySymbolScore(name: string, needle: string, caseSensitive = false): number | null {
  const nLen = needle.length;
  if (nLen === 0) return null;
  const haystack = caseSensitive ? name : name.toLowerCase();
  const hLen = haystack.length;

  // Greedy earliest-subsequence positions.
  const positions: number[] = new Array(nLen);
  let from = 0;
  for (let j = 0; j < nLen; j++) {
    const pos = haystack.indexOf(needle[j]!, from);
    if (pos === -1) return null;
    positions[j] = pos;
    from = pos + 1;
  }

  let score = nLen * SCORE_MATCH;
  let prev = -2;
  for (let j = 0; j < nLen; j++) {
    const pos = positions[j]!;
    if (j === 0) {
      score += pos === 0 ? BONUS_FIRST_CHAR : boundaryBonus(name, pos);
    } else {
      const gap = pos - prev - 1;
      if (gap === 0) score += BONUS_CONSECUTIVE;
      else score -= PENALTY_GAP_START + gap * PENALTY_GAP_EXTENSION;
      score += boundaryBonus(name, pos);
    }
    prev = pos;
  }
  // Shorter names rank slightly higher for equal-quality matches.
  score += Math.max(0, 32 - (hLen >> 2));
  return score;
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface SymbolEntry {
  name: string;
  type: 'function' | 'class' | 'method' | 'variable' | 'interface' | 'type' | 'import' | 'export';
  filePath: string;
  line: number;
  language: string;
}

export interface FileEntry {
  path: string;
  relativePath: string;
  language: string;
  size: number;
  lastModified: number;
  symbols: SymbolEntry[];
  imports: string[];  // file paths this file imports from
}

export interface ImportEdge {
  from: string;  // importing file
  to: string;    // imported file/module
}

/**
 * Result of {@link WorkspaceIndexer.buildIndex}. Beyond the headline counts it
 * carries stage-level introspection (which discovery path ran, per-stage wall
 * time, whether the MAX_FILES cap bit) and a `cached` flag set when a
 * change-detection fast path skipped a redundant full rebuild — mirroring the
 * granular diagnostics the reference logs around its context-gathering path.
 */
export interface BuildIndexResult {
  fileCount: number;
  symbolCount: number;
  duration: number;
  /** Set only when the build was a no-op (e.g. no project root found). */
  skippedReason?: string;
  /** Which discovery path ran: git ls-files, the glob fallback, or none. */
  discovery: 'git' | 'glob' | 'none';
  /** True when a change-detection fast path reused the existing index. */
  cached: boolean;
  /** True when the discovered file list was clamped to MAX_FILES. */
  maxFilesCapped: boolean;
  /** Per-stage wall-clock timings (ms). */
  stages: { discoveryMs: number; parseMs: number };
}

// ---------------------------------------------------------------------------
// Language patterns
// ---------------------------------------------------------------------------

export interface PatternDef {
  regex: RegExp;
  type: SymbolEntry['type'];
  /** For import patterns the captured group is a module path, not a symbol name. */
  isImport?: boolean;
}

export function langPatterns(): Record<string, PatternDef[]> {
  return {
    typescript: [
      { regex: /^(?:export\s+)?(?:async\s+)?function\s+(\w+)/gm, type: 'function' },
      { regex: /^(?:export\s+)?(?:abstract\s+)?class\s+(\w+)/gm, type: 'class' },
      { regex: /^(?:export\s+)?interface\s+(\w+)/gm, type: 'interface' },
      { regex: /^(?:export\s+)?type\s+(\w+)\s*[=<]/gm, type: 'type' },
      { regex: /^(?:export\s+)?(?:const|let|var)\s+(\w+)\s*[=:]/gm, type: 'variable' },
      { regex: /import\s+.*?from\s+['"]([^'"]+)['"]/gm, type: 'import', isImport: true },
    ],
    python: [
      { regex: /^def\s+(\w+)/gm, type: 'function' },
      { regex: /^class\s+(\w+)/gm, type: 'class' },
      { regex: /^from\s+(\S+)\s+import/gm, type: 'import', isImport: true },
      { regex: /^import\s+(\S+)/gm, type: 'import', isImport: true },
    ],
    go: [
      { regex: /^func\s+(?:\(\w+\s+\*?\w+\)\s+)?(\w+)/gm, type: 'function' },
      { regex: /^type\s+(\w+)\s+struct/gm, type: 'class' },
      { regex: /^type\s+(\w+)\s+interface/gm, type: 'interface' },
      { regex: /^\s*"([^"]+)"\s*$/gm, type: 'import', isImport: true },
    ],
    rust: [
      { regex: /^(?:pub\s+)?(?:async\s+)?fn\s+(\w+)/gm, type: 'function' },
      { regex: /^(?:pub\s+)?struct\s+(\w+)/gm, type: 'class' },
      { regex: /^(?:pub\s+)?trait\s+(\w+)/gm, type: 'interface' },
      { regex: /^(?:pub\s+)?enum\s+(\w+)/gm, type: 'type' },
      { regex: /^use\s+(\S+);/gm, type: 'import', isImport: true },
    ],
    java: [
      { regex: /^(?:public|private|protected)?\s*(?:static\s+)?(?:final\s+)?(?:\w+\s+)(\w+)\s*\(/gm, type: 'function' },
      { regex: /(?:public\s+)?(?:abstract\s+)?class\s+(\w+)/gm, type: 'class' },
      { regex: /(?:public\s+)?interface\s+(\w+)/gm, type: 'interface' },
      { regex: /^import\s+(\S+);/gm, type: 'import', isImport: true },
    ],
  };
}

// Map file extensions to language keys
export const EXT_TO_LANG: Record<string, string> = {
  '.ts': 'typescript', '.tsx': 'typescript', '.js': 'typescript', '.jsx': 'typescript',
  '.mts': 'typescript', '.mjs': 'typescript', '.cjs': 'typescript', '.cts': 'typescript',
  '.py': 'python', '.pyw': 'python',
  '.go': 'go',
  '.rs': 'rust',
  '.java': 'java',
};

export const MAX_FILES = 5000; // Cap file count to prevent slow indexing

// ---------------------------------------------------------------------------
// Project detection — prevent indexing non-project directories (e.g. ~)
// ---------------------------------------------------------------------------

export const PROJECT_MARKERS = [
  '.git', 'package.json', 'Cargo.toml', 'go.mod', 'pyproject.toml',
  'setup.py', 'Makefile', 'CMakeLists.txt', 'pom.xml', 'build.gradle',
  'Gemfile', 'composer.json', 'tsconfig.json', 'deno.json', '.project',
  'mix.exs', 'Pipfile', 'cabal.project', 'stack.yaml',
];

export function findProjectRoot(startDir: string): string | null {
  let dir = path.resolve(startDir);
  const root = path.parse(dir).root;
  const home = path.resolve(os.homedir());
  let depth = 0;
  while (dir !== root && depth < 20) {
    for (const marker of PROJECT_MARKERS) {
      try {
        fs.accessSync(path.join(dir, marker));
        return dir;
      } catch { /* not found */ }
    }
    // Stop at home directory — don't walk above it into /home or /
    if (dir === home) break;
    dir = path.dirname(dir);
    depth++;
  }
  return null;
}

// Bound the `git ls-files` subprocess so a hung git (credential prompt on a
// misconfigured remote, a stalled network/overlay filesystem, an enormous repo)
// cannot block index build — and every awaiting search_symbols/find_references
// call — indefinitely. On timeout/error we return null so buildIndex degrades
// gracefully to the glob fallback. Mirrors the reference's 5s git-subprocess cap.
export const GIT_LS_FILES_TIMEOUT_MS = 5_000;

/**
 * Cheap change signal for tracked files: the mtime of `.git/index`, which git
 * bumps on any staged/tracked-file change — so an unchanged mtime means nothing
 * tracked changed and a rebuild can be skipped WITHOUT spawning `git ls-files`.
 * Returns null for worktrees/submodules (`.git` is a file → ENOTDIR), fresh
 * repos with no index yet (ENOENT), and non-git dirs; the caller then falls back
 * to a path-list signature. Mirrors the reference's getGitIndexMtime.
 */
export function gitIndexMtime(projectRoot: string): number | null {
  try {
    const st = fs.statSync(path.join(projectRoot, '.git', 'index'));
    return st.isFile() ? st.mtimeMs : null;
  } catch {
    return null;
  }
}

/**
 * Order-sensitive content signature of a discovered path list (count + FNV-1a
 * hash). Used to skip the full re-parse when a rebuild discovers a byte-identical
 * file set — analogous to the reference's sampled `pathListSignature`.
 */
export function fileListSignature(paths: string[]): string {
  let h = 0x811c9dc5;
  for (const p of paths) {
    for (let i = 0; i < p.length; i++) {
      h = Math.imul(h ^ p.charCodeAt(i), 0x01000193);
    }
    h = Math.imul(h ^ 0x2f, 0x01000193); // separator between entries
  }
  return `${paths.length}:${(h >>> 0).toString(16)}`;
}

// ---------------------------------------------------------------------------
// .gitignore honoring for the non-git (glob fallback) discovery branch.
//
// When a workspace is NOT a git work tree (or `git` is unavailable), discovery
// falls back to a raw glob. Without honoring the project's ignore rules, files
// the author deliberately excluded (secrets, build output, local env dumps)
// would be read, symbol-indexed, and surfaced in getSummary() — a correctness
// and information-exposure gap. These helpers translate a `.gitignore` (and
// `.git/info/exclude`, and nested `.gitignore` files) into tinyglobby `ignore`
// globs so excluded paths are never read.
// ---------------------------------------------------------------------------

/**
 * Convert the contents of a single gitignore-format file into tinyglobby
 * `ignore` glob patterns (POSIX separators, relative to the index cwd).
 *
 * `baseRelDir` is the gitignore file's directory relative to the index root
 * ('' for the root .gitignore). Patterns in a nested .gitignore are anchored
 * to that directory, matching git's semantics.
 *
 * Supported gitignore semantics (a correct, conservative subset):
 *  - blank lines and `#` comments are skipped;
 *  - a leading `!` (negation / un-ignore) line is skipped — we never add an
 *    exclude for it, so negated paths stay visible (conservative: we under-
 *    ignore rather than risk hiding a file the author kept);
 *  - a trailing `/` marks a directory — only its contents are excluded;
 *  - a leading `/` (or any embedded `/`) anchors the pattern to `baseRelDir`;
 *  - a bare name (no slash) matches at any depth below `baseRelDir`;
 *  - `\#` / `\!` escape a literal leading `#` / `!`.
 * Each pattern yields a file-form glob and a dir-contents-form glob so both a
 * matching file and a matching directory's descendants are excluded.
 */
export function gitignoreToIgnoreGlobs(content: string, baseRelDir = ''): string[] {
  const prefix = baseRelDir ? baseRelDir.replace(/\\/g, '/').replace(/\/+$/, '') : '';
  const globs: string[] = [];

  for (const rawLine of content.split(/\r?\n/)) {
    // Trim trailing whitespace (git ignores it unless backslash-escaped, which
    // is rare enough to treat as trailing-space-insensitive here); keep leading.
    let line = rawLine.replace(/\s+$/, '');
    if (line === '' || line.startsWith('#')) continue;
    if (line.startsWith('!')) continue; // negation — conservatively un-honored
    // Unescape a leading escaped '#' or '!'.
    if (line.startsWith('\\#') || line.startsWith('\\!')) line = line.slice(1);

    const dirOnly = line.endsWith('/');
    let pat = dirOnly ? line.slice(0, -1) : line;
    const anchored = pat.startsWith('/');
    if (anchored) pat = pat.slice(1);
    if (pat === '') continue;

    const hasSlash = pat.includes('/');
    // Anchored or path-bearing patterns resolve against baseRelDir; a bare name
    // matches at any depth below it.
    let base: string;
    if (anchored || hasSlash) {
      base = prefix ? `${prefix}/${pat}` : pat;
    } else {
      base = prefix ? `${prefix}/**/${pat}` : `**/${pat}`;
    }

    if (!dirOnly) globs.push(base);
    globs.push(`${base}/**`);
  }

  return globs;
}

// ---------------------------------------------------------------------------
// getSummary presentation knobs — the summary is injected verbatim into the
// system prompt, so it carries two affordances the reference attaches to its own
// injected context (references src/context.ts):
//   1. a snapshot/staleness caveat so the model does not over-trust counts that
//      go stale as it edits (mirrors context.ts:96-97);
//   2. an actionable truncation hint so a clipped summary tells the model how to
//      recover the dropped detail (mirrors context.ts:85-89) instead of a silent
//      '...' slice that hides that anything was cut.
// ---------------------------------------------------------------------------

/** Total character budget for the injected workspace summary (incl. caveat/hint). */
export const SUMMARY_MAX_CHARS = 500;

/** Leading caveat so the model treats the summary as a point-in-time snapshot. */
export const SUMMARY_SNAPSHOT_CAVEAT =
  'Workspace index (snapshot at conversation start; not updated as you edit — re-run search tools for current state):';

/** Actionable hint appended when the summary is truncated to the budget. */
export const SUMMARY_TRUNCATION_HINT =
  ' ...(truncated; use the grep/search_symbols tools to find more symbols)';
