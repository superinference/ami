/* eslint-disable security/detect-unsafe-regex -- Symbol extraction regexes are applied to controlled file content, not user input */
import * as fs from 'fs';
import * as fsp from 'fs/promises';
import * as path from 'path';
import * as os from 'os';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { glob } from 'tinyglobby';
import { isGeneratedFile } from './generated-files';

const execFileAsync = promisify(execFile);

/**
 * File-system hooks for the indexer. Defaults to the real implementations;
 * the `glob` entry is overridable in tests purely as a dependency-injection
 * seam so the glob-failure fallback can be exercised without a pathological
 * filesystem. The `execFileAsync` seam exists so the git-subprocess timeout /
 * failure fallback in getGitTrackedFiles can be exercised deterministically.
 * Production behavior is identical to calling `tinyglobby.glob` /
 * `child_process.execFile`.
 */
export const indexerHooks: { glob: typeof glob; execFileAsync: typeof execFileAsync } = {
  glob,
  execFileAsync,
};

// ---------------------------------------------------------------------------
// Fuzzy (subsequence) symbol-name scoring
//
// Mirrors Claude Code's nucleo/fzf-v2-style scorer (references
// native-ts/file-index/index.ts) so symbol search ranks by MATCH QUALITY
// (boundary / camelCase / consecutive bonuses minus gap penalties) and can
// match non-contiguous queries (e.g. 'wspidx' → 'WorkspaceIndexer'), instead
// of the old case-insensitive substring test + coarse 3-tier alpha sort.
// ---------------------------------------------------------------------------

const SCORE_MATCH = 16;
const BONUS_BOUNDARY = 8;
const BONUS_CAMEL = 6;
const BONUS_CONSECUTIVE = 4;
const BONUS_FIRST_CHAR = 8;
const PENALTY_GAP_START = 3;
const PENALTY_GAP_EXTENSION = 1;

// Tier bonuses layered on the fuzzy score so the historical
// exact > starts-with > subsequence ordering is preserved while still ranking
// by quality within each tier. Large enough to dominate any fuzzy delta.
const BONUS_EXACT = 100_000;
const BONUS_PREFIX = 10_000;

/**
 * Query-length cap for fuzzy symbol search. Bounds the per-symbol scan so a
 * pathological / oversized query cannot blow up scoring cost (DoS / resource
 * guard). Mirrors Claude Code's MAX_QUERY_LEN=64.
 */
export const MAX_SYMBOL_QUERY_LEN = 64;

/**
 * Deliberate down-rank of test / spec / fixture paths so a test file's symbol
 * does not outrank the real implementation on an otherwise-equal match. Mirrors
 * Claude Code's 1.05× test-path penalty, applied here as a small score nudge
 * (ties only — never overrides a genuinely better match).
 */
const TEST_PATH_PENALTY = 2;
const TEST_PATH_RE =
  /(^|[\\/])(__tests__|__mocks__|tests?|spec|fixtures?)([\\/]|$)|\.(test|spec)\./i;

function isBoundary(code: number): boolean {
  // / \ - _ . space
  return code === 47 || code === 92 || code === 45 || code === 95 || code === 46 || code === 32;
}
function isLower(code: number): boolean {
  return code >= 97 && code <= 122;
}
function isUpper(code: number): boolean {
  return code >= 65 && code <= 90;
}

/**
 * Boundary / camelCase bonus for a match at position `pos` (`pos >= 1`) in the
 * original-case `name`. The first-char bonus is handled by the caller.
 */
function boundaryBonus(name: string, pos: number): number {
  const prevCh = name.charCodeAt(pos - 1);
  if (isBoundary(prevCh)) return BONUS_BOUNDARY;
  if (isLower(prevCh) && isUpper(name.charCodeAt(pos))) return BONUS_CAMEL;
  return 0;
}

/**
 * Score how well `needle` (already length-clamped) fuzzy-matches `name`.
 * Returns null when `needle` is not a subsequence of `name`. Higher is better.
 * Boundary/camel/consecutive bonuses + gap penalties mirror Claude Code's
 * file-index scorer; a shorter-name bonus favors tighter matches among
 * equal-quality candidates.
 *
 * Smart case: when `caseSensitive` is false (the default), `needle` must be
 * lowercased by the caller and the match is case-insensitive (the haystack is
 * lowercased here). When `caseSensitive` is true, `needle` keeps its original
 * casing and the match is exact-case — mirroring Claude Code's
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
 * granular diagnostics Claude Code logs around its context-gathering path.
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

interface PatternDef {
  regex: RegExp;
  type: SymbolEntry['type'];
  /** For import patterns the captured group is a module path, not a symbol name. */
  isImport?: boolean;
}

function langPatterns(): Record<string, PatternDef[]> {
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
const EXT_TO_LANG: Record<string, string> = {
  '.ts': 'typescript', '.tsx': 'typescript', '.js': 'typescript', '.jsx': 'typescript',
  '.mts': 'typescript', '.mjs': 'typescript', '.cjs': 'typescript', '.cts': 'typescript',
  '.py': 'python', '.pyw': 'python',
  '.go': 'go',
  '.rs': 'rust',
  '.java': 'java',
};

const SKIP_DIRS = new Set([
  'node_modules', '.git', 'dist', 'build', '__pycache__', 'venv', '.venv',
  'coverage', '.next', '.cache', '.tox', 'target', '.turbo', '.parcel-cache',
  '.nuxt', '.output', '.idea', '.vscode', 'vendor', 'references', '.ami',
]);

const MAX_FILE_SIZE = 500 * 1024; // 500 KB
const MAX_FILES = 5000; // Cap file count to prevent slow indexing

// ---------------------------------------------------------------------------
// Project detection — prevent indexing non-project directories (e.g. ~)
// ---------------------------------------------------------------------------

const PROJECT_MARKERS = [
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
// gracefully to the glob fallback. Mirrors Claude Code's 5s git-subprocess cap.
const GIT_LS_FILES_TIMEOUT_MS = 5_000;

/**
 * Cheap change signal for tracked files: the mtime of `.git/index`, which git
 * bumps on any staged/tracked-file change — so an unchanged mtime means nothing
 * tracked changed and a rebuild can be skipped WITHOUT spawning `git ls-files`.
 * Returns null for worktrees/submodules (`.git` is a file → ENOTDIR), fresh
 * repos with no index yet (ENOENT), and non-git dirs; the caller then falls back
 * to a path-list signature. Mirrors Claude Code's getGitIndexMtime.
 */
function gitIndexMtime(projectRoot: string): number | null {
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
 * file set — analogous to Claude Code's sampled `pathListSignature`.
 */
function fileListSignature(paths: string[]): string {
  let h = 0x811c9dc5;
  for (const p of paths) {
    for (let i = 0; i < p.length; i++) {
      h = Math.imul(h ^ p.charCodeAt(i), 0x01000193);
    }
    h = Math.imul(h ^ 0x2f, 0x01000193); // separator between entries
  }
  return `${paths.length}:${(h >>> 0).toString(16)}`;
}

async function getGitTrackedFiles(cwd: string): Promise<string[] | null> {
  try {
    const { stdout } = await indexerHooks.execFileAsync('git', ['ls-files', '-z'], {
      cwd,
      maxBuffer: 10 * 1024 * 1024,
      timeout: GIT_LS_FILES_TIMEOUT_MS,
    });
    return stdout.split('\0').filter(Boolean);
  } catch {
    return null;
  }
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

/**
 * Gather ignore globs from the project's gitignore sources for the glob
 * fallback branch: the root `.gitignore`, `.git/info/exclude`, and any nested
 * `.gitignore` files. All IO is best-effort — a missing/unreadable source
 * simply contributes nothing. Nested discovery uses the same glob seam as the
 * main scan and is wrapped so a glob failure degrades to "no extra patterns".
 */
export async function collectGitignoreGlobs(cwd: string, skipGlobs: string[]): Promise<string[]> {
  const out: string[] = [];

  // Root .gitignore and .git/info/exclude (anchored at the index root).
  for (const rel of ['.gitignore', path.join('.git', 'info', 'exclude')]) {
    try {
      const content = await fsp.readFile(path.join(cwd, rel), 'utf-8');
      out.push(...gitignoreToIgnoreGlobs(content, ''));
    } catch { /* absent or unreadable — ignore */ }
  }

  // Nested .gitignore files (anchored at their own directory).
  let nested: string[] = [];
  try {
    nested = await indexerHooks.glob('**/.gitignore', {
      cwd,
      absolute: false,
      ignore: skipGlobs,
      dot: true,
      onlyFiles: true,
      followSymbolicLinks: false,
      deep: 10,
      expandDirectories: false,
    });
  } catch { /* glob unavailable/failed — root sources still apply */ }

  for (const relPath of nested) {
    const posix = relPath.replace(/\\/g, '/');
    if (posix === '.gitignore') continue; // already handled as a root source
    const baseRelDir = path.posix.dirname(posix);
    try {
      const content = await fsp.readFile(path.join(cwd, relPath), 'utf-8');
      out.push(...gitignoreToIgnoreGlobs(content, baseRelDir === '.' ? '' : baseRelDir));
    } catch { /* unreadable — ignore */ }
  }

  return out;
}

// ---------------------------------------------------------------------------
// getSummary presentation knobs — the summary is injected verbatim into the
// system prompt, so it carries two affordances Claude Code attaches to its own
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

// ---------------------------------------------------------------------------
// WorkspaceIndexer
// ---------------------------------------------------------------------------

export class WorkspaceIndexer {
  private files: Map<string, FileEntry> = new Map();
  private symbolIndex: Map<string, SymbolEntry[]> = new Map(); // symbol name -> entries
  private importGraph: Map<string, Set<string>> = new Map(); // file -> imported files
  private reverseImports: Map<string, Set<string>> = new Map(); // file -> files that import it
  // Signature of the last successfully-built index (git:<mtime> or
  // <discovery>-list:<hash>) used to skip redundant full rebuilds.
  private lastIndexSignature: string | null = null;

  readonly cwd: string;

  constructor(cwd: string) {
    this.cwd = cwd;
  }

  // -------------------------------------------------------------------------
  // Build the full index
  // -------------------------------------------------------------------------

  async buildIndex(): Promise<BuildIndexResult> {
    const start = Date.now();

    const projectRoot = findProjectRoot(this.cwd);
    if (!projectRoot) {
      return {
        fileCount: 0,
        symbolCount: 0,
        duration: Date.now() - start,
        skippedReason: 'no project root found',
        discovery: 'none',
        cached: false,
        maxFilesCapped: false,
        stages: { discoveryMs: 0, parseMs: 0 },
      };
    }

    // Change-detection fast path: if a prior git-backed build exists and
    // `.git/index` is unchanged, nothing tracked changed — skip spawning
    // `git ls-files` AND the full re-parse.
    const idxMtime = gitIndexMtime(projectRoot);
    if (this.files.size > 0 && idxMtime !== null && this.lastIndexSignature === `git:${idxMtime}`) {
      return {
        fileCount: this.files.size,
        symbolCount: this.getStats().symbolCount,
        duration: Date.now() - start,
        discovery: 'git',
        cached: true,
        maxFilesCapped: false,
        stages: { discoveryMs: 0, parseMs: 0 },
      };
    }

    const extSet = new Set(Object.keys(EXT_TO_LANG));
    let filePaths: string[];
    let discovery: 'git' | 'glob';

    const discoveryStart = Date.now();
    const gitFiles = await getGitTrackedFiles(this.cwd);
    if (gitFiles) {
      discovery = 'git';
      filePaths = gitFiles
        .filter(f => extSet.has(path.extname(f).toLowerCase()))
        .map(f => path.resolve(this.cwd, f));
    } else {
      discovery = 'glob';
      const extensions = Object.keys(EXT_TO_LANG).map(e => e.slice(1));
      const pattern = `**/*.{${extensions.join(',')}}`;
      const ignorePatterns = Array.from(SKIP_DIRS).map(d => `**/${d}/**`);

      // Honor the project's ignore rules so excluded files (secrets, build
      // output, local dumps) are never read or exposed via getSummary().
      const gitignoreGlobs = await collectGitignoreGlobs(this.cwd, ignorePatterns);
      ignorePatterns.push(...gitignoreGlobs);

      try {
        filePaths = await indexerHooks.glob(pattern, {
          cwd: this.cwd,
          absolute: true,
          ignore: ignorePatterns,
          dot: false,
          onlyFiles: true,
          followSymbolicLinks: false,
          deep: 10,
          expandDirectories: false,
        });
      } catch {
        filePaths = [];
      }
    }
    // Exclude generated/vendored files (committed lockfiles, *.d.ts, *.min.js,
    // *.bundle.js, protobuf/gRPC/swagger output, build/vendor dirs, …). On the
    // git path these are tracked and so survive discovery; on the glob path
    // SKIP_DIRS/.gitignore only cover directories. Classifying by the
    // workspace-RELATIVE path (never the absolute prefix) mirrors Claude Code's
    // Linguist-style classifier and keeps machine-generated symbols out of both
    // the index and the summary injected into the system prompt.
    filePaths = filePaths.filter(fp => !isGeneratedFile(path.relative(this.cwd, fp)));

    const discoveryMs = Date.now() - discoveryStart;

    const maxFilesCapped = filePaths.length > MAX_FILES;
    if (maxFilesCapped) {
      filePaths = filePaths.slice(0, MAX_FILES);
    }

    // Signature of the discovered set. Prefer the cheap `.git/index` mtime for
    // the git path; fall back to a content hash of the path list (glob path, or
    // a worktree whose `.git` is a file so no index mtime is available).
    const signature = discovery === 'git' && idxMtime !== null
      ? `git:${idxMtime}`
      : `${discovery}-list:${fileListSignature(filePaths)}`;

    // Post-discovery fast path: a byte-identical file set since the last build
    // would reproduce the same index, so skip the re-parse.
    if (this.files.size > 0 && this.lastIndexSignature === signature) {
      return {
        fileCount: this.files.size,
        symbolCount: this.getStats().symbolCount,
        duration: Date.now() - start,
        discovery,
        cached: true,
        maxFilesCapped,
        stages: { discoveryMs, parseMs: 0 },
      };
    }

    const parseStart = Date.now();
    let symbolCount = 0;
    for (const fp of filePaths) {
      const entry = await this.indexFile(fp);
      if (entry) {
        symbolCount += entry.symbols.length;
      }
    }
    const parseMs = Date.now() - parseStart;

    this.lastIndexSignature = signature;

    return {
      fileCount: this.files.size,
      symbolCount,
      duration: Date.now() - start,
      discovery,
      cached: false,
      maxFilesCapped,
      stages: { discoveryMs, parseMs },
    };
  }

  // -------------------------------------------------------------------------
  // Index a single file (incremental)
  // -------------------------------------------------------------------------

  async indexFile(filePath: string): Promise<FileEntry | null> {
    // Remove old data first (for re-index)
    if (this.files.has(filePath)) {
      this.removeFile(filePath);
    }

    const ext = path.extname(filePath).toLowerCase();
    const language = EXT_TO_LANG[ext];
    if (!language) return null;

    let stat: fs.Stats;
    try {
      stat = await fsp.stat(filePath);
    } catch {
      return null;
    }

    if (!stat.isFile() || stat.size > MAX_FILE_SIZE || stat.size === 0) return null;

    let content: string;
    try {
      content = await fsp.readFile(filePath, 'utf-8');
    } catch {
      return null;
    }

    // Check for binary content (null bytes in first 8KB)
    const probe = content.slice(0, 8192);
    if (probe.includes('\0')) return null;

    const relativePath = path.relative(this.cwd, filePath);
    const symbols = this.extractSymbols(content, filePath, language);
    const imports = await this.extractImports(content, filePath, language);

    const entry: FileEntry = {
      path: filePath,
      relativePath,
      language,
      size: stat.size,
      lastModified: stat.mtimeMs,
      symbols,
      imports,
    };

    // Store file entry
    this.files.set(filePath, entry);

    // Update symbol index
    for (const sym of symbols) {
      const existing = this.symbolIndex.get(sym.name) || [];
      existing.push(sym);
      this.symbolIndex.set(sym.name, existing);
    }

    // Update import graph
    const importSet = new Set(imports);
    this.importGraph.set(filePath, importSet);

    for (const imp of imports) {
      if (!this.reverseImports.has(imp)) {
        this.reverseImports.set(imp, new Set());
      }
      this.reverseImports.get(imp)!.add(filePath);
    }

    return entry;
  }

  // -------------------------------------------------------------------------
  // Remove a file from the index
  // -------------------------------------------------------------------------

  removeFile(filePath: string): void {
    const entry = this.files.get(filePath);
    if (!entry) return;

    // Remove from symbol index
    for (const sym of entry.symbols) {
      const list = this.symbolIndex.get(sym.name);
      if (list) {
        const filtered = list.filter(s => s.filePath !== filePath);
        if (filtered.length === 0) {
          this.symbolIndex.delete(sym.name);
        } else {
          this.symbolIndex.set(sym.name, filtered);
        }
      }
    }

    // Remove from import graph
    const imports = this.importGraph.get(filePath);
    if (imports) {
      for (const imp of imports) {
        const rev = this.reverseImports.get(imp);
        if (rev) {
          rev.delete(filePath);
          if (rev.size === 0) {
            this.reverseImports.delete(imp);
          }
        }
      }
      this.importGraph.delete(filePath);
    }

    // Remove from reverse imports: clean up entries where other files import this file.
    // Other files' importGraph still references this path (stale edge), but
    // the reverse lookup will no longer return this file as having dependents.
    this.reverseImports.delete(filePath);

    this.files.delete(filePath);
  }

  // -------------------------------------------------------------------------
  // Search symbols by name — fuzzy (subsequence) match with weighted scoring.
  //
  // Scans EVERY indexed symbol (no insertion-order early-break) and keeps the
  // globally highest-scoring `limit` entries, so results are the best matches,
  // not merely the earliest-indexed ones. Scoring mirrors Claude Code's
  // file-index scorer (boundary/camel/consecutive bonuses + gap penalties),
  // layered with exact/prefix tier bonuses (to preserve the historical
  // exact > starts-with > contains ordering) and a small test-path penalty.
  // The incoming query is clamped to MAX_SYMBOL_QUERY_LEN to bound scan cost.
  // -------------------------------------------------------------------------

  searchSymbols(query: string, limit: number = 20): SymbolEntry[] {
    if (limit <= 0) return [];
    // Clamp an oversized query to bound per-symbol scan cost (DoS guard).
    const clamped = query.slice(0, MAX_SYMBOL_QUERY_LEN);
    // Smart case (mirrors Claude Code's file-index): an all-lowercase query
    // matches case-insensitively; any uppercase char in the query makes the
    // match case-sensitive. This lets a precise `WorkspaceIndexer` query avoid
    // lowercase noise while a casual `workspaceindexer` still matches broadly.
    const caseSensitive = clamped !== clamped.toLowerCase();
    const needle = caseSensitive ? clamped : clamped.toLowerCase();
    if (needle.length === 0) return [];

    const scored: { entry: SymbolEntry; score: number }[] = [];
    for (const [name, entries] of this.symbolIndex) {
      const base = fuzzySymbolScore(name, needle, caseSensitive);
      if (base === null) continue;

      const hay = caseSensitive ? name : name.toLowerCase();
      let nameScore = base;
      if (hay === needle) nameScore += BONUS_EXACT;
      else if (hay.startsWith(needle)) nameScore += BONUS_PREFIX;

      for (const entry of entries) {
        let s = nameScore;
        // Match on the workspace-relative path so only in-repo test/spec/fixture
        // locations are penalized (not an incidental 'test' in an absolute prefix).
        if (TEST_PATH_RE.test(path.relative(this.cwd, entry.filePath))) {
          s -= TEST_PATH_PENALTY;
        }
        scored.push({ entry, score: s });
      }
    }

    scored.sort(
      (a, b) =>
        b.score - a.score ||
        a.entry.name.localeCompare(b.entry.name) ||
        a.entry.filePath.localeCompare(b.entry.filePath),
    );

    return scored.slice(0, limit).map(x => x.entry);
  }

  // -------------------------------------------------------------------------
  // Find definition(s) of a symbol
  // -------------------------------------------------------------------------

  findDefinition(symbolName: string): SymbolEntry[] {
    return (this.symbolIndex.get(symbolName) || []).filter(
      s => s.type !== 'import' && s.type !== 'export',
    );
  }

  // -------------------------------------------------------------------------
  // Import graph queries
  // -------------------------------------------------------------------------

  /** Find all files that import a given file */
  findDependents(filePath: string): string[] {
    const abs = path.isAbsolute(filePath) ? filePath : path.resolve(this.cwd, filePath);
    const rev = this.reverseImports.get(abs);
    return rev ? Array.from(rev) : [];
  }

  /** Find all files that a given file imports */
  findDependencies(filePath: string): string[] {
    const abs = path.isAbsolute(filePath) ? filePath : path.resolve(this.cwd, filePath);
    const deps = this.importGraph.get(abs);
    return deps ? Array.from(deps) : [];
  }

  // -------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------

  getFile(filePath: string): FileEntry | null {
    return this.files.get(filePath) || null;
  }

  getFiles(): FileEntry[] {
    return Array.from(this.files.values());
  }

  getStats(): { fileCount: number; symbolCount: number; importEdges: number } {
    let symbolCount = 0;
    for (const entries of this.symbolIndex.values()) {
      symbolCount += entries.length;
    }
    let importEdges = 0;
    for (const deps of this.importGraph.values()) {
      importEdges += deps.size;
    }
    return { fileCount: this.files.size, symbolCount, importEdges };
  }

  // -------------------------------------------------------------------------
  // Summary for system prompt (~500 chars)
  // -------------------------------------------------------------------------

  getSummary(): string {
    const stats = this.getStats();
    if (stats.fileCount === 0) {
      return 'Workspace index: empty (no indexable files found).';
    }

    const parts: string[] = [];
    // Staleness caveat first so the model never over-trusts the counts/key
    // symbols below once it starts editing.
    parts.push(SUMMARY_SNAPSHOT_CAVEAT);
    parts.push(`Workspace: ${stats.fileCount} files, ${stats.symbolCount} symbols, ${stats.importEdges} import edges`);

    // Key symbols — a lightweight repo map: rank classes/functions by how
    // connected their defining file is (reverse-import dependents), so the most
    // depended-on code surfaces first instead of an arbitrary Map-order slice.
    // Ties break alphabetically for a deterministic, cache-stable summary.
    const fileRank = (fp: string): number => this.reverseImports.get(fp)?.size ?? 0;
    const candidates: Array<{ name: string; type: string; filePath: string; line: number; rank: number }> = [];
    for (const [name, entries] of this.symbolIndex) {
      const e = entries[0];
      if (e && (e.type === 'class' || e.type === 'function')) {
        candidates.push({ name, type: e.type, filePath: e.filePath, line: e.line, rank: fileRank(e.filePath) });
      }
    }
    candidates.sort((a, b) => b.rank - a.rank || a.name.localeCompare(b.name));
    const keySymbols = candidates.slice(0, 5).map(
      c => `${c.name} (${c.type}, ${path.relative(this.cwd, c.filePath)}:${c.line})`,
    );
    if (keySymbols.length > 0) {
      parts.push(`Key symbols: ${keySymbols.join(', ')}`);
    }

    // Top files by dependents
    const depCounts: Array<{ relPath: string; count: number }> = [];
    for (const [fp, dependents] of this.reverseImports) {
      if (dependents.size > 0) {
        depCounts.push({ relPath: path.relative(this.cwd, fp), count: dependents.size });
      }
    }
    depCounts.sort((a, b) => b.count - a.count);
    const topFiles = depCounts.slice(0, 3).map(d => `${d.relPath} (${d.count} dependents)`);
    if (topFiles.length > 0) {
      parts.push(`Top files by connections: ${topFiles.join(', ')}`);
    }

    const summary = parts.join('\n');
    // Truncate to the budget, but tell the model the summary was clipped AND how
    // to recover the dropped detail (rather than a silent '...' slice).
    if (summary.length > SUMMARY_MAX_CHARS) {
      const keep = SUMMARY_MAX_CHARS - SUMMARY_TRUNCATION_HINT.length;
      return summary.slice(0, keep) + SUMMARY_TRUNCATION_HINT;
    }
    return summary;
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  private extractSymbols(content: string, filePath: string, language: string): SymbolEntry[] {
    const patterns = langPatterns()[language];
    if (!patterns) return [];

    const symbols: SymbolEntry[] = [];
    for (const pat of patterns) {
      if (pat.isImport) continue; // imports handled separately

      // Reset lastIndex for each use
      const regex = new RegExp(pat.regex.source, pat.regex.flags);
      let match: RegExpExecArray | null;

      while ((match = regex.exec(content)) !== null) {
        const name = match[1];
        if (!name) continue;

        // Calculate line number (1-based)
        const lineNum = content.slice(0, match.index).split('\n').length;

        symbols.push({
          name,
          type: pat.type,
          filePath,
          line: lineNum,
          language,
        });
      }
    }

    return symbols;
  }

  private async extractImports(content: string, filePath: string, language: string): Promise<string[]> {
    const patterns = langPatterns()[language];
    if (!patterns) return [];

    const importPaths: string[] = [];

    for (const pat of patterns) {
      if (!pat.isImport) continue;

      const regex = new RegExp(pat.regex.source, pat.regex.flags);
      let match: RegExpExecArray | null;

      while ((match = regex.exec(content)) !== null) {
        // Some patterns have multiple capture groups (Python)
        const raw = match[1] || match[2];
        if (!raw) continue;

        const resolved = await this.resolveImport(raw, filePath, language);
        if (resolved) {
          importPaths.push(resolved);
        }
      }
    }

    return importPaths;
  }

  private async resolveImport(raw: string, fromFile: string, language: string): Promise<string | null> {
    const dir = path.dirname(fromFile);

    if (language === 'typescript') {
      // Relative imports only
      if (!raw.startsWith('.')) return null;
      const candidates = [
        path.resolve(dir, raw + '.ts'),
        path.resolve(dir, raw + '.tsx'),
        path.resolve(dir, raw + '.js'),
        path.resolve(dir, raw + '.jsx'),
        path.resolve(dir, raw, 'index.ts'),
        path.resolve(dir, raw, 'index.tsx'),
        path.resolve(dir, raw, 'index.js'),
      ];
      for (const c of candidates) {
        try {
          const s = await fsp.stat(c);
          if (s.isFile()) return c;
        } catch { /* not found */ }
      }
      return null;
    }

    if (language === 'python') {
      // Relative-ish: try to resolve as a file in the workspace
      const asFile = path.resolve(this.cwd, raw.replace(/\./g, path.sep) + '.py');
      const asPackage = path.resolve(this.cwd, raw.replace(/\./g, path.sep), '__init__.py');
      try { const s = await fsp.stat(asFile); if (s.isFile()) return asFile; } catch { /* */ }
      try { const s = await fsp.stat(asPackage); if (s.isFile()) return asPackage; } catch { /* */ }
      return null;
    }

    // Go / Rust / Java — store raw module path, no resolution
    return null;
  }
}
