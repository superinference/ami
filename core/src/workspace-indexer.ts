/* eslint-disable security/detect-unsafe-regex -- Symbol extraction regexes are applied to controlled file content, not user input */
import * as fs from 'fs';
import * as fsp from 'fs/promises';
import * as path from 'path';
import 'os';
import { glob } from 'tinyglobby';
import { execFileNoThrow } from './utils/shell';
import { isGeneratedFile } from './generated-files';
import { FUZZY_MAX_QUERY_LEN } from './fuzzy-score';
import { FileIndex, type SearchResult } from './file-index';
import { BONUS_EXACT, BONUS_PREFIX, EXT_TO_LANG, GIT_LS_FILES_TIMEOUT_MS, MAX_FILES, SUMMARY_MAX_CHARS, SUMMARY_SNAPSHOT_CAVEAT, SUMMARY_TRUNCATION_HINT, TEST_PATH_PENALTY, TEST_PATH_RE, fileListSignature, findProjectRoot, fuzzySymbolScore, gitIndexMtime, gitignoreToIgnoreGlobs, indexBuildConfig, langPatterns } from './workspace-indexer-gitignore-to-ignore-globs';
import type { BuildIndexResult, FileEntry, SymbolEntry } from './workspace-indexer-gitignore-to-ignore-globs';
export { SUMMARY_MAX_CHARS, SUMMARY_SNAPSHOT_CAVEAT, SUMMARY_TRUNCATION_HINT, findProjectRoot, fuzzySymbolScore, gitignoreToIgnoreGlobs, indexBuildConfig } from './workspace-indexer-gitignore-to-ignore-globs';
export type { BuildIndexResult, FileEntry, ImportEdge, SymbolEntry } from './workspace-indexer-gitignore-to-ignore-globs';

/**
 * File-system hooks for the indexer. Defaults to the real implementations;
 * the `glob` entry is overridable in tests purely as a dependency-injection
 * seam so the glob-failure fallback can be exercised without a pathological
 * filesystem. The `execFileNoThrow` seam routes the `git ls-files` discovery
 * through the shared never-throw file+args wrapper ({@link execFileNoThrow}) so
 * the git-subprocess timeout / failure fallback in getGitTrackedFiles can be
 * exercised deterministically AND a missing/failed `git` never throws. The
 * `yieldToEventLoop` seam exists so the cooperative-yield path in the chunked
 * async build can be observed deterministically in tests. Production behavior is
 * identical to calling the shared `execFileNoThrow` / `tinyglobby.glob` /
 * `setImmediate`.
 */
export const indexerHooks: {
  glob: typeof glob;
  execFileNoThrow: typeof execFileNoThrow;
  yieldToEventLoop: () => Promise<void>;
} = {
  glob,
  execFileNoThrow,
  yieldToEventLoop: () => new Promise<void>(resolve => { setImmediate(resolve); }),
};

/**
 * Query-length cap for fuzzy symbol search. Bounds the per-symbol scan so a
 * pathological / oversized query cannot blow up scoring cost (DoS / resource
 * guard). Mirrors the reference's MAX_QUERY_LEN=64.
 */
export const MAX_SYMBOL_QUERY_LEN = FUZZY_MAX_QUERY_LEN;

const SKIP_DIRS = new Set([
  'node_modules', '.git', 'dist', 'build', '__pycache__', 'venv', '.venv',
  'coverage', '.next', '.cache', '.tox', 'target', '.turbo', '.parcel-cache',
  '.nuxt', '.output', '.idea', '.vscode', 'vendor', 'references', '.ami',
]);

const MAX_FILE_SIZE = 500 * 1024; // 500 KB

async function getGitTrackedFiles(cwd: string): Promise<string[] | null> {
  // Routed through the shared never-throw wrapper: a non-git tree, a missing
  // `git`, or a timeout resolves with a non-zero `code` (never throws), which we
  // treat as "no git listing" and fall back to the glob walk.
  const { stdout, code } = await indexerHooks.execFileNoThrow('git', ['ls-files', '-z'], {
    cwd,
    maxBuffer: 10 * 1024 * 1024,
    timeout: GIT_LS_FILES_TIMEOUT_MS,
  });
  if (code !== 0) return null;
  return stdout.split('\0').filter(Boolean);
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

  /**
   * Build the full index and resolve when the WHOLE index is ready. Thin
   * wrapper over {@link buildIndexAsync}; consumers that don't need progressive
   * queryability await the `done` promise. The underlying build is chunked and
   * cooperatively yields to the event loop (see {@link indexBuildConfig}), so a
   * large repo no longer blocks the main thread for the full parse.
   */
  async buildIndex(): Promise<BuildIndexResult> {
    return this.buildIndexAsync().done;
  }

  /**
   * Progressive, cooperatively-yielding index build. Returns two promises:
   *   - `queryable` resolves as soon as the FIRST chunk is indexed, so
   *     {@link searchSymbols} / {@link getSummary} can serve partial results
   *     against the already-indexed prefix while the build continues (graceful
   *     degradation on a huge repo — mirrors the reference's
   *     `loadFromFileListAsync` { queryable, done } contract);
   *   - `done` resolves with the full {@link BuildIndexResult} when the entire
   *     index is built.
   * `queryable` is always settled (even on a build error or a cached no-op) so
   * a consumer awaiting it never hangs.
   */
  buildIndexAsync(): { queryable: Promise<void>; done: Promise<BuildIndexResult> } {
    let markQueryable: () => void = () => {};
    const queryable = new Promise<void>(resolve => { markQueryable = resolve; });
    // Guarantee `queryable` settles even if the build throws or is a cached
    // no-op that never reaches the first chunk boundary.
    const done = this.runChunkedBuild(markQueryable).finally(() => { markQueryable(); });
    return { queryable, done };
  }

  /**
   * Resolve the discovery/cache plan for a build: find the project root, apply
   * the change-detection fast paths (git-index mtime, then path-list
   * signature), run git/glob discovery, filter generated files and cap to
   * MAX_FILES. Returns a ready-made {@link BuildIndexResult} when the build is a
   * skip or a cache hit, otherwise the file list + metadata to parse. Shared by
   * the sync and chunked build paths so discovery logic lives in one place.
   */
  private async resolveBuildPlan(start: number): Promise<
    | { ready: true; result: BuildIndexResult }
    | {
        ready: false;
        filePaths: string[];
        discovery: 'git' | 'glob';
        discoveryMs: number;
        maxFilesCapped: boolean;
        signature: string;
      }
  > {
    const projectRoot = findProjectRoot(this.cwd);
    if (!projectRoot) {
      return {
        ready: true,
        result: {
          fileCount: 0,
          symbolCount: 0,
          duration: Date.now() - start,
          skippedReason: 'no project root found',
          discovery: 'none',
          cached: false,
          maxFilesCapped: false,
          stages: { discoveryMs: 0, parseMs: 0 },
        },
      };
    }

    // Change-detection fast path: if a prior git-backed build exists and
    // `.git/index` is unchanged, nothing tracked changed — skip spawning
    // `git ls-files` AND the full re-parse.
    const idxMtime = gitIndexMtime(projectRoot);
    if (this.files.size > 0 && idxMtime !== null && this.lastIndexSignature === `git:${idxMtime}`) {
      return {
        ready: true,
        result: {
          fileCount: this.files.size,
          symbolCount: this.getStats().symbolCount,
          duration: Date.now() - start,
          discovery: 'git',
          cached: true,
          maxFilesCapped: false,
          stages: { discoveryMs: 0, parseMs: 0 },
        },
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
    // workspace-RELATIVE path (never the absolute prefix) mirrors the reference's
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
        ready: true,
        result: {
          fileCount: this.files.size,
          symbolCount: this.getStats().symbolCount,
          duration: Date.now() - start,
          discovery,
          cached: true,
          maxFilesCapped,
          stages: { discoveryMs, parseMs: 0 },
        },
      };
    }

    return { ready: false, filePaths, discovery, discoveryMs, maxFilesCapped, signature };
  }

  /**
   * Parse the planned file list, yielding to the event loop on a wall-clock
   * time budget (see {@link indexBuildConfig}) so a large repo stays
   * responsive. `markQueryable` is invoked at the first yield point so a waiting
   * consumer can search the partial index.
   */
  private async runChunkedBuild(markQueryable: () => void): Promise<BuildIndexResult> {
    const start = Date.now();
    const plan = await this.resolveBuildPlan(start);
    if (plan.ready) return plan.result;

    const { chunkMs, chunkCheckInterval } = indexBuildConfig;
    const parseStart = Date.now();
    let symbolCount = 0;
    let chunkStart = performance.now();
    let firstChunk = true;
    for (let i = 0; i < plan.filePaths.length; i++) {
      const entry = await this.indexFile(plan.filePaths[i]!);
      if (entry) {
        symbolCount += entry.symbols.length;
      }
      // Time-based cooperative yield: re-measure only every `chunkCheckInterval`
      // files (amortizing performance.now()), and yield when the wall-clock
      // budget is exceeded. The first yield marks the partial index queryable.
      if ((i + 1) % chunkCheckInterval === 0 && performance.now() - chunkStart > chunkMs) {
        if (firstChunk) {
          markQueryable();
          firstChunk = false;
        }
        await indexerHooks.yieldToEventLoop();
        chunkStart = performance.now();
      }
    }
    const parseMs = Date.now() - parseStart;

    this.lastIndexSignature = plan.signature;

    return {
      fileCount: this.files.size,
      symbolCount,
      duration: Date.now() - start,
      discovery: plan.discovery,
      cached: false,
      maxFilesCapped: plan.maxFilesCapped,
      stages: { discoveryMs: plan.discoveryMs, parseMs },
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
  // not merely the earliest-indexed ones. Scoring mirrors the reference's
  // file-index scorer (boundary/camel/consecutive bonuses + gap penalties),
  // layered with exact/prefix tier bonuses (to preserve the historical
  // exact > starts-with > contains ordering) and a small test-path penalty.
  // The incoming query is clamped to MAX_SYMBOL_QUERY_LEN to bound scan cost.
  // -------------------------------------------------------------------------

  searchSymbols(query: string, limit: number = 20): SymbolEntry[] {
    if (limit <= 0) return [];
    // Clamp an oversized query to bound per-symbol scan cost (DoS guard).
    const clamped = query.slice(0, MAX_SYMBOL_QUERY_LEN);
    // Smart case (mirrors the reference's file-index): an all-lowercase query
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
  // Fuzzy file-PATH search (scalable nucleo/fzf-v2 index)
  //
  // Where searchSymbols ranks symbol NAMES, this ranks whole file PATHS via the
  // typed-array/bitmap/top-k FileIndex (./file-index). The indexer already holds
  // the workspace file set, so a `@`/`/files`-style path finder reuses it rather
  // than re-walking the tree. Built from the live relative paths on each call
  // (the same recompute-per-call contract as searchSymbols), so results never go
  // stale after incremental add/remove.
  // -------------------------------------------------------------------------

  searchFiles(query: string, limit: number = 20): SearchResult[] {
    if (limit <= 0) return [];
    const index = new FileIndex();
    index.loadFromFileList(Array.from(this.files.values(), f => f.relativePath));
    return index.search(query, limit);
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
