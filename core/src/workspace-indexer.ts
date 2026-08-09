/* eslint-disable security/detect-unsafe-regex -- Symbol extraction regexes are applied to controlled file content, not user input */
import * as fs from 'fs';
import * as path from 'path';
import fg from 'fast-glob';

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
  '.nuxt', '.output', '.idea', '.vscode', 'vendor', 'references', '.superinference',
]);

const MAX_FILE_SIZE = 500 * 1024; // 500 KB
const MAX_FILES = 5000; // Cap file count to prevent slow indexing

// ---------------------------------------------------------------------------
// WorkspaceIndexer
// ---------------------------------------------------------------------------

export class WorkspaceIndexer {
  private files: Map<string, FileEntry> = new Map();
  private symbolIndex: Map<string, SymbolEntry[]> = new Map(); // symbol name -> entries
  private importGraph: Map<string, Set<string>> = new Map(); // file -> imported files
  private reverseImports: Map<string, Set<string>> = new Map(); // file -> files that import it

  readonly cwd: string;

  constructor(cwd: string) {
    this.cwd = cwd;
  }

  // -------------------------------------------------------------------------
  // Build the full index
  // -------------------------------------------------------------------------

  async buildIndex(): Promise<{ fileCount: number; symbolCount: number; duration: number }> {
    const start = Date.now();

    const extensions = Object.keys(EXT_TO_LANG).map(e => e.slice(1)); // remove leading dot
    const pattern = `**/*.{${extensions.join(',')}}`;

    const ignorePatterns = Array.from(SKIP_DIRS).map(d => `**/${d}/**`);

    let filePaths: string[];
    try {
      filePaths = await fg(pattern, {
        cwd: this.cwd,
        absolute: true,
        ignore: ignorePatterns,
        dot: false,
        onlyFiles: true,
        followSymbolicLinks: false,
        deep: 10,
      });

      // Cap file count to prevent slow indexing on large repos
      if (filePaths.length > MAX_FILES) {
        filePaths = filePaths.slice(0, MAX_FILES);
      }
    } catch {
      filePaths = [];
    }

    let symbolCount = 0;
    for (const fp of filePaths) {
      const entry = await this.indexFile(fp);
      if (entry) {
        symbolCount += entry.symbols.length;
      }
    }

    const duration = Date.now() - start;
    return {
      fileCount: this.files.size,
      symbolCount,
      duration,
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
      stat = fs.statSync(filePath);
    } catch {
      return null;
    }

    if (!stat.isFile() || stat.size > MAX_FILE_SIZE || stat.size === 0) return null;

    let content: string;
    try {
      content = fs.readFileSync(filePath, 'utf-8');
    } catch {
      return null;
    }

    // Check for binary content (null bytes in first 8KB)
    const probe = content.slice(0, 8192);
    if (probe.includes('\0')) return null;

    const relativePath = path.relative(this.cwd, filePath);
    const symbols = this.extractSymbols(content, filePath, language);
    const imports = this.extractImports(content, filePath, language);

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
  // Search symbols by name (fuzzy-ish: substring match, case-insensitive)
  // -------------------------------------------------------------------------

  searchSymbols(query: string, limit: number = 20): SymbolEntry[] {
    const lowerQuery = query.toLowerCase();
    const results: SymbolEntry[] = [];

    for (const [name, entries] of this.symbolIndex) {
      if (name.toLowerCase().includes(lowerQuery)) {
        results.push(...entries);
      }
      if (results.length >= limit * 3) break; // collect extra for ranking
    }

    // Sort: exact match first, then starts-with, then contains
    results.sort((a, b) => {
      const aLower = a.name.toLowerCase();
      const bLower = b.name.toLowerCase();
      const aExact = aLower === lowerQuery ? 0 : 1;
      const bExact = bLower === lowerQuery ? 0 : 1;
      if (aExact !== bExact) return aExact - bExact;
      const aStarts = aLower.startsWith(lowerQuery) ? 0 : 1;
      const bStarts = bLower.startsWith(lowerQuery) ? 0 : 1;
      if (aStarts !== bStarts) return aStarts - bStarts;
      return a.name.localeCompare(b.name);
    });

    return results.slice(0, limit);
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
    parts.push(`Workspace: ${stats.fileCount} files, ${stats.symbolCount} symbols, ${stats.importEdges} import edges`);

    // Key symbols — pick a few classes and functions
    const keySymbols: string[] = [];
    for (const [name, entries] of this.symbolIndex) {
      if (entries.length > 0 && (entries[0].type === 'class' || entries[0].type === 'function')) {
        const e = entries[0];
        keySymbols.push(`${name} (${e.type}, ${path.relative(this.cwd, e.filePath)}:${e.line})`);
      }
      if (keySymbols.length >= 5) break;
    }
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
    // Truncate to ~500 chars
    if (summary.length > 500) {
      return summary.slice(0, 497) + '...';
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

  private extractImports(content: string, filePath: string, language: string): string[] {
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

        const resolved = this.resolveImport(raw, filePath, language);
        if (resolved) {
          importPaths.push(resolved);
        }
      }
    }

    return importPaths;
  }

  private resolveImport(raw: string, fromFile: string, language: string): string | null {
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
          if (fs.statSync(c).isFile()) return c;
        } catch { /* not found */ }
      }
      return null;
    }

    if (language === 'python') {
      // Relative-ish: try to resolve as a file in the workspace
      const asFile = path.resolve(this.cwd, raw.replace(/\./g, path.sep) + '.py');
      const asPackage = path.resolve(this.cwd, raw.replace(/\./g, path.sep), '__init__.py');
      try { if (fs.statSync(asFile).isFile()) return asFile; } catch { /* */ }
      try { if (fs.statSync(asPackage).isFile()) return asPackage; } catch { /* */ }
      return null;
    }

    // Go / Rust / Java — store raw module path, no resolution
    return null;
  }
}
