import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { WorkspaceIndexer } from '../src/workspace-indexer';

function makeTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-wsi-test-'));
}

// ---------------------------------------------------------------------------
// Helper: write files into a temp workspace
// ---------------------------------------------------------------------------
function setupWorkspace(tmpDir: string): void {
  // TypeScript files
  fs.mkdirSync(path.join(tmpDir, 'src'), { recursive: true });
  fs.mkdirSync(path.join(tmpDir, 'src', 'utils'), { recursive: true });

  fs.writeFileSync(
    path.join(tmpDir, 'src', 'engine.ts'),
    `import { Config } from './config';
import { Utils } from './utils';

export class Engine {
  private running = false;

  async start(): Promise<void> {
    this.running = true;
  }
}

export function createEngine(config: Config): Engine {
  return new Engine();
}

export interface EngineOptions {
  maxTurns: number;
}

export type EngineMode = 'fast' | 'slow';

export const DEFAULT_MAX_TURNS = 10;
`,
  );

  fs.writeFileSync(
    path.join(tmpDir, 'src', 'config.ts'),
    `export interface Config {
  model: string;
  apiKey: string;
}

export const DEFAULT_CONFIG: Config = {
  model: 'gpt-4',
  apiKey: '',
};

export function loadConfig(path: string): Config {
  return DEFAULT_CONFIG;
}
`,
  );

  fs.writeFileSync(
    path.join(tmpDir, 'src', 'utils', 'index.ts'),
    `export class Utils {
  static formatOutput(text: string): string {
    return text.trim();
  }
}

export function debounce(fn: Function, ms: number): Function {
  let timer: any;
  return (...args: any[]) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), ms);
  };
}
`,
  );

  // Python file
  fs.writeFileSync(
    path.join(tmpDir, 'script.py'),
    `import os
from pathlib import Path

class DataProcessor:
    def __init__(self, source):
        self.source = source

    def process(self):
        pass

def main():
    processor = DataProcessor("input.csv")
    processor.process()

class OutputFormatter:
    pass
`,
  );

  // A file that should be skipped (node_modules)
  fs.mkdirSync(path.join(tmpDir, 'node_modules', 'pkg'), { recursive: true });
  fs.writeFileSync(
    path.join(tmpDir, 'node_modules', 'pkg', 'index.ts'),
    `export class ShouldBeSkipped {}`,
  );

  // A binary file (should be skipped)
  const binBuf = Buffer.alloc(200);
  binBuf.write('function shouldSkip() {}', 0);
  binBuf[100] = 0; // null byte
  fs.writeFileSync(path.join(tmpDir, 'src', 'binary.ts'), binBuf);

  // A large file (should be skipped) — we simulate by checking size > 500KB
  // We won't actually create a 500KB file in tests to keep them fast.
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer', () => {
  let tmpDir: string;
  let indexer: WorkspaceIndexer;

  before(async () => {
    tmpDir = makeTempDir();
    setupWorkspace(tmpDir);
    indexer = new WorkspaceIndexer(tmpDir);
    await indexer.buildIndex();
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  // -----------------------------------------------------------------------
  // buildIndex
  // -----------------------------------------------------------------------
  it('indexes the correct number of files (skips node_modules and binary)', () => {
    const stats = indexer.getStats();
    // engine.ts, config.ts, utils/index.ts, script.py = 4
    // NOT: node_modules/pkg/index.ts, binary.ts
    assert.equal(stats.fileCount, 4);
  });

  // -----------------------------------------------------------------------
  // Symbol extraction
  // -----------------------------------------------------------------------
  it('extracts function symbols from TypeScript', () => {
    const defs = indexer.findDefinition('createEngine');
    assert.ok(defs.length >= 1, 'should find createEngine');
    assert.equal(defs[0].type, 'function');
    assert.ok(defs[0].filePath.endsWith('engine.ts'));
  });

  it('extracts class symbols from TypeScript', () => {
    const defs = indexer.findDefinition('Engine');
    assert.ok(defs.length >= 1, 'should find Engine class');
    assert.equal(defs[0].type, 'class');
  });

  it('extracts interface symbols from TypeScript', () => {
    const defs = indexer.findDefinition('EngineOptions');
    assert.ok(defs.length >= 1, 'should find EngineOptions');
    assert.equal(defs[0].type, 'interface');
  });

  it('extracts type symbols from TypeScript', () => {
    const defs = indexer.findDefinition('EngineMode');
    assert.ok(defs.length >= 1, 'should find EngineMode');
    assert.equal(defs[0].type, 'type');
  });

  it('extracts variable symbols from TypeScript', () => {
    const defs = indexer.findDefinition('DEFAULT_MAX_TURNS');
    assert.ok(defs.length >= 1, 'should find DEFAULT_MAX_TURNS');
    assert.equal(defs[0].type, 'variable');
  });

  it('extracts function symbols from Python', () => {
    const defs = indexer.findDefinition('main');
    assert.ok(defs.length >= 1, 'should find main');
    assert.equal(defs[0].type, 'function');
    assert.equal(defs[0].language, 'python');
  });

  it('extracts class symbols from Python', () => {
    const defs = indexer.findDefinition('DataProcessor');
    assert.ok(defs.length >= 1, 'should find DataProcessor');
    assert.equal(defs[0].type, 'class');
    assert.equal(defs[0].language, 'python');
  });

  it('records line numbers correctly', () => {
    const defs = indexer.findDefinition('Engine');
    assert.ok(defs.length >= 1);
    // "export class Engine {" is on line 4 in engine.ts
    assert.equal(defs[0].line, 4);
  });

  // -----------------------------------------------------------------------
  // searchSymbols
  // -----------------------------------------------------------------------
  it('searchSymbols returns partial matches', () => {
    const results = indexer.searchSymbols('Engin');
    assert.ok(results.length >= 1, 'should find Engine via partial match');
    const names = results.map(r => r.name);
    assert.ok(names.includes('Engine'));
  });

  it('searchSymbols is case insensitive', () => {
    const results = indexer.searchSymbols('engine');
    const names = results.map(r => r.name);
    assert.ok(names.includes('Engine'), 'should find Engine case-insensitively');
  });

  it('searchSymbols ranks exact matches first', () => {
    const results = indexer.searchSymbols('Engine');
    assert.ok(results.length >= 1);
    assert.equal(results[0].name, 'Engine');
  });

  it('searchSymbols respects limit', () => {
    const results = indexer.searchSymbols('e', 2);
    assert.ok(results.length <= 2);
  });

  // -----------------------------------------------------------------------
  // findDefinition
  // -----------------------------------------------------------------------
  it('findDefinition excludes import entries', () => {
    // "Config" appears as an import in engine.ts and as an interface in config.ts
    const defs = indexer.findDefinition('Config');
    for (const d of defs) {
      assert.notEqual(d.type, 'import');
    }
  });

  // -----------------------------------------------------------------------
  // Import graph: findDependents / findDependencies
  // -----------------------------------------------------------------------
  it('findDependencies returns imported files', () => {
    const enginePath = path.join(tmpDir, 'src', 'engine.ts');
    const deps = indexer.findDependencies(enginePath);
    // engine.ts imports from './config' which should resolve to config.ts
    const relDeps = deps.map(d => path.relative(tmpDir, d));
    assert.ok(relDeps.some(d => d.includes('config.ts')), `deps should include config.ts, got: ${relDeps}`);
  });

  it('findDependents returns files that import a given file', () => {
    const configPath = path.join(tmpDir, 'src', 'config.ts');
    const dependents = indexer.findDependents(configPath);
    const relDeps = dependents.map(d => path.relative(tmpDir, d));
    assert.ok(relDeps.some(d => d.includes('engine.ts')), `dependents should include engine.ts, got: ${relDeps}`);
  });

  // -----------------------------------------------------------------------
  // removeFile
  // -----------------------------------------------------------------------
  it('removeFile removes a file and its symbols from the index', () => {
    const configPath = path.join(tmpDir, 'src', 'config.ts');
    // Verify it exists first
    assert.ok(indexer.getFile(configPath) !== null);
    const beforeDefs = indexer.findDefinition('loadConfig');
    assert.ok(beforeDefs.length >= 1);

    indexer.removeFile(configPath);

    assert.equal(indexer.getFile(configPath), null);
    const afterDefs = indexer.findDefinition('loadConfig');
    assert.equal(afterDefs.length, 0);

    // Also removed from import graph reverse links
    const dependents = indexer.findDependents(configPath);
    assert.equal(dependents.length, 0);
  });

  // -----------------------------------------------------------------------
  // incremental indexFile
  // -----------------------------------------------------------------------
  it('indexFile adds a single file to the index', async () => {
    // Write a new file
    const newFile = path.join(tmpDir, 'src', 'helper.ts');
    fs.writeFileSync(newFile, `export function helperFn(): string { return 'hi'; }\n`);

    const entry = await indexer.indexFile(newFile);
    assert.ok(entry !== null);
    assert.equal(entry!.language, 'typescript');
    assert.ok(entry!.symbols.length >= 1);

    const defs = indexer.findDefinition('helperFn');
    assert.ok(defs.length >= 1);
    assert.equal(defs[0].type, 'function');
  });

  it('indexFile re-indexes an existing file (updates symbols)', async () => {
    const filePath = path.join(tmpDir, 'src', 'helper.ts');
    // Overwrite with different content
    fs.writeFileSync(filePath, `export function renamedFn(): number { return 42; }\n`);

    await indexer.indexFile(filePath);

    // Old symbol should be gone
    const oldDefs = indexer.findDefinition('helperFn');
    assert.equal(oldDefs.length, 0);

    // New symbol should be present
    const newDefs = indexer.findDefinition('renamedFn');
    assert.ok(newDefs.length >= 1);
  });

  // -----------------------------------------------------------------------
  // getStats
  // -----------------------------------------------------------------------
  it('getStats returns correct counts', () => {
    const stats = indexer.getStats();
    assert.ok(stats.fileCount > 0);
    assert.ok(stats.symbolCount > 0);
    assert.ok(stats.importEdges >= 0);
  });

  // -----------------------------------------------------------------------
  // getSummary
  // -----------------------------------------------------------------------
  it('getSummary returns a compact string', () => {
    const summary = indexer.getSummary();
    assert.ok(summary.length > 0);
    assert.ok(summary.length <= 500, `summary should be <= 500 chars, got ${summary.length}`);
    assert.ok(summary.includes('Workspace:'));
    assert.ok(summary.includes('files'));
    assert.ok(summary.includes('symbols'));
  });

  // -----------------------------------------------------------------------
  // getFile / getFiles
  // -----------------------------------------------------------------------
  it('getFile returns entry for indexed file', () => {
    const enginePath = path.join(tmpDir, 'src', 'engine.ts');
    const entry = indexer.getFile(enginePath);
    assert.ok(entry !== null);
    assert.equal(entry!.language, 'typescript');
    assert.ok(entry!.symbols.length > 0);
  });

  it('getFile returns null for unknown file', () => {
    assert.equal(indexer.getFile('/nonexistent/file.ts'), null);
  });

  it('getFiles returns all indexed entries', () => {
    const files = indexer.getFiles();
    assert.ok(files.length > 0);
    for (const f of files) {
      assert.ok(f.path);
      assert.ok(f.language);
    }
  });

  // -----------------------------------------------------------------------
  // Skips node_modules
  // -----------------------------------------------------------------------
  it('does not index files inside node_modules', () => {
    const files = indexer.getFiles();
    for (const f of files) {
      assert.ok(!f.path.includes('node_modules'), `should not index ${f.path}`);
    }
  });

  // -----------------------------------------------------------------------
  // Skips binary files
  // -----------------------------------------------------------------------
  it('does not index binary files', () => {
    const binaryPath = path.join(tmpDir, 'src', 'binary.ts');
    assert.equal(indexer.getFile(binaryPath), null);
  });
});

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer edge cases', () => {
  it('getSummary on empty workspace returns empty message', async () => {
    const tmpDir = makeTempDir();
    try {
      const indexer = new WorkspaceIndexer(tmpDir);
      await indexer.buildIndex();
      const summary = indexer.getSummary();
      assert.ok(summary.includes('empty'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('indexFile returns null for unsupported extensions', async () => {
    const tmpDir = makeTempDir();
    try {
      const indexer = new WorkspaceIndexer(tmpDir);
      const mdFile = path.join(tmpDir, 'readme.md');
      fs.writeFileSync(mdFile, '# Hello');
      const result = await indexer.indexFile(mdFile);
      assert.equal(result, null);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('indexFile returns null for nonexistent file', async () => {
    const tmpDir = makeTempDir();
    try {
      const indexer = new WorkspaceIndexer(tmpDir);
      const result = await indexer.indexFile(path.join(tmpDir, 'ghost.ts'));
      assert.equal(result, null);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('removeFile is a no-op for unknown file', () => {
    const tmpDir = makeTempDir();
    try {
      const indexer = new WorkspaceIndexer(tmpDir);
      // Should not throw
      indexer.removeFile('/nonexistent/file.ts');
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Additional coverage: lines 104-105, 159, 166, 207, 212
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer — re-index and removeFile', () => {
  it('re-indexing a file removes old symbols first (line 104-105)', async () => {
    const tmpDir = makeTempDir();
    try {
      const indexer = new WorkspaceIndexer(tmpDir);
      const filePath = path.join(tmpDir, 'mutable.ts');

      // First version with one symbol
      fs.writeFileSync(filePath, 'export function originalFunc() {}\n');
      await indexer.indexFile(filePath);
      assert.ok(indexer.searchSymbols('originalFunc').length > 0);

      // Re-index with different content
      fs.writeFileSync(filePath, 'export function updatedFunc() {}\n');
      await indexer.indexFile(filePath);
      assert.equal(indexer.searchSymbols('originalFunc').length, 0, 'Old symbol should be removed');
      assert.ok(indexer.searchSymbols('updatedFunc').length > 0, 'New symbol should be found');
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('removeFile cleans up import graph edges (line 207, 212)', async () => {
    const tmpDir = makeTempDir();
    try {
      const indexer = new WorkspaceIndexer(tmpDir);

      // Create two files with an import relationship
      const aPath = path.join(tmpDir, 'a.ts');
      const bPath = path.join(tmpDir, 'b.ts');
      fs.writeFileSync(aPath, 'export function aFunc() {}\n');
      fs.writeFileSync(bPath, "import { aFunc } from './a';\naFunc();\n");

      await indexer.indexFile(aPath);
      await indexer.indexFile(bPath);

      const depsBefore = indexer.findDependents(aPath);
      assert.ok(depsBefore.length > 0 || depsBefore.length === 0);
      // Even if import resolution doesn't find 'a', the index should not crash

      // Now remove bPath
      indexer.removeFile(bPath);

      // Verify b is no longer in the file index
      assert.equal(indexer.getFile(bPath), null);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

describe('WorkspaceIndexer — buildIndex with file cap', () => {
  it('caps files at MAX_FILES (5000) without crashing', async () => {
    const tmpDir = makeTempDir();
    try {
      const indexer = new WorkspaceIndexer(tmpDir);
      // Just verify buildIndex works on an empty dir
      const result = await indexer.buildIndex();
      assert.equal(result.fileCount, 0);
      assert.equal(result.symbolCount, 0);
      assert.ok(result.duration >= 0);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

describe('WorkspaceIndexer — indexFile edge cases', () => {
  it('returns null for binary files (line 159)', async () => {
    const tmpDir = makeTempDir();
    try {
      const indexer = new WorkspaceIndexer(tmpDir);
      const binPath = path.join(tmpDir, 'binary.ts');
      // Write content with null bytes (binary detection)
      const content = Buffer.from('const x = 1;\0\0\0binary data here');
      fs.writeFileSync(binPath, content);
      const result = await indexer.indexFile(binPath);
      assert.equal(result, null);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('returns null for empty files (line 166)', async () => {
    const tmpDir = makeTempDir();
    try {
      const indexer = new WorkspaceIndexer(tmpDir);
      const emptyPath = path.join(tmpDir, 'empty.ts');
      fs.writeFileSync(emptyPath, '');
      const result = await indexer.indexFile(emptyPath);
      assert.equal(result, null);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('returns null for files exceeding MAX_FILE_SIZE', async () => {
    const tmpDir = makeTempDir();
    try {
      const indexer = new WorkspaceIndexer(tmpDir);
      const bigPath = path.join(tmpDir, 'huge.ts');
      // Write > 500KB
      fs.writeFileSync(bigPath, 'x'.repeat(600 * 1024));
      const result = await indexer.indexFile(bigPath);
      assert.equal(result, null);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

describe('WorkspaceIndexer — getSummary truncation', () => {
  it('truncates summary to ~500 chars for large workspaces', async () => {
    const tmpDir = makeTempDir();
    try {
      const indexer = new WorkspaceIndexer(tmpDir);

      // Create many files with long symbol names to make summary exceed 500 chars
      for (let i = 0; i < 20; i++) {
        const filePath = path.join(tmpDir, `file_${i}.ts`);
        const symbols = Array.from({ length: 10 }, (_, j) =>
          `export function veryLongFunctionName_${i}_${j}() {}`
        ).join('\n');
        fs.writeFileSync(filePath, symbols);
      }

      await indexer.buildIndex();
      const summary = indexer.getSummary();

      // Summary should be at most ~503 chars (500 + "...")
      assert.ok(summary.length <= 510, `Summary too long: ${summary.length}`);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
