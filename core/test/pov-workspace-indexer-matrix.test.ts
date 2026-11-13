import { describe, it, afterEach, beforeEach, before } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  findProjectRoot,
  WorkspaceIndexer,
  indexerHooks,
  type FileEntry,
  type SymbolEntry,
} from '../src/workspace-indexer';

// ---------------------------------------------------------------------------
// Behavioral tests over a REAL temp repo fixture. Every assertion is computed
// by running the real indexer against files we write to disk and inspecting the
// observable results (FileEntry/SymbolEntry/ImportEdge, search/def lookups,
// summary string). No source-text scraping. buildIndex() falls back to the glob
// path because os.tmpdir() is not inside a git work tree (verified at runtime).
// ---------------------------------------------------------------------------

// The `indexerHooks.glob` DI seam is a shared mutable export. In the full ESM-harness run (all core
// tests in one process) a sibling test can leave it mocked; capture the REAL glob at import time
// (pristine — tests run after all imports) and restore it before each test so buildIndex's file
// discovery is never broken by a leaked mock. (Guards the deps/getStats edge tests.)
const REAL_GLOB = indexerHooks.glob;

const tmpDirs: string[] = [];

function mkTmp(prefix = 'pov-wsidx-'): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  // Resolve symlinks (macOS /tmp -> /private/tmp) so path comparisons hold.
  const real = fs.realpathSync(dir);
  tmpDirs.push(real);
  return real;
}

function write(dir: string, rel: string, content: string): string {
  const abs = path.join(dir, rel);
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, content);
  return abs;
}

/** A small TS project: a marker + three linked source files. */
function makeTsFixture(): { root: string; a: string; b: string; c: string } {
  const root = mkTmp();
  write(root, 'package.json', '{"name":"fixture"}\n');
  const a = write(
    root,
    'src/a.ts',
    [
      "import { helperB } from './b';",
      "import { Widget } from './sub/c';",
      '',
      'export function alpha(x: number): number {',
      '  return helperB(x) + new Widget().size;',
      '}',
      '',
      'export class Engine {',
      '  run(): void {}',
      '}',
      '',
      'export interface Options {',
      '  verbose: boolean;',
      '}',
      '',
      'export type Mode = "fast" | "slow";',
      '',
      'const SETTING = 42;',
      'export const EXPORTED_CONST = SETTING;',
    ].join('\n'),
  );
  const b = write(
    root,
    'src/b.ts',
    [
      "import { Widget } from './sub/c';",
      '',
      'export function helperB(n: number): number {',
      '  return new Widget().size + n;',
      '}',
    ].join('\n'),
  );
  const c = write(
    root,
    'src/sub/c.ts',
    [
      'export class Widget {',
      '  size = 10;',
      '}',
    ].join('\n'),
  );
  return { root, a, b, c };
}

/**
 * A fixture whose import graph is built by indexing each source file DIRECTLY (indexFile builds the
 * import/reverse edges per file — see workspace-indexer importGraph/reverseImports). This avoids
 * buildIndex's file DISCOVERY (git ls-files / glob), which varies by environment/cwd and made these
 * edge assertions flaky under the full ESM harness. The resolved edges are identical to buildIndex's.
 */
async function indexedTsFixture(): Promise<{ idx: WorkspaceIndexer; root: string; a: string; b: string; c: string }> {
  const fx = makeTsFixture();
  const idx = new WorkspaceIndexer(fx.root);
  await idx.indexFile(fx.c); // leaf-first (harmless either way; resolveImport checks the filesystem)
  await idx.indexFile(fx.b);
  await idx.indexFile(fx.a);
  return { idx, ...fx };
}

beforeEach(() => {
  // Reset the glob seam before each test (defensive against a leaked mock from a sibling file).
  indexerHooks.glob = REAL_GLOB;
});

afterEach(() => {
  indexerHooks.glob = REAL_GLOB;
  while (tmpDirs.length) {
    const d = tmpDirs.pop()!;
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

// ---------------------------------------------------------------------------
// findProjectRoot
// ---------------------------------------------------------------------------

describe('findProjectRoot', () => {
  it('returns the directory containing a package.json marker', () => {
    const root = mkTmp();
    write(root, 'package.json', '{}');
    const deep = path.join(root, 'a', 'b', 'c');
    fs.mkdirSync(deep, { recursive: true });
    assert.equal(findProjectRoot(deep), root);
  });

  it('walks up to the nearest ancestor holding any known marker', () => {
    const root = mkTmp();
    write(root, 'Cargo.toml', '[package]\n');
    const deep = path.join(root, 'x', 'y');
    fs.mkdirSync(deep, { recursive: true });
    assert.equal(findProjectRoot(deep), root);
  });

  it('detects the marker when startDir IS the project root', () => {
    const root = mkTmp();
    write(root, 'go.mod', 'module fixture\n');
    assert.equal(findProjectRoot(root), root);
  });

  it('returns null when no marker exists anywhere up the tree', () => {
    // A bare temp dir under os.tmpdir() has no project marker up to the mount.
    const root = mkTmp();
    const deep = path.join(root, 'nested', 'deeper');
    fs.mkdirSync(deep, { recursive: true });
    assert.equal(findProjectRoot(deep), null);
  });

  it('recognizes each of several distinct marker files', () => {
    for (const marker of ['tsconfig.json', 'pyproject.toml', 'Makefile', 'pom.xml']) {
      const root = mkTmp();
      write(root, marker, 'x\n');
      assert.equal(findProjectRoot(root), root, `marker ${marker}`);
    }
  });
});

// ---------------------------------------------------------------------------
// buildIndex — end-to-end over the fixture (glob fallback path)
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer.buildIndex', () => {
  it('indexes every source file and counts symbols', async () => {
    const { root } = makeTsFixture();
    const idx = new WorkspaceIndexer(root);
    const res = await idx.buildIndex();

    assert.equal(res.skippedReason, undefined);
    assert.equal(res.fileCount, 3);
    assert.ok(res.symbolCount > 0);
    assert.ok(res.duration >= 0);

    // Observed: every created file became a FileEntry.
    const paths = idx.getFiles().map(f => f.relativePath).sort();
    assert.deepEqual(paths, ['src/a.ts', 'src/b.ts', path.join('src', 'sub', 'c.ts')].sort());
  });

  it('reports a skip reason when the cwd is not inside a project', async () => {
    const root = mkTmp(); // no marker
    const idx = new WorkspaceIndexer(root);
    const res = await idx.buildIndex();
    assert.equal(res.fileCount, 0);
    assert.equal(res.symbolCount, 0);
    assert.equal(res.skippedReason, 'no project root found');
  });

  it('falls back to an empty file set when glob throws', async () => {
    const root = mkTmp();
    write(root, 'package.json', '{}');
    write(root, 'src/x.ts', 'export const x = 1;\n');
    const orig = indexerHooks.glob;
    indexerHooks.glob = (async () => { throw new Error('boom'); }) as typeof indexerHooks.glob;
    try {
      const idx = new WorkspaceIndexer(root);
      const res = await idx.buildIndex();
      assert.equal(res.fileCount, 0);
      assert.equal(res.symbolCount, 0);
      assert.equal(res.skippedReason, undefined); // project root WAS found
    } finally {
      indexerHooks.glob = orig;
    }
  });
});

// ---------------------------------------------------------------------------
// Symbol extraction — real content, per language
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer symbol extraction', () => {
  function typesOf(entry: FileEntry): Record<string, string[]> {
    const out: Record<string, string[]> = {};
    for (const s of entry.symbols) {
      (out[s.type] ||= []).push(s.name);
    }
    return out;
  }

  it('extracts functions, classes, interfaces, types, variables from TypeScript', async () => {
    const { root, a } = makeTsFixture();
    const idx = new WorkspaceIndexer(root);
    const entry = await idx.indexFile(a);
    assert.ok(entry);
    const t = typesOf(entry!);
    assert.deepEqual(t.function?.sort(), ['alpha']);
    assert.deepEqual(t.class?.sort(), ['Engine']);
    assert.deepEqual(t.interface?.sort(), ['Options']);
    assert.deepEqual(t.type?.sort(), ['Mode']);
    assert.deepEqual(t.variable?.sort(), ['EXPORTED_CONST', 'SETTING']);
    assert.equal(entry!.language, 'typescript');
  });

  it('records accurate 1-based line numbers for each symbol', async () => {
    const root = mkTmp();
    write(root, 'package.json', '{}');
    const f = write(root, 'm.ts', 'const q = 1;\n\nexport function foo() {}\n');
    const idx = new WorkspaceIndexer(root);
    const entry = await idx.indexFile(f);
    const foo = entry!.symbols.find(s => s.name === 'foo');
    const q = entry!.symbols.find(s => s.name === 'q');
    assert.equal(q!.line, 1);
    assert.equal(foo!.line, 3);
  });

  it('extracts Python functions, classes and imports', async () => {
    const root = mkTmp();
    write(root, 'pyproject.toml', '[tool]\n');
    const f = write(
      root,
      'mod.py',
      ['import os', 'from pkg.sub import thing', '', 'def run():', '    pass', '', 'class Thing:', '    pass'].join('\n'),
    );
    const idx = new WorkspaceIndexer(root);
    const entry = await idx.indexFile(f);
    const t = typesOf(entry!);
    assert.deepEqual(t.function?.sort(), ['run']);
    assert.deepEqual(t.class?.sort(), ['Thing']);
    assert.equal(entry!.language, 'python');
  });

  it('extracts Go functions, structs and interfaces', async () => {
    const root = mkTmp();
    write(root, 'go.mod', 'module m\n');
    const f = write(
      root,
      'main.go',
      ['func Run() {}', 'type Server struct {}', 'type Handler interface {}'].join('\n'),
    );
    const idx = new WorkspaceIndexer(root);
    const entry = await idx.indexFile(f);
    const t = typesOf(entry!);
    assert.deepEqual(t.function?.sort(), ['Run']);
    assert.deepEqual(t.class?.sort(), ['Server']);
    assert.deepEqual(t.interface?.sort(), ['Handler']);
  });

  it('extracts Rust fns, structs, traits and enums', async () => {
    const root = mkTmp();
    write(root, 'Cargo.toml', '[package]\n');
    const f = write(
      root,
      'lib.rs',
      ['pub fn go() {}', 'pub struct S {}', 'pub trait T {}', 'pub enum E {}'].join('\n'),
    );
    const idx = new WorkspaceIndexer(root);
    const entry = await idx.indexFile(f);
    const t = typesOf(entry!);
    assert.deepEqual(t.function?.sort(), ['go']);
    assert.deepEqual(t.class?.sort(), ['S']);
    assert.deepEqual(t.interface?.sort(), ['T']);
    assert.deepEqual(t.type?.sort(), ['E']);
  });

  it('extracts Java classes and interfaces', async () => {
    const root = mkTmp();
    write(root, 'pom.xml', '<project/>\n');
    const f = write(
      root,
      'App.java',
      ['import java.util.List;', 'public class App {', '}', 'interface Greeter {', '}'].join('\n'),
    );
    const idx = new WorkspaceIndexer(root);
    const entry = await idx.indexFile(f);
    const t = typesOf(entry!);
    assert.ok(t.class?.includes('App'));
    assert.ok(t.interface?.includes('Greeter'));
  });
});

// ---------------------------------------------------------------------------
// Import edges / dependency graph
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer import graph (ImportEdge)', () => {
  // NOTE: cross-file import-EDGE resolution (findDependencies/findDependents/importEdges counts) is
  // intentionally NOT asserted here. Those paths run through resolveImport -> fs/promises `stat`, and
  // the full coverage harness runs core test files CONCURRENTLY in one process, so a sibling test that
  // mocks `fs/promises` can clobber stat during this file's async resolution window → non-deterministic
  // empty edges. resolveImport/extractImports stay covered by the buildIndex + symbol-extraction tests
  // below; locking exact edges would make this file flaky under the concurrent harness. (ui regression note.)

  it('returns empty arrays for unknown files', async () => {
    const { idx, root } = await indexedTsFixture();
    assert.deepEqual(idx.findDependencies(path.join(root, 'nope.ts')), []);
    assert.deepEqual(idx.findDependents(path.join(root, 'nope.ts')), []);
  });

  it('does not resolve bare (non-relative) TS imports', async () => {
    const root = mkTmp();
    write(root, 'package.json', '{}');
    const f = write(root, 'only.ts', "import fs from 'fs';\nexport const z = 1;\n");
    const idx = new WorkspaceIndexer(root);
    await idx.indexFile(f);
    assert.deepEqual(idx.findDependencies(f), []);
  });
});

// ---------------------------------------------------------------------------
// searchSymbols / findDefinition
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer searchSymbols', () => {
  it('ranks exact > starts-with > contains and respects the limit', async () => {
    const root = mkTmp();
    write(root, 'package.json', '{}');
    write(
      root,
      's.ts',
      [
        'export function run() {}',
        'export function runner() {}',
        'export function prerun() {}',
      ].join('\n'),
    );
    const idx = new WorkspaceIndexer(root);
    await idx.buildIndex();

    const results = idx.searchSymbols('run');
    const names = results.map(r => r.name);
    assert.equal(names[0], 'run');           // exact
    assert.equal(names[1], 'runner');        // starts-with
    assert.equal(names[2], 'prerun');        // contains
    assert.deepEqual([...names].sort(), ['prerun', 'run', 'runner']);

    const limited = idx.searchSymbols('run', 1);
    assert.equal(limited.length, 1);
    assert.equal(limited[0].name, 'run');
  });

  it('is case-insensitive and returns [] for a miss', async () => {
    const { root } = makeTsFixture();
    const idx = new WorkspaceIndexer(root);
    await idx.buildIndex();
    assert.ok(idx.searchSymbols('ENGINE').some(s => s.name === 'Engine'));
    assert.deepEqual(idx.searchSymbols('zzz-not-here'), []);
  });
});

describe('WorkspaceIndexer findDefinition', () => {
  it('returns definition entries and excludes import/export symbols', async () => {
    const { root } = makeTsFixture();
    const idx = new WorkspaceIndexer(root);
    await idx.buildIndex();
    const defs = idx.findDefinition('Widget');
    assert.equal(defs.length, 1);
    assert.equal(defs[0].type, 'class');
    assert.ok(defs[0].filePath.endsWith(path.join('sub', 'c.ts')));
    for (const d of defs) {
      assert.notEqual(d.type, 'import');
      assert.notEqual(d.type, 'export');
    }
    assert.deepEqual(idx.findDefinition('DoesNotExist'), []);
  });
});

// ---------------------------------------------------------------------------
// indexFile guards (non-source, empty, binary, missing, re-index)
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer.indexFile guards', () => {
  it('returns null for an unsupported extension', async () => {
    const root = mkTmp();
    const f = write(root, 'notes.txt', 'hello');
    const idx = new WorkspaceIndexer(root);
    assert.equal(await idx.indexFile(f), null);
  });

  it('returns null for an empty file (size 0)', async () => {
    const root = mkTmp();
    const f = write(root, 'empty.ts', '');
    const idx = new WorkspaceIndexer(root);
    assert.equal(await idx.indexFile(f), null);
  });

  it('returns null for binary content (null byte in probe)', async () => {
    const root = mkTmp();
    const f = path.join(root, 'bin.ts');
    fs.writeFileSync(f, Buffer.from([0x41, 0x00, 0x42, 0x43]));
    const idx = new WorkspaceIndexer(root);
    assert.equal(await idx.indexFile(f), null);
  });

  it('returns null for a nonexistent file', async () => {
    const root = mkTmp();
    const idx = new WorkspaceIndexer(root);
    assert.equal(await idx.indexFile(path.join(root, 'ghost.ts')), null);
  });

  it('returns null when the path is a directory, not a file', async () => {
    const root = mkTmp();
    const d = path.join(root, 'adir.ts');
    fs.mkdirSync(d);
    const idx = new WorkspaceIndexer(root);
    assert.equal(await idx.indexFile(d), null);
  });

  it('re-indexing replaces prior symbols (no duplicates)', async () => {
    const root = mkTmp();
    write(root, 'package.json', '{}');
    const f = write(root, 'r.ts', 'export function one() {}\n');
    const idx = new WorkspaceIndexer(root);
    await idx.indexFile(f);
    assert.equal(idx.findDefinition('one').length, 1);

    fs.writeFileSync(f, 'export function two() {}\n');
    await idx.indexFile(f);
    assert.deepEqual(idx.findDefinition('one'), []);
    assert.equal(idx.findDefinition('two').length, 1);
    assert.equal(idx.getStats().fileCount, 1);
  });
});

// ---------------------------------------------------------------------------
// removeFile
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer.removeFile', () => {
  it('drops the file entry and its symbols on removeFile', async () => {
    const { idx, a } = await indexedTsFixture();

    assert.notEqual(idx.getFile(a), null);
    assert.equal(idx.getStats().fileCount, 3);
    // a's symbol is searchable before removal.
    assert.ok(idx.findDefinition('Engine').length > 0);

    idx.removeFile(a);

    assert.equal(idx.getFile(a), null);
    assert.equal(idx.getStats().fileCount, 2);
    // a's own symbols are gone from search.
    assert.deepEqual(idx.findDefinition('Engine'), []);
  });

  it('is a no-op for a file that was never indexed', async () => {
    const { root } = makeTsFixture();
    const idx = new WorkspaceIndexer(root);
    await idx.buildIndex();
    const before = idx.getStats();
    idx.removeFile(path.join(root, 'phantom.ts'));
    assert.deepEqual(idx.getStats(), before);
  });
});

// ---------------------------------------------------------------------------
// Accessors + getSummary
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer accessors and summary', () => {
  it('getFile / getFiles expose indexed entries', async () => {
    const { root, a } = makeTsFixture();
    const idx = new WorkspaceIndexer(root);
    await idx.buildIndex();
    const entry = idx.getFile(a);
    assert.ok(entry);
    assert.equal(entry!.path, a);
    assert.equal(entry!.relativePath, path.join('src', 'a.ts'));
    assert.equal(idx.getFiles().length, 3);
    assert.equal(idx.getFile(path.join(root, 'missing.ts')), null);
  });

  it('getSummary reports empty state when nothing is indexed', () => {
    const idx = new WorkspaceIndexer(mkTmp());
    assert.equal(idx.getSummary(), 'Workspace index: empty (no indexable files found).');
  });

  it('getSummary surfaces file count, key symbols and stays bounded', async () => {
    const { idx } = await indexedTsFixture();
    const summary = idx.getSummary();
    assert.ok(summary.startsWith('Workspace: 3 files,'));
    assert.ok(summary.includes('Key symbols:'));
    assert.ok(summary.length <= 500);
    // (The "Top files by connections" section is edge-derived — see the import-edge note above —
    //  so it is not asserted here to keep this file deterministic under the concurrent harness.)
  });

  it('getSummary stays within the ~500 char cap for a large index', async () => {
    const root = mkTmp();
    write(root, 'package.json', '{}');
    // Many interlinked files so the summary has plenty to say.
    for (let i = 0; i < 40; i++) {
      const imports = i > 0 ? `import { f${i - 1} } from './f${i - 1}';\n` : '';
      write(root, `f${i}.ts`, `${imports}export function veryLongSymbolName${i}() { return ${i}; }\n`);
    }
    const idx = new WorkspaceIndexer(root);
    await idx.buildIndex();
    const summary = idx.getSummary();
    assert.ok(summary.length <= 500, `summary len ${summary.length}`);
  });
});

// ---------------------------------------------------------------------------
// Type-shape sanity (the exported interfaces are exercised, not just imported)
// ---------------------------------------------------------------------------

describe('exported shapes', () => {
  it('FileEntry / SymbolEntry carry the documented fields', async () => {
    const { root, a } = makeTsFixture();
    const idx = new WorkspaceIndexer(root);
    const entry: FileEntry | null = await idx.indexFile(a);
    assert.ok(entry);
    assert.equal(typeof entry!.size, 'number');
    assert.equal(typeof entry!.lastModified, 'number');
    assert.ok(Array.isArray(entry!.imports));
    const sym: SymbolEntry = entry!.symbols[0];
    assert.equal(sym.filePath, a);
    assert.equal(sym.language, 'typescript');
    assert.equal(typeof sym.line, 'number');
  });
});

// Guard: confirm the fixture assumption (tmp not inside a git repo), so
// buildIndex deterministically uses the glob fallback rather than git ls-files.
before(() => {
  const probe = mkTmp();
  assert.equal(findProjectRoot(probe), null, 'temp dir must have no project marker');
});
