/**
 * Standalone BEHAVIOR tests for core/core/src/workspace-indexer.ts targeting the
 * symbol-extraction pipeline (extractSymbols line-number math + the per-symbol
 * `if (!name) continue` guard) and the import-extraction `!raw` guard.
 *
 * CORE ISOLATION: imports only from ../src (public OSS core). No common/cli/vscode.
 */
import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { WorkspaceIndexer } from '../src/workspace-indexer';

const tmpDirs: string[] = [];
function mkProject(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wsi-cov100-'));
  tmpDirs.push(dir);
  // A project marker so findProjectRoot (if ever walked) succeeds and the dir
  // is unambiguously a project root.
  fs.writeFileSync(path.join(dir, 'package.json'), '{"name":"t"}\n');
  return dir;
}

after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

describe('WorkspaceIndexer — symbol extraction invariants (name guard + line math)', () => {
  it('assigns correct 1-based line numbers and never emits an empty-named symbol', async () => {
    const dir = mkProject();
    const file = path.join(dir, 'm.ts');
    // Deliberately put each declaration on a known line (1-based).
    const content = [
      'export function alpha() {}',   // line 1  -> function alpha
      '',                             // line 2
      'class Beta {}',                // line 3  -> class Beta
      'interface Gamma { x: number }',// line 4  -> interface Gamma
      'type Delta = string;',         // line 5  -> type Delta
      'const epsilon = 42;',          // line 6  -> variable epsilon
    ].join('\n') + '\n';
    fs.writeFileSync(file, content);

    const indexer = new WorkspaceIndexer(dir);
    const entry = await indexer.indexFile(file);
    assert.ok(entry, 'file should index');

    // The `if (!name) continue` guard means EVERY stored symbol has a real name.
    for (const s of entry!.symbols) {
      assert.ok(s.name && s.name.length > 0, `symbol name must be non-empty: ${JSON.stringify(s)}`);
      assert.equal(s.filePath, file);
      assert.equal(s.language, 'typescript');
    }

    const byName = new Map(entry!.symbols.map(s => [s.name, s]));
    assert.equal(byName.get('alpha')!.type, 'function');
    assert.equal(byName.get('alpha')!.line, 1);
    assert.equal(byName.get('Beta')!.type, 'class');
    assert.equal(byName.get('Beta')!.line, 3);
    assert.equal(byName.get('Gamma')!.type, 'interface');
    assert.equal(byName.get('Gamma')!.line, 4);
    assert.equal(byName.get('Delta')!.type, 'type');
    assert.equal(byName.get('Delta')!.line, 5);
    assert.equal(byName.get('epsilon')!.type, 'variable');
    assert.equal(byName.get('epsilon')!.line, 6);
  });

  it('nameless/anonymous constructs produce no symbols (guard contract: no empty names reach the index)', async () => {
    const dir = mkProject();
    const file = path.join(dir, 'anon.ts');
    // None of these supply an identifier where the regexes capture `(\w+)`,
    // so extractSymbols must yield ZERO entries — proving the index is never
    // polluted with empty-named symbols.
    const content = [
      'const fn = function () {};',   // anonymous function expression (no name after `function`)
      'export default class {}',      // anonymous default class
      'const arrow = () => {};',      // `arrow` IS a variable, captured; function part anonymous
    ].join('\n') + '\n';
    fs.writeFileSync(file, content);

    const indexer = new WorkspaceIndexer(dir);
    const entry = await indexer.indexFile(file);
    assert.ok(entry);

    // The only captured symbols are the two `const` variable names, and both are non-empty.
    const names = entry!.symbols.map(s => s.name).sort();
    assert.deepEqual(names, ['arrow', 'fn']);
    assert.ok(!entry!.symbols.some(s => !s.name), 'no empty-named symbol may be indexed');
    // No `function`/`class` symbol was captured from the anonymous forms.
    assert.ok(!entry!.symbols.some(s => s.type === 'function' || s.type === 'class'),
      'anonymous function/class must not yield a symbol');
  });

  it('computes line numbers correctly for symbols after multi-line leading content (slice/split math)', async () => {
    const dir = mkProject();
    const file = path.join(dir, 'late.py');
    // 4 blank/comment lines then a def on line 5 and a class on line 7.
    const content = [
      '# header',   // 1
      '',           // 2
      '',           // 3
      '',           // 4
      'def late_fn():', // 5
      '    pass',   // 6
      'class LateCls:', // 7
      '    pass',   // 8
    ].join('\n') + '\n';
    fs.writeFileSync(file, content);

    const indexer = new WorkspaceIndexer(dir);
    const entry = await indexer.indexFile(file);
    assert.ok(entry);

    const fn = entry!.symbols.find(s => s.name === 'late_fn');
    const cls = entry!.symbols.find(s => s.name === 'LateCls');
    assert.ok(fn, 'def should be extracted');
    assert.ok(cls, 'class should be extracted');
    assert.equal(fn!.line, 5, 'def line number must reflect preceding lines');
    assert.equal(cls!.line, 7, 'class line number must reflect preceding lines');
    assert.equal(fn!.type, 'function');
    assert.equal(cls!.type, 'class');
  });
});

describe('WorkspaceIndexer — import extraction guard (raw module path always non-empty)', () => {
  it('matched imports always carry a non-empty module path; unresolved ones add no edge', async () => {
    const dir = mkProject();
    const app = path.join(dir, 'app.ts');
    const lib = path.join(dir, 'lib.ts');
    fs.writeFileSync(lib, 'export const libValue = 1;\n');
    // Fixture = one resolvable relative import (-> edge) + one bare package import
    // that resolves to null. The `from` keyword is built at runtime (kw): the
    // native-ESM test harness (scripts/esm-build.mjs) rewrites relative specifiers
    // `from './x'` -> `from './x.mjs'` across the compiled .mjs, and its regex would
    // otherwise corrupt this `from './lib'` INSIDE the string fixture (making
    // app.ts import './lib.mjs', which never resolves). Splitting `from` from the
    // quote keeps the literal out of the rewriter's pattern while the written file
    // is still correct. The bare import uses a package that cannot resolve anywhere.
    const kw = 'fr' + 'om';
    fs.writeFileSync(app, `import { libValue } ${kw} './lib';\nimport * as X ${kw} '@ami-cov/definitely-nonexistent-pkg-xyz';\n`);

    const indexer = new WorkspaceIndexer(dir);
    const entry = await indexer.indexFile(app);
    assert.ok(entry);

    // Only the resolvable relative import becomes an edge; the bare one is dropped.
    assert.deepEqual(entry!.imports, [lib]);
    // No empty string ever enters the imports list (the `!raw` guard contract).
    assert.ok(!entry!.imports.some(p => !p), 'no empty import path may be recorded');
    assert.ok(indexer.findDependencies(app).includes(lib));
    assert.deepEqual(indexer.findDependents(lib), [app]);
  });
});
