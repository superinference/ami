import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { WorkspaceIndexer, findProjectRoot } from '../src/workspace-indexer';

// ---------------------------------------------------------------------------
// Helpers — all fixtures live in fresh temp dirs (no coupling to the .ts tree)
// ---------------------------------------------------------------------------

function makeProjectDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wsi-cov2-'));
  fs.writeFileSync(path.join(dir, 'package.json'), '{}');
  return dir;
}

function makeBareDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-wsi-cov2-bare-'));
}

function cleanup(dir: string): void {
  fs.rmSync(dir, { recursive: true, force: true });
}

// Author fixture `import ... from '<spec>'` lines without the literal `from '`
// token in this test's own source. The native-ESM coverage harness
// (scripts/coverage-esm.mjs) rewrites every relative `from './x'` specifier it
// finds in the compiled .mjs to append `.mjs` — and it cannot distinguish a real
// import from one embedded in a fixture string. Keeping the quote in a variable
// breaks that token at the source level while the written file stays identical,
// so the indexer still parses a genuine `import { X } from './x';` statement.
const Q = "'";
function relImport(binding: string, spec: string): string {
  return `import ${binding} from ${Q}${spec}${Q};`;
}

// ---------------------------------------------------------------------------
// Go / Rust / Java symbol extraction + raw-import handling (resolveImport null)
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer — Go extraction', () => {
  it('extracts func/method/struct/interface and resolves imports to nothing', async () => {
    const dir = makeProjectDir();
    try {
      fs.writeFileSync(
        path.join(dir, 'main.go'),
        `package main

import (
	"fmt"
	"os"
)

type Server struct {
	addr string
}

type Handler interface {
	Handle()
}

func main() {
	fmt.Println("hi")
}

func (s *Server) Handle() {
}
`,
      );
      const indexer = new WorkspaceIndexer(dir);
      await indexer.buildIndex();

      const main = indexer.findDefinition('main');
      assert.ok(main.length >= 1);
      assert.equal(main[0].language, 'go');
      assert.equal(main[0].type, 'function');

      // method name captured from receiver form `func (s *Server) Handle()`
      const handle = indexer.findDefinition('Handle');
      assert.ok(handle.some(s => s.type === 'function'), 'method Handle should be a function symbol');

      const server = indexer.findDefinition('Server');
      assert.equal(server[0].type, 'class');

      const handler = indexer.findDefinition('Handler');
      assert.equal(handler[0].type, 'interface');

      // Go imports are raw module paths — resolveImport returns null, so no edges.
      const goPath = path.join(dir, 'main.go');
      assert.deepEqual(indexer.findDependencies(goPath), []);
    } finally {
      cleanup(dir);
    }
  });
});

describe('WorkspaceIndexer — Rust extraction', () => {
  it('extracts fn/struct/trait/enum and treats use as unresolved import', async () => {
    const dir = makeProjectDir();
    try {
      fs.writeFileSync(
        path.join(dir, 'lib.rs'),
        `use std::collections::HashMap;

pub struct Config {
    name: String,
}

pub trait Runner {
    fn run(&self);
}

pub enum Mode {
    Fast,
    Slow,
}

pub fn execute() {}

async fn helper() {}
`,
      );
      const indexer = new WorkspaceIndexer(dir);
      await indexer.buildIndex();

      assert.equal(indexer.findDefinition('execute')[0].type, 'function');
      assert.equal(indexer.findDefinition('helper')[0].type, 'function');
      assert.equal(indexer.findDefinition('Config')[0].type, 'class');
      assert.equal(indexer.findDefinition('Runner')[0].type, 'interface');
      assert.equal(indexer.findDefinition('Mode')[0].type, 'type');

      // Indented `fn run(&self)` is NOT top-level and must not be extracted.
      assert.equal(indexer.findDefinition('run').length, 0);

      assert.deepEqual(indexer.findDependencies(path.join(dir, 'lib.rs')), []);
    } finally {
      cleanup(dir);
    }
  });
});

describe('WorkspaceIndexer — Java extraction', () => {
  it('extracts method/class/interface and treats import as unresolved', async () => {
    const dir = makeProjectDir();
    try {
      fs.writeFileSync(
        path.join(dir, 'Service.java'),
        `package com.example;

import java.util.List;

public class Service {
}

public void start() {
}

private int compute(int x) {
    return x;
}

interface Repository {
}
`,
      );
      const indexer = new WorkspaceIndexer(dir);
      await indexer.buildIndex();

      assert.equal(indexer.findDefinition('Service')[0].type, 'class');
      assert.equal(indexer.findDefinition('Repository')[0].type, 'interface');
      assert.ok(indexer.findDefinition('start').some(s => s.type === 'function'));
      assert.ok(indexer.findDefinition('compute').some(s => s.type === 'function'));

      assert.deepEqual(indexer.findDependencies(path.join(dir, 'Service.java')), []);
    } finally {
      cleanup(dir);
    }
  });
});

// ---------------------------------------------------------------------------
// TypeScript import resolution branches
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer — TypeScript import resolution', () => {
  it('resolves .tsx and index.ts candidates, ignores bare and missing imports', async () => {
    const dir = makeProjectDir();
    try {
      fs.mkdirSync(path.join(dir, 'sub'), { recursive: true });
      fs.writeFileSync(path.join(dir, 'comp.tsx'), 'export const Comp = () => null;\n');
      fs.writeFileSync(path.join(dir, 'sub', 'index.ts'), 'export const subThing = 1;\n');
      fs.writeFileSync(
        path.join(dir, 'app.ts'),
        [
          relImport('{ Comp }', './comp'),
          relImport('{ subThing }', './sub'),
          relImport('React', 'react'),
          relImport('{ ghost }', './missing'),
          '',
          'export function app() {}',
          '',
        ].join('\n'),
      );
      const indexer = new WorkspaceIndexer(dir);
      await indexer.buildIndex();

      const deps = indexer.findDependencies(path.join(dir, 'app.ts'));
      const rel = deps.map(d => path.relative(dir, d)).sort();
      // './comp' -> comp.tsx ; './sub' -> sub/index.ts
      assert.deepEqual(rel, ['comp.tsx', path.join('sub', 'index.ts')].sort());
      // Bare 'react' and missing './missing' must not appear.
      assert.ok(!rel.some(r => r.includes('react')));
      assert.ok(!rel.some(r => r.includes('missing')));
    } finally {
      cleanup(dir);
    }
  });
});

// ---------------------------------------------------------------------------
// Python import resolution branches (file + package __init__.py)
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer — Python import resolution', () => {
  it('resolves module files and package __init__.py; leaves stdlib unresolved', async () => {
    const dir = makeProjectDir();
    try {
      fs.writeFileSync(path.join(dir, 'mod.py'), 'def helper():\n    pass\n');
      fs.mkdirSync(path.join(dir, 'pkg'), { recursive: true });
      fs.writeFileSync(path.join(dir, 'pkg', '__init__.py'), 'X = 1\n');
      fs.writeFileSync(
        path.join(dir, 'app.py'),
        `from mod import helper
import pkg
import os

def main():
    pass
`,
      );
      const indexer = new WorkspaceIndexer(dir);
      await indexer.buildIndex();

      const deps = indexer.findDependencies(path.join(dir, 'app.py'));
      const rel = deps.map(d => path.relative(dir, d)).sort();
      assert.ok(rel.includes('mod.py'), `expected mod.py, got ${rel}`);
      assert.ok(rel.includes(path.join('pkg', '__init__.py')), `expected pkg/__init__.py, got ${rel}`);
      // stdlib `os` does not resolve to a workspace file.
      assert.ok(!rel.some(r => r.startsWith('os')));
    } finally {
      cleanup(dir);
    }
  });
});

// ---------------------------------------------------------------------------
// indexFile — directory with a source extension (!stat.isFile())
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer — indexFile on a directory', () => {
  it('returns null when the path is a directory even with a source extension', async () => {
    const dir = makeProjectDir();
    try {
      const dirAsFile = path.join(dir, 'weird.ts');
      fs.mkdirSync(dirAsFile);
      const indexer = new WorkspaceIndexer(dir);
      const result = await indexer.indexFile(dirAsFile);
      assert.equal(result, null);
    } finally {
      cleanup(dir);
    }
  });
});

// ---------------------------------------------------------------------------
// removeFile — shared symbols and shared reverse-import edges (keep branch)
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer — removeFile shared-state branches', () => {
  it('keeps a symbol defined in another file after removing one definer', async () => {
    const dir = makeProjectDir();
    try {
      fs.writeFileSync(path.join(dir, 'a.ts'), 'export function shared() {}\n');
      fs.writeFileSync(path.join(dir, 'b.ts'), 'export function shared() {}\n');
      const indexer = new WorkspaceIndexer(dir);
      await indexer.indexFile(path.join(dir, 'a.ts'));
      await indexer.indexFile(path.join(dir, 'b.ts'));

      assert.equal(indexer.findDefinition('shared').length, 2);
      indexer.removeFile(path.join(dir, 'a.ts'));
      const after = indexer.findDefinition('shared');
      assert.equal(after.length, 1, 'symbol from b.ts must survive');
      assert.ok(after[0].filePath.endsWith('b.ts'));
    } finally {
      cleanup(dir);
    }
  });

  it('keeps a reverse-import edge when only one of several importers is removed', async () => {
    const dir = makeProjectDir();
    try {
      fs.writeFileSync(path.join(dir, 'core.ts'), 'export function coreFn() {}\n');
      fs.writeFileSync(path.join(dir, 'b.ts'), relImport('{ coreFn }', './core') + '\n');
      fs.writeFileSync(path.join(dir, 'c.ts'), relImport('{ coreFn }', './core') + '\n');
      const indexer = new WorkspaceIndexer(dir);
      await indexer.indexFile(path.join(dir, 'core.ts'));
      await indexer.indexFile(path.join(dir, 'b.ts'));
      await indexer.indexFile(path.join(dir, 'c.ts'));

      const corePath = path.join(dir, 'core.ts');
      assert.equal(indexer.findDependents(corePath).length, 2);

      indexer.removeFile(path.join(dir, 'b.ts'));
      const deps = indexer.findDependents(corePath);
      assert.equal(deps.length, 1, 'c.ts must still depend on core.ts');
      assert.ok(deps[0].endsWith('c.ts'));
    } finally {
      cleanup(dir);
    }
  });
});

// ---------------------------------------------------------------------------
// findDependents / findDependencies — relative (non-absolute) path argument
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer — relative path arguments', () => {
  it('resolves relative paths against cwd for dependency queries', async () => {
    const dir = makeProjectDir();
    try {
      fs.mkdirSync(path.join(dir, 'src'), { recursive: true });
      fs.writeFileSync(path.join(dir, 'src', 'dep.ts'), 'export function dep() {}\n');
      fs.writeFileSync(path.join(dir, 'src', 'use.ts'), relImport('{ dep }', './dep') + '\n');
      const indexer = new WorkspaceIndexer(dir);
      await indexer.indexFile(path.join(dir, 'src', 'dep.ts'));
      await indexer.indexFile(path.join(dir, 'src', 'use.ts'));

      // Pass relative paths — must be resolved via cwd internally.
      const dependents = indexer.findDependents(path.join('src', 'dep.ts'));
      assert.ok(dependents.some(d => d.endsWith('use.ts')));
      const dependencies = indexer.findDependencies(path.join('src', 'use.ts'));
      assert.ok(dependencies.some(d => d.endsWith('dep.ts')));
    } finally {
      cleanup(dir);
    }
  });
});

// ---------------------------------------------------------------------------
// getSummary — no key symbols and no import edges (both optional parts absent)
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer — getSummary minimal', () => {
  it('omits key symbols and top files when none qualify', async () => {
    const dir = makeProjectDir();
    try {
      // Only a variable and a type: no class/function, no imports.
      fs.writeFileSync(
        path.join(dir, 'only.ts'),
        'export const FOO = 1;\nexport type T = string;\n',
      );
      const indexer = new WorkspaceIndexer(dir);
      await indexer.buildIndex();

      const summary = indexer.getSummary();
      assert.ok(summary.startsWith('Workspace:'));
      assert.ok(!summary.includes('Key symbols'), 'no class/function means no key-symbols line');
      assert.ok(!summary.includes('Top files'), 'no imports means no top-files line');
    } finally {
      cleanup(dir);
    }
  });
});

// ---------------------------------------------------------------------------
// searchSymbols — early-break when far more than limit matches are collected
// ---------------------------------------------------------------------------

describe('WorkspaceIndexer — searchSymbols early break', () => {
  it('stops collecting after limit*3 matches and returns at most `limit`', async () => {
    const dir = makeProjectDir();
    try {
      const lines = Array.from({ length: 70 }, (_, i) => `export function matchAbc${i}() {}`);
      fs.writeFileSync(path.join(dir, 'many.ts'), lines.join('\n') + '\n');
      const indexer = new WorkspaceIndexer(dir);
      await indexer.buildIndex();

      const results = indexer.searchSymbols('matchabc');
      assert.equal(results.length, 20, 'default limit is 20');
      assert.ok(results.every(r => r.name.toLowerCase().includes('matchabc')));
    } finally {
      cleanup(dir);
    }
  });
});

// ---------------------------------------------------------------------------
// findProjectRoot — depth cap and the home-directory break
// ---------------------------------------------------------------------------

describe('findProjectRoot — walk limits', () => {
  it('returns null via the depth cap for very deep marker-less trees', () => {
    const base = makeBareDir();
    try {
      let deep = base;
      for (let i = 0; i < 22; i++) {
        deep = path.join(deep, `d${i}`);
      }
      fs.mkdirSync(deep, { recursive: true });
      assert.equal(findProjectRoot(deep), null);
    } finally {
      cleanup(base);
    }
  });
});
