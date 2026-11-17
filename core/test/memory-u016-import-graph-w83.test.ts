/**
 * U016 / wave w83 — @-import HIERARCHY as a parent-tracked graph + nested tree view.
 *
 * Parity gaps closed:
 *   - `@include resolved into SEPARATE file entries (parent-before-child) with per-file
 *     parent tracking` (Claude Code `processMemoryFile`, utils/claudemd.ts): AMI inlines
 *     imports into the prompt (kept — superset), and now ALSO exposes the import graph as
 *     an observable, parent-tracked, pre-order (parent-before-child) list via
 *     `MemoryManager.buildImportGraph`.
 *   - `Nested @-import tree view — indented 'L <path>' rows` (Claude Code
 *     MemoryFileSelector): `formatImportTree` renders the graph as indented tree rows,
 *     surfaced per entrypoint by `getInstructionImportTrees` (wired into `/memory`).
 *
 * The graph reuses the SAME security-cleared resolver + prose-line scan as the inline
 * `resolveAtImports`, so a token the inline path refuses (outside-workspace, non-text,
 * code span, cycle) never appears in the graph either.
 *
 * Red-green: with `buildImportGraph`/`getInstructionImportTrees` reverted, the import
 * graph (parent pointers + parent-before-child ordering) and the `/memory` tree rows
 * vanish — these assertions fail (method missing / undefined).
 *
 * Run: cd core/core && node --import tsx --test test/memory-u016-import-graph-w83.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  MemoryManager,
  formatImportTree,
  type MemoryImportNode,
} from '../src/memory';

let ROOT: string;
let CWD: string;
let dataDir: string;
let memDir: string;
const savedEnv: Record<string, string | undefined> = {};

function stashEnv(...keys: string[]): void {
  for (const k of keys) savedEnv[k] = process.env[k];
}
function restoreEnv(): void {
  for (const [k, v] of Object.entries(savedEnv)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
}

function write(rel: string, content: string): string {
  const abs = path.join(CWD, rel);
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, content, 'utf-8');
  return fs.realpathSync(abs);
}

beforeEach(() => {
  ROOT = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-w83ig-')));
  CWD = fs.realpathSync(fs.mkdtempSync(path.join(ROOT, 'ws-')));
  dataDir = fs.realpathSync(fs.mkdtempSync(path.join(ROOT, 'data-')));
  memDir = fs.realpathSync(fs.mkdtempSync(path.join(ROOT, 'mem-')));
  stashEnv('AMI_MEMORY_DIR', 'SUPERINFERENCE_DATA_DIR', 'CLAUDE_CONFIG_DIR',
    'AMI_ALLOW_EXTERNAL_MEMORY_IMPORTS', 'AMI_EXTERNAL_MEMORY_IMPORT_ROOTS');
  process.env.SUPERINFERENCE_DATA_DIR = dataDir;
  process.env.AMI_MEMORY_DIR = memDir;
  delete process.env.CLAUDE_CONFIG_DIR;
  delete process.env.AMI_ALLOW_EXTERNAL_MEMORY_IMPORTS;
  delete process.env.AMI_EXTERNAL_MEMORY_IMPORT_ROOTS;
});

afterEach(() => {
  restoreEnv();
  try { fs.rmSync(ROOT, { recursive: true, force: true }); } catch { /* ignore */ }
});

describe('MemoryManager.buildImportGraph — parent-tracked @-import hierarchy', () => {
  it('a flat file is a single root node (depth 0, parent null)', () => {
    const root = write('AGENTS.md', '# Flat\n\nNo imports here.');
    const g = new MemoryManager(CWD).buildImportGraph(root);
    assert.deepEqual(g, [{ path: root, parent: null, depth: 0 }]);
  });

  it('models an import as a SEPARATE child entry, parent BEFORE child', () => {
    const child = write('child.md', '# Child');
    const root = write('AGENTS.md', '# Root\n\n@./child.md\n');
    const g = new MemoryManager(CWD).buildImportGraph(root);
    assert.equal(g.length, 2);
    // Parent-before-child ordering (parity: processMemoryFile pushes main first).
    assert.equal(g[0].path, root);
    assert.equal(g[0].parent, null);
    assert.equal(g[0].depth, 0);
    // Per-file parent tracking — the child knows WHO imported it.
    assert.equal(g[1].path, child);
    assert.equal(g[1].parent, root);
    assert.equal(g[1].depth, 1);
  });

  it('walks a deep chain pre-order (a -> b -> c) with increasing depth', () => {
    const c = write('c.md', '# C');
    const b = write('b.md', '# B\n@./c.md\n');
    const a = write('AGENTS.md', '# A\n@./b.md\n');
    const g = new MemoryManager(CWD).buildImportGraph(a);
    assert.deepEqual(g.map(n => [path.basename(n.path), n.depth, n.parent]), [
      ['AGENTS.md', 0, null],
      ['b.md', 1, a],
      ['c.md', 2, b],
    ]);
  });

  it('preserves sibling import order and tracks the shared parent', () => {
    write('b.md', '# B');
    write('d.md', '# D');
    const a = write('AGENTS.md', '# A\n@./b.md\n@./d.md\n');
    const g = new MemoryManager(CWD).buildImportGraph(a);
    assert.deepEqual(g.map(n => path.basename(n.path)), ['AGENTS.md', 'b.md', 'd.md']);
    assert.ok(g.slice(1).every(n => n.parent === a && n.depth === 1));
  });

  it('breaks an import cycle without looping (a -> b -> a)', () => {
    write('b.md', '# B\n@./AGENTS.md\n');
    const a = write('AGENTS.md', '# A\n@./b.md\n');
    const g = new MemoryManager(CWD).buildImportGraph(a);
    // a, b — the cyclic back-edge to a is dropped (a already seen).
    assert.deepEqual(g.map(n => path.basename(n.path)), ['AGENTS.md', 'b.md']);
  });

  it('excludes an outside-workspace import (same confinement as inline)', () => {
    const outside = path.join(ROOT, 'secret.md');
    fs.writeFileSync(outside, '# secret', 'utf-8');
    const a = write('AGENTS.md', `# A\n@${outside}\n`);
    const g = new MemoryManager(CWD).buildImportGraph(a);
    assert.deepEqual(g.map(n => path.basename(n.path)), ['AGENTS.md']);
  });

  it('excludes a non-text import target (.png)', () => {
    write('logo.png', 'not really an image');
    const a = write('AGENTS.md', '# A\n@./logo.png\n');
    const g = new MemoryManager(CWD).buildImportGraph(a);
    assert.deepEqual(g.map(n => path.basename(n.path)), ['AGENTS.md']);
  });

  it('ignores an @path written inside fenced / inline code', () => {
    write('child.md', '# Child');
    const a = write('AGENTS.md', '# A\n```\n@./child.md\n```\nText `@./child.md` inline.\n');
    const g = new MemoryManager(CWD).buildImportGraph(a);
    assert.deepEqual(g.map(n => path.basename(n.path)), ['AGENTS.md']);
  });

  it('returns [] for a root that cannot be resolved', () => {
    const g = new MemoryManager(CWD).buildImportGraph(path.join(CWD, 'nope.md'));
    assert.deepEqual(g, []);
  });
});

describe('formatImportTree — indented L <path> rows', () => {
  it('root on its own line, nested rows prefixed with indented "L "', () => {
    const nodes: MemoryImportNode[] = [
      { path: path.join(CWD, 'AGENTS.md'), parent: null, depth: 0 },
      { path: path.join(CWD, 'child.md'), parent: path.join(CWD, 'AGENTS.md'), depth: 1 },
      { path: path.join(CWD, 'deep.md'), parent: path.join(CWD, 'child.md'), depth: 2 },
    ];
    const rows = formatImportTree(nodes, CWD);
    assert.equal(rows[0], './AGENTS.md');
    assert.equal(rows[1], 'L ./child.md');
    assert.equal(rows[2], '  L ./deep.md');
  });
});

describe('MemoryManager.getInstructionImportTrees — /memory surface', () => {
  it('yields a tree only for an entrypoint that imports another file', () => {
    write('child.md', '# Child');
    write('AGENTS.md', '# Root\n@./child.md\n');
    const trees = new MemoryManager(CWD).getInstructionImportTrees();
    const project = trees.find(t => t.scope === 'project');
    assert.ok(project, 'expected a project import tree');
    assert.equal(project!.rows[0], './AGENTS.md');
    assert.ok(project!.rows.some(r => r === 'L ./child.md'));
  });

  it('a flat project entrypoint contributes no tree', () => {
    write('AGENTS.md', '# Root\n\nNothing imported.');
    const trees = new MemoryManager(CWD).getInstructionImportTrees();
    assert.equal(trees.find(t => t.scope === 'project'), undefined);
  });
});
