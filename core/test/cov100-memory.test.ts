import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { MemoryManager } from '../src/memory';
import { projectStateDir } from '../src/reference-journal';

// ===========================================================================
// memory.ts lines 170-177 — memoryFreshness() age-band labeling.
//
// memoryFreshness is private; it runs inside loadMemories(), which stamps each
// MemoryEntry.freshness from the file's mtime. We drive every band + each
// comparison boundary by writing real memory files and backdating their mtime,
// then asserting the exact human-readable label the function produces.
// ===========================================================================

const tmpDirs: string[] = [];

function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'cov100-memory-'));
  tmpDirs.push(d);
  return d;
}

const DAY_MS = 1000 * 60 * 60 * 24;

/**
 * Write a memory .md file and backdate its mtime so loadMemories() computes
 * an age of exactly `ageDays` full days (a +6h offset keeps the floor division
 * landing squarely on `ageDays`, immune to sub-second drift during the test).
 */
function writeMemoryAged(cwd: string, name: string, ageDays: number): void {
  const memDir = projectStateDir(cwd, 'memory');
  fs.mkdirSync(memDir, { recursive: true });
  const fp = path.join(memDir, `${name}.md`);
  fs.writeFileSync(
    fp,
    `---\nname: ${name}\ndescription: d\ntype: project\n---\n\nbody-${name}\n`,
  );
  const when = new Date(Date.now() - (ageDays * DAY_MS + 6 * 60 * 60 * 1000));
  fs.utimesSync(fp, when, when);
}

function freshnessFor(cwd: string, name: string): string | undefined {
  const mgr = new MemoryManager(cwd);
  const mem = mgr.loadMemories().find(m => m.name === name);
  assert.ok(mem, `expected memory "${name}" to load`);
  return mem.freshness;
}

describe('memory.ts — memoryFreshness age bands (lines 170-177)', () => {
  after(() => {
    for (const d of tmpDirs) {
      try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });

  it('ageDays === 0 → "today"', () => {
    const cwd = mkTmp();
    writeMemoryAged(cwd, 'm0', 0);
    assert.equal(freshnessFor(cwd, 'm0'), 'today');
  });

  it('ageDays === 1 → "yesterday"', () => {
    const cwd = mkTmp();
    writeMemoryAged(cwd, 'm1', 1);
    assert.equal(freshnessFor(cwd, 'm1'), 'yesterday');
  });

  it('2 <= ageDays <= 7 → "N days ago" (lower edge)', () => {
    const cwd = mkTmp();
    writeMemoryAged(cwd, 'm2', 2);
    assert.equal(freshnessFor(cwd, 'm2'), '2 days ago');
  });

  it('ageDays === 7 → "7 days ago" (upper edge of the days band)', () => {
    const cwd = mkTmp();
    writeMemoryAged(cwd, 'm7', 7);
    assert.equal(freshnessFor(cwd, 'm7'), '7 days ago');
  });

  it('ageDays === 8 → "1 weeks ago" (first value past the days band)', () => {
    const cwd = mkTmp();
    writeMemoryAged(cwd, 'm8', 8);
    assert.equal(freshnessFor(cwd, 'm8'), '1 weeks ago');
  });

  it('ageDays === 20 → "2 weeks ago" (mid weeks band)', () => {
    const cwd = mkTmp();
    writeMemoryAged(cwd, 'm20', 20);
    assert.equal(freshnessFor(cwd, 'm20'), '2 weeks ago');
  });

  it('ageDays === 30 → "4 weeks ago" (upper edge of the weeks band)', () => {
    const cwd = mkTmp();
    writeMemoryAged(cwd, 'm30', 30);
    assert.equal(freshnessFor(cwd, 'm30'), '4 weeks ago');
  });

  it('ageDays === 31 → "1 months ago" (first value past the weeks band)', () => {
    const cwd = mkTmp();
    writeMemoryAged(cwd, 'm31', 31);
    assert.equal(freshnessFor(cwd, 'm31'), '1 months ago');
  });

  it('ageDays === 90 → "3 months ago" (deep months band)', () => {
    const cwd = mkTmp();
    writeMemoryAged(cwd, 'm90', 90);
    assert.equal(freshnessFor(cwd, 'm90'), '3 months ago');
  });
});
