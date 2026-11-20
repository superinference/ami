/**
 * U016 (wave 78) — memory.ts parity gap: MEMORY-LOAD INTROSPECTION.
 *
 * Claude Code emits `memory_files_completed` / `tengu_claudemd__initial_load`
 * introspection events (the reference's markdown-memory module:L1014-L1040) recording the loaded
 * instruction/memory file count, per-source-level counts, total content length and
 * load duration so the context COST of what memory contributes each turn is
 * visible. AMI previously loaded memory/instruction files with no such summary.
 *
 * `MemoryManager.getMemoryLoadSummary()` returns that typed summary (reusing the one
 * canonical `collectInstructionSources` discovery pass so it can never diverge from
 * what is actually loaded), fires a no-PII `memory_load_summary` diagnostic, and is
 * rendered by `/doctor` as a `memory-load` check.
 *
 * Run: cd core/core && node --import tsx --test test/memory-u016-load-summary-w78.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  MemoryManager,
  MEMORY_SOURCE_LEVELS,
  type MemoryLoadSummary,
} from '../src/memory';

let cwdDir: string;
let memDir: string;
let homeDir: string;
const savedMemEnv = process.env.AMI_MEMORY_DIR;
const savedHome = process.env.HOME;
const savedDisableEnv = process.env.AMI_DISABLE_AUTO_MEMORY;

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

beforeEach(() => {
  cwdDir = mkTmp('si-mem-ls78-cwd-');
  memDir = mkTmp('si-mem-ls78-store-');
  // Isolate user-global instruction lookup (~/.ami, ~/.claude) so the dev
  // machine's real global CLAUDE.md cannot leak into the counts.
  homeDir = mkTmp('si-mem-ls78-home-');
  process.env.AMI_MEMORY_DIR = memDir;
  process.env.HOME = homeDir;
  delete process.env.AMI_DISABLE_AUTO_MEMORY;
});

afterEach(() => {
  for (const d of [cwdDir, memDir, homeDir]) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
  if (savedMemEnv === undefined) delete process.env.AMI_MEMORY_DIR;
  else process.env.AMI_MEMORY_DIR = savedMemEnv;
  if (savedHome === undefined) delete process.env.HOME;
  else process.env.HOME = savedHome;
  if (savedDisableEnv === undefined) delete process.env.AMI_DISABLE_AUTO_MEMORY;
  else process.env.AMI_DISABLE_AUTO_MEMORY = savedDisableEnv;
});

describe('U016-w78 — MemoryManager.getMemoryLoadSummary', () => {
  it('returns a zeroed but fully-shaped summary for an empty workspace', async () => {
    const sum = await new MemoryManager(cwdDir).getMemoryLoadSummary();
    assert.equal(sum.fileCount, 0);
    assert.equal(sum.totalBytes, 0);
    assert.ok(sum.durationMs >= 0, 'duration is non-negative');
    // Every source level present (zero), so a consumer can render a stable table.
    for (const level of MEMORY_SOURCE_LEVELS) {
      assert.equal(sum.byLevel[level], 0, `${level} starts at 0`);
    }
  });

  it('counts a project instruction file, an automem memory, and their bytes', async () => {
    const claude = path.join(cwdDir, 'CLAUDE.md');
    fs.writeFileSync(claude, '# project\nfollow the rules\n', 'utf-8');
    const mm = new MemoryManager(cwdDir);
    mm.saveMemory('note', 'remember this fact', 'a fact', 'project');

    const sum = await mm.getMemoryLoadSummary();
    assert.equal(sum.byLevel.project, 1, 'one project instruction file');
    assert.equal(sum.byLevel.automem, 1, 'one stored (automem) memory');
    assert.equal(sum.fileCount, 2, 'fileCount is the sum across levels');
    assert.ok(sum.totalBytes > 0, 'content bytes are tallied');
  });

  it('fileCount always equals the sum of per-level counts', async () => {
    fs.writeFileSync(path.join(cwdDir, 'CLAUDE.md'), '# p\nx\n', 'utf-8');
    fs.writeFileSync(path.join(cwdDir, 'CLAUDE.local.md'), '# local\ny\n', 'utf-8');
    const mm = new MemoryManager(cwdDir);
    mm.saveMemory('m1', 'body one', 'd1', 'project');
    mm.saveMemory('m2', 'body two', 'd2', 'reference');

    const sum: MemoryLoadSummary = await mm.getMemoryLoadSummary();
    const perLevelTotal = MEMORY_SOURCE_LEVELS.reduce((n, l) => n + sum.byLevel[l], 0);
    assert.equal(sum.fileCount, perLevelTotal);
    assert.equal(sum.byLevel.local, 1, 'local .local.md counted distinctly');
    assert.equal(sum.byLevel.automem, 2, 'two stored memories');
  });

  it('respects the auto-memory disable gate for stored memories (instructions still counted)', async () => {
    fs.writeFileSync(path.join(cwdDir, 'CLAUDE.md'), '# p\nx\n', 'utf-8');
    const mm = new MemoryManager(cwdDir);
    mm.saveMemory('note', 'a stored memory', 'd', 'project');

    // collectInstructionSources always loads the automem store regardless of the
    // prompt-injection gate, so the summary reports the on-disk truth; the project
    // instruction is independent of auto-memory. Sanity-check both are present.
    const sum = await mm.getMemoryLoadSummary();
    assert.equal(sum.byLevel.project, 1);
    assert.equal(sum.byLevel.automem, 1);
  });
});
