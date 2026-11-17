/**
 * U016 (wave 70) — memory.ts parity gap: user-facing memory/instruction-file
 * HEALTH DIAGNOSTICS surfaced through /doctor.
 *
 * Claude Code's buildMemoryDiagnostics (utils/status.tsx + doctorContextWarnings.ts
 * + statusNoticeDefinitions.tsx) walks every loaded memory / CLAUDE.md / AGENTS.md
 * file and emits a user-visible warning for each file over the recommended
 * MAX_MEMORY_CHARACTER_COUNT ("Large <path> will impact performance (N chars > M) ·
 * /memory to edit"), sorted largest-first, plus flags files it cannot read. AMI
 * previously handled these boundary conditions SILENTLY (oversized instruction
 * files truncated with only a coreLog; unreadable files swallowed), so the user
 * was never told WHICH file was bloating / breaking their context.
 *
 * MemoryManager.buildMemoryDiagnostics() returns that typed, user-facing list,
 * and /doctor renders it as a check.
 *
 * Run: cd core/core && node --import tsx --test test/memory-u016-w70.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  MemoryManager,
  MAX_MEMORY_CHARACTER_COUNT,
  type MemoryDiagnostic,
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
  cwdDir = mkTmp('si-mem-w70-cwd-');
  memDir = mkTmp('si-mem-w70-store-');
  // Isolate the user-global instruction lookup (~/.ami, ~/.claude) so the dev
  // machine's real global CLAUDE.md cannot leak diagnostics into the assertions.
  homeDir = mkTmp('si-mem-w70-home-');
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

describe('U016-w70 — buildMemoryDiagnostics', () => {
  it('returns no diagnostics when all files are within limits', () => {
    fs.writeFileSync(path.join(cwdDir, 'CLAUDE.md'), '# small\nhello\n', 'utf-8');
    fs.writeFileSync(path.join(memDir, 'note.md'), '---\nname: note\ntype: project\n---\n\nbody\n', 'utf-8');
    const diags = new MemoryManager(cwdDir).buildMemoryDiagnostics();
    assert.deepEqual(diags, []);
  });

  it('flags an oversized stored memory file (> MAX_MEMORY_CHARACTER_COUNT)', () => {
    const big = path.join(memDir, 'huge.md');
    fs.writeFileSync(big, '---\nname: huge\ntype: project\n---\n\n' + 'x'.repeat(MAX_MEMORY_CHARACTER_COUNT + 500), 'utf-8');
    const diags = new MemoryManager(cwdDir).buildMemoryDiagnostics();
    const hit = diags.find((d: MemoryDiagnostic) => d.file === big);
    assert.ok(hit, 'oversized memory file is flagged');
    assert.equal(hit!.level, 'warn');
    assert.ok(hit!.chars! > MAX_MEMORY_CHARACTER_COUNT);
    assert.ok(/will impact performance/.test(hit!.message), 'perf wording present');
    assert.ok(/\/memory to edit/.test(hit!.message), 'remediation pointer present');
    // Char counts are locale-formatted with a thousands separator.
    assert.ok(hit!.message.includes((40000).toLocaleString('en-US')), 'limit shown with separator');
  });

  it('flags an oversized CLAUDE.md instruction file (> MAX_MEMORY_CHARACTER_COUNT) as a perf warning', () => {
    const claude = path.join(cwdDir, 'CLAUDE.md');
    fs.writeFileSync(claude, '# big\n' + 'y'.repeat(MAX_MEMORY_CHARACTER_COUNT + 100), 'utf-8');
    const diags = new MemoryManager(cwdDir).buildMemoryDiagnostics();
    const hit = diags.find((d: MemoryDiagnostic) => d.file === claude);
    assert.ok(hit, 'oversized CLAUDE.md is flagged');
    assert.ok(/will impact performance/.test(hit!.message));
  });

  it('flags a mid-size instruction file that will be silently truncated', () => {
    // Between the 10_240-byte truncation cap and the 40k perf cap: AMI truncates
    // this silently today, so the user must at least be told it is being cut.
    const agents = path.join(cwdDir, 'AGENTS.md');
    fs.writeFileSync(agents, '# agents\n' + 'z'.repeat(20_000), 'utf-8');
    const diags = new MemoryManager(cwdDir).buildMemoryDiagnostics();
    const hit = diags.find((d: MemoryDiagnostic) => d.file === agents);
    assert.ok(hit, 'mid-size instruction file is flagged');
    assert.ok(/truncat/i.test(hit!.message), 'truncation is surfaced');
  });

  it('sorts stored-memory diagnostics largest-first', () => {
    const a = path.join(memDir, 'a.md');
    const b = path.join(memDir, 'b.md');
    fs.writeFileSync(a, 'x'.repeat(MAX_MEMORY_CHARACTER_COUNT + 10), 'utf-8');
    fs.writeFileSync(b, 'x'.repeat(MAX_MEMORY_CHARACTER_COUNT + 9000), 'utf-8');
    const diags = new MemoryManager(cwdDir).buildMemoryDiagnostics();
    const mem = diags.filter((d: MemoryDiagnostic) => d.file === a || d.file === b);
    assert.equal(mem.length, 2);
    assert.equal(mem[0].file, b, 'larger file first');
  });

  it('home-redacts the path in the message (no raw home dir leaked)', () => {
    const big = path.join(memDir, 'huge.md');
    fs.writeFileSync(big, 'x'.repeat(MAX_MEMORY_CHARACTER_COUNT + 1), 'utf-8');
    const diags = new MemoryManager(cwdDir).buildMemoryDiagnostics();
    const hit = diags.find((d: MemoryDiagnostic) => d.file === big)!;
    // memDir is under the OS tmpdir, not home, so it is shown unchanged; assert the
    // message never contains a literal "/home/<user>" absolute home segment.
    const home = os.homedir();
    if (home && home !== '/' && !memDir.startsWith(home)) {
      assert.ok(!hit.message.includes(home), 'home dir not leaked verbatim');
    }
  });
});
