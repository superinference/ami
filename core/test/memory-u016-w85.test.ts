/**
 * U016 (wave 85) — memory.ts / memory-consolidation.ts parity gaps:
 *
 *   1. CONSOLIDATION LIVE PROGRESS / INTROSPECTION (parity: Claude Code's
 *      DreamDetailDialog + DreamTaskState — components/tasks/DreamDetailDialog.tsx):
 *      a ticking elapsed timer, a `reviewing N sessions · M files touched` summary,
 *      a running/completed/error status, and a per-turn transcript with tool counts.
 *      Implemented as `ConsolidationProgress` + create/record/seal/format/snapshot
 *      helpers, wired into `beginConsolidationRun` / `abortConsolidationRun`.
 *
 *   2. TEAM-MEMORY WRITE DETECTION -> SYNC NOTIFICATION (parity: Claude Code's
 *      sessionFileAccessHooks.ts PostToolUse path — Edit/Write ONLY on an
 *      isTeamMemFile path calls teamMemWatcher.notifyTeamMemoryWrite()):
 *      `MemoryManager.notifyTeamMemoryWrite(tool, path)` emits a `teamMemoryWrite`
 *      hook event only for file_edit/file_write on a team file when enabled.
 *
 *   3/4. ASYNC, PARALLEL, CANCELLABLE HEADER SCAN (parity: Claude Code's
 *      scanMemoryFiles(dir, signal) — memdir/memoryScan.ts): AbortSignal threaded
 *      into the scan + per-file parallel reads via Promise.allSettled with per-file
 *      graceful failure. Implemented as `MemoryManager.scanMemoryHeadersAsync`.
 *
 * Run: cd core/core && node --import tsx --test test/memory-u016-w85.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { MemoryManager } from '../src/memory';
import {
  beginConsolidationRun,
  abortConsolidationRun,
  createConsolidationProgress,
  recordConsolidationTurn,
  recordConsolidationFileTouched,
  completeConsolidationProgress,
  failConsolidationProgress,
  consolidationElapsedMs,
  formatConsolidationProgress,
  consolidationProgressSnapshot,
} from '../src/memory-consolidation';

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

// ───────────────────────── Gap 1: consolidation live progress ─────────────────────────

describe('U016-w85 — consolidation live progress / introspection', () => {
  it('createConsolidationProgress starts running, zero elapsed, clamped sessions', () => {
    const p = createConsolidationProgress(-3.9, 1000);
    assert.equal(p.status, 'running');
    assert.equal(p.startedAt, 1000);
    assert.equal(p.endedAt, null);
    assert.equal(p.sessionsReviewing, 0, 'negative clamps to 0');
    assert.deepEqual(p.filesTouched, []);
    assert.deepEqual(p.turns, []);
    assert.equal(p.error, null);
    assert.equal(consolidationElapsedMs(p, 1000), 0);
  });

  it('elapsed ticks while running and freezes at the terminal transition', () => {
    const p = createConsolidationProgress(2, 1000);
    assert.equal(consolidationElapsedMs(p, 6000), 5000, 'live: now - startedAt');
    completeConsolidationProgress(p, 9000);
    // After completion, elapsed is frozen regardless of `now`.
    assert.equal(consolidationElapsedMs(p, 50000), 8000, 'frozen: endedAt - startedAt');
  });

  it('formatConsolidationProgress matches the reference summary line', () => {
    const p = createConsolidationProgress(1, 1000);
    // 1 session, no files touched → files clause omitted (parity: filesTouched.length>0).
    assert.equal(formatConsolidationProgress(p, 1000), '0s · reviewing 1 session');
    recordConsolidationFileTouched(p, '/mem/a.md');
    assert.equal(
      formatConsolidationProgress(p, 66000),
      '1m 5s · reviewing 1 session · 1 file touched',
      'elapsed + pluralized counts + files clause',
    );
    const p2 = createConsolidationProgress(3, 0);
    recordConsolidationFileTouched(p2, '/mem/a.md');
    recordConsolidationFileTouched(p2, '/mem/b.md');
    assert.equal(
      formatConsolidationProgress(p2, 5000),
      '5s · reviewing 3 sessions · 2 files touched',
    );
  });

  it('recordConsolidationFileTouched de-duplicates (first-seen order, correct count)', () => {
    const p = createConsolidationProgress(0, 0);
    assert.equal(recordConsolidationFileTouched(p, '/x.md'), true, 'new path added');
    assert.equal(recordConsolidationFileTouched(p, '/x.md'), false, 'duplicate ignored');
    assert.equal(recordConsolidationFileTouched(p, '/y.md'), true);
    assert.deepEqual(p.filesTouched, ['/x.md', '/y.md']);
  });

  it('recordConsolidationTurn clamps toolCount and snapshot sums tool calls', () => {
    const p = createConsolidationProgress(4, 0);
    recordConsolidationTurn(p, { summary: 'read logs', toolCount: 2 });
    recordConsolidationTurn(p, { summary: 'no tools' }); // defaults to 0
    recordConsolidationTurn(p, { summary: 'neg', toolCount: -5 }); // clamps to 0
    recordConsolidationTurn(p, { summary: 'frac', toolCount: 3.9 }); // truncates to 3
    assert.deepEqual(p.turns.map(t => t.toolCount), [2, 0, 0, 3]);
    recordConsolidationFileTouched(p, '/z.md');
    const snap = consolidationProgressSnapshot(p, 2000);
    assert.equal(snap.status, 'running');
    assert.equal(snap.elapsedMs, 2000);
    assert.equal(snap.sessionsReviewing, 4);
    assert.equal(snap.filesTouched, 1);
    assert.equal(snap.turns, 4);
    assert.equal(snap.totalToolCalls, 5, '2+0+0+3');
    assert.equal(snap.summary, formatConsolidationProgress(p, 2000));
  });

  it('completeConsolidationProgress is idempotent and never clobbers an error', () => {
    const p = createConsolidationProgress(1, 0);
    failConsolidationProgress(p, 'boom', 100);
    assert.equal(p.status, 'error');
    assert.equal(p.error, 'boom');
    assert.equal(p.endedAt, 100);
    // A later complete must NOT overwrite the recorded error (first terminal wins).
    completeConsolidationProgress(p, 200);
    assert.equal(p.status, 'error');
    assert.equal(p.endedAt, 100);
    // And a second fail is also a no-op.
    failConsolidationProgress(p, 'other', 300);
    assert.equal(p.error, 'boom');
  });

  it('beginConsolidationRun attaches a running progress; abort seals it as error', () => {
    const lockDir = mkTmp('si-mem-w85-lock-');
    try {
      const run = beginConsolidationRun({ lockDir, sessionsReviewing: 7, now: 1000 });
      assert.ok(run, 'fresh dir → run acquired');
      assert.equal(run.progress.status, 'running');
      assert.equal(run.progress.sessionsReviewing, 7);
      assert.equal(run.progress.startedAt, 1000);
      abortConsolidationRun(lockDir, run);
      assert.equal(run.progress.status, 'error', 'abort seals progress as error');
      assert.equal(run.progress.error, 'aborted');
      assert.equal(run.abortController.signal.aborted, true);
    } finally {
      fs.rmSync(lockDir, { recursive: true, force: true });
    }
  });
});

// ───────────────────────── Gap 2: team-memory write notification ─────────────────────────

describe('U016-w85 — team-memory write detection -> sync notification', () => {
  let cwdDir: string;
  let memDir: string;
  const savedMemEnv = process.env.AMI_MEMORY_DIR;
  const savedTeamEnv = process.env.AMI_ENABLE_TEAM_MEMORY;

  beforeEach(() => {
    cwdDir = mkTmp('si-mem-w85-cwd-');
    memDir = mkTmp('si-mem-w85-store-');
    process.env.AMI_MEMORY_DIR = memDir;
    process.env.AMI_ENABLE_TEAM_MEMORY = '1';
  });
  afterEach(() => {
    for (const d of [cwdDir, memDir]) {
      try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
    }
    if (savedMemEnv === undefined) delete process.env.AMI_MEMORY_DIR;
    else process.env.AMI_MEMORY_DIR = savedMemEnv;
    if (savedTeamEnv === undefined) delete process.env.AMI_ENABLE_TEAM_MEMORY;
    else process.env.AMI_ENABLE_TEAM_MEMORY = savedTeamEnv;
  });

  function teamFile(mgr: MemoryManager, name: string): string {
    return path.join(mgr.getTeamMemoryDir(), name);
  }

  it('fires the teamMemoryWrite hook for file_edit AND file_write on a team file', () => {
    const mgr = new MemoryManager(cwdDir);
    const events: Array<{ event: string; data: any }> = [];
    mgr.setHookCallback((event, data) => events.push({ event, data }));

    const tf = teamFile(mgr, 'shared.md');
    assert.equal(mgr.notifyTeamMemoryWrite('file_edit', tf), true, 'edit fires');
    assert.equal(mgr.notifyTeamMemoryWrite('file_write', tf), true, 'write fires');

    assert.equal(events.length, 2);
    for (const e of events) {
      assert.equal(e.event, 'teamMemoryWrite');
      assert.equal(e.data.filePath, tf);
      assert.equal(e.data.scope, 'team');
      assert.ok(/^Team \(/.test(e.data.location), 'location names the team store');
    }
  });

  it('does NOT fire for file_read (Edit/Write-only parity) or non-team paths', () => {
    const mgr = new MemoryManager(cwdDir);
    const events: string[] = [];
    mgr.setHookCallback((event) => events.push(event));

    const tf = teamFile(mgr, 'shared.md');
    assert.equal(mgr.notifyTeamMemoryWrite('file_read', tf), false, 'read never notifies');
    assert.equal(mgr.notifyTeamMemoryWrite('grep', tf), false, 'search never notifies');
    // A mutation outside the team store (e.g. a personal memory / project file).
    assert.equal(
      mgr.notifyTeamMemoryWrite('file_edit', path.join(cwdDir, 'src', 'a.ts')),
      false,
      'non-team path never notifies',
    );
    assert.equal(events.length, 0, 'no hook fired');
  });

  it('is gated by the team-memory feature flag', () => {
    delete process.env.AMI_ENABLE_TEAM_MEMORY;
    const mgr = new MemoryManager(cwdDir);
    let fired = false;
    mgr.setHookCallback(() => { fired = true; });
    const tf = teamFile(mgr, 'shared.md');
    assert.equal(mgr.notifyTeamMemoryWrite('file_edit', tf), false, 'disabled → no notify');
    assert.equal(fired, false);
    // Explicit env override re-enables it without touching process.env.
    assert.equal(
      mgr.notifyTeamMemoryWrite('file_edit', tf, { AMI_ENABLE_TEAM_MEMORY: '1' }),
      true,
      'explicit env arg honored',
    );
    assert.equal(fired, true);
  });

  it('returns true without throwing when no hook callback is registered', () => {
    const mgr = new MemoryManager(cwdDir);
    const tf = teamFile(mgr, 'shared.md');
    assert.equal(mgr.notifyTeamMemoryWrite('file_edit', tf), true);
  });
});

// ───────────────────────── Gaps 3 & 4: async parallel cancellable scan ─────────────────────────

describe('U016-w85 — scanMemoryHeadersAsync (parallel + abortable)', () => {
  let cwdDir: string;
  let memDir: string;
  const savedMemEnv = process.env.AMI_MEMORY_DIR;

  beforeEach(() => {
    cwdDir = mkTmp('si-mem-w85a-cwd-');
    memDir = mkTmp('si-mem-w85a-store-');
    process.env.AMI_MEMORY_DIR = memDir;
  });
  afterEach(() => {
    for (const d of [cwdDir, memDir]) {
      try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
    }
    if (savedMemEnv === undefined) delete process.env.AMI_MEMORY_DIR;
    else process.env.AMI_MEMORY_DIR = savedMemEnv;
  });

  function writeMem(name: string, frontmatter: Record<string, string>, body = 'body'): void {
    const fm = Object.entries(frontmatter).map(([k, v]) => `${k}: ${v}`).join('\n');
    fs.writeFileSync(path.join(memDir, name), `---\n${fm}\n---\n${body}\n`);
  }

  it('returns the SAME headers as the sync scan (shape parity)', async () => {
    writeMem('a.md', { name: 'Alpha', description: 'first', type: 'project' });
    writeMem('b.md', { name: 'Beta', description: 'second', type: 'user' });
    const mgr = new MemoryManager(cwdDir);
    const sync = mgr.scanMemoryHeaders();
    const async_ = await mgr.scanMemoryHeadersAsync();
    const norm = (h: typeof sync) =>
      [...h].map(x => ({ name: x.name, description: x.description, memoryType: x.memoryType, bytes: x.bytes }))
        .sort((p, q) => p.name.localeCompare(q.name));
    assert.deepEqual(norm(async_), norm(sync));
    assert.equal(async_.length, 2);
    // Excludes MEMORY.md index and the team subtree (same filter).
    writeMem('MEMORY.md', { name: 'idx' });
    fs.mkdirSync(path.join(memDir, 'team'), { recursive: true });
    writeMem('team/shared.md', { name: 'Shared' });
    const after = await mgr.scanMemoryHeadersAsync();
    assert.deepEqual(after.map(h => h.name).sort(), ['Alpha', 'Beta']);
  });

  it('reads in parallel and survives one unreadable file (per-file graceful failure)', async () => {
    writeMem('good1.md', { name: 'Good1' });
    writeMem('good2.md', { name: 'Good2' });
    // A directory named like a .md entry: stat says not-a-file → skipped, scan continues.
    fs.mkdirSync(path.join(memDir, 'notafile.md'));
    const mgr = new MemoryManager(cwdDir);
    const headers = await mgr.scanMemoryHeadersAsync();
    assert.deepEqual(headers.map(h => h.name).sort(), ['Good1', 'Good2']);
  });

  it('threads an AbortSignal: a pre-aborted scan drops all in-flight reads', async () => {
    writeMem('a.md', { name: 'Alpha' });
    writeMem('b.md', { name: 'Beta' });
    const mgr = new MemoryManager(cwdDir);
    const ac = new AbortController();
    ac.abort();
    const headers = await mgr.scanMemoryHeadersAsync(ac.signal);
    assert.deepEqual(headers, [], 'aborted scan yields no headers (all reads dropped)');
  });

  it('returns [] when the memory dir does not exist (graceful readdir failure)', async () => {
    fs.rmSync(memDir, { recursive: true, force: true });
    const mgr = new MemoryManager(cwdDir);
    assert.deepEqual(await mgr.scanMemoryHeadersAsync(), []);
  });
});
