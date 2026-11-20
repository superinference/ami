/**
 * U290 (wave 114) — agent-memory snapshot three-way UPDATE RESOLUTION
 * (parity with the reference's `SnapshotUpdateDialog` / `main.tsx` L2260-2273).
 *
 * The detection (`checkAgentMemorySnapshot`) and the sync operations
 * (`initialize`/`replace`) already shipped. This covers the user-facing
 * reconciliation CHOICE between a committed snapshot and local memory:
 *
 *   - resolveSnapshotUpdateChoice — normalize an untrusted/cancel value to one of
 *     'merge' | 'keep' | 'replace', defaulting to the safe 'keep' on cancel
 *     (parity: `onCancel={() => done('keep')}`, dialogLaunchers.tsx L37).
 *   - buildSnapshotMergePrompt — the merge prompt injected on 'merge'
 *     (parity: buildMergePrompt, main.tsx L2268).
 *   - applySnapshotUpdateResolution — dispatch each outcome to the right sync op:
 *     replace → replaceFromSnapshot; merge/keep → mark synced (keep local);
 *     merge also returns a prompt.
 *
 * Run: cd core/core && node --import tsx --test test/agent-memory-snapshot-resolution-u290-w114.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  applySnapshotUpdateResolution,
  resolveSnapshotUpdateChoice,
  buildSnapshotMergePrompt,
  DEFAULT_SNAPSHOT_UPDATE_RESOLUTION,
  SNAPSHOT_UPDATE_RESOLUTIONS,
  getSnapshotDirForAgent,
  checkAgentMemorySnapshot,
  SNAPSHOT_META_FILENAME,
  SYNCED_META_FILENAME,
} from '../src/agent-memory-snapshot';
import { getAgentMemoryDir } from '../src/memory';
import type { AgentMemoryScope } from '../src/skills';

const AGENT = 'planner';
const SCOPE: AgentMemoryScope = 'project';
const SNAP_TS = '2026-03-03T00:00:00Z';

describe('U290-w114 — agent-memory snapshot three-way update resolution', () => {
  let tmpDir: string;

  function snapDir(): string {
    return getSnapshotDirForAgent(AGENT, tmpDir);
  }
  function localDir(): string {
    return getAgentMemoryDir(AGENT, SCOPE, tmpDir);
  }
  function seedSnapshot(files: Record<string, string>, updatedAt = SNAP_TS): void {
    fs.mkdirSync(snapDir(), { recursive: true });
    fs.writeFileSync(path.join(snapDir(), SNAPSHOT_META_FILENAME), JSON.stringify({ updatedAt }));
    for (const [name, content] of Object.entries(files)) {
      fs.writeFileSync(path.join(snapDir(), name), content);
    }
  }
  function seedLocal(files: Record<string, string>): void {
    fs.mkdirSync(localDir(), { recursive: true });
    for (const [name, content] of Object.entries(files)) {
      fs.writeFileSync(path.join(localDir(), name), content);
    }
  }
  function syncedMarker(): string | null {
    try {
      return JSON.parse(fs.readFileSync(path.join(localDir(), SYNCED_META_FILENAME), 'utf-8')).syncedFrom;
    } catch {
      return null;
    }
  }

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-snap-res-u290-'));
    fs.mkdirSync(path.join(tmpDir, '.git'), { recursive: true });
  });
  afterEach(() => {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  // --- resolveSnapshotUpdateChoice ---------------------------------------

  it('resolveSnapshotUpdateChoice passes through each of the three valid outcomes', () => {
    for (const r of SNAPSHOT_UPDATE_RESOLUTIONS) {
      assert.equal(resolveSnapshotUpdateChoice(r), r);
    }
    assert.deepEqual([...SNAPSHOT_UPDATE_RESOLUTIONS], ['merge', 'keep', 'replace']);
  });

  it('resolveSnapshotUpdateChoice degrades cancel / garbage to the safe "keep" default', () => {
    assert.equal(DEFAULT_SNAPSHOT_UPDATE_RESOLUTION, 'keep');
    assert.equal(resolveSnapshotUpdateChoice(null), 'keep');
    assert.equal(resolveSnapshotUpdateChoice(undefined), 'keep');
    assert.equal(resolveSnapshotUpdateChoice('nonsense'), 'keep');
    assert.equal(resolveSnapshotUpdateChoice(42), 'keep');
    assert.equal(resolveSnapshotUpdateChoice({ choice: 'replace' }), 'keep');
  });

  // --- buildSnapshotMergePrompt ------------------------------------------

  it('buildSnapshotMergePrompt names the agent + scope and tells it to reconcile (not overwrite)', () => {
    const prompt = buildSnapshotMergePrompt(AGENT, SCOPE);
    assert.match(prompt, new RegExp(AGENT));
    assert.match(prompt, /project/);
    assert.match(prompt, /merge/i);
    assert.match(prompt, /not .*overwrite/i);
  });

  // --- applySnapshotUpdateResolution: replace ----------------------------

  it('"replace" discards local .md and takes the snapshot wholesale + marks synced', () => {
    seedSnapshot({ 'a.md': '# snapshot A' });
    seedLocal({ 'old.md': '# stale local', 'keep.txt': 'not markdown' });

    const out = applySnapshotUpdateResolution(AGENT, SCOPE, SNAP_TS, 'replace', tmpDir);
    assert.equal(out.resolution, 'replace');
    assert.equal(out.mergePrompt, undefined);

    // Snapshot content copied in, stale local .md removed (orphan cleanup),
    // non-.md local file left untouched.
    assert.equal(fs.readFileSync(path.join(localDir(), 'a.md'), 'utf-8'), '# snapshot A');
    assert.ok(!fs.existsSync(path.join(localDir(), 'old.md')));
    assert.ok(fs.existsSync(path.join(localDir(), 'keep.txt')));
    assert.equal(syncedMarker(), SNAP_TS);

    // After a replace, the snapshot check reports no further action.
    assert.equal(checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir).action, 'none');
  });

  // --- applySnapshotUpdateResolution: keep -------------------------------

  it('"keep" preserves local memory and only records the sync marker', () => {
    seedSnapshot({ 'a.md': '# snapshot A' });
    seedLocal({ 'local.md': '# my local notes' });

    const out = applySnapshotUpdateResolution(AGENT, SCOPE, SNAP_TS, 'keep', tmpDir);
    assert.equal(out.resolution, 'keep');
    assert.equal(out.mergePrompt, undefined);

    // Local memory untouched; snapshot content NOT copied in.
    assert.equal(fs.readFileSync(path.join(localDir(), 'local.md'), 'utf-8'), '# my local notes');
    assert.ok(!fs.existsSync(path.join(localDir(), 'a.md')));
    assert.equal(syncedMarker(), SNAP_TS);
    assert.equal(checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir).action, 'none');
  });

  // --- applySnapshotUpdateResolution: merge ------------------------------

  it('"merge" preserves local memory, marks synced, and returns a merge prompt', () => {
    seedSnapshot({ 'a.md': '# snapshot A' });
    seedLocal({ 'local.md': '# my local notes' });

    const out = applySnapshotUpdateResolution(AGENT, SCOPE, SNAP_TS, 'merge', tmpDir);
    assert.equal(out.resolution, 'merge');
    assert.equal(out.mergePrompt, buildSnapshotMergePrompt(AGENT, SCOPE));

    // Local memory preserved for the agent to reconcile; snapshot NOT copied in.
    assert.equal(fs.readFileSync(path.join(localDir(), 'local.md'), 'utf-8'), '# my local notes');
    assert.ok(!fs.existsSync(path.join(localDir(), 'a.md')));
    assert.equal(syncedMarker(), SNAP_TS);
    assert.equal(checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir).action, 'none');
  });

  // --- applySnapshotUpdateResolution: cancel safety ----------------------

  it('a cancelled/garbage choice degrades to the safe no-op "keep" (never replaces local)', () => {
    seedSnapshot({ 'a.md': '# snapshot A' });
    seedLocal({ 'local.md': '# precious local notes' });

    const out = applySnapshotUpdateResolution(AGENT, SCOPE, SNAP_TS, null, tmpDir);
    assert.equal(out.resolution, 'keep');
    assert.equal(out.mergePrompt, undefined);

    // Cancel must NOT destroy local memory.
    assert.equal(fs.readFileSync(path.join(localDir(), 'local.md'), 'utf-8'), '# precious local notes');
    assert.ok(!fs.existsSync(path.join(localDir(), 'a.md')));
    assert.equal(syncedMarker(), SNAP_TS);
  });
});
