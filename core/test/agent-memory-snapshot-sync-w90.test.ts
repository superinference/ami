/**
 * U309 (wave 90) — agent-memory snapshot ACT/sync operations (parity with the reference
 * Code's tools/AgentTool/agentMemorySnapshot.ts). The detection half
 * (`checkAgentMemorySnapshot`) already shipped; this adds the counterpart that
 * actually performs the sync the check decides on:
 *
 *   - copySnapshotToLocal  — mirror snapshot content files into local memory,
 *     SKIPPING the metadata file and any non-regular-file dirent (symlink-skip),
 *     best-effort on I/O failure (ref L56-77).
 *   - initializeFromSnapshot — first-time seed: copy + save synced marker (ref L149-159).
 *   - replaceFromSnapshot  — full replace: delete stale local .md (orphan cleanup,
 *     scoped to .md only) then copy + save synced marker (ref L164-186).
 *
 * Run: cd core/core && node --import tsx --test test/agent-memory-snapshot-sync-w90.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  copySnapshotToLocal,
  initializeFromSnapshot,
  replaceFromSnapshot,
  checkAgentMemorySnapshot,
  getSnapshotDirForAgent,
  SNAPSHOT_META_FILENAME,
  SYNCED_META_FILENAME,
} from '../src/agent-memory-snapshot';
import { getAgentMemoryDir } from '../src/memory';

const AGENT = 'my-agent';
const SCOPE = 'project' as const;

describe('U309-w90 — agent-memory snapshot sync (copy/initialize/replace)', () => {
  let tmpDir: string;

  function snapDir(): string {
    return getSnapshotDirForAgent(AGENT, tmpDir);
  }
  function localDir(): string {
    return getAgentMemoryDir(AGENT, SCOPE, tmpDir);
  }
  function seedSnapshot(files: Record<string, string>, updatedAt = '2026-02-02T00:00:00Z'): void {
    fs.mkdirSync(snapDir(), { recursive: true });
    fs.writeFileSync(path.join(snapDir(), SNAPSHOT_META_FILENAME), JSON.stringify({ updatedAt }));
    for (const [name, content] of Object.entries(files)) {
      fs.writeFileSync(path.join(snapDir(), name), content);
    }
  }

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-snap-sync90-'));
    fs.mkdirSync(path.join(tmpDir, '.git'), { recursive: true });
  });
  afterEach(() => {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('copySnapshotToLocal copies content files but SKIPS the metadata file', () => {
    seedSnapshot({ 'a.md': '# A', 'b.md': '# B' });
    copySnapshotToLocal(AGENT, SCOPE, tmpDir);
    assert.equal(fs.readFileSync(path.join(localDir(), 'a.md'), 'utf-8'), '# A');
    assert.equal(fs.readFileSync(path.join(localDir(), 'b.md'), 'utf-8'), '# B');
    // The snapshot metadata file is NOT mirrored into local memory.
    assert.ok(!fs.existsSync(path.join(localDir(), SNAPSHOT_META_FILENAME)));
  });

  it('copySnapshotToLocal skips non-regular-file dirents (a planted symlink is never copied)', () => {
    seedSnapshot({ 'real.md': '# real' });
    // Plant a symlink in the snapshot dir pointing at a secret outside it.
    const secret = path.join(tmpDir, 'secret.md');
    fs.writeFileSync(secret, 'TOP SECRET');
    fs.symlinkSync(secret, path.join(snapDir(), 'link.md'));
    copySnapshotToLocal(AGENT, SCOPE, tmpDir);
    assert.equal(fs.readFileSync(path.join(localDir(), 'real.md'), 'utf-8'), '# real');
    // The symlink was skipped — not dereferenced/copied into local memory.
    assert.ok(!fs.existsSync(path.join(localDir(), 'link.md')), 'symlink must not be copied');
  });

  it('copySnapshotToLocal is best-effort: a missing snapshot dir does not throw', () => {
    // No snapshot seeded at all.
    assert.doesNotThrow(() => copySnapshotToLocal(AGENT, SCOPE, tmpDir));
    // Local dir is created but stays empty.
    assert.deepEqual(fs.readdirSync(localDir()), []);
  });

  it('initializeFromSnapshot seeds local memory AND writes a synced marker (check then returns none)', () => {
    seedSnapshot({ 'notes.md': '# notes' }, '2026-03-03T00:00:00Z');
    // Before: check says initialize (snapshot present, no local memory).
    assert.equal(checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir).action, 'initialize');
    initializeFromSnapshot(AGENT, SCOPE, tmpDir, '2026-03-03T00:00:00Z');
    assert.equal(fs.readFileSync(path.join(localDir(), 'notes.md'), 'utf-8'), '# notes');
    assert.equal(
      JSON.parse(fs.readFileSync(path.join(localDir(), SYNCED_META_FILENAME), 'utf-8')).syncedFrom,
      '2026-03-03T00:00:00Z',
    );
    // After: synced to the latest snapshot → no action.
    assert.equal(checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir).action, 'none');
  });

  it('replaceFromSnapshot removes stale local .md orphans before copying, scoped to .md only', () => {
    seedSnapshot({ 'keep.md': '# new keep' }, '2026-04-04T00:00:00Z');
    // Seed local memory with a stale orphan .md and a non-.md file that must survive.
    fs.mkdirSync(localDir(), { recursive: true });
    fs.writeFileSync(path.join(localDir(), 'orphan.md'), '# stale');
    fs.writeFileSync(path.join(localDir(), 'data.json'), '{"x":1}');
    replaceFromSnapshot(AGENT, SCOPE, tmpDir, '2026-04-04T00:00:00Z');
    // Orphan .md removed; snapshot .md copied in.
    assert.ok(!fs.existsSync(path.join(localDir(), 'orphan.md')), 'stale orphan .md removed');
    assert.equal(fs.readFileSync(path.join(localDir(), 'keep.md'), 'utf-8'), '# new keep');
    // Non-.md local file untouched (deletion is scoped strictly to .md).
    assert.equal(fs.readFileSync(path.join(localDir(), 'data.json'), 'utf-8'), '{"x":1}');
    assert.equal(checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir).action, 'none');
  });

  it('replaceFromSnapshot tolerates a not-yet-existing local memory dir', () => {
    seedSnapshot({ 'x.md': '# x' }, '2026-05-05T00:00:00Z');
    assert.doesNotThrow(() => replaceFromSnapshot(AGENT, SCOPE, tmpDir, '2026-05-05T00:00:00Z'));
    assert.equal(fs.readFileSync(path.join(localDir(), 'x.md'), 'utf-8'), '# x');
  });

  it('sync operations emit no-PII introspection diagnostics (action/scope only, no paths)', () => {
    const diagFile = path.join(tmpDir, 'diag.jsonl');
    const saved = process.env.AMI_DIAGNOSTICS_FILE;
    process.env.AMI_DIAGNOSTICS_FILE = diagFile;
    try {
      seedSnapshot({ 'n.md': '# n' }, '2026-06-06T00:00:00Z');
      initializeFromSnapshot(AGENT, SCOPE, tmpDir, '2026-06-06T00:00:00Z');
      const raw = fs.readFileSync(diagFile, 'utf-8');
      const actions = raw
        .split('\n')
        .filter(Boolean)
        .map((l) => JSON.parse(l) as Record<string, unknown>)
        .filter((r) => r.event === 'agent_memory_snapshot')
        .map((r) => (r.data as Record<string, unknown>).action as string);
      assert.ok(actions.includes('initialize-sync'), 'initialize emits an initialize-sync diagnostic');
      assert.ok(actions.includes('synced'), 'saving the marker emits a synced diagnostic');
      assert.ok(!raw.includes(tmpDir), 'no filesystem path leaks into the no-PII channel');
    } finally {
      if (saved === undefined) delete process.env.AMI_DIAGNOSTICS_FILE;
      else process.env.AMI_DIAGNOSTICS_FILE = saved;
    }
  });
});
