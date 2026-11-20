import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  AUTO_DREAM_DEFAULTS,
  resolveAutoDreamConfig,
  consolidationLockPath,
  readLastConsolidatedAt,
  tryAcquireConsolidationLock,
  rollbackConsolidationLock,
  recordConsolidation,
  isConsolidationDue,
  buildConsolidationPrompt,
} from '../src/memory-consolidation';
import {
  validateSnapshotMeta,
  validateSyncedMeta,
  readJsonFile,
  getSnapshotDirForAgent,
  checkAgentMemorySnapshot,
  writeAgentSnapshotMeta,
  saveAgentSyncedMeta,
  SNAPSHOT_META_FILENAME,
  SYNCED_META_FILENAME,
} from '../src/agent-memory-snapshot';
import { getAgentMemoryDir } from '../src/memory';

/**
 * U016 / wave-75 — background memory-consolidation primitives
 * (AutoDreamConfig + defaults/validation, mtime-is-timestamp consolidation lock,
 * due-gate, consolidation-prompt builder) and per-agent memory snapshot
 * staleness detection (schema-validated corruption-tolerant JSON read +
 * three-state drift algorithm). Parity with the reference's
 * services/autoDream/{autoDream,consolidationLock,consolidationPrompt}.ts and
 * tools/AgentTool/agentMemorySnapshot.ts.
 */

let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mem-w75-'));
  // A `.git` dir makes canonicalGitRoot resolve deterministically to tmpDir.
  fs.mkdirSync(path.join(tmpDir, '.git'), { recursive: true });
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('AutoDreamConfig resolution (gap: scheduling knobs + defaults + validation)', () => {
  it('uses typed defaults when raw is absent/garbage', () => {
    assert.deepEqual(AUTO_DREAM_DEFAULTS, { minHours: 24, minSessions: 5 });
    assert.deepEqual(resolveAutoDreamConfig(undefined), AUTO_DREAM_DEFAULTS);
    assert.deepEqual(resolveAutoDreamConfig(null), AUTO_DREAM_DEFAULTS);
    assert.deepEqual(resolveAutoDreamConfig('nope'), AUTO_DREAM_DEFAULTS);
    assert.deepEqual(resolveAutoDreamConfig(42), AUTO_DREAM_DEFAULTS);
  });

  it('accepts valid per-field overrides', () => {
    assert.deepEqual(resolveAutoDreamConfig({ minHours: 6, minSessions: 2 }), { minHours: 6, minSessions: 2 });
  });

  it('rejects non-finite / non-positive / wrong-type fields per-field', () => {
    assert.deepEqual(resolveAutoDreamConfig({ minHours: 0, minSessions: 3 }), { minHours: 24, minSessions: 3 });
    assert.deepEqual(resolveAutoDreamConfig({ minHours: -1, minSessions: 3 }), { minHours: 24, minSessions: 3 });
    assert.deepEqual(resolveAutoDreamConfig({ minHours: Infinity, minSessions: 3 }), { minHours: 24, minSessions: 3 });
    assert.deepEqual(resolveAutoDreamConfig({ minHours: NaN, minSessions: 3 }), { minHours: 24, minSessions: 3 });
    assert.deepEqual(resolveAutoDreamConfig({ minHours: 6, minSessions: '7' }), { minHours: 6, minSessions: 5 });
  });
});

describe('consolidation lock (mtime IS lastConsolidatedAt; body is PID)', () => {
  it('readLastConsolidatedAt returns 0 when no lock exists', () => {
    assert.equal(readLastConsolidatedAt(tmpDir), 0);
  });

  it('acquire creates the lock, writes our PID, and stamps mtime', () => {
    const prior = tryAcquireConsolidationLock(tmpDir);
    assert.equal(prior, 0); // no prior lock
    const p = consolidationLockPath(tmpDir);
    assert.ok(fs.existsSync(p));
    assert.equal(fs.readFileSync(p, 'utf-8').trim(), String(process.pid));
    assert.ok(readLastConsolidatedAt(tmpDir) > 0);
  });

  it('blocks when a FRESH lock is held by a live PID (this process)', () => {
    tryAcquireConsolidationLock(tmpDir);
    // Our own PID is live, lock is fresh → a second acquire is blocked.
    assert.equal(tryAcquireConsolidationLock(tmpDir), null);
  });

  it('reclaims a lock held by a DEAD pid even when fresh', () => {
    const p = consolidationLockPath(tmpDir);
    fs.writeFileSync(p, '999999999'); // implausible, dead PID
    const prior = tryAcquireConsolidationLock(tmpDir);
    assert.notEqual(prior, null);
    assert.equal(fs.readFileSync(p, 'utf-8').trim(), String(process.pid));
  });

  it('reclaims a STALE lock even if the PID is live', () => {
    const p = consolidationLockPath(tmpDir);
    fs.writeFileSync(p, String(process.pid));
    // now far in the future so the existing lock is older than HOLDER_STALE_MS.
    const future = Date.now() + 2 * 60 * 60 * 1000;
    const prior = tryAcquireConsolidationLock(tmpDir, future);
    assert.notEqual(prior, null);
  });

  it('reclaims a lock with an unparseable PID body', () => {
    fs.writeFileSync(consolidationLockPath(tmpDir), 'not-a-pid');
    assert.notEqual(tryAcquireConsolidationLock(tmpDir), null);
  });

  it('rollback to 0 removes the lock; rollback to a prior mtime rewinds it', () => {
    // Pre-existing lock from a dead PID at a known mtime.
    const p = consolidationLockPath(tmpDir);
    fs.writeFileSync(p, '999999999');
    const priorMtime = fs.statSync(p).mtimeMs;
    const got = tryAcquireConsolidationLock(tmpDir);
    assert.equal(got, priorMtime);
    rollbackConsolidationLock(tmpDir, priorMtime);
    assert.equal(Math.round(readLastConsolidatedAt(tmpDir) / 1000), Math.round(priorMtime / 1000));
    assert.equal(fs.readFileSync(p, 'utf-8'), '');

    // rollback to 0 removes the file entirely.
    rollbackConsolidationLock(tmpDir, 0);
    assert.ok(!fs.existsSync(p));
  });

  it('recordConsolidation stamps even with no prior lock and creates the dir', () => {
    const sub = path.join(tmpDir, 'nested', 'memory');
    recordConsolidation(sub);
    assert.ok(fs.existsSync(consolidationLockPath(sub)));
    assert.ok(readLastConsolidatedAt(sub) > 0);
  });
});

describe('isConsolidationDue (two-gate: hours AND sessions)', () => {
  it('fires when never consolidated and enough sessions accumulated', () => {
    const g = isConsolidationDue({ lockDir: tmpDir, sessionCount: 5 });
    assert.equal(g.due, true);
    assert.equal(g.hoursSince, Infinity);
    assert.equal(g.lastConsolidatedAt, 0);
    assert.deepEqual(g.config, AUTO_DREAM_DEFAULTS);
  });

  it('does NOT fire when the session gate is not met', () => {
    const g = isConsolidationDue({ lockDir: tmpDir, sessionCount: 4 });
    assert.equal(g.due, false);
  });

  it('does NOT fire when the time gate is not met (recently consolidated)', () => {
    recordConsolidation(tmpDir);
    const g = isConsolidationDue({ lockDir: tmpDir, sessionCount: 100 });
    assert.equal(g.due, false);
    assert.ok(g.hoursSince < 24);
  });

  it('fires again once enough hours elapse (now override)', () => {
    recordConsolidation(tmpDir);
    const last = readLastConsolidatedAt(tmpDir);
    const g = isConsolidationDue({ lockDir: tmpDir, sessionCount: 5, now: last + 25 * 60 * 60 * 1000 });
    assert.equal(g.due, true);
  });

  it('honors validated config overrides', () => {
    recordConsolidation(tmpDir);
    const last = readLastConsolidatedAt(tmpDir);
    const g = isConsolidationDue({
      lockDir: tmpDir,
      sessionCount: 2,
      rawConfig: { minHours: 1, minSessions: 2 },
      now: last + 2 * 60 * 60 * 1000,
    });
    assert.equal(g.due, true);
    assert.deepEqual(g.config, { minHours: 1, minSessions: 2 });
  });
});

describe('buildConsolidationPrompt (parameterized: memoryRoot, transcriptDir, extra)', () => {
  it('embeds both paths and omits the extra section by default', () => {
    const p = buildConsolidationPrompt('/mem/root', '/t/dir');
    assert.ok(p.includes('/mem/root'));
    assert.ok(p.includes('/t/dir'));
    assert.ok(p.includes('MEMORY.md'));
    assert.ok(p.includes('200 lines'));
    assert.ok(!p.includes('## Additional context'));
  });

  it('appends the optional extra additional-context block', () => {
    const p = buildConsolidationPrompt('/m', '/t', 'focus on the build failure');
    assert.ok(p.includes('## Additional context'));
    assert.ok(p.includes('focus on the build failure'));
  });
});

describe('agent-memory snapshot metadata validators', () => {
  it('validateSnapshotMeta accepts a non-empty updatedAt, rejects everything else', () => {
    assert.deepEqual(validateSnapshotMeta({ updatedAt: '2026-01-01' }), { updatedAt: '2026-01-01' });
    assert.equal(validateSnapshotMeta({ updatedAt: '' }), null);
    assert.equal(validateSnapshotMeta({ updatedAt: 5 }), null);
    assert.equal(validateSnapshotMeta({}), null);
    assert.equal(validateSnapshotMeta(null), null);
    assert.equal(validateSnapshotMeta('x'), null);
  });

  it('validateSyncedMeta accepts a non-empty syncedFrom, rejects everything else', () => {
    assert.deepEqual(validateSyncedMeta({ syncedFrom: 'ts' }), { syncedFrom: 'ts' });
    assert.equal(validateSyncedMeta({ syncedFrom: '' }), null);
    assert.equal(validateSyncedMeta({}), null);
    assert.equal(validateSyncedMeta(undefined), null);
  });
});

describe('readJsonFile (corruption-tolerant, schema-validated)', () => {
  it('returns null for a missing file', () => {
    assert.equal(readJsonFile(path.join(tmpDir, 'nope.json'), validateSnapshotMeta), null);
  });

  it('returns null for malformed JSON', () => {
    const f = path.join(tmpDir, 'bad.json');
    fs.writeFileSync(f, '{not json');
    assert.equal(readJsonFile(f, validateSnapshotMeta), null);
  });

  it('returns null for schema-mismatched JSON', () => {
    const f = path.join(tmpDir, 'mismatch.json');
    fs.writeFileSync(f, JSON.stringify({ other: 1 }));
    assert.equal(readJsonFile(f, validateSnapshotMeta), null);
  });

  it('returns the validated value for a good file', () => {
    const f = path.join(tmpDir, 'ok.json');
    fs.writeFileSync(f, JSON.stringify({ updatedAt: '2026-05-05' }));
    assert.deepEqual(readJsonFile(f, validateSnapshotMeta), { updatedAt: '2026-05-05' });
  });
});

describe('checkAgentMemorySnapshot (three-state drift detection)', () => {
  const AGENT = 'my-agent';
  const SCOPE = 'project' as const;

  it('returns none when no snapshot.json exists', () => {
    assert.deepEqual(checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir), { action: 'none' });
  });

  it('returns none when snapshot.json is corrupt', () => {
    const dir = getSnapshotDirForAgent(AGENT, tmpDir);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, SNAPSHOT_META_FILENAME), '{broken');
    assert.deepEqual(checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir), { action: 'none' });
  });

  it('returns initialize when a snapshot exists but no local .md memory', () => {
    writeAgentSnapshotMeta(AGENT, tmpDir, '2026-01-02T00:00:00Z');
    const r = checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir);
    assert.equal(r.action, 'initialize');
    assert.equal(r.snapshotTimestamp, '2026-01-02T00:00:00Z');
  });

  it('returns prompt-update when local memory exists but was never synced', () => {
    writeAgentSnapshotMeta(AGENT, tmpDir, '2026-01-02T00:00:00Z');
    const localDir = getAgentMemoryDir(AGENT, SCOPE, tmpDir);
    fs.mkdirSync(localDir, { recursive: true });
    fs.writeFileSync(path.join(localDir, 'learnings.md'), '# notes');
    const r = checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir);
    assert.equal(r.action, 'prompt-update');
    assert.equal(r.snapshotTimestamp, '2026-01-02T00:00:00Z');
  });

  it('returns prompt-update when the snapshot is NEWER than the synced marker', () => {
    writeAgentSnapshotMeta(AGENT, tmpDir, '2026-02-01T00:00:00Z');
    const localDir = getAgentMemoryDir(AGENT, SCOPE, tmpDir);
    fs.mkdirSync(localDir, { recursive: true });
    fs.writeFileSync(path.join(localDir, 'x.md'), '# notes');
    saveAgentSyncedMeta(AGENT, SCOPE, tmpDir, '2026-01-01T00:00:00Z');
    assert.equal(checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir).action, 'prompt-update');
  });

  it('returns none when already synced to the latest snapshot', () => {
    const ts = '2026-03-03T00:00:00Z';
    writeAgentSnapshotMeta(AGENT, tmpDir, ts);
    const localDir = getAgentMemoryDir(AGENT, SCOPE, tmpDir);
    fs.mkdirSync(localDir, { recursive: true });
    fs.writeFileSync(path.join(localDir, 'x.md'), '# notes');
    saveAgentSyncedMeta(AGENT, SCOPE, tmpDir, ts);
    assert.deepEqual(checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir), { action: 'none' });
    // the synced marker lives in the local agent-memory dir.
    assert.ok(fs.existsSync(path.join(localDir, SYNCED_META_FILENAME)));
  });
});
