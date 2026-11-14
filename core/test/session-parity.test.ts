import { describe, it, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execSync } from 'child_process';

import {
  SessionManager,
  resolveSessionTitle,
  acquireSessionLock,
  isSessionLocked,
  isPidAlive,
  getProcessCommand,
  releaseAllSessionLocks,
  SESSION_LOCK_FILE,
  LOCK_FALLBACK_STALE_MS,
  safeAppendTranscript,
  repairTranscriptFile,
  type Session,
} from '../src/session';
import { atomicWriteFileSync, secureMkdirSync, fsHooks } from '../src/utils/fs-atomic';
import { getGitBranch } from '../src/utils/git';
import { searchSessions } from '../src/session-search';
import { asSessionId } from '../src/types';
import { AMI_VERSION } from '../src/version';

const IS_WIN = process.platform === 'win32';
const IS_ROOT = typeof process.getuid === 'function' && process.getuid() === 0;
const tmps: string[] = [];
function tmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'session-parity-'));
  tmps.push(d);
  return d;
}
after(() => { for (const d of tmps) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } } });

function mode(p: string): number {
  return fs.statSync(p).mode & 0o777;
}

/** Initialize a git repo at `dir` on branch `branch` with one empty commit so
 *  `git rev-parse --abbrev-ref HEAD` resolves (an unborn branch does not). */
function initRepo(dir: string, branch: string): void {
  const env = { ...process.env, GIT_TERMINAL_PROMPT: '0' };
  execSync(`git init -q -b ${branch}`, { cwd: dir, env });
  execSync('git -c user.email=t@t.test -c user.name=t commit -q --allow-empty -m init', { cwd: dir, env });
}

function makeSession(id: string, over: Partial<Session> = {}): Session {
  return {
    id: asSessionId(id),
    messages: [{ role: 'user', content: 'hello world' }],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'm', baseUrl: 'http://localhost' },
    ...over,
  };
}

// ===========================================================================
// fs-atomic — shared crash-safe owner-only writer
// ===========================================================================
describe('fs-atomic: atomicWriteFileSync / secureMkdirSync', () => {
  it('writes the content and leaves no temp file behind', () => {
    const dir = tmpDir();
    const target = path.join(dir, 'sub', 'file.txt');
    atomicWriteFileSync(target, 'payload');
    assert.equal(fs.readFileSync(target, 'utf-8'), 'payload');
    // No `.tmp.` sidecar should survive a successful write.
    assert.ok(!fs.readdirSync(path.dirname(target)).some(f => f.includes('.tmp.')));
  });

  it('writes owner-only by default (0o600 file, 0o700 dir)', () => {
    if (IS_WIN) return;
    const dir = tmpDir();
    const target = path.join(dir, 'secure', 'secret.json');
    atomicWriteFileSync(target, '{"k":1}');
    assert.equal(mode(target) & 0o077, 0, 'no group/other access on file');
    assert.ok(mode(target) & 0o600, 'owner rw on file');
    assert.equal(mode(path.dirname(target)) & 0o077, 0, 'no group/other access on dir');
  });

  it('honors an explicit mode override', () => {
    if (IS_WIN) return;
    const dir = tmpDir();
    const target = path.join(dir, 'f.txt');
    atomicWriteFileSync(target, 'x', { mode: 0o644 });
    assert.equal(mode(target), 0o644);
  });

  it('falls back to a direct write when rename fails, cleaning the temp file', () => {
    const dir = tmpDir();
    const target = path.join(dir, 'fallback.txt');
    const m = mock.method(fsHooks, 'renameSync', () => { throw new Error('boom'); });
    try {
      atomicWriteFileSync(target, 'via-fallback');
    } finally {
      m.mock.restore();
    }
    assert.equal(fs.readFileSync(target, 'utf-8'), 'via-fallback');
    assert.ok(!fs.readdirSync(dir).some(f => f.includes('.tmp.')));
  });

  it('secureMkdirSync is idempotent and tightens an existing dir', () => {
    if (IS_WIN || IS_ROOT) return;
    const dir = tmpDir();
    const sub = path.join(dir, 'loose');
    fs.mkdirSync(sub, { mode: 0o755 });
    fs.chmodSync(sub, 0o755);
    secureMkdirSync(sub);
    assert.equal(mode(sub) & 0o077, 0, 'existing dir tightened to owner-only');
  });
});

// ===========================================================================
// utils/git — getGitBranch
// ===========================================================================
describe('getGitBranch', () => {
  it('returns undefined outside a git working tree', () => {
    assert.equal(getGitBranch(tmpDir()), undefined);
  });

  it('returns the current branch inside a git repo', () => {
    if (IS_WIN) return;
    const dir = tmpDir();
    initRepo(dir, 'feature-x');
    assert.equal(getGitBranch(dir), 'feature-x');
  });
});

// ===========================================================================
// save() — crash-safe + owner-only + provenance stamping
// ===========================================================================
describe('SessionManager.save — security + provenance', () => {
  it('writes the session JSON owner-only (0o600) in a 0o700 dir', () => {
    if (IS_WIN) return;
    const cwd = tmpDir();
    const sm = new SessionManager(cwd, path.join(tmpDir(), 'sessions'));
    sm.save(makeSession('s-perm'));
    const file = path.join(sm.getSessionDir(), 's-perm.json');
    assert.equal(mode(file) & 0o077, 0);
    assert.equal(mode(sm.getSessionDir()) & 0o077, 0);
  });

  it('writes offloaded blobs owner-only (0o600)', () => {
    if (IS_WIN) return;
    const sm = new SessionManager(tmpDir(), path.join(tmpDir(), 'sessions'));
    const big = 'x'.repeat(6000);
    sm.save(makeSession('s-blob', { messages: [{ role: 'user', content: big }] }));
    const blobDir = path.join(sm.getSessionDir(), 'blobs', 's-blob');
    const blob = fs.readdirSync(blobDir).find(f => f.endsWith('.blob'))!;
    assert.equal(mode(path.join(blobDir, blob)) & 0o077, 0);
    // Round-trips the full content back on load.
    assert.equal(sm.load('s-blob')!.messages[0].content, big);
  });

  it('stamps version + projectPath + gitBranch on save', () => {
    if (IS_WIN) return;
    const cwd = tmpDir();
    initRepo(cwd, 'mybranch');
    const sm = new SessionManager(cwd, path.join(tmpDir(), 'sessions'));
    sm.save(makeSession('s-prov'));
    const loaded = sm.load('s-prov')!;
    assert.equal(loaded.version, AMI_VERSION);
    assert.equal(loaded.projectPath, cwd);
    assert.equal(loaded.gitBranch, 'mybranch');
  });

  it('never clobbers a user customTitle with an aiTitle-only save', () => {
    const sm = new SessionManager(tmpDir(), path.join(tmpDir(), 'sessions'));
    sm.save(makeSession('s-title', { customTitle: 'User Named This' }));
    // A later save carrying only an aiTitle (as the engine does) must keep it.
    sm.save(makeSession('s-title', { aiTitle: 'auto generated' }));
    const loaded = sm.load('s-title')!;
    assert.equal(loaded.customTitle, 'User Named This');
    assert.equal(loaded.aiTitle, 'auto generated');
    assert.equal(resolveSessionTitle(loaded), 'User Named This');
  });

  it('resolveSessionTitle prefers customTitle, falls back to aiTitle', () => {
    assert.equal(resolveSessionTitle({ customTitle: 'a', aiTitle: 'b' }), 'a');
    assert.equal(resolveSessionTitle({ aiTitle: 'b' }), 'b');
    assert.equal(resolveSessionTitle({}), undefined);
  });

  it('is crash-safe: an interrupted temp write never corrupts the prior file', () => {
    const sm = new SessionManager(tmpDir(), path.join(tmpDir(), 'sessions'));
    sm.save(makeSession('s-crash', { messages: [{ role: 'user', content: 'good v1' }] }));
    const file = path.join(sm.getSessionDir(), 's-crash.json');
    const before = fs.readFileSync(file, 'utf-8');
    // Simulate a crash mid-rename: the temp write happened but rename/fallback fail.
    const mR = mock.method(fsHooks, 'renameSync', () => { throw new Error('crash'); });
    const realWrite = fsHooks.writeFileSync;
    let calls = 0;
    const mW = mock.method(fsHooks, 'writeFileSync', (...args: any[]) => {
      // Let the temp write happen, make the non-atomic fallback throw.
      calls++;
      if (calls >= 2) throw new Error('enospc');
      return (realWrite as any)(...args);
    });
    try {
      assert.throws(() => sm.save(makeSession('s-crash', { messages: [{ role: 'user', content: 'bad v2' }] })));
    } finally {
      mR.mock.restore();
      mW.mock.restore();
    }
    // The original intact file is untouched; no corrupt temp replaced it.
    assert.equal(fs.readFileSync(file, 'utf-8'), before);
  });
});

// ===========================================================================
// list() — richer metadata
// ===========================================================================
describe('SessionManager.list — metadata', () => {
  it('reports messageCount, fileSize, gitBranch and resolved title', () => {
    if (IS_WIN) return;
    const cwd = tmpDir();
    initRepo(cwd, 'listbranch');
    const sm = new SessionManager(cwd, path.join(tmpDir(), 'sessions'));
    sm.save(makeSession('l-a', {
      customTitle: 'Pinned Title',
      messages: [{ role: 'user', content: 'one' }, { role: 'assistant', content: 'two' }],
    }));
    const [entry] = sm.list();
    assert.equal(entry.messageCount, 2);
    assert.ok((entry.fileSize ?? 0) > 0);
    assert.equal(entry.gitBranch, 'listbranch');
    assert.equal(entry.title, 'Pinned Title');
  });
});

// ===========================================================================
// Lock hardening
// ===========================================================================
describe('getProcessCommand', () => {
  it('returns a command string for a live pid and null for sentinels', () => {
    if (IS_WIN) return;
    const cmd = getProcessCommand(process.pid);
    assert.equal(typeof cmd, 'string');
    assert.ok((cmd as string).length > 0);
    assert.equal(getProcessCommand(1), null);
    assert.equal(getProcessCommand(0), null);
  });
});

describe('acquireSessionLock — PID-reuse + TTL + race + bounded retry', () => {
  function writePayload(dir: string, p: { pid: number; startedAt?: number; execPath?: string }): void {
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, SESSION_LOCK_FILE), JSON.stringify({
      pid: p.pid, startedAt: p.startedAt ?? Date.now(), execPath: p.execPath ?? '/usr/bin/ami',
    }));
  }

  it('reclaims a lock whose PID is alive but is an unrelated program (PID reuse)', () => {
    const dir = tmpDir();
    writePayload(dir, { pid: 424242, execPath: '/usr/bin/ami' });
    const lock = acquireSessionLock(dir, {
      isPidAlive: () => true,
      getProcessCommand: () => '/usr/sbin/unrelated-daemon --serve',
    });
    assert.equal(lock.acquired, true);
    lock.release();
  });

  it('blocks when the live holder looks like one of ours (command contains ami)', () => {
    const dir = tmpDir();
    writePayload(dir, { pid: 424243 });
    const lock = acquireSessionLock(dir, {
      isPidAlive: () => true,
      getProcessCommand: () => 'node /opt/ami/dist/superinference.mjs',
    });
    assert.equal(lock.acquired, false);
    assert.equal(lock.pid, 424243);
  });

  it('time-based stale fallback reclaims an old, unverifiable lock', () => {
    const dir = tmpDir();
    writePayload(dir, { pid: 424244, startedAt: Date.now() - (LOCK_FALLBACK_STALE_MS + 1000) });
    const lock = acquireSessionLock(dir, {
      isPidAlive: () => true,
      getProcessCommand: () => null, // identity cannot be verified
    });
    assert.equal(lock.acquired, true);
    lock.release();
  });

  it('keeps a recent unverifiable lock held (within the TTL)', () => {
    const dir = tmpDir();
    writePayload(dir, { pid: 424245, startedAt: Date.now() });
    const lock = acquireSessionLock(dir, {
      isPidAlive: () => true,
      getProcessCommand: () => null,
    });
    assert.equal(lock.acquired, false);
    assert.equal(lock.pid, 424245);
  });

  it('detects a lost race via the post-write re-read (verify ownership)', () => {
    const dir = tmpDir();
    fs.mkdirSync(dir, { recursive: true });
    const mW = mock.method(fsHooks, 'writeFileSync', () => undefined); // pretend our wx write "won"
    const mR = mock.method(fsHooks, 'readFileSync', () =>
      JSON.stringify({ pid: 999999, startedAt: Date.now(), execPath: '/x' }));
    let result;
    try {
      result = acquireSessionLock(dir);
    } finally {
      mW.mock.restore();
      mR.mock.restore();
    }
    assert.equal(result.acquired, false);
    assert.equal(result.pid, 999999);
  });

  it('is bounded: a persistently re-created stale lock yields {acquired:false}, not infinite recursion', () => {
    const dir = tmpDir();
    fs.mkdirSync(dir, { recursive: true });
    const eexist = Object.assign(new Error('exists'), { code: 'EEXIST' });
    const mW = mock.method(fsHooks, 'writeFileSync', () => { throw eexist; });
    const mR = mock.method(fsHooks, 'readFileSync', () =>
      JSON.stringify({ pid: 321, startedAt: 0, execPath: '/x' }));
    const mU = mock.method(fsHooks, 'unlinkSync', () => undefined);
    let result;
    try {
      result = acquireSessionLock(dir, { isPidAlive: () => false }); // always stale
    } finally {
      mW.mock.restore();
      mR.mock.restore();
      mU.mock.restore();
    }
    assert.equal(result.acquired, false);
    assert.equal(result.pid, 321);
  });
});

describe('releaseAllSessionLocks — exit cleanup registry', () => {
  it('releases every lock this process still holds', () => {
    const a = tmpDir();
    const b = tmpDir();
    const la = acquireSessionLock(a);
    const lb = acquireSessionLock(b);
    assert.equal(la.acquired && lb.acquired, true);
    assert.ok(fs.existsSync(path.join(a, SESSION_LOCK_FILE)));
    releaseAllSessionLocks();
    assert.equal(fs.existsSync(path.join(a, SESSION_LOCK_FILE)), false);
    assert.equal(fs.existsSync(path.join(b, SESSION_LOCK_FILE)), false);
    // Calling the handles afterwards is a safe no-op.
    assert.doesNotThrow(() => { la.release(); lb.release(); });
  });
});

describe('isSessionLocked — reuse-aware', () => {
  it('reports false when the live holder is an unrelated program (reused PID)', () => {
    const dir = tmpDir();
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, SESSION_LOCK_FILE), JSON.stringify({
      pid: 777, startedAt: Date.now(), execPath: '/usr/bin/ami',
    }));
    assert.equal(isSessionLocked(dir, { isPidAlive: () => true, getProcessCommand: () => '/bin/sleep 999' }), false);
  });
});

// ===========================================================================
// Transcript — concurrency-safe append + flush + perms, repair perms
// ===========================================================================
describe('safeAppendTranscript / repairTranscriptFile', () => {
  it('appends entries durably and owner-only', () => {
    if (IS_WIN) return;
    const dir = tmpDir();
    const file = path.join(dir, 'sub', 'transcript.jsonl');
    safeAppendTranscript(file, { type: 'a', timestamp: 1 });
    safeAppendTranscript(file, { type: 'b', timestamp: 2 });
    const lines = fs.readFileSync(file, 'utf-8').trim().split('\n');
    assert.equal(lines.length, 2);
    assert.equal(JSON.parse(lines[0]).type, 'a');
    assert.equal(mode(file) & 0o077, 0);
    // The advisory lock sidecar is released (not left behind).
    assert.ok(!fs.existsSync(file + '.lock'));
  });

  it('serializes concurrent-ish appends without tearing a line', () => {
    const dir = tmpDir();
    const file = path.join(dir, 't.jsonl');
    for (let i = 0; i < 50; i++) safeAppendTranscript(file, { type: 'x', timestamp: i });
    const lines = fs.readFileSync(file, 'utf-8').trim().split('\n');
    assert.equal(lines.length, 50);
    for (const l of lines) assert.doesNotThrow(() => JSON.parse(l));
  });

  it('repairTranscriptFile rewrites the repaired file owner-only', () => {
    if (IS_WIN) return;
    const dir = tmpDir();
    const file = path.join(dir, 'r.jsonl');
    fs.writeFileSync(file, JSON.stringify({ type: 'ok', timestamp: 1 }) + '\n' + '{bad json');
    const res = repairTranscriptFile(file);
    assert.equal(res.droppedLines, 1);
    assert.equal(res.repairedLines, 1);
    assert.equal(mode(file) & 0o077, 0);
  });
});

// ===========================================================================
// session-search — tag / title searchable
// ===========================================================================
describe('searchSessions — tag + title', () => {
  it('matches a session by its user tag even when no message body matches', () => {
    const cwd = tmpDir();
    const sm = new SessionManager(cwd);
    sm.save(makeSession('tagged', {
      tag: 'quarterly-report',
      messages: [{ role: 'user', content: 'totally unrelated body' }],
    }));
    const results = searchSessions(cwd, 'quarterly-report');
    assert.ok(results.some(r => r.sessionId === 'tagged' && r.role === 'meta'));
  });

  it('matches a session by its title', () => {
    const cwd = tmpDir();
    const sm = new SessionManager(cwd);
    sm.save(makeSession('titled', {
      customTitle: 'Migration Plan',
      messages: [{ role: 'user', content: 'nothing here' }],
    }));
    const results = searchSessions(cwd, 'migration plan');
    assert.ok(results.some(r => r.sessionId === 'titled'));
  });
});
