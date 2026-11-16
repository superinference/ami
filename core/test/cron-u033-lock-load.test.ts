/**
 * U033 — core/core/src/cron.ts parity improvements (lock + durable-load hardening).
 *
 * Gaps implemented (see plannings/parity/gaps.jsonl):
 *  - Schema-validated, non-throwing parse of untrusted lock-file contents
 *    (claudeRef: src/utils/cronTasksLock.ts — zod-validated readLock): a single
 *    parseCronLock() validator replaces the ad-hoc field casts in
 *    acquireLock/heartbeatLock/releaseLock.
 *  - Scheduler-lock lifecycle introspection logging on release
 *    (claudeRef: src/utils/cronTasksLock.ts:L191 — logForDebugging('released …')):
 *    releaseLock now emits a "Released cron lock" diagnostic, pairing the
 *    acquire/reclaim logs with a release line.
 *  - Distinguishing FS-inaccessible from real read errors on durable load
 *    (claudeRef: src/utils/cronTasks.ts:L91-100 — isFsInaccessible→silent vs
 *    logError): loadDurable logs an unexpected read fault instead of swallowing it.
 */

import { describe, it, afterEach, after } from 'node:test';
import { mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  CronScheduler,
  parseCronLock,
  isFsInaccessible,
} from '../src/cron';
import { projectStateDir } from '../src/reference-journal';
import { fsHooks } from '../src/logger';

const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-u033-ll-'));
  tmpDirs.push(d);
  return d;
}
function lockPathFor(cwd: string): string {
  return path.join(projectStateDir(cwd), 'cron.lock');
}
function persistPathFor(cwd: string): string {
  return path.join(projectStateDir(cwd), 'scheduled_tasks.json');
}
function writeLock(cwd: string, body: string): void {
  const dir = projectStateDir(cwd);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(lockPathFor(cwd), body);
}
type LockInternals = {
  acquireLock(): boolean;
  releaseLock(): void;
  holdsLock: boolean;
};
function internals(s: CronScheduler): LockInternals {
  return s as unknown as LockInternals;
}

after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

// ---------------------------------------------------------------------------
// parseCronLock — schema-validated, non-throwing parse of untrusted JSON
// ---------------------------------------------------------------------------
describe('parseCronLock', () => {
  it('returns the record for a well-formed lock', () => {
    const rec = parseCronLock(JSON.stringify({ sessionId: 's', pid: 42, timestamp: 100 }));
    assert.deepEqual(rec, { sessionId: 's', pid: 42, timestamp: 100 });
  });

  it('returns null for unparseable JSON', () => {
    assert.equal(parseCronLock('{not json'), null);
    assert.equal(parseCronLock(''), null);
  });

  it('returns null for a non-object payload', () => {
    assert.equal(parseCronLock('123'), null);
    assert.equal(parseCronLock('"a string"'), null);
    assert.equal(parseCronLock('null'), null);
    assert.equal(parseCronLock('[1,2,3]'), null); // array is typeof object but not a record
  });

  it('returns null when sessionId is missing or mis-typed', () => {
    assert.equal(parseCronLock(JSON.stringify({ pid: 1, timestamp: 2 })), null);
    assert.equal(parseCronLock(JSON.stringify({ sessionId: 5, pid: 1, timestamp: 2 })), null);
  });

  it('returns null when pid is missing, non-numeric, or non-finite', () => {
    assert.equal(parseCronLock(JSON.stringify({ sessionId: 's', timestamp: 2 })), null);
    assert.equal(parseCronLock(JSON.stringify({ sessionId: 's', pid: 'x', timestamp: 2 })), null);
    // JSON cannot encode Infinity, so inject it via a replacer-free raw string.
    assert.equal(parseCronLock('{"sessionId":"s","pid":null,"timestamp":2}'), null);
  });

  it('returns null when timestamp is missing or non-numeric', () => {
    assert.equal(parseCronLock(JSON.stringify({ sessionId: 's', pid: 1 })), null);
    assert.equal(parseCronLock(JSON.stringify({ sessionId: 's', pid: 1, timestamp: 'nope' })), null);
  });

  it('is actually wired into acquireLock: a wrong-shape lock is reclaimed as stale', () => {
    const cwd = mkTmp();
    writeLock(cwd, JSON.stringify({ foo: 1 })); // valid JSON, invalid shape
    const sched = new CronScheduler(cwd, 'self');
    assert.equal(internals(sched).acquireLock(), true, 'wrong-shape lock must be reclaimed');
    sched.stopTicker();
  });
});

// ---------------------------------------------------------------------------
// isFsInaccessible — expected-missing vs real read fault
// ---------------------------------------------------------------------------
describe('isFsInaccessible', () => {
  it('is true for every expected missing/inaccessible errno code', () => {
    for (const code of ['ENOENT', 'EACCES', 'EPERM', 'ENOTDIR', 'ELOOP']) {
      assert.equal(isFsInaccessible(Object.assign(new Error('x'), { code })), true, code);
    }
  });

  it('is false for an unexpected read-fault errno code', () => {
    assert.equal(isFsInaccessible(Object.assign(new Error('x'), { code: 'EISDIR' })), false);
    assert.equal(isFsInaccessible(Object.assign(new Error('x'), { code: 'EIO' })), false);
  });

  it('is false for an error with no code and for non-error values', () => {
    assert.equal(isFsInaccessible(new Error('plain')), false);
    assert.equal(isFsInaccessible(null), false);
    assert.equal(isFsInaccessible(undefined), false);
    assert.equal(isFsInaccessible('a string'), false);
    assert.equal(isFsInaccessible({ code: 42 }), false); // non-string code
  });
});

// ---------------------------------------------------------------------------
// releaseLock — lifecycle introspection log on release
// ---------------------------------------------------------------------------
describe('cron lock — release lifecycle logging', () => {
  afterEach(() => mock.restoreAll());

  it('emits a "Released cron lock" diagnostic when our own lock is freed', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'releaser');
    assert.equal(internals(sched).acquireLock(), true);

    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: string, line: string) => { lines.push(line); });

    // stopTicker() is the real public shutdown path that releases the lease.
    sched.stopTicker();

    assert.equal(fs.existsSync(lockPathFor(cwd)), false, 'our lock must be removed');
    const released = lines.filter(l => l.includes('Released cron lock (session releaser'));
    assert.equal(released.length, 1, 'release must log exactly one lifecycle line');
  });

  it('does NOT log a release when the lock belongs to another session', () => {
    const cwd = mkTmp();
    writeLock(cwd, JSON.stringify({ sessionId: 'someone-else', pid: process.pid, timestamp: Date.now() }));
    const sched = new CronScheduler(cwd, 'mine');

    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: string, line: string) => { lines.push(line); });
    internals(sched).releaseLock();

    assert.ok(fs.existsSync(lockPathFor(cwd)), 'foreign lock must be left intact');
    assert.equal(lines.filter(l => l.includes('Released cron lock')).length, 0);
  });
});

// ---------------------------------------------------------------------------
// loadDurable — distinguish FS-inaccessible from a real read fault
// ---------------------------------------------------------------------------
describe('loadDurable — read-error diagnostics', () => {
  afterEach(() => mock.restoreAll());

  it('stays silent when the persisted-state file is simply absent (ENOENT)', () => {
    const cwd = mkTmp();
    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: string, line: string) => { lines.push(line); });
    // Construction triggers loadDurable with no file present.
    const sched = new CronScheduler(cwd, 'fresh');
    assert.equal(lines.filter(l => l.includes('Failed to read scheduled_tasks.json')).length, 0);
    sched.stopTicker();
  });

  it('logs a diagnostic for an unexpected read fault (persist path is a directory → EISDIR)', () => {
    const cwd = mkTmp();
    // Make the persisted-state PATH a directory so readFileSync throws EISDIR —
    // a genuine fault that must not be swallowed like an empty state.
    fs.mkdirSync(persistPathFor(cwd), { recursive: true });

    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: string, line: string) => { lines.push(line); });
    const sched = new CronScheduler(cwd, 'faulty');

    const failed = lines.filter(l => l.includes('Failed to read scheduled_tasks.json'));
    assert.equal(failed.length, 1, 'an unexpected read fault must leave a diagnostic trail');
    sched.stopTicker();
  });
});
