import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  SESSION_LOCK_FILE,
  LOCK_FALLBACK_STALE_MS,
  acquireSessionLock,
  cleanupStaleLocks,
  parseSessionIdentifier,
  deriveFirstPrompt,
} from '../src/session';
import type { Message } from '../src/types';

function tmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'sess-u021-sweep-'));
  after(() => fs.rmSync(d, { recursive: true, force: true }));
  return d;
}

function writeLock(dir: string, payload: unknown): void {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, SESSION_LOCK_FILE), JSON.stringify(payload));
}

// ---------------------------------------------------------------------------
// U021 — cleanupStaleLocks: bulk stale/legacy lock sweep across session dirs
// ---------------------------------------------------------------------------
describe('cleanupStaleLocks', () => {
  it('unlinks a dead-holder lock and counts it', () => {
    const dir = tmpDir();
    writeLock(dir, { pid: 999999, startedAt: Date.now(), execPath: '/x/ami' });
    const cleaned = cleanupStaleLocks([dir], { isPidAlive: () => false, isWsl: () => false });
    assert.equal(cleaned, 1);
    assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), false);
  });

  it('unlinks a corrupt/unparseable lock', () => {
    const dir = tmpDir();
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, SESSION_LOCK_FILE), 'not-json-at-all');
    const cleaned = cleanupStaleLocks([dir]);
    assert.equal(cleaned, 1);
    assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), false);
  });

  it('preserves a lock with a live, verified holder', () => {
    const dir = tmpDir();
    writeLock(dir, { pid: 4242, startedAt: Date.now(), execPath: '/opt/ami' });
    const cleaned = cleanupStaleLocks([dir], {
      isPidAlive: () => true,
      getProcessCommand: () => '/opt/ami --prompt',
      isWsl: () => false,
    });
    assert.equal(cleaned, 0);
    assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), true);
  });

  it('never sweeps a lock held locally by this process', () => {
    const dir = tmpDir();
    const handle = acquireSessionLock(dir);
    assert.equal(handle.acquired, true);
    // Even with probes that would declare any PID dead, a locally-held lock stays.
    const cleaned = cleanupStaleLocks([dir], { isPidAlive: () => false });
    assert.equal(cleaned, 0);
    assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), true);
    handle.release();
  });

  it('skips a dir with no lock and returns 0', () => {
    const dir = tmpDir();
    fs.mkdirSync(dir, { recursive: true });
    assert.equal(cleanupStaleLocks([dir]), 0);
  });

  it('isolates a per-dir error and still sweeps the rest', () => {
    const good = tmpDir();
    writeLock(good, { pid: 999999, startedAt: Date.now(), execPath: '/x/ami' });
    // A non-existent dir path exercises the per-dir try/catch without aborting.
    const missing = path.join(good, 'does', 'not', 'exist');
    const cleaned = cleanupStaleLocks([missing, good], { isPidAlive: () => false, isWsl: () => false });
    assert.equal(cleaned, 1);
  });

  it('respects the WSL cross-OS fallback window (old lock reclaimed, fresh kept)', () => {
    const fresh = tmpDir();
    const stale = tmpDir();
    const now = Date.now();
    writeLock(fresh, { pid: 123, startedAt: now, execPath: '/x/ami' });
    writeLock(stale, { pid: 123, startedAt: now - LOCK_FALLBACK_STALE_MS - 1000, execPath: '/x/ami' });
    const deps = { isPidAlive: () => false, isWsl: () => true, now: () => now };
    assert.equal(cleanupStaleLocks([fresh], deps), 0); // within fallback window → still held
    assert.equal(cleanupStaleLocks([stale], deps), 1); // aged past fallback → reclaimed
  });
});

// ---------------------------------------------------------------------------
// U021 — parseSessionIdentifier: .jsonl path / id / url classifier
// ---------------------------------------------------------------------------
describe('parseSessionIdentifier', () => {
  it('classifies a .jsonl transcript path (case-insensitive)', () => {
    assert.deepEqual(parseSessionIdentifier('/home/u/.ami/s.jsonl'), {
      kind: 'jsonl',
      path: '/home/u/.ami/s.jsonl',
    });
    assert.deepEqual(parseSessionIdentifier('C:\\runs\\S.JSONL'), {
      kind: 'jsonl',
      path: 'C:\\runs\\S.JSONL',
    });
  });

  it('classifies a bare managed session id', () => {
    assert.deepEqual(parseSessionIdentifier('session-20260101-000000-000-0'), {
      kind: 'id',
      id: 'session-20260101-000000-000-0',
    });
  });

  it('classifies an http/https URL', () => {
    const r = parseSessionIdentifier('https://example.com/sessions/abc');
    assert.deepEqual(r, { kind: 'url', url: 'https://example.com/sessions/abc' });
  });

  it('rejects a non-http(s) transport-smuggling scheme', () => {
    assert.equal(parseSessionIdentifier('ext::sh -c "touch /tmp/pwned"'), null);
    assert.equal(parseSessionIdentifier('file:///etc/passwd'), null);
  });

  it('trims surrounding whitespace before classifying', () => {
    assert.deepEqual(parseSessionIdentifier('   session-abc   '), { kind: 'id', id: 'session-abc' });
    assert.deepEqual(parseSessionIdentifier('  /x/y.jsonl '), { kind: 'jsonl', path: '/x/y.jsonl' });
  });

  it('returns null for empty, whitespace, and non-string input', () => {
    assert.equal(parseSessionIdentifier(''), null);
    assert.equal(parseSessionIdentifier('   '), null);
    assert.equal(parseSessionIdentifier(undefined as unknown as string), null);
    assert.equal(parseSessionIdentifier(42 as unknown as string), null);
  });

  it('.jsonl wins over id/url even when it would otherwise parse as a URL', () => {
    // A value ending in .jsonl is always the jsonl branch (Windows-path safety).
    const r = parseSessionIdentifier('https://x/a.jsonl');
    assert.equal(r?.kind, 'jsonl');
  });
});

// ---------------------------------------------------------------------------
// U021 — deriveFirstPrompt: single-line title derivation from first user message
// ---------------------------------------------------------------------------
describe('deriveFirstPrompt', () => {
  const user = (content: unknown): Message => ({ role: 'user', content } as unknown as Message);

  it('collapses multi-line/whitespace into a single line and caps at 100 chars', () => {
    const long = 'fix the\n\n   bug   in\tthe parser';
    assert.equal(deriveFirstPrompt([user(long)]), 'fix the bug in the parser');
    const huge = 'x'.repeat(250);
    assert.equal(deriveFirstPrompt([user(huge)]).length, 100);
  });

  it('extracts the first text block of a multimodal array message', () => {
    const msg = user([{ type: 'text', text: 'describe this  image' }, { type: 'image', source: {} }]);
    assert.equal(deriveFirstPrompt([msg]), 'describe this image');
  });

  it('skips sidechain messages and non-user roles', () => {
    const side = { role: 'user', content: 'subagent turn', isSidechain: true } as unknown as Message;
    const asst = { role: 'assistant', content: 'hi' } as unknown as Message;
    assert.equal(deriveFirstPrompt([side, asst, user('real prompt')]), 'real prompt');
  });

  it('falls back to "Branched conversation" when there is no usable first prompt', () => {
    assert.equal(deriveFirstPrompt([]), 'Branched conversation');
    assert.equal(deriveFirstPrompt([user('   ')]), 'Branched conversation');
    assert.equal(deriveFirstPrompt([user([{ type: 'image', source: {} }])]), 'Branched conversation');
  });
});
