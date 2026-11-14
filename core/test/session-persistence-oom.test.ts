/**
 * Claude-parity: session persistence opt-out (H26) + OOM read boundary (H27).
 *
 * Run: cd core/core && node --import tsx --test test/session-persistence-oom.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  SessionManager,
  MAX_SESSION_READ_BYTES,
  isSessionPersistenceDisabled,
  repairTranscriptFile,
} from '../src/session';
import type { Session } from '../src/session';

let dir: string;
beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sess-po-')); });
afterEach(() => {
  delete process.env.AMI_SKIP_SESSION_HISTORY;
  delete process.env.CLAUDE_CODE_SKIP_PROMPT_HISTORY;
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

function mkSession(id: string): Session {
  return {
    id: id as Session['id'],
    messages: [{ role: 'user', content: 'hi' }],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'm', baseUrl: 'b' },
  };
}

describe('isSessionPersistenceDisabled (H26)', () => {
  it('is false with no env set', () => {
    assert.equal(isSessionPersistenceDisabled({}), false);
  });
  it('honours AMI_SKIP_SESSION_HISTORY truthy values', () => {
    assert.equal(isSessionPersistenceDisabled({ AMI_SKIP_SESSION_HISTORY: '1' }), true);
    assert.equal(isSessionPersistenceDisabled({ AMI_SKIP_SESSION_HISTORY: 'true' }), true);
  });
  it('honours CLAUDE_CODE_SKIP_PROMPT_HISTORY', () => {
    assert.equal(isSessionPersistenceDisabled({ CLAUDE_CODE_SKIP_PROMPT_HISTORY: 'yes' }), true);
  });
  it('treats 0/false/off/empty as NOT disabled', () => {
    for (const v of ['0', 'false', 'off', 'no', '']) {
      assert.equal(isSessionPersistenceDisabled({ AMI_SKIP_SESSION_HISTORY: v }), false, v);
    }
  });
});

describe('SessionManager.save — persistence opt-out (H26)', () => {
  it('writes nothing when persistence is disabled', () => {
    process.env.AMI_SKIP_SESSION_HISTORY = '1';
    const mgr = new SessionManager(dir, dir);
    mgr.save(mkSession('session-skip'));
    assert.equal(mgr.load('session-skip'), null, 'no file should be written');
    assert.equal(fs.existsSync(path.join(dir, 'session-skip.json')), false);
  });
  it('writes normally when persistence is enabled', () => {
    const mgr = new SessionManager(dir, dir);
    mgr.save(mkSession('session-keep'));
    assert.ok(mgr.load('session-keep'), 'file should be written');
  });
});

describe('OOM read boundary (H27)', () => {
  it('skips a session file larger than MAX_SESSION_READ_BYTES instead of reading it', () => {
    const mgr = new SessionManager(dir, dir);
    mgr.save(mkSession('session-huge'));
    const p = path.join(dir, 'session-huge.json');
    assert.ok(fs.existsSync(p));
    // Grow the file (sparse) past the cap — load must treat it as unreadable.
    fs.truncateSync(p, MAX_SESSION_READ_BYTES + 1);
    assert.equal(mgr.load('session-huge'), null, 'oversized file must be skipped, not loaded');
  });
  it('repairTranscriptFile leaves an oversized transcript untouched', () => {
    const p = path.join(dir, 't.jsonl');
    fs.writeFileSync(p, JSON.stringify({ type: 'x', timestamp: 1 }) + '\n');
    fs.truncateSync(p, MAX_SESSION_READ_BYTES + 1);
    const res = repairTranscriptFile(p);
    assert.deepEqual(res, { repairedLines: 0, droppedLines: 0 });
    // Still the oversized size — not rewritten.
    assert.ok(fs.statSync(p).size > MAX_SESSION_READ_BYTES);
  });
});
