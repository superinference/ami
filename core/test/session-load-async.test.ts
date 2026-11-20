/**
 * SessionManager.loadAsync — non-blocking lazy full-transcript load (parity:
 * the reference's async `loadFullLog`). loadAsync must be a drop-in async equivalent of
 * the sync `load`: same result for a normal session, the same blob rehydration
 * (including the missing-blob fallback), the same `null` for a missing /
 * oversized / corrupt file, and the same id sanitization — just off the event
 * loop so a large/blob-heavy transcript doesn't block startup/the TUI.
 *
 * Run: cd core/core && node --import tsx --test test/session-load-async.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { SessionManager, MAX_SESSION_READ_BYTES } from '../src/session';
import type { Session } from '../src/session';

let dir: string;
let mgr: SessionManager;

beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sess-load-async-'));
  mgr = new SessionManager(dir, dir);
});
afterEach(() => {
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

function mkSession(id: string, messages: Session['messages']): Session {
  return {
    id: id as Session['id'],
    messages,
    createdAt: new Date('2024-01-01T00:00:00.000Z').toISOString(),
    updatedAt: new Date('2024-01-02T00:00:00.000Z').toISOString(),
    provider: { model: 'm', baseUrl: 'b' },
  };
}

describe('SessionManager.loadAsync', () => {
  it('loads a saved session asynchronously (Promise of the full Session)', async () => {
    mgr.save(mkSession('s1', [{ role: 'user', content: 'hello' }]));
    const result = mgr.loadAsync('s1');
    assert.ok(result instanceof Promise, 'loadAsync must return a Promise');
    const loaded = await result;
    assert.ok(loaded);
    assert.equal(loaded.id, 's1');
    assert.equal(loaded.messages.length, 1);
    assert.equal(loaded.messages[0].content, 'hello');
  });

  it('returns null for a nonexistent session', async () => {
    assert.equal(await mgr.loadAsync('does-not-exist'), null);
  });

  it('rehydrates an offloaded blob exactly like the sync load', async () => {
    const big = 'X'.repeat(20_000); // > MAX_CONTENT_LENGTH (5000) → offloaded to a blob
    mgr.save(mkSession('s-blob', [
      { role: 'user', content: big },
      { role: 'assistant', content: 'ok' },
    ]));
    const sync = mgr.load('s-blob');
    const async = await mgr.loadAsync('s-blob');
    assert.ok(sync && async);
    // Full content must be restored from the blob (not the 5000-char preview).
    assert.equal(async.messages[0].content, big);
    assert.equal(async.messages[0].content, sync.messages[0].content);
    assert.equal(async.messages[1].content, 'ok');
  });

  it('falls back to the preview marker when a blob is missing (same as sync)', async () => {
    const big = 'Y'.repeat(20_000);
    mgr.save(mkSession('s-missing-blob', [{ role: 'user', content: big }]));
    // Remove the blob sidecar to simulate a lost/unavailable blob.
    const blobRoot = path.join(dir, 'blobs', 's-missing-blob');
    for (const f of fs.readdirSync(blobRoot)) fs.rmSync(path.join(blobRoot, f));
    const sync = mgr.load('s-missing-blob');
    const async = await mgr.loadAsync('s-missing-blob');
    assert.ok(sync && async);
    assert.match(String(async.messages[0].content), /\.\.\. \(truncated; blob unavailable\)$/);
    assert.equal(async.messages[0].content, sync.messages[0].content);
  });

  it('returns null for a file larger than MAX_SESSION_READ_BYTES (OOM boundary)', async () => {
    mgr.save(mkSession('s-huge', [{ role: 'user', content: 'hi' }]));
    fs.truncateSync(path.join(dir, 's-huge.json'), MAX_SESSION_READ_BYTES + 1);
    assert.equal(await mgr.loadAsync('s-huge'), null);
  });

  it('returns null for a corrupt JSON file', async () => {
    fs.writeFileSync(path.join(dir, 's-corrupt.json'), '{ not valid json');
    assert.equal(await mgr.loadAsync('s-corrupt'), null);
  });

  it('sanitizes a traversal id the same way the sync load does', async () => {
    mgr.save(mkSession('s-safe', [{ role: 'user', content: 'safe' }]));
    // Both paths sanitize path separators/.. — neither escapes the session dir.
    assert.equal(await mgr.loadAsync('../../etc/passwd'), mgr.load('../../etc/passwd'));
  });

  it('produces output identical to sync load for a multi-message session', async () => {
    mgr.save(mkSession('s-parity', [
      { role: 'user', content: 'a'.repeat(9000) },
      { role: 'assistant', content: 'b' },
      { role: 'tool', content: 'c'.repeat(8000), tool_call_id: 't1' },
    ]));
    assert.deepEqual(await mgr.loadAsync('s-parity'), mgr.load('s-parity'));
  });
});
