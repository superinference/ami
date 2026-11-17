import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { SessionManager, loadMessagesFromTranscriptPath, MAX_SESSION_READ_BYTES } from '../src/session';
import type { Message } from '../src/types';

function tmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'sess-resume-u021-'));
  after(() => fs.rmSync(d, { recursive: true, force: true }));
  return d;
}

function writeSessionFile(dir: string, id: string, extra: Record<string, unknown>): void {
  fs.writeFileSync(
    path.join(dir, `${id}.json`),
    JSON.stringify({
      id,
      messages: [],
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      provider: { model: 'm', baseUrl: 'b' },
      ...extra,
    }),
  );
}

// U021-w70: allowlist-sanitize untrusted persisted state on resume. The on-disk
// JSON is untrusted; an unknown permissionMode must be DROPPED, never restored.
describe('sanitize persisted permissionMode on load', () => {
  it('drops an unknown permissionMode from the loaded session (sync)', () => {
    const dir = tmpDir();
    const mgr = new SessionManager(dir, dir);
    writeSessionFile(dir, 'session-bad-mode', {
      settings: { permissionMode: 'launch-the-missiles', thinkingLevel: 'high' },
    });
    const loaded = mgr.load('session-bad-mode');
    assert.ok(loaded);
    assert.equal(loaded!.settings?.permissionMode, undefined); // bogus mode dropped
    assert.equal(loaded!.settings?.thinkingLevel, 'high');     // valid sibling kept
  });

  it('keeps a valid permissionMode (round-trips through the allowlist)', () => {
    const dir = tmpDir();
    const mgr = new SessionManager(dir, dir);
    writeSessionFile(dir, 'session-ok-mode', { settings: { permissionMode: 'deny-all' } });
    const loaded = mgr.load('session-ok-mode');
    assert.equal(loaded!.settings?.permissionMode, 'deny-all');
  });

  it('drops an unknown permissionMode on the async load path too', async () => {
    const dir = tmpDir();
    const mgr = new SessionManager(dir, dir);
    writeSessionFile(dir, 'session-bad-async', { settings: { permissionMode: 'nope' } });
    const loaded = await mgr.loadAsync('session-bad-async');
    assert.ok(loaded);
    assert.equal(loaded!.settings?.permissionMode, undefined);
  });

  it('surfaces an unknown mode as undefined in list() entries', () => {
    const dir = tmpDir();
    const mgr = new SessionManager(dir, dir);
    writeSessionFile(dir, 'session-list-mode', {
      settings: { permissionMode: 'bogus' },
      messages: [{ role: 'user', content: 'hi there' }],
    });
    const entry = mgr.list().find(e => String(e.id) === 'session-list-mode');
    assert.ok(entry);
    assert.equal(entry!.permissionMode, undefined);
  });
});

// U021-w70: cross-directory resume from an arbitrary transcript .jsonl path.
describe('loadMessagesFromTranscriptPath', () => {
  function msg(uuid: string, parentUuid: string | undefined, role: string, content: string, extra: Record<string, unknown> = {}): Record<string, unknown> {
    return { uuid, parentUuid, role, content, ...extra };
  }

  it('reconstructs the main-thread chain from an arbitrary jsonl path', () => {
    const dir = tmpDir();
    const file = path.join(dir, 'transcript.jsonl');
    const rows = [
      msg('a', undefined, 'user', 'first'),
      msg('b', 'a', 'assistant', 'reply'),
      msg('c', 'b', 'user', 'second'),
      // a sidechain branch off b — must NOT appear in the main thread
      msg('s1', 'b', 'assistant', 'subagent', { isSidechain: true }),
    ];
    fs.writeFileSync(file, rows.map(r => JSON.stringify(r)).join('\n') + '\n');

    const chain = loadMessagesFromTranscriptPath(file);
    assert.deepEqual(chain.map((m: Message) => (m as { uuid?: string }).uuid), ['a', 'b', 'c']);
  });

  it('skips a corrupt/truncated trailing line instead of failing', () => {
    const dir = tmpDir();
    const file = path.join(dir, 'torn.jsonl');
    const good = JSON.stringify(msg('a', undefined, 'user', 'hello'));
    fs.writeFileSync(file, good + '\n' + '{"uuid":"b","role":"assist'); // torn tail
    const chain = loadMessagesFromTranscriptPath(file);
    assert.deepEqual(chain.map((m: Message) => (m as { uuid?: string }).uuid), ['a']);
  });

  it('can resume a specific leaf branch via leafUuid', () => {
    const dir = tmpDir();
    const file = path.join(dir, 'branch.jsonl');
    const rows = [
      msg('a', undefined, 'user', 'root'),
      msg('b', 'a', 'assistant', 'path-1-leaf'),
      msg('c', 'a', 'assistant', 'path-2-leaf'),
    ];
    fs.writeFileSync(file, rows.map(r => JSON.stringify(r)).join('\n'));
    const chain = loadMessagesFromTranscriptPath(file, { leafUuid: 'b' });
    assert.deepEqual(chain.map((m: Message) => (m as { uuid?: string }).uuid), ['a', 'b']);
  });

  it('returns [] for a missing path', () => {
    assert.deepEqual(loadMessagesFromTranscriptPath(path.join(tmpDir(), 'nope.jsonl')), []);
  });

  it('returns [] for a file with no valid message rows', () => {
    const dir = tmpDir();
    const file = path.join(dir, 'empty.jsonl');
    fs.writeFileSync(file, '\n{"not":"a message"}\n{"also":"nope"}\n');
    assert.deepEqual(loadMessagesFromTranscriptPath(file), []);
  });

  it('refuses to slurp a transcript above the OOM boundary', () => {
    const dir = tmpDir();
    const file = path.join(dir, 'huge.jsonl');
    // Create a valid first line, then pad the file past the read cap cheaply.
    fs.writeFileSync(file, JSON.stringify(msg('a', undefined, 'user', 'hi')) + '\n');
    fs.truncateSync(file, MAX_SESSION_READ_BYTES + 1);
    assert.deepEqual(loadMessagesFromTranscriptPath(file), []);
  });
});
