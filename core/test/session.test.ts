import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { SessionManager } from '../src/session';
import type { Session } from '../src/session';

function makeTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-session-test-'));
}

function makeSession(id: string, messages: Session['messages'] = []): Session {
  return {
    id,
    messages,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'test-model', baseUrl: 'http://localhost' },
  };
}

describe('SessionManager', () => {
  let tmpDir: string;
  let sm: SessionManager;

  beforeEach(() => {
    tmpDir = makeTempDir();
    sm = new SessionManager(tmpDir);
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('save and load a session', () => {
    const session = makeSession('s1', [{ role: 'user', content: 'hello' }]);
    sm.save(session);
    const loaded = sm.load('s1');
    assert.ok(loaded);
    assert.equal(loaded.id, 's1');
    assert.equal(loaded.messages.length, 1);
    assert.equal(loaded.messages[0].content, 'hello');
  });

  it('load returns null for nonexistent session', () => {
    assert.equal(sm.load('nonexistent'), null);
  });

  it('loadLatest returns null when no sessions exist', () => {
    assert.equal(sm.loadLatest(), null);
  });

  it('loadLatest returns the most recently modified session', () => {
    const s1 = makeSession('s1', [{ role: 'user', content: 'first' }]);
    sm.save(s1);

    // Ensure different mtime
    const s2 = makeSession('s2', [{ role: 'user', content: 'second' }]);
    const sessionDir = sm.getSessionDir();
    const s1Path = path.join(sessionDir, 's1.json');
    const past = new Date(Date.now() - 10000);
    fs.utimesSync(s1Path, past, past);

    sm.save(s2);
    const latest = sm.loadLatest();
    assert.ok(latest);
    assert.equal(latest.id, 's2');
  });

  it('list returns all sessions sorted by date descending', () => {
    sm.save(makeSession('a', [{ role: 'user', content: 'alpha message here' }]));
    sm.save(makeSession('b', [{ role: 'user', content: 'beta message here' }]));
    const entries = sm.list();
    assert.equal(entries.length, 2);
    assert.ok(entries[0].id === 'a' || entries[0].id === 'b');
  });

  it('list extracts preview from first user message', () => {
    sm.save(makeSession('x', [
      { role: 'system', content: 'you are a bot' },
      { role: 'user', content: 'Fix the bug in login' },
    ]));
    const entries = sm.list();
    assert.equal(entries.length, 1);
    assert.ok(entries[0].preview.includes('Fix the bug'));
  });

  it('list preview shows "(no messages)" when no user message', () => {
    sm.save(makeSession('empty', [{ role: 'system', content: 'system only' }]));
    const entries = sm.list();
    assert.equal(entries[0].preview, '(no messages)');
  });

  it('list preview truncates long messages', () => {
    const longMsg = 'a'.repeat(200);
    sm.save(makeSession('long', [{ role: 'user', content: longMsg }]));
    const entries = sm.list();
    assert.ok(entries[0].preview.length <= 104);
    assert.ok(entries[0].preview.endsWith('...'));
  });

  it('delete removes a session', () => {
    sm.save(makeSession('del'));
    assert.ok(sm.load('del'));
    sm.delete('del');
    assert.equal(sm.load('del'), null);
  });

  it('delete is a no-op for nonexistent sessions', () => {
    sm.delete('nope');
  });

  it('newId returns a timestamp-based id', () => {
    const id = SessionManager.newId();
    assert.ok(id.startsWith('session-'));
    assert.ok(id.length > 10);
  });

  it('getSessionDir returns the configured directory', () => {
    const dir = sm.getSessionDir();
    assert.ok(dir.includes('.superinference'));
  });

  it('truncates long user messages on save', () => {
    const longContent = 'x'.repeat(10000);
    sm.save(makeSession('trunc', [{ role: 'user', content: longContent }]));
    const loaded = sm.load('trunc');
    assert.ok(loaded);
    assert.ok(loaded.messages[0].content!.length < 10000);
    assert.ok((loaded.messages[0].content as string).includes('truncated'));
  });

  it('truncates long tool messages on save', () => {
    const longContent = 'y'.repeat(10000);
    sm.save(makeSession('trunc-tool', [{ role: 'tool', content: longContent, tool_call_id: 'tc1' }]));
    const loaded = sm.load('trunc-tool');
    assert.ok(loaded);
    assert.ok((loaded.messages[0].content as string).includes('truncated'));
  });

  it('does not truncate short messages', () => {
    sm.save(makeSession('short', [{ role: 'user', content: 'short msg' }]));
    const loaded = sm.load('short');
    assert.ok(loaded);
    assert.equal(loaded.messages[0].content, 'short msg');
  });

  it('handles assistant messages without truncation', () => {
    sm.save(makeSession('asst', [{ role: 'assistant', content: 'z'.repeat(10000) }]));
    const loaded = sm.load('asst');
    assert.ok(loaded);
    assert.equal((loaded.messages[0].content as string).length, 10000);
  });

  it('handles custom sessionDirOverride', () => {
    const customDir = path.join(tmpDir, 'custom-sessions');
    const customSm = new SessionManager(tmpDir, customDir);
    assert.equal(customSm.getSessionDir(), customDir);
    customSm.save(makeSession('custom'));
    assert.ok(customSm.load('custom'));
  });

  it('list handles malformed JSON files gracefully', () => {
    sm.save(makeSession('good', [{ role: 'user', content: 'ok' }]));
    const sessionDir = sm.getSessionDir();
    fs.writeFileSync(path.join(sessionDir, 'bad.json'), 'not json!!!', 'utf-8');
    const entries = sm.list();
    assert.equal(entries.length, 1);
  });

  it('preview handles non-string user content', () => {
    sm.save(makeSession('arr', [{ role: 'user', content: [{ type: 'text', text: 'hello' }] as any }]));
    const entries = sm.list();
    assert.equal(entries[0].preview, '(no messages)');
  });

  it('sanitizes session IDs with path traversal sequences', () => {
    const session = makeSession('../../etc/evil');
    sm.save(session);
    assert.equal(fs.existsSync('/etc/evil.json'), false);
    const sessionDir = sm.getSessionDir();
    const files = fs.readdirSync(sessionDir);
    assert.ok(files.some(f => f.includes('evil')));
    assert.ok(files.every(f => !f.includes('..')));
  });

  it('sanitizes session IDs with slashes', () => {
    const session = makeSession('a/b/c');
    sm.save(session);
    const loaded = sm.load('a/b/c');
    assert.ok(loaded);
  });
});
