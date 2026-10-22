import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as fs from 'fs';
import * as path from 'path';
import { searchSessions } from '../src/session-search';

describe('searchSessions', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-test-'));
    const sessDir = path.join(tmpDir, '.superinference', 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
    fs.writeFileSync(path.join(sessDir, 'session-1.json'), JSON.stringify({
      id: 'session-1',
      messages: [
        { role: 'user', content: 'fix the typescript error in utils' },
        { role: 'assistant', content: 'I found the issue in the type definition' },
      ],
    }));
    fs.writeFileSync(path.join(sessDir, 'session-2.json'), JSON.stringify({
      id: 'session-2',
      messages: [
        { role: 'user', content: 'deploy to production' },
      ],
    }));
  });

  it('finds matches by term', () => {
    const results = searchSessions(tmpDir, 'typescript');
    assert.ok(results.length > 0);
    assert.equal(results[0].sessionId, 'session-1');
  });

  it('returns empty for no matches', () => {
    const results = searchSessions(tmpDir, 'nonexistentterm12345');
    assert.equal(results.length, 0);
  });

  it('returns empty for empty query', () => {
    const results = searchSessions(tmpDir, '');
    assert.equal(results.length, 0);
  });

  it('respects limit', () => {
    const results = searchSessions(tmpDir, 'the', 1);
    assert.ok(results.length <= 1);
  });

  it('returns empty when sessions directory does not exist', () => {
    const noDir = fs.mkdtempSync(path.join(os.tmpdir(), 'no-sessions-'));
    const results = searchSessions(noDir, 'test');
    assert.equal(results.length, 0);
    fs.rmSync(noDir, { recursive: true });
  });

  it('skips malformed JSON session files', () => {
    const sessDir = path.join(tmpDir, '.superinference', 'sessions');
    fs.writeFileSync(path.join(sessDir, 'bad.json'), 'not json');
    const results = searchSessions(tmpDir, 'typescript');
    assert.ok(results.length > 0);
  });

  it('skips non-json files', () => {
    const sessDir = path.join(tmpDir, '.superinference', 'sessions');
    fs.writeFileSync(path.join(sessDir, 'readme.txt'), 'not a session');
    const results = searchSessions(tmpDir, 'typescript');
    assert.ok(results.length > 0);
  });

  it('handles messages with non-string content', () => {
    const sessDir = path.join(tmpDir, '.superinference', 'sessions');
    fs.writeFileSync(path.join(sessDir, 'array-content.json'), JSON.stringify({
      id: 'array-content',
      messages: [
        { role: 'user', content: [{ type: 'text', text: 'hello' }] },
      ],
    }));
    const results = searchSessions(tmpDir, 'hello');
    const arrayHit = results.find(r => r.sessionId === 'array-content');
    assert.equal(arrayHit, undefined);
  });

  it('handles session without id', () => {
    const sessDir = path.join(tmpDir, '.superinference', 'sessions');
    fs.writeFileSync(path.join(sessDir, 'no-id.json'), JSON.stringify({
      messages: [{ role: 'user', content: 'unique search keyword zzzxxx' }],
    }));
    const results = searchSessions(tmpDir, 'zzzxxx');
    assert.ok(results.length > 0);
    assert.equal(results[0].sessionId, 'no-id');
  });

  it('handles session without messages', () => {
    const sessDir = path.join(tmpDir, '.superinference', 'sessions');
    fs.writeFileSync(path.join(sessDir, 'no-msgs.json'), JSON.stringify({ id: 'no-msgs' }));
    const results = searchSessions(tmpDir, 'test');
    const hit = results.find(r => r.sessionId === 'no-msgs');
    assert.equal(hit, undefined);
  });

  it('sorts results by score (more matching terms = higher)', () => {
    const sessDir = path.join(tmpDir, '.superinference', 'sessions');
    fs.writeFileSync(path.join(sessDir, 'multi.json'), JSON.stringify({
      id: 'multi',
      messages: [{ role: 'user', content: 'typescript error fix deploy' }],
    }));
    const results = searchSessions(tmpDir, 'typescript error fix');
    if (results.length > 1) {
      assert.ok(results[0].score >= results[1].score);
    }
  });

  it('truncates content to 300 chars', () => {
    const sessDir = path.join(tmpDir, '.superinference', 'sessions');
    fs.writeFileSync(path.join(sessDir, 'long.json'), JSON.stringify({
      id: 'long',
      messages: [{ role: 'user', content: 'keyword ' + 'x'.repeat(400) }],
    }));
    const results = searchSessions(tmpDir, 'keyword');
    assert.ok(results.length > 0);
    assert.ok(results[0].content.length <= 300);
  });

  it('hoists literal (contiguous) phrase matches above scattered-term matches', () => {
    const sessDir = path.join(tmpDir, '.superinference', 'sessions');
    fs.writeFileSync(path.join(sessDir, 'phrase.json'), JSON.stringify({
      id: 'phrase',
      messages: [
        { role: 'user', content: 'typescript foo error bar' },
        { role: 'assistant', content: 'typescript error now' },
      ],
    }));
    const results = searchSessions(tmpDir, 'typescript error');
    const own = results.filter(r => r.sessionId === 'phrase');
    assert.equal(own[0].content, 'typescript error now');
    assert.ok(own[0].score > own[1].score);
  });

  it('breaks score ties by recency (newer sessions first)', () => {
    const sessDir = path.join(tmpDir, '.superinference', 'sessions');
    fs.writeFileSync(path.join(sessDir, 'aaa-older.json'), JSON.stringify({
      id: 'older',
      createdAt: '2020-01-01T00:00:00.000Z',
      messages: [{ role: 'user', content: 'ranktoken deploy' }],
    }));
    fs.writeFileSync(path.join(sessDir, 'zzz-newer.json'), JSON.stringify({
      id: 'newer',
      createdAt: '2024-01-01T00:00:00.000Z',
      messages: [{ role: 'user', content: 'ranktoken deploy' }],
    }));
    const results = searchSessions(tmpDir, 'ranktoken');
    const ids = results
      .filter(r => r.sessionId === 'older' || r.sessionId === 'newer')
      .map(r => r.sessionId);
    assert.deepEqual(ids, ['newer', 'older']);
  });
});
