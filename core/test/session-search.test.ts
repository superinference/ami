import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as fs from 'fs';
import * as path from 'path';
import { searchSessions, searchSessionsRanked } from '../src/session-search';
import { projectStateDir } from '../src/reference-journal';

describe('searchSessions', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-test-'));
    const sessDir = projectStateDir(tmpDir, 'sessions');
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
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'bad.json'), 'not json');
    const results = searchSessions(tmpDir, 'typescript');
    assert.ok(results.length > 0);
  });

  it('skips non-json files', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'readme.txt'), 'not a session');
    const results = searchSessions(tmpDir, 'typescript');
    assert.ok(results.length > 0);
  });

  it('flattens structured (block-array) content so it is searchable (H33)', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'array-content.json'), JSON.stringify({
      id: 'array-content',
      messages: [
        { role: 'user', content: [{ type: 'text', text: 'hello' }] },
      ],
    }));
    const results = searchSessions(tmpDir, 'hello');
    const arrayHit = results.find(r => r.sessionId === 'array-content');
    assert.ok(arrayHit, 'text inside a block-array message must be searchable');
  });

  it('flattens tool_use inputs and tool_call arguments (H33)', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'tooluse.json'), JSON.stringify({
      id: 'tooluse',
      messages: [
        { role: 'assistant', content: [{ type: 'tool_use', name: 'bash', input: { command: 'uniqgrepneedle' } }] },
        { role: 'assistant', content: '', tool_calls: [{ function: { name: 'file_edit', arguments: '{"file_path":"zzzargtoken"}' } }] },
      ],
    }));
    const byInput = searchSessions(tmpDir, 'uniqgrepneedle').find(r => r.sessionId === 'tooluse');
    assert.ok(byInput, 'tool_use input must be searchable');
    const byArgs = searchSessions(tmpDir, 'zzzargtoken').find(r => r.sessionId === 'tooluse');
    assert.ok(byArgs, 'tool_call arguments must be searchable');
  });

  it('matches a session by its git branch metadata (H13)', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'branchy.json'), JSON.stringify({
      id: 'branchy',
      gitBranch: 'feature-xyzbranch',
      messages: [{ role: 'user', content: 'nothing relevant' }],
    }));
    const results = searchSessions(tmpDir, 'xyzbranch');
    assert.ok(results.find(r => r.sessionId === 'branchy'), 'branch name must be searchable');
  });

  it('handles session without id', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'no-id.json'), JSON.stringify({
      messages: [{ role: 'user', content: 'unique search keyword zzzxxx' }],
    }));
    const results = searchSessions(tmpDir, 'zzzxxx');
    assert.ok(results.length > 0);
    assert.equal(results[0].sessionId, 'no-id');
  });

  it('handles session without messages', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'no-msgs.json'), JSON.stringify({ id: 'no-msgs' }));
    const results = searchSessions(tmpDir, 'test');
    const hit = results.find(r => r.sessionId === 'no-msgs');
    assert.equal(hit, undefined);
  });

  it('sorts results by score (more matching terms = higher)', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
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
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'long.json'), JSON.stringify({
      id: 'long',
      messages: [{ role: 'user', content: 'keyword ' + 'x'.repeat(400) }],
    }));
    const results = searchSessions(tmpDir, 'keyword');
    assert.ok(results.length > 0);
    assert.ok(results[0].content.length <= 300);
  });

  it('hoists literal (contiguous) phrase matches above scattered-term matches', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
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
    const sessDir = projectStateDir(tmpDir, 'sessions');
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

describe('searchSessionsRanked (optional LLM-ranking tier)', () => {
  let tmpDir: string;

  // Three sessions each matching the query token with equal score, so the
  // substring/recency order is deterministic and a reranker can reorder them.
  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-rank-test-'));
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
    fs.writeFileSync(path.join(sessDir, 'a.json'), JSON.stringify({
      id: 'alpha',
      createdAt: '2024-03-01T00:00:00.000Z',
      messages: [{ role: 'user', content: 'ranktoken alpha' }],
    }));
    fs.writeFileSync(path.join(sessDir, 'b.json'), JSON.stringify({
      id: 'beta',
      createdAt: '2024-02-01T00:00:00.000Z',
      messages: [{ role: 'user', content: 'ranktoken beta' }],
    }));
    fs.writeFileSync(path.join(sessDir, 'c.json'), JSON.stringify({
      id: 'gamma',
      createdAt: '2024-01-01T00:00:00.000Z',
      messages: [{ role: 'user', content: 'ranktoken gamma' }],
    }));
  });

  it('with no rankFn is identical to searchSessions (zero behavior change)', async () => {
    const substring = searchSessions(tmpDir, 'ranktoken');
    const ranked = await searchSessionsRanked(tmpDir, 'ranktoken');
    assert.deepEqual(ranked, substring);
  });

  it('with no rankFn preserves substring recency order (newest first)', async () => {
    const ranked = await searchSessionsRanked(tmpDir, 'ranktoken');
    assert.deepEqual(ranked.map(r => r.sessionId), ['alpha', 'beta', 'gamma']);
  });

  it('respects limit before reranking', async () => {
    let seen = 0;
    const ranked = await searchSessionsRanked(tmpDir, 'ranktoken', {
      limit: 2,
      rankFn: async (_q, candidates) => {
        seen = candidates.length;
        return candidates.map(c => c.sessionId);
      },
    });
    assert.equal(seen, 2);
    assert.equal(ranked.length, 2);
  });

  it('reorders candidates per the injected rankFn', async () => {
    const rankFn = async (query: string, candidates: { sessionId: string }[]) => {
      assert.equal(query, 'ranktoken');
      assert.equal(candidates.length, 3);
      // Deliberately invert the default (recency) order.
      return ['gamma', 'beta', 'alpha'];
    };
    const ranked = await searchSessionsRanked(tmpDir, 'ranktoken', { rankFn });
    assert.deepEqual(ranked.map(r => r.sessionId), ['gamma', 'beta', 'alpha']);
    // Prove it differs from the no-rankFn (substring) order.
    const substring = await searchSessionsRanked(tmpDir, 'ranktoken');
    assert.notDeepEqual(ranked.map(r => r.sessionId), substring.map(r => r.sessionId));
  });

  it('appends candidates omitted from the rankFn output in original order', async () => {
    // Ranker only mentions one id; the other two must be appended (original order).
    const ranked = await searchSessionsRanked(tmpDir, 'ranktoken', {
      rankFn: async () => ['gamma'],
    });
    assert.deepEqual(ranked.map(r => r.sessionId), ['gamma', 'alpha', 'beta']);
  });

  it('ignores unknown ids returned by the rankFn', async () => {
    const ranked = await searchSessionsRanked(tmpDir, 'ranktoken', {
      rankFn: async () => ['does-not-exist', 'beta'],
    });
    assert.deepEqual(ranked.map(r => r.sessionId), ['beta', 'alpha', 'gamma']);
  });

  it('degrades gracefully to substring order when rankFn throws', async () => {
    const substring = searchSessions(tmpDir, 'ranktoken');
    const ranked = await searchSessionsRanked(tmpDir, 'ranktoken', {
      rankFn: async () => { throw new Error('LLM unavailable'); },
    });
    assert.deepEqual(ranked.map(r => r.sessionId), substring.map(r => r.sessionId));
  });

  it('degrades gracefully to substring order when rankFn rejects', async () => {
    const substring = searchSessions(tmpDir, 'ranktoken');
    const ranked = await searchSessionsRanked(tmpDir, 'ranktoken', {
      rankFn: () => Promise.reject(new Error('timeout')),
    });
    assert.deepEqual(ranked.map(r => r.sessionId), substring.map(r => r.sessionId));
  });

  it('does not invoke rankFn when there are no candidates', async () => {
    let called = false;
    const ranked = await searchSessionsRanked(tmpDir, 'nonexistentterm12345', {
      rankFn: async () => { called = true; return []; },
    });
    assert.equal(ranked.length, 0);
    assert.equal(called, false);
  });
});
