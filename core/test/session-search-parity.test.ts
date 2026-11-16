import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as fs from 'fs';
import * as path from 'path';
import { searchSessions, searchSessionsRanked } from '../src/session-search';
import { projectStateDir } from '../src/reference-journal';

/**
 * Parity features ported from Claude Code's session/transcript search:
 *  - RENDERED_AS_SENTINEL exclusion (transcriptSearch.ts L9-43): interrupt
 *    placeholder strings render as a UI widget, so their raw text must NOT be
 *    indexed (phantom match: /interrupted -> "[... interrupted ...]").
 *  - Subsequence (fuzzy) match tier (HistorySearchDialog.tsx L66-78): a query
 *    that is a subsequence (not substring) of a session still surfaces, ranked
 *    strictly below every exact substring hit.
 *  - Fallback-to-recent fill (agenticSessionSearch.ts L161-175): when a semantic
 *    (rankFn) search finds fewer literal matches than the limit, pad the ranker's
 *    candidate pool with the most-recent non-matching sessions.
 */

function writeSession(dir: string, file: string, obj: unknown): void {
  fs.writeFileSync(path.join(dir, file), JSON.stringify(obj));
}

describe('searchSessions — interrupt-sentinel exclusion (RENDERED_AS_SENTINEL)', () => {
  let tmpDir: string;
  let sessDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-sentinel-'));
    sessDir = projectStateDir(tmpDir, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
  });

  it('does NOT index the "[Response interrupted by server error]" placeholder text', () => {
    writeSession(sessDir, 'resp.json', {
      id: 'resp',
      messages: [
        { role: 'assistant', content: 'real analysis here\n\n[Response interrupted by server error]' },
      ],
    });
    // "interrupted" only appears inside the sentinel -> must yield no hit.
    assert.equal(searchSessions(tmpDir, 'interrupted').length, 0);
    // The genuine content is still searchable.
    assert.ok(searchSessions(tmpDir, 'analysis').some(r => r.sessionId === 'resp'));
  });

  it('does NOT index the "[Tool call interrupted …]" placeholder text', () => {
    writeSession(sessDir, 'tool.json', {
      id: 'tool',
      messages: [
        { role: 'tool', content: '[Tool call interrupted — no result available]' },
      ],
    });
    assert.equal(searchSessions(tmpDir, 'interrupted').length, 0);
  });

  it('strips a sentinel nested inside structured block content', () => {
    writeSession(sessDir, 'blk.json', {
      id: 'blk',
      messages: [
        { role: 'user', content: [{ type: 'text', text: '[Tool call interrupted — no result available]' }] },
      ],
    });
    assert.equal(searchSessions(tmpDir, 'interrupted').length, 0);
  });
});

describe('searchSessions — subsequence (fuzzy) match tier', () => {
  let tmpDir: string;
  let sessDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-fuzzy-'));
    sessDir = projectStateDir(tmpDir, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
  });

  it('surfaces a session whose text is a subsequence (not substring) of the query', () => {
    writeSession(sessDir, 'fuzzy.json', {
      id: 'fuzzy',
      messages: [{ role: 'user', content: 'please test stop the run' }],
    });
    // "tststp" is a subsequence of "...test stop..." but not a substring.
    const hit = searchSessions(tmpDir, 'tststp').find(r => r.sessionId === 'fuzzy');
    assert.ok(hit, 'subsequence query must surface the session');
    assert.ok(hit!.score < 1, 'fuzzy score must stay below any exact substring score');
  });

  it('ranks exact substring hits strictly above subsequence hits for the same query', () => {
    writeSession(sessDir, 'exact.json', {
      id: 'exact',
      messages: [{ role: 'user', content: 'stop now please' }],
    });
    writeSession(sessDir, 'sub.json', {
      id: 'sub',
      messages: [{ role: 'user', content: 'sxtxoxp placeholder' }],
    });
    const results = searchSessions(tmpDir, 'stop');
    const exact = results.find(r => r.sessionId === 'exact');
    const sub = results.find(r => r.sessionId === 'sub');
    assert.ok(exact, 'substring hit present');
    assert.ok(sub, 'subsequence hit present');
    assert.ok(exact!.score > sub!.score, 'exact must outrank fuzzy');
    assert.equal(results[0].sessionId, 'exact');
  });

  it('does not fuzzy-match multi-token queries (gated to a single short token)', () => {
    writeSession(sessDir, 'multi.json', {
      id: 'multi',
      messages: [{ role: 'user', content: 'alpha beta gamma delta' }],
    });
    // Two tokens, neither a substring -> no subsequence fallback.
    assert.equal(searchSessions(tmpDir, 'zz yy').length, 0);
  });
});

describe('searchSessionsRanked — fallback to recent sessions (recentFill)', () => {
  let tmpDir: string;
  let sessDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-recentfill-'));
    sessDir = projectStateDir(tmpDir, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
    writeSession(sessDir, 'r1.json', { id: 'r1', createdAt: '2024-01-01T00:00:00.000Z', messages: [{ role: 'user', content: 'deploy pipeline' }] });
    writeSession(sessDir, 'r2.json', { id: 'r2', createdAt: '2024-02-01T00:00:00.000Z', messages: [{ role: 'user', content: 'refactor parser' }] });
    writeSession(sessDir, 'r3.json', { id: 'r3', createdAt: '2024-03-01T00:00:00.000Z', messages: [{ role: 'user', content: 'write docs' }] });
  });

  it('pads the ranker pool with recent sessions when literal matches < limit', async () => {
    let poolSize = 0;
    const ranked = await searchSessionsRanked(tmpDir, 'zzznoliteralmatch', {
      limit: 2,
      recentFill: true,
      rankFn: async (_q, candidates) => {
        poolSize = candidates.length;
        return candidates.map(c => c.sessionId);
      },
    });
    assert.ok(poolSize > 0, 'recent sessions must be added to the pool');
    assert.ok(ranked.length > 0, 'a semantic search can now surface non-literal sessions');
    // Newest-first fill.
    assert.equal(ranked[0].sessionId, 'r3');
  });

  it('without recentFill, a non-matching query never invokes the ranker (unchanged default)', async () => {
    let called = false;
    const ranked = await searchSessionsRanked(tmpDir, 'zzznoliteralmatch', {
      rankFn: async () => { called = true; return []; },
    });
    assert.equal(ranked.length, 0);
    assert.equal(called, false);
  });

  it('recentFill does not duplicate a session that already matched literally', async () => {
    writeSession(sessDir, 'hit.json', { id: 'hit', createdAt: '2024-04-01T00:00:00.000Z', messages: [{ role: 'user', content: 'deploy pipeline uniquetok' }] });
    let pool: string[] = [];
    await searchSessionsRanked(tmpDir, 'uniquetok', {
      limit: 5,
      recentFill: true,
      rankFn: async (_q, candidates) => { pool = candidates.map(c => c.sessionId); return pool; },
    });
    const occurrences = pool.filter(id => id === 'hit').length;
    assert.equal(occurrences, 1, 'the literal match must appear exactly once');
  });

  it('recentFill is a no-op when literal matches already fill the limit', async () => {
    let pool: string[] = [];
    await searchSessionsRanked(tmpDir, 'deploy', {
      limit: 1,
      recentFill: true,
      rankFn: async (_q, candidates) => { pool = candidates.map(c => c.sessionId); return pool; },
    });
    assert.equal(pool.length, 1);
  });
});
