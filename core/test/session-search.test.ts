import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as fs from 'fs';
import * as path from 'path';
import {
  searchSessions,
  searchSessionsRanked,
  toolUseSearchText,
  toolResultSearchText,
  extractRankingTranscript,
  flattenMessageText,
  renderableSearchText,
  prSearchText,
  DATE_TIE_THRESHOLD_MS,
  MAX_RANKING_TRANSCRIPT_CHARS,
  MAX_RANKING_MESSAGES_TO_SCAN,
} from '../src/session-search';
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

  it('caps the candidate pool handed to the ranker (boundary cap)', async () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    // Seed well over MAX_RANKING_CANDIDATES recent sessions so recent-fill would
    // overflow the pool without the cap.
    for (let i = 0; i < 130; i++) {
      fs.writeFileSync(path.join(sessDir, `cap-${i}.json`), JSON.stringify({
        id: `cap-${i}`,
        createdAt: `2024-01-01T00:00:${String(i % 60).padStart(2, '0')}.000Z`,
        messages: [{ role: 'user', content: `filler ${i}` }],
      }));
    }
    let seen = 0;
    await searchSessionsRanked(tmpDir, 'ranktoken', {
      limit: 200,
      recentFill: true,
      rankFn: async (_q, candidates) => { seen = candidates.length; return candidates.map(c => c.sessionId); },
    });
    assert.ok(seen <= 100, `pool must be capped at 100, saw ${seen}`);
  });

  it('recent-fill candidates carry a head+tail transcript, not just a 300-char head', async () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    // A long session lacking the literal query term, so it only enters via recentFill.
    fs.writeFileSync(path.join(sessDir, 'rich.json'), JSON.stringify({
      id: 'rich',
      createdAt: '2025-06-01T00:00:00.000Z',
      messages: [
        { role: 'user', content: 'OPENINGMARKER ' + 'x'.repeat(500) },
        { role: 'assistant', content: 'y'.repeat(500) },
        { role: 'user', content: 'z'.repeat(500) + ' CLOSINGMARKER' },
      ],
    }));
    let poolContent = '';
    await searchSessionsRanked(tmpDir, 'nomatchzzz', {
      limit: 5,
      recentFill: true,
      rankFn: async (_q, candidates) => {
        const hit = candidates.find(c => c.sessionId === 'rich');
        poolContent = hit ? hit.content : '';
        return candidates.map(c => c.sessionId);
      },
    });
    // Both the opening and closing markers must survive (head+tail sampling),
    // which a flat 300-char head could never contain together.
    assert.ok(poolContent.includes('OPENINGMARKER'), 'head sample missing');
    assert.ok(poolContent.includes('CLOSINGMARKER'), 'tail sample missing');
  });
});

describe('tool-use / tool-result allowlisted extraction (phantom-match guard)', () => {
  let tmpDir: string;
  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-allow-'));
    fs.mkdirSync(projectStateDir(tmpDir, 'sessions'), { recursive: true });
  });

  it('indexes rendered tool_use fields but NOT non-rendered bookkeeping keys', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'tu.json'), JSON.stringify({
      id: 'tu',
      messages: [
        { role: 'assistant', content: [{ type: 'tool_use', name: 'bash', input: { command: 'realcmdtoken', backgroundTaskId: 'phantomzzz' } }] },
      ],
    }));
    assert.ok(searchSessions(tmpDir, 'realcmdtoken').some(r => r.sessionId === 'tu'), 'rendered command must be searchable');
    assert.equal(searchSessions(tmpDir, 'phantomzzz').length, 0, 'bookkeeping field must NOT be indexed');
  });

  it('parses tool_call arguments and indexes only allowlisted keys', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'tc.json'), JSON.stringify({
      id: 'tc',
      messages: [
        { role: 'assistant', content: '', tool_calls: [{ function: { name: 'file_edit', arguments: '{"file_path":"realpathtoken","rawOutputPath":"phantomyyy"}' } }] },
      ],
    }));
    assert.ok(searchSessions(tmpDir, 'realpathtoken').some(r => r.sessionId === 'tc'), 'rendered file_path must be searchable');
    assert.equal(searchSessions(tmpDir, 'phantomyyy').length, 0, 'bookkeeping argument key must NOT be indexed');
  });

  it('falls back to the raw string for non-JSON tool_call arguments', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'bad-args.json'), JSON.stringify({
      id: 'bad-args',
      messages: [
        { role: 'assistant', content: '', tool_calls: [{ function: { name: 'x', arguments: 'rawargstoken not json' } }] },
      ],
    }));
    assert.ok(searchSessions(tmpDir, 'rawargstoken').some(r => r.sessionId === 'bad-args'));
  });

  it('indexes native tool_result output fields but NOT non-rendered keys', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'tr.json'), JSON.stringify({
      id: 'tr',
      messages: [
        { role: 'user', content: [{ type: 'tool_result', content: { stdout: 'realstdouttoken', rawOutputPath: 'phantomqqq' } }] },
        { role: 'user', content: [{ type: 'tool_result', content: { file: { content: 'filecontenttoken' } } }] },
      ],
    }));
    assert.ok(searchSessions(tmpDir, 'realstdouttoken').some(r => r.sessionId === 'tr'), 'stdout must be searchable');
    assert.ok(searchSessions(tmpDir, 'filecontenttoken').some(r => r.sessionId === 'tr'), 'file.content must be searchable');
    assert.equal(searchSessions(tmpDir, 'phantomqqq').length, 0, 'non-rendered result key must NOT be indexed');
  });

  it('toolUseSearchText: allowlist, arrays, and non-object inputs', () => {
    assert.equal(toolUseSearchText({ command: 'ls', secret: 'hidden' }), 'ls');
    assert.equal(toolUseSearchText({ args: ['a', 'b'], files: ['c'] }), 'a b c');
    assert.equal(toolUseSearchText({ args: ['a', 2] }), '', 'mixed-type arrays are skipped');
    assert.equal(toolUseSearchText('plain'), 'plain');
    assert.equal(toolUseSearchText(null), '');
    assert.equal(toolUseSearchText(42), '');
  });

  it('toolResultSearchText: known shapes, allowlist, and fallbacks', () => {
    assert.equal(toolResultSearchText({ stdout: 'out', stderr: 'err' }), 'out\nerr');
    assert.equal(toolResultSearchText({ stdout: 'out' }), 'out');
    assert.equal(toolResultSearchText({ file: { content: 'fc' } }), 'fc');
    assert.equal(toolResultSearchText({ output: 'o', durationMs: 5 }), 'o');
    assert.equal(toolResultSearchText({ filenames: ['f1', 'f2'] }), 'f1\nf2');
    assert.equal(toolResultSearchText('str'), 'str');
    assert.equal(toolResultSearchText(null), '');
    assert.equal(toolResultSearchText(7), '');
    assert.equal(toolResultSearchText({ file: { content: 5 } }), '', 'non-string file.content falls through to allowlist');
  });
});

describe('subsequence (fuzzy) fallback tier', () => {
  let tmpDir: string;
  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-fuzzy-'));
    fs.mkdirSync(projectStateDir(tmpDir, 'sessions'), { recursive: true });
  });

  it('surfaces a short single-token query via subsequence when no substring matches', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'fz.json'), JSON.stringify({
      id: 'fz',
      messages: [{ role: 'user', content: 'test stop button' }],
    }));
    // 'tststp' is not a substring but IS a subsequence of 'test stop'.
    const results = searchSessions(tmpDir, 'tststp');
    const hit = results.find(r => r.sessionId === 'fz');
    assert.ok(hit, 'fuzzy subsequence must surface the session');
    assert.equal(hit!.role, 'fuzzy');
  });

  it('ranks every exact substring hit strictly above a fuzzy hit', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'exact.json'), JSON.stringify({
      id: 'exact',
      messages: [{ role: 'user', content: 'abcd exact match' }],
    }));
    fs.writeFileSync(path.join(sessDir, 'fuzzy.json'), JSON.stringify({
      id: 'fuzzy',
      messages: [{ role: 'user', content: 'a b c d spread out' }],
    }));
    const results = searchSessions(tmpDir, 'abcd');
    assert.equal(results[0].sessionId, 'exact', 'exact substring hit must outrank fuzzy');
  });

  it('does not fuzzy-match multi-token queries', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'multi.json'), JSON.stringify({
      id: 'multi',
      messages: [{ role: 'user', content: 'test stop button' }],
    }));
    assert.equal(searchSessions(tmpDir, 'tst stp').length, 0, 'multi-token fuzzy must not fire');
  });
});

describe('render-replaced sentinel exclusion (phantom-match guard)', () => {
  let tmpDir: string;
  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-sentinel-'));
    fs.mkdirSync(projectStateDir(tmpDir, 'sessions'), { recursive: true });
  });

  it('excludes interrupt-sentinel text so it cannot produce phantom matches', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'sent.json'), JSON.stringify({
      id: 'sent',
      messages: [
        { role: 'assistant', content: '[Response interrupted by server error]' },
        { role: 'user', content: '[Tool call interrupted by the user]' },
      ],
    }));
    // The literal word "interrupted" renders as a component, never as text — a
    // search for it must NOT match these sentinel messages.
    assert.equal(searchSessions(tmpDir, 'interrupted').length, 0, 'sentinel text must be excluded');
  });

  it('excludes model-only system-reminder spans from the index', () => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'rem.json'), JSON.stringify({
      id: 'rem',
      messages: [
        { role: 'user', content: 'visible text <system-reminder>hiddenremindertoken</system-reminder> more' },
      ],
    }));
    assert.ok(searchSessions(tmpDir, 'visible').some(r => r.sessionId === 'rem'), 'visible text still searchable');
    assert.equal(searchSessions(tmpDir, 'hiddenremindertoken').length, 0, 'reminder text must be excluded');
  });
});

describe('extractRankingTranscript (bounded head+tail sampling)', () => {
  it('returns empty for empty / non-array input', () => {
    assert.equal(extractRankingTranscript([]), '');
    assert.equal(extractRankingTranscript(undefined as unknown as unknown[]), '');
  });

  it('collapses whitespace across flattened messages', () => {
    const out = extractRankingTranscript([
      { role: 'user', content: 'alpha   \n\t beta' },
      { role: 'assistant', content: 'gamma' },
    ]);
    assert.equal(out, 'alpha beta gamma');
  });

  it('samples from both ends and drops the middle when over the message cap', () => {
    const messages = [];
    for (let i = 0; i < MAX_RANKING_MESSAGES_TO_SCAN + 20; i++) {
      messages.push({ role: 'user', content: `m${i}` });
    }
    const out = extractRankingTranscript(messages);
    assert.ok(out.includes('m0'), 'head must be present');
    assert.ok(out.includes(`m${MAX_RANKING_MESSAGES_TO_SCAN + 19}`), 'tail must be present');
    const mid = Math.floor((MAX_RANKING_MESSAGES_TO_SCAN + 20) / 2);
    assert.ok(!out.includes(` m${mid} `), 'middle messages must be dropped');
  });

  it('hard-caps the char count with a trailing ellipsis', () => {
    const out = extractRankingTranscript([{ role: 'user', content: 'q'.repeat(MAX_RANKING_TRANSCRIPT_CHARS + 500) }]);
    assert.equal(out.length, MAX_RANKING_TRANSCRIPT_CHARS + 1); // + ellipsis char
    assert.ok(out.endsWith('…'));
  });
});

describe('prSearchText (PR searchable field — LogSelector.tsx L432 parity)', () => {
  it('renders `pr #<number> <repository>` for a full PR link', () => {
    assert.equal(prSearchText({ number: 42, url: 'x', repository: 'octo/cat' }), 'pr #42 octo/cat');
  });
  it('omits a missing/empty repository', () => {
    assert.equal(prSearchText({ number: 7, url: 'x' }), 'pr #7');
    assert.equal(prSearchText({ number: 7, url: 'x', repository: '' }), 'pr #7');
  });
  it('returns empty for a non-object / non-numeric / non-finite number', () => {
    assert.equal(prSearchText(undefined), '');
    assert.equal(prSearchText('nope'), '');
    assert.equal(prSearchText({ repository: 'octo/cat' }), '');
    assert.equal(prSearchText({ number: '12' }), '');
    assert.equal(prSearchText({ number: Number.NaN }), '');
  });
});

describe('searchSessions — PR fields are searchable', () => {
  let tmpDir: string;
  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-pr-'));
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
    fs.writeFileSync(path.join(sessDir, 'pr-session.json'), JSON.stringify({
      id: 'pr-session',
      pr: { number: 1234, url: 'https://github.com/octo/widget/pull/1234', repository: 'octo/widget' },
      messages: [{ role: 'user', content: 'unrelated body text' }],
    }));
  });

  it('surfaces a session by its PR number', () => {
    const results = searchSessions(tmpDir, '#1234');
    assert.equal(results.length, 1);
    assert.equal(results[0].sessionId, 'pr-session');
    assert.equal(results[0].role, 'meta');
  });

  it('surfaces a session by its PR repository slug', () => {
    const results = searchSessions(tmpDir, 'octo/widget');
    assert.ok(results.some(r => r.sessionId === 'pr-session'));
  });
});

describe('flattenMessageText — isMeta / task-notification exclusion (transcriptSearch.ts L82-112 parity)', () => {
  it('excludes isMeta messages (ephemeral control, rendered as a component)', () => {
    assert.equal(flattenMessageText({ role: 'user', content: 'injected nudge text', isMeta: true }), '');
  });
  it('excludes task-notification queued commands', () => {
    assert.equal(flattenMessageText({ role: 'user', content: 'task done', commandMode: 'task-notification' }), '');
    assert.equal(flattenMessageText({ role: 'user', content: 'task done', mode: 'task-notification' }), '');
  });
  it('still indexes ordinary (non-meta) messages', () => {
    assert.equal(flattenMessageText({ role: 'user', content: 'hello world' }), 'hello world');
  });
});

describe('searchSessions — isMeta messages do not produce phantom hits', () => {
  it('does not match text that lives only in an isMeta message', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-meta-'));
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
    fs.writeFileSync(path.join(sessDir, 's.json'), JSON.stringify({
      id: 's',
      messages: [
        { role: 'user', content: 'phantomtoken only here', isMeta: true },
        { role: 'assistant', content: 'a real reply' },
      ],
    }));
    assert.equal(searchSessions(tmpDir, 'phantomtoken').length, 0);
    assert.ok(searchSessions(tmpDir, 'reply').length > 0);
    fs.rmSync(tmpDir, { recursive: true });
  });
});

describe('searchSessions — hybrid recency ordering (--recent, DATE_TIE_THRESHOLD_MS parity)', () => {
  let tmpDir: string;
  // older session has the STRONGER relevance score; newer session is weaker but recent.
  const now = Date.now();
  const makeSession = (id: string, createdAtMs: number, content: string): void => {
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.writeFileSync(path.join(sessDir, `${id}.json`), JSON.stringify({
      id,
      createdAt: new Date(createdAtMs).toISOString(),
      messages: [{ role: 'user', content }],
    }));
  };

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-order-'));
    fs.mkdirSync(projectStateDir(tmpDir, 'sessions'), { recursive: true });
    // Contiguous phrase 'alpha beta' => literal phrase bonus = strongest relevance, OLD.
    makeSession('old-strong', now - 10 * DATE_TIE_THRESHOLD_MS, 'alpha beta here');
    // Both terms but scattered (no contiguous phrase) = weaker relevance, but NEW.
    makeSession('new-weak', now, 'beta then alpha here');
  });

  it("relevance (default) ranks the stronger-scoring older session first", () => {
    const r = searchSessions(tmpDir, 'alpha beta', 20, 'relevance');
    assert.equal(r[0].sessionId, 'old-strong');
  });

  it("recency ranks the newer session first when outside the tie window", () => {
    const r = searchSessions(tmpDir, 'alpha beta', 20, 'recency');
    assert.equal(r[0].sessionId, 'new-weak');
  });

  it("recency falls back to relevance WITHIN the tie window", () => {
    // Both effectively same time (< DATE_TIE_THRESHOLD_MS apart) => stronger score wins.
    const close = fs.mkdtempSync(path.join(os.tmpdir(), 'search-tie-'));
    fs.mkdirSync(projectStateDir(close, 'sessions'), { recursive: true });
    const sessDir = projectStateDir(close, 'sessions');
    fs.writeFileSync(path.join(sessDir, 'strong.json'), JSON.stringify({
      id: 'strong', createdAt: new Date(now).toISOString(),
      messages: [{ role: 'user', content: 'delta gamma' }],
    }));
    fs.writeFileSync(path.join(sessDir, 'weak.json'), JSON.stringify({
      id: 'weak', createdAt: new Date(now + DATE_TIE_THRESHOLD_MS - 1).toISOString(),
      messages: [{ role: 'user', content: 'gamma then delta' }],
    }));
    const r = searchSessions(close, 'delta gamma', 20, 'recency');
    assert.equal(r[0].sessionId, 'strong');
    fs.rmSync(close, { recursive: true });
  });
});

describe('searchSessionsRanked — ordering option threads to the literal pass', () => {
  it('honors ordering=recency with no rankFn (zero-network)', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ranked-order-'));
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
    const now = Date.now();
    fs.writeFileSync(path.join(sessDir, 'old.json'), JSON.stringify({
      id: 'old', createdAt: new Date(now - 10 * DATE_TIE_THRESHOLD_MS).toISOString(),
      messages: [{ role: 'user', content: 'gamma delta' }],
    }));
    fs.writeFileSync(path.join(sessDir, 'new.json'), JSON.stringify({
      id: 'new', createdAt: new Date(now).toISOString(),
      messages: [{ role: 'user', content: 'delta then gamma' }],
    }));
    const r = await searchSessionsRanked(tmpDir, 'gamma delta', { ordering: 'recency' });
    assert.equal(r[0].sessionId, 'new');
    fs.rmSync(tmpDir, { recursive: true });
  });
});

describe('renderableSearchText / flattenMessageText — WeakMap per-message cache (transcriptSearch.ts L16-29 parity)', () => {
  it('renderableSearchText returns the lowercased flattened text', () => {
    const msg = { role: 'user', content: 'Fix The TypeScript Error' };
    assert.equal(renderableSearchText(msg), flattenMessageText(msg).toLowerCase());
    assert.equal(renderableSearchText(msg), 'fix the typescript error');
  });

  it('renderableSearchText returns "" for non-object messages', () => {
    assert.equal(renderableSearchText(undefined), '');
    assert.equal(renderableSearchText(null), '');
    assert.equal(renderableSearchText('a string'), '');
  });

  it('flattenMessageText memoizes per immutable message object (cache hit path)', () => {
    const msg: { role: string; content: string } = { role: 'user', content: 'original text here' };
    const first = flattenMessageText(msg);
    assert.equal(first, 'original text here');
    // Messages are append-only/immutable in a transcript; mutating the SAME object
    // must return the cached flatten (proves identity-keyed memoization, exercises
    // the cache-hit branch) rather than recomputing.
    msg.content = 'mutated text now';
    assert.equal(flattenMessageText(msg), 'original text here');
    // A different object with the mutated content flattens fresh (no false share).
    assert.equal(flattenMessageText({ role: 'user', content: 'mutated text now' }), 'mutated text now');
  });

  it('renderableSearchText memoizes the lowercased result per message object', () => {
    const msg: { role: string; content: string } = { role: 'user', content: 'CACHED Lower' };
    assert.equal(renderableSearchText(msg), 'cached lower');
    msg.content = 'DIFFERENT';
    // Cached lowercased value returned on the second call (hit branch).
    assert.equal(renderableSearchText(msg), 'cached lower');
  });

  it('searchSessions matching still finds body text (cache wired, behavior preserved)', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-cache-'));
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
    fs.writeFileSync(path.join(sessDir, 's.json'), JSON.stringify({
      id: 's', createdAt: new Date().toISOString(),
      messages: [{ role: 'user', content: 'WeakMapCachedNeedle in the body' }],
    }));
    const r = searchSessions(tmpDir, 'weakmapcachedneedle');
    assert.equal(r.length, 1);
    assert.equal(r[0].sessionId, 's');
    fs.rmSync(tmpDir, { recursive: true });
  });
});
