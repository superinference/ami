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
  searchTranscript,
  flattenMessageText,
  renderableSearchText,
  attachmentSearchText,
  prSearchText,
  DATE_TIE_THRESHOLD_MS,
  MAX_RANKING_TRANSCRIPT_CHARS,
  MAX_RANKING_MESSAGES_TO_SCAN,
  MAX_SNIPPET_CHARS,
} from '../src/session-search';
import type { SearchTrace } from '../src/session-search';
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

describe('attachmentSearchText — per-attachment-type extraction (transcriptSearch.ts L82-112 parity)', () => {
  it('extracts joined memory content from a relevant_memories attachment', () => {
    assert.equal(
      attachmentSearchText({ type: 'relevant_memories', memories: [{ content: 'alpha recall' }, { content: 'beta recall' }] }),
      'alpha recall\nbeta recall',
    );
  });
  it('skips non-string / malformed memory entries', () => {
    assert.equal(
      attachmentSearchText({ type: 'relevant_memories', memories: [{ content: 'keep' }, { content: 42 }, null, 'nope'] }),
      'keep',
    );
    assert.equal(attachmentSearchText({ type: 'relevant_memories', memories: 'not-an-array' }), '');
  });
  it('extracts a string queued_command prompt', () => {
    assert.equal(attachmentSearchText({ type: 'queued_command', prompt: 'deploy the service' }), 'deploy the service');
  });
  it('extracts text blocks from a queued_command block-array prompt', () => {
    assert.equal(
      attachmentSearchText({ type: 'queued_command', prompt: [{ type: 'text', text: 'run the' }, { type: 'text', text: 'migration' }] }),
      'run the migration',
    );
  });
  it('excludes a task-notification queued command (machine-owned chrome)', () => {
    assert.equal(
      attachmentSearchText({ type: 'queued_command', prompt: 'task done report', commandMode: 'task-notification' }),
      '',
    );
  });
  it('excludes an isMeta queued command attachment', () => {
    assert.equal(attachmentSearchText({ type: 'queued_command', prompt: 'nudge', isMeta: true }), '');
  });
  it('indexes empty for unknown / non-object attachments', () => {
    assert.equal(attachmentSearchText({ type: 'something_else', data: 'x' }), '');
    assert.equal(attachmentSearchText(null), '');
    assert.equal(attachmentSearchText('str'), '');
  });
});

describe('flattenMessageText — attachment + origin extraction/exclusion', () => {
  it('indexes a relevant_memories attachment message (renders in transcript, else unsearchable)', () => {
    assert.equal(
      flattenMessageText({ role: 'user', attachment: { type: 'relevant_memories', memories: [{ content: 'remembered fact' }] } }),
      'remembered fact',
    );
  });
  it('indexes a queued_command attachment prompt', () => {
    assert.equal(
      flattenMessageText({ role: 'user', attachment: { type: 'queued_command', prompt: 'queued work item' } }),
      'queued work item',
    );
  });
  it('indexes a collapsed_read_search message relevantMemories', () => {
    assert.equal(
      flattenMessageText({ role: 'user', relevantMemories: [{ content: 'collapsed memory' }] }),
      'collapsed memory',
    );
  });
  it('excludes a message whose provenance origin is task-notification', () => {
    assert.equal(
      flattenMessageText({ role: 'user', content: 'auto task report', origin: { kind: 'task-notification' } }),
      '',
    );
  });
  it('still indexes a user message whose origin is a plain user', () => {
    assert.equal(
      flattenMessageText({ role: 'user', content: 'real question', origin: { kind: 'user' } }),
      'real question',
    );
  });
});

describe('searchSessions — attachment-shaped messages are searchable end-to-end', () => {
  it('finds a session by relevant-memory / queued-prompt attachment text, and excludes task-notification origin', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-attach-'));
    const sessDir = projectStateDir(tmpDir, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
    fs.writeFileSync(path.join(sessDir, 's.json'), JSON.stringify({
      id: 's',
      messages: [
        { role: 'user', attachment: { type: 'relevant_memories', memories: [{ content: 'xyzmemneedle lives here' }] } },
        { role: 'user', attachment: { type: 'queued_command', prompt: 'xyzqueuedneedle queued prompt' } },
        { role: 'user', content: 'xyznotifyneedle ignore me', origin: { kind: 'task-notification' } },
        { role: 'assistant', content: 'ordinary reply' },
      ],
    }));
    assert.ok(searchSessions(tmpDir, 'xyzmemneedle').length > 0);
    assert.ok(searchSessions(tmpDir, 'xyzqueuedneedle').length > 0);
    assert.equal(searchSessions(tmpDir, 'xyznotifyneedle').length, 0);
    fs.rmSync(tmpDir, { recursive: true });
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

describe('searchSessions — configurable scan knobs (SearchScanOptions)', () => {
  let tmpDir: string;
  let sessDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-knobs-'));
    sessDir = projectStateDir(tmpDir, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
  });

  function writeSession(name: string, content: string, createdAt: string): void {
    fs.writeFileSync(path.join(sessDir, name), JSON.stringify({
      id: name.replace('.json', ''), createdAt,
      messages: [{ role: 'user', content }],
    }));
  }

  it('snippetChars caps the result snippet below the default budget', () => {
    const long = 'needle ' + 'x'.repeat(1000);
    writeSession('s1.json', long, new Date().toISOString());
    const def = searchSessions(tmpDir, 'needle');
    assert.ok(def[0].content.length <= MAX_SNIPPET_CHARS);
    const tight = searchSessions(tmpDir, 'needle', 20, 'relevance', { snippetChars: 40 });
    assert.ok(tight[0].content.length <= 40, `snippet ${tight[0].content.length} should be <= 40`);
    // Default budget is wider than the tight one — proves the knob actually shrinks it.
    assert.ok(tight[0].content.length < def[0].content.length);
  });

  it('maxSessions caps how many (newest-first) session files are scanned', () => {
    // Three sessions, distinct mtimes; all contain the needle.
    writeSession('old.json', 'needle old', '2020-01-01T00:00:00Z');
    writeSession('mid.json', 'needle mid', '2021-01-01T00:00:00Z');
    writeSession('new.json', 'needle new', '2022-01-01T00:00:00Z');
    const now = Date.now();
    fs.utimesSync(path.join(sessDir, 'old.json'), new Date(now - 30000), new Date(now - 30000));
    fs.utimesSync(path.join(sessDir, 'mid.json'), new Date(now - 20000), new Date(now - 20000));
    fs.utimesSync(path.join(sessDir, 'new.json'), new Date(now - 10000), new Date(now - 10000));
    const all = searchSessions(tmpDir, 'needle');
    assert.equal(all.length, 3);
    const capped = searchSessions(tmpDir, 'needle', 20, 'relevance', { maxSessions: 1 });
    assert.equal(capped.length, 1);
    // Newest file (by mtime) survives the cap.
    assert.equal(capped[0].sessionId, 'new');
  });

  it('maxMessages caps how many messages per session are scanned', () => {
    fs.writeFileSync(path.join(sessDir, 's.json'), JSON.stringify({
      id: 's', createdAt: new Date().toISOString(),
      messages: [
        { role: 'user', content: 'alpha one' },
        { role: 'assistant', content: 'beta two' },
        { role: 'user', content: 'needle three' },
      ],
    }));
    // With only the first message scanned, the needle in message 3 is invisible.
    const capped = searchSessions(tmpDir, 'needle', 20, 'relevance', { maxMessages: 1 });
    assert.equal(capped.length, 0);
    // Default scans all messages and finds it.
    assert.equal(searchSessions(tmpDir, 'needle').length, 1);
  });

  it('maxSessions=0 scans nothing (boundary)', () => {
    writeSession('s.json', 'needle here', new Date().toISOString());
    assert.equal(searchSessions(tmpDir, 'needle', 20, 'relevance', { maxSessions: 0 }).length, 0);
  });

  it('omitting the options bag is identical to the legacy positional call', () => {
    writeSession('s.json', 'needle body text', new Date().toISOString());
    const a = searchSessions(tmpDir, 'needle');
    const b = searchSessions(tmpDir, 'needle', 20, 'relevance', {});
    assert.deepEqual(a, b);
  });
});

describe('searchSessions / searchSessionsRanked — diagnostic trace', () => {
  let tmpDir: string;
  let sessDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-trace-'));
    sessDir = projectStateDir(tmpDir, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
    fs.writeFileSync(path.join(sessDir, 'hit.json'), JSON.stringify({
      id: 'hit', createdAt: '2022-01-01T00:00:00Z',
      messages: [{ role: 'user', content: 'the needle is here' }],
    }));
    fs.writeFileSync(path.join(sessDir, 'miss.json'), JSON.stringify({
      id: 'miss', createdAt: '2021-01-01T00:00:00Z',
      messages: [{ role: 'user', content: 'unrelated content' }],
    }));
  });

  it('emits a trace with scanned/matched/hits/results counts', () => {
    const traces: SearchTrace[] = [];
    const r = searchSessions(tmpDir, 'needle', 20, 'relevance', { trace: t => traces.push(t) });
    assert.equal(traces.length, 1);
    const t = traces[0];
    assert.equal(t.query, 'needle');
    assert.equal(t.scanned, 2);          // both files scanned
    assert.equal(t.matched, 1);          // only 'hit' produced a row
    assert.equal(t.hits, 1);
    assert.equal(t.results, r.length);
    assert.equal(t.ordering, 'relevance');
    assert.equal(t.limit, 20);
    assert.equal(t.rerank, undefined);   // no rerank on the plain path
  });

  it('emits a zero-count trace when the sessions dir is absent', () => {
    const empty = fs.mkdtempSync(path.join(os.tmpdir(), 'search-trace-empty-'));
    const traces: SearchTrace[] = [];
    searchSessions(empty, 'needle', 20, 'relevance', { trace: t => traces.push(t) });
    assert.equal(traces.length, 1);
    assert.deepEqual(
      { scanned: traces[0].scanned, matched: traces[0].matched, hits: traces[0].hits, results: traces[0].results },
      { scanned: 0, matched: 0, hits: 0, results: 0 },
    );
    fs.rmSync(empty, { recursive: true });
  });

  it('emits a zero-count trace for an empty query', () => {
    const traces: SearchTrace[] = [];
    searchSessions(tmpDir, '   ', 20, 'relevance', { trace: t => traces.push(t) });
    assert.equal(traces.length, 1);
    assert.equal(traces[0].hits, 0);
    assert.equal(traces[0].scanned, 0);
  });

  it('ranked trace reports skipped=no-rankfn when no ranker is supplied', async () => {
    const traces: SearchTrace[] = [];
    await searchSessionsRanked(tmpDir, 'needle', { trace: t => traces.push(t) });
    assert.equal(traces.length, 1);
    assert.equal(traces[0].rerank?.applied, false);
    assert.equal(traces[0].rerank?.skipped, 'no-rankfn');
    assert.equal(traces[0].matched, 1);
  });

  it('ranked trace reports applied=true and the candidate-pool size when a ranker runs', async () => {
    const traces: SearchTrace[] = [];
    const out = await searchSessionsRanked(tmpDir, 'needle', {
      rankFn: async (_q, cands) => cands.map(c => c.sessionId).reverse(),
      trace: t => traces.push(t),
    });
    assert.equal(traces.length, 1);
    assert.equal(traces[0].rerank?.applied, true);
    assert.equal(traces[0].rerank?.candidates, out.length);
    assert.equal(traces[0].rerank?.skipped, undefined);
  });

  it('ranked trace reports skipped=aborted when the signal is already aborted', async () => {
    const traces: SearchTrace[] = [];
    await searchSessionsRanked(tmpDir, 'needle', {
      rankFn: async () => { throw new Error('should not run'); },
      signal: AbortSignal.abort(),
      trace: t => traces.push(t),
    });
    assert.equal(traces[0].rerank?.skipped, 'aborted');
    assert.equal(traces[0].rerank?.applied, false);
  });

  it('ranked trace reports skipped=error when the ranker throws', async () => {
    const traces: SearchTrace[] = [];
    await searchSessionsRanked(tmpDir, 'needle', {
      rankFn: async () => { throw new Error('boom'); },
      trace: t => traces.push(t),
    });
    assert.equal(traces[0].rerank?.skipped, 'error');
    assert.equal(traces[0].rerank?.applied, false);
  });

  it('ranked trace reports skipped=empty + recentFilled when there are no candidates', async () => {
    const traces: SearchTrace[] = [];
    await searchSessionsRanked(tmpDir, 'zzznomatchzzz', {
      rankFn: async (_q, c) => c.map(x => x.sessionId),
      trace: t => traces.push(t),
    });
    assert.equal(traces[0].rerank?.skipped, 'empty');
    assert.equal(traces[0].rerank?.candidates, 0);
  });

  it('ranked trace records recentFilled when recentFill widens the pool', async () => {
    const traces: SearchTrace[] = [];
    await searchSessionsRanked(tmpDir, 'needle', {
      limit: 20,
      recentFill: true,
      rankFn: async (_q, c) => c.map(x => x.sessionId),
      trace: t => traces.push(t),
    });
    // One literal hit ('hit'); recentFill backfills the non-matching 'miss'.
    assert.equal(traces[0].rerank?.recentFilled, 1);
    assert.equal(traces[0].rerank?.candidates, 2);
  });

  it('no trace callback => no overhead, no throw (default path unchanged)', async () => {
    assert.equal(searchSessions(tmpDir, 'needle').length, 1);
    const ranked = await searchSessionsRanked(tmpDir, 'needle', {});
    assert.equal(ranked.length, 1);
  });
});

describe('searchTranscript — live in-memory transcript search (transcriptSearch.ts parity)', () => {
  const transcript = [
    { role: 'user', content: 'fix the typescript error' },
    { role: 'assistant', content: 'I looked at the type definition' },
    { role: 'user', content: 'now add a unit test' },
    { role: 'assistant', content: 'done, the test passes now' },
  ];

  it('returns matching messages with index / role / snippet / score', () => {
    const hits = searchTranscript(transcript, 'test');
    // messages 2 and 3 contain "test"
    assert.deepEqual(hits.map(h => h.index).sort(), [2, 3]);
    const first = hits.find(h => h.index === 2)!;
    assert.equal(first.role, 'user');
    assert.ok(first.content.includes('test'));
    assert.equal(first.score, 1); // single term, full coverage
  });

  it('empty query or empty transcript returns no matches', () => {
    assert.deepEqual(searchTranscript(transcript, ''), []);
    assert.deepEqual(searchTranscript(transcript, '   '), []);
    assert.deepEqual(searchTranscript([], 'test'), []);
    // non-array input is tolerated
    assert.deepEqual(searchTranscript(undefined as unknown as unknown[], 'x'), []);
  });

  it('relevance ordering (default) puts the best score first, position breaks ties', () => {
    const msgs = [
      { role: 'user', content: 'alpha beta' },          // 1 of 2 terms
      { role: 'assistant', content: 'alpha beta gamma' }, // 2 of 2 + phrase
    ];
    const hits = searchTranscript(msgs, 'alpha gamma');
    assert.equal(hits[0].index, 1); // higher coverage ranks first
  });

  it('position ordering preserves transcript order regardless of score', () => {
    const hits = searchTranscript(transcript, 'test', { ordering: 'position' });
    assert.deepEqual(hits.map(h => h.index), [2, 3]);
  });

  it('respects the limit', () => {
    const hits = searchTranscript(transcript, 'test', { limit: 1 });
    assert.equal(hits.length, 1);
  });

  it('matches user-visible tool_use arguments via the shared cached extractor', () => {
    const msgs = [
      { role: 'assistant', content: [{ type: 'tool_use', input: { command: 'rg needle src' } }] },
    ];
    const hits = searchTranscript(msgs, 'needle');
    assert.equal(hits.length, 1);
    assert.equal(hits[0].index, 0);
  });

  it('does not match model-only system-reminder chrome (no phantom hits)', () => {
    const msgs = [
      { role: 'user', content: 'hello <system-reminder>secretphantom</system-reminder> world' },
    ];
    assert.deepEqual(searchTranscript(msgs, 'secretphantom'), []);
    // the user-visible text around it still matches
    assert.equal(searchTranscript(msgs, 'world').length, 1);
  });

  it('skips meta messages (rendered as chrome, not text)', () => {
    const msgs = [
      { role: 'user', isMeta: true, content: 'ephemeral control nudge' },
      { role: 'user', content: 'real content here' },
    ];
    assert.deepEqual(searchTranscript(msgs, 'ephemeral'), []);
    assert.equal(searchTranscript(msgs, 'real').length, 1);
  });

  it('fuzzy fallback: a short single-token typo surfaces a subsequence match below exact', () => {
    const msgs = [{ role: 'user', content: 'test stop handler' }];
    // "tststp" is a subsequence of "test stop" but not a substring
    const hits = searchTranscript(msgs, 'tststp');
    assert.equal(hits.length, 1);
    assert.ok(hits[0].score < 1); // fuzzy tier scores strictly below a substring hit
  });

  it('fuzzy fallback does NOT fire when any substring hit exists', () => {
    const msgs = [
      { role: 'user', content: 'exact test match' },
      { role: 'assistant', content: 'txe scattered letters' }, // subsequence of "test"? no — guard anyway
    ];
    const hits = searchTranscript(msgs, 'test');
    // only the substring message matches; fuzzy tier is suppressed
    assert.deepEqual(hits.map(h => h.index), [0]);
  });

  it('honors snippetChars for the result snippet', () => {
    const long = 'x'.repeat(50) + ' needle ' + 'y'.repeat(50);
    const hits = searchTranscript([{ role: 'user', content: long }], 'needle', { snippetChars: 20 });
    assert.ok(hits[0].content.length <= 20);
  });
});
