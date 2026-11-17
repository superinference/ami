import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as fs from 'fs';
import * as path from 'path';
import { Skillbook, SKILLBOOK_FEEDBACK_VALUES } from '../src/skillbook';

describe('Skillbook', () => {
  let tmpDir: string;
  let skillbook: Skillbook;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-test-'));
    fs.mkdirSync(path.join(tmpDir, '.ami'), { recursive: true });
    skillbook = new Skillbook(tmpDir);
  });

  it('starts empty', () => {
    assert.equal(skillbook.getAll().length, 0);
  });

  it('ADD creates an entry', () => {
    const entry = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['ts'], issue: 'type error', insight: 'check types' } });
    assert.ok(entry);
    assert.equal(entry!.section, 'context');
    assert.equal(skillbook.getAll().length, 1);
  });

  it('TAG increments counters', () => {
    const entry = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['go'], issue: 'nil', insight: 'check nil' } });
    skillbook.apply({ type: 'TAG', id: entry!.id, feedback: 'helpful' });
    skillbook.apply({ type: 'TAG', id: entry!.id, feedback: 'helpful' });
    skillbook.apply({ type: 'TAG', id: entry!.id, feedback: 'harmful' });
    const updated = skillbook.getAll()[0];
    assert.equal(updated.helpfulCount, 2);
    assert.equal(updated.harmfulCount, 1);
  });

  it('UPDATE modifies insight', () => {
    const entry = skillbook.apply({ type: 'ADD', entry: { section: 'harness', keywords: ['test'], issue: 'slow', insight: 'cache' } });
    skillbook.apply({ type: 'UPDATE', id: entry!.id, insight: 'use memoization' });
    assert.equal(skillbook.getAll()[0].insight, 'use memoization');
  });

  it('REMOVE deletes entry', () => {
    const entry = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['x'], issue: 'x', insight: 'x' } });
    skillbook.apply({ type: 'REMOVE', id: entry!.id });
    assert.equal(skillbook.getAll().length, 0);
  });

  it('persists to disk and reloads', () => {
    skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['persist'], issue: 'test', insight: 'works' } });
    const reloaded = new Skillbook(tmpDir);
    assert.equal(reloaded.getAll().length, 1);
    assert.equal(reloaded.getAll()[0].keywords[0], 'persist');
  });

  it('search finds by keyword', () => {
    skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['react'], issue: 'hooks', insight: 'use useEffect' } });
    skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['python'], issue: 'import', insight: 'use absolute' } });
    const results = skillbook.search('react');
    assert.equal(results.length, 1);
    assert.ok(results[0].keywords.includes('react'));
  });

  it('getContext returns helpful entries', () => {
    skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['ts'], issue: 'types', insight: 'use strict' } });
    const entry = skillbook.getAll()[0];
    skillbook.apply({ type: 'TAG', id: entry.id, feedback: 'helpful' });
    const context = skillbook.getContext();
    assert.ok(context.includes('Learned Strategies'));
    assert.ok(context.includes('use strict'));
  });

  it('getContext excludes harmful entries', () => {
    skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['bad'], issue: 'wrong', insight: 'bad advice' } });
    const entry = skillbook.getAll()[0];
    skillbook.apply({ type: 'TAG', id: entry.id, feedback: 'harmful' });
    skillbook.apply({ type: 'TAG', id: entry.id, feedback: 'harmful' });
    const context = skillbook.getContext();
    assert.ok(!context.includes('bad advice'));
  });

  it('search with leading/trailing whitespace does not match everything', () => {
    skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['react'], issue: 'hooks', insight: 'use hooks' } });
    skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['python'], issue: 'types', insight: 'use mypy' } });
    const results = skillbook.search(' react ');
    assert.equal(results.length, 1);
    assert.ok(results[0].keywords.includes('react'));
  });

  it('search with pure whitespace returns nothing', () => {
    skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['test'], issue: 'a', insight: 'b' } });
    const results = skillbook.search('   ');
    assert.equal(results.length, 0);
  });

  it('TAG neutral increments neutralCount', () => {
    const entry = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['n'], issue: 'n', insight: 'n' } });
    skillbook.apply({ type: 'TAG', id: entry!.id, feedback: 'neutral' });
    skillbook.apply({ type: 'TAG', id: entry!.id, feedback: 'neutral' });
    const updated = skillbook.getAll()[0];
    assert.equal(updated.neutralCount, 2);
  });

  it('REMOVE returns the removed entry', () => {
    const entry = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['rm'], issue: 'rm', insight: 'rm' } });
    const removed = skillbook.apply({ type: 'REMOVE', id: entry!.id });
    assert.ok(removed);
    assert.equal(removed!.id, entry!.id);
    assert.equal(skillbook.getAll().length, 0);
  });

  it('REMOVE returns null for non-existent entry', () => {
    const removed = skillbook.apply({ type: 'REMOVE', id: 'nonexistent' });
    assert.equal(removed, null);
  });

  it('handles corrupt skillbook file gracefully', () => {
    const corruptDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-corrupt-'));
    const siDir = path.join(corruptDir, '.ami');
    fs.mkdirSync(siDir, { recursive: true });
    fs.writeFileSync(path.join(siDir, 'skillbook.json'), 'NOT VALID JSON{{{');
    const sb = new Skillbook(corruptDir);
    assert.equal(sb.getAll().length, 0);
    fs.rmSync(corruptDir, { recursive: true, force: true });
  });

  it('loads a BOM-prefixed skillbook file (PowerShell 5.x / Windows editors)', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-bom-'));
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    const json = JSON.stringify({
      version: 1,
      entries: [{ id: 'bom', section: 'context', keywords: ['k'], issue: 'i', insight: 'survives the BOM', helpfulCount: 1, harmfulCount: 0, neutralCount: 0, occurrences: [], createdAt: 1, updatedAt: 1 }],
    });
    // Write the file WITH a leading UTF-8 BOM (U+FEFF), exactly as PowerShell
    // 5.x Out-File / Set-Content and several Windows editors do.
    fs.writeFileSync(path.join(dir, '.ami', 'skillbook.json'), '﻿' + json);
    const sb = new Skillbook(dir);
    assert.equal(sb.getAll().length, 1);
    assert.equal(sb.getAll()[0].insight, 'survives the BOM');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('TAG returns null for non-existent entry', () => {
    const result = skillbook.apply({ type: 'TAG', id: 'nonexistent', feedback: 'helpful' });
    assert.equal(result, null);
  });

  it('UPDATE returns null for non-existent entry', () => {
    const result = skillbook.apply({ type: 'UPDATE', id: 'nonexistent', insight: 'new' });
    assert.equal(result, null);
  });

  it('getBySection filters correctly', () => {
    skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['a'], issue: 'a', insight: 'a' } });
    skillbook.apply({ type: 'ADD', entry: { section: 'harness', keywords: ['b'], issue: 'b', insight: 'b' } });
    assert.equal(skillbook.getBySection('context').length, 1);
    assert.equal(skillbook.getBySection('harness').length, 1);
  });

  it('getContext returns empty string when no helpful entries', () => {
    const context = skillbook.getContext();
    assert.equal(context, '');
  });

  it('UPDATE modifies keywords', () => {
    const entry = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['old'], issue: 'x', insight: 'x' } });
    skillbook.apply({ type: 'UPDATE', id: entry!.id, keywords: ['new', 'keywords'] });
    assert.deepEqual(skillbook.getAll()[0].keywords, ['new', 'keywords']);
  });

  // --- Size/length caps on persisted + prompt-injected content ---

  it('ADD clamps an over-long insight to the per-entry cap', () => {
    const huge = 'x'.repeat(5000);
    const entry = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['k'], issue: 'i', insight: huge } });
    assert.equal(entry!.insight.length, 2000);
  });

  it('ADD clamps issue length and keyword count/length', () => {
    const entry = skillbook.apply({ type: 'ADD', entry: {
      section: 'context',
      keywords: Array.from({ length: 50 }, (_, i) => `${'k'.repeat(200)}${i}`),
      issue: 'y'.repeat(2000),
      insight: 'ok',
    } });
    assert.equal(entry!.issue.length, 500);
    assert.equal(entry!.keywords.length, 20);
    assert.ok(entry!.keywords.every(k => k.length <= 100));
  });

  it('UPDATE clamps an over-long insight', () => {
    const entry = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['k'], issue: 'i', insight: 'short' } });
    skillbook.apply({ type: 'UPDATE', id: entry!.id, insight: 'z'.repeat(9000) });
    assert.equal(skillbook.getAll()[0].insight.length, 2000);
  });

  it('getContext truncates and reports omitted entries when over the byte budget', () => {
    // Each insight ~1900 bytes; many helpful entries blow past the 25KB budget.
    for (let i = 0; i < 40; i++) {
      const e = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: [`k${i}`], issue: 'i', insight: 'a'.repeat(1900) } });
      skillbook.apply({ type: 'TAG', id: e!.id, feedback: 'helpful' });
    }
    const context = skillbook.getContext();
    assert.ok(Buffer.byteLength(context, 'utf-8') <= 25_000 + 200);
    assert.ok(context.includes('more omitted to stay within the context budget'));
  });

  // --- Schema validation of untrusted on-disk entries ---

  it('drops malformed entries loaded from disk (bad section / wrong types)', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-bad-'));
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'skillbook.json'), JSON.stringify({
      version: 1,
      entries: [
        { id: 'ok', section: 'context', keywords: ['k'], issue: 'i', insight: 'good', helpfulCount: 1, harmfulCount: 0, neutralCount: 0, occurrences: [], createdAt: 1, updatedAt: 1 },
        { id: 'bad-section', section: 'evil', keywords: [], issue: '', insight: 'x' },
        { id: 123, section: 'context', keywords: [], issue: '', insight: 'x' },
        { section: 'context', keywords: [], issue: '', insight: 'no id' },
        { id: 'no-insight', section: 'context', keywords: [], issue: '' },
        { id: 'bad-keywords', section: 'context', keywords: 'nope', issue: '', insight: 'x' },
        'not-an-object',
        null,
      ],
    }));
    const sb = new Skillbook(dir);
    const all = sb.getAll();
    assert.equal(all.length, 1);
    assert.equal(all[0].id, 'ok');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('coerces invalid counts and keeps non-string keywords out on load', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-coerce-'));
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'skillbook.json'), JSON.stringify({
      version: 1,
      entries: [
        { id: 'c', section: 'context', keywords: ['a', 5, 'b'], issue: 'i', insight: 'x', helpfulCount: 'NaN', harmfulCount: -3, neutralCount: 2, occurrences: [{ sessionId: 's', timestamp: 1 }, { bad: true }], createdAt: 'x', updatedAt: null },
      ],
    }));
    const sb = new Skillbook(dir);
    const e = sb.getAll()[0];
    assert.deepEqual(e.keywords, ['a', 'b']);
    assert.equal(e.helpfulCount, 0);
    assert.equal(e.harmfulCount, 0);
    assert.equal(e.neutralCount, 2);
    assert.equal(e.occurrences.length, 1);
    assert.equal(typeof e.createdAt, 'number');
    assert.equal(typeof e.updatedAt, 'number');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('clamps an over-long insight loaded from disk', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-clamp-'));
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'skillbook.json'), JSON.stringify({
      version: 1,
      entries: [{ id: 'big', section: 'context', keywords: ['k'], issue: 'i', insight: 'q'.repeat(9000), helpfulCount: 0, harmfulCount: 0, neutralCount: 0, occurrences: [], createdAt: 1, updatedAt: 1 }],
    }));
    const sb = new Skillbook(dir);
    assert.equal(sb.getAll()[0].insight.length, 2000);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  // --- Persisted-schema version validation ---

  it('discards entries when the persisted schema version mismatches', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-ver-'));
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'skillbook.json'), JSON.stringify({
      version: 999,
      entries: [{ id: 'future', section: 'context', keywords: ['k'], issue: 'i', insight: 'x', helpfulCount: 1, harmfulCount: 0, neutralCount: 0, occurrences: [], createdAt: 1, updatedAt: 1 }],
    }));
    const sb = new Skillbook(dir);
    assert.equal(sb.getAll().length, 0);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('discards a legacy file with no version field', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-nover-'));
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'skillbook.json'), JSON.stringify({
      entries: [{ id: 'legacy', section: 'context', keywords: ['k'], issue: 'i', insight: 'x', helpfulCount: 1, harmfulCount: 0, neutralCount: 0, occurrences: [], createdAt: 1, updatedAt: 1 }],
    }));
    const sb = new Skillbook(dir);
    assert.equal(sb.getAll().length, 0);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  // --- Multi-section labelling (harness learnings surfaced) ---

  it('getContext surfaces harness learnings under a distinct sub-header', () => {
    const c = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['ctx'], issue: 'i', insight: 'context insight' } });
    skillbook.apply({ type: 'TAG', id: c!.id, feedback: 'helpful' });
    const h = skillbook.apply({ type: 'ADD', entry: { section: 'harness', keywords: ['hns'], issue: 'i', insight: 'harness insight' } });
    skillbook.apply({ type: 'TAG', id: h!.id, feedback: 'helpful' });
    const context = skillbook.getContext();
    assert.ok(context.includes('### Context strategies'));
    assert.ok(context.includes('context insight'));
    assert.ok(context.includes('### Harness strategies'));
    assert.ok(context.includes('harness insight'));
  });

  it('getContext omits the harness sub-header when no harness entries qualify', () => {
    const c = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['ctx'], issue: 'i', insight: 'only context' } });
    skillbook.apply({ type: 'TAG', id: c!.id, feedback: 'helpful' });
    const context = skillbook.getContext();
    assert.ok(context.includes('### Context strategies'));
    assert.ok(!context.includes('### Harness strategies'));
  });

  // --- Negative / caution learnings channel ---

  it('getCautions surfaces entries tagged more harmful than helpful', () => {
    const e = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['danger'], issue: 'risky', insight: 'avoid this approach' } });
    skillbook.apply({ type: 'TAG', id: e!.id, feedback: 'harmful' });
    skillbook.apply({ type: 'TAG', id: e!.id, feedback: 'harmful' });
    const cautions = skillbook.getCautions();
    assert.ok(cautions.includes('Learnings to Avoid'));
    assert.ok(cautions.includes('avoid this approach'));
    assert.ok(cautions.includes('2 harmful'));
    // And such entries remain excluded from the positive context channel.
    assert.ok(!skillbook.getContext().includes('avoid this approach'));
  });

  it('getCautions returns empty string when nothing is net-harmful', () => {
    const e = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['good'], issue: 'i', insight: 'helpful thing' } });
    skillbook.apply({ type: 'TAG', id: e!.id, feedback: 'helpful' });
    assert.equal(skillbook.getCautions(), '');
  });

  it('getCautions truncates when over the byte budget', () => {
    for (let i = 0; i < 40; i++) {
      const e = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: [`k${i}`], issue: 'i', insight: 'b'.repeat(1900) } });
      skillbook.apply({ type: 'TAG', id: e!.id, feedback: 'harmful' });
    }
    const cautions = skillbook.getCautions();
    assert.ok(Buffer.byteLength(cautions, 'utf-8') <= 25_000 + 200);
    assert.ok(cautions.includes('more omitted to stay within the context budget'));
  });

  // --- Graded feedback granularity (score) ---

  it('exposes the full graded feedback scale', () => {
    assert.deepEqual(
      [...SKILLBOOK_FEEDBACK_VALUES].sort(),
      ['essential', 'harmful', 'helpful', 'neutral', 'slightly_helpful', 'unhelpful', 'very_helpful'].sort(),
    );
  });

  it('ADD initializes score to 0', () => {
    const entry = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['k'], issue: 'i', insight: 'x' } });
    assert.equal(entry!.score, 0);
  });

  it('graded TAG accumulates a weighted score (essential > helpful)', () => {
    const e = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['k'], issue: 'i', insight: 'x' } });
    skillbook.apply({ type: 'TAG', id: e!.id, feedback: 'essential' });
    const updated = skillbook.getAll()[0];
    assert.equal(updated.score, 4);          // essential weight
    assert.equal(updated.helpfulCount, 1);   // positive grade → helpful counter
    assert.equal(updated.harmfulCount, 0);
  });

  it('graded negative TAG (unhelpful) lowers score and bumps the harmful counter', () => {
    const e = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['k'], issue: 'i', insight: 'x' } });
    skillbook.apply({ type: 'TAG', id: e!.id, feedback: 'unhelpful' });
    const updated = skillbook.getAll()[0];
    assert.equal(updated.score, -1);
    assert.equal(updated.harmfulCount, 1);
    assert.equal(updated.helpfulCount, 0);
  });

  it('getContext ranks by graded score, not raw helpful count', () => {
    // Both get one positive tag (helpfulCount === 1), but "essential" outranks
    // "slightly_helpful" via the finer score.
    const slight = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['slight'], issue: 'i', insight: 'slightly useful' } });
    skillbook.apply({ type: 'TAG', id: slight!.id, feedback: 'slightly_helpful' });
    const ess = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['ess'], issue: 'i', insight: 'essential insight' } });
    skillbook.apply({ type: 'TAG', id: ess!.id, feedback: 'essential' });
    const context = skillbook.getContext();
    assert.ok(context.indexOf('essential insight') < context.indexOf('slightly useful'));
  });

  it('backfills score from counts for a legacy entry persisted without it', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-score-'));
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'skillbook.json'), JSON.stringify({
      version: 1,
      entries: [{ id: 'legacy', section: 'context', keywords: ['k'], issue: 'i', insight: 'x', helpfulCount: 5, harmfulCount: 2, neutralCount: 0, occurrences: [], createdAt: 1, updatedAt: 1 }],
    }));
    const sb = new Skillbook(dir);
    assert.equal(sb.getAll()[0].score, 3); // 5 helpful - 2 harmful
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('loads a persisted negative score verbatim (not clamped to >= 0)', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-negscore-'));
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'skillbook.json'), JSON.stringify({
      version: 1,
      entries: [{ id: 'n', section: 'context', keywords: ['k'], issue: 'i', insight: 'x', helpfulCount: 0, harmfulCount: 0, neutralCount: 0, score: -5, occurrences: [], createdAt: 1, updatedAt: 1 }],
    }));
    const sb = new Skillbook(dir);
    assert.equal(sb.getAll()[0].score, -5);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  // --- CSPRNG entry ids (no Math.random) ---

  it('generates well-formed, unique, crypto-random entry ids', () => {
    const ids = new Set<string>();
    for (let i = 0; i < 200; i++) {
      const e = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['k'], issue: 'i', insight: 'x' } });
      assert.match(e!.id, /^sk-\d+-[a-z0-9]{6}$/);
      ids.add(e!.id);
    }
    assert.equal(ids.size, 200);
  });

  // --- Relevance-ranked search + noise filter ---

  it('ranks search results by distinct-term coverage, not helpfulCount', () => {
    // `full` matches both query terms; `one` matches only "react". With pure
    // popularity ordering `one` (more upvotes) would win, but coverage must rank
    // the better match first.
    const full = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['react', 'hooks'], issue: 'i', insight: 'both terms' } });
    const one = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['react'], issue: 'i', insight: 'one term, very popular' } });
    skillbook.apply({ type: 'TAG', id: one!.id, feedback: 'helpful' });
    skillbook.apply({ type: 'TAG', id: one!.id, feedback: 'helpful' });
    const results = skillbook.search('react hooks');
    assert.equal(results[0].id, full!.id);
    assert.ok(results.length >= 1 && results[0].insight === 'both terms');
  });

  it('noise filter drops a single incidental term match from a multi-term query', () => {
    skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['flaky', 'test', 'timeout'], issue: 'i', insight: 'real match' } });
    skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['unit', 'test'], issue: 'i', insight: 'incidental only' } });
    const results = skillbook.search('flaky test timeout');
    // Only the entry matching >1 distinct term survives; the lone "test" match is noise.
    assert.equal(results.length, 1);
    assert.equal(results[0].insight, 'real match');
  });

  it('single-term query still matches its one term', () => {
    skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['solo'], issue: 'i', insight: 'kept' } });
    const results = skillbook.search('solo');
    assert.equal(results.length, 1);
    assert.equal(results[0].insight, 'kept');
  });

  it('helpfulCount then score break ties among equally-covering entries', () => {
    const a = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['x', 'y'], issue: 'i', insight: 'A' } });
    const b = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['x', 'y'], issue: 'i', insight: 'B' } });
    skillbook.apply({ type: 'TAG', id: b!.id, feedback: 'helpful' });
    void a;
    const results = skillbook.search('x y');
    assert.equal(results[0].insight, 'B'); // both match 2 terms; B has the upvote
  });

  it('empty / whitespace-only query returns nothing', () => {
    skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['k'], issue: 'i', insight: 'x' } });
    assert.equal(skillbook.search('   ').length, 0);
    assert.equal(skillbook.search('').length, 0);
  });

  // --- Staleness / freshness caveat on prompt-injected learnings ---

  it('getContext appends a staleness caveat to entries older than the threshold', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-stale-'));
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    const old = Date.now() - 45 * 86_400_000;
    fs.writeFileSync(path.join(dir, '.ami', 'skillbook.json'), JSON.stringify({
      version: 1,
      entries: [{ id: 'old', section: 'context', keywords: ['k'], issue: 'i', insight: 'aging insight', helpfulCount: 1, harmfulCount: 0, neutralCount: 0, score: 1, occurrences: [], createdAt: old, updatedAt: old }],
    }));
    const sb = new Skillbook(dir);
    const ctx = sb.getContext();
    assert.ok(ctx.includes('aging insight'));
    assert.match(ctx, /learned 4[0-9] days ago — may be outdated, verify before relying on it/);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('getContext does NOT add a staleness caveat to fresh entries', () => {
    const e = skillbook.apply({ type: 'ADD', entry: { section: 'context', keywords: ['k'], issue: 'i', insight: 'fresh insight' } });
    skillbook.apply({ type: 'TAG', id: e!.id, feedback: 'helpful' });
    const ctx = skillbook.getContext();
    assert.ok(ctx.includes('fresh insight'));
    assert.ok(!ctx.includes('verify before relying on it'));
  });

  // --- Crash-safe + concurrency-safe persistence (atomic rename + merge) ---

  it('save merges concurrent entries instead of clobbering another session', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-merge-'));
    const a = new Skillbook(dir);
    a.apply({ type: 'ADD', entry: { section: 'context', keywords: ['a1'], issue: 'i', insight: 'from A first' } });
    // Second session loads the current book (sees a1 only).
    const b = new Skillbook(dir);
    // A adds another entry AFTER B loaded.
    a.apply({ type: 'ADD', entry: { section: 'context', keywords: ['a2'], issue: 'i', insight: 'A added later' } });
    // B now writes — a naive overwrite would drop A's second entry.
    b.apply({ type: 'ADD', entry: { section: 'context', keywords: ['b1'], issue: 'i', insight: 'from B' } });
    const reloaded = new Skillbook(dir).getAll().map(e => e.insight).sort();
    assert.deepEqual(reloaded, ['A added later', 'from A first', 'from B']);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('REMOVE is not resurrected by the merge-on-write', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-rm-merge-'));
    const sb = new Skillbook(dir);
    const e = sb.apply({ type: 'ADD', entry: { section: 'context', keywords: ['k'], issue: 'i', insight: 'doomed' } });
    sb.apply({ type: 'REMOVE', id: e!.id });
    assert.equal(new Skillbook(dir).getAll().length, 0);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('save leaves no temp files behind and writes a valid atomic file', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-atomic-'));
    const sb = new Skillbook(dir);
    sb.apply({ type: 'ADD', entry: { section: 'context', keywords: ['k'], issue: 'i', insight: 'x' } });
    const files = fs.readdirSync(path.join(dir, '.ami'));
    assert.deepEqual(files, ['skillbook.json']); // no .tmp residue
    const parsed = JSON.parse(fs.readFileSync(path.join(dir, '.ami', 'skillbook.json'), 'utf-8'));
    assert.equal(parsed.version, 1);
    assert.equal(parsed.entries.length, 1);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  // --- Restrictive file permissions on persisted user data ---

  it('persists the skillbook file owner-only (0o600) and its dir owner-only (0o700)', { skip: process.platform === 'win32' }, () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sb-perms-'));
    const sb = new Skillbook(dir); // .ami not pre-created, so saveToDisk mkdirs it
    sb.apply({ type: 'ADD', entry: { section: 'context', keywords: ['k'], issue: 'i', insight: 'secret' } });
    const fileMode = fs.statSync(path.join(dir, '.ami', 'skillbook.json')).mode & 0o777;
    const dirMode = fs.statSync(path.join(dir, '.ami')).mode & 0o777;
    assert.equal(fileMode, 0o600);
    assert.equal(dirMode, 0o700);
    fs.rmSync(dir, { recursive: true, force: true });
  });
});
