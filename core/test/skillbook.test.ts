import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as fs from 'fs';
import * as path from 'path';
import { Skillbook } from '../src/skillbook';

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
});
