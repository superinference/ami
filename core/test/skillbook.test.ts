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
    fs.mkdirSync(path.join(tmpDir, '.superinference'), { recursive: true });
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
    const siDir = path.join(corruptDir, '.superinference');
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
});
