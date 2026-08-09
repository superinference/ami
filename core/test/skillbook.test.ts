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
});
