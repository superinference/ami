import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  readMemoryTool,
  memoryFreshnessNote,
  capMemoryBody,
  sliceMemoryBody,
  suggestMemoryNames,
  MAX_READ_MEMORY_LINES,
  MAX_READ_MEMORY_BYTES,
} from '../src/tools/read-memory';
import { MemoryManager } from '../src/memory';
import type { ToolContext } from '../src/types';

let tmpDir: string;
const ctx = (cwd: string) => ({ cwd } as unknown as ToolContext);

describe('read_memory tool', () => {
  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-readmem-')); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns the full body of a named memory', async () => {
    new MemoryManager(tmpDir).saveMemory('deploy-notes', 'Deploy via the container runtime only.', 'deployment', 'project');
    const res = await readMemoryTool.execute({ name: 'deploy-notes' }, ctx(tmpDir));
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('Deploy via the container runtime only.'));
    assert.ok(res.output.includes('## Memory: deploy-notes'));
  });

  it('errors on an empty name', async () => {
    const res = await readMemoryTool.execute({ name: '   ' }, ctx(tmpDir));
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('must not be empty'));
  });

  it('errors on a non-string name', async () => {
    const res = await readMemoryTool.execute({}, ctx(tmpDir));
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('must not be empty'));
  });

  it('errors when the named memory does not exist', async () => {
    const res = await readMemoryTool.execute({ name: 'ghost' }, ctx(tmpDir));
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('no stored memory named "ghost"'));
  });

  it('is read-only and concurrency-safe', () => {
    assert.equal(readMemoryTool.isReadOnly, true);
    assert.equal(readMemoryTool.isConcurrencySafe, true);
  });

  it('fresh memory (<=1 day) carries no staleness note', async () => {
    new MemoryManager(tmpDir).saveMemory('recent', 'fresh fact', 'd', 'project');
    const res = await readMemoryTool.execute({ name: 'recent' }, ctx(tmpDir));
    assert.doesNotMatch(res.output, /Note: this memory was last updated/);
  });

  it('a stale memory (>1 day old) carries a freshness caveat', async () => {
    const mm = new MemoryManager(tmpDir);
    mm.saveMemory('old-fact', 'possibly outdated', 'd', 'project');
    // Backdate the file mtime so it reads as stale.
    const dir = mm.getMemoryDir();
    const file = path.join(dir, 'old-fact.md');
    const old = Date.now() - 5 * 24 * 60 * 60 * 1000;
    fs.utimesSync(file, new Date(old), new Date(old));
    const res = await readMemoryTool.execute({ name: 'old-fact' }, ctx(tmpDir));
    assert.match(res.output, /Note: this memory was last updated 5 days ago/);
  });

  it('caps an over-long memory body and names the line cap that fired', async () => {
    const big = Array.from({ length: MAX_READ_MEMORY_LINES + 50 }, (_, i) => `line-${i}`).join('\n');
    new MemoryManager(tmpDir).saveMemory('huge', big, 'd', 'project');
    const res = await readMemoryTool.execute({ name: 'huge' }, ctx(tmpDir));
    assert.equal(res.isError, undefined);
    assert.match(res.output, new RegExp(`truncated to the ${MAX_READ_MEMORY_LINES}-line cap`));
    assert.ok(res.output.includes('re-read with offset/limit'));
    // The overflow lines past the cap are not present.
    assert.ok(!res.output.includes(`line-${MAX_READ_MEMORY_LINES + 10}`));
  });

  it('pages a memory body via offset and limit', async () => {
    const body = ['alpha', 'bravo', 'charlie', 'delta', 'echo'].join('\n');
    new MemoryManager(tmpDir).saveMemory('phonetic', body, 'd', 'project');
    const res = await readMemoryTool.execute({ name: 'phonetic', offset: 1, limit: 2 }, ctx(tmpDir));
    assert.ok(res.output.includes('bravo'));
    assert.ok(res.output.includes('charlie'));
    assert.ok(!res.output.includes('alpha'));
    assert.ok(!res.output.includes('delta'));
  });

  it('ignores invalid offset/limit values and returns the whole body', async () => {
    const body = ['one', 'two', 'three'].join('\n');
    new MemoryManager(tmpDir).saveMemory('nums', body, 'd', 'project');
    const res = await readMemoryTool.execute(
      { name: 'nums', offset: -1, limit: 0 },
      ctx(tmpDir),
    );
    assert.ok(res.output.includes('one'));
    assert.ok(res.output.includes('two'));
    assert.ok(res.output.includes('three'));
  });

  it('suggests a near-match and lists available names when the name is not found', async () => {
    const mm = new MemoryManager(tmpDir);
    mm.saveMemory('deploy-notes', 'body', 'd', 'project');
    mm.saveMemory('build-notes', 'body', 'd', 'project');
    const res = await readMemoryTool.execute({ name: 'deploy-note' }, ctx(tmpDir));
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Did you mean "deploy-notes"?'));
    assert.ok(res.output.includes('Available memories:'));
    assert.ok(res.output.includes('build-notes'));
  });

  it('lists available names without a near-match when nothing is close', async () => {
    new MemoryManager(tmpDir).saveMemory('alpha', 'body', 'd', 'project');
    const res = await readMemoryTool.execute({ name: 'zzzzzzzz' }, ctx(tmpDir));
    assert.equal(res.isError, true);
    assert.ok(!res.output.includes('Did you mean'));
    assert.ok(res.output.includes('Available memories: alpha.'));
  });

  it('gives a bare not-found error when no memories exist at all', async () => {
    const res = await readMemoryTool.execute({ name: 'ghost' }, ctx(tmpDir));
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('no stored memory named "ghost"'));
    assert.ok(!res.output.includes('Available memories'));
    assert.ok(!res.output.includes('Did you mean'));
  });

  describe('capMemoryBody', () => {
    it('returns content unchanged when within both caps', () => {
      assert.equal(capMemoryBody('short body'), 'short body');
    });
    it('truncates on the line cap with a named warning', () => {
      const body = Array.from({ length: MAX_READ_MEMORY_LINES + 5 }, () => 'x').join('\n');
      const out = capMemoryBody(body);
      assert.match(out, new RegExp(`${MAX_READ_MEMORY_LINES}-line cap`));
      assert.equal(out.split('\n').length, MAX_READ_MEMORY_LINES + 2); // + blank + warning line
    });
    it('truncates on the byte cap with a named warning', () => {
      const body = 'x'.repeat(MAX_READ_MEMORY_BYTES + 1000); // one line, over the byte cap
      const out = capMemoryBody(body);
      assert.match(out, new RegExp(`${MAX_READ_MEMORY_BYTES}-byte cap`));
    });
  });

  describe('sliceMemoryBody', () => {
    const body = ['a', 'b', 'c', 'd'].join('\n');
    it('returns the body unchanged with no offset/limit', () => {
      assert.equal(sliceMemoryBody(body), body);
    });
    it('applies offset only', () => {
      assert.equal(sliceMemoryBody(body, 2), 'c\nd');
    });
    it('applies limit only', () => {
      assert.equal(sliceMemoryBody(body, undefined, 2), 'a\nb');
    });
    it('applies offset and limit together', () => {
      assert.equal(sliceMemoryBody(body, 1, 2), 'b\nc');
    });
  });

  describe('suggestMemoryNames', () => {
    it('returns empty when there are no names', () => {
      assert.equal(suggestMemoryNames('x', []), '');
      assert.equal(suggestMemoryNames('x', ['  ']), '');
    });
    it('suggests a close name and lists all', () => {
      const out = suggestMemoryNames('deploi-notes', ['deploy-notes', 'other']);
      assert.ok(out.includes('Did you mean "deploy-notes"?'));
      assert.ok(out.includes('Available memories: deploy-notes, other.'));
    });
    it('omits the near-match when nothing is within threshold', () => {
      const out = suggestMemoryNames('qqqqqqqq', ['alpha']);
      assert.ok(!out.includes('Did you mean'));
      assert.ok(out.includes('Available memories: alpha.'));
    });
  });

  describe('memoryFreshnessNote', () => {
    it('returns empty for undefined, 0, or 1 day', () => {
      assert.equal(memoryFreshnessNote(undefined), '');
      assert.equal(memoryFreshnessNote(0), '');
      assert.equal(memoryFreshnessNote(1), '');
    });
    it('returns a note for ages over a day', () => {
      assert.match(memoryFreshnessNote(2), /2 days ago/);
    });
  });
});
