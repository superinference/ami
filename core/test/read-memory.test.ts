import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { readMemoryTool, memoryFreshnessNote } from '../src/tools/read-memory';
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
