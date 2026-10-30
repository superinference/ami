import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { readMemoryTool } from '../src/tools/read-memory';
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
});
