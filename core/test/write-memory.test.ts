import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { writeMemoryTool } from '../src/tools/write-memory';
import { readMemoryTool } from '../src/tools/read-memory';
import { MemoryManager } from '../src/memory';
import type { ToolContext } from '../src/types';

let tmp: string;
const ctx = () => ({ cwd: tmp, abortSignal: new AbortController().signal } as unknown as ToolContext);

beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'write-mem-')); });
afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

describe('write_memory tool', () => {
  it('persists a memory that read_memory can read back', async () => {
    const r = await writeMemoryTool.execute({ name: 'deploy-cmd', content: 'run deploy.sh' }, ctx());
    assert.notEqual(r.isError, true);
    assert.match(String(r.output), /Saved memory "deploy-cmd"/);
    const back = await readMemoryTool.execute({ name: 'deploy-cmd' }, ctx());
    assert.match(String(back.output), /run deploy\.sh/);
  });

  it('rejects an empty name', async () => {
    const r = await writeMemoryTool.execute({ name: '  ', content: 'x' }, ctx());
    assert.equal(r.isError, true);
    assert.match(String(r.output), /name must not be empty/);
  });

  it('rejects empty content', async () => {
    const r = await writeMemoryTool.execute({ name: 'n', content: '   ' }, ctx());
    assert.equal(r.isError, true);
    assert.match(String(r.output), /content must not be empty/);
  });

  it('accepts a valid type and records it', async () => {
    const r = await writeMemoryTool.execute({ name: 'pref', content: 'c', type: 'user', description: 'a pref' }, ctx());
    assert.match(String(r.output), /type: user/);
    assert.ok(new MemoryManager(tmp).readMemory('pref') !== null, 'memory persisted');
  });

  it('ignores an invalid type (falls back to default — no type suffix)', async () => {
    const r = await writeMemoryTool.execute({ name: 'x', content: 'c', type: 'bogus' }, ctx());
    assert.notEqual(r.isError, true);
    assert.doesNotMatch(String(r.output), /type:/);
  });

  it('treats a non-string name as empty (error)', async () => {
    const r = await writeMemoryTool.execute({ name: 123 as unknown as string, content: 'x' }, ctx());
    assert.equal(r.isError, true);
  });

  it('treats non-string content as empty (error)', async () => {
    const r = await writeMemoryTool.execute({ name: 'n', content: 42 as unknown as string }, ctx());
    assert.equal(r.isError, true);
  });

  it('treats a blank description as absent', async () => {
    const r = await writeMemoryTool.execute({ name: 'd', content: 'c', description: '   ' }, ctx());
    assert.notEqual(r.isError, true);
  });

  it('is a non-read-only write tool', () => {
    assert.equal(writeMemoryTool.isReadOnly, false);
    assert.equal(writeMemoryTool.name, 'write_memory');
  });
});
