import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  writeMemoryTool,
  MAX_MEMORY_BODY_LINES,
  MAX_MEMORY_BODY_BYTES,
} from '../src/tools/write-memory';
import { MemoryManager } from '../src/memory';
import type { ToolContext } from '../src/types';

let tmp: string;
const ctx = () => ({ cwd: tmp, abortSignal: new AbortController().signal } as unknown as ToolContext);

beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'write-mem-w75-')); });
afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

describe('write_memory — content size cap (gap: memdir MAX_ENTRYPOINT size knobs)', () => {
  it('rejects a body exceeding the line cap, naming the line cap, and persists nothing', async () => {
    const big = Array.from({ length: MAX_MEMORY_BODY_LINES + 1 }, (_, i) => `line ${i}`).join('\n');
    const r = await writeMemoryTool.execute({ name: 'toolong', content: big }, ctx());
    assert.equal(r.isError, true);
    assert.match(String(r.output), /too large/);
    assert.match(String(r.output), new RegExp(`${MAX_MEMORY_BODY_LINES}-line cap`));
    assert.equal(new MemoryManager(tmp).readMemory('toolong'), null, 'oversized memory must not persist');
  });

  it('rejects a body exceeding the byte cap, naming the byte cap', async () => {
    // One long line: under the line cap but over the byte cap.
    const big = 'x'.repeat(MAX_MEMORY_BODY_BYTES + 1);
    const r = await writeMemoryTool.execute({ name: 'toobig', content: big }, ctx());
    assert.equal(r.isError, true);
    assert.match(String(r.output), new RegExp(`${MAX_MEMORY_BODY_BYTES}-byte cap`));
    assert.equal(new MemoryManager(tmp).readMemory('toobig'), null);
  });

  it('accepts a body exactly at the line cap (boundary — not rejected)', async () => {
    const atCap = Array.from({ length: MAX_MEMORY_BODY_LINES }, (_, i) => `l${i}`).join('\n');
    const r = await writeMemoryTool.execute({ name: 'atcap', content: atCap }, ctx());
    assert.notEqual(r.isError, true);
    assert.ok(new MemoryManager(tmp).readMemory('atcap') !== null);
  });
});

describe('write_memory — typed filesystem-error handling (gap: FileWriteTool typed errors)', () => {
  it('returns a structured actionable error (not a raw errno) when the write throws', async (t) => {
    // Force the I/O boundary to throw an EACCES-tagged error.
    const err = Object.assign(new Error('EACCES: permission denied'), { code: 'EACCES' });
    t.mock.method(MemoryManager.prototype, 'saveMemory', () => { throw err; });
    const r = await writeMemoryTool.execute({ name: 'denied', content: 'c' }, ctx());
    assert.equal(r.isError, true);
    assert.match(String(r.output), /could not save memory "denied"/);
    assert.match(String(r.output), /permission denied writing the memory directory/);
  });

  it('maps ENOSPC to a no-space hint', async (t) => {
    const err = Object.assign(new Error('ENOSPC: no space'), { code: 'ENOSPC' });
    t.mock.method(MemoryManager.prototype, 'saveMemory', () => { throw err; });
    const r = await writeMemoryTool.execute({ name: 'full', content: 'c' }, ctx());
    assert.equal(r.isError, true);
    assert.match(String(r.output), /no space left on device/);
  });

  it('falls back to the raw message for an unmapped error class', async (t) => {
    t.mock.method(MemoryManager.prototype, 'saveMemory', () => { throw new Error('weird failure'); });
    const r = await writeMemoryTool.execute({ name: 'weird', content: 'c' }, ctx());
    assert.equal(r.isError, true);
    assert.match(String(r.output), /weird failure/);
  });

  it('handles a non-Error throw value without crashing', async (t) => {
    t.mock.method(MemoryManager.prototype, 'saveMemory', () => { throw 'string failure'; });
    const r = await writeMemoryTool.execute({ name: 'strthrow', content: 'c' }, ctx());
    assert.equal(r.isError, true);
    assert.match(String(r.output), /string failure/);
  });
});

describe('write_memory — update-vs-new clobber reporting (gap: existing-file clobber guard)', () => {
  it('reports "Saved" for a brand-new memory', async () => {
    const r = await writeMemoryTool.execute({ name: 'fresh', content: 'body' }, ctx());
    assert.match(String(r.output), /^Saved memory "fresh"/);
  });

  it('reports "Updated" when the same name is written again (in-place replace)', async () => {
    const c = ctx();
    await writeMemoryTool.execute({ name: 'dup', content: 'first body' }, c);
    const r = await writeMemoryTool.execute({ name: 'dup', content: 'second body' }, c);
    assert.match(String(r.output), /^Updated memory "dup"/);
    // Count is unchanged — it replaced, did not add.
    assert.match(String(r.output), /You now have 1 private memory/);
    // And the new body is what persists.
    assert.match(String(new MemoryManager(tmp).readMemory('dup')), /second body/);
  });

  it('reports "Saved" (not "Updated") for a distinct second memory', async () => {
    const c = ctx();
    await writeMemoryTool.execute({ name: 'one', content: 'a' }, c);
    const r = await writeMemoryTool.execute({ name: 'two', content: 'b' }, c);
    assert.match(String(r.output), /^Saved memory "two"/);
    assert.match(String(r.output), /You now have 2 private memories/);
  });
});
