import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { forgetMemoryTool } from '../src/tools/forget-memory';
import { writeMemoryTool } from '../src/tools/write-memory';
import { MemoryManager, memoryFileSlug } from '../src/memory';
import type { ToolContext } from '../src/types';

let tmp: string;
const ctx = () => ({ cwd: tmp, abortSignal: new AbortController().signal } as unknown as ToolContext);

beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'forget-mem-u100-')); });
afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

describe('forget_memory — delete-by-name affordance (gap: extractMemories forget)', () => {
  it('is registered with a required name input', () => {
    assert.equal(forgetMemoryTool.name, 'forget_memory');
    assert.equal(forgetMemoryTool.isReadOnly, false);
    const schema = forgetMemoryTool.inputSchema as { required?: string[] };
    assert.deepEqual(schema.required, ['name']);
  });

  it('rejects an empty name', async () => {
    const r = await forgetMemoryTool.execute({ name: '  ' }, ctx());
    assert.equal(r.isError, true);
    assert.match(String(r.output), /name must not be empty/);
  });

  it('removes the topic file AND prunes the MEMORY.md index pointer (two-step inverse)', async () => {
    const c = ctx();
    await writeMemoryTool.execute({ name: 'deploy-cmd', content: 'run deploy.sh' }, c);
    const mgr = new MemoryManager(tmp);
    const indexPath = path.join(mgr.getMemoryDir(), 'MEMORY.md');
    const filePath = path.join(mgr.getMemoryDir(), 'deploy-cmd.md');
    assert.ok(fs.existsSync(filePath), 'topic file written');
    assert.match(fs.readFileSync(indexPath, 'utf-8'), /deploy-cmd\.md/);

    const r = await forgetMemoryTool.execute({ name: 'deploy-cmd' }, c);
    assert.notEqual(r.isError, true);
    assert.match(String(r.output), /Forgot 1 memory named "deploy-cmd" \(private\)/);
    assert.equal(fs.existsSync(filePath), false, 'topic file deleted');
    assert.doesNotMatch(fs.readFileSync(indexPath, 'utf-8'), /deploy-cmd\.md/, 'index pointer pruned');
  });

  it('reports an error and removes nothing when no memory matches', async () => {
    const c = ctx();
    await writeMemoryTool.execute({ name: 'keep-me', content: 'keep' }, c);
    const r = await forgetMemoryTool.execute({ name: 'ghost' }, c);
    assert.equal(r.isError, true);
    assert.match(String(r.output), /No memory named "ghost" was found/);
    assert.equal(new MemoryManager(tmp).readMemory('keep-me') !== null, true, 'unrelated memory untouched');
  });

  it('forgets a team-scoped memory and surfaces the team scope', async () => {
    const c = ctx();
    await writeMemoryTool.execute({ name: 'shared-ref', content: 'see board', scope: 'team' }, c);
    assert.equal(new MemoryManager(tmp).loadTeamMemories().length, 1);
    const r = await forgetMemoryTool.execute({ name: 'shared-ref' }, c);
    assert.notEqual(r.isError, true);
    assert.match(String(r.output), /\(team\)/);
    assert.equal(new MemoryManager(tmp).loadTeamMemories().length, 0, 'team memory removed');
  });

  it('reports the remaining count-by-scope after a delete', async () => {
    const c = ctx();
    await writeMemoryTool.execute({ name: 'a', content: 'x' }, c);
    await writeMemoryTool.execute({ name: 'b', content: 'y' }, c);
    const r = await forgetMemoryTool.execute({ name: 'a' }, c);
    assert.match(String(r.output), /You now have 1 private memory\. View with \/memory\./);
  });
});

describe('MemoryManager.deleteMemory', () => {
  it('returns an empty list when nothing matches', () => {
    assert.deepEqual(new MemoryManager(tmp).deleteMemory('nope'), []);
  });

  it('matches by frontmatter name even when the slug differs, and is idempotent', () => {
    const mgr = new MemoryManager(tmp);
    mgr.saveMemory('Foo Bar', 'body', undefined, undefined, 'private', true);
    const removed = mgr.deleteMemory('Foo Bar');
    assert.equal(removed.length, 1);
    assert.equal(removed[0].scope, 'private');
    // Second call finds nothing — idempotent.
    assert.deepEqual(mgr.deleteMemory('Foo Bar'), []);
  });

  it('does not remove an unrelated memory that normalizes to a different slug', () => {
    const mgr = new MemoryManager(tmp);
    mgr.saveMemory('alpha', 'a', undefined, undefined, 'private', true);
    mgr.saveMemory('beta', 'b', undefined, undefined, 'private', true);
    mgr.deleteMemory('alpha');
    assert.equal(mgr.readMemory('alpha'), null);
    assert.notEqual(mgr.readMemory('beta'), null);
  });
});

describe('memoryFileSlug — shared slug helper (No-Duplication)', () => {
  it('matches the filename saveMemory derives', () => {
    const mgr = new MemoryManager(tmp);
    const p = mgr.saveMemory('Deploy Command!', 'x');
    assert.equal(path.basename(p), `${memoryFileSlug('Deploy Command!')}.md`);
  });
});
