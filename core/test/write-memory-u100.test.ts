import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { writeMemoryTool } from '../src/tools/write-memory';
import { MemoryManager } from '../src/memory';
import type { ToolContext } from '../src/types';

let tmp: string;
const ctx = () => ({ cwd: tmp, abortSignal: new AbortController().signal } as unknown as ToolContext);

beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'write-mem-u100-')); });
afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

describe('write_memory — skipIndex one-step vs two-step save (gap: teamMemPrompts skipIndex branch)', () => {
  it('exposes skipIndex as a boolean input-schema property', () => {
    const props = writeMemoryTool.inputSchema.properties as Record<string, { type?: string }>;
    assert.equal(props.skipIndex?.type, 'boolean');
  });

  it('default (two-step) writes the memory AND a MEMORY.md index pointer', async () => {
    const r = await writeMemoryTool.execute({ name: 'deploy-cmd', content: 'run deploy.sh' }, ctx());
    assert.notEqual(r.isError, true);
    assert.match(String(r.output), /indexed it in MEMORY\.md/);
    const indexPath = path.join(new MemoryManager(tmp).getMemoryDir(), 'MEMORY.md');
    assert.ok(fs.existsSync(indexPath), 'MEMORY.md index was created');
    assert.match(fs.readFileSync(indexPath, 'utf-8'), /\[deploy-cmd\]\(deploy-cmd\.md\)/);
  });

  it('skipIndex=true does a ONE-step save — file written, no index pointer, no index note', async () => {
    const r = await writeMemoryTool.execute(
      { name: 'one-step', content: 'file only', skipIndex: true },
      ctx(),
    );
    assert.notEqual(r.isError, true);
    assert.doesNotMatch(String(r.output), /indexed it in MEMORY\.md/);
    // The topic file is still persisted and readable back.
    assert.ok(new MemoryManager(tmp).readMemory('one-step') !== null, 'topic file persisted');
    // ...but no MEMORY.md index pointer was appended.
    const indexPath = path.join(new MemoryManager(tmp).getMemoryDir(), 'MEMORY.md');
    assert.equal(fs.existsSync(indexPath), false, 'no index file for a one-step save');
  });

  it('a non-true skipIndex value is treated as the default two-step save', async () => {
    const r = await writeMemoryTool.execute(
      { name: 'coerce', content: 'c', skipIndex: 'yes' as unknown as boolean },
      ctx(),
    );
    assert.match(String(r.output), /indexed it in MEMORY\.md/);
  });
});

describe('write_memory — scope-aware secret rejection (gap: teamMemSecretGuard filePath scope)', () => {
  it('a team-scoped secret leak names the collaborator-sharing consequence', async () => {
    const r = await writeMemoryTool.execute(
      { name: 'teamleak', content: 'token ghp_abcdefghijklmnopqrstuvwxyz0123456789', scope: 'team' },
      ctx(),
    );
    assert.equal(r.isError, true);
    assert.match(String(r.output), /Team memory is shared with all repository collaborators/);
    assert.equal(new MemoryManager(tmp).loadTeamMemories().length, 0, 'nothing persisted to team');
  });

  it('a private-scoped secret leak uses the generic persistence wording (not the team message)', async () => {
    const r = await writeMemoryTool.execute(
      { name: 'privleak', content: 'token ghp_abcdefghijklmnopqrstuvwxyz0123456789' },
      ctx(),
    );
    assert.equal(r.isError, true);
    assert.match(String(r.output), /Memories persist across sessions and may be shared/);
    assert.doesNotMatch(String(r.output), /repository collaborators/);
  });
});

describe('write_memory — count-bearing confirmation (gap: teamMemSaved team/private split)', () => {
  it('a private save reports the private count (singular) and suppresses the team segment', async () => {
    const r = await writeMemoryTool.execute({ name: 'first', content: 'c' }, ctx());
    assert.match(String(r.output), /You now have 1 private memory\. View with \/memory\./);
    assert.doesNotMatch(String(r.output), /team memor/);
  });

  it('pluralizes the private count after a second save', async () => {
    const c = ctx();
    await writeMemoryTool.execute({ name: 'a', content: 'c' }, c);
    const r = await writeMemoryTool.execute({ name: 'b', content: 'c' }, c);
    assert.match(String(r.output), /You now have 2 private memories/);
  });

  it('a team save surfaces both private and team counts with correct pluralization', async () => {
    const c = ctx();
    await writeMemoryTool.execute({ name: 'p1', content: 'c' }, c);
    const r = await writeMemoryTool.execute(
      { name: 't1', content: 'shared', scope: 'team', type: 'reference' },
      c,
    );
    assert.match(String(r.output), /You now have 1 private memory and 1 team memory/);
  });

  it('a team-only save reports zero private memories and one team memory', async () => {
    const r = await writeMemoryTool.execute(
      { name: 'only-team', content: 'shared', scope: 'team' },
      ctx(),
    );
    assert.match(String(r.output), /You now have 0 private memories and 1 team memory/);
  });
});

describe('MemoryManager.countMemoriesByScope', () => {
  it('returns zeros when no memory directory exists yet', () => {
    assert.deepEqual(new MemoryManager(tmp).countMemoriesByScope(), { private: 0, team: 0 });
  });

  it('counts private and team topic files, excluding the MEMORY.md index', () => {
    const mgr = new MemoryManager(tmp);
    mgr.saveMemory('priv-a', 'a', undefined, undefined, 'private', true);
    mgr.saveMemory('priv-b', 'b', undefined, undefined, 'private', true);
    mgr.saveMemory('team-a', 'c', undefined, undefined, 'team', true);
    const counts = mgr.countMemoriesByScope();
    assert.deepEqual(counts, { private: 2, team: 1 });
    // The index pointer file itself is NOT counted as a memory.
    assert.ok(fs.existsSync(path.join(mgr.getMemoryDir(), 'MEMORY.md')));
  });
});
