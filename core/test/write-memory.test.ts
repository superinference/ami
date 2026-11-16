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

  it('blocks a write whose content contains a detected secret (and does not persist it)', async () => {
    const c = ctx();
    const r = await writeMemoryTool.execute(
      { name: 'leak', content: 'the api key is sk-ant-abcdefghijklmnopqrstuvwxyz0123' },
      c,
    );
    assert.equal(r.isError, true);
    assert.match(String(r.output), /refusing to store/);
    assert.match(String(r.output), /secret/);
    // The secret value itself must NOT be echoed back.
    assert.doesNotMatch(String(r.output), /sk-ant-abcdefghijklmnopqrstuvwxyz/);
    // Nothing was written.
    assert.equal(new MemoryManager(tmp).readMemory('leak'), null);
  });

  it('names the detected credential TYPE in the rejection (not just a count, never the value)', async () => {
    const c = ctx();
    const r = await writeMemoryTool.execute(
      { name: 'leak3', content: 'remember this: ghp_abcdefghijklmnopqrstuvwxyz0123456789' },
      c,
    );
    assert.equal(r.isError, true);
    // The matched rule TYPE (brand-correct label) is surfaced so the model knows
    // what kind of secret to remove — this discriminates the fix from the old
    // count-only message ("API key / token / credential").
    assert.match(String(r.output), /GitHub PAT/);
    // The secret value itself must still NEVER be echoed.
    assert.doesNotMatch(String(r.output), /ghp_abcdefghij/);
    assert.equal(new MemoryManager(tmp).readMemory('leak3'), null);
  });

  it('lists each distinct credential type for mixed matches', async () => {
    const c = ctx();
    const r = await writeMemoryTool.execute(
      { name: 'leak4', content: 'sk-ant-abcdefghijklmnopqrstuvwxyz0123\nghp_abcdefghijklmnopqrstuvwxyz0123456789' },
      c,
    );
    assert.equal(r.isError, true);
    assert.match(String(r.output), /Anthropic Key/);
    assert.match(String(r.output), /GitHub PAT/);
  });

  it('pluralizes the secret count for multiple matches', async () => {
    const c = ctx();
    const r = await writeMemoryTool.execute(
      { name: 'leak2', content: 'sk-ant-abcdefghijklmnopqrstuvwxyz0123\nghp_abcdefghijklmnopqrstuvwxyz0123456789' },
      c,
    );
    assert.equal(r.isError, true);
    assert.match(String(r.output), /secrets/);
  });

  it('description documents secret exclusion, dedup, and body structure guidance', () => {
    const d = writeMemoryTool.description;
    assert.match(d, /Do NOT store secrets/);
    assert.match(d, /even when the user explicitly asks/);
    assert.match(d, /UPDATE an existing entry/);
    assert.match(d, /How to apply/);
  });

  // ── scope (private vs team) on the WRITE path ──────────────────────────
  it('defaults to private scope and surfaces the saved file path', async () => {
    const r = await writeMemoryTool.execute({ name: 'deploy-cmd', content: 'run deploy.sh' }, ctx());
    assert.notEqual(r.isError, true);
    // scope named + the concrete saved file path surfaced
    assert.match(String(r.output), /\(private\)/);
    assert.match(String(r.output), /deploy-cmd\.md/);
    // a private memory goes to the auto-memory root (readMemory reads the root)
    assert.ok(new MemoryManager(tmp).readMemory('deploy-cmd') !== null, 'private memory persisted at root');
    // and NOT into the team dir
    assert.equal(new MemoryManager(tmp).loadTeamMemories().length, 0, 'team dir must stay empty for a private write');
  });

  it('scope=team routes the write into the shared team directory (authorable team memory)', async () => {
    const r = await writeMemoryTool.execute(
      { name: 'ci-runner', content: 'CI uses the arm64 pool', scope: 'team', type: 'reference' },
      ctx(),
    );
    assert.notEqual(r.isError, true);
    assert.match(String(r.output), /\(team, type: reference\)/);
    // The saved path lands under a team/ segment
    assert.match(String(r.output), /[/\\]team[/\\]ci-runner\.md/);
    // loadTeamMemories (the read side) now sees the authored team memory
    const team = new MemoryManager(tmp).loadTeamMemories();
    assert.equal(team.length, 1, 'team memory is readable back');
    assert.match(team[0], /arm64 pool/);
    // It is NOT counted as a private root memory
    assert.equal(new MemoryManager(tmp).readMemory('ci-runner'), null, 'team write must not land in the private root');
  });

  it('an unrecognized scope falls back to private (never mis-routes to team)', async () => {
    const r = await writeMemoryTool.execute(
      { name: 'safe', content: 'body', scope: 'everyone' },
      ctx(),
    );
    assert.notEqual(r.isError, true);
    assert.match(String(r.output), /\(private\)/);
    assert.equal(new MemoryManager(tmp).loadTeamMemories().length, 0, 'bad scope must not write to team');
    assert.ok(new MemoryManager(tmp).readMemory('safe') !== null);
  });

  it('a secret is blocked even when targeting team scope (never persisted anywhere)', async () => {
    const r = await writeMemoryTool.execute(
      { name: 'teamleak', content: 'token ghp_abcdefghijklmnopqrstuvwxyz0123456789', scope: 'team' },
      ctx(),
    );
    assert.equal(r.isError, true);
    assert.match(String(r.output), /refusing to store/);
    assert.equal(new MemoryManager(tmp).loadTeamMemories().length, 0, 'no team file written for a blocked secret');
  });

  it('exposes scope in the input schema enum', () => {
    const scope = (writeMemoryTool.inputSchema.properties as Record<string, { enum?: string[] }>).scope;
    assert.deepEqual(scope?.enum, ['private', 'team']);
    assert.match(writeMemoryTool.description, /never save .*credentials.*team|NEVER save secrets or user credentials in a team/i);
  });
});
