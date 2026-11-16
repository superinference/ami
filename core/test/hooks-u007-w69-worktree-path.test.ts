/**
 * U007 (w69) — WorktreeCreate provider hook: the hook may supply the filesystem
 * path of the worktree it created, from stdout (first non-empty line) or
 * `hookSpecificOutput.worktreePath`. executeWorktreeCreate returns the FIRST
 * non-empty path so the caller uses it as the created worktree instead of the
 * default `.ami/worktrees/<slug>` location (Claude Code's executeWorktreeCreateHook).
 *
 * Covers gaps:
 *   - WorktreeCreate hook returning `worktreePath` (stdout / hookSpecificOutput.worktreePath)
 *   - WorktreeCreate hook returns the worktree path from hook stdout (first successful non-empty output)
 *   - WorktreeCreate hook output worktreePath (types.ts hookSpecificOutput.worktreePath)
 *
 * RED (before the fix): executeWorktreeCreate returned void (discarded hook output)
 * and WorktreeHookFn had no return channel, so a hook-provided worktreePath had no
 * effect — all the `worktreePath` assertions below would be `undefined`.
 */
import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { HookManager, extractWorktreePath, type WorktreeContext } from '../src/hooks';

function writeTmpHooks(entries: unknown[]): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-wt-'));
  const dir = path.join(tmpDir, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return tmpDir;
}

describe('extractWorktreePath', () => {
  it('reads the first non-empty trimmed stdout line (command hook echo)', () => {
    assert.equal(extractWorktreePath('\n  /tmp/custom/wt \n'), '/tmp/custom/wt');
  });

  it('reads hookSpecificOutput.worktreePath from JSON output (http hook)', () => {
    const out = JSON.stringify({ hookSpecificOutput: { hookEventName: 'WorktreeCreate', worktreePath: '/srv/wt-42' } });
    assert.equal(extractWorktreePath(out), '/srv/wt-42');
  });

  it('reads a flat top-level worktreePath from JSON output', () => {
    assert.equal(extractWorktreePath(JSON.stringify({ worktreePath: '/flat/wt' })), '/flat/wt');
  });

  it('returns undefined for empty / whitespace-only output (observe-only)', () => {
    assert.equal(extractWorktreePath(''), undefined);
    assert.equal(extractWorktreePath('   \n  \n'), undefined);
  });

  it('returns undefined for JSON without a worktreePath', () => {
    assert.equal(extractWorktreePath(JSON.stringify({ continue: true })), undefined);
  });
});

describe('HookManager.executeWorktreeCreate — provider return channel', () => {
  let hm: HookManager;
  beforeEach(() => { hm = new HookManager(); });

  it('returns the worktreePath a programmatic hook reports', async () => {
    hm.onWorktreeCreate(async (ctx: WorktreeContext) => {
      assert.equal(ctx.name, 'wt1');
      return { worktreePath: '/provided/by/hook' };
    });
    const r = await hm.executeWorktreeCreate({ name: 'wt1', worktreePath: '/default/wt1' });
    assert.deepEqual(r, { worktreePath: '/provided/by/hook' });
  });

  it('returns void when all hooks are observe-only (no worktreePath)', async () => {
    let fired = false;
    hm.onWorktreeCreate(async () => { fired = true; });
    const r = await hm.executeWorktreeCreate({ name: 'wt2', worktreePath: '/default/wt2' });
    assert.equal(fired, true);
    assert.equal(r, undefined);
  });

  it('first non-empty worktreePath wins', async () => {
    hm.onWorktreeCreate(async () => { /* observe-only */ });
    hm.onWorktreeCreate(async () => ({ worktreePath: '  /winner  ' }));
    hm.onWorktreeCreate(async () => ({ worktreePath: '/loser' }));
    const r = await hm.executeWorktreeCreate({ name: 'wt3' });
    assert.deepEqual(r, { worktreePath: '/winner' });
  });

  it('a command WorktreeCreate hook that echoes a path is used as the provider path', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'worktreeCreate', hook: { type: 'command', command: 'echo /tmp/cmd-provided-wt' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    assert.equal(hm.getSummary().worktreeCreate, 1);
    const r = await hm.executeWorktreeCreate({ name: 'cmdwt', worktreePath: '/default/cmdwt' });
    assert.deepEqual(r, { worktreePath: '/tmp/cmd-provided-wt' });
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('a command WorktreeCreate hook with no stdout stays observe-only (void)', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'worktreeCreate', hook: { type: 'command', command: 'true' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const r = await hm.executeWorktreeCreate({ name: 'cmdwt2', worktreePath: '/default/cmdwt2' });
    assert.equal(r, undefined);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});
