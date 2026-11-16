/**
 * U007 (w69) — end-to-end: a WorktreeCreate *provider* hook supplies the path of
 * the worktree it created, and enter_worktree uses that path (session switches
 * into it) instead of the default `.ami/worktrees/<slug>` location.
 *
 * RED (revert the enter-worktree `effectiveDir`/`executeWorktreeCreate` change):
 *   the tool ignored the hook-returned worktreePath and required the default
 *   `.ami/worktrees/<slug>` dir to exist — so with a hook that creates a DIFFERENT
 *   custom dir, the tool would error "did not create <default>" and never switch
 *   the session into the custom path.
 * GREEN: the hook's returned worktreePath overrides the default; the session and
 *   the output report the custom path.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { setWorktreeSession, getCurrentWorktreeSession } from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

const ctx = (over: Partial<ToolContext> = {}) =>
  ({ cwd: process.cwd(), abortSignal: new AbortController().signal, ...over } as ToolContext);

describe('enter_worktree — U007 w69 WorktreeCreate provider path', () => {
  const dirs: string[] = [];
  beforeEach(() => { setWorktreeSession(null); });
  afterEach(() => {
    for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
    setWorktreeSession(null);
  });
  const track = (d: string) => { dirs.push(d); return d; };
  const nonGitDir = () => track(fs.mkdtempSync(path.join(os.tmpdir(), 'ew-u007-nogit-')));

  it('uses the hook-provided worktreePath as the created worktree (custom location)', async () => {
    const dir = nonGitDir();
    const customRoot = track(fs.mkdtempSync(path.join(os.tmpdir(), 'ew-u007-custom-')));
    const customDir = path.join(customRoot, 'provider-wt');
    let received: { name?: string; worktreePath?: string } | undefined;
    const hookManager = {
      getSummary: () => ({ worktreeCreate: 1 }),
      executeWorktreeCreate: async (c: { name?: string; worktreePath?: string }) => {
        received = c;
        fs.mkdirSync(customDir, { recursive: true });
        return { worktreePath: customDir };
      },
      executeCwdChanged: async () => {},
    } as unknown as NonNullable<ToolContext['_hookManager']>;

    const context = ctx({ cwd: dir, _hookManager: hookManager });
    const r = await enterWorktreeTool.execute({ name: 'provwt' }, context);
    assert.equal(r.isError, undefined, r.output);
    // The hook was still passed the DEFAULT suggestion, but its returned path wins.
    assert.equal(received?.worktreePath, path.join(dir, '.ami', 'worktrees', 'provwt'));
    assert.match(r.output, new RegExp(customDir.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
    assert.equal((r.metadata as { worktreePath?: string } | undefined)?.worktreePath, customDir);
    // Session switched into the hook-provided custom dir, not the default slug dir.
    const sess = getCurrentWorktreeSession();
    assert.equal(sess?.worktreePath, customDir);
    assert.equal(sess?.hookBased, true);
    assert.equal((context as unknown as { cwd: string }).cwd, customDir);
  });

  it('errors naming the hook-provided path when the hook did not create it', async () => {
    const dir = nonGitDir();
    const ghost = path.join(os.tmpdir(), 'ew-u007-ghost', 'never-made');
    const hookManager = {
      getSummary: () => ({ worktreeCreate: 1 }),
      executeWorktreeCreate: async () => ({ worktreePath: ghost }),
      executeCwdChanged: async () => {},
    } as unknown as NonNullable<ToolContext['_hookManager']>;
    const r = await enterWorktreeTool.execute({ name: 'ghostwt' }, ctx({ cwd: dir, _hookManager: hookManager }));
    assert.equal(r.isError, true);
    assert.ok(r.output.includes(path.resolve(ghost)), r.output);
    assert.equal(getCurrentWorktreeSession(), null);
  });

  it('falls back to the default slug dir when the hook returns no path (observe-only)', async () => {
    const dir = nonGitDir();
    const defaultDir = path.join(dir, '.ami', 'worktrees', 'obswt');
    const hookManager = {
      getSummary: () => ({ worktreeCreate: 1 }),
      executeWorktreeCreate: async (c: { worktreePath?: string }) => { fs.mkdirSync(c.worktreePath!, { recursive: true }); },
      executeCwdChanged: async () => {},
    } as unknown as NonNullable<ToolContext['_hookManager']>;
    const context = ctx({ cwd: dir, _hookManager: hookManager });
    const r = await enterWorktreeTool.execute({ name: 'obswt' }, context);
    assert.equal(r.isError, undefined, r.output);
    assert.equal(getCurrentWorktreeSession()?.worktreePath, defaultDir);
  });
});
