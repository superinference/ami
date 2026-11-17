import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';

import {
  generateTmuxSessionName,
  createTmuxSessionForWorktree,
  killTmuxSession,
  createWorktreeSession,
  gitExec,
  setWorktreeSession,
  _tmuxHooks,
} from '../src/worktree-manager';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { exitWorktreeTool } from '../src/tools/exit-worktree';
import { validateProjectConfigValues } from '../src/config';
import type { ProjectConfig } from '../src/config';
import type { ToolContext } from '../src/types';

const GIT_ENV = {
  GIT_TERMINAL_PROMPT: '0',
  GIT_ASKPASS: '',
  GIT_AUTHOR_NAME: 'Test',
  GIT_AUTHOR_EMAIL: 'test@test.com',
  GIT_COMMITTER_NAME: 'Test',
  GIT_COMMITTER_EMAIL: 'test@test.com',
};

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8', env: { ...process.env, ...GIT_ENV } }).trim();
}

function makeRepo(): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'u057-tmux-')));
  git(dir, 'init', '-b', 'main');
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# repo\n');
  git(dir, 'add', 'README.md');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

function writeConfig(repo: string, cfg: unknown): void {
  fs.mkdirSync(path.join(repo, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(repo, '.ami', 'config.json'), JSON.stringify(cfg));
}

function ctx(repo: string, overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: repo, abortSignal: new AbortController().signal, ...overrides } as ToolContext;
}

const LAUNCH_CWD = process.cwd();

// ---------------------------------------------------------------------------
// U057 — generateTmuxSessionName (pure; parity: utils/worktree.ts generateTmuxSessionName)
// ---------------------------------------------------------------------------
describe('U057 tmux: generateTmuxSessionName', () => {
  it('basenames the repo path and replaces / and . with _', () => {
    assert.equal(generateTmuxSessionName('/home/me/my.repo', 'worktree-feat'), 'my_repo_worktree-feat');
  });
  it('uses only the final path component (never the full path)', () => {
    const name = generateTmuxSessionName('/a/b/c/project', 'worktree/x');
    assert.equal(name, 'project_worktree_x');
    assert.ok(!name.includes('/'));
  });
  it('replaces every . so tmux will accept the name', () => {
    assert.ok(!generateTmuxSessionName('repo', 'v1.2.3').includes('.'));
  });
});

// ---------------------------------------------------------------------------
// U057 — create/kill tmux session via the DI seam (no real tmux needed)
// ---------------------------------------------------------------------------
describe('U057 tmux: createTmuxSessionForWorktree / killTmuxSession', () => {
  afterEach(() => mock.restoreAll());

  it('createTmuxSessionForWorktree spawns a detached session and reports created', () => {
    const calls: string[][] = [];
    mock.method(_tmuxHooks, 'run', (args: string[]) => { calls.push(args); return { ok: true }; });
    const res = createTmuxSessionForWorktree('sess', '/wt/path');
    assert.deepEqual(res, { created: true });
    assert.deepEqual(calls[0], ['new-session', '-d', '-s', 'sess', '-c', '/wt/path']);
  });

  it('createTmuxSessionForWorktree surfaces the error when tmux fails', () => {
    mock.method(_tmuxHooks, 'run', () => ({ ok: false, error: 'tmux: not found' }));
    const res = createTmuxSessionForWorktree('sess', '/wt/path');
    assert.equal(res.created, false);
    assert.equal(res.error, 'tmux: not found');
  });

  it('killTmuxSession returns true/false from the seam and targets the session', () => {
    const calls: string[][] = [];
    mock.method(_tmuxHooks, 'run', (args: string[]) => { calls.push(args); return { ok: true }; });
    assert.equal(killTmuxSession('sess'), true);
    assert.deepEqual(calls[0], ['kill-session', '-t', 'sess']);
    mock.restoreAll();
    mock.method(_tmuxHooks, 'run', () => ({ ok: false, error: 'no such session' }));
    assert.equal(killTmuxSession('gone'), false);
  });

  it('_tmuxHooks.run returns ok:false (never throws) when tmux is absent', () => {
    // The real default seam must swallow a spawn failure — exercise it with an
    // argv that makes tmux exit non-zero if present, or ENOENT if absent.
    const res = _tmuxHooks.run(['kill-session', '-t', 'definitely-not-a-real-session-xyz']);
    assert.equal(typeof res.ok, 'boolean');
    assert.equal(res.ok, false);
  });
});

// ---------------------------------------------------------------------------
// U057 GAP8 — AbortSignal threading through the worktree-manager git runner
// ---------------------------------------------------------------------------
describe('U057 GAP8: gitExec / createWorktreeSession honor AbortSignal', () => {
  let repo: string;
  beforeEach(() => { repo = makeRepo(); });
  afterEach(() => {
    try { process.chdir(LAUNCH_CWD); } catch { /* ignore */ }
    setWorktreeSession(null);
    try { fs.rmSync(repo, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('gitExec throws immediately when the signal is already aborted', () => {
    const ac = new AbortController();
    ac.abort();
    assert.throws(() => gitExec(['rev-parse', 'HEAD'], { cwd: repo, signal: ac.signal }));
  });

  it('createWorktreeSession aborts the worktree-add and leaves no worktree behind', () => {
    const ac = new AbortController();
    ac.abort();
    const name = 'aborted-wt';
    assert.throws(() => createWorktreeSession(repo, name, { signal: ac.signal }));
    // The teardown must remove the half-created dir (no debris).
    assert.equal(fs.existsSync(path.join(repo, '.ami', 'worktrees', name)), false);
  });

  it('createWorktreeSession succeeds with a live (un-aborted) signal', () => {
    const ac = new AbortController();
    const session = createWorktreeSession(repo, 'live-wt', { signal: ac.signal });
    assert.ok(fs.existsSync(session.worktreePath));
  });
});

// ---------------------------------------------------------------------------
// U057 — enter_worktree / exit_worktree tmux lifecycle (end-to-end through tools)
// ---------------------------------------------------------------------------
describe('U057 tmux: enter_worktree + exit_worktree lifecycle', () => {
  let repo: string;
  beforeEach(() => { repo = makeRepo(); });
  afterEach(() => {
    mock.restoreAll();
    try { process.chdir(LAUNCH_CWD); } catch { /* ignore */ }
    setWorktreeSession(null);
    try { fs.rmSync(repo, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('creates a tmux session + attach hint when settings.worktree.tmux is enabled', async () => {
    writeConfig(repo, { worktree: { tmux: true } });
    const runCalls: string[][] = [];
    mock.method(_tmuxHooks, 'run', (args: string[]) => { runCalls.push(args); return { ok: true }; });

    const res = await enterWorktreeTool.execute({ name: 'feat' }, ctx(repo));
    assert.equal(res.isError, undefined);
    assert.match(res.output, /Created tmux session: /);
    assert.match(res.output, /To attach: tmux attach -t /);
    // The session was spawned detached, rooted at the worktree.
    assert.equal(runCalls[0][0], 'new-session');
    assert.equal(runCalls[0][1], '-d');
    // The session name is recorded for teardown/reattach.
    assert.ok((res.metadata as { tmuxSessionName?: string }).tmuxSessionName);
  });

  it('surfaces a non-fatal warning (and no session name) when tmux creation fails', async () => {
    writeConfig(repo, { worktree: { tmux: true } });
    mock.method(_tmuxHooks, 'run', () => ({ ok: false, error: 'tmux: command not found' }));
    const res = await enterWorktreeTool.execute({ name: 'feat' }, ctx(repo));
    // The worktree is still created (warning only, not an error).
    assert.equal(res.isError, undefined);
    assert.match(res.output, /Warning: Failed to create tmux session: tmux: command not found/);
    assert.equal((res.metadata as { tmuxSessionName?: string }).tmuxSessionName, undefined);
  });

  it('does NOT create a tmux session when the knob is absent (default off)', async () => {
    const run = mock.method(_tmuxHooks, 'run', () => ({ ok: true }));
    const res = await enterWorktreeTool.execute({ name: 'feat' }, ctx(repo));
    assert.equal(res.isError, undefined);
    // No tmux session line is emitted (the repo path itself may contain the temp
    // prefix, so assert on the feature's own wording, not the bare word).
    assert.doesNotMatch(res.output, /Created tmux session|Failed to create tmux session|tmux attach -t/);
    assert.equal(run.mock.callCount(), 0);
  });

  it('keep surfaces the reattach command; remove kills the tmux session', async () => {
    writeConfig(repo, { worktree: { tmux: true } });
    let created: string | undefined;
    mock.method(_tmuxHooks, 'run', (args: string[]) => {
      if (args[0] === 'new-session') created = args[3];
      return { ok: true };
    });
    const enter = await enterWorktreeTool.execute({ name: 'feat' }, ctx(repo));
    const wtPath = (enter.metadata as { worktreePath: string }).worktreePath;
    assert.ok(created);

    // KEEP: session left running, reattach hint surfaced, no kill issued.
    const killBefore: string[][] = [];
    mock.restoreAll();
    mock.method(_tmuxHooks, 'run', (args: string[]) => { if (args[0] === 'kill-session') killBefore.push(args); return { ok: true }; });
    const keep = await exitWorktreeTool.execute({ action: 'keep' }, ctx(wtPath));
    assert.match(keep.output, new RegExp(`reattach with: tmux attach -t ${created}`));
    assert.equal((keep.metadata as { tmuxSessionName?: string }).tmuxSessionName, created);
    assert.equal(killBefore.length, 0, 'keep must not kill the tmux session');
  });

  it('remove kills the tmux session before tearing down the worktree', async () => {
    writeConfig(repo, { worktree: { tmux: true } });
    let created: string | undefined;
    const killed: string[] = [];
    mock.method(_tmuxHooks, 'run', (args: string[]) => {
      if (args[0] === 'new-session') created = args[3];
      if (args[0] === 'kill-session') killed.push(args[2]);
      return { ok: true };
    });
    const enter = await enterWorktreeTool.execute({ name: 'feat' }, ctx(repo));
    const wtPath = (enter.metadata as { worktreePath: string }).worktreePath;

    const remove = await exitWorktreeTool.execute({ action: 'remove' }, ctx(wtPath));
    assert.equal(remove.isError, undefined);
    assert.deepEqual(killed, [created], 'remove must kill exactly the worktree tmux session');
    assert.equal(fs.existsSync(wtPath), false);
  });
});

// ---------------------------------------------------------------------------
// U057 — config schema accepts/validates worktree.tmux
// ---------------------------------------------------------------------------
describe('U057: settings.worktree.tmux config validation', () => {
  it('accepts a boolean worktree.tmux', () => {
    const issues = validateProjectConfigValues({ worktree: { tmux: true } } as ProjectConfig);
    assert.equal(issues.some(i => i.field === 'worktree.tmux'), false);
  });
  it('rejects a non-boolean worktree.tmux with a typed issue', () => {
    const issues = validateProjectConfigValues({ worktree: { tmux: 'yes' } } as unknown as ProjectConfig);
    const issue = issues.find(i => i.field === 'worktree.tmux');
    assert.ok(issue, 'expected a worktree.tmux issue');
    assert.match(issue!.message, /boolean/);
  });
});

// ---------------------------------------------------------------------------
// U057 — usedSparsePaths persisted on the session (parity: WorktreeSession.usedSparsePaths)
// ---------------------------------------------------------------------------
describe('U057: usedSparsePaths field', () => {
  let repo: string;
  beforeEach(() => { repo = makeRepo(); });
  afterEach(() => {
    try { process.chdir(LAUNCH_CWD); } catch { /* ignore */ }
    setWorktreeSession(null);
    try { fs.rmSync(repo, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('is false for a normal full-checkout worktree', async () => {
    const { getCurrentWorktreeSession } = await import('../src/worktree-manager');
    await enterWorktreeTool.execute({ name: 'full' }, ctx(repo));
    assert.equal(getCurrentWorktreeSession()?.usedSparsePaths, false);
  });

  it('is true when settings.worktree.sparsePaths materialises a partial checkout', async () => {
    writeConfig(repo, { worktree: { sparsePaths: ['README.md'] } });
    const { getCurrentWorktreeSession } = await import('../src/worktree-manager');
    const res = await enterWorktreeTool.execute({ name: 'sparse' }, ctx(repo));
    assert.equal(res.isError, undefined);
    assert.equal(getCurrentWorktreeSession()?.usedSparsePaths, true);
  });
});
