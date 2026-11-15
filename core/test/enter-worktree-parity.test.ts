import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { setWorktreeSession, MAX_WORKTREES } from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8' }).trim();
}

function makeRepo(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ewp-'));
  git(dir, 'init', '-q');
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  git(dir, 'add', 'README.md');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

const ctx = (over: Partial<ToolContext> = {}) =>
  ({ cwd: process.cwd(), abortSignal: new AbortController().signal, ...over } as ToolContext);

describe('enter_worktree — additive parity hardening', () => {
  let repo: string;
  beforeEach(() => { repo = makeRepo(); setWorktreeSession(null); });
  afterEach(() => { fs.rmSync(repo, { recursive: true, force: true }); setWorktreeSession(null); });

  // --- GAP 1: strict input (unknown-parameter rejection) ------------------
  // RED (revert): unknown keys are silently ignored and the worktree is created
  // → isError undefined. GREEN: the unknown key is rejected loudly.
  it('rejects unknown parameters instead of silently dropping them', async () => {
    const r = await enterWorktreeTool.execute(
      { name: 'ok', bogus: 1, another: 'x' } as Record<string, unknown>,
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /unknown parameter\(s\): bogus, another/);
    // Nothing should have been created.
    assert.equal(fs.existsSync(path.join(repo, '.ami', 'worktrees', 'ok')), false);
  });

  it('still accepts the full allowed key set (name/base_ref)', async () => {
    const r = await enterWorktreeTool.execute(
      { name: 'allk', base_ref: 'HEAD' },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Worktree created/);
  });

  // --- GAP 2: MAX_WORKTREES cap enforcement -------------------------------
  // RED (revert): with MAX_WORKTREES managed worktrees already present, a new
  // create still succeeds. GREEN: it is refused with a "limit reached" message.
  it('refuses to create a new worktree once MAX_WORKTREES already exist', async () => {
    const base = path.join(repo, '.ami', 'worktrees');
    fs.mkdirSync(base, { recursive: true });
    for (let i = 0; i < MAX_WORKTREES; i++) {
      fs.mkdirSync(path.join(base, `occupied-${i}`));
    }
    const r = await enterWorktreeTool.execute({ name: 'overflow' }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /worktree limit reached/);
    assert.match(r.output, new RegExp(`${MAX_WORKTREES}/${MAX_WORKTREES}`));
    // The overflow worktree must NOT have been created.
    assert.equal(fs.existsSync(path.join(base, 'overflow')), false);
  });

  it('allows creation when below the MAX_WORKTREES ceiling', async () => {
    const base = path.join(repo, '.ami', 'worktrees');
    fs.mkdirSync(base, { recursive: true });
    for (let i = 0; i < MAX_WORKTREES - 1; i++) {
      fs.mkdirSync(path.join(base, `occupied-${i}`));
    }
    const r = await enterWorktreeTool.execute({ name: 'headroom' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Worktree created/);
    assert.equal(fs.existsSync(path.join(base, 'headroom')), true);
  });

  // --- GAP 3: reject base_ref combined with path --------------------------
  // RED (revert): base_ref is silently ignored and the existing worktree is
  // entered → "Entered existing worktree", isError undefined. GREEN: rejected.
  it('rejects base_ref when entering an existing worktree via path', async () => {
    const created = await enterWorktreeTool.execute({ name: 'enterme' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined);
    setWorktreeSession(null);
    const wtPath = path.join(repo, '.ami', 'worktrees', 'enterme');

    const r = await enterWorktreeTool.execute(
      { path: wtPath, base_ref: 'HEAD' },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /base_ref cannot be combined with path/);
  });

  it('path without base_ref still enters an existing worktree', async () => {
    const created = await enterWorktreeTool.execute({ name: 'plainenter' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined);
    setWorktreeSession(null);
    const wtPath = path.join(repo, '.ami', 'worktrees', 'plainenter');
    const r = await enterWorktreeTool.execute({ path: wtPath }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Entered existing worktree/);
  });

  // --- GAP 4: name is trimmed ---------------------------------------------
  // RED (revert): " spaced " keeps its surrounding spaces, fails slug
  // validation → "invalid worktree name". GREEN: trimmed to "spaced", created.
  it('trims surrounding whitespace on the name before validation', async () => {
    const r = await enterWorktreeTool.execute({ name: '  spaced  ' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    assert.match(r.output, /Worktree created/);
    assert.equal(fs.existsSync(path.join(repo, '.ami', 'worktrees', 'spaced')), true);
  });

  it('treats a whitespace-only name as omitted (generates a random name)', async () => {
    const r = await enterWorktreeTool.execute({ name: '   ' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    assert.match(r.output, /Worktree created/);
    assert.match(r.output, /wt-[0-9a-f]{8}/);
  });
});
