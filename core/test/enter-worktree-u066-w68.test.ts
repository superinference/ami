import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { enterWorktreeTool, _enterWorktreeHooks } from '../src/tools/enter-worktree';
import { setWorktreeSession } from '../src/worktree-manager';
import type { ToolContext } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf-8',
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' },
  }).trim();
}

/** A bare-ish "server" repo (acts as origin) with a single commit on `main`. */
function makeServer(): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ew68-srv-')));
  git(dir, 'init', '-q', '-b', 'main');
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  git(dir, 'add', '-A');
  git(dir, 'commit', '-q', '-m', 'c1');
  return dir;
}

function cloneOf(server: string): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ew68-cln-')));
  fs.rmSync(dir, { recursive: true, force: true });
  execFileSync('git', ['clone', '-q', server, dir], { encoding: 'utf-8' });
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  return dir;
}

/** Append a new commit on `main` in `server`, returning its short SHA. */
function newServerCommit(server: string, file: string): string {
  fs.writeFileSync(path.join(server, file), `${file}\n`);
  git(server, 'add', '-A');
  git(server, 'commit', '-q', '-m', `add ${file}`);
  return git(server, 'rev-parse', '--short', 'HEAD');
}

const ctx = (over: Partial<ToolContext> = {}) =>
  ({ cwd: process.cwd(), abortSignal: new AbortController().signal, ...over } as ToolContext);

describe('enter_worktree — U066-w68 (default-base fetch fallback + fs ref read)', () => {
  const dirs: string[] = [];
  const track = (d: string) => { dirs.push(d); return d; };
  beforeEach(() => { setWorktreeSession(null); });
  afterEach(() => {
    mock.reset();
    for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
    setWorktreeSession(null);
  });

  // --- GAP: fetch origin/<default> when the base is absent locally -----------
  // RED (remove the fetch-fallback block): with no local origin/main, the base
  // resolution falls straight to HEAD (the stale client commit) instead of the
  // fetched latest default branch, so the worktree is based on c1, not c2.
  it('stale clone → fetches origin/<default> so the worktree is based on the LATEST default branch', async () => {
    const server = track(makeServer());
    const client = track(cloneOf(server));
    const c1 = git(client, 'rev-parse', '--short', 'HEAD');

    // Strip every locally-resolvable remote base so findRemoteBase() -> null and
    // resolveRef() misses: delete origin/HEAD, drop the remote-tracking ref, and
    // unset the branch upstream.
    git(client, 'remote', 'set-head', 'origin', '-d');
    git(client, 'update-ref', '-d', 'refs/remotes/origin/main');
    try { git(client, 'branch', '--unset-upstream'); } catch { /* may already be unset */ }

    // Advance the server so a fetch produces a DIFFERENT base than the client HEAD.
    const c2 = newServerCommit(server, 'feature.txt');
    assert.notEqual(c1, c2);

    const res = await enterWorktreeTool.execute({ name: 'fetched' }, ctx({ cwd: client }));
    assert.equal(res.isError, undefined, res.output);

    const wtDir = path.join(client, '.ami', 'worktrees', 'fetched');
    const base = git(wtDir, 'rev-parse', '--short', 'HEAD');
    assert.equal(base, c2, `worktree must be based on the fetched latest default (c2=${c2}), not stale HEAD (c1=${c1}); got ${base}`);
    assert.match(res.output, /based on origin\/main/);
  });

  // --- GAP: no network fetch when the base already resolves locally ----------
  // The fs ref read / local base short-circuits before any `git fetch`.
  it('normal clone → base resolves locally, NO fetch subprocess is spawned', async () => {
    const server = track(makeServer());
    const client = track(cloneOf(server));

    const calls: string[][] = [];
    const orig = _enterWorktreeHooks.exec;
    mock.method(_enterWorktreeHooks, 'exec', (args: string[], opts: Parameters<typeof orig>[1]) => {
      calls.push(args);
      return orig(args, opts);
    });

    const res = await enterWorktreeTool.execute({ name: 'local' }, ctx({ cwd: client }));
    assert.equal(res.isError, undefined, res.output);

    const fetched = calls.some(a => a[0] === 'fetch');
    assert.equal(fetched, false, `expected no fetch; exec calls: ${JSON.stringify(calls)}`);
    assert.match(res.output, /based on origin\/main/);
  });

  // --- Explicit base_ref still bypasses the default-base fetch path ----------
  it('explicit base_ref does not trigger the default-base fetch', async () => {
    const server = track(makeServer());
    const client = track(cloneOf(server));

    const calls: string[][] = [];
    const orig = _enterWorktreeHooks.exec;
    mock.method(_enterWorktreeHooks, 'exec', (args: string[], opts: Parameters<typeof orig>[1]) => {
      calls.push(args);
      return orig(args, opts);
    });

    const res = await enterWorktreeTool.execute({ name: 'fromref', base_ref: 'HEAD' }, ctx({ cwd: client }));
    assert.equal(res.isError, undefined, res.output);
    assert.equal(calls.some(a => a[0] === 'fetch'), false, 'base_ref path must not fetch');
  });
});
