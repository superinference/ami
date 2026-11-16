import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { setWorktreeSession, getCurrentWorktreeSession, parsePRReference } from '../src/worktree-manager';
import { validateProjectConfigValues, findUnknownConfigKeys } from '../src/config';
import type { ToolContext } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8' }).trim();
}

function makeRepo(initArgs: string[] = ['init', '-q']): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ew66w66-'));
  git(dir, ...initArgs);
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

describe('enter_worktree — U066-w66 parity (PR refs + symlinkDirectories knob)', () => {
  const dirs: string[] = [];
  const track = (d: string) => { dirs.push(d); return d; };
  beforeEach(() => { setWorktreeSession(null); });
  afterEach(() => {
    for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
    setWorktreeSession(null);
  });

  // ---------------------------------------------------------------------------
  // parsePRReference (shared helper, worktree-manager)
  // ---------------------------------------------------------------------------
  describe('parsePRReference', () => {
    it('parses a GitHub PR URL', () => {
      assert.equal(parsePRReference('https://github.com/owner/repo/pull/123'), 123);
    });
    it('parses a GHE (any-host) PR URL with a trailing slash + query', () => {
      assert.equal(parsePRReference('https://ghe.example.com/o/r/pull/7/?x=1'), 7);
    });
    it('parses the #N shorthand', () => {
      assert.equal(parsePRReference('#42'), 42);
    });
    it('returns null for a non-PR string', () => {
      assert.equal(parsePRReference('main'), null);
      assert.equal(parsePRReference('https://github.com/o/r/issues/5'), null);
      assert.equal(parsePRReference('#abc'), null);
    });
  });

  // ---------------------------------------------------------------------------
  // PR-based worktree creation (parity: getOrCreateWorktree({prNumber}))
  // ---------------------------------------------------------------------------
  // Build a repo whose `origin` carries refs/pull/5/head at a distinct commit.
  function repoWithPR(prNum: number): { repo: string; prSha: string; base: string } {
    const remote = track(fs.mkdtempSync(path.join(os.tmpdir(), 'ew66w66-origin-')));
    git(remote, 'init', '-q', '--bare');
    const repo = track(makeRepo());
    const base = git(repo, 'rev-parse', '--abbrev-ref', 'HEAD');
    git(repo, 'remote', 'add', 'origin', remote);
    git(repo, 'push', '-q', 'origin', `${base}:${base}`);
    // A distinct PR commit pushed only to refs/pull/<N>/head on origin.
    git(repo, 'checkout', '-q', '-b', 'prwork');
    fs.writeFileSync(path.join(repo, 'pr.txt'), 'from pr\n');
    git(repo, 'add', 'pr.txt');
    git(repo, 'commit', '-q', '-m', 'pr change');
    const prSha = git(repo, 'rev-parse', 'HEAD');
    git(repo, 'push', '-q', 'origin', `HEAD:refs/pull/${prNum}/head`);
    // Return to base and drop the local branch so the PR head lives only on origin.
    git(repo, 'checkout', '-q', base);
    git(repo, 'branch', '-q', '-D', 'prwork');
    return { repo, prSha, base };
  }

  it('creates a worktree based on a PR (#N), auto-named pr-N', async () => {
    const { repo, prSha } = repoWithPR(5);
    const r = await enterWorktreeTool.execute({ pr: '#5' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'pr-5');
    assert.equal(fs.existsSync(wtDir), true, `worktree dir missing: ${r.output}`);
    // The worktree HEAD is the PR commit, and the PR's file is present.
    assert.equal(git(wtDir, 'rev-parse', 'HEAD'), prSha);
    assert.equal(fs.existsSync(path.join(wtDir, 'pr.txt')), true);
    assert.match(r.output, /based on PR #5/);
    assert.match(r.output, /worktree\/pr-5/);
  });

  it('accepts a full PR URL and respects an explicit name over pr-N', async () => {
    const { repo, prSha } = repoWithPR(9);
    const r = await enterWorktreeTool.execute(
      { pr: 'https://github.com/o/r/pull/9', name: 'review' },
      ctx({ cwd: repo }),
    );
    assert.equal(r.isError, undefined, r.output);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'review');
    assert.equal(git(wtDir, 'rev-parse', 'HEAD'), prSha);
    // pr-N must NOT be used when a name is given.
    assert.equal(fs.existsSync(path.join(repo, '.ami', 'worktrees', 'pr-9')), false);
  });

  it('errors with actionable wording when the PR fetch fails (no such PR)', async () => {
    const { repo } = repoWithPR(1);
    const r = await enterWorktreeTool.execute({ pr: '#999' }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /failed to fetch PR #999/);
    assert.match(r.output, /may not exist|remote named "origin"/);
    // No worktree was created.
    assert.equal(fs.existsSync(path.join(repo, '.ami', 'worktrees', 'pr-999')), false);
  });

  it('rejects an unrecognized pr value before any git work', async () => {
    const repo = track(makeRepo());
    const r = await enterWorktreeTool.execute({ pr: 'not-a-pr' }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /invalid pr "not-a-pr"/);
  });

  it('rejects pr combined with path', async () => {
    const repo = track(makeRepo());
    const r = await enterWorktreeTool.execute({ pr: '#1', path: '/tmp/x' }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /pr cannot be combined with path/);
  });

  it('rejects pr combined with base_ref', async () => {
    const repo = track(makeRepo());
    const r = await enterWorktreeTool.execute({ pr: '#1', base_ref: 'main' }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /pr and base_ref are mutually exclusive/);
  });

  it('rejects pr in the non-git hook path (cannot fetch a PR without git)', async () => {
    const dir = track(fs.mkdtempSync(path.join(os.tmpdir(), 'ew66w66-nogit-')));
    const fakeHooks = { getSummary: () => ({ worktreeCreate: 1 }) } as unknown as NonNullable<ToolContext['_hookManager']>;
    const r = await enterWorktreeTool.execute({ pr: '#1' }, ctx({ cwd: dir, _hookManager: fakeHooks }));
    assert.equal(r.isError, true);
    assert.match(r.output, /pr requires a git repository/);
  });

  // ---------------------------------------------------------------------------
  // settings.worktree.symlinkDirectories knob
  // ---------------------------------------------------------------------------
  function writeWorktreeConfig(repo: string, symlinkDirectories: unknown): void {
    const cfgDir = path.join(repo, '.ami');
    fs.mkdirSync(cfgDir, { recursive: true });
    fs.writeFileSync(path.join(cfgDir, 'config.json'), JSON.stringify({ worktree: { symlinkDirectories } }));
  }

  it('honors a configured symlinkDirectories list (and skips the default node_modules)', async () => {
    const repo = track(makeRepo());
    fs.mkdirSync(path.join(repo, 'vendor'));
    fs.writeFileSync(path.join(repo, 'vendor', 'lib.txt'), 'x');
    fs.mkdirSync(path.join(repo, 'node_modules'));
    fs.writeFileSync(path.join(repo, 'node_modules', 'pkg.txt'), 'y');
    writeWorktreeConfig(repo, ['vendor']);
    const r = await enterWorktreeTool.execute({ name: 'cfgwt' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'cfgwt');
    // vendor is symlinked; node_modules is NOT (the configured list overrides the default).
    assert.equal(fs.lstatSync(path.join(wtDir, 'vendor')).isSymbolicLink(), true);
    assert.equal(fs.existsSync(path.join(wtDir, 'node_modules')), false);
  });

  it('an empty symlinkDirectories list symlinks nothing (opt-out)', async () => {
    const repo = track(makeRepo());
    fs.mkdirSync(path.join(repo, 'node_modules'));
    fs.writeFileSync(path.join(repo, 'node_modules', 'pkg.txt'), 'y');
    writeWorktreeConfig(repo, []);
    const r = await enterWorktreeTool.execute({ name: 'emptywt' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'emptywt');
    assert.equal(fs.existsSync(path.join(wtDir, 'node_modules')), false);
  });

  it('with NO worktree config, keeps the AMI default of symlinking node_modules', async () => {
    const repo = track(makeRepo());
    fs.mkdirSync(path.join(repo, 'node_modules'));
    fs.writeFileSync(path.join(repo, 'node_modules', 'pkg.txt'), 'y');
    const r = await enterWorktreeTool.execute({ name: 'defwt' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'defwt');
    assert.equal(fs.lstatSync(path.join(wtDir, 'node_modules')).isSymbolicLink(), true);
  });

  // ---------------------------------------------------------------------------
  // config validation wiring for the new worktree knob
  // ---------------------------------------------------------------------------
  it('worktree is a recognized config key (no unknown-key warning)', () => {
    assert.deepEqual(findUnknownConfigKeys({ worktree: { symlinkDirectories: ['a'] } }), []);
  });

  it('flags a non-array worktree.symlinkDirectories value', () => {
    // @ts-expect-error — deliberately wrong type
    const issues = validateProjectConfigValues({ worktree: { symlinkDirectories: 'node_modules' } });
    assert.ok(issues.some(i => i.field === 'worktree.symlinkDirectories'), JSON.stringify(issues));
  });

  it('accepts a valid worktree.symlinkDirectories list', () => {
    const issues = validateProjectConfigValues({ worktree: { symlinkDirectories: ['vendor', 'node_modules'] } });
    assert.deepEqual(issues, []);
  });
});
