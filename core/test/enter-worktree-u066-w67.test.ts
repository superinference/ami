import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { enterWorktreeTool, _enterWorktreeHooks } from '../src/tools/enter-worktree';
import { setWorktreeSession } from '../src/worktree-manager';
import { validateProjectConfigValues, salvageManagedConfig } from '../src/config';
import type { ToolContext } from '../src/types';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8' }).trim();
}

function makeRepo(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ew66w67-'));
  git(dir, 'init', '-q');
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  // Two subtrees so cone-mode sparse-checkout can include one and exclude the other.
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  fs.mkdirSync(path.join(dir, 'keep'));
  fs.writeFileSync(path.join(dir, 'keep', 'a.txt'), 'keep me\n');
  fs.mkdirSync(path.join(dir, 'drop'));
  fs.writeFileSync(path.join(dir, 'drop', 'b.txt'), 'drop me\n');
  git(dir, 'add', '-A');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

function writeWorktreeConfig(repo: string, worktree: unknown): void {
  const cfgDir = path.join(repo, '.ami');
  fs.mkdirSync(cfgDir, { recursive: true });
  fs.writeFileSync(path.join(cfgDir, 'config.json'), JSON.stringify({ worktree }));
}

const ctx = (over: Partial<ToolContext> = {}) =>
  ({ cwd: process.cwd(), abortSignal: new AbortController().signal, ...over } as ToolContext);

describe('enter_worktree — U066-w67 (sparsePaths knob + structured outputSchema)', () => {
  const dirs: string[] = [];
  const track = (d: string) => { dirs.push(d); return d; };
  beforeEach(() => { setWorktreeSession(null); });
  afterEach(() => {
    mock.reset();
    for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
    setWorktreeSession(null);
  });

  // ---------------------------------------------------------------------------
  // settings.worktree.sparsePaths — cone-mode sparse checkout
  // ---------------------------------------------------------------------------
  it('a configured sparsePaths list checks out ONLY the listed subtrees', async () => {
    const repo = track(makeRepo());
    writeWorktreeConfig(repo, { sparsePaths: ['keep'] });
    const r = await enterWorktreeTool.execute({ name: 'sparsewt' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'sparsewt');
    // Root files + the listed subtree materialise; the excluded subtree does not.
    assert.equal(fs.existsSync(path.join(wtDir, 'README.md')), true);
    assert.equal(fs.existsSync(path.join(wtDir, 'keep', 'a.txt')), true, 'keep/ should be present');
    assert.equal(fs.existsSync(path.join(wtDir, 'drop', 'b.txt')), false, 'drop/ should be excluded by cone');
  });

  it('NO sparsePaths config performs a normal full checkout', async () => {
    const repo = track(makeRepo());
    const r = await enterWorktreeTool.execute({ name: 'fullwt' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'fullwt');
    assert.equal(fs.existsSync(path.join(wtDir, 'keep', 'a.txt')), true);
    assert.equal(fs.existsSync(path.join(wtDir, 'drop', 'b.txt')), true, 'full checkout keeps every subtree');
  });

  it('an empty sparsePaths list opts out (full checkout, no --no-checkout)', async () => {
    const repo = track(makeRepo());
    writeWorktreeConfig(repo, { sparsePaths: [] });
    const r = await enterWorktreeTool.execute({ name: 'emptywt' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'emptywt');
    assert.equal(fs.existsSync(path.join(wtDir, 'drop', 'b.txt')), true);
  });

  it('tears the half-created worktree down when sparse-checkout fails', async () => {
    const repo = track(makeRepo());
    writeWorktreeConfig(repo, { sparsePaths: ['keep'] });
    const orig = _enterWorktreeHooks.exec;
    // Let `worktree add --no-checkout` succeed; make the cone-narrowing step fail
    // so the worktree is left with an empty working tree and must be rolled back.
    mock.method(_enterWorktreeHooks, 'exec', (args: string[], opts: Parameters<typeof orig>[1]) => {
      if (args[0] === 'sparse-checkout') throw new Error('simulated sparse-checkout failure');
      return orig(args, opts);
    });
    const r = await enterWorktreeTool.execute({ name: 'brokenwt' }, ctx({ cwd: repo }));
    assert.equal(r.isError, true);
    assert.match(r.output, /sparse-checkout setup failed, rolled back/);
    // The half-created worktree directory and its branch were removed.
    const wtDir = path.join(repo, '.ami', 'worktrees', 'brokenwt');
    assert.equal(fs.existsSync(wtDir), false, 'broken worktree dir must be torn down');
    const branches = git(repo, 'branch', '--list', 'worktree/brokenwt');
    assert.equal(branches, '', 'orphan branch must be dropped');
  });

  // ---------------------------------------------------------------------------
  // config validation + salvage wiring for the new knob
  // ---------------------------------------------------------------------------
  it('accepts a valid worktree.sparsePaths list', () => {
    const issues = validateProjectConfigValues({ worktree: { sparsePaths: ['src', 'test'] } });
    assert.deepEqual(issues, []);
  });

  it('flags a non-array worktree.sparsePaths value', () => {
    // @ts-expect-error — deliberately wrong type
    const issues = validateProjectConfigValues({ worktree: { sparsePaths: 'src' } });
    assert.ok(issues.some(i => i.field === 'worktree.sparsePaths'), JSON.stringify(issues));
  });

  it('salvages an invalid sparsePaths while keeping a valid sibling', () => {
    const salvaged = salvageManagedConfig(
      // @ts-expect-error — deliberately wrong sparsePaths type
      { worktree: { symlinkDirectories: ['node_modules'], sparsePaths: 123 } },
      '/tmp/managed.json',
    );
    assert.deepEqual(salvaged.worktree, { symlinkDirectories: ['node_modules'] });
  });

  // ---------------------------------------------------------------------------
  // Per-tool structured outputSchema + metadata (parity: Claude EnterWorktreeTool)
  // ---------------------------------------------------------------------------
  it('declares a structured outputSchema (worktreePath/worktreeBranch/message)', () => {
    const schema = enterWorktreeTool.outputSchema;
    assert.ok(schema, 'outputSchema must be declared');
    assert.equal(schema!.type, 'object');
    assert.ok(schema!.properties?.worktreePath);
    assert.ok(schema!.properties?.worktreeBranch);
    assert.ok(schema!.properties?.message);
    assert.deepEqual(schema!.required, ['worktreePath', 'message']);
  });

  it('create success carries structured metadata matching the text output', async () => {
    const repo = track(makeRepo());
    const r = await enterWorktreeTool.execute({ name: 'metawt' }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'metawt');
    assert.equal(r.metadata?.worktreePath, wtDir);
    assert.equal(r.metadata?.worktreeBranch, 'worktree/metawt');
    assert.equal(r.metadata?.message, r.output);
  });

  it('enter-existing success carries structured metadata with the branch', async () => {
    const repo = track(makeRepo());
    const created = await enterWorktreeTool.execute({ name: 'existingwt' }, ctx({ cwd: repo }));
    assert.equal(created.isError, undefined, created.output);
    setWorktreeSession(null);
    const wtDir = path.join(repo, '.ami', 'worktrees', 'existingwt');
    const r = await enterWorktreeTool.execute({ path: wtDir }, ctx({ cwd: repo }));
    assert.equal(r.isError, undefined, r.output);
    assert.equal(r.metadata?.worktreePath, path.resolve(wtDir));
    assert.equal(r.metadata?.worktreeBranch, 'worktree/existingwt');
    assert.equal(r.metadata?.message, r.output);
  });
});
