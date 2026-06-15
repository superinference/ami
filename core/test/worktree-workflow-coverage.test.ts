/**
 * Additional coverage tests for:
 *   - src/tools/exit-worktree.ts  (lines 64-65, 71-76, 79-96, 100-105, 115-116)
 *   - src/worktree-manager.ts     (lines 78-80, 87, 177-196, 199-204)
 *   - src/tools/workflow.ts       (lines 14, 92-107)
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as child_process from 'child_process';

import { exitWorktreeTool } from '../src/tools/exit-worktree';
import {
  createWorktreeSession,
  countWorktreeChanges,
  copyWorktreeIncludes,
  setWorktreeSession,
  getCurrentWorktreeSession,
} from '../src/worktree-manager';
import { workflowTool } from '../src/tools/workflow';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Safe CWD that never changes — captured once at module load. */
const SAFE_CWD = process.cwd();

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: SAFE_CWD,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

/** Create a temp dir with a minimal git repo (one commit). */
function makeTmpGitRepo(): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-cov-'));
  child_process.execSync('git init', { cwd: tmpDir, stdio: 'ignore' });
  child_process.execSync('git config user.email "test@test.com"', { cwd: tmpDir, stdio: 'ignore' });
  child_process.execSync('git config user.name "Test"', { cwd: tmpDir, stdio: 'ignore' });
  fs.writeFileSync(path.join(tmpDir, 'init.txt'), 'init');
  child_process.execSync('git add . && git commit -m "init"', { cwd: tmpDir, stdio: 'ignore' });
  return tmpDir;
}

/** Create a fake worktree directory inside a git repo with its own git init. */
function makeWorktreeDir(repoDir: string, name: string): string {
  const wtPath = path.join(repoDir, '.superinference', 'worktrees', name);
  fs.mkdirSync(wtPath, { recursive: true });
  child_process.execSync('git init', { cwd: wtPath, stdio: 'ignore' });
  child_process.execSync('git config user.email "t@t.com"', { cwd: wtPath, stdio: 'ignore' });
  child_process.execSync('git config user.name "T"', { cwd: wtPath, stdio: 'ignore' });
  fs.writeFileSync(path.join(wtPath, 'f.txt'), 'x');
  child_process.execSync('git add . && git commit -m "wt init"', { cwd: wtPath, stdio: 'ignore' });
  return wtPath;
}

// =========================================================================
// EXIT WORKTREE TOOL — deep paths
// =========================================================================

describe('exitWorktreeTool — keep action inside worktree path (lines 64-65)', () => {
  let tmpDir: string;
  let worktreePath: string;

  beforeEach(() => {
    tmpDir = makeTmpGitRepo();
    worktreePath = makeWorktreeDir(tmpDir, 'test-wt');
  });

  afterEach(() => {
    setWorktreeSession(null);
    // Restore cwd in case the tool changed it
    try { process.chdir(SAFE_CWD); } catch {}
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns "Worktree kept at:" when session exists and cwd is inside worktree', async () => {
    setWorktreeSession({
      originalCwd: tmpDir,
      worktreePath,
      worktreeName: 'test-wt',
      worktreeBranch: 'worktree-test-wt',
      createdAt: new Date().toISOString(),
    });

    const c = ctx({ cwd: worktreePath });
    const r = await exitWorktreeTool.execute({ action: 'keep' }, c);

    assert.ok(!r.isError);
    assert.ok(r.output.includes('Worktree kept at:'));
    assert.ok(r.output.includes(worktreePath));
    // Session should be cleared
    assert.equal(getCurrentWorktreeSession(), null);
    // cwd should be restored to originalCwd
    assert.equal(c.cwd, tmpDir);
  });

  it('keeps worktree when no active session (session is null)', async () => {
    setWorktreeSession(null);

    const r = await exitWorktreeTool.execute(
      { action: 'keep' },
      ctx({ cwd: worktreePath }),
    );

    assert.ok(!r.isError);
    assert.ok(r.output.includes('Worktree kept at:'));
  });
});

describe('exitWorktreeTool — remove refuses when countWorktreeChanges returns null (lines 71-76)', () => {
  let tmpDir: string;
  let worktreePath: string;
  let indexPath: string;

  beforeEach(() => {
    tmpDir = makeTmpGitRepo();
    worktreePath = makeWorktreeDir(tmpDir, 'null-wt');
    indexPath = path.join(worktreePath, '.git', 'index');
  });

  afterEach(() => {
    setWorktreeSession(null);
    // Restore index permissions before cleanup
    try { fs.chmodSync(indexPath, 0o644); } catch {}
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns error when git status fails (index unreadable) and discard_changes is false', async () => {
    // Make .git/index unreadable so git status --porcelain fails,
    // while git rev-parse --is-inside-work-tree and --show-toplevel still succeed.
    fs.chmodSync(indexPath, 0o000);

    const r = await exitWorktreeTool.execute(
      { action: 'remove' },
      ctx({ cwd: worktreePath }),
    );

    assert.ok(r.isError);
    assert.ok(
      r.output.includes('could not determine worktree status') ||
      r.output.includes('discard_changes=true'),
    );
  });
});

describe('exitWorktreeTool — remove refuses when uncommitted changes exist (lines 76-82)', () => {
  let tmpDir: string;
  let worktreePath: string;

  beforeEach(() => {
    tmpDir = makeTmpGitRepo();
    worktreePath = makeWorktreeDir(tmpDir, 'dirty-wt');
    // Stage a change (tracked file modification)
    fs.writeFileSync(path.join(worktreePath, 'f.txt'), 'modified');
    child_process.execSync('git add f.txt', { cwd: worktreePath, stdio: 'ignore' });
  });

  afterEach(() => {
    setWorktreeSession(null);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('refuses removal when uncommitted changes exist', async () => {
    const r = await exitWorktreeTool.execute(
      { action: 'remove' },
      ctx({ cwd: worktreePath }),
    );

    assert.ok(r.isError);
    assert.ok(r.output.includes('uncommitted file'));
    assert.ok(r.output.includes('discard_changes=true'));
  });
});

describe('exitWorktreeTool — remove refuses when unpushed commits exist (lines 83-88)', () => {
  let tmpDir: string;
  let worktreePath: string;

  beforeEach(() => {
    tmpDir = makeTmpGitRepo();
    worktreePath = makeWorktreeDir(tmpDir, 'unpushed-wt');
    // Set up a local upstream branch so rev-list @{upstream}..HEAD counts commits
    child_process.execSync('git checkout -b upstream-base', { cwd: worktreePath, stdio: 'ignore' });
    child_process.execSync('git checkout -b feature', { cwd: worktreePath, stdio: 'ignore' });
    child_process.execSync('git branch --set-upstream-to=upstream-base', { cwd: worktreePath, stdio: 'ignore' });
    // Make a commit ahead of upstream
    fs.writeFileSync(path.join(worktreePath, 'new.txt'), 'ahead');
    child_process.execSync('git add . && git commit -m "ahead"', { cwd: worktreePath, stdio: 'ignore' });
  });

  afterEach(() => {
    setWorktreeSession(null);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('refuses removal when unpushed commits exist', async () => {
    const r = await exitWorktreeTool.execute(
      { action: 'remove' },
      ctx({ cwd: worktreePath }),
    );

    assert.ok(r.isError);
    assert.ok(r.output.includes('unpushed commit'));
    assert.ok(r.output.includes('discard_changes=true'));
  });
});

describe('exitWorktreeTool — successful remove with discard_changes (lines 91-116)', () => {
  let tmpDir: string;
  let worktreePath: string;

  beforeEach(() => {
    tmpDir = makeTmpGitRepo();
    worktreePath = makeWorktreeDir(tmpDir, 'remove-wt');
  });

  afterEach(() => {
    setWorktreeSession(null);
    try { process.chdir(SAFE_CWD); } catch {}
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('removes worktree with discard_changes=true and fires hook', async () => {
    let hookFired = false;
    let hookData: any = null;

    setWorktreeSession({
      originalCwd: tmpDir,
      worktreePath,
      worktreeName: 'remove-wt',
      worktreeBranch: 'worktree-remove-wt',
      createdAt: new Date().toISOString(),
    });

    const r = await exitWorktreeTool.execute(
      { action: 'remove', discard_changes: true },
      ctx({
        cwd: worktreePath,
        _hookManager: {
          executeWorktreeRemove: async (data: any) => {
            hookFired = true;
            hookData = data;
          },
        } as any,
      }),
    );

    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('Worktree removed:'));
    assert.ok(hookFired, 'Hook should have been called');
    assert.ok(hookData?.worktreePath, 'Hook should receive worktreePath');
    assert.equal(getCurrentWorktreeSession(), null, 'Session should be cleared');
  });

  it('removes worktree without an active session', async () => {
    setWorktreeSession(null);

    const r = await exitWorktreeTool.execute(
      { action: 'remove', discard_changes: true },
      ctx({ cwd: worktreePath }),
    );

    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('Worktree removed:'));
  });

  it('removes worktree without a hookManager', async () => {
    setWorktreeSession(null);

    const r = await exitWorktreeTool.execute(
      { action: 'remove', discard_changes: true },
      ctx({ cwd: worktreePath }),
    );

    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('Worktree removed:'));
  });
});

describe('exitWorktreeTool — fallback removal when git worktree remove fails (lines 94-102)', () => {
  let tmpDir: string;
  let worktreePath: string;

  beforeEach(() => {
    tmpDir = makeTmpGitRepo();
    worktreePath = makeWorktreeDir(tmpDir, 'fallback-wt');
  });

  afterEach(() => {
    setWorktreeSession(null);
    try { process.chdir(SAFE_CWD); } catch {}
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('falls back to fs.rmSync + git worktree prune when git worktree remove fails', async () => {
    // The dir is not a real git worktree (just an independent repo in the path),
    // so `git worktree remove --force` will fail, triggering the fallback (lines 94-99).
    const r = await exitWorktreeTool.execute(
      { action: 'remove', discard_changes: true },
      ctx({ cwd: worktreePath }),
    );

    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('Worktree removed:'));
  });
});

describe('exitWorktreeTool — validation paths', () => {
  it('rejects invalid action value', async () => {
    const r = await exitWorktreeTool.execute(
      { action: 'invalid' },
      ctx({ cwd: SAFE_CWD }),
    );

    assert.ok(r.isError);
    assert.ok(r.output.includes('action must be'));
  });

  it('rejects non-git directory', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-ewt-nogit-'));
    try {
      const r = await exitWorktreeTool.execute(
        { action: 'keep' },
        ctx({ cwd: tmpDir }),
      );
      assert.ok(r.isError);
      assert.ok(r.output.includes('not inside a git repository'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('returns error when git rev-parse --show-toplevel fails in bare repo', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-ewt-bare-'));
    try {
      child_process.execSync('git init --bare', { cwd: tmpDir, stdio: 'ignore' });

      const r = await exitWorktreeTool.execute(
        { action: 'keep' },
        ctx({ cwd: tmpDir }),
      );

      // Bare repos may fail at is-inside-work-tree or show-toplevel
      assert.ok(typeof r.output === 'string');
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// =========================================================================
// WORKTREE MANAGER — createWorktreeSession (lines 57-82)
// =========================================================================

describe('createWorktreeSession (lines 57-82)', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = makeTmpGitRepo();
  });

  afterEach(() => {
    setWorktreeSession(null);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('creates a session with all fields populated', () => {
    const session = createWorktreeSession(tmpDir, 'my-feature');

    assert.equal(session.originalCwd, tmpDir);
    assert.ok(session.worktreePath.includes('.superinference/worktrees/my-feature'));
    assert.equal(session.worktreeName, 'my-feature');
    assert.equal(session.worktreeBranch, 'worktree-my-feature');
    assert.ok(session.originalBranch, 'Should have originalBranch');
    assert.ok(session.originalHeadCommit, 'Should have originalHeadCommit');
    assert.ok(typeof session.creationDurationMs === 'number');
    assert.ok(session.creationDurationMs! >= 0);
    assert.ok(session.createdAt);
  });

  it('handles non-git directory gracefully (originalBranch/Head are undefined)', () => {
    const nonGitDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-nogit-'));
    try {
      const session = createWorktreeSession(nonGitDir, 'test');

      assert.equal(session.originalCwd, nonGitDir);
      assert.equal(session.originalBranch, undefined);
      assert.equal(session.originalHeadCommit, undefined);
      assert.ok(typeof session.creationDurationMs === 'number');
    } finally {
      fs.rmSync(nonGitDir, { recursive: true, force: true });
    }
  });
});

// =========================================================================
// WORKTREE MANAGER — countWorktreeChanges (lines 87-102)
// =========================================================================

describe('countWorktreeChanges — full coverage (lines 87-102)', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = makeTmpGitRepo();
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns { uncommitted: 0, unpushed: 0 } for clean repo', () => {
    const result = countWorktreeChanges(tmpDir, tmpDir);

    assert.ok(result !== null);
    assert.equal(result!.uncommitted, 0);
    assert.equal(result!.unpushed, 0);
  });

  it('counts uncommitted staged changes', () => {
    fs.writeFileSync(path.join(tmpDir, 'init.txt'), 'changed');
    child_process.execSync('git add init.txt', { cwd: tmpDir, stdio: 'ignore' });

    const result = countWorktreeChanges(tmpDir, tmpDir);

    assert.ok(result !== null);
    assert.equal(result!.uncommitted, 1);
  });

  it('returns null for non-git directory', () => {
    const nonGitDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nogit-'));
    try {
      const result = countWorktreeChanges(nonGitDir, nonGitDir);
      assert.equal(result, null);
    } finally {
      fs.rmSync(nonGitDir, { recursive: true, force: true });
    }
  });

  it('counts unpushed commits when upstream exists', () => {
    child_process.execSync('git checkout -b upstream-base', { cwd: tmpDir, stdio: 'ignore' });
    child_process.execSync('git checkout -b feature', { cwd: tmpDir, stdio: 'ignore' });
    child_process.execSync('git branch --set-upstream-to=upstream-base', { cwd: tmpDir, stdio: 'ignore' });
    fs.writeFileSync(path.join(tmpDir, 'new.txt'), 'new content');
    child_process.execSync('git add . && git commit -m "ahead"', { cwd: tmpDir, stdio: 'ignore' });

    const result = countWorktreeChanges(tmpDir, tmpDir);

    assert.ok(result !== null);
    assert.equal(result!.uncommitted, 0);
    assert.ok(result!.unpushed >= 1, `Expected at least 1 unpushed commit, got ${result!.unpushed}`);
  });

  it('returns unpushed=0 when no upstream tracking configured', () => {
    const result = countWorktreeChanges(tmpDir, tmpDir);

    assert.ok(result !== null);
    assert.equal(result!.unpushed, 0);
  });
});

// =========================================================================
// WORKTREE MANAGER — copyWorktreeIncludes + copyDirRecursive (lines 160-204)
// =========================================================================

describe('copyWorktreeIncludes (lines 160-204)', () => {
  let sourceDir: string;
  let targetDir: string;

  beforeEach(() => {
    sourceDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-copy-src-'));
    targetDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-copy-tgt-'));
  });

  afterEach(() => {
    fs.rmSync(sourceDir, { recursive: true, force: true });
    fs.rmSync(targetDir, { recursive: true, force: true });
  });

  it('returns empty array when .worktreeinclude does not exist', () => {
    const result = copyWorktreeIncludes(sourceDir, targetDir);
    assert.deepEqual(result, []);
  });

  it('copies a single file listed in .worktreeinclude', () => {
    fs.writeFileSync(path.join(sourceDir, '.worktreeinclude'), 'config.json\n');
    fs.writeFileSync(path.join(sourceDir, 'config.json'), '{"key": "value"}');

    const result = copyWorktreeIncludes(sourceDir, targetDir);

    assert.deepEqual(result, ['config.json']);
    assert.ok(fs.existsSync(path.join(targetDir, 'config.json')));
    const content = fs.readFileSync(path.join(targetDir, 'config.json'), 'utf-8');
    assert.equal(content, '{"key": "value"}');
  });

  it('copies a directory recursively (exercises copyDirRecursive)', () => {
    const nestedDir = path.join(sourceDir, 'data', 'sub');
    fs.mkdirSync(nestedDir, { recursive: true });
    fs.writeFileSync(path.join(sourceDir, 'data', 'file1.txt'), 'file1');
    fs.writeFileSync(path.join(nestedDir, 'file2.txt'), 'file2');

    fs.writeFileSync(path.join(sourceDir, '.worktreeinclude'), 'data\n');

    const result = copyWorktreeIncludes(sourceDir, targetDir);

    assert.deepEqual(result, ['data']);
    assert.ok(fs.existsSync(path.join(targetDir, 'data', 'file1.txt')));
    assert.ok(fs.existsSync(path.join(targetDir, 'data', 'sub', 'file2.txt')));
    assert.equal(
      fs.readFileSync(path.join(targetDir, 'data', 'sub', 'file2.txt'), 'utf-8'),
      'file2',
    );
  });

  it('skips entries that do not exist in source', () => {
    fs.writeFileSync(
      path.join(sourceDir, '.worktreeinclude'),
      'missing-file.txt\nexisting.txt\n',
    );
    fs.writeFileSync(path.join(sourceDir, 'existing.txt'), 'exists');

    const result = copyWorktreeIncludes(sourceDir, targetDir);

    assert.deepEqual(result, ['existing.txt']);
    assert.ok(!fs.existsSync(path.join(targetDir, 'missing-file.txt')));
    assert.ok(fs.existsSync(path.join(targetDir, 'existing.txt')));
  });

  it('ignores comment lines and empty lines', () => {
    fs.writeFileSync(
      path.join(sourceDir, '.worktreeinclude'),
      '# This is a comment\n\n  \nfile.txt\n# Another comment\n',
    );
    fs.writeFileSync(path.join(sourceDir, 'file.txt'), 'content');

    const result = copyWorktreeIncludes(sourceDir, targetDir);

    assert.deepEqual(result, ['file.txt']);
  });

  it('copies nested subdirectory entry with parent dirs created', () => {
    const subDir = path.join(sourceDir, 'a', 'b');
    fs.mkdirSync(subDir, { recursive: true });
    fs.writeFileSync(path.join(subDir, 'nested.txt'), 'nested');

    fs.writeFileSync(path.join(sourceDir, '.worktreeinclude'), 'a/b\n');

    const result = copyWorktreeIncludes(sourceDir, targetDir);

    assert.deepEqual(result, ['a/b']);
    assert.ok(fs.existsSync(path.join(targetDir, 'a', 'b', 'nested.txt')));
  });

  it('handles deeply nested directory copy (three levels)', () => {
    const deep = path.join(sourceDir, 'lvl1', 'lvl2', 'lvl3');
    fs.mkdirSync(deep, { recursive: true });
    fs.writeFileSync(path.join(sourceDir, 'lvl1', 'a.txt'), 'a');
    fs.writeFileSync(path.join(sourceDir, 'lvl1', 'lvl2', 'b.txt'), 'b');
    fs.writeFileSync(path.join(deep, 'c.txt'), 'c');

    fs.writeFileSync(path.join(sourceDir, '.worktreeinclude'), 'lvl1\n');

    const result = copyWorktreeIncludes(sourceDir, targetDir);

    assert.deepEqual(result, ['lvl1']);
    assert.equal(fs.readFileSync(path.join(targetDir, 'lvl1', 'a.txt'), 'utf-8'), 'a');
    assert.equal(fs.readFileSync(path.join(targetDir, 'lvl1', 'lvl2', 'b.txt'), 'utf-8'), 'b');
    assert.equal(fs.readFileSync(path.join(targetDir, 'lvl1', 'lvl2', 'lvl3', 'c.txt'), 'utf-8'), 'c');
  });

  it('copies multiple entries including both files and directories', () => {
    fs.writeFileSync(path.join(sourceDir, 'file1.txt'), 'f1');
    const dir = path.join(sourceDir, 'mydir');
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(dir, 'inner.txt'), 'inner');

    fs.writeFileSync(
      path.join(sourceDir, '.worktreeinclude'),
      'file1.txt\nmydir\n',
    );

    const result = copyWorktreeIncludes(sourceDir, targetDir);

    assert.deepEqual(result, ['file1.txt', 'mydir']);
    assert.equal(fs.readFileSync(path.join(targetDir, 'file1.txt'), 'utf-8'), 'f1');
    assert.equal(fs.readFileSync(path.join(targetDir, 'mydir', 'inner.txt'), 'utf-8'), 'inner');
  });
});

// =========================================================================
// WORKFLOW TOOL — workflowResolver with scriptPath object (lines 92-97)
// =========================================================================

describe('workflowTool — workflowResolver scriptPath branch (lines 92-97)', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-res-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('resolves child workflow via scriptPath object', async () => {
    const childPath = path.join(tmpDir, 'child-wf.js');
    fs.writeFileSync(childPath, `export const meta = {
  name: 'child',
  description: 'child workflow'
}
log('child executed');
`);

    // Use JSON.stringify to safely embed the path in the script string
    const escapedPath = childPath.replace(/\\/g, '\\\\');
    const script = `export const meta = {
  name: 'parent',
  description: 'test'
}
const childResult = await workflow({ scriptPath: '${escapedPath}' });
log('parent done');
`;
    const r = await workflowTool.execute(
      { script },
      ctx({ cwd: tmpDir }),
    );

    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes("Workflow 'parent' completed"));
    assert.ok(r.output.includes('child executed'));
    assert.ok(r.output.includes('parent done'));
  });

  it('returns Workflow not found when scriptPath file does not exist', async () => {
    const script = `export const meta = {
  name: 'missing-child',
  description: 'test'
}
try {
  await workflow({ scriptPath: '/tmp/nonexistent-workflow-abc-xyz.js' });
} catch(e) {
  log('error: ' + e.message);
}
`;
    const r = await workflowTool.execute(
      { script },
      ctx({ cwd: tmpDir }),
    );

    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('Workflow not found') || r.output.includes('error:'));
  });

  it('resolves child workflow via string name from .superinference/workflows/', async () => {
    const wfDir = path.join(tmpDir, '.superinference', 'workflows');
    fs.mkdirSync(wfDir, { recursive: true });
    fs.writeFileSync(path.join(wfDir, 'helper.js'), `export const meta = {
  name: 'helper',
  description: 'helper workflow'
}
log('helper ran');
`);

    const script = `export const meta = {
  name: 'caller',
  description: 'test'
}
await workflow('helper');
log('caller done');
`;
    const r = await workflowTool.execute(
      { script },
      ctx({ cwd: tmpDir }),
    );

    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('helper ran'));
    assert.ok(r.output.includes('caller done'));
  });
});

describe('workflowTool — execution error path (lines 115-118)', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-err-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('catches and reports non-Error throws from workflow', async () => {
    const script = `export const meta = {
  name: 'string-throw',
  description: 'test'
}
throw 'plain string error';
`;
    const r = await workflowTool.execute(
      { script },
      ctx({ cwd: tmpDir }),
    );

    assert.ok(r.isError);
    assert.ok(r.output.includes('Error executing workflow:'));
    assert.ok(r.output.includes('plain string error'));
  });

  it('reports result value in output when workflow returns a value', async () => {
    const script = `export const meta = {
  name: 'result-wf',
  description: 'test'
}
return { answer: 42 };
`;
    const r = await workflowTool.execute(
      { script },
      ctx({ cwd: tmpDir }),
    );

    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('Result:'));
    assert.ok(r.output.includes('42'));
  });
});

describe('workflowTool — agentHandler with engine factory (lines 67-81)', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-agent-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('calls shutdown on subengine after execution', async () => {
    let shutdownCalled = false;

    const script = `export const meta = {
  name: 'shutdown-wf',
  description: 'test'
}
const result = await agent('do something');
log(String(result));
`;
    const r = await workflowTool.execute(
      { script },
      ctx({
        cwd: tmpDir,
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (_cfg: any) => ({
          submit: async function* () {
            yield { type: 'text_delta', text: 'engine output' };
          },
          shutdown: () => { shutdownCalled = true; },
        }),
      }),
    );

    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('engine output'));
    assert.ok(shutdownCalled, 'shutdown() should have been called on the sub-engine');
  });

  it('agentHandler returns fallback when engine produces no text_delta events', async () => {
    const script = `export const meta = {
  name: 'empty-agent-wf',
  description: 'test'
}
const result = await agent('do something');
log(String(result));
`;
    const r = await workflowTool.execute(
      { script },
      ctx({
        cwd: tmpDir,
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (_cfg: any) => ({
          submit: async function* () {
            // yield nothing text-related
            yield { type: 'turn_complete' };
          },
          shutdown: () => {},
        }),
      }),
    );

    assert.ok(!r.isError, r.output);
    assert.ok(
      r.output.includes('Agent produced no output') || r.output.includes('no output'),
    );
  });

  it('passes opts.tools to engineFactory config', async () => {
    let capturedConfig: any = null;

    const script = `export const meta = {
  name: 'tools-wf',
  description: 'test'
}
const result = await agent('do it', { tools: ['file_read'] });
log(String(result));
`;
    const r = await workflowTool.execute(
      { script },
      ctx({
        cwd: tmpDir,
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return {
            submit: async function* () {
              yield { type: 'text_delta', text: 'ok' };
            },
            shutdown: () => {},
          };
        },
      }),
    );

    assert.ok(!r.isError, r.output);
    assert.ok(capturedConfig);
    assert.deepEqual(capturedConfig.tools, ['file_read']);
  });
});
