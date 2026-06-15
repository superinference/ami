import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as child_process from 'child_process';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { exitWorktreeTool } from '../src/tools/exit-worktree';
import type { ToolContext } from '../src/types';

function initGitRepo(dir: string): void {
  child_process.execSync('git init', { cwd: dir, stdio: 'ignore' });
  child_process.execSync('git config user.email "test@test.com"', { cwd: dir, stdio: 'ignore' });
  child_process.execSync('git config user.name "Test"', { cwd: dir, stdio: 'ignore' });
}

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Tool definitions
// ---------------------------------------------------------------------------

describe('enterWorktreeTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(enterWorktreeTool.name, 'enter_worktree');
  });

  it('is not read-only', () => {
    assert.equal(enterWorktreeTool.isReadOnly, false);
  });

  it('has name and path properties in schema', () => {
    const props = enterWorktreeTool.inputSchema.properties;
    assert.ok('name' in props);
    assert.ok('path' in props);
  });
});

describe('exitWorktreeTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(exitWorktreeTool.name, 'exit_worktree');
  });

  it('requires action parameter', () => {
    assert.ok(exitWorktreeTool.inputSchema.required?.includes('action'));
  });

  it('action has keep and remove enum values', () => {
    const actionProp = exitWorktreeTool.inputSchema.properties.action;
    assert.ok(actionProp.enum?.includes('keep'));
    assert.ok(actionProp.enum?.includes('remove'));
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe('enterWorktreeTool – validation', () => {
  it('rejects mutually exclusive name and path', async () => {
    const result = await enterWorktreeTool.execute(
      { name: 'test', path: '/tmp/x' },
      ctx(),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('mutually exclusive'));
  });

  it('rejects non-git directory', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-'));
    try {
      const result = await enterWorktreeTool.execute({}, ctx({ cwd: tmpDir }));
      assert.ok(result.isError);
      assert.ok(result.output.includes('not inside a git repository'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('rejects invalid slug', async () => {
    const result = await enterWorktreeTool.execute(
      { name: 'bad slug with spaces!' },
      ctx(),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('invalid worktree name'));
  });

  it('rejects non-existent path', async () => {
    const result = await enterWorktreeTool.execute(
      { path: '/tmp/nonexistent-worktree-xyz' },
      ctx(),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('does not exist'));
  });
});

describe('exitWorktreeTool – validation', () => {
  it('rejects invalid action', async () => {
    const result = await exitWorktreeTool.execute(
      { action: 'invalid' },
      ctx(),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('must be "keep" or "remove"'));
  });

  it('reports no active session when not in worktree path', async () => {
    const result = await exitWorktreeTool.execute(
      { action: 'keep' },
      ctx(),
    );
    assert.ok(result.output.includes('No active worktree session') || result.output.includes('Worktree kept'));
  });
});

// ---------------------------------------------------------------------------
// Full lifecycle (creates a real git worktree, then removes it)
// ---------------------------------------------------------------------------

describe('worktree tools – lifecycle', () => {
  it('creates and removes a worktree', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-'));

    try {
      initGitRepo(tmpDir);
      fs.writeFileSync(path.join(tmpDir, 'README.md'), 'test');
      child_process.execSync('git add . && git commit -m "init"', { cwd: tmpDir, stdio: 'ignore' });

      const createResult = await enterWorktreeTool.execute(
        { name: 'test-wt' },
        ctx({ cwd: tmpDir }),
      );

      assert.ok(!createResult.isError, `Create failed: ${createResult.output}`);
      assert.ok(createResult.output.includes('Worktree created'));
      assert.ok(createResult.output.includes('test-wt'));
      assert.ok(createResult.output.includes('worktree/test-wt'));

      const wtPath = path.join(tmpDir, '.superinference', 'worktrees', 'test-wt');
      assert.ok(fs.existsSync(wtPath), 'Worktree directory should exist');
      assert.ok(fs.existsSync(path.join(wtPath, 'README.md')), 'Worktree should contain files');

      const removeResult = await exitWorktreeTool.execute(
        { action: 'remove', discard_changes: true },
        ctx({ cwd: wtPath }),
      );
      assert.ok(removeResult.output.includes('removed') || removeResult.output.includes('Worktree'), `Remove output: ${removeResult.output}`);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('rejects duplicate worktree name', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-'));

    try {
      initGitRepo(tmpDir);
      fs.writeFileSync(path.join(tmpDir, 'f.txt'), 'x');
      child_process.execSync('git add . && git commit -m "init"', { cwd: tmpDir, stdio: 'ignore' });

      await enterWorktreeTool.execute({ name: 'dup-test' }, ctx({ cwd: tmpDir }));
      const dup = await enterWorktreeTool.execute({ name: 'dup-test' }, ctx({ cwd: tmpDir }));
      assert.ok(dup.isError);
      assert.ok(dup.output.includes('already exists'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('generates random name when none provided', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-'));

    try {
      initGitRepo(tmpDir);
      fs.writeFileSync(path.join(tmpDir, 'f.txt'), 'x');
      child_process.execSync('git add . && git commit -m "init"', { cwd: tmpDir, stdio: 'ignore' });

      const result = await enterWorktreeTool.execute({}, ctx({ cwd: tmpDir }));
      assert.ok(!result.isError, `Failed: ${result.output}`);
      assert.ok(result.output.includes('wt-'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
