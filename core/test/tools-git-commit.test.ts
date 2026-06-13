import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as childProcess from 'child_process';
import { gitCommitTool, buildFinalMessage, CO_AUTHOR_TRAILER, TRAILER_REGEX } from '../src/tools/git-commit';
import type { ToolContext } from '../src/types';

function ctx(cwd: string, overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

function git(args: string, cwd: string): string {
  return childProcess.execSync(`git ${args}`, { cwd, encoding: 'utf-8', stdio: ['pipe', 'pipe', 'pipe'] }).trim();
}

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('gitCommitTool — definition', () => {
  it('has the correct name', () => {
    assert.equal(gitCommitTool.name, 'git_commit');
  });

  it('has a description mentioning Co-Authored-By', () => {
    assert.ok(gitCommitTool.description.includes('Co-Authored-By'));
  });

  it('is not read-only', () => {
    assert.equal(gitCommitTool.isReadOnly, false);
  });

  it('schema requires "message"', () => {
    assert.ok(gitCommitTool.inputSchema.required?.includes('message'));
  });

  it('schema defines message and files properties', () => {
    const props = gitCommitTool.inputSchema.properties;
    assert.ok('message' in props);
    assert.ok('files' in props);
  });
});

// ---------------------------------------------------------------------------
// buildFinalMessage — trailer logic
// ---------------------------------------------------------------------------

describe('buildFinalMessage — trailer injection', () => {
  it('appends trailer to a simple message', () => {
    const result = buildFinalMessage('fix: resolve null pointer');
    assert.ok(result.includes(CO_AUTHOR_TRAILER));
  });

  it('separates trailer with a blank line', () => {
    const result = buildFinalMessage('fix: resolve null pointer');
    assert.ok(result.includes('\n\n' + CO_AUTHOR_TRAILER));
  });

  it('does not duplicate trailer if already present', () => {
    const msg = `fix: something\n\n${CO_AUTHOR_TRAILER}`;
    const result = buildFinalMessage(msg);
    const count = result.split(CO_AUTHOR_TRAILER).length - 1;
    assert.equal(count, 1);
  });

  it('does not duplicate trailer with different casing', () => {
    const msg = 'fix: something\n\nco-authored-by: AMI <ami@superinference.org>';
    const result = buildFinalMessage(msg);
    assert.ok(TRAILER_REGEX.test(result));
    const trailerMatches = result.match(/co-authored-by/gi);
    assert.equal(trailerMatches?.length, 1);
  });

  it('preserves multi-line commit messages', () => {
    const msg = 'feat: add auth\n\nThis adds OAuth2 support\nwith refresh tokens.';
    const result = buildFinalMessage(msg);
    assert.ok(result.startsWith('feat: add auth'));
    assert.ok(result.includes('OAuth2'));
    assert.ok(result.includes('refresh tokens'));
    assert.ok(result.endsWith(CO_AUTHOR_TRAILER));
  });

  it('handles messages with special characters', () => {
    const msg = 'fix: handle "quotes" and $variables and `backticks`';
    const result = buildFinalMessage(msg);
    assert.ok(result.includes('"quotes"'));
    assert.ok(result.includes('$variables'));
    assert.ok(result.includes('`backticks`'));
    assert.ok(result.includes(CO_AUTHOR_TRAILER));
  });

  it('trims whitespace from the message', () => {
    const result = buildFinalMessage('  fix: spaces  \n\n  ');
    assert.ok(result.startsWith('fix: spaces'));
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe('gitCommitTool — validation', () => {
  it('rejects empty message', async () => {
    const result = await gitCommitTool.execute({ message: '' }, ctx(process.cwd()));
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('empty'));
  });

  it('rejects whitespace-only message', async () => {
    const result = await gitCommitTool.execute({ message: '   ' }, ctx(process.cwd()));
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('empty'));
  });

  it('rejects flag injection in files array (--amend)', async () => {
    const result = await gitCommitTool.execute(
      { message: 'test', files: ['--amend'] },
      ctx(process.cwd()),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not flags'));
  });

  it('rejects single-dash flag injection in files array (-a)', async () => {
    const result = await gitCommitTool.execute(
      { message: 'test', files: ['-a'] },
      ctx(process.cwd()),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not flags'));
  });

  it('rejects --no-verify flag injection', async () => {
    const result = await gitCommitTool.execute(
      { message: 'test', files: ['--no-verify'] },
      ctx(process.cwd()),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not flags'));
  });

  it('accepts files parameter with empty array (skips staging)', async () => {
    const result = await gitCommitTool.execute(
      { message: 'test', files: [] },
      ctx(process.cwd()),
    );
    // Should reach the staging check, not the flag check
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('nothing staged'));
  });

  it('accepts "." and ".." as valid file entries', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-git-commit-val-'));
    try {
      git('init', tmpDir);
      git('config user.name "Test"', tmpDir);
      git('config user.email "t@t.com"', tmpDir);

      const result1 = await gitCommitTool.execute(
        { message: 'test', files: ['.'] },
        ctx(tmpDir),
      );
      assert.ok(!result1.output.includes('not flags'));

      const result2 = await gitCommitTool.execute(
        { message: 'test', files: ['..'] },
        ctx(tmpDir),
      );
      assert.ok(!result2.output.includes('not flags'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Git operations (require temp repo)
// ---------------------------------------------------------------------------

describe('gitCommitTool — git operations', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-git-commit-test-'));
    git('init', tmpDir);
    git('config user.name "Test User"', tmpDir);
    git('config user.email "test@example.com"', tmpDir);
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns error when nothing is staged', async () => {
    const result = await gitCommitTool.execute({ message: 'test' }, ctx(tmpDir));
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('nothing staged'));
  });

  it('stages and commits specified files', async () => {
    const filePath = path.join(tmpDir, 'hello.txt');
    fs.writeFileSync(filePath, 'hello world\n');

    const result = await gitCommitTool.execute(
      { message: 'feat: add hello', files: ['hello.txt'] },
      ctx(tmpDir),
    );
    assert.equal(result.isError, false);

    const commitMsg = git('log -1 --format=%B', tmpDir);
    assert.ok(commitMsg.includes('feat: add hello'));
    assert.ok(commitMsg.includes(CO_AUTHOR_TRAILER));
  });

  it('stages all tracked changes with files=["."]', async () => {
    const filePath = path.join(tmpDir, 'hello.txt');
    fs.writeFileSync(filePath, 'hello world updated\n');

    const result = await gitCommitTool.execute(
      { message: 'chore: update hello', files: ['.'] },
      ctx(tmpDir),
    );
    assert.equal(result.isError, false);

    const commitMsg = git('log -1 --format=%B', tmpDir);
    assert.ok(commitMsg.includes('chore: update hello'));
    assert.ok(commitMsg.includes(CO_AUTHOR_TRAILER));
  });

  it('commits already-staged changes without files parameter', async () => {
    const filePath = path.join(tmpDir, 'staged.txt');
    fs.writeFileSync(filePath, 'staged content\n');
    git('add staged.txt', tmpDir);

    const result = await gitCommitTool.execute(
      { message: 'feat: add staged file' },
      ctx(tmpDir),
    );
    assert.equal(result.isError, false);

    const commitMsg = git('log -1 --format=%B', tmpDir);
    assert.ok(commitMsg.includes('feat: add staged file'));
    assert.ok(commitMsg.includes(CO_AUTHOR_TRAILER));
  });

  it('handles multi-line messages with special characters correctly', async () => {
    const filePath = path.join(tmpDir, 'special.txt');
    fs.writeFileSync(filePath, 'special\n');

    const msg = 'fix: handle "edge" cases\n\nThis fixes $PATH and `cmd` issues.';
    const result = await gitCommitTool.execute(
      { message: msg, files: ['special.txt'] },
      ctx(tmpDir),
    );
    assert.equal(result.isError, false);

    const commitMsg = git('log -1 --format=%B', tmpDir);
    assert.ok(commitMsg.includes('"edge"'));
    assert.ok(commitMsg.includes('$PATH'));
    assert.ok(commitMsg.includes('`cmd`'));
    assert.ok(commitMsg.includes(CO_AUTHOR_TRAILER));
  });

  it('does not duplicate trailer when message already contains it', async () => {
    const filePath = path.join(tmpDir, 'dedup.txt');
    fs.writeFileSync(filePath, 'dedup test\n');

    const msg = `chore: test dedup\n\n${CO_AUTHOR_TRAILER}`;
    const result = await gitCommitTool.execute(
      { message: msg, files: ['dedup.txt'] },
      ctx(tmpDir),
    );
    assert.equal(result.isError, false);

    const commitMsg = git('log -1 --format=%B', tmpDir);
    const count = commitMsg.split(CO_AUTHOR_TRAILER).length - 1;
    assert.equal(count, 1);
  });

  it('returns commit hash in output', async () => {
    const filePath = path.join(tmpDir, 'hash-test.txt');
    fs.writeFileSync(filePath, 'hash test\n');

    const result = await gitCommitTool.execute(
      { message: 'test: check output hash', files: ['hash-test.txt'] },
      ctx(tmpDir),
    );
    assert.equal(result.isError, false);

    const head = git('rev-parse --short HEAD', tmpDir);
    assert.ok(result.output.includes(head));
  });

  it('handles files with spaces in names', async () => {
    const filePath = path.join(tmpDir, 'file with spaces.txt');
    fs.writeFileSync(filePath, 'spaces\n');

    const result = await gitCommitTool.execute(
      { message: 'test: file with spaces', files: ['file with spaces.txt'] },
      ctx(tmpDir),
    );
    assert.equal(result.isError, false);

    const commitMsg = git('log -1 --format=%B', tmpDir);
    assert.ok(commitMsg.includes(CO_AUTHOR_TRAILER));
  });

  it('handles files with quotes in names', async () => {
    const filePath = path.join(tmpDir, "file'quote.txt");
    fs.writeFileSync(filePath, 'quotes\n');

    const result = await gitCommitTool.execute(
      { message: 'test: file with quote', files: ["file'quote.txt"] },
      ctx(tmpDir),
    );
    assert.equal(result.isError, false);

    const commitMsg = git('log -1 --format=%B', tmpDir);
    assert.ok(commitMsg.includes(CO_AUTHOR_TRAILER));
  });

  it('returns error for non-existent file in files array', async () => {
    const result = await gitCommitTool.execute(
      { message: 'test', files: ['nonexistent-file-xyz.txt'] },
      ctx(tmpDir),
    );
    assert.equal(result.isError, true);
  });

  it('handles multiple files in a single commit', async () => {
    const file1 = path.join(tmpDir, 'multi1.txt');
    const file2 = path.join(tmpDir, 'multi2.txt');
    fs.writeFileSync(file1, 'multi1\n');
    fs.writeFileSync(file2, 'multi2\n');

    const result = await gitCommitTool.execute(
      { message: 'feat: add multiple files', files: ['multi1.txt', 'multi2.txt'] },
      ctx(tmpDir),
    );
    assert.equal(result.isError, false);

    const commitMsg = git('log -1 --format=%B', tmpDir);
    assert.ok(commitMsg.includes('feat: add multiple files'));
    assert.ok(commitMsg.includes(CO_AUTHOR_TRAILER));

    // Verify both files are in the commit
    const filesInCommit = git('diff-tree --no-commit-id --name-only -r HEAD', tmpDir);
    assert.ok(filesInCommit.includes('multi1.txt'));
    assert.ok(filesInCommit.includes('multi2.txt'));
  });

  it('returns error outside a git repository', async () => {
    const nonGitDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-no-git-'));
    try {
      fs.writeFileSync(path.join(nonGitDir, 'file.txt'), 'test\n');
      const result = await gitCommitTool.execute(
        { message: 'test', files: ['file.txt'] },
        ctx(nonGitDir),
      );
      assert.equal(result.isError, true);
    } finally {
      fs.rmSync(nonGitDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Integration — tool registry
// ---------------------------------------------------------------------------

describe('gitCommitTool — integration', () => {
  it('is registered in createDefaultTools', () => {
    const { createDefaultTools } = require('../src/tools/index');
    const registry = createDefaultTools('/tmp');
    const tool = registry.get('git_commit');
    assert.ok(tool, 'git_commit should be in the default registry');
    assert.equal(tool.name, 'git_commit');
    assert.equal(tool.isReadOnly, false);
  });

  it('is included in the tool list with correct properties', () => {
    const { createDefaultTools } = require('../src/tools/index');
    const registry = createDefaultTools('/tmp');
    const all = registry.getAll();
    const names = all.map((t: any) => t.name);
    assert.ok(names.includes('git_commit'));

    const formatted = registry.toOpenAIFormat();
    const gitCommitEntry = formatted.find((e: any) => e.function.name === 'git_commit');
    assert.ok(gitCommitEntry);
    assert.equal(gitCommitEntry.type, 'function');
    assert.ok(gitCommitEntry.function.description.includes('Co-Authored-By'));
    assert.ok(gitCommitEntry.function.parameters.required?.includes('message'));
  });
});
