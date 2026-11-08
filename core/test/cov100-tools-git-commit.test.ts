import { describe, it, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as childProcess from 'child_process';
import { gitCommitTool, _gitCommitFs } from '../src/tools/git-commit';
import type { ToolContext } from '../src/types';

function ctx(cwd: string, overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

function git(args: string, cwd: string): string {
  return childProcess
    .execSync(`git ${args}`, { cwd, encoding: 'utf-8', stdio: ['pipe', 'pipe', 'pipe'] })
    .trim();
}

function initRepo(prefix: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  git('init', dir);
  git('config user.name "Cov Tester"', dir);
  git('config user.email "cov@test.local"', dir);
  return dir;
}

const dirs: string[] = [];
after(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Secret scanning branch (secrets.length > 0)
// ---------------------------------------------------------------------------

describe('gitCommitTool — secret scan branch', () => {
  it('blocks the commit when the message contains a detected secret', async () => {
    const dir = initRepo('ami-gc-secret-');
    dirs.push(dir);
    fs.writeFileSync(path.join(dir, 'f.txt'), 'content\n');

    // AWS access-key shaped token in the message must trip scanForSecrets.
    const result = await gitCommitTool.execute(
      { message: 'chore: add key AKIAIOSFODNN7EXAMPLE now', files: ['f.txt'] },
      ctx(dir),
    );

    assert.equal(result.isError, true);
    assert.match(result.output, /Potential secrets detected/);
    assert.match(result.output, /Remove secrets before committing/);

    // The commit must NOT have been created — repo should have no HEAD.
    assert.throws(() => git('rev-parse HEAD', dir));
  });

  it('allows a clean message to commit (secret branch not taken)', async () => {
    const dir = initRepo('ami-gc-clean-');
    dirs.push(dir);
    fs.writeFileSync(path.join(dir, 'g.txt'), 'clean\n');

    const result = await gitCommitTool.execute(
      { message: 'chore: no secrets here', files: ['g.txt'] },
      ctx(dir),
    );
    assert.equal(result.isError, false, JSON.stringify(result));
    assert.ok(git('log -1 --format=%B', dir).includes('chore: no secrets here'));
  });
});

// ---------------------------------------------------------------------------
// Non-string entries in the files array (typeof f === 'string' guard)
// ---------------------------------------------------------------------------

describe('gitCommitTool — non-string file entries are filtered out', () => {
  it('ignores non-string entries and reaches the nothing-staged check', async () => {
    const dir = initRepo('ami-gc-nonstr-');
    dirs.push(dir);

    // A numeric entry: not a flag (typeof !== 'string'), filtered from safeFiles,
    // so staging adds nothing and the diff check reports nothing staged.
    const result = await gitCommitTool.execute(
      { message: 'test', files: [123 as unknown as string] },
      ctx(dir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('nothing staged'));
  });
});

// ---------------------------------------------------------------------------
// Temp-file cleanup seam: unlinkSync failure in finally is swallowed
// ---------------------------------------------------------------------------

describe('gitCommitTool — unlink failure is swallowed in finally', () => {
  it('still returns the successful commit result when temp-file cleanup throws', async () => {
    const dir = initRepo('ami-gc-unlink-');
    dirs.push(dir);
    fs.writeFileSync(path.join(dir, 'u.txt'), 'u\n');

    const spy = mock.method(_gitCommitFs, 'unlinkSync', () => {
      throw new Error('simulated unlink failure');
    });
    try {
      const result = await gitCommitTool.execute(
        { message: 'feat: unlink seam', files: ['u.txt'] },
        ctx(dir),
      );
      // The thrown unlink must be caught by the finally's try/catch — commit succeeds.
      assert.equal(result.isError, false, JSON.stringify(result));
      assert.ok(git('log -1 --format=%B', dir).includes('feat: unlink seam'));
      assert.equal(spy.mock.calls.length, 1);
    } finally {
      spy.mock.restore();
    }
  });
});
