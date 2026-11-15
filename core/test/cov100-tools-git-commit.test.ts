import { describe, it, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as childProcess from 'child_process';
import {
  gitCommitTool,
  _gitCommitFs,
  buildFinalMessage,
  isSecretFile,
  stripDisallowedAttribution,
  CO_AUTHOR_TRAILER,
} from '../src/tools/git-commit';
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

// ---------------------------------------------------------------------------
// Secret-bearing FILE guard — refuse to commit credential files
// ---------------------------------------------------------------------------

describe('isSecretFile — credential-file classifier', () => {
  it('flags .env and env variants but not safe templates', () => {
    assert.equal(isSecretFile('.env'), true);
    assert.equal(isSecretFile('.env.local'), true);
    assert.equal(isSecretFile('.env.production'), true);
    assert.equal(isSecretFile('.env.example'), false);
    assert.equal(isSecretFile('.env.sample'), false);
    assert.equal(isSecretFile('.env.template'), false);
  });

  it('flags known secret basenames', () => {
    assert.equal(isSecretFile('id_rsa'), true);
    assert.equal(isSecretFile('id_ed25519'), true);
    assert.equal(isSecretFile('.netrc'), true);
    assert.equal(isSecretFile('credentials.json'), true);
  });

  it('flags secret extensions (case-insensitive) and secrets.yaml patterns', () => {
    assert.equal(isSecretFile('server.pem'), true);
    assert.equal(isSecretFile('SERVER.KEY'), true);
    assert.equal(isSecretFile('store.p12'), true);
    assert.equal(isSecretFile('secrets.yaml'), true);
    assert.equal(isSecretFile('secrets.yml'), true);
  });

  it('allows ordinary source files', () => {
    assert.equal(isSecretFile('index.ts'), false);
    assert.equal(isSecretFile('README.md'), false);
    assert.equal(isSecretFile('config.json'), false);
  });
});

describe('gitCommitTool — secret-bearing file commit guard', () => {
  it('refuses to commit a staged .env file', async () => {
    const dir = initRepo('ami-gc-envfile-');
    dirs.push(dir);
    fs.writeFileSync(path.join(dir, '.env'), 'API_KEY=supersecretvalue\n');

    const result = await gitCommitTool.execute(
      { message: 'chore: add env', files: ['.env'] },
      ctx(dir),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /refusing to commit files that likely contain secrets/);
    assert.match(result.output, /\.env/);
    // Commit must not have been created.
    assert.throws(() => git('rev-parse HEAD', dir));
  });

  it('refuses when a secret file is staged alongside a normal file', async () => {
    const dir = initRepo('ami-gc-envmix-');
    dirs.push(dir);
    fs.writeFileSync(path.join(dir, 'app.ts'), 'export const x = 1;\n');
    fs.writeFileSync(path.join(dir, 'id_rsa'), 'PRIVATE\n');

    const result = await gitCommitTool.execute(
      { message: 'feat: app', files: ['app.ts', 'id_rsa'] },
      ctx(dir),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /id_rsa/);
    assert.throws(() => git('rev-parse HEAD', dir));
  });

  it('allows committing a safe .env.example template', async () => {
    const dir = initRepo('ami-gc-envexample-');
    dirs.push(dir);
    fs.writeFileSync(path.join(dir, '.env.example'), 'API_KEY=\n');

    const result = await gitCommitTool.execute(
      { message: 'docs: add env template', files: ['.env.example'] },
      ctx(dir),
    );
    assert.equal(result.isError, false, JSON.stringify(result));
    assert.ok(git('log -1 --format=%B', dir).includes('docs: add env template'));
  });
});

// ---------------------------------------------------------------------------
// Disallowed-attribution stripping in buildFinalMessage
// ---------------------------------------------------------------------------

describe('stripDisallowedAttribution + buildFinalMessage', () => {
  it('removes a foreign Co-Authored-By line but keeps the AMI trailer', () => {
    const msg = 'fix: thing\n\nCo-Authored-By: SomeBot <bot@example.com>';
    const out = stripDisallowedAttribution(msg);
    assert.ok(!out.includes('SomeBot'));
    const built = buildFinalMessage(msg);
    assert.ok(!built.includes('SomeBot'));
    assert.ok(built.includes(CO_AUTHOR_TRAILER));
  });

  it('removes an AI provenance footer line carrying the robot marker', () => {
    const msg = 'feat: x\n\n\u{1F916} Generated with some-tool (https://example.com)';
    const out = stripDisallowedAttribution(msg);
    assert.ok(!out.includes('Generated with'));
  });

  it('keeps the AMI Co-Authored-By trailer intact', () => {
    const msg = `fix: y\n\n${CO_AUTHOR_TRAILER}`;
    const out = stripDisallowedAttribution(msg);
    assert.ok(out.includes(CO_AUTHOR_TRAILER));
  });

  it('collapses the blank-line gap left by a stripped trailer', () => {
    const msg = 'fix: z\n\nCo-Authored-By: Other <o@e.com>\n\n\n';
    const out = stripDisallowedAttribution(msg);
    assert.ok(!/\n{3,}/.test(out));
    assert.ok(!out.includes('Other'));
  });

  it('git_commit strips a foreign trailer from the real commit', async () => {
    const dir = initRepo('ami-gc-strip-');
    dirs.push(dir);
    fs.writeFileSync(path.join(dir, 's.txt'), 's\n');
    const result = await gitCommitTool.execute(
      { message: 'feat: strip\n\nCo-Authored-By: Foreign AI <x@y.z>', files: ['s.txt'] },
      ctx(dir),
    );
    assert.equal(result.isError, false, JSON.stringify(result));
    const commitMsg = git('log -1 --format=%B', dir);
    assert.ok(!commitMsg.includes('Foreign AI'));
    assert.ok(commitMsg.includes(CO_AUTHOR_TRAILER));
  });
});

// ---------------------------------------------------------------------------
// Temp-file write failure is reported, not thrown
// ---------------------------------------------------------------------------

describe('gitCommitTool — commit-message temp-file write failure', () => {
  it('returns an error result when writeFileSync throws (does not reject)', async () => {
    const dir = initRepo('ami-gc-writefail-');
    dirs.push(dir);
    fs.writeFileSync(path.join(dir, 'w.txt'), 'w\n');

    const spy = mock.method(_gitCommitFs, 'writeFileSync', () => {
      throw new Error('simulated write failure (EEXIST)');
    });
    try {
      const result = await gitCommitTool.execute(
        { message: 'feat: write seam', files: ['w.txt'] },
        ctx(dir),
      );
      assert.equal(result.isError, true, JSON.stringify(result));
      assert.match(result.output, /Error writing commit message file/);
      assert.match(result.output, /simulated write failure/);
      // The commit must not have been created.
      assert.throws(() => git('rev-parse HEAD', dir));
    } finally {
      spy.mock.restore();
    }
  });
});
