/**
 * U079 — isPathGitignored: authoritative per-path ignore check via `git check-ignore`.
 *
 * Parity with Claude Code's utils/git/gitignore.ts isPathGitignored: the predicate
 * delegates the whole "is this path ignored?" question to git so ALL ignore sources
 * resolve with git's real precedence (nested .gitignore, .git/info/exclude, global
 * gitignore). Exit-code branching: 0 → ignored (true), 1 → not ignored (false),
 * 128 → not a git repo (false, fail OPEN).
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as childProcess from 'child_process';
import { isPathGitignored } from '../src/utils/git';

function git(args: string, cwd: string): void {
  childProcess.execSync(`git ${args}`, { cwd, encoding: 'utf-8', stdio: ['pipe', 'pipe', 'pipe'] });
}

const dirs: string[] = [];
function initRepo(): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'u079-gi-')));
  dirs.push(dir);
  git('init', dir);
  return dir;
}

after(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

describe('U079 isPathGitignored — git check-ignore delegation', () => {
  it('returns true for a path matched by .gitignore (exit 0)', () => {
    const repo = initRepo();
    fs.writeFileSync(path.join(repo, '.gitignore'), 'ignored/\n*.log\n');
    fs.mkdirSync(path.join(repo, 'ignored'));
    assert.equal(isPathGitignored(path.join(repo, 'ignored'), repo), true);
    assert.equal(isPathGitignored('app.log', repo), true, 'relative path matched by glob rule');
  });

  it('returns false for a path NOT ignored (exit 1)', () => {
    const repo = initRepo();
    fs.writeFileSync(path.join(repo, '.gitignore'), 'ignored/\n');
    fs.mkdirSync(path.join(repo, 'src'));
    assert.equal(isPathGitignored(path.join(repo, 'src'), repo), false);
  });

  it('honors .git/info/exclude (a source the in-process matchers can miss)', () => {
    const repo = initRepo();
    fs.writeFileSync(path.join(repo, '.git', 'info', 'exclude'), 'secret-dir/\n');
    fs.mkdirSync(path.join(repo, 'secret-dir'));
    assert.equal(isPathGitignored(path.join(repo, 'secret-dir'), repo), true);
  });

  it('fails OPEN (false) when cwd is not a git repository (exit 128)', () => {
    const plain = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'u079-nogit-')));
    dirs.push(plain);
    fs.mkdirSync(path.join(plain, 'anything'));
    assert.equal(isPathGitignored(path.join(plain, 'anything'), plain), false);
  });

  it('fails OPEN (false) when git cannot be spawned at all', () => {
    const repo = initRepo();
    // An empty PATH makes `git` unspawnable → runGit returns null → fail open.
    const savedPath = process.env.PATH;
    process.env.PATH = '';
    try {
      assert.equal(isPathGitignored(path.join(repo, 'x'), repo), false);
    } finally {
      process.env.PATH = savedPath;
    }
  });
});
