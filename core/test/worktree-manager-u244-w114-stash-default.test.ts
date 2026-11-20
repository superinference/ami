/**
 * U244 (w114) — stashToCleanState: optional message parameter with a timestamped
 * default + untracked-file preservation.
 *
 * Confirmed parity gaps against the reference's `stashToCleanState(message?)`:
 *   - OPTIONAL MESSAGE / TIMESTAMPED DEFAULT: Claude's helper takes an optional
 *     stash message and, when omitted, falls back to a timestamped default so an
 *     auto-stash is always identifiable in `git stash list`. AMI's helper
 *     previously REQUIRED a message. It now accepts none and generates
 *     `${AUTO_STASH_MESSAGE_PREFIX} - <ISO timestamp>` (re-branded from Claude's
 *     "Claude Code auto-stash - <ISO>").
 *   - UNTRACKED PRESERVATION: Claude stages untracked files before `git stash push`
 *     so they are not lost. AMI reaches the SAME guarantee via `git stash push -u`
 *     (untracked files are included in the stash); this test proves an untracked
 *     file survives and is restored by `git stash apply`.
 *
 * RED (pre-fix): `message` was required, so `stashToCleanState(repo)` passed
 * `undefined` to the git invocation (argv must be strings) → the call threw and
 * the helper returned false even for a dirty tree, and no timestamped stash was
 * created. GREEN (post-fix): the no-arg call stashes with a timestamped default
 * and returns true.
 */
import { execFileSync } from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';

import { stashToCleanState, AUTO_STASH_MESSAGE_PREFIX } from '../src/worktree-manager';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf-8' }).trim();
}

function makeRepo(): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'u244w114-')));
  git(dir, 'init', '-q');
  git(dir, 'config', 'user.email', 't@t.com');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  try { git(dir, 'checkout', '-q', '-B', 'main'); } catch { /* older git */ }
  fs.writeFileSync(path.join(dir, 'README.md'), '# r\n');
  git(dir, 'add', 'README.md');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

describe('U244 w114 — stashToCleanState optional message + timestamped default', () => {
  let repo: string;

  beforeEach(() => { repo = makeRepo(); });
  afterEach(() => { fs.rmSync(repo, { recursive: true, force: true }); });

  it('stashes a dirty tree with NO message using a timestamped default label', () => {
    fs.writeFileSync(path.join(repo, 'README.md'), '# r\nmodified\n');

    // No message argument — the additive optional-message parity behavior.
    assert.equal(stashToCleanState(repo), true, 'dirty tree with no message → stashed');
    assert.equal(git(repo, 'status', '--porcelain'), '', 'tree clean after stash');

    const list = git(repo, 'stash', 'list');
    // Default label: "AMI auto-stash - <ISO timestamp>".
    const re = new RegExp(`${AUTO_STASH_MESSAGE_PREFIX} - \\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}`);
    assert.match(list, re, 'default stash message must be the timestamped AMI auto-stash label');
  });

  it('treats an empty-string message as "no message" and uses the default', () => {
    fs.writeFileSync(path.join(repo, 'README.md'), '# r\nmod2\n');

    assert.equal(stashToCleanState(repo, '   '), true, 'blank message → stashed with default');
    const list = git(repo, 'stash', 'list');
    assert.match(list, new RegExp(`${AUTO_STASH_MESSAGE_PREFIX} - `), 'blank message falls back to the default');
  });

  it('uses an explicit message verbatim (regression: existing callers unchanged)', () => {
    fs.writeFileSync(path.join(repo, 'README.md'), '# r\nmod3\n');

    assert.equal(stashToCleanState(repo, 'exit_worktree auto-stash (feat/x)'), true);
    const list = git(repo, 'stash', 'list');
    assert.match(list, /exit_worktree auto-stash \(feat\/x\)/, 'explicit message preserved');
    assert.doesNotMatch(list, new RegExp(AUTO_STASH_MESSAGE_PREFIX), 'no default label when a message is given');
  });

  it('returns false for a clean tree (nothing to stash) with no message', () => {
    assert.equal(stashToCleanState(repo), false, 'clean tree → nothing to stash');
    assert.equal(git(repo, 'stash', 'list'), '', 'no stash created for a clean tree');
  });

  it('preserves an UNTRACKED file in the stash (git stash push -u) and restores it', () => {
    fs.writeFileSync(path.join(repo, 'brand-new.txt'), 'wip\n');

    assert.equal(stashToCleanState(repo), true, 'untracked-only dirty tree → stashed');
    assert.equal(fs.existsSync(path.join(repo, 'brand-new.txt')), false, 'untracked file removed from tree by stash');

    git(repo, 'stash', 'apply');
    assert.equal(fs.existsSync(path.join(repo, 'brand-new.txt')), true, 'untracked file restored by stash apply');
    assert.equal(fs.readFileSync(path.join(repo, 'brand-new.txt'), 'utf-8'), 'wip\n', 'untracked content intact');
  });
});
