/**
 * U003 / wave w83 — permissions.ts parity hardening.
 *
 * Covers two genuinely-missing capabilities ported from Claude Code:
 *
 *  1. Bounded recursive command decomposition (DoS guard). AMI's
 *     extractBaseCommands / extractCommandSubstitutions recurse into
 *     $(...)/backticks/subshells/`-c` payloads with NO depth cap, so a
 *     pathologically deep nesting chain can blow the call stack (RangeError) or
 *     hang the safety layer. MAX_COMMAND_PARSE_DEPTH bounds the descent, mirroring
 *     Claude's `recursionDepth > 10` guard in utils/bash/prefix.ts.
 *
 *  2. Disguised-bare-git-repo detection (hook-execution sandbox-escape boundary).
 *     isDisguisedBareGitRepo() detects a cwd manipulated to look like a bare repo
 *     so that any git command would make git treat the cwd as the git dir and run
 *     attacker-planted hooks. check() forces an explicit prompt for git commands
 *     from such a cwd, even under auto-allow/bypass. Mirrors the reference's
 *     isCurrentDirectoryBareGitRepo (utils/git.ts).
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  PermissionManager,
  MAX_COMMAND_PARSE_DEPTH,
  isDisguisedBareGitRepo,
  commandInvokesGit,
} from '../src/permissions';

function mkTmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-w83-'));
}

// ---------------------------------------------------------------------------
// Feature 1 — bounded recursive command decomposition (DoS guard)
// ---------------------------------------------------------------------------

test('MAX_COMMAND_PARSE_DEPTH is a small positive bound (the reference parity: 10)', () => {
  assert.equal(MAX_COMMAND_PARSE_DEPTH, 10);
});

test('classifyBashCommand does not blow the stack on a deeply-nested substitution chain', () => {
  const n = 20000;
  const deep = '$('.repeat(n) + 'echo hi' + ')'.repeat(n);
  const pm = new PermissionManager();
  // Without the depth bound this recurses n levels deep and throws RangeError
  // (Maximum call stack size exceeded). With the bound it returns a valid
  // classification without throwing.
  let result: string | undefined;
  assert.doesNotThrow(() => {
    result = pm.classifyBashCommand(deep);
  });
  assert.ok(['safe', 'unsafe', 'destructive'].includes(result!));
});

test('deny-matching does not blow the stack on a deeply-nested substitution chain', async () => {
  const n = 20000;
  const deep = 'echo ' + '$('.repeat(n) + 'rm -rf /' + ')'.repeat(n);
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'rm -rf /', action: 'deny' },
  ]);
  // check() runs extractCommandSubstitutions for deny-matching — it must not throw.
  let verdict: string | undefined;
  await assert.doesNotReject(async () => {
    verdict = await pm.check('bash', { command: deep });
  });
  assert.ok(['allow', 'deny', 'ask'].includes(verdict!));
});

test('depth bound preserves shallow decomposition (nested destructive still caught)', () => {
  const pm = new PermissionManager();
  // One level of substitution is well under the cap — the inner destructive
  // command must still be detected, proving the bound does not weaken real checks.
  assert.equal(pm.classifyBashCommand('echo $(rm -rf /)'), 'destructive');
  assert.equal(pm.classifyBashCommand('echo `rm -rf /`'), 'destructive');
});

// ---------------------------------------------------------------------------
// Feature 2 — disguised-bare-git-repo detection + check() wiring
// ---------------------------------------------------------------------------

test('isDisguisedBareGitRepo: normal repo (.git dir with HEAD file) is safe', () => {
  const dir = mkTmp();
  try {
    fs.mkdirSync(path.join(dir, '.git'));
    fs.writeFileSync(path.join(dir, '.git', 'HEAD'), 'ref: refs/heads/main\n');
    assert.equal(isDisguisedBareGitRepo(dir), false);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('isDisguisedBareGitRepo: .git as a FILE (worktree/submodule) is safe', () => {
  const dir = mkTmp();
  try {
    fs.writeFileSync(path.join(dir, '.git'), 'gitdir: /somewhere/.git/worktrees/x\n');
    // Even if bare indicators are present, a .git FILE means git follows the ref.
    fs.mkdirSync(path.join(dir, 'objects'));
    assert.equal(isDisguisedBareGitRepo(dir), false);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('isDisguisedBareGitRepo: empty/plain directory is safe', () => {
  const dir = mkTmp();
  try {
    assert.equal(isDisguisedBareGitRepo(dir), false);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('isDisguisedBareGitRepo: cwd with HEAD+objects/+refs/ and no .git is flagged', () => {
  const dir = mkTmp();
  try {
    fs.writeFileSync(path.join(dir, 'HEAD'), 'ref: refs/heads/main\n');
    fs.mkdirSync(path.join(dir, 'objects'));
    fs.mkdirSync(path.join(dir, 'refs'));
    assert.equal(isDisguisedBareGitRepo(dir), true);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('isDisguisedBareGitRepo: .git dir whose HEAD is a directory falls back to bare check', () => {
  const dir = mkTmp();
  try {
    fs.mkdirSync(path.join(dir, '.git'));
    fs.mkdirSync(path.join(dir, '.git', 'HEAD')); // invalid HEAD (a directory)
    fs.mkdirSync(path.join(dir, 'refs')); // bare indicator in cwd
    assert.equal(isDisguisedBareGitRepo(dir), true);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('isDisguisedBareGitRepo: a lone objects/ directory is enough to flag', () => {
  const dir = mkTmp();
  try {
    fs.mkdirSync(path.join(dir, 'objects'));
    assert.equal(isDisguisedBareGitRepo(dir), true);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('commandInvokesGit: detects git across env prefixes, wrappers, and compounds', () => {
  assert.equal(commandInvokesGit('git status'), true);
  assert.equal(commandInvokesGit('git'), true);
  assert.equal(commandInvokesGit('GIT_DIR=/x git log'), true);
  assert.equal(commandInvokesGit('cd /tmp && git status'), true);
  assert.equal(commandInvokesGit('ls -la'), false);
  assert.equal(commandInvokesGit('echo git'), false);
});

test('check(): git from a disguised bare repo must prompt even under auto-allow', async () => {
  const dir = mkTmp();
  const prev = process.cwd();
  try {
    fs.writeFileSync(path.join(dir, 'HEAD'), 'ref: refs/heads/main\n');
    fs.mkdirSync(path.join(dir, 'objects'));
    fs.mkdirSync(path.join(dir, 'refs'));
    process.chdir(dir);
    const pm = new PermissionManager('auto-allow');
    // A git command here would execute attacker-planted hooks — never silently allow.
    assert.equal(await pm.check('bash', { command: 'git status' }), 'ask');
    // Non-git commands are unaffected by the guard.
    assert.equal(await pm.check('bash', { command: 'echo hi' }), 'allow');
  } finally {
    process.chdir(prev);
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('check(): git from a normal (non-bare) cwd is still auto-allowed', async () => {
  const dir = mkTmp();
  const prev = process.cwd();
  try {
    fs.mkdirSync(path.join(dir, '.git'));
    fs.writeFileSync(path.join(dir, '.git', 'HEAD'), 'ref: refs/heads/main\n');
    process.chdir(dir);
    const pm = new PermissionManager('auto-allow');
    assert.equal(await pm.check('bash', { command: 'git status' }), 'allow');
  } finally {
    process.chdir(prev);
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('check(): deny-all still denies git from a disguised bare repo (guard does not widen)', async () => {
  const dir = mkTmp();
  const prev = process.cwd();
  try {
    fs.writeFileSync(path.join(dir, 'HEAD'), 'ref: refs/heads/main\n');
    fs.mkdirSync(path.join(dir, 'objects'));
    fs.mkdirSync(path.join(dir, 'refs'));
    process.chdir(dir);
    const pm = new PermissionManager('deny-all');
    assert.equal(await pm.check('bash', { command: 'git status' }), 'deny');
  } finally {
    process.chdir(prev);
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
