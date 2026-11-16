import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  isGitInvocationReadOnly,
  getSimpleCommandPrefix,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003-w68 — Spec-driven flag-value skipping in the bash CLASSIFIER + rule base
// extraction (gap: references/claude/claude-code-main/src/utils/shell/specPrefix.ts
//   L40-137 findFirstSubcommand/flagTakesArg; utils/powershell/staticPrefix.ts
//   command-spec option schema). Before this, a global flag BEFORE the subcommand
// defeated SAFE_PREFIXES / DOCKER_SAFE_SUBCOMMANDS matching — `git -C /repo status`
// tested only `git`, `git -C`, `git -C /repo` (never `git status`) and the docker
// check read the first token after `docker` verbatim (`docker -D ps` → `-D`).
// ---------------------------------------------------------------------------

test('classifier: benign global flags before the git subcommand still reach a safe read', () => {
  const pm = new PermissionManager();
  // A non-value, non-exec-context global (`--no-pager`, `--no-optional-locks`) is
  // skipped so the real `status`/`log` subcommand is found and classified safe.
  assert.equal(pm.classifyBashCommand('git --no-pager status'), 'safe');
  assert.equal(pm.classifyBashCommand('git --no-optional-locks log'), 'safe');
  // A `-C /repo`-style value flag consumes its value; the plain command is safe.
  assert.equal(pm.classifyBashCommand('git status'), 'safe');
});

test('classifier: docker global flags are skipped to reach the real subcommand', () => {
  const pm = new PermissionManager();
  // `-D` (debug) is skipped → the real subcommand `ps` is read and classified safe.
  assert.equal(pm.classifyBashCommand('docker -D ps'), 'safe');
  // `-H <host>` consumes its value → `images` is the real subcommand.
  assert.equal(pm.classifyBashCommand('docker -H tcp://x:1 images'), 'safe');
  // After skipping globals the subcommand `run` is NOT read-only → unsafe.
  assert.equal(pm.classifyBashCommand('docker -H tcp://x:1 run ubuntu'), 'unsafe');
  // A write subcommand reached verbatim stays unsafe.
  assert.equal(pm.classifyBashCommand('docker rm foo'), 'unsafe');
});

test('classifier: npm global value-flag before the subcommand reaches a safe read', () => {
  const pm = new PermissionManager();
  // `--prefix /x` consumes its value → `list` is the real subcommand (SAFE_PREFIX).
  assert.equal(pm.classifyBashCommand('npm --prefix /x list'), 'safe');
});

test('classifier: a global flag before the subcommand works inside a compound chain', () => {
  const pm = new PermissionManager();
  // allSegmentsKnown must also skip globals per-segment.
  assert.equal(pm.classifyBashCommand('git --no-pager status && git --no-pager log'), 'safe');
});

test('SECURITY: git exec-context globals (-C / -c injection) are never auto-allowed', () => {
  const pm = new PermissionManager();
  // `-c core.pager=<cmd>` / `-c core.hooksPath=<dir>` inject config that runs
  // arbitrary code during a "read"; a different repo via `-C <dir>` can carry a
  // malicious `core.fsmonitor`. These must stay gated (parity with AMI's cd+git
  // RCE guard), even though the real subcommand is a read-only `status`.
  assert.equal(pm.classifyBashCommand('git -c core.pager=sh status'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git -c core.hooksPath=/evil status'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git -C /repo status'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git --git-dir=/evil/.git status'), 'unsafe');
});

test('isGitInvocationReadOnly: exec-context globals return false; benign globals pass', () => {
  // Exec-context change / config injection → not read-only safe.
  assert.equal(isGitInvocationReadOnly(['git', '-C', '/repo', 'status']), false);
  assert.equal(isGitInvocationReadOnly(['git', '-c', 'core.pager=sh', 'status']), false);
  assert.equal(isGitInvocationReadOnly(['git', '--git-dir=/evil/.git', 'status']), false);
  assert.equal(isGitInvocationReadOnly(['git', '--config-env=core.pager=X', 'status']), false);
  // Benign globals (non-value, non-exec-context) are skipped → read-only stays true.
  assert.equal(isGitInvocationReadOnly(['git', '--no-pager', 'status']), true);
  assert.equal(isGitInvocationReadOnly(['git', 'status']), true);
  // A write subcommand after a benign global is still a write.
  assert.equal(isGitInvocationReadOnly(['git', '--no-pager', 'branch', 'newref']), false);
});

test('getSimpleCommandPrefix: docker/npm global flags skipped for suggestion too', () => {
  // The same spec-driven skip underlies the "allow always" prefix suggestion.
  assert.equal(getSimpleCommandPrefix('docker -D ps'), 'docker ps');
  assert.equal(getSimpleCommandPrefix('npm --prefix /x list'), 'npm list');
  assert.equal(getSimpleCommandPrefix('git -C /repo status --short'), 'git status');
});
