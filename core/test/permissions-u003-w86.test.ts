import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  getDestructiveCommandWarning,
  PermissionManager,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 (w86): three additive, pure-core permission-parity refinements, each
// wired through an EXISTING consumer (no new schema field, no cross-layer churn):
//
//  1. `git clean` dry-run is no longer classified destructive. A `-n`/`--dry-run`
//     preview only LISTS what would be removed (read-only); only an actual clean
//     (`-f`/`--force`) deletes untracked files. AMI classified every `git clean`
//     as destructive via a flat DESTRUCTIVE_COMMANDS membership, so `git clean -n`
//     was wrongly escalated to a destructive-approval prompt. The warning engine
//     already excluded dry-runs; this brings the CLASSIFIER into line, sharing one
//     canonical regex. Parity with Claude's git-clean negative-lookahead + required
//     `-f` (destructiveCommandWarning.ts L67-71).
//
//  2. `printf` added to the xargs safe-target allowlist. `printf` is a pure
//     read-only output command (no file writes), so `… | xargs printf` is safe to
//     auto-classify, matching Claude's SAFE_TARGET_COMMANDS_FOR_XARGS (which lists
//     echo/printf/wc/grep/head/tail). AMI's list omitted `printf`.
//
//  3. Specific destructive-warning NOTES for the PowerShell deletions AMI already
//     escalates (`Remove-Item -Recurse/-Force`, `Clear-Content`). classifyBashCommand
//     classifies these as `destructive`, but getDestructiveCommandWarning returned
//     null, so the prompt fell back to the generic "is destructive" line. Add the
//     per-pattern note so the prompt explains WHY. Parity with Claude's PowerShell
//     destructiveCommandWarning cmdlet notes.
// ---------------------------------------------------------------------------

// --- Feature 1: git clean dry-run is not destructive -----------------------

test('classifyBashCommand: `git clean -n` (dry-run) is NOT destructive', () => {
  const pm = new PermissionManager('ask');
  assert.notEqual(pm.classifyBashCommand('git clean -n'), 'destructive');
  assert.notEqual(pm.classifyBashCommand('git clean --dry-run'), 'destructive');
});

test('classifyBashCommand: an actual `git clean -f` stays destructive', () => {
  const pm = new PermissionManager('ask');
  assert.equal(pm.classifyBashCommand('git clean -fd'), 'destructive');
  assert.equal(pm.classifyBashCommand('git clean -f'), 'destructive');
  // `-n` overrides `-f` into a dry run — not destructive.
  assert.notEqual(pm.classifyBashCommand('git clean -nf'), 'destructive');
});

test('classifyBashCommand: a destructive sibling in a dry-run compound still wins', () => {
  const pm = new PermissionManager('ask');
  assert.equal(pm.classifyBashCommand('git clean -n && rm -rf build'), 'destructive');
});

// --- Feature 2: printf is an xargs-safe target -----------------------------

test('classifyBashCommand: `… | xargs printf` is safe (read-only target)', () => {
  const pm = new PermissionManager('ask');
  assert.equal(pm.classifyBashCommand('echo hi | xargs printf %s'), 'safe');
  // regression: a non-read-only xargs target is still unsafe.
  assert.equal(pm.classifyBashCommand('echo hi | xargs chmod +x'), 'unsafe');
});

// --- Feature 3: PowerShell destructive-warning notes -----------------------

test('getDestructiveCommandWarning: Remove-Item -Recurse -Force carries a specific note', () => {
  assert.equal(
    getDestructiveCommandWarning('Remove-Item -Recurse -Force C:\\tmp\\build'),
    'Note: may recursively force-remove files',
  );
  // `-Force` without `-Recurse` force-removes but does NOT recurse into dirs, so
  // it gets the narrower flag-differentiated note (not the recurse+force note).
  assert.equal(
    getDestructiveCommandWarning('Remove-Item -Force foo'),
    'Note: may force-remove files',
  );
});

test('getDestructiveCommandWarning: Clear-Content carries a specific note', () => {
  assert.equal(
    getDestructiveCommandWarning('Clear-Content app.log'),
    'Note: may erase the contents of the target files',
  );
});

test('getDestructiveCommandWarning: a plain Remove-Item (no -Recurse/-Force) is not flagged', () => {
  assert.equal(getDestructiveCommandWarning('Remove-Item foo.txt'), null);
});

// --- e2e: the PowerShell note surfaces through the destructive safetyCheck --

test('checkWithReason surfaces the Remove-Item note in the destructive safetyCheck', async () => {
  const pm = new PermissionManager('ask');
  const r = await pm.checkWithReason('bash', { command: 'Remove-Item -Recurse -Force build' });
  assert.equal(r.decision, 'ask');
  assert.ok(r.reason && r.reason.type === 'safetyCheck' && r.reason.check === 'destructive');
  assert.match(r.message ?? '', /may recursively force-remove files/);
});

test('checkWithReason: a `git clean -n` dry-run is not gated as destructive', async () => {
  const pm = new PermissionManager('ask');
  const r = await pm.checkWithReason('bash', { command: 'git clean -n' });
  // It may still ask (git is not read-only), but NOT via the destructive check.
  assert.ok(!(r.reason && r.reason.type === 'safetyCheck' && r.reason.check === 'destructive'));
});
