import { test } from 'node:test';
import assert from 'node:assert/strict';

import { getDestructiveCommandWarning } from '../src/permissions';

// ---------------------------------------------------------------------------
// U015-w86 — PowerShell destructive-command advisory warnings.
//
// getDestructiveCommandWarning is the purely-informational note surfaced in the
// permission prompt (it never gates auto-approval). Before this change it only
// recognised POSIX/bash signatures, so every PowerShell destructive cmdlet
// returned null — the operator saw no WHY-is-this-dangerous note. Parity with
// Claude's PowerShellTool/destructiveCommandWarning.ts DESTRUCTIVE_PATTERNS.
// ---------------------------------------------------------------------------

test('Remove-Item -Recurse -Force warns about recursive force-removal', () => {
  assert.equal(
    getDestructiveCommandWarning('Remove-Item -Recurse -Force ./build'),
    'Note: may recursively force-remove files',
  );
  // Flag order is irrelevant.
  assert.equal(
    getDestructiveCommandWarning('Remove-Item -Force -Recurse ./build'),
    'Note: may recursively force-remove files',
  );
});

test('Remove-Item with only -Recurse or only -Force gets the narrower note', () => {
  assert.equal(
    getDestructiveCommandWarning('Remove-Item -Recurse ./logs'),
    'Note: may recursively remove files',
  );
  assert.equal(
    getDestructiveCommandWarning('Remove-Item -Force ./file.txt'),
    'Note: may force-remove files',
  );
});

test('Remove-Item aliases (ri) with -Force -Recurse warn', () => {
  assert.equal(
    getDestructiveCommandWarning('ri -Force -Recurse ./x'),
    'Note: may recursively force-remove files',
  );
});

test('a bare ri (ruby ri) without PowerShell flags does NOT warn', () => {
  assert.equal(getDestructiveCommandWarning('ri Array#map'), null);
});

test('warning is anchored to a statement start inside a block', () => {
  assert.equal(
    getDestructiveCommandWarning('Get-ChildItem | ForEach-Object { Remove-Item -Recurse -Force $_ }'),
    'Note: may recursively force-remove files',
  );
});

test('Clear-Content on a glob gets the multi-file note; a single file gets the general note', () => {
  assert.equal(
    getDestructiveCommandWarning('Clear-Content ./logs/*.log'),
    'Note: may clear content of multiple files',
  );
  // A single-target Clear-Content still erases that file's contents in place —
  // real data loss — so it carries the general content-erasure note.
  assert.equal(
    getDestructiveCommandWarning('Clear-Content ./one.txt'),
    'Note: may erase the contents of the target files',
  );
});

test('disk-level cmdlets warn', () => {
  assert.equal(
    getDestructiveCommandWarning('Format-Volume -DriveLetter D'),
    'Note: may format a disk volume',
  );
  assert.equal(
    getDestructiveCommandWarning('Clear-Disk -Number 1 -RemoveData'),
    'Note: may clear a disk',
  );
  assert.equal(
    getDestructiveCommandWarning('Clear-RecycleBin -Force'),
    'Note: permanently deletes recycled files',
  );
});

test('power-state cmdlets warn with the certain "will" wording', () => {
  assert.equal(
    getDestructiveCommandWarning('Stop-Computer -Force'),
    'Note: will shut down the computer',
  );
  assert.equal(
    getDestructiveCommandWarning('Restart-Computer'),
    'Note: will restart the computer',
  );
});

test('a benign read-only PowerShell command gets no warning', () => {
  assert.equal(getDestructiveCommandWarning('Get-ChildItem -Recurse ./src'), null);
  assert.equal(getDestructiveCommandWarning('Get-Content ./file.txt'), null);
});

test('existing POSIX warnings are unchanged (regression lock)', () => {
  assert.equal(
    getDestructiveCommandWarning('rm -rf ./build'),
    'Note: may recursively force-remove files',
  );
  assert.equal(
    getDestructiveCommandWarning('git reset --hard'),
    'Note: may discard uncommitted changes',
  );
});
