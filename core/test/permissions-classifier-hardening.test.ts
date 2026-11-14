import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  normalizeLineContinuations,
  detectHardlineCommand,
} from '../src/permissions';

const pm = new PermissionManager();

test('normalizeLineContinuations joins only ODD backslash runs (even run keeps the newline)', () => {
  // odd (1) backslash before newline -> true continuation, joined
  assert.equal(normalizeLineContinuations('echo a\\\nb'), 'echo ab');
  // even (2) backslashes -> literal escape pair, newline kept as separator
  assert.equal(normalizeLineContinuations('echo a\\\\\nb'), 'echo a\\\\\nb');
  // no backslash -> newline kept
  assert.equal(normalizeLineContinuations('a\nb'), 'a\nb');
  // three backslashes (odd) -> join, one escaping backslash removed
  assert.equal(normalizeLineContinuations('x\\\\\\\ny'), 'x\\\\y');
});

test('PowerShell power-control / disk cmdlets are hardline-blocked', () => {
  assert.equal(detectHardlineCommand('Stop-Computer -Force').blocked, true);
  assert.equal(detectHardlineCommand('Restart-Computer').blocked, true);
  assert.equal(detectHardlineCommand('Clear-RecycleBin -Force').blocked, true);
  assert.equal(detectHardlineCommand('Format-Volume -DriveLetter D').blocked, true);
  assert.equal(detectHardlineCommand('Clear-Disk -Number 1').blocked, true);
  assert.equal(detectHardlineCommand('echo hello').blocked, false);
});

test('PowerShell recursive/forced deletes classify as destructive', () => {
  assert.equal(pm.classifyBashCommand('Remove-Item -Recurse -Force ./x'), 'destructive');
  assert.equal(pm.classifyBashCommand('Remove-Item -Force foo'), 'destructive');
  assert.equal(pm.classifyBashCommand('Clear-Content *'), 'destructive');
});

test('docker read-only auto-allow rejects $-substitution (parser differential)', () => {
  assert.equal(pm.classifyBashCommand('docker ps'), 'safe');
  assert.equal(pm.classifyBashCommand('docker ps $(whoami)'), 'unsafe');
  assert.equal(pm.classifyBashCommand('docker inspect $CONTAINER'), 'unsafe');
});

test('git read-only auto-allow rejects $-substitution', () => {
  assert.equal(pm.classifyBashCommand('git log --oneline'), 'safe');
  assert.equal(pm.classifyBashCommand('git log $(cat evil)'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git show $REF'), 'unsafe');
});

test('date: format strings are safe, clock-setting positionals/flags are not', () => {
  assert.equal(pm.classifyBashCommand('date'), 'safe');
  assert.equal(pm.classifyBashCommand('date -u'), 'safe');
  assert.equal(pm.classifyBashCommand('date +%Y-%m-%d'), 'safe');
  assert.equal(pm.classifyBashCommand("date '+%s'"), 'safe');
  assert.equal(pm.classifyBashCommand('date 010203042024'), 'unsafe');
  assert.equal(pm.classifyBashCommand('date -s "2020-01-01"'), 'unsafe');
});
