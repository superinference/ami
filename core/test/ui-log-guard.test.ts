import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isProtectedLogTier,
  canModifyLog,
  UILogGuard,
} from '../src/ui-log-guard';
import type { LogTier } from '../src/ui-log-guard';

describe('isProtectedLogTier', () => {
  it('returns true for prepend', () => {
    assert.equal(isProtectedLogTier('prepend'), true);
  });

  it('returns true for append', () => {
    assert.equal(isProtectedLogTier('append'), true);
  });

  it('returns true for builtin', () => {
    assert.equal(isProtectedLogTier('builtin'), true);
  });

  it('returns true for engine', () => {
    assert.equal(isProtectedLogTier('engine'), true);
  });

  it('returns false for user', () => {
    assert.equal(isProtectedLogTier('user'), false);
  });

  it('returns false for core', () => {
    assert.equal(isProtectedLogTier('core'), false);
  });
});

describe('canModifyLog', () => {
  it('allows user modifying user log', () => {
    assert.equal(canModifyLog('user', 'user'), true);
  });

  it('denies user modifying prepend log', () => {
    assert.equal(canModifyLog('prepend', 'user'), false);
  });

  it('denies user modifying builtin log', () => {
    assert.equal(canModifyLog('builtin', 'user'), false);
  });

  it('denies user modifying engine log', () => {
    assert.equal(canModifyLog('engine', 'user'), false);
  });

  it('allows engine modifying prepend log', () => {
    assert.equal(canModifyLog('prepend', 'engine'), true);
  });

  it('allows engine modifying builtin log', () => {
    assert.equal(canModifyLog('builtin', 'engine'), true);
  });

  it('allows engine modifying append log', () => {
    assert.equal(canModifyLog('append', 'engine'), true);
  });

  it('allows builtin modifying prepend log', () => {
    assert.equal(canModifyLog('prepend', 'builtin'), true);
  });

  it('allows prepend modifying prepend log', () => {
    assert.equal(canModifyLog('prepend', 'prepend'), true);
  });

  it('allows any tier modifying core log (unprotected)', () => {
    assert.equal(canModifyLog('core', 'user'), true);
  });

  it('denies append modifying builtin log', () => {
    assert.equal(canModifyLog('builtin', 'append'), false);
  });

  it('denies core modifying engine log', () => {
    assert.equal(canModifyLog('engine', 'core'), false);
  });
});

describe('UILogGuard', () => {
  it('addLog creates entry with timestamp', () => {
    const guard = new UILogGuard();
    const before = Date.now();
    const entry = guard.addLog('hello', 'user', 'test-plugin');
    assert.equal(entry.message, 'hello');
    assert.equal(entry.tier, 'user');
    assert.equal(entry.source, 'test-plugin');
    assert.equal(entry.level, 'info');
    assert.ok(entry.timestamp >= before);
  });

  it('addLog respects max capacity', () => {
    const guard = new UILogGuard(2);
    guard.addLog('a', 'user', 's');
    guard.addLog('b', 'user', 's');
    guard.addLog('c', 'user', 's');
    assert.equal(guard.getLogCount(), 2);
    assert.equal(guard.getSuppressedCount(), 1);
  });

  it('addLog accepts custom level', () => {
    const guard = new UILogGuard();
    const entry = guard.addLog('err', 'engine', 'sys', 'error');
    assert.equal(entry.level, 'error');
  });

  it('checkModification blocks user on protected log', () => {
    const guard = new UILogGuard();
    guard.addLog('org msg', 'prepend', 'org-plugin');
    assert.equal(guard.checkModification(0, 'user'), false);
  });

  it('checkModification allows valid modifier', () => {
    const guard = new UILogGuard();
    guard.addLog('org msg', 'prepend', 'org-plugin');
    assert.equal(guard.checkModification(0, 'engine'), true);
  });

  it('checkModification returns false for out-of-bounds index', () => {
    const guard = new UILogGuard();
    assert.equal(guard.checkModification(99, 'engine'), false);
  });

  it('getLogsByTier filters correctly', () => {
    const guard = new UILogGuard();
    guard.addLog('a', 'user', 's');
    guard.addLog('b', 'prepend', 's');
    guard.addLog('c', 'user', 's');
    const userLogs = guard.getLogsByTier('user');
    assert.equal(userLogs.length, 2);
    assert.equal(userLogs[0].message, 'a');
  });

  it('getLogsByLevel filters correctly', () => {
    const guard = new UILogGuard();
    guard.addLog('info1', 'user', 's', 'info');
    guard.addLog('warn1', 'user', 's', 'warn');
    guard.addLog('info2', 'user', 's', 'info');
    const warns = guard.getLogsByLevel('warn');
    assert.equal(warns.length, 1);
    assert.equal(warns[0].message, 'warn1');
  });

  it('getLogsBySource filters correctly', () => {
    const guard = new UILogGuard();
    guard.addLog('a', 'user', 'alpha');
    guard.addLog('b', 'user', 'beta');
    guard.addLog('c', 'user', 'alpha');
    const alpha = guard.getLogsBySource('alpha');
    assert.equal(alpha.length, 2);
  });

  it('getLogCount returns correct count', () => {
    const guard = new UILogGuard();
    assert.equal(guard.getLogCount(), 0);
    guard.addLog('x', 'user', 's');
    assert.equal(guard.getLogCount(), 1);
  });

  it('getSuppressedCount tracks overflow', () => {
    const guard = new UILogGuard(1);
    guard.addLog('a', 'user', 's');
    guard.addLog('b', 'user', 's');
    guard.addLog('c', 'user', 's');
    assert.equal(guard.getSuppressedCount(), 2);
  });

  it('clear resets everything', () => {
    const guard = new UILogGuard(2);
    guard.addLog('a', 'user', 's');
    guard.addLog('b', 'user', 's');
    guard.addLog('c', 'user', 's');
    guard.clear();
    assert.equal(guard.getLogCount(), 0);
    assert.equal(guard.getSuppressedCount(), 0);
  });

  it('getLogs returns readonly array', () => {
    const guard = new UILogGuard();
    guard.addLog('x', 'engine', 'sys');
    const logs = guard.getLogs();
    assert.equal(logs.length, 1);
    assert.equal(logs[0].message, 'x');
  });
});
