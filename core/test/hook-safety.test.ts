import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  MAX_HOOK_TIMEOUT_MS,
  MAX_HOOKS_PER_EVENT,
  BLOCKED_HOOK_COMMANDS,
  validateHookCommand,
  validateHookTimeout,
} from '../src/hooks';

describe('Hook safety constants', () => {
  it('MAX_HOOK_TIMEOUT_MS is 30000', () => {
    assert.equal(MAX_HOOK_TIMEOUT_MS, 30_000);
  });

  it('MAX_HOOKS_PER_EVENT is 10', () => {
    assert.equal(MAX_HOOKS_PER_EVENT, 10);
  });

  it('BLOCKED_HOOK_COMMANDS contains expected entries', () => {
    assert.ok(BLOCKED_HOOK_COMMANDS.has('rm -rf /'));
    assert.ok(BLOCKED_HOOK_COMMANDS.has('mkfs'));
    assert.ok(BLOCKED_HOOK_COMMANDS.has('dd if='));
    assert.ok(BLOCKED_HOOK_COMMANDS.has(':(){'));
    assert.ok(BLOCKED_HOOK_COMMANDS.has('shutdown'));
    assert.ok(BLOCKED_HOOK_COMMANDS.has('reboot'));
    assert.ok(BLOCKED_HOOK_COMMANDS.has('halt'));
    assert.ok(BLOCKED_HOOK_COMMANDS.has('init 0'));
    assert.ok(BLOCKED_HOOK_COMMANDS.has('init 6'));
  });
});

describe('validateHookCommand', () => {
  it('accepts normal commands', () => {
    assert.deepEqual(validateHookCommand('ls -la'), { valid: true });
    assert.deepEqual(validateHookCommand('echo hello'), { valid: true });
    assert.deepEqual(validateHookCommand('npm test'), { valid: true });
    assert.deepEqual(validateHookCommand('git status'), { valid: true });
  });

  it('rejects empty command', () => {
    const r = validateHookCommand('');
    assert.equal(r.valid, false);
    assert.ok(r.reason!.includes('empty'));
  });

  it('rejects whitespace-only command', () => {
    const r = validateHookCommand('   ');
    assert.equal(r.valid, false);
    assert.ok(r.reason!.includes('empty'));
  });

  it('rejects rm -rf /', () => {
    const r = validateHookCommand('rm -rf /');
    assert.equal(r.valid, false);
    assert.ok(r.reason!.includes('rm -rf /'));
  });

  it('rejects rm -rf / embedded in longer command', () => {
    const r = validateHookCommand('sudo rm -rf / --no-preserve-root');
    assert.equal(r.valid, false);
  });

  it('rejects mkfs', () => {
    const r = validateHookCommand('mkfs.ext4 /dev/sda1');
    assert.equal(r.valid, false);
    assert.ok(r.reason!.includes('mkfs'));
  });

  it('rejects dd if=', () => {
    const r = validateHookCommand('dd if=/dev/zero of=/dev/sda');
    assert.equal(r.valid, false);
    assert.ok(r.reason!.includes('dd if='));
  });

  it('rejects fork bomb pattern', () => {
    const r = validateHookCommand(':(){:|:&};:');
    assert.equal(r.valid, false);
    assert.ok(r.reason!.includes(':(){'));
  });

  it('rejects shutdown command', () => {
    const r = validateHookCommand('shutdown -h now');
    assert.equal(r.valid, false);
  });

  it('rejects reboot command', () => {
    const r = validateHookCommand('reboot');
    assert.equal(r.valid, false);
  });

  it('rejects device writes via redirect', () => {
    const r = validateHookCommand('echo bad > /dev/sda');
    assert.equal(r.valid, false);
    assert.ok(r.reason!.includes('device'));
  });

  it('rejects chmod 777 on /etc', () => {
    const r = validateHookCommand('chmod 777 /etc');
    assert.equal(r.valid, false);
    assert.ok(r.reason!.includes('chmod 777'));
  });

  it('rejects chmod 777 on /usr', () => {
    const r = validateHookCommand('chmod 777 /usr');
    assert.equal(r.valid, false);
  });

  it('rejects chmod 777 on /bin', () => {
    const r = validateHookCommand('chmod 777 /bin');
    assert.equal(r.valid, false);
  });

  it('rejects chmod 777 on /sbin', () => {
    const r = validateHookCommand('chmod 777 /sbin');
    assert.equal(r.valid, false);
  });

  it('allows chmod 777 on non-system paths', () => {
    assert.deepEqual(validateHookCommand('chmod 777 /tmp/foo'), { valid: true });
  });

  it('rejects overly long commands', () => {
    const long = 'echo ' + 'a'.repeat(4100);
    const r = validateHookCommand(long);
    assert.equal(r.valid, false);
    assert.ok(r.reason!.includes('maximum length'));
  });

  it('allows commands at exactly the length limit', () => {
    const atLimit = 'a'.repeat(4096);
    assert.deepEqual(validateHookCommand(atLimit), { valid: true });
  });
});

describe('validateHookTimeout', () => {
  it('clamps low values to 100', () => {
    assert.equal(validateHookTimeout(0), 100);
    assert.equal(validateHookTimeout(50), 100);
    assert.equal(validateHookTimeout(-1), 100);
  });

  it('clamps high values to MAX_HOOK_TIMEOUT_MS', () => {
    assert.equal(validateHookTimeout(60_000), MAX_HOOK_TIMEOUT_MS);
    assert.equal(validateHookTimeout(999_999), MAX_HOOK_TIMEOUT_MS);
  });

  it('passes valid values through', () => {
    assert.equal(validateHookTimeout(100), 100);
    assert.equal(validateHookTimeout(5000), 5000);
    assert.equal(validateHookTimeout(MAX_HOOK_TIMEOUT_MS), MAX_HOOK_TIMEOUT_MS);
    assert.equal(validateHookTimeout(10_000), 10_000);
  });
});
