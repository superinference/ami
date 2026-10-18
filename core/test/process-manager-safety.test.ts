import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { spawn } from 'child_process';
import {
  GRACEFUL_SHUTDOWN_MS,
  MAX_BACKGROUND_PROCESSES,
  isProcessAlive,
  forceKillAfterTimeout,
  validateBackgroundCommand,
} from '../src/process-manager';

describe('GRACEFUL_SHUTDOWN_MS', () => {
  it('is 1500', () => {
    assert.equal(GRACEFUL_SHUTDOWN_MS, 1500);
  });
});

describe('MAX_BACKGROUND_PROCESSES', () => {
  it('is 20', () => {
    assert.equal(MAX_BACKGROUND_PROCESSES, 20);
  });
});

describe('isProcessAlive', () => {
  it('returns true for current process', () => {
    assert.equal(isProcessAlive(process.pid), true);
  });

  it('returns false for nonexistent PID', () => {
    assert.equal(isProcessAlive(99999999), false);
  });

  it('returns true for a running child process', () => {
    const child = spawn('sleep', ['60'], { stdio: 'ignore' });
    try {
      assert.equal(isProcessAlive(child.pid!), true);
    } finally {
      child.kill('SIGKILL');
    }
  });
});

describe('forceKillAfterTimeout', () => {
  it('kills a running sleep process', async () => {
    const child = spawn('sleep', ['120'], { stdio: 'ignore' });
    const pid = child.pid!;
    assert.equal(isProcessAlive(pid), true);
    const result = await forceKillAfterTimeout(pid, 200);
    assert.equal(result, true);
    // Give OS a moment to clean up
    await new Promise(r => setTimeout(r, 100));
    assert.equal(isProcessAlive(pid), false);
  });

  it('returns false for a nonexistent PID', async () => {
    const result = await forceKillAfterTimeout(99999999);
    assert.equal(result, false);
  });

  it('returns true when process dies during grace period', async () => {
    const child = spawn('sleep', ['0.05'], { stdio: 'ignore' });
    const pid = child.pid!;
    // The process runs for 50ms; timeout is 2s — it will exit on its own
    const result = await forceKillAfterTimeout(pid, 2000);
    assert.equal(result, true);
  });
});

describe('validateBackgroundCommand', () => {
  it('allows normal commands', () => {
    assert.deepEqual(validateBackgroundCommand('ls -la'), { allowed: true });
    assert.deepEqual(validateBackgroundCommand('npm test'), { allowed: true });
    assert.deepEqual(validateBackgroundCommand('node server.js'), { allowed: true });
    assert.deepEqual(validateBackgroundCommand('grep -r "foo" .'), { allowed: true });
  });

  it('rejects empty command', () => {
    const r = validateBackgroundCommand('');
    assert.equal(r.allowed, false);
    assert.ok(r.reason!.includes('empty'));
  });

  it('rejects whitespace-only command', () => {
    const r = validateBackgroundCommand('   ');
    assert.equal(r.allowed, false);
  });

  it('rejects while true; do loop', () => {
    const r = validateBackgroundCommand('while true; do echo x; done');
    assert.equal(r.allowed, false);
    assert.ok(r.reason!.includes('infinite'));
  });

  it('rejects while : ; do loop', () => {
    const r = validateBackgroundCommand('while : ; do sleep 1; done');
    assert.equal(r.allowed, false);
  });

  it('rejects while(true) loop', () => {
    const r = validateBackgroundCommand('while( true ) { echo; }');
    assert.equal(r.allowed, false);
  });

  it('rejects for(;;) loop', () => {
    const r = validateBackgroundCommand('for(;;) { echo; }');
    assert.equal(r.allowed, false);
  });

  it('rejects yes | pipe', () => {
    const r = validateBackgroundCommand('yes | head -5');
    assert.equal(r.allowed, false);
  });

  it('rejects cat /dev/urandom', () => {
    const r = validateBackgroundCommand('cat /dev/urandom | head');
    assert.equal(r.allowed, false);
  });

  it('rejects tail -f /dev/null', () => {
    const r = validateBackgroundCommand('tail -f /dev/null');
    assert.equal(r.allowed, false);
  });

  it('rejects commands discarding all output to /dev/null', () => {
    const r = validateBackgroundCommand('some_cmd > /dev/null 2>&1');
    assert.equal(r.allowed, false);
    assert.ok(r.reason!.includes('/dev/null'));
  });

  it('rejects &>/dev/null output discard', () => {
    const r = validateBackgroundCommand('some_cmd &>/dev/null');
    assert.equal(r.allowed, false);
  });

  it('allows commands with partial /dev/null redirect', () => {
    // stderr-only redirect is fine — stdout still captured
    assert.deepEqual(validateBackgroundCommand('cmd 2>/dev/null'), { allowed: true });
  });
});
