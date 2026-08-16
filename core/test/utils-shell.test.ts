import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { execCommand } from '../src/utils/shell';
import * as os from 'os';

// ---------------------------------------------------------------------------
// Basic command execution
// ---------------------------------------------------------------------------

describe('execCommand – basic execution', () => {
  it('runs a simple echo command', async () => {
    const result = await execCommand('echo hello', { cwd: process.cwd() });
    assert.ok(result.stdout.includes('hello'));
    assert.equal(result.exitCode, 0);
  });

  it('captures stderr', async () => {
    const result = await execCommand('echo error >&2', { cwd: process.cwd() });
    assert.ok(result.stderr.includes('error'));
    assert.equal(result.exitCode, 0);
  });

  it('returns exit code for failing commands', async () => {
    const result = await execCommand('exit 42', { cwd: process.cwd() });
    assert.equal(result.exitCode, 42);
  });

  it('handles commands with no output', async () => {
    const result = await execCommand('true', { cwd: process.cwd() });
    assert.equal(result.exitCode, 0);
    assert.equal(result.stdout, '');
  });
});

// ---------------------------------------------------------------------------
// cwd option
// ---------------------------------------------------------------------------

describe('execCommand – cwd', () => {
  it('executes in the specified working directory', async () => {
    const result = await execCommand('pwd', { cwd: '/tmp' });
    assert.ok(result.stdout.includes('/tmp'));
  });
});

// ---------------------------------------------------------------------------
// timeout option
// ---------------------------------------------------------------------------

describe('execCommand – timeout', () => {
  it('kills the command after the timeout', async () => {
    const result = await execCommand('sleep 60', {
      cwd: process.cwd(),
      timeout: 200,
    });
    // When killed, exitCode should be null
    assert.equal(result.exitCode, null);
  });
});

// ---------------------------------------------------------------------------
// abort signal
// ---------------------------------------------------------------------------

describe('execCommand – abort signal', () => {
  it('aborts immediately when signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const result = await execCommand('sleep 60', {
      cwd: process.cwd(),
      abortSignal: ac.signal,
    });
    assert.ok(result.stderr.includes('Aborted'));
    assert.equal(result.exitCode, null);
  });

  it('aborts a running command when signal fires', async () => {
    const ac = new AbortController();
    const promise = execCommand('sleep 60', {
      cwd: process.cwd(),
      abortSignal: ac.signal,
    });
    setTimeout(() => ac.abort(), 100);
    const result = await promise;
    assert.equal(result.exitCode, null);
  });
});

// ---------------------------------------------------------------------------
// onData callback
// ---------------------------------------------------------------------------

describe('execCommand – onData callback', () => {
  it('streams stdout chunks through onData', async () => {
    const chunks: string[] = [];
    const result = await execCommand('echo line1 && echo line2', {
      cwd: process.cwd(),
      onData: (chunk) => chunks.push(chunk),
    });
    assert.equal(result.exitCode, 0);
    const all = chunks.join('');
    assert.ok(all.includes('line1'));
    assert.ok(all.includes('line2'));
  });

  it('streams stderr chunks through onData', async () => {
    const chunks: string[] = [];
    await execCommand('echo err >&2', {
      cwd: process.cwd(),
      onData: (chunk) => chunks.push(chunk),
    });
    assert.ok(chunks.join('').includes('err'));
  });
});

// ---------------------------------------------------------------------------
// Output truncation
// ---------------------------------------------------------------------------

describe('execCommand – output truncation', () => {
  it('truncates very long stdout', async () => {
    // Generate output longer than MAX_OUTPUT_CHARS (100000)
    const result = await execCommand(
      'python3 -c "print(\'x\' * 120000)"',
      { cwd: process.cwd() },
    );
    if (result.stdout.length > 100000) {
      assert.ok(result.stdout.includes('[truncated]'));
    }
  });
});

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

describe('execCommand – error handling', () => {
  it('handles command that does not exist', async () => {
    const result = await execCommand('nonexistentcommand12345', {
      cwd: process.cwd(),
    });
    // Should get an error in stderr
    assert.ok(result.stderr.length > 0 || result.exitCode !== 0);
  });

  it('handles commands with special characters', async () => {
    const result = await execCommand('echo "hello world" && echo "done"', {
      cwd: process.cwd(),
    });
    assert.ok(result.stdout.includes('hello world'));
    assert.ok(result.stdout.includes('done'));
  });
});

// ---------------------------------------------------------------------------
// Timeout handling (lines 98-99)
// ---------------------------------------------------------------------------

describe('execCommand – timeout', () => {
  it('kills process tree on timeout', async () => {
    const result = await execCommand('sleep 30', {
      cwd: process.cwd(),
      timeout: 200,
    });
    // Process was killed by timeout — exitCode is null
    assert.equal(result.exitCode, null);
  });
});

// ---------------------------------------------------------------------------
// Abort signal handling (lines 107-112)
// ---------------------------------------------------------------------------

describe('execCommand – abort signal', () => {
  it('kills process on abort signal', async () => {
    const ac = new AbortController();
    const promise = execCommand('sleep 30', {
      cwd: process.cwd(),
      abortSignal: ac.signal,
    });
    setTimeout(() => ac.abort(), 200);
    const result = await promise;
    // Aborted — exitCode is null
    assert.equal(result.exitCode, null);
  });

  it('returns immediately when signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const result = await execCommand('echo should-not-run', {
      cwd: process.cwd(),
      abortSignal: ac.signal,
    });
    assert.equal(result.stderr, 'Aborted');
    assert.equal(result.exitCode, null);
  });
});

// ---------------------------------------------------------------------------
// Process error event (lines 105-108)
// ---------------------------------------------------------------------------

describe('execCommand – process error', () => {
  it('handles process error from non-existent cwd', async () => {
    const result = await execCommand('echo test', {
      cwd: '/nonexistent/path/xyz',
    });
    // Should get an error in stderr or null exit code
    assert.ok(result.stderr.length > 0 || result.exitCode === null);
  });
});

// ---------------------------------------------------------------------------
// EBADF regression: killing a process with active pipes must not crash
// ---------------------------------------------------------------------------

describe('execCommand – EBADF regression', () => {
  it('does not throw EBADF when a process is killed while pipes are active', async () => {
    // Spawn a process that writes continuously to stdout, then timeout-kill it.
    // Before the fix, this could leave pipe fds in an invalid state.
    const result = await execCommand(
      'while true; do echo ping; sleep 0.01; done',
      { cwd: os.tmpdir(), timeout: 200 },
    );
    // Timeout should produce null exit code, not an EBADF crash
    assert.equal(result.exitCode, null);
    assert.ok(result.stdout.includes('ping'));
  });

  it('does not throw EBADF when a process is aborted while pipes are active', async () => {
    const ac = new AbortController();
    const resultP = execCommand(
      'while true; do echo data; sleep 0.01; done',
      { cwd: os.tmpdir(), timeout: 10000, abortSignal: ac.signal },
    );
    await new Promise(r => setTimeout(r, 200));
    ac.abort();
    const result = await resultP;
    assert.equal(result.exitCode, null);
  });
});
