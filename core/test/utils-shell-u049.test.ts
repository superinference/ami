import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import { EventEmitter, once } from 'events';
import { PassThrough } from 'stream';

import { execCommand, shellHooks } from '../src/utils/shell';

// Deterministic fake child process: its stdout/stderr are PassThroughs the test
// drives, and `close` is emitted manually to settle execCommand.
class FakeProc extends EventEmitter {
  stdout = new PassThrough();
  stderr = new PassThrough();
  stdin = new PassThrough();
  pid: number | undefined = 4242;
  kill(): boolean { return true; }
}

afterEach(() => {
  mock.restoreAll();
});

// ---------------------------------------------------------------------------
// windowsHide spawn flag (U049 — parity with Claude Shell.ts:336)
// ---------------------------------------------------------------------------

describe('execCommand – windowsHide spawn flag', () => {
  it('passes windowsHide: true to spawn so Windows never flashes a console window', async () => {
    const fake = new FakeProc();
    let capturedOpts: Record<string, unknown> | undefined;
    mock.method(shellHooks, 'spawn', (_cmd: string, _args: string[], opts: Record<string, unknown>) => {
      capturedOpts = opts;
      return fake as unknown as ReturnType<typeof shellHooks.spawn>;
    });
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    fake.emit('close', 0, null);
    await p;
    assert.ok(capturedOpts, 'spawn should have been called');
    assert.equal(capturedOpts!.windowsHide, true);
  });
});

// ---------------------------------------------------------------------------
// Enriched truncation message reporting total size + kept bounds (U049)
// ---------------------------------------------------------------------------

describe('execCommand – truncation message reports total size', () => {
  it('reports kept head+tail and the true total char count when output is truncated', async () => {
    const fake = new FakeProc();
    mock.method(shellHooks, 'spawn', () => fake as unknown as ReturnType<typeof shellHooks.spawn>);
    const p = execCommand('cat bigfile', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    // Emit more than HEAD_CHARS (670k) + TAIL_CHARS (330k): 100 chars must drop.
    const big = 'x'.repeat(670_000 + 330_000 + 100);
    fake.stdout.end(big);
    await once(fake.stdout, 'end'); // all chunks consumed by execCommand's reader
    fake.emit('close', 0, null);
    const r = await p;
    assert.match(r.stdout, /kept first 670000 \+ last 330000 of 1000100 chars; 100 truncated/);
  });

  it('does not add a truncation note when output fits within the bound', async () => {
    const fake = new FakeProc();
    mock.method(shellHooks, 'spawn', () => fake as unknown as ReturnType<typeof shellHooks.spawn>);
    const p = execCommand('echo small', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    fake.stdout.end('small-output');
    await once(fake.stdout, 'end');
    fake.emit('close', 0, null);
    const r = await p;
    assert.equal(r.stdout, 'small-output');
    assert.ok(!r.stdout.includes('truncated'));
  });
});
