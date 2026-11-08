import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager } from '../src/process-manager';

// ---------------------------------------------------------------------------
// Targeted coverage for ProcessManager.destroyStreams() — the private helper
// kill() uses to tear down a child's stdio pipes BEFORE sending SIGKILL so the
// OS closing the child end of the pipe can't surface an uncaught EBADF.
//
// The happy path (live stream, destroy() succeeds) is already covered by the
// EBADF regression test. The UNCOVERED path is the defensive `catch` arm: when
// `stream.destroy()` itself throws, destroyStreams must swallow the error (the
// whole point of the guard) and keep tearing down the remaining streams.
//
// We drive the real method with deterministic fake stream objects — no child
// processes, no signals, no races. Each stream records whether destroy() ran so
// we can assert the branch decisions (null skip / already-destroyed skip / live
// destroy / throwing destroy swallowed) behave correctly and independently.
// ---------------------------------------------------------------------------

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-cov100-'));
}

type FakeStream = { destroyed: boolean; destroyCalls: number; destroy: () => void };

function liveStream(): FakeStream {
  const s: FakeStream = {
    destroyed: false,
    destroyCalls: 0,
    destroy() {
      s.destroyCalls++;
      s.destroyed = true;
    },
  };
  return s;
}

function throwingStream(): FakeStream {
  const s: FakeStream = {
    destroyed: false,
    destroyCalls: 0,
    destroy() {
      s.destroyCalls++;
      throw Object.assign(new Error('EBADF: bad file descriptor, close'), { code: 'EBADF' });
    },
  };
  return s;
}

describe('ProcessManager.destroyStreams — defensive teardown', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(() => {
    if (pm) pm.cleanup();
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('swallows an error thrown by stream.destroy() (the EBADF guard)', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);

    const stdout = throwingStream();
    const fakeProc = { stdout, stderr: null, stdin: null } as any;

    // The uncovered catch arm: destroy() throws and the error MUST NOT propagate.
    assert.doesNotThrow(() => (pm as any).destroyStreams(fakeProc));
    // Proof the throwing branch actually executed (destroy was attempted once).
    assert.equal(stdout.destroyCalls, 1);
  });

  it('keeps destroying the remaining streams after one throws', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);

    const stdout = throwingStream();
    const stderr = liveStream();
    const stdin = liveStream();
    const fakeProc = { stdout, stderr, stdin } as any;

    assert.doesNotThrow(() => (pm as any).destroyStreams(fakeProc));

    // stdout threw but the loop continued to the next streams.
    assert.equal(stdout.destroyCalls, 1);
    assert.equal(stderr.destroyCalls, 1);
    assert.equal(stderr.destroyed, true);
    assert.equal(stdin.destroyCalls, 1);
    assert.equal(stdin.destroyed, true);
  });

  it('destroys a live stream exactly once', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);

    const stdout = liveStream();
    const fakeProc = { stdout, stderr: null, stdin: null } as any;

    (pm as any).destroyStreams(fakeProc);
    assert.equal(stdout.destroyCalls, 1);
    assert.equal(stdout.destroyed, true);
  });

  it('skips already-destroyed streams (does not call destroy again)', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);

    const stdout = liveStream();
    stdout.destroyed = true; // pretend it was torn down earlier
    const fakeProc = { stdout, stderr: null, stdin: null } as any;

    (pm as any).destroyStreams(fakeProc);
    assert.equal(stdout.destroyCalls, 0);
  });

  it('skips null / absent streams without throwing', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);

    const fakeProc = { stdout: null, stderr: undefined, stdin: null } as any;
    assert.doesNotThrow(() => (pm as any).destroyStreams(fakeProc));
  });
});
