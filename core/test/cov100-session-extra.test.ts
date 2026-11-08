import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { isPidAlive } from '../src/session';

/**
 * Targets the EPERM arm of isPidAlive():
 *
 *   catch (err) {
 *     if (err?.code === 'ESRCH') return false;
 *     if (err?.code === 'EPERM') return true;   // <-- this arm
 *     return false;
 *   }
 *
 * The existing suite only reaches EPERM via isPidAlive(1) (signalling init as a
 * non-root user). When the test runner is root (containers / CI), process.kill(1, 0)
 * SUCCEEDS instead of throwing EPERM, so isPidAlive returns true through the happy
 * path and the EPERM arm never executes. Mocking process.kill makes every arm
 * deterministic regardless of the runner's privilege level.
 */
describe('isPidAlive — deterministic error classification (root-safe)', () => {
  afterEach(() => mock.restoreAll());

  it('returns true when process.kill throws EPERM (process exists, not signalable)', () => {
    mock.method(process, 'kill', () => {
      const err: any = new Error('operation not permitted');
      err.code = 'EPERM';
      throw err;
    });
    // EPERM means the PID is owned by another user but is definitely alive.
    assert.equal(isPidAlive(424242), true);
  });

  it('returns false when process.kill throws ESRCH (no such process)', () => {
    mock.method(process, 'kill', () => {
      const err: any = new Error('no such process');
      err.code = 'ESRCH';
      throw err;
    });
    assert.equal(isPidAlive(424242), false);
  });

  it('returns false when process.kill throws an error without a known code', () => {
    mock.method(process, 'kill', () => {
      const err: any = new TypeError('pid must be an integer');
      // no .code -> falls through past ESRCH/EPERM to the final `return false`
      throw err;
    });
    assert.equal(isPidAlive(Number.NaN), false);
  });

  it('returns false when the thrown value is null (err?.code is undefined)', () => {
    mock.method(process, 'kill', () => {
      // optional-chaining guard: `null?.code` is undefined, so neither
      // ESRCH nor EPERM matches and the final `return false` fires.
      throw null;
    });
    assert.equal(isPidAlive(1), false);
  });

  it('returns true when process.kill succeeds (signal delivered, process alive)', () => {
    let seen: [number, number | string] | null = null;
    mock.method(process, 'kill', (pid: number, sig: number | string) => {
      seen = [pid, sig];
      return true;
    });
    assert.equal(isPidAlive(777), true);
    // Confirms isPidAlive probes with signal 0 (existence check, no actual signal).
    assert.deepEqual(seen, [777, 0]);
  });
});
