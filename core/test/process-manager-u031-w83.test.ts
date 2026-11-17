import { describe, it, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import {
  collectProcessTree,
  DESCENDANT_WALK_TIMEOUT_MS,
  shellHooks,
} from '../src/utils/shell';

// ---------------------------------------------------------------------------
// U031 / w83 — confirmed parity gap for process management:
//   Bounded + timed child/descendant-PID enumeration with an empty-result
//   fallback (genericProcessUtils.ts getChildPids: 1000ms cap + []-on-failure).
//
// AMI's DESCENDANT walk (collectProcessTree, backing killTree) read the system
// process table via execSync WITHOUT a timeout, unlike the ANCESTOR walk
// (readParentPidMap, bounded by ANCESTOR_WALK_TIMEOUT_MS). A hung `ps` during a
// kill/cleanup/shutdown could therefore stall the whole kill path indefinitely.
// This locks the descendant walk to a hard per-read timeout (parity with
// Claude's 1000ms getChildPids bound) that still degrades to [rootPid] on
// failure, matching the no-throw empty-result fallback.
//
// Red-green: reverting the `timeout: timeoutMs` wiring in collectProcessTree
// fails "passes the hard per-read timeout to execSync"; removing the constant
// fails the import.
// ---------------------------------------------------------------------------

afterEach(() => { mock.restoreAll(); });

describe('collectProcessTree — bounded + timed descendant enumeration', () => {
  function mockPosixTable(table: string): void {
    mock.method(shellHooks, 'platform', () => 'linux' as NodeJS.Platform);
    mock.method(shellHooks, 'execSync', (() => table) as typeof shellHooks.execSync);
  }

  it('passes the hard per-read timeout to execSync (default = DESCENDANT_WALK_TIMEOUT_MS)', () => {
    let seenTimeout: number | undefined;
    mock.method(shellHooks, 'execSync', ((_cmd: string, opts: { timeout?: number }) => {
      seenTimeout = opts?.timeout;
      return '100 1\n';
    }) as unknown as typeof shellHooks.execSync);
    collectProcessTree(100);
    assert.equal(seenTimeout, DESCENDANT_WALK_TIMEOUT_MS);
    assert.equal(typeof DESCENDANT_WALK_TIMEOUT_MS, 'number');
    assert.ok(DESCENDANT_WALK_TIMEOUT_MS > 0);
  });

  it('honors an explicit per-call timeout override', () => {
    let seenTimeout: number | undefined;
    mock.method(shellHooks, 'execSync', ((_cmd: string, opts: { timeout?: number }) => {
      seenTimeout = opts?.timeout;
      return '100 1\n';
    }) as unknown as typeof shellHooks.execSync);
    collectProcessTree(100, 250);
    assert.equal(seenTimeout, 250);
  });

  it('degrades to [rootPid] when the bounded read throws (timeout / ps unavailable)', () => {
    mock.method(shellHooks, 'platform', () => 'linux' as NodeJS.Platform);
    mock.method(shellHooks, 'execSync', (() => { throw new Error('ps timed out'); }) as typeof shellHooks.execSync);
    assert.deepEqual(collectProcessTree(4242), [4242]);
  });

  it('still enumerates the full descendant subtree (regression lock)', () => {
    // 100 -> 101 -> 102, plus an unrelated 900 that must NOT appear.
    mockPosixTable('100 1\n101 100\n102 101\n900 1\n');
    const tree = collectProcessTree(100);
    assert.deepEqual(new Set(tree), new Set([100, 101, 102]));
    assert.ok(!tree.includes(900));
  });
});
