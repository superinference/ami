import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  PROCESS_START_TIME_TOLERANCE_MS,
  mintSpawnToken,
  isPidPresent,
  computeStartTimeMs,
  readProcessStartTimeMsSync,
  readProcessStartTimeMs,
  captureProcessIdentitySync,
  probeProcessIdentitySync,
  probeProcessIdentity,
  isSafeToSteal,
  isProvenAlive,
  type ProcessIdentity,
} from '../src/utils/process-identity';

const ABSENT_PID = 2 ** 30; // almost certainly not a live process

function id(partial: Partial<ProcessIdentity> = {}): ProcessIdentity {
  return { pid: 4242, processStartTimeMs: 1000, spawnToken: null, ...partial };
}

describe('process-identity — spawn tokens', () => {
  it('mints unique 128-bit hex tokens', () => {
    const a = mintSpawnToken();
    const b = mintSpawnToken();
    assert.match(a, /^[0-9a-f]{32}$/);
    assert.notEqual(a, b);
  });
});

describe('process-identity — isPidPresent', () => {
  afterEach(() => mock.restoreAll());

  it('treats pid <= 1 as absent (self-group / init)', () => {
    assert.equal(isPidPresent(0), false);
    assert.equal(isPidPresent(1), false);
  });

  it('reports the current process as present', () => {
    assert.equal(isPidPresent(process.pid), true);
  });

  it('reports an ESRCH pid as absent', () => {
    mock.method(process, 'kill', () => { const e: NodeJS.ErrnoException = new Error('x'); e.code = 'ESRCH'; throw e; });
    assert.equal(isPidPresent(12345), false);
  });

  it('treats EPERM (present but foreign-owned) as present', () => {
    mock.method(process, 'kill', () => { const e: NodeJS.ErrnoException = new Error('x'); e.code = 'EPERM'; throw e; });
    assert.equal(isPidPresent(12345), true);
  });

  it('treats a code-less failure as present (fail-safe)', () => {
    mock.method(process, 'kill', () => { throw new Error('weird'); });
    assert.equal(isPidPresent(12345), true);
  });
});

describe('process-identity — computeStartTimeMs', () => {
  // btime 1000s → 1_000_000ms; starttime field 22 = 250 ticks → 2500ms.
  const comm = '1234 (node) S 1 1234 1234 0 -1 0 0 0 0 0 0 0 0 0 0 0 0 0 0';
  // Build a stat line with exactly field 22 (index 19 after ") ") = 250.
  function statWithStart(ticks: number, name = 'node'): string {
    const tail = Array.from({ length: 50 }, (_, i) => (i === 19 ? String(ticks) : '0')).join(' ');
    return `1234 (${name}) ${tail}`;
  }

  it('anchors starttime ticks against btime', () => {
    const ms = computeStartTimeMs(statWithStart(250), 'cpu 1 2 3\nbtime 1000\nprocesses 9');
    assert.equal(ms, 1_000_000 + 2500);
  });

  it('handles a comm containing spaces and parens', () => {
    const ms = computeStartTimeMs(statWithStart(100, 'my proc (x)'), 'btime 500');
    assert.equal(ms, 500_000 + 1000);
  });

  it('returns null when ticks are unparseable', () => {
    assert.equal(computeStartTimeMs('1234 (node) S', 'btime 1000'), null);
  });

  it('returns null when btime is missing', () => {
    assert.equal(computeStartTimeMs(statWithStart(250), 'cpu 1 2 3'), null);
  });

  it('matches the comm template used for reference', () => {
    assert.ok(comm.includes('node'));
  });
});

describe('process-identity — readProcessStartTimeMs (sync + async)', () => {
  it('returns null on non-linux platforms', async () => {
    assert.equal(readProcessStartTimeMsSync(process.pid, 'darwin'), null);
    assert.equal(readProcessStartTimeMsSync(process.pid, 'win32'), null);
    assert.equal(await readProcessStartTimeMs(process.pid, 'darwin'), null);
  });

  it('returns null for an absent pid on linux (unreadable /proc)', () => {
    assert.equal(readProcessStartTimeMsSync(ABSENT_PID, 'linux'), null);
  });

  if (process.platform === 'linux') {
    it('reads a real start time for the current process on linux', async () => {
      const sync = readProcessStartTimeMsSync(process.pid);
      const async = await readProcessStartTimeMs(process.pid);
      assert.equal(typeof sync, 'number');
      assert.equal(typeof async, 'number');
      // The two readings of the same process agree within tolerance.
      assert.ok(Math.abs((sync as number) - (async as number)) <= PROCESS_START_TIME_TOLERANCE_MS);
    });
  }
});

describe('process-identity — captureProcessIdentitySync', () => {
  it('snapshots the current pid + passed token', () => {
    const snap = captureProcessIdentitySync({ spawnToken: 'tok' });
    assert.equal(snap.pid, process.pid);
    assert.equal(snap.spawnToken, 'tok');
  });

  it('defaults token to null and honors a platform override', () => {
    const snap = captureProcessIdentitySync({ pid: 999, platform: 'darwin' });
    assert.equal(snap.pid, 999);
    assert.equal(snap.spawnToken, null);
    assert.equal(snap.processStartTimeMs, null); // darwin ⇒ unavailable
  });
});

describe('process-identity — probeProcessIdentitySync', () => {
  it('observedExit short-circuits to exited', () => {
    assert.deepEqual(probeProcessIdentitySync(id(), { observedExit: true }), { outcome: 'exited' });
  });

  it('absent pid ⇒ exited', () => {
    const p = probeProcessIdentitySync(id(), { isPidPresent: () => false });
    assert.equal(p.outcome, 'exited');
  });

  it('matching spawn token ⇒ live', () => {
    const p = probeProcessIdentitySync(id({ spawnToken: 'abc', processStartTimeMs: null }), {
      isPidPresent: () => true,
      readEchoedSpawnToken: () => 'abc',
    });
    assert.equal(p.outcome, 'live');
    assert.deepEqual(p.matchedOn, ['spawn-token']);
  });

  it('mismatching spawn token ⇒ identity-mismatch', () => {
    const p = probeProcessIdentitySync(id({ spawnToken: 'abc', processStartTimeMs: null }), {
      isPidPresent: () => true,
      readEchoedSpawnToken: () => 'zzz',
    });
    assert.equal(p.outcome, 'identity-mismatch');
    assert.equal(p.field, 'spawn-token');
  });

  it('matching start time ⇒ live', () => {
    const p = probeProcessIdentitySync(id({ processStartTimeMs: 5000 }), {
      isPidPresent: () => true,
      readProcessStartTimeMs: () => 5000 + 500, // within tolerance
    });
    assert.equal(p.outcome, 'live');
    assert.deepEqual(p.matchedOn, ['process-start-time']);
  });

  it('mismatching start time (no token) ⇒ identity-mismatch', () => {
    const p = probeProcessIdentitySync(id({ processStartTimeMs: 5000 }), {
      isPidPresent: () => true,
      readProcessStartTimeMs: () => 999_999,
    });
    assert.equal(p.outcome, 'identity-mismatch');
    assert.equal(p.field, 'process-start-time');
  });

  it('start-time contradiction WITH a verified token ⇒ indeterminate', () => {
    const p = probeProcessIdentitySync(id({ spawnToken: 'abc', processStartTimeMs: 5000 }), {
      isPidPresent: () => true,
      readEchoedSpawnToken: () => 'abc',
      readProcessStartTimeMs: () => 999_999,
    });
    assert.equal(p.outcome, 'indeterminate');
  });

  it('present pid with no verifiable evidence ⇒ indeterminate (fail closed)', () => {
    const p = probeProcessIdentitySync(id({ spawnToken: null, processStartTimeMs: null }), {
      isPidPresent: () => true,
    });
    assert.equal(p.outcome, 'indeterminate');
  });

  it('ignores a recorded token the host cannot echo', () => {
    const p = probeProcessIdentitySync(id({ spawnToken: 'abc', processStartTimeMs: 5000 }), {
      isPidPresent: () => true,
      readEchoedSpawnToken: () => null, // no echo
      readProcessStartTimeMs: () => 5000,
    });
    assert.equal(p.outcome, 'live');
    assert.deepEqual(p.matchedOn, ['process-start-time']);
  });

  it('start time unreadable (null) with a null recorded ⇒ falls to indeterminate', () => {
    const p = probeProcessIdentitySync(id({ processStartTimeMs: 5000, spawnToken: null }), {
      isPidPresent: () => true,
      readProcessStartTimeMs: () => null, // host couldn't read it
    });
    assert.equal(p.outcome, 'indeterminate');
  });
});

describe('process-identity — probeProcessIdentity (async)', () => {
  it('observedExit ⇒ exited', async () => {
    assert.deepEqual(await probeProcessIdentity(id(), { observedExit: true }), { outcome: 'exited' });
  });

  it('absent pid ⇒ exited', async () => {
    const p = await probeProcessIdentity(id(), { isPidPresent: () => false });
    assert.equal(p.outcome, 'exited');
  });

  it('matching token + start time ⇒ live', async () => {
    const p = await probeProcessIdentity(id({ spawnToken: 'abc', processStartTimeMs: 5000 }), {
      isPidPresent: () => true,
      readEchoedSpawnToken: async () => 'abc',
      readProcessStartTimeMs: async () => 5000,
    });
    assert.equal(p.outcome, 'live');
  });

  it('swallows a throwing token/start-time reader (⇒ indeterminate)', async () => {
    const p = await probeProcessIdentity(id({ spawnToken: 'abc', processStartTimeMs: 5000 }), {
      isPidPresent: () => true,
      readEchoedSpawnToken: async () => { throw new Error('boom'); },
      readProcessStartTimeMs: async () => { throw new Error('boom'); },
    });
    assert.equal(p.outcome, 'indeterminate');
  });

  it('uses the default /proc reader when none injected (absent pid ⇒ indeterminate)', async () => {
    // isPidPresent forced true but the default reader can't read ABSENT_PID's /proc.
    const p = await probeProcessIdentity(id({ pid: ABSENT_PID, processStartTimeMs: 5000 }), {
      isPidPresent: () => true,
    });
    assert.equal(p.outcome, 'indeterminate');
  });
});

describe('process-identity — steal decision', () => {
  it('isSafeToSteal: true only for exited / identity-mismatch', () => {
    assert.equal(isSafeToSteal({ outcome: 'exited' }), true);
    assert.equal(isSafeToSteal({ outcome: 'identity-mismatch', field: 'spawn-token' }), true);
    assert.equal(isSafeToSteal({ outcome: 'live', matchedOn: ['spawn-token'] }), false);
    assert.equal(isSafeToSteal({ outcome: 'indeterminate', reason: 'x' }), false);
  });

  it('isProvenAlive: true only for live', () => {
    assert.equal(isProvenAlive({ outcome: 'live', matchedOn: ['process-start-time'] }), true);
    assert.equal(isProvenAlive({ outcome: 'indeterminate', reason: 'x' }), false);
    assert.equal(isProvenAlive({ outcome: 'exited' }), false);
  });
});
