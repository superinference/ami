import { describe, it, afterEach, after, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { ProcessManager, type TaskMessage } from '../src/process-manager';
import {
  collectAncestorPids,
  readParentPidMap,
  ANCESTOR_WALK_TIMEOUT_MS,
  shellHooks,
} from '../src/utils/shell';

// ---------------------------------------------------------------------------
// U031 / w80: three confirmed parity gaps for process management —
//  (1) live message-sync of a foregrounded AGENT into the main view + its abort
//      signal adopted for Escape  (useSessionBackgrounding.ts:L91-L127)
//  (2) bounded + timed cross-platform ancestor-PID chain walk
//      (genericProcessUtils.ts:L36-L86)
//  (3) on-kill transcript truncation to the final message (memory release)
//      (spawnInProcess.ts:L287-L296)
// Red-green: reverting appendTaskMessage / syncForegroundedMessages /
// collectAncestorPids / releaseKilledTranscript fails the matching cases below.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function makeTmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-w80-'));
  tmpDirs.push(d);
  return d;
}
function makePm(): ProcessManager {
  return new ProcessManager(makeTmpDir());
}
function registerAgent(pm: ProcessManager, id: string, opts: { abortSignal?: AbortSignal; abortFn?: () => void } = {}): void {
  pm.registerAgent(id, {
    command: `[agent] ${id}`,
    description: id,
    outputPath: path.join(makeTmpDir(), `${id}.out`),
    abortFn: opts.abortFn ?? (() => { /* no-op abort */ }),
    ...(opts.abortSignal ? { abortSignal: opts.abortSignal } : {}),
  });
}
function msg(role: TaskMessage['role'], content: string): TaskMessage {
  return { role, content };
}

afterEach(() => { mock.restoreAll(); });
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best-effort */ } }
});

// ---------------------------------------------------------------------------
// Gap 2 — collectAncestorPids / readParentPidMap (bounded + timed + no-throw)
// ---------------------------------------------------------------------------

describe('collectAncestorPids (POSIX)', () => {
  function mockPosixTable(table: string): void {
    mock.method(shellHooks, 'platform', () => 'linux' as NodeJS.Platform);
    mock.method(shellHooks, 'execSync', (() => table) as typeof shellHooks.execSync);
  }

  it('walks the parent chain from immediate parent to furthest, stopping at init', () => {
    // 100 -> 90 -> 80 -> (ppid 1 = init, stop)
    mockPosixTable('  100  90\n   90  80\n   80   1\n 9999   1\n');
    assert.deepEqual(collectAncestorPids(100), [90, 80]);
  });

  it('caps the walk at maxDepth even for a long chain', () => {
    mockPosixTable('100 90\n90 80\n80 70\n70 60\n60 50\n50 2\n');
    assert.deepEqual(collectAncestorPids(100, 2), [90, 80]);
  });

  it('breaks a ppid cycle via the visited-set (no infinite loop)', () => {
    // 100 -> 90 -> 100 (cycle)
    mockPosixTable('100 90\n90 100\n');
    assert.deepEqual(collectAncestorPids(100, 10), [90]);
  });

  it('returns [] for a pid <= 1, a non-integer pid, or maxDepth <= 0', () => {
    mockPosixTable('100 90\n90 2\n');
    assert.deepEqual(collectAncestorPids(1), []);
    assert.deepEqual(collectAncestorPids(0), []);
    assert.deepEqual(collectAncestorPids(1.5), []);
    assert.deepEqual(collectAncestorPids(100, 0), []);
  });

  it('returns [] when the start pid is absent from the table', () => {
    mockPosixTable('200 100\n100 2\n');
    assert.deepEqual(collectAncestorPids(999), []);
  });

  it('no-throws to [] on an exec failure and on an empty table', () => {
    mock.method(shellHooks, 'platform', () => 'linux' as NodeJS.Platform);
    mock.method(shellHooks, 'execSync', (() => { throw new Error('ps unavailable'); }) as typeof shellHooks.execSync);
    assert.deepEqual(collectAncestorPids(100), []);
    mock.restoreAll();
    mockPosixTable('   \n\n');
    assert.deepEqual(collectAncestorPids(100), []);
  });

  it('passes the hard per-read timeout to execSync (default = ANCESTOR_WALK_TIMEOUT_MS)', () => {
    mock.method(shellHooks, 'platform', () => 'linux' as NodeJS.Platform);
    let seenTimeout: number | undefined;
    mock.method(shellHooks, 'execSync', ((_cmd: string, opts: { timeout?: number }) => {
      seenTimeout = opts?.timeout;
      return '100 2\n';
    }) as unknown as typeof shellHooks.execSync);
    collectAncestorPids(100);
    assert.equal(seenTimeout, ANCESTOR_WALK_TIMEOUT_MS);
    readParentPidMap(1234);
    assert.equal(seenTimeout, 1234);
  });
});

describe('readParentPidMap (Windows column order)', () => {
  it('parses wmic ParentProcessId,ProcessId columns (child -> parent)', () => {
    mock.method(shellHooks, 'platform', () => 'win32' as NodeJS.Platform);
    // wmic prints the header then `<ParentProcessId>  <ProcessId>` rows.
    mock.method(shellHooks, 'execSync', (() => 'ParentProcessId  ProcessId\n90  100\n80  90\n') as typeof shellHooks.execSync);
    const map = readParentPidMap();
    assert.equal(map.get(100), 90);
    assert.equal(map.get(90), 80);
    assert.deepEqual(collectAncestorPids(100, 10), [90, 80]);
  });
});

// ---------------------------------------------------------------------------
// Gap 1 — per-task message buffer + foreground sync
// ---------------------------------------------------------------------------

describe('ProcessManager.appendTaskMessage / getTaskMessages', () => {
  it('appends to a running agent and returns a snapshot copy', () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    assert.equal(pm.appendTaskMessage('a1', msg('user', 'hi')), true);
    assert.equal(pm.appendTaskMessage('a1', msg('assistant', 'yo')), true);
    const got = pm.getTaskMessages('a1');
    assert.deepEqual(got.map((m) => m.content), ['hi', 'yo']);
    // Snapshot is a copy — mutating it does not affect the buffer.
    got.push(msg('user', 'leak'));
    assert.equal(pm.getTaskMessages('a1').length, 2);
  });

  it('refuses unknown, bash, and terminal tasks', () => {
    const pm = makePm();
    assert.equal(pm.appendTaskMessage('nope', msg('user', 'x')), false);
    const bashId = pm.spawn('echo hi', { cwd: makeTmpDir() });
    assert.equal(pm.appendTaskMessage(bashId, msg('user', 'x')), false);
    registerAgent(pm, 'a2');
    pm.kill('a2'); // now terminal
    assert.equal(pm.appendTaskMessage('a2', msg('user', 'x')), false);
    assert.deepEqual(pm.getTaskMessages('nope'), []);
    pm.cleanup();
  });
});

describe('ProcessManager.syncForegroundedMessages', () => {
  it('returns null when nothing is foregrounded', () => {
    const pm = makePm();
    assert.equal(pm.syncForegroundedMessages(), null);
  });

  it('syncs a running agent, flags changed only when the buffer grows, and adopts its abort signal', () => {
    const pm = makePm();
    const ac = new AbortController();
    registerAgent(pm, 'a1', { abortSignal: ac.signal });
    pm.appendTaskMessage('a1', msg('user', 'q'));
    pm.appendTaskMessage('a1', msg('assistant', 'a'));
    assert.equal(pm.foregroundTask('a1'), true);

    const s1 = pm.syncForegroundedMessages()!;
    assert.equal(s1.cleared, false);
    assert.equal(s1.isLoading, true);
    assert.equal(s1.changed, true); // 0 -> 2
    assert.equal(s1.messages.length, 2);
    assert.equal(s1.abortSignal, ac.signal);
    assert.equal(pm.foregroundedAbortSignal(), ac.signal);

    // No growth → not changed, but still loading.
    const s2 = pm.syncForegroundedMessages()!;
    assert.equal(s2.changed, false);
    assert.equal(s2.isLoading, true);

    // Growth → changed again.
    pm.appendTaskMessage('a1', msg('assistant', 'more'));
    const s3 = pm.syncForegroundedMessages()!;
    assert.equal(s3.changed, true);
    assert.equal(s3.messages.length, 3);
    pm.cleanup();
  });

  it('clears the foreground slot + re-backgrounds when the agent is aborted (Escape)', () => {
    const pm = makePm();
    const ac = new AbortController();
    registerAgent(pm, 'a1', { abortSignal: ac.signal });
    pm.foregroundTask('a1');
    ac.abort();
    const s = pm.syncForegroundedMessages()!;
    assert.equal(s.cleared, true);
    assert.equal(s.isLoading, false);
    assert.equal(pm.getForegroundedTaskId(), undefined);
    assert.equal(pm.get('a1')?.isBackgrounded, true);
    pm.cleanup();
  });

  it('clears + restores to background when the agent reaches a terminal state', () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    pm.foregroundTask('a1');
    pm.updateEntry('a1', { status: 'completed' });
    const s = pm.syncForegroundedMessages()!;
    assert.equal(s.cleared, true);
    assert.equal(s.isLoading, false);
    assert.equal(pm.getForegroundedTaskId(), undefined);
  });

  it('clears when the foregrounded task is not an agent (type !== local_agent)', () => {
    const pm = makePm();
    const bashId = pm.spawn('echo hi', { cwd: makeTmpDir() });
    assert.equal(pm.foregroundTask(bashId), true);
    const s = pm.syncForegroundedMessages()!;
    assert.equal(s.cleared, true);
    assert.equal(pm.getForegroundedTaskId(), undefined);
    pm.cleanup();
  });

  it('resets the sync cursor on re-foreground so the transcript re-renders', () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    pm.appendTaskMessage('a1', msg('user', '1'));
    pm.appendTaskMessage('a1', msg('user', '2'));
    pm.foregroundTask('a1');
    assert.equal(pm.syncForegroundedMessages()!.changed, true);  // 0 -> 2
    assert.equal(pm.syncForegroundedMessages()!.changed, false); // no growth
    pm.backgroundTask('a1');
    pm.foregroundTask('a1');
    // Cursor was reset on (re-)foreground → first sync re-renders even with no growth.
    assert.equal(pm.syncForegroundedMessages()!.changed, true);
  });
});

// ---------------------------------------------------------------------------
// Gap 3 — on-kill transcript truncation to the final message
// ---------------------------------------------------------------------------

describe('ProcessManager on-kill transcript release', () => {
  it('collapses a killed agent transcript to just the final message', () => {
    const pm = makePm();
    registerAgent(pm, 'a1');
    pm.appendTaskMessage('a1', msg('user', 'first'));
    pm.appendTaskMessage('a1', msg('assistant', 'middle'));
    pm.appendTaskMessage('a1', msg('assistant', 'last'));
    assert.equal(pm.getTaskMessages('a1').length, 3);
    pm.kill('a1');
    const after = pm.getTaskMessages('a1');
    assert.equal(after.length, 1);
    assert.equal(after[0].content, 'last');
  });

  it('is a no-op for a single-message / empty transcript', () => {
    const pm = makePm();
    registerAgent(pm, 'one');
    pm.appendTaskMessage('one', msg('assistant', 'only'));
    pm.kill('one');
    assert.deepEqual(pm.getTaskMessages('one').map((m) => m.content), ['only']);

    registerAgent(pm, 'none');
    pm.kill('none');
    assert.deepEqual(pm.getTaskMessages('none'), []);
  });

  it('does NOT truncate a completed/failed agent (only a kill releases)', () => {
    const pm = makePm();
    registerAgent(pm, 'done');
    pm.appendTaskMessage('done', msg('user', 'a'));
    pm.appendTaskMessage('done', msg('assistant', 'b'));
    pm.updateEntry('done', { status: 'completed' });
    // A completed agent keeps its full transcript for the drill view.
    assert.equal(pm.getTaskMessages('done').length, 2);
  });
});
