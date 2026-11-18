import { describe, it, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import {
  getProcessCommand,
  collectChildPids,
  collectAncestorCommands,
  PROCESS_COMMAND_TIMEOUT_MS,
} from '../src/utils/process-utils';
import { shellHooks } from '../src/utils/shell';

// ---------------------------------------------------------------------------
// U266 / w83: three confirmed parity gaps for process introspection, parity
// with Claude Code's genericProcessUtils.ts:
//  - getProcessCommand       (L94-L107)  single-pid command lookup, 1000ms cap
//  - collectChildPids        (L163-L184) direct-child PID enumeration, 1000ms cap
//  - collectAncestorCommands (L115-L156) ancestor command-line chain, maxDepth
// (getAncestorPidsAsync L36-L86 is already covered by shell.ts collectAncestorPids.)
//
// All three route through the shellHooks seam (platform + execSync) so the
// Windows branches are exercised without a real subprocess. Reverting any of the
// three helpers fails the matching cases below.
// ---------------------------------------------------------------------------

afterEach(() => { mock.restoreAll(); });

/**
 * Install an execSync mock that routes by command substring, plus the platform.
 * `router` returns the canned stdout, or throws to simulate a spawn failure.
 */
function mockExec(platform: NodeJS.Platform, router: (cmd: string, opts: { timeout?: number }) => string): void {
  mock.method(shellHooks, 'platform', () => platform);
  mock.method(shellHooks, 'execSync', ((cmd: string, opts: { timeout?: number }) => router(cmd, opts)) as unknown as typeof shellHooks.execSync);
}

// --- getProcessCommand (POSIX) --------------------------------------------

describe('getProcessCommand (POSIX)', () => {
  it('returns the trimmed command line for a pid', () => {
    mockExec('linux', (cmd) => {
      assert.ok(cmd.startsWith('ps -o command= -p 4321'));
      return '  node /app/server.js --port 3000  \n';
    });
    assert.equal(getProcessCommand(4321), 'node /app/server.js --port 3000');
  });

  it('accepts a string pid', () => {
    mockExec('linux', () => 'bash\n');
    assert.equal(getProcessCommand('555'), 'bash');
  });

  it('returns null on empty output', () => {
    mockExec('linux', () => '   \n');
    assert.equal(getProcessCommand(4321), null);
  });

  it('returns null when the command throws (process gone / timeout)', () => {
    mockExec('linux', () => { throw new Error('ps failed'); });
    assert.equal(getProcessCommand(4321), null);
  });

  it('rejects an invalid pid without spawning', () => {
    let called = false;
    mockExec('linux', () => { called = true; return 'x'; });
    assert.equal(getProcessCommand(0), null);
    assert.equal(getProcessCommand(-5), null);
    assert.equal(getProcessCommand(1.5), null);
    assert.equal(getProcessCommand('notapid'), null);
    assert.equal(called, false);
  });

  it('bounds the lookup with the 1000ms timeout', () => {
    let seen = -1;
    mockExec('linux', (_cmd, opts) => { seen = opts.timeout ?? -1; return 'x'; });
    getProcessCommand(4321);
    assert.equal(seen, PROCESS_COMMAND_TIMEOUT_MS);
    assert.equal(PROCESS_COMMAND_TIMEOUT_MS, 1000);
  });

  // Claude Code's getAncestorCommandsAsync reads ALL ancestor commands in a single
  // `ps` invocation and must therefore use a NUL ('\0') record separator so a
  // command line containing an interior newline does not corrupt the split
  // (genericProcessUtils.ts:L115-L156). AMI reads exactly one pid per invocation,
  // so there is no cross-record parsing to corrupt — an embedded newline is part of
  // the one command and must survive intact (only leading/trailing whitespace is
  // trimmed). This locks that newline-safety invariant so the one-pid design is not
  // later "optimised" into a separator-dependent batched read.
  it('preserves interior newlines in a command line (NUL-separator robustness)', () => {
    mockExec('linux', () => '  sh -c "foo\nbar baz"  \n');
    assert.equal(getProcessCommand(4321), 'sh -c "foo\nbar baz"');
  });
});

// --- getProcessCommand (Windows) ------------------------------------------

describe('getProcessCommand (Windows)', () => {
  it('parses CommandLine= from wmic /value output', () => {
    mockExec('win32', (cmd) => {
      assert.ok(cmd.includes('wmic process where processid=4321'));
      assert.ok(cmd.includes('CommandLine /value'));
      return '\r\nCommandLine=C:\\app\\node.exe --port 3000\r\n\r\n';
    });
    assert.equal(getProcessCommand(4321), 'C:\\app\\node.exe --port 3000');
  });

  it('returns null when CommandLine is empty', () => {
    mockExec('win32', () => 'CommandLine=\r\n');
    assert.equal(getProcessCommand(4321), null);
  });

  it('returns null when no CommandLine record is present', () => {
    mockExec('win32', () => 'No Instance(s) Available.\r\n');
    assert.equal(getProcessCommand(4321), null);
  });
});

// --- collectChildPids ------------------------------------------------------

describe('collectChildPids (POSIX)', () => {
  // ps -A -o pid=,ppid=  →  "pid ppid" rows
  const table = '100 90\n101 90\n102 100\n200 1\n';

  it('returns only the DIRECT children of the pid', () => {
    mockExec('linux', () => table);
    assert.deepEqual(collectChildPids(90).sort((a, b) => a - b), [100, 101]);
    assert.deepEqual(collectChildPids(100), [102]);
  });

  it('returns [] when the pid has no children', () => {
    mockExec('linux', () => table);
    assert.deepEqual(collectChildPids(102), []);
  });

  it('returns [] when the process table cannot be read', () => {
    mockExec('linux', () => { throw new Error('ps unavailable'); });
    assert.deepEqual(collectChildPids(90), []);
  });

  it('rejects an invalid pid without spawning', () => {
    let called = false;
    mockExec('linux', () => { called = true; return table; });
    assert.deepEqual(collectChildPids(-1), []);
    assert.deepEqual(collectChildPids(1.2), []);
    assert.deepEqual(collectChildPids('nope'), []);
    assert.equal(called, false);
  });

  it('bounds the table read with the 1000ms timeout', () => {
    let seen = -1;
    mockExec('linux', (_cmd, opts) => { seen = opts.timeout ?? -1; return table; });
    collectChildPids(90);
    assert.equal(seen, PROCESS_COMMAND_TIMEOUT_MS);
  });
});

describe('collectChildPids (Windows)', () => {
  it('enumerates children via the wmic ParentProcessId/ProcessId table', () => {
    // wmic prints columns alphabetically: ParentProcessId then ProcessId.
    mockExec('win32', () => 'ParentProcessId  ProcessId\n90  100\n90  101\n100  102\n');
    assert.deepEqual(collectChildPids(90).sort((a, b) => a - b), [100, 101]);
  });
});

// --- collectAncestorCommands ----------------------------------------------

describe('collectAncestorCommands (POSIX)', () => {
  // ppid table: 100 -> 90 -> 80 -> 1
  const ppidTable = '100 90\n90 80\n80 1\n';
  const commandOf: Record<number, string> = {
    100: 'node worker.js',
    90: 'bash -l',
    80: '/usr/bin/login',
  };

  function router(cmd: string): string {
    if (cmd.includes('-o pid=,ppid=')) return ppidTable;
    const m = cmd.match(/ps -o command= -p (\d+)/);
    if (m) return (commandOf[Number(m[1])] ?? '') + '\n';
    throw new Error(`unexpected command: ${cmd}`);
  }

  it('returns the command chain nearest-first: self then ancestors', () => {
    mockExec('linux', router);
    assert.deepEqual(collectAncestorCommands(100), [
      'node worker.js',
      'bash -l',
      '/usr/bin/login',
    ]);
  });

  it('caps the chain at maxDepth entries (self counts as the first)', () => {
    mockExec('linux', router);
    assert.deepEqual(collectAncestorCommands(100, 2), ['node worker.js', 'bash -l']);
    assert.deepEqual(collectAncestorCommands(100, 1), ['node worker.js']);
  });

  it('skips pids whose command cannot be read (filter Boolean parity)', () => {
    mockExec('linux', (cmd) => {
      if (cmd.includes('-o pid=,ppid=')) return ppidTable;
      const m = cmd.match(/ps -o command= -p (\d+)/);
      // The middle ancestor (90) has no command.
      if (m && Number(m[1]) === 90) return '\n';
      if (m) return (commandOf[Number(m[1])] ?? '') + '\n';
      throw new Error('unexpected');
    });
    assert.deepEqual(collectAncestorCommands(100), ['node worker.js', '/usr/bin/login']);
  });

  it('returns just the start command when there are no ancestors', () => {
    mockExec('linux', (cmd) => {
      if (cmd.includes('-o pid=,ppid=')) return '100 1\n'; // parent is init -> stops
      const m = cmd.match(/ps -o command= -p (\d+)/);
      if (m) return (commandOf[Number(m[1])] ?? '') + '\n';
      throw new Error('unexpected');
    });
    assert.deepEqual(collectAncestorCommands(100), ['node worker.js']);
  });

  it('returns [] for pid <= 1, non-positive maxDepth, or an invalid pid', () => {
    mockExec('linux', router);
    assert.deepEqual(collectAncestorCommands(1), []);
    assert.deepEqual(collectAncestorCommands(0), []);
    assert.deepEqual(collectAncestorCommands(100, 0), []);
    assert.deepEqual(collectAncestorCommands(100, -3), []);
    assert.deepEqual(collectAncestorCommands(2.5), []);
    assert.deepEqual(collectAncestorCommands('bad'), []);
  });

  it('returns [] when even the start pid has no readable command', () => {
    mockExec('linux', (cmd) => {
      if (cmd.includes('-o pid=,ppid=')) return '100 1\n';
      return '\n'; // no command for anyone
    });
    assert.deepEqual(collectAncestorCommands(100), []);
  });
});
