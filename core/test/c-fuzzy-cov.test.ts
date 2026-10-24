import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'events';
import { grepTool, grepHooks } from '../src/tools/grep';
import { scoutTool, scoutHooks } from '../src/tools/scout';
import type { ToolContext } from '../src/types';

// ===========================================================================
// Shared helpers
// ===========================================================================

function tmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function ctx(cwd: string): ToolContext {
  return { cwd, abortSignal: new AbortController().signal };
}

interface FakeChild extends EventEmitter {
  stdout: EventEmitter;
  stderr: EventEmitter;
  kill: (signal?: string) => boolean;
}

function makeFakeChild(): FakeChild {
  const child = new EventEmitter() as FakeChild;
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.kill = () => true;
  return child;
}

/** Install a fake spawn whose per-command behavior is supplied by `impl`. */
function mockSpawn(impl: (cmd: string, child: FakeChild) => void): void {
  mock.method(grepHooks, 'spawn', ((cmd: string) => {
    const child = makeFakeChild();
    impl(cmd, child);
    return child as unknown as ReturnType<typeof grepHooks.spawn>;
  }) as typeof grepHooks.spawn);
}

afterEach(() => {
  mock.restoreAll();
});

// ===========================================================================
// grep.ts — in-process fallback catch arms (execute)
// ===========================================================================

describe('grepTool – in-process fallback catch arms', () => {
  it('maps a non-Error thrown by the in-process fallback via String(err)', async () => {
    // rg and grep both unavailable (spawn-level ENOENT) so no searchError is
    // recorded, forcing the in-process fallback. It throws a NON-Error value,
    // exercising the `: String(err)` arm of the message computation.
    const dir = tmp('c-grep-strerr-');
    try {
      fs.writeFileSync(path.join(dir, 'f.txt'), 'hello\n');
      mockSpawn((_cmd, child) => {
        setImmediate(() => child.emit('error', { code: 'ENOENT' }));
      });
      mock.method(grepHooks, 'searchFilesForPattern', () => {
        // eslint-disable-next-line no-throw-literal
        throw 'raw-string-failure';
      });
      const result = await grepTool.execute({ pattern: 'hello', path: dir }, ctx(dir));
      assert.equal(result.isError, true);
      assert.match(result.output, /raw-string-failure/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('surfaces aggregated searcher errors when the fallback also fails', async () => {
    // Both rg and grep exit with code > 1 and no stdout → each contributes a
    // searchError. The in-process fallback then throws a plain (non-regex)
    // Error, so the `searchErrors.length > 0` aggregation branch runs.
    const dir = tmp('c-grep-agg-');
    try {
      fs.writeFileSync(path.join(dir, 'f.txt'), 'hello\n');
      mockSpawn((_cmd, child) => {
        setImmediate(() => {
          child.stderr.emit('data', Buffer.from('permission denied'));
          child.emit('close', 2, null);
        });
      });
      mock.method(grepHooks, 'searchFilesForPattern', () => {
        throw new Error('disk exploded');
      });
      const result = await grepTool.execute({ pattern: 'hello', path: dir }, ctx(dir));
      assert.equal(result.isError, true);
      assert.match(result.output, /Search failed/);
      assert.match(result.output, /rg: permission denied/);
      assert.match(result.output, /grep: permission denied/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('reports a generic failure when the fallback throws and no searcher errored', async () => {
    const dir = tmp('c-grep-generic-');
    try {
      fs.writeFileSync(path.join(dir, 'f.txt'), 'hello\n');
      mockSpawn((_cmd, child) => {
        setImmediate(() => child.emit('error', { code: 'ENOENT' }));
      });
      mock.method(grepHooks, 'searchFilesForPattern', () => {
        throw new Error('unexpected failure mode');
      });
      const result = await grepTool.execute({ pattern: 'hello', path: dir }, ctx(dir));
      assert.equal(result.isError, true);
      assert.match(result.output, /Neither rg nor grep is available/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('returns the unreadable-target error for a missing search path inside cwd', async () => {
    const dir = tmp('c-grep-unread-');
    try {
      const result = await grepTool.execute(
        { pattern: 'hello', path: path.join(dir, 'does-not-exist') },
        ctx(dir),
      );
      assert.equal(result.isError, true);
      assert.match(result.output, /cannot read/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// scout.ts — non-Error catch arm
// ===========================================================================

describe('scoutTool – non-Error failure', () => {
  it('stringifies a non-Error thrown during scouting via String(err)', async () => {
    const dir = tmp('c-scout-strerr-');
    try {
      mock.method(scoutHooks, 'existsSync', () => true); // skip clone
      mock.method(scoutHooks, 'execSync', () => {
        // eslint-disable-next-line no-throw-literal
        throw 'scout-raw-failure';
      });
      const result = await scoutTool.execute({ repo: 'owner/repo' }, ctx(dir));
      assert.equal(result.isError, true);
      assert.match(result.output, /Error scouting owner\/repo: scout-raw-failure/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
