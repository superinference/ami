/**
 * U055 — streaming line output with cross-chunk remainder carry (parity with
 * Claude Code's `ripGrepStream`, references/claude/claude-code-main/src/utils/ripgrep.ts:L295-L343).
 *
 * AMI's `runSearch` previously only had the fully-buffered path (wait for 'close',
 * parse all stdout at once). These tests cover the added incremental path:
 *   - `splitStreamLines` splits each chunk into COMPLETE lines, carries the partial
 *     trailing line forward as a remainder, and strips a trailing CR per line.
 *   - `runSearch`, when given a `context.onProgress` sink, forwards complete result
 *     lines as each stdout chunk arrives and flushes the final partial line only on
 *     a CLEAN close — never a torn tail from a timed-out / aborted process.
 *
 * RED on revert: remove the `splitStreamLines`/`streamProgress` wiring in grep.ts and
 * these streaming assertions fail (no lines arrive before 'close'; remainder leaks on
 * abort/timeout).
 */
import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { runSearch, splitStreamLines, stripTrailingCR, grepHooks, grepTool } from '../src/tools/grep';
import type { ToolContext } from '../src/types';
import * as os from 'os';

function makeChild() {
  const child: any = new EventEmitter();
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.killCalls = [];
  child.kill = (signal?: string) => { child.killCalls.push(signal); return true; };
  return child;
}

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, ...overrides };
}

const savedKillGraceMs = grepHooks.killGraceMs;
const savedPlatform = grepHooks.platform;
afterEach(() => {
  mock.restoreAll();
  grepHooks.killGraceMs = savedKillGraceMs;
  grepHooks.platform = savedPlatform;
});

describe('splitStreamLines — per-chunk split with cross-chunk remainder carry', () => {
  it('returns complete lines and holds the partial trailing line as remainder', () => {
    const r = splitStreamLines('', 'a\nb\nc');
    assert.deepEqual(r.lines, ['a', 'b']);
    assert.equal(r.remainder, 'c');
  });

  it('carries a partial line across the chunk boundary', () => {
    const r1 = splitStreamLines('', 'par');
    assert.deepEqual(r1.lines, []);
    assert.equal(r1.remainder, 'par');
    const r2 = splitStreamLines(r1.remainder, 'tial\ndone');
    assert.deepEqual(r2.lines, ['partial']);
    assert.equal(r2.remainder, 'done');
  });

  it('strips a trailing CR from each complete line (CRLF output)', () => {
    const r = splitStreamLines('', 'a\r\nb\r\n');
    assert.deepEqual(r.lines, ['a', 'b']);
    assert.equal(r.remainder, '');
  });

  it('treats a chunk with no newline as pure remainder', () => {
    const r = splitStreamLines('', 'abc');
    assert.deepEqual(r.lines, []);
    assert.equal(r.remainder, 'abc');
  });

  it('an empty chunk preserves the existing remainder', () => {
    const r = splitStreamLines('x', '');
    assert.deepEqual(r.lines, []);
    assert.equal(r.remainder, 'x');
  });
});

describe('stripTrailingCR', () => {
  it('removes exactly one trailing CR', () => {
    assert.equal(stripTrailingCR('a\r'), 'a');
    assert.equal(stripTrailingCR('a'), 'a');
    assert.equal(stripTrailingCR(''), '');
    assert.equal(stripTrailingCR('a\r\r'), 'a\r');
  });
});

describe('runSearch — streams complete lines through onProgress as chunks arrive', () => {
  it('emits finished lines incrementally and flushes the final remainder on a clean close', async () => {
    const child = makeChild();
    mock.method(grepHooks, 'spawn', () => child as any);
    const lines: string[] = [];
    const p = runSearch(['pat', '/tmp'], 'rg', ctx({ onProgress: (d) => lines.push(d) }), 0);

    // Two writes: the first lands mid-line (no trailing newline on 'b:2:').
    child.stdout.emit('data', Buffer.from('a:1:x\r\nb:2:'));
    // Only the first complete line is painted; the partial 'b:2:' is held.
    assert.deepEqual(lines, ['a:1:x']);
    child.stdout.emit('data', Buffer.from('y\nc:3:z'));
    // 'b:2:y' completed across the boundary; 'c:3:z' still partial.
    assert.deepEqual(lines, ['a:1:x', 'b:2:y']);

    child.emit('close', 0, null);
    const outcome = await p;
    // Final remainder flushed exactly once on the clean close.
    assert.deepEqual(lines, ['a:1:x', 'b:2:y', 'c:3:z']);
    // The authoritative buffered output is unchanged (full raw stdout).
    assert.equal(outcome.output, 'a:1:x\r\nb:2:y\nc:3:z');
  });

  it('does NOT flush a torn tail when the search is aborted', async () => {
    const child = makeChild();
    mock.method(grepHooks, 'spawn', () => child as any);
    grepHooks.killGraceMs = 10;
    grepHooks.platform = 'linux';
    const lines: string[] = [];
    const ac = new AbortController();
    const p = runSearch(['pat', '/tmp'], 'rg', ctx({ onProgress: (d) => lines.push(d), abortSignal: ac.signal }), 0);

    child.stdout.emit('data', Buffer.from('hit:1:found\npartial-tail'));
    assert.deepEqual(lines, ['hit:1:found']);
    setImmediate(() => ac.abort());
    await p;
    // The partial trailing line from the killed process is never painted.
    assert.deepEqual(lines, ['hit:1:found']);
  });

  it('does NOT flush a torn tail on timeout', async () => {
    const child = makeChild();
    mock.method(grepHooks, 'spawn', () => child as any);
    grepHooks.killGraceMs = 10;
    grepHooks.platform = 'linux';
    const lines: string[] = [];
    const p = runSearch(['pat', '/tmp'], 'rg', ctx({ onProgress: (d) => lines.push(d) }), 10);

    child.stdout.emit('data', Buffer.from('hit:1:found\ntorn'));
    const outcome = await p;
    assert.equal(outcome.timedOut, true);
    assert.deepEqual(lines, ['hit:1:found']);
  });

  it('buffers normally and never throws when no onProgress sink is provided', async () => {
    const child = makeChild();
    mock.method(grepHooks, 'spawn', () => child as any);
    const p = runSearch(['pat', '/tmp'], 'rg', ctx(), 0);
    child.stdout.emit('data', Buffer.from('a:1:x\nb:2:y'));
    child.emit('close', 0, null);
    const outcome = await p;
    assert.equal(outcome.output, 'a:1:x\nb:2:y');
  });
});

describe('grepTool.execute — forwards context.onProgress for early-result streaming (e2e)', () => {
  it('paints each match line through the caller onProgress sink while rg is still running', async () => {
    // Mock the searcher so, after runSearch attaches listeners, it writes two
    // chunks (the first ending mid-line) then closes cleanly.
    mock.method(grepHooks, 'spawn', () => {
      const child: any = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = () => true;
      setImmediate(() => {
        child.stdout.emit('data', Buffer.from('app.ts:1:alpha\napp.ts:2:be'));
        setImmediate(() => {
          child.stdout.emit('data', Buffer.from('ta\napp.ts:3:gamma'));
          setImmediate(() => child.emit('close', 0, null));
        });
      });
      return child;
    });

    const painted: string[] = [];
    const context: ToolContext = {
      cwd: os.tmpdir(),
      abortSignal: new AbortController().signal,
      onProgress: (d) => painted.push(d),
    };
    const result = await grepTool.execute(
      { pattern: 'alpha', output_mode: 'content' },
      context,
    );

    assert.ok(!result.isError, 'grep should succeed');
    // The grep TOOL forwarded onProgress into runSearch, so every complete line was
    // painted incrementally: the first two as complete lines (the partial
    // 'app.ts:2:be' + 'ta' joined across the chunk boundary), the last via the
    // clean-close remainder flush — proving the whole wired path end to end.
    assert.deepEqual(painted, ['app.ts:1:alpha', 'app.ts:2:beta', 'app.ts:3:gamma']);
    // The authoritative buffered result still carries the full output.
    assert.match(result.output, /gamma/);
  });
});
