import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { runSearch, grepHooks, MAX_SEARCH_OUTPUT_BYTES } from '../src/tools/grep';
import type { ToolContext } from '../src/types';

function makeChild() {
  const child: any = new EventEmitter();
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.killCalls = [];
  child.kill = (signal?: string) => { child.killCalls.push(signal); return true; };
  return child;
}

function ctx(): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal };
}

afterEach(() => { mock.restoreAll(); });

describe('runSearch – bounded stderr accumulation (U055 memory cap)', () => {
  it('stops accumulating stderr past the byte cap without killing the child or discarding stdout', async () => {
    const child = makeChild();
    mock.method(grepHooks, 'spawn', () => child as any);

    const p = runSearch(['x', '/tmp'], 'rg', ctx());
    // A search that spews far more than the cap to stderr (e.g. permission-denied
    // noise over a huge tree) must not grow memory without bound.
    child.stderr.emit('data', Buffer.alloc(MAX_SEARCH_OUTPUT_BYTES + 5_000, 0x62));
    // The search still completes normally with useful stdout — stderr overflow
    // must NOT abort it (unlike the stdout cap, which kills the child).
    child.stdout.emit('data', Buffer.from('a.ts\nb.ts\n'));
    child.emit('close', 0, null);
    const outcome = await p;

    // stdout preserved, child never killed on stderr overflow.
    assert.equal(outcome.output, 'a.ts\nb.ts\n');
    assert.deepEqual(child.killCalls, []);
    assert.notEqual(outcome.outputTruncated, true);
  });

  it('bounds the stderr buffer that is surfaced in an error outcome to the cap', async () => {
    const child = makeChild();
    mock.method(grepHooks, 'spawn', () => child as any);

    const p = runSearch(['x', '/tmp'], 'rg', ctx());
    // Overflow stderr, then exit with a hard error (code 2) and no stdout so the
    // accumulated stderr becomes the surfaced searchError message.
    child.stderr.emit('data', Buffer.alloc(MAX_SEARCH_OUTPUT_BYTES + 5_000, 0x62));
    child.emit('close', 2, null);
    const outcome = await p;

    assert.equal(outcome.output, null);
    assert.ok(outcome.searchError);
    // The surfaced message is bounded by the cap (plus a short fixed prefix), not
    // the full unbounded overflow — proving stderr accumulation was capped.
    assert.ok(
      (outcome.searchError as string).length <= MAX_SEARCH_OUTPUT_BYTES + 64,
      `searchError length ${(outcome.searchError as string).length} exceeded cap`,
    );
  });
});
