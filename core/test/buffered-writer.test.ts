import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { createBufferedWriter } from '../src/buffered-writer';

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

describe('createBufferedWriter — immediate mode', () => {
  it('writes every record straight through to writeFn', () => {
    const out: string[] = [];
    const w = createBufferedWriter({ writeFn: (c) => out.push(c), immediateMode: true });
    w.write('a');
    w.write('b');
    assert.deepEqual(out, ['a', 'b']);
  });

  it('flush/dispose are no-ops with nothing buffered', () => {
    const out: string[] = [];
    const w = createBufferedWriter({ writeFn: (c) => out.push(c), immediateMode: true });
    w.flush();
    w.dispose();
    assert.deepEqual(out, []);
  });
});

describe('createBufferedWriter — buffered mode', () => {
  it('coalesces buffered records into a single writeFn call on flush', () => {
    const out: string[] = [];
    const w = createBufferedWriter({ writeFn: (c) => out.push(c), flushIntervalMs: 10_000 });
    w.write('a');
    w.write('b');
    w.write('c');
    assert.deepEqual(out, []); // nothing written yet
    w.flush();
    assert.deepEqual(out, ['abc']);
  });

  it('flush with an empty buffer and no pending overflow writes nothing', () => {
    const out: string[] = [];
    const w = createBufferedWriter({ writeFn: (c) => out.push(c), flushIntervalMs: 10_000 });
    w.flush();
    assert.deepEqual(out, []);
  });

  it('dispose flushes the pending buffer', () => {
    const out: string[] = [];
    const w = createBufferedWriter({ writeFn: (c) => out.push(c), flushIntervalMs: 10_000 });
    w.write('x');
    w.dispose();
    assert.deepEqual(out, ['x']);
  });

  it('auto-flushes on the idle timer', async () => {
    const out: string[] = [];
    const w = createBufferedWriter({ writeFn: (c) => out.push(c), flushIntervalMs: 5 });
    w.write('a');
    w.write('b'); // second write hits the "timer already scheduled" branch
    await new Promise((r) => setTimeout(r, 30));
    assert.deepEqual(out, ['ab']);
    // After the timer flush the buffer is clean; a later write re-arms a new timer.
    w.write('c');
    await new Promise((r) => setTimeout(r, 30));
    assert.deepEqual(out, ['ab', 'c']);
  });
});

describe('createBufferedWriter — deferred overflow', () => {
  it('detaches on maxBufferSize overflow and writes on the next setImmediate', async () => {
    const out: string[] = [];
    const w = createBufferedWriter({ writeFn: (c) => out.push(c), maxBufferSize: 2, flushIntervalMs: 10_000 });
    w.write('a');
    w.write('b'); // reaches maxBufferSize -> deferred flush
    assert.deepEqual(out, []); // write is deferred, not synchronous
    await tick();
    assert.deepEqual(out, ['ab']);
  });

  it('detaches on maxBufferBytes overflow', async () => {
    const out: string[] = [];
    const w = createBufferedWriter({ writeFn: (c) => out.push(c), maxBufferBytes: 3, maxBufferSize: 1000, flushIntervalMs: 10_000 });
    w.write('abcd'); // 4 bytes >= 3 -> deferred flush
    await tick();
    assert.deepEqual(out, ['abcd']);
  });

  it('coalesces a second overflow into the still-pending batch, preserving order', async () => {
    const out: string[] = [];
    const w = createBufferedWriter({ writeFn: (c) => out.push(c), maxBufferSize: 2, flushIntervalMs: 10_000 });
    w.write('a');
    w.write('b'); // overflow #1 -> pendingOverflow=[a,b], setImmediate scheduled
    w.write('c');
    w.write('d'); // overflow #2 (synchronous, before setImmediate) -> coalesce into pending
    await tick();
    assert.deepEqual(out, ['abcd']); // single ordered write
  });

  it('flush() synchronously drains a pending overflow; the deferred write then no-ops', async () => {
    const out: string[] = [];
    const w = createBufferedWriter({ writeFn: (c) => out.push(c), maxBufferSize: 2, flushIntervalMs: 10_000 });
    w.write('a');
    w.write('b'); // overflow -> pendingOverflow set, setImmediate scheduled
    w.flush(); // drains pendingOverflow synchronously, nulls it
    assert.deepEqual(out, ['ab']);
    await tick(); // setImmediate fires but toWrite is now null -> no second write
    assert.deepEqual(out, ['ab']);
  });

  it('flush() drains pending overflow AND the live buffer in order', async () => {
    const out: string[] = [];
    const w = createBufferedWriter({ writeFn: (c) => out.push(c), maxBufferSize: 2, flushIntervalMs: 10_000 });
    w.write('a');
    w.write('b'); // overflow -> pending=[a,b]
    w.write('c'); // back into the live buffer
    w.flush(); // writes pending 'ab' then buffer 'c'
    assert.deepEqual(out, ['ab', 'c']);
    await tick();
    assert.deepEqual(out, ['ab', 'c']);
  });
});
