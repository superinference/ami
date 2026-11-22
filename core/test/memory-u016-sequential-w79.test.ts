/**
 * U016 (w79) — generic async serialization wrapper `sequential()` + its use to
 * serialize MemoryManager index mutations.
 *
 * Gap: core had no reusable "serialize an arbitrary async fn and get its result"
 * primitive (parity: the reference utils/sequential.ts). Concurrent memory-index
 * read-then-write cycles could interleave and lose entries.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as fsp from 'fs/promises';
import * as os from 'os';
import * as path from 'path';
import { sequential } from '../src/utils/sequential';
import { MemoryManager } from '../src/memory';

describe('sequential() async serializer', () => {
  it('runs concurrent calls strictly one-at-a-time in FIFO order', async () => {
    const events: string[] = [];
    let active = 0;
    let maxActive = 0;
    const run = sequential(async (id: number) => {
      active++;
      maxActive = Math.max(maxActive, active);
      events.push(`start:${id}`);
      await new Promise(r => setTimeout(r, id === 0 ? 20 : 1));
      events.push(`end:${id}`);
      active--;
      return id * 10;
    });

    const results = await Promise.all([run(0), run(1), run(2)]);

    // Each caller gets its OWN return value back.
    assert.deepEqual(results, [0, 10, 20]);
    // Never more than one in flight at a time.
    assert.equal(maxActive, 1);
    // FIFO: each call fully completes before the next starts.
    assert.deepEqual(events, [
      'start:0', 'end:0', 'start:1', 'end:1', 'start:2', 'end:2',
    ]);
  });

  it('isolates a rejection to its own caller and keeps draining', async () => {
    const run = sequential(async (x: number) => {
      if (x === 1) throw new Error('boom');
      return x;
    });
    const settled = await Promise.allSettled([run(0), run(1), run(2)]);
    assert.equal(settled[0].status, 'fulfilled');
    assert.equal((settled[0] as PromiseFulfilledResult<number>).value, 0);
    assert.equal(settled[1].status, 'rejected');
    assert.match(String((settled[1] as PromiseRejectedResult).reason), /boom/);
    // The post-rejection item still ran and resolved.
    assert.equal(settled[2].status, 'fulfilled');
    assert.equal((settled[2] as PromiseFulfilledResult<number>).value, 2);
  });

  it('preserves the caller `this` context', async () => {
    const obj = {
      tag: 'ctx',
      run: sequential(async function (this: { tag: string }) {
        return this?.tag;
      }),
    };
    assert.equal(await obj.run(), 'ctx');
  });

  it('picks up items enqueued after the queue drained (post-drain re-check)', async () => {
    const order: number[] = [];
    const run = sequential(async (id: number) => {
      order.push(id);
      await Promise.resolve();
      return id;
    });
    await run(1);
    // Enqueue AFTER the first drain completed — a fresh drain must start.
    const p = run(2);
    assert.equal(await p, 2);
    assert.deepEqual(order, [1, 2]);
  });

  it('drains items enqueued during a sync thenable resolve in the same loop', async () => {
    // A thenable that fulfills synchronously and enqueues another call while
    // processing=true must still be drained by the while loop (the reentrancy
    // guard no-ops the nested processQueue, then the while condition picks it up).
    const order: number[] = [];
    const run = sequential(async (id: number) => {
      order.push(id);
      if (id === 1) {
        return {
          then(onFulfilled: (v: number) => void) {
            onFulfilled(1);
            void run(2);
          },
        } as PromiseLike<number>;
      }
      return id;
    });
    assert.equal(await run(1), 1);
    await new Promise((r) => setImmediate(r));
    assert.deepEqual(order, [1, 2]);
  });
});

describe('MemoryManager index mutations are serialized', () => {
  it('does not lose entries under concurrent addToMemoryIndex (end-to-end on disk)', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-seq-'));
    const prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = dir;
    try {
      const mm = new MemoryManager(dir);
      const N = 25;
      // Fire all appends concurrently. Without serialization the read-then-write
      // cycles interleave and most entries are clobbered.
      await Promise.all(
        Array.from({ length: N }, (_, i) =>
          mm.addToMemoryIndex(dir, `- [topic${i}](file${i}.md) — entry ${i}`),
        ),
      );
      const lines = await mm.readMemoryIndex(dir);
      // Every distinct entry survived.
      for (let i = 0; i < N; i++) {
        assert.ok(
          lines.some(l => l.includes(`file${i}.md`)),
          `entry ${i} missing — index mutations were not serialized`,
        );
      }
    } finally {
      if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
      else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
      await fsp.rm(dir, { recursive: true, force: true });
    }
  });

  it('serializes a concurrent add + remove without a lost update', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-seq2-'));
    const prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = dir;
    try {
      const mm = new MemoryManager(dir);
      await mm.addToMemoryIndex(dir, '- [keep](keep.md) — keep me');
      await Promise.all([
        mm.addToMemoryIndex(dir, '- [add](add.md) — new entry'),
        mm.removeFromMemoryIndex(dir, 'nonexistent-substring'),
      ]);
      const lines = await mm.readMemoryIndex(dir);
      assert.ok(lines.some(l => l.includes('keep.md')));
      assert.ok(lines.some(l => l.includes('add.md')));
    } finally {
      if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
      else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
      await fsp.rm(dir, { recursive: true, force: true });
    }
  });
});
