import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import {
  TelemetryBatchSender,
  sendOtlpBatch,
  type TelemetryBatch,
} from '../src/analytics';

// Keep the process from lingering on any stray unref'd timers / open sockets.
after(() => { setTimeout(() => process.exit(0), 200); });

const delay = (ms: number) => new Promise<void>(res => setTimeout(res, ms));

// ---------------------------------------------------------------------------
// startAutoFlush's setInterval callback (`() => this.flush()`): proves the
// auto-flush timer actually drains the queue on its own, with no manual flush.
// ---------------------------------------------------------------------------
describe('TelemetryBatchSender auto-flush timer callback', () => {
  it('fires flush() on the interval, emptying the queue and bumping the batch id', async () => {
    const sender = new TelemetryBatchSender({
      sessionId: 'auto-sess',
      flushIntervalMs: 10,
      // keep maxBatchSize high so enqueue() itself never triggers a flush —
      // only the interval callback can drain the queue.
      maxBatchSize: 1000,
    });

    sender.enqueue('tool_use', { tool: 'bash' });
    sender.enqueue('tool_use', { tool: 'grep' });
    assert.equal(sender.queueSize, 2, 'both events queued before the timer fires');
    assert.equal(sender.batchId, 0, 'no batch produced yet');

    sender.startAutoFlush();
    // startAutoFlush is idempotent: a second call must NOT create a 2nd timer.
    sender.startAutoFlush();

    // Wait long enough for at least one interval tick to run the callback.
    const deadline = Date.now() + 2000;
    while (sender.queueSize > 0 && Date.now() < deadline) {
      await delay(5);
    }
    sender.stopAutoFlush();

    assert.equal(sender.queueSize, 0, 'interval callback drained the queue');
    assert.ok(sender.batchId >= 1, 'interval callback produced at least one batch');
    assert.equal(sender.totalSent, 2, 'both events were flushed by the timer');

    // stopAutoFlush is safe to call again after the timer is cleared.
    sender.stopAutoFlush();
  });
});

// ---------------------------------------------------------------------------
// sendOtlpBatch's setTimeout callback (`() => controller.abort()`): proves the
// abort path triggers when the endpoint hangs past timeoutMs, and the aborted
// fetch is caught and reported as a best-effort failure (false, never throws).
// ---------------------------------------------------------------------------
describe('sendOtlpBatch timeout/abort callback', () => {
  const sockets: import('net').Socket[] = [];
  // Server accepts the connection but never responds, forcing the abort timer.
  const server = http.createServer((_req, _res) => { /* intentionally hang */ });
  server.on('connection', s => { sockets.push(s); });

  after(() => {
    for (const s of sockets) s.destroy();
    server.close();
  });

  it('aborts a hung request after timeoutMs and returns false', async () => {
    await new Promise<void>(res => server.listen(0, '127.0.0.1', res));
    const addr = server.address() as import('net').AddressInfo;
    const endpoint = `http://127.0.0.1:${addr.port}`;

    const batch: TelemetryBatch = {
      events: [{ type: 'tool_use', timestamp: 1_700_000_000_000, data: { tool: 'bash' } }],
      sessionId: 'abort-sess',
      batchId: 1,
    };

    const started = Date.now();
    const ok = await sendOtlpBatch(endpoint, batch, { timeoutMs: 30 });
    const elapsed = Date.now() - started;

    assert.equal(ok, false, 'hung request resolves to a best-effort failure');
    // It must have returned via the abort timer, not hung indefinitely.
    assert.ok(elapsed < 2000, 'aborted promptly rather than waiting for a real response');
  });
});
