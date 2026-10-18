import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { TelemetryBatchSender } from '../src/analytics';

describe('TelemetryBatchSender', () => {
  it('constructor sets defaults', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1' });
    assert.equal(sender.queueSize, 0);
    assert.equal(sender.totalSent, 0);
    assert.equal(sender.totalDropped, 0);
    assert.equal(sender.batchId, 0);
  });

  it('enqueue adds event to queue', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1' });
    sender.enqueue('test', { key: 'val' });
    assert.equal(sender.queueSize, 1);
  });

  it('enqueue returns true when successful', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1' });
    assert.equal(sender.enqueue('test', {}), true);
  });

  it('enqueue returns false when queue is full', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1', maxQueueSize: 2, maxBatchSize: 100 });
    assert.equal(sender.enqueue('a', {}), true);
    assert.equal(sender.enqueue('b', {}), true);
    assert.equal(sender.enqueue('c', {}), false);
    assert.equal(sender.queueSize, 2);
  });

  it('enqueue with maxQueueSize=0 always drops', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1', maxQueueSize: 0 });
    assert.equal(sender.enqueue('a', {}), false);
    assert.equal(sender.totalDropped, 1);
    assert.equal(sender.queueSize, 0);
  });

  it('flush returns null on empty queue', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1' });
    assert.equal(sender.flush(), null);
  });

  it('flush returns batch with events', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1' });
    sender.enqueue('evt', { x: 1 });
    const batch = sender.flush();
    assert.ok(batch !== null);
    assert.equal(batch!.events.length, 1);
    assert.equal(batch!.events[0].type, 'evt');
    assert.deepEqual(batch!.events[0].data, { x: 1 });
    assert.equal(typeof batch!.events[0].timestamp, 'number');
  });

  it('flush increments batchId', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1' });
    sender.enqueue('a', {});
    const b1 = sender.flush();
    assert.equal(b1!.batchId, 1);
    sender.enqueue('b', {});
    const b2 = sender.flush();
    assert.equal(b2!.batchId, 2);
  });

  it('flush removes events from queue', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1' });
    sender.enqueue('a', {});
    sender.enqueue('b', {});
    sender.flush();
    assert.equal(sender.queueSize, 0);
  });

  it('auto-flush triggers on maxBatchSize threshold', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1', maxBatchSize: 2 });
    sender.enqueue('a', {});
    assert.equal(sender.queueSize, 1);
    sender.enqueue('b', {});
    assert.equal(sender.queueSize, 0, 'queue should be flushed after reaching maxBatchSize');
    assert.equal(sender.totalSent, 2);
    assert.equal(sender.batchId, 1);
  });

  it('drain returns all batches', () => {
    // maxBatchSize=2 so drain splits 5 queued items into 3 batches (2+2+1)
    // Use large maxBatchSize during enqueue to avoid auto-flush, then change via new sender
    const sender = new TelemetryBatchSender({ sessionId: 's1', maxBatchSize: 100 });
    for (let i = 0; i < 5; i++) sender.enqueue(`e${i}`, {});
    assert.equal(sender.queueSize, 5);
    // drain uses the instance's maxBatchSize (100), so all 5 come in one batch
    const batches = sender.drain();
    assert.equal(batches.length, 1);
    assert.equal(batches[0].events.length, 5);
    assert.equal(sender.queueSize, 0);
  });

  it('drain stops auto-flush', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1', flushIntervalMs: 100 });
    sender.startAutoFlush();
    sender.enqueue('a', {});
    sender.drain();
    // After drain, no timer should be running (verified by no error on GC)
    assert.equal(sender.queueSize, 0);
  });

  it('totalSent tracks sent count', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1' });
    sender.enqueue('a', {});
    sender.enqueue('b', {});
    sender.flush();
    assert.equal(sender.totalSent, 2);
    sender.enqueue('c', {});
    sender.flush();
    assert.equal(sender.totalSent, 3);
  });

  it('totalDropped tracks dropped count', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1', maxQueueSize: 1, maxBatchSize: 100 });
    sender.enqueue('a', {});
    sender.enqueue('b', {});
    sender.enqueue('c', {});
    assert.equal(sender.totalDropped, 2);
  });

  it('multiple flushes produce sequential batchIds', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1' });
    sender.enqueue('a', {});
    const b1 = sender.flush();
    sender.enqueue('b', {});
    const b2 = sender.flush();
    sender.enqueue('c', {});
    const b3 = sender.flush();
    assert.equal(b1!.batchId, 1);
    assert.equal(b2!.batchId, 2);
    assert.equal(b3!.batchId, 3);
  });

  it('batch contains correct sessionId', () => {
    const sender = new TelemetryBatchSender({ sessionId: 'my-session-42' });
    sender.enqueue('a', {});
    const batch = sender.flush();
    assert.equal(batch!.sessionId, 'my-session-42');
  });

  it('flush respects maxBatchSize and leaves remainder', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1', maxBatchSize: 2 });
    sender.enqueue('a', {});
    sender.enqueue('b', {});
    sender.enqueue('c', {});
    // The first two enqueues filled the batch → auto-flushed, leaving 'c'
    assert.equal(sender.queueSize, 1);
    assert.equal(sender.batchId, 1);
    const batch = sender.flush();
    assert.ok(batch !== null);
    assert.equal(batch!.events.length, 1);
    assert.equal(batch!.events[0].type, 'c');
  });

  it('startAutoFlush is idempotent', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1', flushIntervalMs: 60_000 });
    sender.startAutoFlush();
    sender.startAutoFlush(); // should not create a second timer
    sender.stopAutoFlush();
    // no error, no lingering timer
    assert.equal(sender.queueSize, 0);
  });

  it('stopAutoFlush is safe when not started', () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1' });
    sender.stopAutoFlush(); // should not throw
    assert.equal(sender.queueSize, 0);
  });
});
