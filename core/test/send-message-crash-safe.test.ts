/**
 * Crash-safe at-least-once message delivery: read (peek) is separated from
 * mark-as-read (ack). An unacked message requeues; acking removes BY ID so a
 * message arriving between a peek and the ack is never dropped; a broadcast
 * acked in one mailbox is untouched in the others. Mirrors the reference's
 * readUnreadMessages / markMessagesAsRead split (useInboxPoller + teammateMailbox).
 */
import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  sendMessageTool,
  resetAllMailboxes,
  getMailbox,
  peekMailbox,
  markMailboxDelivered,
} from '../src/tools/send-message';
import type { ToolContext } from '../src/types';

const ctx = (agentId?: string): ToolContext =>
  ({ cwd: '/tmp', abortSignal: new AbortController().signal, agentId } as unknown as ToolContext);

describe('send_message crash-safe at-least-once delivery', () => {
  beforeEach(() => resetAllMailboxes());

  it('every queued message carries a stable id', async () => {
    await sendMessageTool.execute!({ to: 'worker', content: 'hi' }, ctx());
    const box = getMailbox('worker');
    assert.equal(box.length, 1);
    assert.equal(typeof box[0].id, 'string');
    assert.ok(box[0].id!.length > 0);
  });

  it('peekMailbox reads without clearing (re-peek returns the same messages)', async () => {
    await sendMessageTool.execute!({ to: 'worker', content: 'one' }, ctx());
    await sendMessageTool.execute!({ to: 'worker', content: 'two' }, ctx());
    const first = peekMailbox('worker');
    assert.deepEqual(first.map(m => m.content), ['one', 'two']);
    // Re-peek WITHOUT acking returns the same messages — at-least-once, never lost.
    const second = peekMailbox('worker');
    assert.deepEqual(second.map(m => m.content), ['one', 'two']);
    assert.equal(getMailbox('worker').length, 2, 'peek never removes');
  });

  it('peek returns a snapshot copy — mutating it cannot corrupt the live mailbox', async () => {
    await sendMessageTool.execute!({ to: 'worker', content: 'a' }, ctx());
    const snap = peekMailbox('worker');
    snap.length = 0;
    snap.push({ id: 'x', from: 'evil', to: 'worker', content: 'tampered', timestamp: 0 } as never);
    assert.equal(getMailbox('worker').length, 1);
    assert.equal(getMailbox('worker')[0].content, 'a');
  });

  it('markMailboxDelivered removes only the acked ids; the rest requeue', async () => {
    await sendMessageTool.execute!({ to: 'worker', content: 'keep-me' }, ctx());
    await sendMessageTool.execute!({ to: 'worker', content: 'ack-me' }, ctx());
    const box = peekMailbox('worker');
    const ackId = box.find(m => m.content === 'ack-me')!.id!;
    const removed = markMailboxDelivered('worker', [ackId]);
    assert.equal(removed, 1);
    const left = peekMailbox('worker');
    assert.deepEqual(left.map(m => m.content), ['keep-me'], 'unacked message requeues');
  });

  it('simulated crash: peek then NO ack → next peek redelivers (at-least-once)', async () => {
    await sendMessageTool.execute!({ to: 'worker', content: 'critical' }, ctx());
    const consumed = peekMailbox('worker');
    assert.equal(consumed.length, 1);
    // Consumer "crashes" before calling markMailboxDelivered — nothing acked.
    const afterCrash = peekMailbox('worker');
    assert.equal(afterCrash.length, 1, 'message survives a crash before ack');
    assert.equal(afterCrash[0].content, 'critical');
  });

  it('ack by id is immune to a message arriving between peek and ack', async () => {
    await sendMessageTool.execute!({ to: 'worker', content: 'early' }, ctx());
    const peeked = peekMailbox('worker');
    const peekedIds = peeked.map(m => m.id!);
    // A new message lands AFTER the peek (e.g. via the eager notifier).
    await sendMessageTool.execute!({ to: 'worker', content: 'late' }, ctx());
    // Ack only what was peeked — the late arrival must NOT be dropped.
    markMailboxDelivered('worker', peekedIds);
    const left = peekMailbox('worker');
    assert.deepEqual(left.map(m => m.content), ['late'], 'late arrival is not swallowed by the ack');
  });

  it('dedup: an acked message is never redelivered', async () => {
    await sendMessageTool.execute!({ to: 'worker', content: 'once' }, ctx());
    const ids = peekMailbox('worker').map(m => m.id!);
    markMailboxDelivered('worker', ids);
    assert.equal(peekMailbox('worker').length, 0, 'acked message is gone');
    // Acking the same ids again is a harmless no-op.
    assert.equal(markMailboxDelivered('worker', ids), 0);
  });

  it('broadcast acked in one mailbox leaves the other recipients intact', async () => {
    // Seed two live mailboxes so the broadcast fans out to both.
    await sendMessageTool.execute!({ to: 'alpha', content: 'seed' }, ctx());
    await sendMessageTool.execute!({ to: 'beta', content: 'seed' }, ctx());
    await sendMessageTool.execute!({ to: '*', content: 'all-hands' }, ctx());
    const alphaBroadcast = peekMailbox('alpha').find(m => m.content === 'all-hands')!;
    markMailboxDelivered('alpha', [alphaBroadcast.id!]);
    // beta still has the broadcast — the shared object was not mutated across boxes.
    assert.ok(
      peekMailbox('beta').some(m => m.content === 'all-hands'),
      'acking alpha must not remove the broadcast from beta',
    );
  });

  it('markMailboxDelivered on an empty/unknown mailbox or empty id list returns 0', async () => {
    assert.equal(markMailboxDelivered('nobody', ['x']), 0);
    await sendMessageTool.execute!({ to: 'worker', content: 'x' }, ctx());
    assert.equal(markMailboxDelivered('worker', []), 0, 'empty ack list is a no-op');
    assert.equal(peekMailbox('worker').length, 1);
  });
});
