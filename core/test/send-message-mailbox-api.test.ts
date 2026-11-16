/**
 * U022 — Predicate-filtered + blocking Mailbox API parity.
 *
 * Mirrors Claude Code's utils/mailbox.ts Mailbox class (poll(fn)/receive(fn) +
 * waiter handoff in send()) and its revision/length introspection getters, which
 * AMI's module-level mailbox store previously lacked (it only had the drain-all
 * pollMailbox + non-destructive peek/ack). Implements selective, predicate-based
 * consumption and awaitable receive() so a caller can correlate a specific
 * request/response message without draining the whole mailbox.
 *
 * Gaps covered:
 *  - Async promise-based receive() with direct waiter handoff in send()
 *  - Predicate-filtered + blocking message consumption (poll(fn)/receive(fn) + waiter queue)
 *  - Selective message-retrieval predicate parameter (fn) with default () => true
 *  - Predicate-based selective extraction of a single message
 *  - `revision` monotonic send-counter + `length` getters for mailbox introspection
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  resetAllMailboxes,
  getMailbox,
  pollMessage,
  receiveMessage,
  mailboxLength,
  mailboxRevision,
} from '../src/tools/send-message';
import { resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';

function ctx(agentId?: string): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, agentId } as ToolContext;
}

beforeEach(() => { resetAllMailboxes(); resetAgentRegistry(); });
afterEach(() => { resetAllMailboxes(); resetAgentRegistry(); });

describe('U022 predicate Mailbox API', () => {
  it('pollMessage() with no predicate removes and returns the first queued message', async () => {
    await sendMessageTool.execute({ to: 'worker', content: 'first' }, ctx());
    await sendMessageTool.execute({ to: 'worker', content: 'second' }, ctx());
    assert.equal(mailboxLength('worker'), 2);
    const m = pollMessage('worker');
    assert.ok(m);
    assert.equal(m!.content, 'first');
    assert.equal(mailboxLength('worker'), 1);
    // remaining message is intact
    assert.equal(getMailbox('worker')[0].content, 'second');
  });

  it('pollMessage(fn) selectively extracts only the matching message, leaving others', async () => {
    await sendMessageTool.execute({ to: 'worker', content: 'alpha' }, ctx());
    await sendMessageTool.execute({ to: 'worker', content: 'beta' }, ctx());
    const m = pollMessage('worker', msg => msg.content === 'beta');
    assert.ok(m);
    assert.equal(m!.content, 'beta');
    assert.equal(mailboxLength('worker'), 1);
    assert.equal(getMailbox('worker')[0].content, 'alpha');
  });

  it('pollMessage returns undefined when nothing matches / mailbox empty', () => {
    assert.equal(pollMessage('ghost'), undefined);
    assert.equal(pollMessage('ghost', () => true), undefined);
  });

  it('receiveMessage resolves immediately from a queued matching message (destructive)', async () => {
    await sendMessageTool.execute({ to: 'worker', content: 'ready' }, ctx());
    const m = await receiveMessage('worker');
    assert.equal(m.content, 'ready');
    assert.equal(mailboxLength('worker'), 0);
  });

  it('receiveMessage blocks until a matching message arrives, handed off directly (bypasses the queue)', async () => {
    const p = receiveMessage('worker', msg => msg.content.startsWith('go'));
    // nothing queued yet
    assert.equal(mailboxLength('worker'), 0);
    // a non-matching send must NOT satisfy the waiter and IS queued
    await sendMessageTool.execute({ to: 'worker', content: 'wait' }, ctx());
    assert.equal(mailboxLength('worker'), 1);
    // the matching send resolves the waiter directly and is NOT enqueued
    await sendMessageTool.execute({ to: 'worker', content: 'go-now' }, ctx());
    const m = await p;
    assert.equal(m.content, 'go-now');
    // only the earlier non-matching message remains queued
    assert.equal(mailboxLength('worker'), 1);
    assert.equal(getMailbox('worker')[0].content, 'wait');
  });

  it('mailboxRevision increments on every delivery (even when handed to a waiter)', async () => {
    assert.equal(mailboxRevision('worker'), 0);
    await sendMessageTool.execute({ to: 'worker', content: 'one' }, ctx());
    assert.equal(mailboxRevision('worker'), 1);
    const p = receiveMessage('worker', m => m.content === 'two');
    await sendMessageTool.execute({ to: 'worker', content: 'two' }, ctx());
    await p;
    // handoff to a waiter still counts as a delivery
    assert.equal(mailboxRevision('worker'), 2);
  });

  it('resetAllMailboxes clears pending waiters and revision counters', async () => {
    await sendMessageTool.execute({ to: 'worker', content: 'x' }, ctx());
    assert.equal(mailboxRevision('worker'), 1);
    resetAllMailboxes();
    assert.equal(mailboxRevision('worker'), 0);
    assert.equal(mailboxLength('worker'), 0);
  });
});
