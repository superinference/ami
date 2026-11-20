/**
 * U022-F0767: ordered receive-side relay router (relayIncomingMessage).
 *
 * Mirrors the reference's UserTeammateMessage ordered dispatch (L62-136): lifecycle
 * signals are processed silently (dropped), a completed task renders as a terse
 * `✓ Completed task #id` line, every other structured control message relays as a
 * concise summary, and plain text relays verbatim — a raw control-plane JSON blob
 * is never surfaced. These tests pin each arm of the router.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { relayIncomingMessage } from '../src/tools/send-message';

describe('U022-F0767: relayIncomingMessage receive-side router', () => {
  it('drops a teammate_terminated exit notice (Claude-compatible, silent)', () => {
    const r = relayIncomingMessage(JSON.stringify({ type: 'teammate_terminated', agentId: 'worker' }), 'worker');
    assert.deepEqual(r, { drop: true });
  });

  it('drops an APPROVED shutdown_response (teammate exiting, processed silently)', () => {
    const r = relayIncomingMessage(
      JSON.stringify({ type: 'shutdown_response', requestId: 'shutdown-1@worker', approve: true }),
      'worker',
    );
    assert.equal(r.drop, true);
  });

  it('still relays a REJECTED shutdown_response as a summary (not dropped)', () => {
    const r = relayIncomingMessage(
      JSON.stringify({ type: 'shutdown_response', requestId: 'shutdown-1@worker', approve: false, reason: 'busy' }),
      'worker',
    );
    assert.equal(r.drop, false);
    assert.match(r.text!, /\[Shutdown Rejected\]/);
    assert.ok(!r.text!.includes('"shutdown_response"'), 'must not leak raw JSON');
  });

  it('drops a bare idle_notification (silent "I\'m idle")', () => {
    const r = relayIncomingMessage(JSON.stringify({ type: 'idle_notification', idleReason: 'available' }), 'worker');
    assert.deepEqual(r, { drop: true });
  });

  it('renders a completed-task idle_notification as a terse completion line with subject', () => {
    const r = relayIncomingMessage(
      JSON.stringify({ type: 'idle_notification', completedTaskId: '7', summary: 'shipped it' }),
      'worker',
    );
    assert.equal(r.drop, false);
    assert.equal(r.text, '✓ Completed task #7 (shipped it)');
  });

  it('renders a completed-task idle_notification without a subject (no parens)', () => {
    const r = relayIncomingMessage(JSON.stringify({ type: 'idle_notification', completedTaskId: '7' }), 'worker');
    assert.equal(r.text, '✓ Completed task #7');
  });

  it('renders a Claude-compatible task_completed message as the same terse line', () => {
    const r = relayIncomingMessage(
      JSON.stringify({ type: 'task_completed', from: 'worker', taskId: '9', taskSubject: 'docs' }),
      'worker',
    );
    assert.equal(r.drop, false);
    assert.equal(r.text, '✓ Completed task #9 (docs)');
  });

  it('accepts task_completed with a `subject` field alias and a missing taskId', () => {
    const r = relayIncomingMessage(JSON.stringify({ type: 'task_completed', subject: 'thing' }), 'worker');
    assert.equal(r.text, '✓ Completed task #? (thing)');
  });

  it('relays another structured control message (task_assignment) as its summary', () => {
    const r = relayIncomingMessage(JSON.stringify({ type: 'task_assignment', taskId: '3', subject: 'Ship' }), 'main');
    assert.equal(r.drop, false);
    assert.match(r.text!, /\[Task Assigned\] #3 - Ship/);
    assert.ok(!r.text!.includes('"task_assignment"'), 'must not leak raw JSON');
  });

  it('drops a malformed idle_notification (bad enum) as a silent lifecycle signal', () => {
    // parseStructuredMessage rejects the bad enum; the router still treats the
    // known discriminator as a silent lifecycle signal rather than leaking JSON.
    const r = relayIncomingMessage(JSON.stringify({ type: 'idle_notification', idleReason: 'bogus' }), 'worker');
    assert.deepEqual(r, { drop: true });
  });

  it('relays plain conversational text verbatim', () => {
    const r = relayIncomingMessage('hello from a peer', 'worker');
    assert.deepEqual(r, { drop: false, text: 'hello from a peer' });
  });

  it('relays non-discriminated JSON verbatim (not a control message)', () => {
    const payload = JSON.stringify({ hello: 'world' });
    const r = relayIncomingMessage(payload, 'worker');
    assert.deepEqual(r, { drop: false, text: payload });
  });
});
