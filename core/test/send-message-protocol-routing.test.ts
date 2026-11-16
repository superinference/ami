import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  isStructuredProtocolMessage,
  getShutdownMessageSummary,
  summarizeStructuredMessage,
  generateRequestId,
} from '../src/tools/send-message';

// ---------------------------------------------------------------------------
// U022: control-plane boundary guard — classify + concise-summarize structured
// protocol messages so they are ROUTED, never dumped as raw JSON into context.
// ---------------------------------------------------------------------------

describe('isStructuredProtocolMessage', () => {
  it('is true for every known protocol discriminator', () => {
    const rid = generateRequestId('shutdown', 'w1');
    const cases: string[] = [
      JSON.stringify({ type: 'shutdown_request', reason: 'done' }),
      JSON.stringify({ type: 'shutdown_response', requestId: rid, approve: true }),
      JSON.stringify({ type: 'plan_approval_request', planContent: 'plan' }),
      JSON.stringify({ type: 'plan_approval_response', requestId: rid, approve: false, feedback: 'no' }),
      JSON.stringify({ type: 'permission_request', tool: 'bash', description: 'rm' }),
      JSON.stringify({ type: 'permission_response', requestId: rid, approve: true }),
    ];
    for (const c of cases) assert.equal(isStructuredProtocolMessage(c), true, c);
  });

  it('is true even when a known discriminator carries a malformed payload', () => {
    // shutdown_response with no requestId → parseStructuredMessage returns { error }
    // but it is still control-plane, not conversation, so it must be gated.
    assert.equal(isStructuredProtocolMessage(JSON.stringify({ type: 'shutdown_response', approve: true })), true);
  });

  it('is false for plain text, non-JSON, and unknown JSON objects', () => {
    assert.equal(isStructuredProtocolMessage('hello teammate'), false);
    assert.equal(isStructuredProtocolMessage('not json {'), false);
    assert.equal(isStructuredProtocolMessage(JSON.stringify({ type: 'chit_chat', x: 1 })), false);
    assert.equal(isStructuredProtocolMessage(JSON.stringify({ foo: 'bar' })), false);
  });
});

describe('getShutdownMessageSummary', () => {
  it('formats a request with the sender and optional reason', () => {
    assert.equal(
      getShutdownMessageSummary(JSON.stringify({ type: 'shutdown_request', reason: 'wrap up' }), 'worker'),
      '[Shutdown Request from worker] wrap up',
    );
    assert.equal(
      getShutdownMessageSummary(JSON.stringify({ type: 'shutdown_request' }), 'worker'),
      '[Shutdown Request from worker]',
    );
  });

  it('formats an approval distinctly', () => {
    const rid = generateRequestId('shutdown', 'worker');
    assert.equal(
      getShutdownMessageSummary(JSON.stringify({ type: 'shutdown_response', requestId: rid, approve: true }), 'worker'),
      '[Shutdown Approved] worker is now exiting',
    );
  });

  it('formats a rejection with the reason plus the non-fatal fallback boundary', () => {
    const rid = generateRequestId('shutdown', 'worker');
    const out = getShutdownMessageSummary(
      JSON.stringify({ type: 'shutdown_response', requestId: rid, approve: false, reason: 'mid task' }),
      'worker',
    );
    assert.match(out!, /\[Shutdown Rejected\] worker: mid task/);
    assert.match(out!, /continuing to work; you may request shutdown again later/);
  });

  it('returns null for a non-shutdown message', () => {
    assert.equal(getShutdownMessageSummary(JSON.stringify({ type: 'permission_response', requestId: 'x', approve: true })), null);
    assert.equal(getShutdownMessageSummary('plain text'), null);
  });
});

describe('summarizeStructuredMessage', () => {
  const rid = generateRequestId('x', 'w');
  it('summarizes each protocol type as a one-liner without raw JSON', () => {
    assert.match(summarizeStructuredMessage(JSON.stringify({ type: 'plan_approval_request', planContent: 'p', planFilePath: '/a.md' }))!, /\[Plan Approval Request\] \(\/a\.md\)/);
    // Plan-approval summaries carry Claude-parity next-step guidance (approved →
    // "proceed"; rejected → feedback-or-default + ExitPlanMode affordance).
    assert.match(summarizeStructuredMessage(JSON.stringify({ type: 'plan_approval_response', requestId: rid, approve: true }))!, /^\[Plan Approved\] You can now proceed with implementation/);
    assert.match(summarizeStructuredMessage(JSON.stringify({ type: 'plan_approval_response', requestId: rid, approve: false, feedback: 'redo' }))!, /\[Plan Rejected\] redo — revise and call ExitPlanMode again/);
    assert.match(summarizeStructuredMessage(JSON.stringify({ type: 'permission_request', tool: 'bash', description: 'rm -rf' }))!, /\[Permission Request\] tool "bash": rm -rf/);
    assert.equal(summarizeStructuredMessage(JSON.stringify({ type: 'permission_response', requestId: rid, approve: false })), '[Permission Denied]');
    assert.match(summarizeStructuredMessage(JSON.stringify({ type: 'shutdown_request', reason: 'r' }), 'w')!, /\[Shutdown Request from w\] r/);
  });

  it('returns null for plain text or a malformed protocol payload', () => {
    assert.equal(summarizeStructuredMessage('just chatting'), null);
    assert.equal(summarizeStructuredMessage(JSON.stringify({ type: 'permission_request' /* no tool */ })), null);
  });

  it('never leaks the raw JSON discriminator keys in its output', () => {
    const s = summarizeStructuredMessage(JSON.stringify({ type: 'shutdown_request', reason: 'bye' }), 'w')!;
    assert.ok(!s.includes('"type"'), s);
    assert.ok(!s.includes('shutdown_request'), s);
  });
});
