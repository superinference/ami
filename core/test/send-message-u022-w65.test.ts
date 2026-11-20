import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { sendMessageTool } from '../src/tools/send-message';
import { isToolReadOnlyForInput } from '../src/tools/tool-categories';

// ---------------------------------------------------------------------------
// U022 (w65) — per-invocation read-only verdict for send_message.
//
// Parity: the reference's SendMessageTool declares `isReadOnly(input) =>
// typeof input.message === 'string'` — a plain-text chat message mutates
// nothing (auto-allow, no permission prompt), while a STRUCTURED control
// message (shutdown / plan / permission / mode / task) changes a teammate's
// lifecycle/posture and must stay on the permission-asking path. AMI encodes
// content as a string (structured messages are JSON), so the verdict keys on
// the single canonical structured-message detector.
// ---------------------------------------------------------------------------

describe('sendMessageTool – isReadOnlyForInput (per-call read-only verdict)', () => {
  it('declares the predicate (not just the static flag)', () => {
    assert.equal(typeof sendMessageTool.isReadOnlyForInput, 'function');
  });

  it('plain-text content is read-only', () => {
    assert.equal(sendMessageTool.isReadOnlyForInput!({ to: 'worker', content: 'hello there' }), true);
  });

  it('plain-text via the "message" alias is read-only', () => {
    assert.equal(sendMessageTool.isReadOnlyForInput!({ to: 'worker', message: 'ping' }), true);
  });

  it('a structured shutdown_request is NOT read-only (mutating lifecycle)', () => {
    const content = JSON.stringify({ type: 'shutdown_request', reason: 'done' });
    assert.equal(sendMessageTool.isReadOnlyForInput!({ to: 'worker', content }), false);
  });

  it('a structured plan_approval_request is NOT read-only', () => {
    const content = JSON.stringify({ type: 'plan_approval_request', planContent: 'do x' });
    assert.equal(sendMessageTool.isReadOnlyForInput!({ to: 'lead', content }), false);
  });

  it('a structured task_assignment is NOT read-only', () => {
    const content = JSON.stringify({ type: 'task_assignment', taskId: '1', subject: 's' });
    assert.equal(sendMessageTool.isReadOnlyForInput!({ to: 'worker', content }), false);
  });

  it('missing/non-string content defaults to read-only (harmless; body errors)', () => {
    assert.equal(sendMessageTool.isReadOnlyForInput!({ to: 'worker' }), true);
  });

  it('JSON that is not a known protocol discriminator is still plain text (read-only)', () => {
    // A recognized discriminator makes it structured; arbitrary JSON is not.
    const content = JSON.stringify({ hello: 'world' });
    assert.equal(sendMessageTool.isReadOnlyForInput!({ to: 'worker', content }), true);
  });

  it('is honored end-to-end through isToolReadOnlyForInput (permission wiring)', () => {
    // The permission layer (tool-executor) routes through isToolReadOnlyForInput,
    // so the predicate's verdict is what actually drives auto-allow vs ask.
    assert.equal(
      isToolReadOnlyForInput(sendMessageTool, { to: 'worker', content: 'hi' }),
      true,
    );
    const structured = JSON.stringify({ type: 'mode_set_request', mode: 'default' });
    assert.equal(
      isToolReadOnlyForInput(sendMessageTool, { to: 'worker', content: structured }),
      false,
    );
  });
});

describe('sendMessageTool – isEnabled gate (teammate subsystem)', () => {
  it('exposes an isEnabled() capability gate', () => {
    // PARITY: Claude gates visibility with isEnabled() = isAgentSwarmsEnabled().
    assert.equal(typeof sendMessageTool.isEnabled, 'function');
  });
});

describe('sendMessageTool – reply affordance documentation', () => {
  it('tells the model to reply by copying the incoming sender into "to"', () => {
    const d = sendMessageTool.description;
    assert.match(d, /reply to an incoming message/i);
    assert.match(d, /from/i);
    assert.match(d, /copy/i);
  });
});
