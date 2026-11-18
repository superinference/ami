/**
 * U022 — transcript-suppression decision for routed / async-request send_message
 * results.
 *
 * Mirrors Claude Code's SendMessageTool renderToolResultMessage two null-render
 * branches (tools/SendMessageTool/UI.tsx L21-26): a result whose metadata carries a
 * truthy `routing` field (fire-and-forward / hub-routed RESPONSE) OR both
 * `request_id` and `target` (async REQUEST/response correlation) is ABSORBED from
 * the transcript rather than echoed as a confirmation row; a plain DM / broadcast
 * (no correlation metadata) still renders. `shouldSuppressSendMessageTranscript` is
 * the single canonical decision the tool-executor keys the `tool_use_result`
 * `suppressTranscript` flag on; this proves it against the ACTUAL metadata shapes
 * `sendMessageTool.execute` produces (not hand-rolled objects).
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  resetAllMailboxes,
  generateRequestId,
  shouldSuppressSendMessageTranscript,
  TEAM_LEAD_NAME,
} from '../src/tools/send-message';
import { resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';

function ctx(agentId?: string): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, agentId } as ToolContext;
}

beforeEach(() => { resetAllMailboxes(); resetAgentRegistry(); });
afterEach(() => { resetAllMailboxes(); resetAgentRegistry(); });

describe('shouldSuppressSendMessageTranscript (pure decision)', () => {
  it('suppresses a result carrying a truthy routing marker', () => {
    assert.equal(shouldSuppressSendMessageTranscript({ routing: true, request_id: 'x', target: 'y' }), true);
  });

  it('suppresses an async request/response shape (request_id + target, no routing)', () => {
    assert.equal(shouldSuppressSendMessageTranscript({ request_id: 'shutdown-1@w', target: 'worker' }), true);
  });

  it('does NOT suppress a plain delivery-warning / status-only metadata', () => {
    assert.equal(shouldSuppressSendMessageTranscript({ status: 'proactive' }), false);
    assert.equal(shouldSuppressSendMessageTranscript({ deliveryWarning: 'unknown_recipient' }), false);
  });

  it('does NOT suppress when only one correlation half is present', () => {
    assert.equal(shouldSuppressSendMessageTranscript({ request_id: 'x' }), false);
    assert.equal(shouldSuppressSendMessageTranscript({ target: 'y' }), false);
  });

  it('does NOT suppress a falsy routing marker on its own', () => {
    assert.equal(shouldSuppressSendMessageTranscript({ routing: false }), false);
  });

  it('returns false for absent metadata', () => {
    assert.equal(shouldSuppressSendMessageTranscript(undefined), false);
    assert.equal(shouldSuppressSendMessageTranscript({}), false);
  });
});

describe('suppression keyed on real execute() metadata', () => {
  it('a RESPONSE send (routing:true) is suppressed', async () => {
    const rid = generateRequestId('shutdown', 'worker');
    const r = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: JSON.stringify({ type: 'shutdown_response', requestId: rid, approve: true }) },
      ctx('agent-9'),
    );
    assert.equal(shouldSuppressSendMessageTranscript(r.metadata), true);
  });

  it('a REQUEST send (request_id + target) is suppressed', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'shutdown_request', reason: 'wrap up' }) },
      ctx(),
    );
    assert.equal(shouldSuppressSendMessageTranscript(r.metadata), true);
  });

  it('a plain-text DM is NOT suppressed (the confirmation still renders)', async () => {
    const r = await sendMessageTool.execute({ to: 'worker', content: 'hello there' }, ctx());
    assert.equal(shouldSuppressSendMessageTranscript(r.metadata), false);
  });

  it('a broadcast is NOT suppressed', async () => {
    // Register a live recipient so the broadcast reaches someone and returns a result.
    const { registerMailboxNotifier } = await import('../src/tools/send-message');
    registerMailboxNotifier('peer', () => {});
    const r = await sendMessageTool.execute({ to: '*', content: 'team update' }, ctx('lead'));
    assert.equal(r.isError, undefined);
    assert.equal(shouldSuppressSendMessageTranscript(r.metadata), false);
  });
});
