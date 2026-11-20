/**
 * U022 (w84) — pending cross-agent handshake purge for session reset.
 *
 * `clearPendingCrossAgentHandshakes()` drops ONLY the pending-decision registries
 * (outstanding shutdown-request handshakes + blocked-on-plan-approval boundaries),
 * AMI's analog of the reference's `clearAllPendingCallbacks` on /clear
 * (hooks/useSwarmPermissionPoller.ts). It must:
 *   - clear outstanding shutdown requests (hasOutstandingShutdownRequest → false),
 *   - clear awaiting-plan-approval boundaries (isAwaitingPlanApproval → false),
 *   - return the count of pending entries cleared,
 *   - leave live mailbox QUEUES untouched (an in-process teammate keeps its inbox),
 *   - fix the real latent bug: a re-sent shutdown_request after a clear is NO LONGER
 *     short-circuited as an idempotent no-op against the stale handshake.
 *
 * Red-green: before the fix, `clearPendingCrossAgentHandshakes` does not exist (import
 * fails); with it, a cleared+re-sent shutdown_request queues a fresh request rather
 * than being swallowed as "already requested".
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';

import {
  sendMessageTool,
  clearPendingCrossAgentHandshakes,
  hasOutstandingShutdownRequest,
  isAwaitingPlanApproval,
  getMailbox,
  resetAllMailboxes,
} from '../src/tools/send-message';
import type { ToolContext } from '../src/types';

function ctxFor(agentId?: string): ToolContext {
  return { agentId } as unknown as ToolContext;
}

describe('U022/w84 — clearPendingCrossAgentHandshakes', () => {
  beforeEach(() => { resetAllMailboxes(); });
  afterEach(() => { resetAllMailboxes(); });

  it('clears outstanding shutdown + awaiting-plan state and returns the count', async () => {
    // Lead opens a shutdown handshake against a worker.
    await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'shutdown_request' }) },
      ctxFor(),
    );
    // A worker submits a plan and becomes blocked on approval.
    await sendMessageTool.execute(
      { to: 'main', content: JSON.stringify({ type: 'plan_approval_request', planContent: 'do x' }) },
      ctxFor('peerX'),
    );
    assert.equal(hasOutstandingShutdownRequest('worker'), true, 'precondition: shutdown outstanding');
    assert.equal(isAwaitingPlanApproval('peerX'), true, 'precondition: awaiting plan approval');

    const cleared = clearPendingCrossAgentHandshakes();
    assert.equal(cleared, 2, 'returns the number of pending entries cleared');
    assert.equal(hasOutstandingShutdownRequest('worker'), false, 'shutdown handshake purged');
    assert.equal(isAwaitingPlanApproval('peerX'), false, 'plan-approval boundary purged');
  });

  it('returns 0 when there is no pending handshake state', () => {
    assert.equal(clearPendingCrossAgentHandshakes(), 0);
  });

  it('leaves live mailbox queues untouched (only pending decisions are reset)', async () => {
    // Open a shutdown handshake (which also queues a message into the worker's inbox)
    // plus a separate plain-text DM.
    await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'shutdown_request' }) },
      ctxFor(),
    );
    await sendMessageTool.execute({ to: 'worker', content: 'keep this' }, ctxFor());
    assert.equal(getMailbox('worker').length, 2, 'precondition: two queued messages');

    clearPendingCrossAgentHandshakes();
    assert.equal(hasOutstandingShutdownRequest('worker'), false, 'pending decision reset');
    assert.equal(getMailbox('worker').length, 2, 'queued messages are preserved');
  });

  it('fixes the stale-handshake short-circuit: a re-sent shutdown after clear is not swallowed', async () => {
    const first = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'shutdown_request' }) },
      ctxFor(),
    );
    assert.ok(!/already requested/i.test(first.output), 'first request is sent, not a no-op');

    // Without a clear, a re-send IS the idempotent no-op (documents the behavior we fix).
    const dup = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'shutdown_request' }) },
      ctxFor(),
    );
    assert.match(dup.output, /already requested/i, 'duplicate is idempotently skipped');

    // After clearing pending handshakes (as /clear does), a fresh request is NOT swallowed.
    clearPendingCrossAgentHandshakes();
    const afterClear = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'shutdown_request' }) },
      ctxFor(),
    );
    assert.ok(!/already requested/i.test(afterClear.output), 'post-clear request is sent anew');
    assert.match(afterClear.output, /Shutdown request sent/i);
  });
});
