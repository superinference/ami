/**
 * U022 wave-76 additions to the teammate send_message subsystem. Each block maps to
 * a confirmed the reference parity gap that was NOT yet implemented on the file:
 *
 *  - Teammate blocked-on-plan-approval state tracking + reset on leader response —
 *    utils/inProcessTeammateHelpers.ts setAwaitingPlanApproval / handlePlanApprovalResponse
 *    (L48-L83). Claude tracks a per-teammate `awaitingPlanApproval` boundary state and
 *    resets it exactly when a plan_approval_response arrives from the lead, so a teammate
 *    can submit a plan, block awaiting the lead, and be cleanly unblocked. AMI constructed
 *    and routed both plan_approval messages but tracked NO blocked state — the
 *    submit/unblock boundary did not exist. This adds the state machine fully within the
 *    send_message execute() paths (set on plan_approval_request, cleared on the matching
 *    plan_approval_response) + an introspection surface, mirroring the outstandingShutdowns
 *    handshake.
 *
 *  - Typed delivery-warning discriminants (unknown_recipient | no_team_context) —
 *    utils/directMemberMessage.ts sendDirectMemberMessage (L22-L68) returns a
 *    discriminated `{success:false, error:'no_team_context'|'unknown_recipient'}` so a
 *    caller can distinguish a stale/typo recipient from a missing team context. AMI only
 *    emitted a free-form human warning (and deliberately still QUEUES — its
 *    queue-to-coming-online superset). This attaches a machine-readable
 *    `metadata.deliveryWarning` discriminant ALONGSIDE the human warning so a programmatic
 *    consumer gets the same classification Claude surfaces, without regressing AMI's
 *    queue behavior.
 *
 * Red-green: the imported symbols (isAwaitingPlanApproval, getAwaitingPlanApprovalRequestId,
 * DeliveryWarning) do not exist before the fix; metadata.deliveryWarning is absent and the
 * awaiting-approval state is never set/cleared.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';

import {
  sendMessageTool,
  getMailbox,
  resetAllMailboxes,
  purgeMailbox,
  isAwaitingPlanApproval,
  getAwaitingPlanApprovalRequestId,
  TEAM_LEAD_NAME,
  type DeliveryWarning,
} from '../src/tools/send-message';
import { resetAgentRegistry } from '../src/tools/task';
import { resetTeam, registerTeamMember, teamCreateTool } from '../src/tools/team';
import type { ToolContext } from '../src/types';

function ctx(agentId?: string): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, agentId } as ToolContext;
}

beforeEach(() => { resetAllMailboxes(); resetAgentRegistry(); resetTeam(); });
afterEach(() => { resetAllMailboxes(); resetAgentRegistry(); resetTeam(); });

// ---------------------------------------------------------------------------
// Teammate blocked-on-plan-approval state tracking + reset on leader response
// ---------------------------------------------------------------------------
describe('U022-w76 plan-approval awaiting state', () => {
  it('a teammate is not awaiting approval before it submits a plan', () => {
    assert.equal(isAwaitingPlanApproval('worker'), false);
    assert.equal(getAwaitingPlanApprovalRequestId('worker'), undefined);
  });

  it('sets the awaiting-approval boundary when a teammate submits a plan_approval_request', async () => {
    const res = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: JSON.stringify({ type: 'plan_approval_request', planContent: 'do the thing' }) },
      ctx('worker'),
    );
    assert.equal(res.isError, undefined);
    assert.equal(isAwaitingPlanApproval('worker'), true);
    const reqId = getAwaitingPlanApprovalRequestId('worker');
    assert.ok(reqId && reqId.startsWith('plan-'), `expected a structured plan request id, got ${reqId}`);
  });

  it('clears the awaiting-approval boundary when the lead sends a plan_approval_response (approve)', async () => {
    await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: JSON.stringify({ type: 'plan_approval_request', planContent: 'plan X' }) },
      ctx('worker'),
    );
    assert.equal(isAwaitingPlanApproval('worker'), true);
    const reqId = getAwaitingPlanApprovalRequestId('worker')!;
    // Lead (no agentId) answers the teammate.
    const res = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'plan_approval_response', requestId: reqId, approve: true }) },
      ctx(),
    );
    assert.equal(res.isError, undefined);
    assert.equal(isAwaitingPlanApproval('worker'), false, 'approval must unblock the teammate');
  });

  it('clears the awaiting-approval boundary on a REJECTED plan_approval_response (revise path)', async () => {
    await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: JSON.stringify({ type: 'plan_approval_request', planContent: 'plan Y' }) },
      ctx('worker'),
    );
    const reqId = getAwaitingPlanApprovalRequestId('worker')!;
    await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'plan_approval_response', requestId: reqId, approve: false, feedback: 'narrow the scope' }) },
      ctx(),
    );
    assert.equal(isAwaitingPlanApproval('worker'), false, 'a rejection also unblocks so the teammate can revise');
  });

  it('purgeMailbox tears down a dangling awaiting-approval boundary', async () => {
    await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: JSON.stringify({ type: 'plan_approval_request', planContent: 'plan Z' }) },
      ctx('worker'),
    );
    assert.equal(isAwaitingPlanApproval('worker'), true);
    purgeMailbox('worker');
    assert.equal(isAwaitingPlanApproval('worker'), false);
  });

  it('resetAllMailboxes clears all awaiting-approval state', async () => {
    await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: JSON.stringify({ type: 'plan_approval_request', planContent: 'plan Q' }) },
      ctx('worker'),
    );
    assert.equal(isAwaitingPlanApproval('worker'), true);
    resetAllMailboxes();
    assert.equal(isAwaitingPlanApproval('worker'), false);
  });
});

// ---------------------------------------------------------------------------
// Typed delivery-warning discriminants (unknown_recipient | no_team_context)
// ---------------------------------------------------------------------------
describe('U022-w76 typed delivery-warning discriminants', () => {
  it('attaches metadata.deliveryWarning="unknown_recipient" when a roster exists but the name is not in it', async () => {
    // Establish a roster with a known member, then message a name NOT on it.
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'alice', agentId: 'a-alice' });
    const res = await sendMessageTool.execute(
      { to: 'nonsuch', content: 'hello?' },
      ctx(),
    );
    assert.equal(res.isError, undefined, 'still queues — queue-to-coming-online superset');
    const dw: DeliveryWarning | undefined = res.metadata?.deliveryWarning as DeliveryWarning | undefined;
    assert.equal(dw, 'unknown_recipient');
    // The human warning is still present.
    assert.match(res.output, /not a known\/live agent/);
    // And the message IS queued (not hard-rejected).
    assert.equal(getMailbox('nonsuch').length, 1);
  });

  it('classifies as "no_team_context" when there is no roster but another agent is live', async () => {
    // A live notifier makes the session "has other agents" without a roster.
    const { registerMailboxNotifier } = await import('../src/tools/send-message');
    registerMailboxNotifier('live-peer', () => {});
    const res = await sendMessageTool.execute(
      { to: 'ghost', content: 'anyone?' },
      ctx(),
    );
    assert.equal(res.metadata?.deliveryWarning, 'no_team_context');
    assert.match(res.output, /not a known\/live agent/);
  });

  it('attaches NO deliveryWarning when the recipient is reachable (roster member)', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'bob', agentId: 'a-bob' });
    const res = await sendMessageTool.execute(
      { to: 'bob', content: 'hi bob' },
      ctx(),
    );
    assert.equal(res.metadata?.deliveryWarning, undefined);
    assert.doesNotMatch(res.output, /warning/);
  });

  it('attaches NO deliveryWarning in a solo session (no roster, no live peers)', async () => {
    const res = await sendMessageTool.execute(
      { to: 'whoever', content: 'solo send' },
      ctx(),
    );
    assert.equal(res.metadata?.deliveryWarning, undefined);
  });
});
