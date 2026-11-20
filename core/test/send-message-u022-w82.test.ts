/**
 * U022 (w82) — two additive send_message capabilities:
 *
 *  1. Message URGENCY status (`normal` | `proactive`), mirroring the reference's
 *     BriefTool `status` discriminator on the model→user/teammate channel. A
 *     plain-text message may be marked `proactive`; it is stamped on the stored
 *     AgentMessage, surfaced on the send result's metadata, and drained AHEAD of
 *     ordinary peer chatter (anti-starvation) by drainMailboxPrioritized.
 *
 *  2. Open task-assignment tracking → a termination notice with a task-unassignment
 *     summary, mirroring the reference's unassignTeammateTasks (useInboxPoller). A
 *     task_assignment opens an assignment; a completed idle_notification closes it;
 *     notifyLeadTeammateTerminated reports the still-open ones as orphaned.
 *
 * Red-green: before the fix, `status` is neither validated/stamped/prioritized nor
 * surfaced (the drain never reorders proactive messages, metadata.status is absent),
 * and notifyLeadTeammateTerminated carries no unassignment summary
 * (getOpenTaskAssignments does not exist → import fails).
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';

import {
  sendMessageTool,
  drainMailboxPrioritized,
  getMailbox,
  pollMailbox,
  resetAllMailboxes,
  parseStructuredMessage,
  notifyLeadTeammateTerminated,
  getOpenTaskAssignments,
  purgeMailbox,
  TEAM_LEAD_NAME,
  SEND_MESSAGE_VALIDATION_ERROR_CODE,
  type TeammateTerminated,
} from '../src/tools/send-message';
import type { ToolContext } from '../src/types';

function ctxFor(agentId?: string): ToolContext {
  return { agentId } as unknown as ToolContext;
}

describe('U022/w82 send_message — message urgency status (proactive/normal)', () => {
  beforeEach(() => { resetAllMailboxes(); });
  afterEach(() => { resetAllMailboxes(); });

  it('stamps a proactive message and surfaces status on the result metadata', async () => {
    const res = await sendMessageTool.execute(
      { to: 'target', content: 'I hit a blocker', status: 'proactive' },
      ctxFor('peerB'),
    );
    assert.equal(res.isError, undefined);
    assert.deepEqual(res.metadata, { status: 'proactive' });
    const box = getMailbox('target');
    assert.equal(box.length, 1);
    assert.equal(box[0].status, 'proactive', 'proactive urgency stamped on the stored message');
  });

  it('treats a normal / omitted status as the default (not stamped, no metadata)', async () => {
    const res = await sendMessageTool.execute(
      { to: 'target', content: 'just replying', status: 'normal' },
      ctxFor('peerA'),
    );
    assert.equal(res.metadata, undefined, 'normal is the default — no metadata.status');
    assert.equal(getMailbox('target')[0].status, undefined, 'normal is not stamped');

    const res2 = await sendMessageTool.execute({ to: 'target2', content: 'no status field' }, ctxFor('peerA'));
    assert.equal(res2.metadata, undefined);
    assert.equal(getMailbox('target2')[0].status, undefined);
  });

  it('rejects an invalid status with the input-shape validation error code', async () => {
    const res = await sendMessageTool.execute(
      { to: 'target', content: 'x', status: 'urgent' },
      ctxFor('peerA'),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /status.*must be one of/);
    assert.equal(res.metadata?.errorCode, SEND_MESSAGE_VALIDATION_ERROR_CODE);
    assert.equal(getMailbox('target').length, 0, 'nothing queued on a rejected send');
  });

  it('drains proactive peer messages ahead of ordinary peer chatter, after the lead', async () => {
    // Lead message, a normal peer message, and a proactive peer message — delivered
    // in an order that is NOT the priority order, to prove the drain reorders.
    await sendMessageTool.execute({ to: 'target', content: 'normal peer' }, ctxFor('peerA'));
    await sendMessageTool.execute({ to: 'target', content: 'urgent peer', status: 'proactive' }, ctxFor('peerB'));
    await sendMessageTool.execute({ to: 'target', content: 'from the lead' }, ctxFor()); // lead (no agentId)

    const drained = drainMailboxPrioritized('target');
    assert.deepEqual(
      drained.map(m => m.content),
      ['from the lead', 'urgent peer', 'normal peer'],
      'order: lead → proactive peer → normal peer',
    );
    assert.equal(getMailbox('target').length, 0, 'drain removes every message');
  });

  it('carries status on a broadcast result when proactive', async () => {
    // Seed two reachable recipients.
    await sendMessageTool.execute({ to: 'a', content: 'seed' }, ctxFor('peerA'));
    await sendMessageTool.execute({ to: 'b', content: 'seed' }, ctxFor('peerA'));
    pollMailbox('a'); pollMailbox('b');

    const res = await sendMessageTool.execute(
      { to: '*', content: 'all hands — prod is down', status: 'proactive' },
      ctxFor('peerC'),
    );
    assert.equal(res.isError, undefined);
    assert.deepEqual(res.metadata, { status: 'proactive' });
    assert.equal(getMailbox('a')[0].status, 'proactive', 'broadcast stamps each recipient copy');
    assert.equal(getMailbox('b')[0].status, 'proactive');
  });
});

describe('U022/w82 send_message — task-unassignment summary on termination', () => {
  beforeEach(() => { resetAllMailboxes(); });
  afterEach(() => { resetAllMailboxes(); });

  async function assign(taskId: string, subject: string): Promise<void> {
    const res = await sendMessageTool.execute(
      { to: 'worker', message: { type: 'task_assignment', taskId, subject } },
      ctxFor(), // lead assigns
    );
    assert.equal(res.isError, undefined, `assignment ${taskId} succeeded`);
  }

  it('tracks an open assignment and exposes it via getOpenTaskAssignments', async () => {
    await assign('T1', 'build the parser');
    assert.deepEqual(getOpenTaskAssignments('worker'), [{ taskId: 'T1', subject: 'build the parser' }]);
    assert.deepEqual(getOpenTaskAssignments('nobody'), [], 'empty for a teammate with no tasks');
  });

  it('closes an assignment when the teammate reports it completed via idle_notification', async () => {
    await assign('T1', 'build the parser');
    await assign('T2', 'write the docs');
    const res = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, message: { type: 'idle_notification', completedTaskId: 'T1', completedStatus: 'resolved' } },
      ctxFor('worker'),
    );
    assert.equal(res.isError, undefined);
    assert.deepEqual(getOpenTaskAssignments('worker'), [{ taskId: 'T2', subject: 'write the docs' }], 'only the open task remains');
  });

  it('folds the orphaned tasks into the termination notice and releases them', async () => {
    await assign('T1', 'build the parser');
    await assign('T2', 'write the docs');

    const ok = notifyLeadTeammateTerminated('worker', 'agent killed');
    assert.equal(ok, true);

    const box = getMailbox(TEAM_LEAD_NAME);
    assert.equal(box.length, 1);
    const parsed = parseStructuredMessage(box[0].content) as TeammateTerminated;
    assert.equal(parsed.type, 'teammate_terminated');
    assert.match(parsed.message!, /agent killed/, 'keeps the base reason');
    assert.match(parsed.message!, /2 task\(s\) now unassigned/, 'summarizes the orphaned count');
    assert.match(parsed.message!, /#T1 \(build the parser\)/);
    assert.match(parsed.message!, /#T2 \(write the docs\)/);

    assert.deepEqual(getOpenTaskAssignments('worker'), [], 'orphaned tasks released from the registry');
  });

  it('emits a bare notice (no unassignment clause) when no task is open', async () => {
    const ok = notifyLeadTeammateTerminated('worker');
    assert.equal(ok, true);
    const parsed = parseStructuredMessage(getMailbox(TEAM_LEAD_NAME)[0].content) as TeammateTerminated;
    assert.equal(parsed.message, undefined, 'no message when there is nothing to report');
  });

  it('purge clears a teammate\'s open assignments', async () => {
    await assign('T1', 'build the parser');
    purgeMailbox('worker');
    assert.deepEqual(getOpenTaskAssignments('worker'), []);
  });
});
