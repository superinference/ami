/**
 * U022 (gap F: proactive 'now idle / available' notification) — the automatic,
 * push-based half of the teammate idle lifecycle.
 *
 * Claude Code registers a Stop hook so a teammate that finishes its turn writes
 * `createIdleNotification(agentName, { idleReason: 'available', summary })` to the
 * team leader's inbox — the lead is TOLD a peer is free, without polling. AMI had
 * the `idle_notification` message type + manual send path, but a teammate that
 * simply stopped never emitted one. `notifyLeadTeammateIdle` closes that: it pushes
 * an `idle_notification` to the lead, with the summary auto-derived from the
 * teammate's last peer DM. `markTeammateIdle` (task.ts) fires it on every finish.
 *
 * Red-green: `notifyLeadTeammateIdle` does not exist before the fix (import/ref
 * fails), and `markTeammateIdle` did not push to the lead mailbox before the wiring.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  notifyLeadTeammateIdle,
  getMailbox,
  pollMailbox,
  resetAllMailboxes,
  parseStructuredMessage,
  TEAM_LEAD_NAME,
  sendMessageTool,
  type IdleNotification,
} from '../src/tools/send-message';
import { markTeammateIdle } from '../src/tools/task';
import { teamCreateTool, getCurrentTeam, resetTeam, registerTeamMember } from '../src/tools/team';
import type { ToolContext } from '../src/types';

describe('U022 notifyLeadTeammateIdle — proactive idle/available push to the lead', () => {
  beforeEach(() => { resetAllMailboxes(); });
  afterEach(() => { resetAllMailboxes(); });

  it('queues an idle_notification (idleReason "available") into the lead mailbox', () => {
    const ok = notifyLeadTeammateIdle('worker');
    assert.equal(ok, true, 'returns true when a notification was queued');

    const box = getMailbox(TEAM_LEAD_NAME);
    assert.equal(box.length, 1, 'exactly one idle_notification queued to the lead');
    assert.equal(box[0].from, 'worker');
    assert.equal(box[0].to, TEAM_LEAD_NAME);

    const parsed = parseStructuredMessage(box[0].content);
    assert.ok(parsed && !('error' in parsed), 'content is a well-formed structured message');
    assert.equal((parsed as IdleNotification).type, 'idle_notification');
    assert.equal((parsed as IdleNotification).idleReason, 'available');
    // Classified as a work-lifecycle 'task' message at the canonical delivery site.
    assert.equal(box[0].source, 'task');
  });

  it('derives the summary from the teammate\'s last PEER DM', async () => {
    // The teammate sends a plain-text DM to a peer (recorded as its last peer DM).
    const ctx = { agentId: 'worker' } as unknown as ToolContext;
    await sendMessageTool.execute(
      { to: 'peer', content: 'handling the auth refactor', summary: 'auth refactor' },
      ctx,
    );
    // Drain so only the idle_notification remains to inspect.
    pollMailbox('peer');

    notifyLeadTeammateIdle('worker');
    const box = getMailbox(TEAM_LEAD_NAME);
    assert.equal(box.length, 1);
    const parsed = parseStructuredMessage(box[0].content) as IdleNotification;
    assert.equal(parsed.summary, '[to peer] auth refactor', 'idle summary carries the last peer DM');
  });

  it('omits the summary when the teammate has no prior peer DM', () => {
    notifyLeadTeammateIdle('fresh');
    const parsed = parseStructuredMessage(getMailbox(TEAM_LEAD_NAME)[0].content) as IdleNotification;
    assert.equal(parsed.summary, undefined, 'no summary field when nothing to derive');
  });

  it('is a no-op for the lead itself (no self-notify) and for a blank name', () => {
    assert.equal(notifyLeadTeammateIdle(TEAM_LEAD_NAME), false, 'lead does not notify itself');
    assert.equal(notifyLeadTeammateIdle('MAIN'), false, 'case-insensitive lead guard');
    assert.equal(notifyLeadTeammateIdle('   '), false, 'blank name rejected');
    assert.equal(notifyLeadTeammateIdle(''), false, 'empty name rejected');
    assert.equal(getMailbox(TEAM_LEAD_NAME).length, 0, 'nothing queued');
  });
});

describe('U022 markTeammateIdle — end-to-end: a finished teammate auto-notifies the lead', () => {
  let tmp: string;
  beforeEach(() => { resetTeam(); resetAllMailboxes(); tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-idle-push-')); });
  afterEach(() => { resetTeam(); resetAllMailboxes(); try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } });

  function ctx(): ToolContext {
    return {
      cwd: tmp,
      sessionId: 'sess-1',
      abortSignal: new AbortController().signal,
    } as unknown as ToolContext;
  }

  it('pushes an idle_notification to the lead when a teammate is marked idle', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'worker', agentId: 'a-1' });

    markTeammateIdle(ctx(), 'a-1');

    const box = getMailbox(TEAM_LEAD_NAME);
    assert.equal(box.length, 1, 'lead received exactly one idle notification');
    const parsed = parseStructuredMessage(box[0].content) as IdleNotification;
    assert.equal(parsed.type, 'idle_notification');
    assert.equal(parsed.idleReason, 'available');
    assert.equal(box[0].from, 'worker', 'notification is from the finished teammate by name');
    // Teammate is marked idle (existing behavior preserved).
    assert.equal(getCurrentTeam()!.members.find(m => m.agentId === 'a-1')!.isActive, false);
  });

  it('does not notify when the finishing id is not a team member', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    markTeammateIdle(ctx(), 'ghost');
    assert.equal(getMailbox(TEAM_LEAD_NAME).length, 0, 'no notification for an unknown teammate');
  });
});
