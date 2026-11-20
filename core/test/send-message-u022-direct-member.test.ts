/**
 * U022 — user-facing `@agent-name message` direct-message delivery.
 *
 * Parity with the reference's utils/directMemberMessage.ts (sendDirectMemberMessage):
 * the human hands a message straight to a named teammate's mailbox, bypassing the
 * model. `deliverDirectMemberMessage` is the real delivery leg (the pure parser
 * lives in common/src/direct-member-mode.ts). It reuses the canonical in-session
 * transport (enqueue + the mailbox), so a delivered message is drained exactly like
 * a model-sent plain-text DM, classified as a `user` source (sent as the lead).
 *
 * Red-green: `deliverDirectMemberMessage` does not exist before the change (import
 * fails); the result discriminants (success / unknown_recipient / no_team_context)
 * encode the behavior that discriminates the fix from "always queue".
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  deliverDirectMemberMessage,
  registerMailboxNotifier,
  getMailbox,
  pollMailbox,
  resetAllMailboxes,
  classifyMessageSource,
  TEAM_LEAD_NAME,
} from '../src/tools/send-message';
import { teamCreateTool, resetTeam, registerTeamMember } from '../src/tools/team';
import type { ToolContext } from '../src/types';

describe('U022 deliverDirectMemberMessage — user @name fast path', () => {
  beforeEach(() => { resetTeam(); resetAllMailboxes(); });
  afterEach(() => { resetTeam(); resetAllMailboxes(); });

  it('no_team_context when there is no roster and no live agent', () => {
    const r = deliverDirectMemberMessage('worker', 'hello there');
    assert.deepEqual(r, { success: false, error: 'no_team_context', recipientName: 'worker' });
    assert.equal(getMailbox('worker').length, 0);
  });

  it('delivers to a live teammate (registered notifier) as the lead/user', () => {
    let notified = 0;
    registerMailboxNotifier('worker', () => { notified++; });
    const r = deliverDirectMemberMessage('worker', 'ship the fix');
    assert.equal(r.success, true);
    if (r.success) {
      assert.equal(r.recipientName, 'worker');
      assert.equal(r.from, TEAM_LEAD_NAME);
    }
    // Eager notifier fired on delivery.
    assert.ok(notified >= 1);
    const box = getMailbox('worker');
    assert.equal(box.length, 1);
    assert.equal(box[0].from, TEAM_LEAD_NAME);
    assert.equal(box[0].content, 'ship the fix');
    // A lead-authored message classifies as a `user` source.
    assert.equal(box[0].source, 'user');
    assert.equal(classifyMessageSource(box[0].content, box[0].from), 'user');
  });

  it('unknown_recipient when a team is live but the name is not a member', () => {
    registerMailboxNotifier('worker', () => {});
    const r = deliverDirectMemberMessage('typo', 'are you there');
    assert.deepEqual(r, { success: false, error: 'unknown_recipient', recipientName: 'typo' });
    assert.equal(getMailbox('typo').length, 0);
  });

  it('delivers to a roster member that has not yet come online', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-dm-'));
    try {
      const ctx = { cwd: tmp } as unknown as ToolContext;
      await teamCreateTool.execute({ team_name: 'sq' }, ctx);
      registerTeamMember({ name: 'researcher', agentId: 'a-9' });
      const r = deliverDirectMemberMessage('researcher', 'start the survey');
      assert.equal(r.success, true);
      // Delivered under the recipient key the roster member is addressed by.
      const box = getMailbox('researcher');
      assert.equal(box.length, 1);
      assert.equal(box[0].content, 'start the survey');
    } finally {
      try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });

  it('cannot direct-message the lead/self through this path', () => {
    registerMailboxNotifier('worker', () => {});
    const r = deliverDirectMemberMessage(TEAM_LEAD_NAME, 'note to self');
    assert.equal(r.success, false);
    if (!r.success) assert.equal(r.error, 'unknown_recipient');
  });

  it('rejects an empty body even for a live recipient', () => {
    registerMailboxNotifier('worker', () => {});
    const r = deliverDirectMemberMessage('worker', '   ');
    assert.equal(r.success, false);
    assert.equal(getMailbox('worker').length, 0);
  });

  it('a delivered DM drains like an ordinary plain-text message', () => {
    registerMailboxNotifier('worker', () => {});
    deliverDirectMemberMessage('worker', 'one');
    deliverDirectMemberMessage('worker', 'two');
    const drained = pollMailbox('worker');
    assert.deepEqual(drained.map(m => m.content), ['one', 'two']);
    assert.equal(getMailbox('worker').length, 0);
  });
});
