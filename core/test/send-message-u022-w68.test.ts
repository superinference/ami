/**
 * U022 (w68) — additive per-message sender COLOR on the teammate mailbox.
 *
 * Parity gap (the reference teammateMailbox TeammateMessage.color /
 * UserTeammateMessage): every delivered inter-agent message should carry the
 * sender's assigned display color so a consumer can render it in that teammate's
 * own color. AMI assigns per-agent colors in team.ts (round-robin palette) but
 * the mailbox dropped them — a queued message had no `color`.
 *
 * This proves the whole in-file pipeline: team roster color assignment →
 * send_message execute → enqueue (the single canonical delivery site stamps the
 * color) → pollMailbox returns it, for direct, broadcast, structured, and
 * waiter-handoff deliveries. The color is resolved from the trusted roster, never
 * model-supplied.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  sendMessageTool,
  pollMailbox,
  getMailbox,
  receiveMessage,
  resetAllMailboxes,
} from '../src/tools/send-message';
import {
  teamCreateTool,
  registerTeamMember,
  resetTeam,
} from '../src/tools/team';
import { resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';

let tmp: string;
function ctx(agentId?: string): ToolContext {
  return { cwd: tmp, abortSignal: new AbortController().signal, agentId } as ToolContext;
}

beforeEach(() => {
  resetAllMailboxes();
  resetAgentRegistry();
  resetTeam();
  tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u022-w68-'));
});
afterEach(() => {
  resetAllMailboxes();
  resetAgentRegistry();
  resetTeam();
  try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
});

describe('U022/w68 — per-message sender color', () => {
  it('stamps a DM with the sender\'s trusted roster color', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    // Two distinct members; explicit colors so the assertion is deterministic.
    registerTeamMember({ name: 'alice', agentId: 'a-1', color: 'blue' });
    registerTeamMember({ name: 'bob', agentId: 'b-1', color: 'green' });

    const res = await sendMessageTool.execute(
      { to: 'bob', message: 'hello bob' },
      ctx('a-1'),
    );
    assert.equal(res.isError, undefined);
    const box = getMailbox('bob');
    assert.equal(box.length, 1);
    assert.equal(box[0].color, 'blue', 'the message carries the SENDER (alice) color');
    // from is the trusted agent identity (agentId; no name-registry entry here).
    assert.equal(box[0].from, 'a-1');
  });

  it('does not stamp a color when the sender is not a colored roster member', async () => {
    // No team → no roster → no color (color is a best-effort hint, not required).
    const res = await sendMessageTool.execute(
      { to: 'ghost', message: 'ping' },
      ctx(),
    );
    assert.equal(res.isError, undefined);
    const box = getMailbox('ghost');
    assert.equal(box.length, 1);
    assert.equal(box[0].color, undefined);
  });

  it('carries the sender color on every recipient of a broadcast', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'lead', agentId: 'lead-1', color: 'magenta' });
    registerTeamMember({ name: 'w1', agentId: 'w-1', color: 'cyan' });
    registerTeamMember({ name: 'w2', agentId: 'w-2', color: 'yellow' });
    // Give each recipient a mailbox so the broadcast roster includes them.
    await sendMessageTool.execute({ to: 'w1', message: 'warmup' }, ctx('lead-1'));
    await sendMessageTool.execute({ to: 'w2', message: 'warmup' }, ctx('lead-1'));
    pollMailbox('w1'); pollMailbox('w2');

    const res = await sendMessageTool.execute(
      { to: '*', message: 'all hands' },
      ctx('lead-1'),
    );
    assert.equal(res.isError, undefined);
    for (const name of ['w1', 'w2']) {
      const box = getMailbox(name);
      const bc = box.find(m => m.content === 'all hands');
      assert.ok(bc, `broadcast reached ${name}`);
      assert.equal(bc!.color, 'magenta', `broadcast carries the lead color to ${name}`);
    }
  });

  it('carries the sender color on a structured control message', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'lead', agentId: 'lead-1', color: 'red' });
    registerTeamMember({ name: 'worker', agentId: 'w-1', color: 'blue' });

    const res = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'task_assignment', taskId: 'T1', subject: 'do it' }) },
      ctx('lead-1'),
    );
    assert.equal(res.isError, undefined);
    const box = getMailbox('worker');
    assert.equal(box.length, 1);
    assert.equal(box[0].color, 'red', 'the structured message carries the lead color');
  });

  it('stamps the color on a waiter hand-off (receiveMessage) delivery', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'alice', agentId: 'a-1', color: 'green' });
    registerTeamMember({ name: 'bob', agentId: 'b-1', color: 'blue' });

    const waiting = receiveMessage('bob');
    await sendMessageTool.execute({ to: 'bob', message: 'direct handoff' }, ctx('a-1'));
    const got = await waiting;
    assert.equal(got.content, 'direct handoff');
    assert.equal(got.color, 'green', 'a waiter hand-off carries the sender color too');
  });
});
