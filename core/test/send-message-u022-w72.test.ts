/**
 * U022 wave-72 additions to the teammate send_message subsystem. Each block maps to
 * a confirmed the reference parity gap:
 *
 *  - teammate_terminated structured message type ({type, message}) —
 *    components/messages/PlanApprovalMessage.tsx + UserTeammateMessage.tsx: a silent
 *    lifecycle notice a consumer filters out of the model-facing render. Added to the
 *    structured union so it parses/summarizes/classifies/routes like a first-class
 *    control message (and the receive-side relay keeps dropping it).
 *  - permission_response feedback + updatedInput —
 *    utils/swarm/permissionSync.ts SwarmPermissionResolution: a resolver attaches a
 *    rejection `feedback` and/or an edited `updatedInput` that round-trip to the worker.
 *  - Corrective "known agents" affordance on an unknown recipient —
 *    utils/swarm/backends/PaneBackendExecutor.ts + utils/directMemberMessage.ts: when a
 *    DM targets an unrecognized name the sender is told which agents ARE known so it can
 *    self-correct (additive to AMI's queue-to-coming-online warning, non-breaking).
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  sendMessageTool,
  getMailbox,
  pollMailbox,
  resetAllMailboxes,
  parseStructuredMessage,
  summarizeStructuredMessage,
  toAutoClassifierInput,
  classifyMessageSource,
  isStructuredProtocolMessage,
  relayIncomingMessage,
  registerMailboxNotifier,
  TEAM_LEAD_NAME,
} from '../src/tools/send-message';
import { resetAgentRegistry } from '../src/tools/task';
import { teamCreateTool, registerTeamMember, resetTeam } from '../src/tools/team';
import type { ToolContext } from '../src/types';

let tmp: string;
function ctx(agentId?: string): ToolContext {
  return { cwd: tmp, abortSignal: new AbortController().signal, agentId } as ToolContext;
}

beforeEach(() => {
  resetAllMailboxes();
  resetAgentRegistry();
  resetTeam();
  tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u022-w72-'));
});
afterEach(() => {
  resetAllMailboxes();
  resetAgentRegistry();
  resetTeam();
  try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
});

// ---------------------------------------------------------------------------
// teammate_terminated structured message type
// ---------------------------------------------------------------------------
describe('U022-w72 teammate_terminated message', () => {
  it('parses a well-formed teammate_terminated (message optional)', () => {
    const m = parseStructuredMessage(JSON.stringify({ type: 'teammate_terminated', message: 'killed by lead' }));
    assert.ok(m && !('error' in m));
    assert.equal(m!.type, 'teammate_terminated');
    assert.equal((m as { message?: string }).message, 'killed by lead');

    const bare = parseStructuredMessage(JSON.stringify({ type: 'teammate_terminated' }));
    assert.ok(bare && !('error' in bare));
    assert.equal((bare as { message?: string }).message, undefined);
  });

  it('is recognized as a structured protocol message (not plain text)', () => {
    assert.equal(isStructuredProtocolMessage(JSON.stringify({ type: 'teammate_terminated' })), true);
  });

  it('summarizes with the sender and optional message', () => {
    const c = JSON.stringify({ type: 'teammate_terminated', message: 'OOM' });
    assert.equal(summarizeStructuredMessage(c, 'worker'), '[Teammate Terminated] worker — OOM');
    const bare = JSON.stringify({ type: 'teammate_terminated' });
    assert.equal(summarizeStructuredMessage(bare, 'worker'), '[Teammate Terminated] worker');
  });

  it('projects a terse classifier phrase', () => {
    assert.equal(toAutoClassifierInput(JSON.stringify({ type: 'teammate_terminated' }), 'main'), 'teammate_terminated to main');
  });

  it('classifies as a lifecycle ("task") source', () => {
    assert.equal(classifyMessageSource(JSON.stringify({ type: 'teammate_terminated' }), 'worker'), 'task');
  });

  it('is DROPPED by the receive-side relay (silent lifecycle signal, never chatter)', () => {
    const r = relayIncomingMessage(JSON.stringify({ type: 'teammate_terminated', message: 'gone' }), 'worker');
    assert.equal(r.drop, true);
    assert.equal(r.text, undefined);
  });

  it('routes to the lead on execute and rejects a non-lead recipient', async () => {
    const ok = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: JSON.stringify({ type: 'teammate_terminated', message: 'peer w1 exited' }) },
      ctx('a-1'),
    );
    assert.equal(ok.isError, undefined, ok.output);
    assert.match(ok.output, /termination notice sent to main/);
    assert.equal(getMailbox(TEAM_LEAD_NAME).length, 1);

    const bad = await sendMessageTool.execute(
      { to: 'w2', content: JSON.stringify({ type: 'teammate_terminated' }) },
      ctx('a-1'),
    );
    assert.equal(bad.isError, true);
    assert.match(bad.output, /must be sent to "main"/);
  });

  it('cannot be broadcast ("*")', async () => {
    const r = await sendMessageTool.execute(
      { to: '*', content: JSON.stringify({ type: 'teammate_terminated' }) },
      ctx('a-1'),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /cannot be broadcast/);
  });
});

// ---------------------------------------------------------------------------
// permission_response feedback + updatedInput (resolver-edited input)
// ---------------------------------------------------------------------------
describe('U022-w72 permission_response feedback + updatedInput', () => {
  it('parses feedback and a resolver-edited object input; ignores a non-object updatedInput', () => {
    const m = parseStructuredMessage(JSON.stringify({
      type: 'permission_response', requestId: 'r1', approve: false,
      feedback: 'too broad', updatedInput: { glob: '*.ts' },
    }));
    assert.ok(m && !('error' in m));
    assert.equal((m as { feedback?: string }).feedback, 'too broad');
    assert.deepEqual((m as { updatedInput?: Record<string, unknown> }).updatedInput, { glob: '*.ts' });

    const bad = parseStructuredMessage(JSON.stringify({
      type: 'permission_response', requestId: 'r1', approve: true, updatedInput: 'not-an-object',
    }));
    assert.ok(bad && !('error' in bad));
    assert.equal((bad as { updatedInput?: unknown }).updatedInput, undefined);
    // An array is not a valid tool-input object either.
    const arr = parseStructuredMessage(JSON.stringify({
      type: 'permission_response', requestId: 'r1', approve: true, updatedInput: [1, 2],
    }));
    assert.equal((arr as { updatedInput?: unknown }).updatedInput, undefined);
  });

  it('summarizes a denial with feedback and an approval with an edited-input tag', () => {
    const denied = JSON.stringify({ type: 'permission_response', requestId: 'r1', approve: false, feedback: 'nope' });
    assert.equal(summarizeStructuredMessage(denied), '[Permission Denied] nope');
    const deniedNoFb = JSON.stringify({ type: 'permission_response', requestId: 'r1', approve: false });
    assert.equal(summarizeStructuredMessage(deniedNoFb), '[Permission Denied] request refused by the team lead');
    const edited = JSON.stringify({ type: 'permission_response', requestId: 'r1', approve: true, updatedInput: { a: 1 } });
    assert.equal(summarizeStructuredMessage(edited), '[Permission Granted] (input edited by approver)');
  });

  it('execute (lead) surfaces feedback and edited-input in the confirmation and routes to the worker', async () => {
    const r = await sendMessageTool.execute(
      {
        to: 'worker',
        content: JSON.stringify({
          type: 'permission_response', requestId: 'permission-1@worker', approve: false,
          feedback: 'scope it down', updatedInput: { path: 'src' },
        }),
      },
      ctx(), // lead (no agentId)
    );
    assert.equal(r.isError, undefined, r.output);
    assert.match(r.output, /Permission denied with edited input to worker/);
    assert.match(r.output, /scope it down/);
    // The full payload (incl. feedback + updatedInput) reaches the worker verbatim.
    const box = getMailbox('worker');
    assert.equal(box.length, 1);
    const payload = JSON.parse(box[0].content);
    assert.equal(payload.feedback, 'scope it down');
    assert.deepEqual(payload.updatedInput, { path: 'src' });
  });

  it('a non-lead sender cannot answer a permission request', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'permission_response', requestId: 'r1', approve: true }) },
      ctx('a-2'),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /only the team lead/);
  });
});

// ---------------------------------------------------------------------------
// Corrective "known agents" affordance for an unknown recipient
// ---------------------------------------------------------------------------
describe('U022-w72 corrective unknown-recipient affordance', () => {
  it('lists the known agents when a DM targets an unrecognized name (roster established)', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'alice', agentId: 'a-1' });
    registerTeamMember({ name: 'bob', agentId: 'b-1' });

    const r = await sendMessageTool.execute({ to: 'alica', content: 'typo' }, ctx());
    assert.equal(r.isError, undefined, 'still queued (coming-online superset), just warned');
    assert.match(r.output, /not a known\/live agent/);
    assert.match(r.output, /Known agents:/);
    assert.match(r.output, /alice/);
    assert.match(r.output, /bob/);
  });

  it('does NOT warn when the recipient IS a declared roster member not yet spawned', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'carol', agentId: 'c-1' });
    const r = await sendMessageTool.execute({ to: 'carol', content: 'hi' }, ctx());
    assert.equal(r.isError, undefined);
    assert.ok(!r.output.includes('not a known/live agent'), r.output);
    assert.equal(getMailbox('carol').length, 1);
  });

  it('does NOT warn when there is no roster and no live notifier (plain queue)', async () => {
    const r = await sendMessageTool.execute({ to: 'ghost', content: 'hi' }, ctx());
    assert.ok(!r.output.includes('not a known/live agent'), r.output);
    assert.equal(pollMailbox('ghost').length, 1);
  });

  it('warns + lists known agents when a live notifier exists but no team', async () => {
    registerMailboxNotifier('alive', () => {});
    const r = await sendMessageTool.execute({ to: 'ghost', content: 'hi' }, ctx());
    assert.match(r.output, /not a known\/live agent/);
    assert.match(r.output, /Known agents:.*alive/);
  });
});
