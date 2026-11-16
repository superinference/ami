/**
 * U022 (w69) — additive teammate-mailbox parity: last-peer-DM-derived idle
 * summaries, and a full exit-boundary mailbox teardown (purgeMailbox).
 *
 * Gaps implemented here (all ADDITIVE, fully wired through the real send_message
 * tool + team disband path):
 *
 *  - getLastPeerDmSummary + idle_notification summary auto-derivation
 *    (Claude Code utils/teammateMailbox.ts getLastPeerDmSummary feeding the
 *    idle_notification summary): a teammate that omits an idle summary has it
 *    auto-filled from its LAST peer DM so the lead sees what it was coordinating
 *    on, without the teammate restating it. Peer/lead/"*" exclusion matches
 *    Claude's backward scan.
 *
 *  - purgeMailbox exit-boundary teardown (Claude purges an exiting teammate's
 *    inbox at its lifecycle boundary): on team disband every member's queue,
 *    pending receive() waiters, eager-delivery notifier, revision counter, and
 *    outbound-DM history are reclaimed so a reused name inherits nothing.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  sendMessageTool,
  getLastPeerDmSummary,
  purgeMailbox,
  getMailbox,
  mailboxLength,
  mailboxRevision,
  receiveMessage,
  registerMailboxNotifier,
  notifyMailbox,
  pollMailbox,
  parseStructuredMessage,
  resetAllMailboxes,
  MAX_OUTBOUND_DM_HISTORY,
} from '../src/tools/send-message';
import {
  teamCreateTool,
  teamDeleteTool,
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
  tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u022-w69-'));
});
afterEach(() => {
  resetAllMailboxes();
  resetAgentRegistry();
  resetTeam();
  try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
});

describe('U022/w69 — getLastPeerDmSummary', () => {
  it('returns the last PEER DM, excluding lead DMs and broadcasts', async () => {
    // Peer DM (recorded), then a lead DM + a broadcast (NOT peer DMs).
    await sendMessageTool.execute({ to: 'peer2', message: 'working on the parser', summary: 'parser work' }, ctx('w1'));
    await sendMessageTool.execute({ to: 'main', message: 'status: 50% done' }, ctx('w1'));
    // A broadcast needs a roster; create one recipient mailbox first.
    await sendMessageTool.execute({ to: 'other', message: 'warmup' }, ctx('w1'));
    await sendMessageTool.execute({ to: '*', message: 'heads up all' }, ctx('w1'));

    // The most recent PEER DM is "other" (broadcast + lead DM are excluded).
    assert.equal(getLastPeerDmSummary('w1'), '[to other] warmup');
  });

  it('uses the model summary when given, else truncates the content to 80 chars', async () => {
    await sendMessageTool.execute({ to: 'peerA', message: 'x'.repeat(200) }, ctx('w2'));
    const derived = getLastPeerDmSummary('w2');
    assert.ok(derived);
    assert.equal(derived, `[to peerA] ${'x'.repeat(80)}`);
  });

  it('returns undefined for a sender that sent no peer DM (lead-only)', async () => {
    await sendMessageTool.execute({ to: 'main', message: 'just the lead' }, ctx('w3'));
    assert.equal(getLastPeerDmSummary('w3'), undefined);
    assert.equal(getLastPeerDmSummary('never-sent'), undefined);
  });

  it('caps retained outbound DMs at MAX_OUTBOUND_DM_HISTORY', async () => {
    // Send more than the cap; the latest peer DM must still be readable (the
    // oldest are dropped, newest-wins).
    for (let i = 0; i < MAX_OUTBOUND_DM_HISTORY + 10; i++) {
      await sendMessageTool.execute({ to: `peer${i}`, message: `m${i}`, summary: `s${i}` }, ctx('w4'));
    }
    const last = MAX_OUTBOUND_DM_HISTORY + 9;
    assert.equal(getLastPeerDmSummary('w4'), `[to peer${last}] s${last}`);
  });
});

describe('U022/w69 — idle_notification summary auto-derivation', () => {
  it('auto-fills the idle summary from the last peer DM when omitted', async () => {
    await sendMessageTool.execute({ to: 'peerX', message: 'refactoring the loader', summary: 'loader refactor' }, ctx('worker'));
    const res = await sendMessageTool.execute(
      { to: 'main', content: JSON.stringify({ type: 'idle_notification', idleReason: 'available' }) },
      ctx('worker'),
    );
    assert.equal(res.isError, undefined);
    assert.match(res.output, /summary: \[to peerX\] loader refactor/);

    // The queued payload the lead receives carries the derived summary.
    const box = getMailbox('main');
    const idle = box.find(m => m.from === 'worker');
    assert.ok(idle);
    const parsed = parseStructuredMessage(idle!.content);
    assert.ok(parsed && !('error' in parsed) && parsed.type === 'idle_notification');
    assert.equal((parsed as { summary?: string }).summary, '[to peerX] loader refactor');
  });

  it('does NOT override an explicit idle summary', async () => {
    await sendMessageTool.execute({ to: 'peerX', message: 'old context' }, ctx('worker'));
    const res = await sendMessageTool.execute(
      { to: 'main', content: JSON.stringify({ type: 'idle_notification', summary: 'explicit summary' }) },
      ctx('worker'),
    );
    assert.equal(res.isError, undefined);
    assert.doesNotMatch(res.output, /summary:/);
    const idle = getMailbox('main').find(m => m.from === 'worker');
    const parsed = parseStructuredMessage(idle!.content) as { summary?: string };
    assert.equal(parsed.summary, 'explicit summary');
  });

  it('leaves the summary empty when the teammate sent no peer DM', async () => {
    const res = await sendMessageTool.execute(
      { to: 'main', content: JSON.stringify({ type: 'idle_notification' }) },
      ctx('loner'),
    );
    assert.equal(res.isError, undefined);
    assert.doesNotMatch(res.output, /summary:/);
    const idle = getMailbox('main').find(m => m.from === 'loner');
    const parsed = parseStructuredMessage(idle!.content) as { summary?: string };
    assert.equal(parsed.summary, undefined);
  });
});

describe('U022/w69 — purgeMailbox exit-boundary teardown', () => {
  it('reclaims queue, notifier, revision and outbound-DM history', async () => {
    // Build up per-agent state: a queued message (bumps revision), a notifier,
    // and an outbound-DM record under the SAME name as a sender.
    await sendMessageTool.execute({ to: 'victim', message: 'queued' }, ctx());
    let fired = 0;
    registerMailboxNotifier('victim', () => { fired++; });
    await sendMessageTool.execute({ to: 'peerQ', message: 'from victim' }, ctx('victim'));
    assert.equal(getLastPeerDmSummary('victim'), '[to peerQ] from victim');
    assert.ok(mailboxLength('victim') >= 1);
    assert.ok(mailboxRevision('victim') >= 1);

    purgeMailbox('victim');

    assert.equal(mailboxLength('victim'), 0, 'queue flushed');
    assert.equal(mailboxRevision('victim'), 0, 'revision reset');
    assert.equal(getLastPeerDmSummary('victim'), undefined, 'outbound history dropped');
    // Notifier removed: a stray notify is a no-op (does not fire the old callback).
    notifyMailbox('victim');
    assert.equal(fired, 0, 'notifier unregistered');
  });

  it('drops a pending receive() waiter so a later send does not resolve it', async () => {
    let resolved = false;
    receiveMessage('gone').then(() => { resolved = true; }).catch(() => {});
    purgeMailbox('gone');
    // After purge the waiter is gone — a new delivery enqueues normally instead
    // of resolving the stale promise.
    await sendMessageTool.execute({ to: 'gone', message: 'late' }, ctx());
    await new Promise(r => setTimeout(r, 10));
    assert.equal(resolved, false, 'stale waiter was not resolved');
    assert.equal(mailboxLength('gone'), 1, 'the late message enqueued normally');
  });
});

describe('U022/w69 — team disband purges every member (integration)', () => {
  it('a reused team name inherits no stale mailbox state', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'alpha', agentId: 'a-1', isActive: false });
    // Queue a message to alpha and record an outbound DM from alpha.
    await sendMessageTool.execute({ to: 'alpha', message: 'pending work' }, ctx());
    await sendMessageTool.execute({ to: 'beta', message: 'alpha to beta' }, ctx('a-1'));
    assert.equal(mailboxLength('alpha'), 1);
    assert.equal(getLastPeerDmSummary('a-1'), '[to beta] alpha to beta');

    const del = await teamDeleteTool.execute({}, ctx());
    assert.equal(del.isError, undefined, del.output);

    // alpha's queue is purged by disband (purgeMailbox), not just left dangling.
    assert.equal(mailboxLength('alpha'), 0, 'disband purged the member mailbox');
    assert.equal(pollMailbox('alpha').length, 0);
  });
});
