/**
 * U022 wave-71 additions to the teammate send_message subsystem. Each block maps to
 * a confirmed the reference parity gap:
 *
 *  - Multi-subscriber mailbox change signal (subscribeMailbox) — utils/mailbox.ts
 *    `Mailbox.subscribe` (createSignal), coexisting with AMI's single eager notifier.
 *  - Idempotent graceful-shutdown handshake (hasOutstandingShutdownRequest + guard) —
 *    utils/swarm/backends/InProcessBackend.ts `terminate()` early-return on an already
 *    set `shutdownRequested` flag, resolved by the shutdown_response.
 *  - Two-tier prioritized mailbox drain (drainMailboxPrioritized) —
 *    utils/swarm/inProcessRunner.ts scan order: shutdown_request first, then team-lead
 *    messages, then peer FIFO (anti-starvation).
 *  - toAutoClassifierInput — tools/SendMessageTool/SendMessageTool.ts compact
 *    per-message-type natural-language projection for the permission auto-classifier.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  getMailbox,
  resetAllMailboxes,
  registerMailboxNotifier,
  subscribeMailbox,
  notifyMailbox,
  drainMailboxPrioritized,
  hasOutstandingShutdownRequest,
  toAutoClassifierInput,
  purgeMailbox,
  TEAM_LEAD_NAME,
} from '../src/tools/send-message';
import { resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';

function ctx(agentId?: string): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, agentId } as ToolContext;
}

beforeEach(() => { resetAllMailboxes(); resetAgentRegistry(); });
afterEach(() => { resetAllMailboxes(); resetAgentRegistry(); });

// ---------------------------------------------------------------------------
// Multi-subscriber change signal (subscribe/emit)
// ---------------------------------------------------------------------------
describe('U022-w71 multi-subscriber mailbox change signal', () => {
  it('fires EVERY subscriber on a delivery (fan-out, not single-callback)', async () => {
    let a = 0, b = 0;
    subscribeMailbox('worker', () => { a++; });
    subscribeMailbox('worker', () => { b++; });
    await sendMessageTool.execute({ to: 'worker', content: 'hi', summary: 's' }, ctx());
    assert.equal(a, 1);
    assert.equal(b, 1);
  });

  it('unsubscribe stops a single subscriber without affecting the others', async () => {
    let a = 0, b = 0;
    const off = subscribeMailbox('worker', () => { a++; });
    subscribeMailbox('worker', () => { b++; });
    off();
    notifyMailbox('worker');
    assert.equal(a, 0, 'unsubscribed listener must not fire');
    assert.equal(b, 1);
  });

  it('coexists with the single eager-delivery notifier (both fire)', () => {
    let eager = 0, sub = 0;
    registerMailboxNotifier('worker', () => { eager++; });
    subscribeMailbox('worker', () => { sub++; });
    notifyMailbox('worker');
    assert.equal(eager, 1);
    assert.equal(sub, 1);
  });

  it('one throwing subscriber never starves the rest or breaks delivery', () => {
    let ok = 0;
    subscribeMailbox('worker', () => { throw new Error('boom'); });
    subscribeMailbox('worker', () => { ok++; });
    assert.doesNotThrow(() => notifyMailbox('worker'));
    assert.equal(ok, 1);
  });

  it('purge/reset drops subscribers (a reused name never inherits a stale observer)', () => {
    let n = 0;
    subscribeMailbox('worker', () => { n++; });
    purgeMailbox('worker');
    notifyMailbox('worker');
    assert.equal(n, 0);
  });
});

// ---------------------------------------------------------------------------
// Idempotent graceful-shutdown handshake
// ---------------------------------------------------------------------------
describe('U022-w71 idempotent shutdown handshake', () => {
  it('a re-sent shutdown_request is a no-op (not re-queued) while one is outstanding', async () => {
    const first = await sendMessageTool.execute(
      { to: 'worker', content: '{"type":"shutdown_request","reason":"wrap up"}' }, ctx());
    assert.equal(first.isError, undefined);
    assert.equal(hasOutstandingShutdownRequest('worker'), true);
    assert.equal(getMailbox('worker').length, 1);

    const second = await sendMessageTool.execute(
      { to: 'worker', content: '{"type":"shutdown_request","reason":"again"}' }, ctx());
    assert.equal(second.isError, undefined);
    assert.match(second.output, /already requested for worker/);
    assert.match(second.output, /idempotent/);
    assert.equal(getMailbox('worker').length, 1, 'no duplicate shutdown must be queued');
  });

  it('the shutdown_response resolves the handshake so a fresh request can be opened', async () => {
    await sendMessageTool.execute(
      { to: 'worker', content: '{"type":"shutdown_request"}' }, ctx());
    assert.equal(hasOutstandingShutdownRequest('worker'), true);

    // The teammate "worker" rejects (continues working); its response closes the handshake.
    const resp = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: '{"type":"shutdown_response","requestId":"shutdown-1@worker","approve":false,"reason":"still busy"}' },
      ctx('worker'),
    );
    assert.equal(resp.isError, undefined);
    assert.equal(hasOutstandingShutdownRequest('worker'), false, 'response must clear the outstanding flag');

    // A later shutdown_request to the same teammate is now allowed again.
    const again = await sendMessageTool.execute(
      { to: 'worker', content: '{"type":"shutdown_request"}' }, ctx());
    assert.doesNotMatch(again.output, /idempotent/);
    assert.equal(hasOutstandingShutdownRequest('worker'), true);
  });

  it('purge clears an outstanding shutdown-request flag', async () => {
    await sendMessageTool.execute({ to: 'worker', content: '{"type":"shutdown_request"}' }, ctx());
    assert.equal(hasOutstandingShutdownRequest('worker'), true);
    purgeMailbox('worker');
    assert.equal(hasOutstandingShutdownRequest('worker'), false);
  });
});

// ---------------------------------------------------------------------------
// Two-tier prioritized mailbox drain
// ---------------------------------------------------------------------------
describe('U022-w71 drainMailboxPrioritized (anti-starvation ordering)', () => {
  async function seed(from: string, content: string, agentId?: string) {
    // Each goes to "lead" so we control one mailbox; sender identity varies.
    await sendMessageTool.execute({ to: 'lead', content, summary: 's' }, ctx(agentId));
    return from;
  }

  it('surfaces a shutdown_request first, then team-lead messages, then peers (FIFO within tier)', async () => {
    // Register "lead" as an agent so sends resolve; use raw sends from different ids.
    // Peer chatter (from an agentId → non-lead), a lead message, and a shutdown.
    await sendMessageTool.execute({ to: 'lead', content: 'peer-one', summary: 's' }, ctx('p1'));
    await sendMessageTool.execute({ to: 'lead', content: 'from-lead', summary: 's' }, ctx()); // from = main (TEAM_LEAD_NAME)
    await sendMessageTool.execute({ to: 'lead', content: 'peer-two', summary: 's' }, ctx('p2'));
    await sendMessageTool.execute({ to: 'lead', content: '{"type":"shutdown_request"}' }, ctx('p3'));

    const drained = drainMailboxPrioritized('lead');
    const order = drained.map(m => {
      if (m.content.includes('shutdown_request')) return 'shutdown';
      if (m.from === TEAM_LEAD_NAME) return 'lead';
      return m.content;
    });
    assert.deepEqual(order, ['shutdown', 'lead', 'peer-one', 'peer-two']);
    assert.equal(getMailbox('lead').length, 0, 'drain removes every message');
  });

  it('is a no-op-shaped short-circuit for an empty or single-message mailbox', async () => {
    assert.deepEqual(drainMailboxPrioritized('nobody'), []);
    await sendMessageTool.execute({ to: 'solo', content: 'only', summary: 's' }, ctx('p1'));
    const one = drainMailboxPrioritized('solo');
    assert.equal(one.length, 1);
    assert.equal(one[0].content, 'only');
  });

  it('preserves membership — same messages as a raw drain, only reordered', async () => {
    await sendMessageTool.execute({ to: 'lead', content: 'a', summary: 's' }, ctx('p1'));
    await sendMessageTool.execute({ to: 'lead', content: '{"type":"shutdown_request"}' }, ctx('p2'));
    const drained = drainMailboxPrioritized('lead');
    const contents = new Set(drained.map(m => m.content));
    assert.equal(drained.length, 2);
    assert.ok(contents.has('a'));
    assert.ok([...contents].some(c => c.includes('shutdown_request')));
  });
});

// ---------------------------------------------------------------------------
// toAutoClassifierInput
// ---------------------------------------------------------------------------
describe('U022-w71 toAutoClassifierInput projection', () => {
  it('projects plain text as "to <recipient>: <text>"', () => {
    assert.equal(toAutoClassifierInput('hello there', 'worker'), 'to worker: hello there');
  });

  it('projects a malformed/foreign JSON payload as plain text (fail-open)', () => {
    assert.equal(toAutoClassifierInput('{not json', 'worker'), 'to worker: {not json');
  });

  it('projects structured control messages to terse intent phrases (no raw JSON)', () => {
    const cases: Array<[string, RegExp]> = [
      ['{"type":"shutdown_request"}', /^shutdown_request to worker$/],
      ['{"type":"shutdown_response","requestId":"r1","approve":true}', /^shutdown_response approve r1$/],
      ['{"type":"plan_approval_request","planContent":"do x"}', /^plan_approval_request to worker$/],
      ['{"type":"plan_approval_response","requestId":"r1","approve":false}', /^plan_approval reject to worker$/],
      ['{"type":"permission_request","tool":"bash","description":"d"}', /^permission_request "bash" to worker$/],
      ['{"type":"permission_response","requestId":"r1","approve":true}', /^permission_response grant r1$/],
      ['{"type":"sandbox_permission_request","host":"api.x"}', /^sandbox_permission_request host "api.x" to worker$/],
      ['{"type":"sandbox_permission_response","requestId":"r1","host":"api.x","allow":false}', /^sandbox_permission_response deny host "api.x"$/],
      ['{"type":"team_permission_update","toolName":"bash","behavior":"allow"}', /^team_permission_update allow "bash" to worker$/],
      ['{"type":"task_assignment","taskId":"7","subject":"s"}', /^task_assignment #7 to worker$/],
      ['{"type":"idle_notification"}', /^idle_notification to worker$/],
      ['{"type":"mode_set_request","mode":"plan"}', /^mode_set_request plan to worker$/],
    ];
    for (const [payload, re] of cases) {
      assert.match(toAutoClassifierInput(payload, 'worker'), re, payload);
    }
  });

  it('never leaks the raw JSON discriminator blob into the classifier input', () => {
    const p = '{"type":"shutdown_request","reason":"secret-reason"}';
    const out = toAutoClassifierInput(p, 'worker');
    assert.ok(!out.includes('"type"'), out);
    assert.ok(!out.includes('secret-reason'), out);
  });
});
