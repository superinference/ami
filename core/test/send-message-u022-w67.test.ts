/**
 * U022 (w67) — additive structured-protocol + mailbox parity for send_message.
 *
 * New capabilities covered (all additive; Claude-parity schemas):
 *  - sandbox_permission_request / sandbox_permission_response (host-egress
 *    delegation worker→lead→worker)  — teammateMailbox.SandboxPermission*Message
 *  - team_permission_update (lead-only team-wide rule; DM or "*" broadcast)
 *    — teammateMailbox.TeamPermissionUpdateMessage
 *  - shutdown_request.from (self-describing requester identity)
 *    — createShutdownRequestMessage
 *  - shutdown_response.paneId / backendType (teardown routing on approval)
 *    — ShutdownApprovedMessage
 *  - pollMailbox(agentName, predicate) selective drain — useMailboxBridge filter
 *
 * Each feature is proven through the whole in-file pipeline the engine drives:
 * parse (schema validation) → summarize / relayIncomingMessage (the receive-side
 * router the engine calls) → execute (routing + lead-only authorization).
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  getMailbox,
  pollMailbox,
  resetAllMailboxes,
  parseStructuredMessage,
  summarizeStructuredMessage,
  relayIncomingMessage,
  isStructuredProtocolMessage,
  classifyMessageSource,
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
// sandbox_permission_request / sandbox_permission_response
// ---------------------------------------------------------------------------
describe('sandbox_permission_request / _response message', () => {
  it('parses a well-formed request (flat host) and rejects a hostless one', () => {
    const ok = parseStructuredMessage(JSON.stringify({ type: 'sandbox_permission_request', host: 'api.example.com' }));
    assert.ok(ok && !('error' in ok));
    assert.equal((ok as any).host, 'api.example.com');
    const bad = parseStructuredMessage(JSON.stringify({ type: 'sandbox_permission_request' }));
    assert.ok(bad && 'error' in bad);
  });

  it('accepts Claude\'s nested hostPattern.host form', () => {
    const m = parseStructuredMessage(JSON.stringify({
      type: 'sandbox_permission_request', hostPattern: { host: 'cdn.example.com' },
    }));
    assert.ok(m && !('error' in m));
    assert.equal((m as any).host, 'cdn.example.com');
  });

  it('parses a response with allow coerced from string, requires requestId + host', () => {
    const m = parseStructuredMessage(JSON.stringify({
      type: 'sandbox_permission_response', requestId: 'r1', host: 'h', allow: 'true',
    }));
    assert.ok(m && !('error' in m));
    assert.equal((m as any).allow, true);
    const noId = parseStructuredMessage(JSON.stringify({ type: 'sandbox_permission_response', host: 'h', allow: true }));
    assert.ok(noId && 'error' in noId);
    const noHost = parseStructuredMessage(JSON.stringify({ type: 'sandbox_permission_response', requestId: 'r', allow: true }));
    assert.ok(noHost && 'error' in noHost);
  });

  it('is classified as a system control message (not relayed as raw text)', () => {
    const c = JSON.stringify({ type: 'sandbox_permission_request', host: 'h' });
    assert.equal(isStructuredProtocolMessage(c), true);
    assert.equal(classifyMessageSource(c, 'worker'), 'system');
  });

  it('summarizes both directions concisely', () => {
    assert.equal(
      summarizeStructuredMessage(JSON.stringify({ type: 'sandbox_permission_request', host: 'h' }), 'worker'),
      '[Sandbox Permission Request] from worker — network access to "h"',
    );
    assert.equal(
      summarizeStructuredMessage(JSON.stringify({ type: 'sandbox_permission_response', requestId: 'r', host: 'h', allow: false })),
      '[Sandbox Access Denied] host "h"',
    );
  });

  it('forwards a request from a worker to the lead with an auto-generated id', async () => {
    const r = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: JSON.stringify({ type: 'sandbox_permission_request', host: 'api.io' }) },
      ctx('agent-worker'), // a worker (not the lead) forwards the blocked host up
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Sandbox network request for host "api\.io" forwarded to main \(id: sandbox-/);
    const box = getMailbox(TEAM_LEAD_NAME);
    assert.equal(box.length, 1);
    assert.ok(JSON.parse(box[0].content).requestId.startsWith('sandbox-'));
  });

  it('lets the lead answer and routes the decision back to the worker', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'sandbox_permission_response', requestId: 'sandbox-1@worker', host: 'api.io', allow: true }) },
      ctx(), // no agentId → lead
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Sandbox access to "api\.io" allowed for worker/);
    assert.equal(getMailbox('worker').length, 1);
  });

  it('refuses a non-lead answering a sandbox request', async () => {
    const r = await sendMessageTool.execute(
      { to: 'other', content: JSON.stringify({ type: 'sandbox_permission_response', requestId: 'x', host: 'h', allow: true }) },
      ctx('agent-abc'),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /only the team lead/);
  });
});

// ---------------------------------------------------------------------------
// team_permission_update
// ---------------------------------------------------------------------------
describe('team_permission_update message', () => {
  it('parses flat + Claude-nested forms and validates behavior', () => {
    const flat = parseStructuredMessage(JSON.stringify({ type: 'team_permission_update', toolName: 'bash', behavior: 'allow' }));
    assert.ok(flat && !('error' in flat));
    assert.equal((flat as any).behavior, 'allow');
    const nested = parseStructuredMessage(JSON.stringify({
      type: 'team_permission_update',
      permissionUpdate: { type: 'addRules', behavior: 'deny', rules: [{ toolName: 'web_fetch', ruleContent: 'evil.com' }] },
    }));
    assert.ok(nested && !('error' in nested));
    assert.equal((nested as any).toolName, 'web_fetch');
    assert.equal((nested as any).rule, 'evil.com');
    const badBeh = parseStructuredMessage(JSON.stringify({ type: 'team_permission_update', toolName: 'bash', behavior: 'maybe' }));
    assert.ok(badBeh && 'error' in badBeh);
    const noTool = parseStructuredMessage(JSON.stringify({ type: 'team_permission_update', behavior: 'allow' }));
    assert.ok(noTool && 'error' in noTool);
  });

  it('summarizes with rule + directory scope', () => {
    const s = summarizeStructuredMessage(JSON.stringify({
      type: 'team_permission_update', toolName: 'bash', behavior: 'allow', rule: 'ls:*', directoryPath: '/repo',
    }));
    assert.equal(s, '[Team Permission Update] allow "bash" (ls:*) in /repo');
  });

  it('is the ONLY structured message a lead may broadcast to "*"', async () => {
    // seed a roster
    await sendMessageTool.execute({ to: 'a', content: 'hi' }, ctx());
    await sendMessageTool.execute({ to: 'b', content: 'hi' }, ctx());
    const r = await sendMessageTool.execute(
      { to: '*', content: JSON.stringify({ type: 'team_permission_update', toolName: 'bash', behavior: 'deny' }) },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /broadcast to 2 agent\(s\)/);
    assert.equal(getMailbox('a').length, 2);
    assert.equal(getMailbox('b').length, 2);
  });

  it('still rejects broadcasting any OTHER structured message', async () => {
    const r = await sendMessageTool.execute(
      { to: '*', content: JSON.stringify({ type: 'mode_set_request', mode: 'plan' }) },
      ctx(),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /cannot be broadcast/);
  });

  it('refuses a non-lead sender', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'team_permission_update', toolName: 'bash', behavior: 'allow' }) },
      ctx('agent-xyz'),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /only the team lead/);
  });

  it('reports when a broadcast reaches nobody', async () => {
    const r = await sendMessageTool.execute(
      { to: '*', content: JSON.stringify({ type: 'team_permission_update', toolName: 'bash', behavior: 'allow' }) },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /No teammates to broadcast/);
  });
});

// ---------------------------------------------------------------------------
// shutdown_request.from  (self-describing requester identity)
// ---------------------------------------------------------------------------
describe('shutdown_request carries a self-describing `from`', () => {
  it('parse surfaces a present `from`', () => {
    const m = parseStructuredMessage(JSON.stringify({ type: 'shutdown_request', from: 'lead', reason: 'done' }));
    assert.ok(m && !('error' in m));
    assert.equal((m as any).from, 'lead');
  });

  it('execute stamps the trusted sender into the queued payload', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'shutdown_request', reason: 'wrap up' }) },
      ctx(), // lead
    );
    assert.equal(r.isError, undefined);
    const wire = JSON.parse(getMailbox('worker')[0].content);
    assert.equal(wire.from, TEAM_LEAD_NAME);
    assert.ok(wire.requestId.startsWith('shutdown-'));
  });

  it('summary prefers the payload `from` over the envelope sender', () => {
    const s = summarizeStructuredMessage(
      JSON.stringify({ type: 'shutdown_request', from: 'alice', reason: 'rebalance' }),
      'envelope-sender',
    );
    assert.equal(s, '[Shutdown Request from alice] rebalance');
  });
});

// ---------------------------------------------------------------------------
// shutdown_response teardown routing (paneId / backendType)
// ---------------------------------------------------------------------------
describe('shutdown_response teardown routing fields', () => {
  it('parse surfaces paneId + backendType', () => {
    const m = parseStructuredMessage(JSON.stringify({
      type: 'shutdown_response', requestId: 'r', approve: true, paneId: 'p3', backendType: 'tmux',
    }));
    assert.ok(m && !('error' in m));
    assert.equal((m as any).paneId, 'p3');
    assert.equal((m as any).backendType, 'tmux');
  });

  it('approval summary includes a teardown hint', () => {
    const s = summarizeStructuredMessage(
      JSON.stringify({ type: 'shutdown_response', requestId: 'r', approve: true, paneId: 'p3', backendType: 'tmux' }),
      'worker',
    );
    assert.equal(s, '[Shutdown Approved] worker is now exiting (teardown: tmux p3)');
  });

  it('approval without teardown fields omits the hint (regression lock)', () => {
    const s = summarizeStructuredMessage(
      JSON.stringify({ type: 'shutdown_response', requestId: 'r', approve: true }),
      'worker',
    );
    assert.equal(s, '[Shutdown Approved] worker is now exiting');
  });
});

// ---------------------------------------------------------------------------
// relayIncomingMessage — the engine's receive-side consumer handles the new types
// ---------------------------------------------------------------------------
describe('relayIncomingMessage routes the new control messages (not raw JSON)', () => {
  it('summarizes a sandbox request rather than dumping JSON', () => {
    const relay = relayIncomingMessage(JSON.stringify({ type: 'sandbox_permission_request', host: 'h' }), 'worker');
    assert.equal(relay.drop, false);
    assert.equal(relay.text, '[Sandbox Permission Request] from worker — network access to "h"');
  });

  it('summarizes a team_permission_update', () => {
    const relay = relayIncomingMessage(JSON.stringify({ type: 'team_permission_update', toolName: 'bash', behavior: 'deny' }), 'main');
    assert.equal(relay.drop, false);
    assert.equal(relay.text, '[Team Permission Update] deny "bash"');
  });
});

// ---------------------------------------------------------------------------
// pollMailbox predicate — selective drain
// ---------------------------------------------------------------------------
describe('pollMailbox(agentName, predicate)', () => {
  it('drains ALL when no predicate is given (unchanged default)', async () => {
    await sendMessageTool.execute({ to: 'w', content: 'one' }, ctx());
    await sendMessageTool.execute({ to: 'w', content: 'two' }, ctx());
    const all = pollMailbox('w');
    assert.equal(all.length, 2);
    assert.equal(pollMailbox('w').length, 0);
  });

  it('drains only matching messages and leaves the rest queued', async () => {
    await sendMessageTool.execute({ to: 'w', content: 'keep-me' }, ctx());
    await sendMessageTool.execute({ to: 'w', content: 'TAKE' }, ctx());
    await sendMessageTool.execute({ to: 'w', content: 'keep-me-too' }, ctx());
    const taken = pollMailbox('w', (m) => m.content === 'TAKE');
    assert.equal(taken.length, 1);
    assert.equal(taken[0].content, 'TAKE');
    const left = getMailbox('w');
    assert.equal(left.length, 2);
    assert.deepEqual(left.map((m) => m.content), ['keep-me', 'keep-me-too']);
  });

  it('returns empty (and mutates nothing) when the predicate matches nothing', async () => {
    await sendMessageTool.execute({ to: 'w', content: 'x' }, ctx());
    const taken = pollMailbox('w', () => false);
    assert.equal(taken.length, 0);
    assert.equal(getMailbox('w').length, 1);
  });

  it('returns empty for an unknown agent', () => {
    assert.equal(pollMailbox('nobody').length, 0);
    assert.equal(pollMailbox('nobody', () => true).length, 0);
  });
});
