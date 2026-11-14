import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  getMailbox,
  pollMailbox,
  resetAllMailboxes,
  registerMailboxNotifier,
  parseStructuredMessage,
  TEAM_LEAD_NAME,
} from '../src/tools/send-message';
import { resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';

function ctx(agentId?: string): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, agentId } as ToolContext;
}

describe('send_message structured protocol', () => {
  beforeEach(() => { resetAllMailboxes(); resetAgentRegistry(); });
  afterEach(() => { resetAllMailboxes(); resetAgentRegistry(); });

  it('parseStructuredMessage validates and discriminates each type', () => {
    assert.equal(parseStructuredMessage('plain text'), null);
    assert.equal(parseStructuredMessage('{"type":"unknown"}'), null);
    const sr = parseStructuredMessage('{"type":"shutdown_request","requestId":"r1"}');
    assert.deepEqual(sr, { type: 'shutdown_request', requestId: 'r1', reason: undefined });
    const bad = parseStructuredMessage('{"type":"shutdown_response"}');
    assert.ok(bad && 'error' in bad);
    // approve accepts the string "true"
    const resp = parseStructuredMessage('{"type":"shutdown_response","requestId":"r1","approve":"true"}');
    assert.deepEqual(resp, { type: 'shutdown_response', requestId: 'r1', approve: true, reason: undefined });
  });

  it('rejects "@" addresses and broadcast of structured messages', async () => {
    const at = await sendMessageTool.execute({ to: 'team@host', content: 'hi' }, ctx());
    assert.ok(at.isError && at.output.includes('@'));
    const bc = await sendMessageTool.execute({ to: '*', content: '{"type":"shutdown_request","requestId":"r1"}' }, ctx());
    assert.ok(bc.isError && bc.output.includes('cannot be broadcast'));
  });

  it('shutdown_response must target the lead and needs a reason to reject', async () => {
    const wrongTarget = await sendMessageTool.execute(
      { to: 'worker', content: '{"type":"shutdown_response","requestId":"r1","approve":true}' }, ctx('agent-1'));
    assert.ok(wrongTarget.isError && wrongTarget.output.includes(TEAM_LEAD_NAME));
    const noReason = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: '{"type":"shutdown_response","requestId":"r1","approve":false}' }, ctx('agent-1'));
    assert.ok(noReason.isError && noReason.output.includes('reason is required'));
    const ok = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: '{"type":"shutdown_response","requestId":"r1","approve":true}' }, ctx('agent-1'));
    assert.equal(ok.isError, undefined);
    assert.ok(ok.output.includes('approved'));
    assert.equal(getMailbox(TEAM_LEAD_NAME).length, 1);
  });

  it('only the team lead may approve plans or answer permission requests', async () => {
    // A teammate (has agentId) cannot approve a plan.
    const teammate = await sendMessageTool.execute(
      { to: 'worker', content: '{"type":"plan_approval_response","requestId":"p1","approve":true}' }, ctx('agent-9'));
    assert.ok(teammate.isError && teammate.output.includes('only the team lead'));
    // The lead (no agentId) can.
    const lead = await sendMessageTool.execute(
      { to: 'worker', content: '{"type":"plan_approval_response","requestId":"p1","approve":true}' }, ctx());
    assert.equal(lead.isError, undefined);
    assert.ok(lead.output.includes('approved'));
    // permission_response is lead-only too.
    const permDenied = await sendMessageTool.execute(
      { to: 'worker', content: '{"type":"permission_response","requestId":"x1","approve":true}' }, ctx('agent-9'));
    assert.ok(permDenied.isError && permDenied.output.includes('only the team lead'));
  });

  it('worker forwards a permission_request to the lead', async () => {
    const r = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: '{"type":"permission_request","requestId":"x1","tool":"bash","description":"rm -rf"}' }, ctx('agent-7'));
    assert.equal(r.isError, undefined);
    assert.ok(r.output.includes('forwarded'));
    assert.equal(getMailbox(TEAM_LEAD_NAME).length, 1);
  });

  it('anti-impersonation: sender is the trusted agent identity, not input.from', async () => {
    // A sub-agent (context.agentId set) that tries to claim from: "main" is
    // stamped with its trusted identity (the agentId) instead.
    const r = await sendMessageTool.execute(
      { to: 'other', content: 'sneaky', from: 'main', summary: 's' }, ctx('agent-5'));
    assert.equal(r.isError, undefined);
    const box = pollMailbox('other');
    assert.equal(box[0].from, 'agent-5', 'spoofed from is ignored; trusted id used');
    // The lead (no agentId) may still set from.
    const lead = await sendMessageTool.execute(
      { to: 'other', content: 'ok', from: 'main', summary: 's' }, ctx());
    assert.equal(lead.isError, undefined);
    assert.equal(pollMailbox('other')[0].from, 'main');
  });

  it('warns on an unreachable recipient when other agents are live', async () => {
    registerMailboxNotifier('alive', () => {});
    const r = await sendMessageTool.execute({ to: 'ghost', content: 'hi' }, ctx());
    assert.ok(r.output.includes('not a known/live agent'));
    // No warning when the recipient is reachable.
    const ok = await sendMessageTool.execute({ to: 'alive', content: 'hi' }, ctx());
    assert.ok(!ok.output.includes('not a known/live agent'));
  });
});
