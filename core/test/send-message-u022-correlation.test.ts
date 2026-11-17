/**
 * U022 — request/response correlation output fields on send_message results.
 *
 * Mirrors Claude Code's SendMessageToolOutput discriminator fields
 * (tools/SendMessageTool/UI.tsx L21-26): a REQUEST-type control send surfaces a
 * `request_id` + `target` so a later response can be correlated to it, and a
 * RESPONSE-type send is marked `routing: true` (a fire-and-forward routed reply)
 * so a consumer can correlate the decision and suppress re-echoing it. These ride
 * the same structured-output `metadata` channel the existing typed discriminants
 * (`deliveryWarning`, `errorCode`) already use — an ADDITIVE programmatic contract,
 * no behavior change to delivery.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  resetAllMailboxes,
  generateRequestId,
  TEAM_LEAD_NAME,
} from '../src/tools/send-message';
import { resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';

function ctx(agentId?: string): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, agentId } as ToolContext;
}

beforeEach(() => { resetAllMailboxes(); resetAgentRegistry(); });
afterEach(() => { resetAllMailboxes(); resetAgentRegistry(); });

describe('send_message correlation metadata (request side)', () => {
  it('a shutdown_request surfaces request_id + target and no routing marker', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'shutdown_request', reason: 'wrap up' }) },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    const meta = r.metadata as Record<string, unknown> | undefined;
    assert.ok(meta, 'expected correlation metadata on a shutdown_request');
    assert.equal(meta!.target, 'worker');
    // request_id is the auto-generated id echoed in the human output.
    assert.equal(typeof meta!.request_id, 'string');
    assert.match(r.output, new RegExp(String(meta!.request_id).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
    assert.equal(meta!.routing, undefined);
  });

  it('a plan_approval_request surfaces the target and the generated request_id', async () => {
    const r = await sendMessageTool.execute(
      { to: 'lead', content: JSON.stringify({ type: 'plan_approval_request', planContent: 'ship it' }) },
      ctx('agent-7'),
    );
    assert.equal(r.isError, undefined);
    const meta = r.metadata as Record<string, unknown>;
    assert.equal(meta.target, 'lead');
    assert.equal(typeof meta.request_id, 'string');
    assert.equal(meta.routing, undefined);
  });
});

describe('send_message correlation metadata (response side)', () => {
  it('a shutdown_response is marked routing:true with the echoed request_id + lead target', async () => {
    const rid = generateRequestId('shutdown', 'worker');
    const r = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: JSON.stringify({ type: 'shutdown_response', requestId: rid, approve: false, reason: 'mid task' }) },
      ctx('agent-9'),
    );
    assert.equal(r.isError, undefined);
    const meta = r.metadata as Record<string, unknown>;
    assert.equal(meta.routing, true);
    assert.equal(meta.request_id, rid);
    assert.equal(meta.target, TEAM_LEAD_NAME);
  });

  it('a plan_approval_response (lead) is marked routing:true carrying the request_id + target', async () => {
    const rid = generateRequestId('plan', 'worker');
    const r = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'plan_approval_response', requestId: rid, approve: true }) },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    const meta = r.metadata as Record<string, unknown>;
    assert.equal(meta.routing, true);
    assert.equal(meta.request_id, rid);
    assert.equal(meta.target, 'worker');
  });

  it('a permission_response (lead) is marked routing:true for suppression/correlation', async () => {
    const rid = generateRequestId('permission', 'worker');
    const r = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'permission_response', requestId: rid, approve: true }) },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    const meta = r.metadata as Record<string, unknown>;
    assert.equal(meta.routing, true);
    assert.equal(meta.request_id, rid);
    assert.equal(meta.target, 'worker');
  });

  it('plain-text sends carry no correlation metadata (no request_id/routing)', async () => {
    const r = await sendMessageTool.execute({ to: 'worker', content: 'hello there' }, ctx());
    assert.equal(r.isError, undefined);
    const meta = (r.metadata ?? {}) as Record<string, unknown>;
    assert.equal(meta.request_id, undefined);
    assert.equal(meta.routing, undefined);
  });
});
