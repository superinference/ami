import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  getMailbox,
  resetAllMailboxes,
  registerMailboxNotifier,
  parseStructuredMessage,
  MAX_MESSAGE_CHARS,
  TEAM_LEAD_NAME,
} from '../src/tools/send-message';
import { resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';

function ctx(agentId?: string): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, agentId } as ToolContext;
}

describe('send_message additions', () => {
  beforeEach(() => { resetAllMailboxes(); resetAgentRegistry(); });
  afterEach(() => { resetAllMailboxes(); resetAgentRegistry(); });

  describe('plan_approval_request (request half)', () => {
    it('parses planContent (required) and allows an omitted requestId (auto-generated at send)', () => {
      const ok = parseStructuredMessage('{"type":"plan_approval_request","requestId":"p1","planContent":"do X","planFilePath":"/tmp/plan.md"}');
      assert.deepEqual(ok, { type: 'plan_approval_request', requestId: 'p1', planContent: 'do X', planFilePath: '/tmp/plan.md' });
      // requestId is optional on the wire for a *request* — it is auto-generated
      // before queueing (parity with Claude's generateRequestId).
      const noId = parseStructuredMessage('{"type":"plan_approval_request","planContent":"x"}');
      assert.deepEqual(noId, { type: 'plan_approval_request', requestId: undefined, planContent: 'x', planFilePath: undefined });
      const noPlan = parseStructuredMessage('{"type":"plan_approval_request","requestId":"p1"}');
      assert.ok(noPlan && 'error' in noPlan && /planContent/.test(noPlan.error));
      const blankPlan = parseStructuredMessage('{"type":"plan_approval_request","requestId":"p1","planContent":"  "}');
      assert.ok(blankPlan && 'error' in blankPlan, 'blank planContent is rejected');
    });

    it('routes a plan_approval_request to the reviewer\'s mailbox', async () => {
      const r = await sendMessageTool.execute(
        { to: 'lead-reviewer', content: '{"type":"plan_approval_request","requestId":"p1","planContent":"migrate DB","planFilePath":"/p.md"}' },
        ctx('agent-1'));
      assert.equal(r.isError, undefined);
      assert.ok(r.output.includes('Plan submitted'));
      assert.ok(r.output.includes('/p.md'));
      const box = getMailbox('lead-reviewer');
      assert.equal(box.length, 1);
      assert.equal(box[0].from, 'agent-1');
    });

    it('cannot be broadcast', async () => {
      const r = await sendMessageTool.execute(
        { to: '*', content: '{"type":"plan_approval_request","requestId":"p1","planContent":"x"}' }, ctx());
      assert.ok(r.isError && r.output.includes('cannot be broadcast'));
    });
  });

  describe('approve/approved alias parity', () => {
    it('accepts both the AMI "approve" and Claude "approved" field names', () => {
      const a = parseStructuredMessage('{"type":"plan_approval_response","requestId":"r1","approve":true}');
      assert.ok(a && !('error' in a) && a.type === 'plan_approval_response' && a.approve === true);
      const b = parseStructuredMessage('{"type":"plan_approval_response","requestId":"r1","approved":true}');
      assert.ok(b && !('error' in b) && b.type === 'plan_approval_response' && b.approve === true);
      const c = parseStructuredMessage('{"type":"shutdown_response","requestId":"r1","approved":"true"}');
      assert.ok(c && !('error' in c) && c.type === 'shutdown_response' && c.approve === true);
      const d = parseStructuredMessage('{"type":"permission_response","requestId":"r1","approved":false}');
      assert.ok(d && !('error' in d) && d.type === 'permission_response' && d.approve === false);
    });
  });

  describe('per-message content size cap', () => {
    it('rejects content larger than MAX_MESSAGE_CHARS without queueing it', async () => {
      const big = 'x'.repeat(MAX_MESSAGE_CHARS + 1);
      const r = await sendMessageTool.execute({ to: 'worker', content: big }, ctx());
      assert.ok(r.isError);
      assert.match(r.output, /too large/);
      assert.equal(getMailbox('worker').length, 0, 'oversized message is not queued');
    });

    it('accepts content exactly at the cap', async () => {
      const atCap = 'y'.repeat(MAX_MESSAGE_CHARS);
      const r = await sendMessageTool.execute({ to: 'worker', content: atCap }, ctx());
      assert.equal(r.isError, undefined);
      assert.equal(getMailbox('worker').length, 1);
    });
  });

  describe('broadcast recipient listing + empty-team handling', () => {
    it('lists recipient names in the broadcast receipt', async () => {
      registerMailboxNotifier('alice', () => {});
      registerMailboxNotifier('bob', () => {});
      // Prime mailboxes so both are broadcast targets.
      await sendMessageTool.execute({ to: 'alice', content: 'hi' }, ctx());
      await sendMessageTool.execute({ to: 'bob', content: 'hi' }, ctx());
      const r = await sendMessageTool.execute({ to: '*', content: 'all hands' }, ctx());
      assert.equal(r.isError, undefined);
      assert.ok(r.output.includes('alice'), r.output);
      assert.ok(r.output.includes('bob'), r.output);
      assert.ok(/Broadcast sent to 2 agent/.test(r.output), r.output);
    });

    it('reports a clear message when there is nobody to broadcast to', async () => {
      const r = await sendMessageTool.execute({ to: '*', content: 'anyone?' }, ctx());
      assert.equal(r.isError, undefined);
      assert.match(r.output, /No teammates to broadcast to/);
    });

    it('excludes the sender from its own broadcast', async () => {
      await sendMessageTool.execute({ to: 'peer', content: 'seed' }, ctx('agent-self'));
      // Give the sender a mailbox too.
      await sendMessageTool.execute({ to: 'agent-self', content: 'seed' }, ctx());
      const before = getMailbox('agent-self').length;
      await sendMessageTool.execute({ to: '*', content: 'broadcast' }, ctx('agent-self'));
      assert.equal(getMailbox('agent-self').length, before, 'sender does not receive its own broadcast');
    });
  });

  describe('shutdown approval actually aborts the approving agent', () => {
    function ctxWithKill(agentId: string, killed: string[]): ToolContext {
      return {
        cwd: process.cwd(),
        abortSignal: new AbortController().signal,
        agentId,
        processManager: {
          getStatus: () => 'running',
          kill: (taskId: string) => { killed.push(taskId); return true; },
        },
      } as unknown as ToolContext;
    }

    it('calls processManager.kill on the approving agent and reports the abort', async () => {
      const killed: string[] = [];
      const r = await sendMessageTool.execute(
        { to: TEAM_LEAD_NAME, content: '{"type":"shutdown_response","requestId":"r1","approve":true}' },
        ctxWithKill('agent-7', killed));
      assert.equal(r.isError, undefined);
      assert.deepEqual(killed, ['agent-7'], 'the approving agent aborts its own task');
      assert.match(r.output, /aborted/);
      // Confirmation still reaches the lead.
      assert.equal(getMailbox(TEAM_LEAD_NAME).length, 1);
    });

    it('does not abort on rejection', async () => {
      const killed: string[] = [];
      const r = await sendMessageTool.execute(
        { to: TEAM_LEAD_NAME, content: '{"type":"shutdown_response","requestId":"r1","approve":false,"reason":"still busy"}' },
        ctxWithKill('agent-7', killed));
      assert.equal(r.isError, undefined);
      assert.deepEqual(killed, [], 'a rejection never aborts');
      assert.match(r.output, /continues working/);
    });

    it('approval without a processManager still succeeds (lead-driven drain)', async () => {
      const r = await sendMessageTool.execute(
        { to: TEAM_LEAD_NAME, content: '{"type":"shutdown_response","requestId":"r1","approve":true}' },
        ctx('agent-7'));
      assert.equal(r.isError, undefined);
      assert.match(r.output, /is exiting/);
    });
  });
});
