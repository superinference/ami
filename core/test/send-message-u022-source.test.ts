import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  getMailbox,
  resetAllMailboxes,
  receiveMessage,
  registerMailboxNotifier,
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
// U022 — typed MessageSource classification (parity with Claude's mailbox
// Message.source). Lets a consumer route by origin without re-parsing content.
// ---------------------------------------------------------------------------

describe('classifyMessageSource', () => {
  it('classifies plain text from the lead/main session as "user"', () => {
    assert.equal(classifyMessageSource('hello team', TEAM_LEAD_NAME), 'user');
  });

  it('classifies plain text from a non-lead agent as "teammate"', () => {
    assert.equal(classifyMessageSource('working on it', 'researcher'), 'teammate');
  });

  it('classifies work-lifecycle control messages as "task"', () => {
    assert.equal(
      classifyMessageSource(JSON.stringify({ type: 'task_assignment', taskId: '1', subject: 'do x' }), TEAM_LEAD_NAME),
      'task',
    );
    assert.equal(
      classifyMessageSource(JSON.stringify({ type: 'idle_notification', idleReason: 'available' }), 'worker'),
      'task',
    );
  });

  it('classifies other structured control messages as "system"', () => {
    for (const content of [
      JSON.stringify({ type: 'shutdown_request', requestId: 'r1' }),
      JSON.stringify({ type: 'plan_approval_response', requestId: 'p1', approve: true }),
      JSON.stringify({ type: 'permission_request', requestId: 'x1', tool: 'bash', description: 'rm' }),
      JSON.stringify({ type: 'mode_set_request', mode: 'ask' }),
    ]) {
      assert.equal(classifyMessageSource(content, 'worker'), 'system', content);
    }
  });

  it('treats a malformed structured payload as plain text (falls back by sender)', () => {
    // A recognized discriminator with a bad payload is never enqueued by execute(),
    // but the classifier must not crash on it — it falls back to the sender rule.
    assert.equal(classifyMessageSource('{"type":"mode_set_request"}', 'worker'), 'teammate');
  });
});

describe('enqueue stamps a typed source on delivered messages', () => {
  it('stamps "user" on a plain message from the lead', async () => {
    await sendMessageTool.execute!({ to: 'worker', content: 'ping', summary: 's' }, ctx());
    const box = getMailbox('worker');
    assert.equal(box.length, 1);
    assert.equal(box[0].source, 'user');
  });

  it('stamps "teammate" on a plain message from a sub-agent', async () => {
    await sendMessageTool.execute!({ to: 'lead', content: 'eta 5m', summary: 's' }, ctx('agent-1'));
    const box = getMailbox('lead');
    assert.equal(box[0].source, 'teammate');
  });

  it('stamps "task" on a task_assignment delivery', async () => {
    await sendMessageTool.execute!(
      { to: 'worker', content: JSON.stringify({ type: 'task_assignment', taskId: '7', subject: 'build' }) },
      ctx(),
    );
    assert.equal(getMailbox('worker')[0].source, 'task');
  });

  it('stamps "system" on a structured control delivery', async () => {
    await sendMessageTool.execute!(
      { to: 'worker', content: JSON.stringify({ type: 'shutdown_request' }) },
      ctx(),
    );
    assert.equal(getMailbox('worker')[0].source, 'system');
  });

  it('stamps every recipient of a broadcast', async () => {
    await sendMessageTool.execute!({ to: 'a', content: 'seed', summary: 's' }, ctx());
    await sendMessageTool.execute!({ to: 'b', content: 'seed', summary: 's' }, ctx());
    await sendMessageTool.execute!({ to: '*', content: 'all hands', summary: 's' }, ctx());
    assert.equal(getMailbox('a').at(-1)!.source, 'user');
    assert.equal(getMailbox('b').at(-1)!.source, 'user');
  });

  it('stamps a source on a message handed directly to a waiter (bypassing the queue)', async () => {
    registerMailboxNotifier('worker', () => {});
    const pending = receiveMessage('worker', () => true);
    await sendMessageTool.execute!({ to: 'worker', content: 'direct', summary: 's' }, ctx());
    const got = await pending;
    assert.equal(got.source, 'user');
  });
});
