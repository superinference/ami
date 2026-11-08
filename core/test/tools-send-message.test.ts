import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { sendMessageTool } from '../src/tools/send-message';
import { getMailbox, clearMailbox, resetAllMailboxes, registerMailboxNotifier, unregisterMailboxNotifier } from '../src/tools/send-message';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
  };
}

beforeEach(() => {
  resetAllMailboxes();
});

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('sendMessageTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(sendMessageTool.name, 'send_message');
  });

  it('is not read-only', () => {
    assert.equal(sendMessageTool.isReadOnly, false);
  });

  it('requires to and content', () => {
    assert.ok(sendMessageTool.inputSchema.required?.includes('to'));
    assert.ok(sendMessageTool.inputSchema.required?.includes('content'));
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe('sendMessageTool – validation', () => {
  it('rejects empty to', async () => {
    const result = await sendMessageTool.execute({ to: '', content: 'hi' }, ctx());
    assert.ok(result.isError);
    assert.ok(result.output.includes('"to"'));
  });

  it('rejects empty content', async () => {
    const result = await sendMessageTool.execute({ to: 'agent-1', content: '' }, ctx());
    assert.ok(result.isError);
    assert.ok(result.output.includes('"content"'));
  });
});

// ---------------------------------------------------------------------------
// Functionality
// ---------------------------------------------------------------------------

describe('sendMessageTool – messaging', () => {
  it('sends a message and stores in mailbox', async () => {
    resetAllMailboxes();
    const result = await sendMessageTool.execute(
      { to: 'agent-1', content: 'hello agent', from: 'main' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Message sent'));
    assert.ok(result.output.includes('agent-1'));

    const mbox = getMailbox('agent-1');
    assert.equal(mbox.length, 1);
    assert.equal(mbox[0].content, 'hello agent');
    assert.equal(mbox[0].from, 'main');
    assert.equal(mbox[0].to, 'agent-1');
  });

  it('defaults from to "main"', async () => {
    resetAllMailboxes();
    await sendMessageTool.execute({ to: 'agent-2', content: 'test' }, ctx());
    const mbox = getMailbox('agent-2');
    assert.equal(mbox[0].from, 'main');
  });

  it('queues multiple messages', async () => {
    resetAllMailboxes();
    await sendMessageTool.execute({ to: 'agent-3', content: 'msg1' }, ctx());
    await sendMessageTool.execute({ to: 'agent-3', content: 'msg2' }, ctx());
    await sendMessageTool.execute({ to: 'agent-3', content: 'msg3' }, ctx());
    assert.equal(getMailbox('agent-3').length, 3);
  });

  it('clears mailbox', async () => {
    resetAllMailboxes();
    await sendMessageTool.execute({ to: 'agent-4', content: 'hi' }, ctx());
    assert.equal(getMailbox('agent-4').length, 1);
    clearMailbox('agent-4');
    assert.equal(getMailbox('agent-4').length, 0);
  });

  it('resetAllMailboxes clears everything', async () => {
    await sendMessageTool.execute({ to: 'a', content: 'x' }, ctx());
    await sendMessageTool.execute({ to: 'b', content: 'y' }, ctx());
    resetAllMailboxes();
    assert.equal(getMailbox('a').length, 0);
    assert.equal(getMailbox('b').length, 0);
  });
});

describe('eager-delivery notifiers (F3)', () => {
  beforeEach(() => resetAllMailboxes());

  it('fires a registered notifier synchronously on send, and the message is queued', async () => {
    let fired = 0;
    registerMailboxNotifier('agent-x', () => { fired++; });
    await sendMessageTool.execute!({ to: 'agent-x', content: 'hi' }, ctx());
    assert.equal(fired, 1, 'notifier must fire on delivery');
    assert.equal(getMailbox('agent-x').length, 1, 'message still queued for pull');
  });

  it('does not fire after unregister', async () => {
    let fired = 0;
    registerMailboxNotifier('agent-x', () => { fired++; });
    unregisterMailboxNotifier('agent-x');
    await sendMessageTool.execute!({ to: 'agent-x', content: 'hi' }, ctx());
    assert.equal(fired, 0);
  });

  it('notifies each registered recipient on broadcast', async () => {
    let a = 0, b = 0;
    // broadcast only reaches agents that already have a mailbox
    getMailbox('a'); await sendMessageTool.execute!({ to: 'a', content: 'seed' }, ctx());
    getMailbox('b'); await sendMessageTool.execute!({ to: 'b', content: 'seed' }, ctx());
    registerMailboxNotifier('a', () => { a++; });
    registerMailboxNotifier('b', () => { b++; });
    await sendMessageTool.execute!({ to: '*', content: 'broadcast', from: 'main' }, ctx());
    assert.equal(a, 1);
    assert.equal(b, 1);
  });

  it('a notifier that throws never breaks delivery', async () => {
    registerMailboxNotifier('agent-x', () => { throw new Error('boom'); });
    const r = await sendMessageTool.execute!({ to: 'agent-x', content: 'hi' }, ctx());
    assert.equal(r.isError, undefined);
    assert.equal(getMailbox('agent-x').length, 1);
  });

  it('resetAllMailboxes clears notifiers', async () => {
    let fired = 0;
    registerMailboxNotifier('agent-x', () => { fired++; });
    resetAllMailboxes();
    await sendMessageTool.execute!({ to: 'agent-x', content: 'hi' }, ctx());
    assert.equal(fired, 0);
  });
});
