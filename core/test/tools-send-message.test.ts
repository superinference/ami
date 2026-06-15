import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { sendMessageTool } from '../src/tools/send-message';
import { getMailbox, clearMailbox, resetAllMailboxes } from '../src/tools/send-message';
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
