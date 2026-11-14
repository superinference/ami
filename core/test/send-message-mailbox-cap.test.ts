import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  sendMessageTool,
  getMailbox,
  resetAllMailboxes,
  MAX_MAILBOX,
} from '../src/tools/send-message';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal };
}

beforeEach(() => {
  resetAllMailboxes();
});

describe('send_message — bounded mailbox (appendCapped / MAX_MAILBOX)', () => {
  it('exposes a positive integer cap', () => {
    assert.ok(Number.isInteger(MAX_MAILBOX));
    assert.ok(MAX_MAILBOX > 0);
  });

  it('caps a direct recipient mailbox at MAX_MAILBOX, dropping the oldest', async () => {
    const overflow = 5;
    const total = MAX_MAILBOX + overflow;
    for (let i = 0; i < total; i++) {
      await sendMessageTool.execute(
        { to: 'worker', content: `m${i}`, from: 'main' },
        ctx(),
      );
    }
    const box = getMailbox('worker');
    // Without the cap this would be `total` (MAX_MAILBOX + overflow).
    assert.equal(box.length, MAX_MAILBOX);
    // The oldest `overflow` entries were dropped; newest are retained in order.
    assert.equal(box[0].content, `m${overflow}`);
    assert.equal(box[box.length - 1].content, `m${total - 1}`);
  });

  it('caps the broadcast delivery path too', async () => {
    // Seed a live mailbox so broadcast has a recipient.
    await sendMessageTool.execute({ to: 'peer', content: 'seed', from: 'main' }, ctx());
    for (let i = 0; i < MAX_MAILBOX + 3; i++) {
      await sendMessageTool.execute({ to: '*', content: `b${i}`, from: 'main' }, ctx());
    }
    const box = getMailbox('peer');
    assert.equal(box.length, MAX_MAILBOX);
    // Newest broadcast survives, seed (oldest) was dropped.
    assert.equal(box[box.length - 1].content, `b${MAX_MAILBOX + 2}`);
    assert.ok(!box.some(m => m.content === 'seed'));
  });
});
