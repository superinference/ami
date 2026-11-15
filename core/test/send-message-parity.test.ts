import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  getMailbox,
  resetAllMailboxes,
  registerMailboxNotifier,
  TEAM_LEAD_NAME,
} from '../src/tools/send-message';
import { resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';

function ctx(agentId?: string): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, agentId } as ToolContext;
}

describe('send_message parity — recipient resolution & self-send', () => {
  beforeEach(() => { resetAllMailboxes(); resetAgentRegistry(); });
  afterEach(() => { resetAllMailboxes(); resetAgentRegistry(); });

  // -------------------------------------------------------------------------
  // FIX 1: recipient-name normalization (trim). A padded name must resolve to
  // the real teammate, not a dead whitespace-keyed mailbox; " * " must still be
  // recognized as broadcast. RED without the trim: the message is queued under
  // the padded key and the real mailbox stays empty (and " * " is treated as a
  // literal recipient, not a broadcast).
  // -------------------------------------------------------------------------
  describe('FIX 1 — recipient name is trimmed before resolution', () => {
    it('delivers a padded recipient name to the clean mailbox (not the padded key)', async () => {
      registerMailboxNotifier('worker', () => {});
      const r = await sendMessageTool.execute({ to: '  worker  ', content: 'hi' }, ctx());
      assert.equal(r.isError, undefined);
      // Fix: resolves to the clean key. Revert: lands under '  worker  '.
      assert.equal(getMailbox('worker').length, 1, 'padded name resolves to the clean mailbox');
      assert.equal(getMailbox('  worker  ').length, 0, 'nothing queued under the padded key');
    });

    it('does not warn "unreachable" for a padded name of a live agent', async () => {
      registerMailboxNotifier('worker', () => {});
      const r = await sendMessageTool.execute({ to: ' worker ', content: 'hi' }, ctx());
      // Fix: trimmed 'worker' is reachable → no warning. Revert: ' worker ' is
      // not reachable while a notifier is live → warning appears.
      assert.ok(!r.output.includes('not a known/live agent'), r.output);
    });

    it('recognizes a padded "*" as a broadcast, not a literal recipient', async () => {
      // Seed a live mailbox so broadcast has a target.
      await sendMessageTool.execute({ to: 'peer', content: 'seed', from: 'main' }, ctx());
      const r = await sendMessageTool.execute({ to: ' * ', content: 'all hands', from: 'main' }, ctx());
      assert.equal(r.isError, undefined);
      // Fix: trimmed to '*' → broadcast. Revert: literal ' * ' recipient → "Message sent".
      assert.match(r.output, /Broadcast sent/);
      assert.equal(getMailbox(' * ').length, 0, 'no literal " * " mailbox is created');
    });
  });

  // -------------------------------------------------------------------------
  // FIX 2: self-send guard. Addressing your own identity is a no-op echo; the
  // sender can be named by its trusted name or its raw agentId, and the target
  // resolves to either. RED without the guard: the message is queued into the
  // sender's own mailbox and no error is returned.
  // -------------------------------------------------------------------------
  describe('FIX 2 — cannot send a message to yourself', () => {
    it('rejects the lead messaging "main" (itself)', async () => {
      const r = await sendMessageTool.execute({ to: TEAM_LEAD_NAME, content: 'note to self' }, ctx());
      // Fix: error. Revert: queued into main's own mailbox with no error.
      assert.ok(r.isError, r.output);
      assert.match(r.output, /cannot send a message to yourself/);
      assert.equal(getMailbox(TEAM_LEAD_NAME).length, 0, 'nothing queued on a self-send');
    });

    it('rejects a sub-agent messaging its own agentId', async () => {
      const r = await sendMessageTool.execute({ to: 'agent-5', content: 'loop' }, ctx('agent-5'));
      assert.ok(r.isError, r.output);
      assert.match(r.output, /cannot send a message to yourself/);
      assert.equal(getMailbox('agent-5').length, 0);
    });

    it('rejects a self-send even when addressed with surrounding whitespace', async () => {
      // Combines FIX 1 + FIX 2: the trimmed name equals the sender.
      const r = await sendMessageTool.execute({ to: '  agent-5 ', content: 'loop' }, ctx('agent-5'));
      assert.ok(r.isError, r.output);
      assert.match(r.output, /cannot send a message to yourself/);
    });

    it('still delivers a normal message to a different teammate', async () => {
      const r = await sendMessageTool.execute({ to: 'other', content: 'hi' }, ctx('agent-5'));
      assert.equal(r.isError, undefined);
      assert.equal(getMailbox('other').length, 1);
      assert.equal(getMailbox('other')[0].from, 'agent-5');
    });

    it('does not treat a broadcast as a self-send', async () => {
      await sendMessageTool.execute({ to: 'peer', content: 'seed' }, ctx('agent-5'));
      const r = await sendMessageTool.execute({ to: '*', content: 'all' }, ctx('agent-5'));
      assert.equal(r.isError, undefined);
      assert.match(r.output, /Broadcast sent|No teammates/);
    });
  });
});
