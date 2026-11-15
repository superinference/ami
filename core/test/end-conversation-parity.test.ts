import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  endConversationTool,
  resetEndedConversations,
  isConversationEnded,
  countPendingWork,
} from '../src/tools/end-conversation';
import type { ToolContext } from '../src/types';

function ctx(overrides: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

describe('end-conversation parity (additive superset)', () => {
  beforeEach(() => resetEndedConversations());

  // --- Fix 1: idempotent end (shutdown handshake no-op) ---
  describe('idempotency', () => {
    it('second end on the same session is a no-op acknowledgment', async () => {
      const c = ctx({ sessionId: 'sess-A' });
      const first = await endConversationTool.execute({ reason: 'abuse' }, c);
      assert.equal(first.metadata?.alreadyEnded, undefined);
      assert.equal(first.output, 'Conversation ended: abuse');

      const second = await endConversationTool.execute({ reason: 'abuse' }, c);
      assert.equal(second.metadata?.alreadyEnded, true);
      assert.equal(second.output, 'Conversation already ended: abuse');
      assert.equal(second.metadata?.endConversation, true);
    });

    it('agentId distinguishes sub-engine sessions sharing a sessionId', async () => {
      const a = ctx({ sessionId: 'S', agentId: 'agent-1' });
      const b = ctx({ sessionId: 'S', agentId: 'agent-2' });
      await endConversationTool.execute({ reason: 'x' }, a);
      const r = await endConversationTool.execute({ reason: 'x' }, b);
      assert.equal(r.metadata?.alreadyEnded, undefined);
    });

    it('contexts without a sessionId never dedup (legacy single-shot)', async () => {
      const c = ctx();
      const r1 = await endConversationTool.execute({ reason: 'y' }, c);
      const r2 = await endConversationTool.execute({ reason: 'y' }, c);
      assert.equal(r1.metadata?.alreadyEnded, undefined);
      assert.equal(r2.metadata?.alreadyEnded, undefined);
    });

    it('isConversationEnded reflects tracked state', async () => {
      const c = ctx({ sessionId: 'sess-B' });
      assert.equal(isConversationEnded(c), false);
      await endConversationTool.execute({ reason: 'z' }, c);
      assert.equal(isConversationEnded(c), true);
      assert.equal(isConversationEnded(ctx()), false);
    });
  });

  // --- Fix 2: active-work guard ---
  describe('active-work guard', () => {
    it('refuses to end while caller-declared work is pending', async () => {
      const r = await endConversationTool.execute(
        { reason: 'done', pendingWork: 3 },
        ctx(),
      );
      assert.equal(r.isError, true);
      assert.equal(r.metadata?.blocked, true);
      assert.equal(r.metadata?.endConversation, false);
      assert.equal(r.metadata?.pendingWork, 3);
      assert.match(r.output, /pending work/i);
    });

    it('a blocked attempt does NOT mark the session ended', async () => {
      const c = ctx({ sessionId: 'sess-C' });
      await endConversationTool.execute({ reason: 'r', pendingWork: 1 }, c);
      assert.equal(isConversationEnded(c), false);
    });

    it('counts still-running background processes from the ProcessManager', async () => {
      const pm = { runningCount: () => 2 } as unknown as ToolContext['processManager'];
      const c = ctx({ processManager: pm });
      const r = await endConversationTool.execute({ reason: 'r' }, c);
      assert.equal(r.isError, true);
      assert.equal(r.metadata?.pendingWork, 2);
    });

    it('countPendingWork sums declared + running and tolerates a throwing PM', () => {
      const throwingPm = {
        runningCount: () => {
          throw new Error('boom');
        },
      } as unknown as ToolContext['processManager'];
      assert.equal(countPendingWork({ pendingWork: 2 }, ctx()), 2);
      assert.equal(countPendingWork({ pendingWork: true }, ctx()), 1);
      assert.equal(
        countPendingWork({ pendingWork: 1 }, ctx({ processManager: throwingPm })),
        1,
      );
    });

    it('force:true overrides the guard and ends, recording forced metadata', async () => {
      const r = await endConversationTool.execute(
        { reason: 'urgent', pendingWork: 5, force: true },
        ctx(),
      );
      assert.equal(r.isError, undefined);
      assert.equal(r.metadata?.endConversation, true);
      assert.equal(r.metadata?.forced, true);
      assert.equal(r.metadata?.pendingWork, 5);
      assert.equal(r.metadata?.graceful, false);
    });
  });

  // --- Fix 3: final-state flush ---
  describe('final-state flush', () => {
    it('emits a terminal progress line when the host wired onProgress', async () => {
      const seen: string[] = [];
      const c = ctx({ onProgress: (d) => seen.push(d) });
      await endConversationTool.execute({ reason: 'bye' }, c);
      assert.equal(seen.length, 1);
      assert.match(seen[0], /Ending conversation/);
    });

    it('a throwing onProgress sink does not break shutdown', async () => {
      const c = ctx({
        onProgress: () => {
          throw new Error('sink down');
        },
      });
      const r = await endConversationTool.execute({ reason: 'bye' }, c);
      assert.equal(r.metadata?.endConversation, true);
    });

    it('stamps an ISO endedAt timestamp and graceful flag in metadata', async () => {
      const r = await endConversationTool.execute({ reason: 'ok' }, ctx());
      assert.equal(typeof r.metadata?.endedAt, 'string');
      assert.ok(!Number.isNaN(Date.parse(r.metadata?.endedAt as string)));
      assert.equal(r.metadata?.graceful, true);
    });

    it('carries summary/handoff notes in output and metadata', async () => {
      const r = await endConversationTool.execute(
        { reason: 'handing off', summary: 'tests green; deploy pending' },
        ctx(),
      );
      assert.match(r.output, /Summary: tests green; deploy pending/);
      assert.equal(r.metadata?.summary, 'tests green; deploy pending');
    });
  });

  // --- Fix 4: shutdown handshake acknowledgment ---
  describe('shutdown handshake', () => {
    it('acknowledges a shutdownRequest in output and metadata', async () => {
      const r = await endConversationTool.execute(
        { reason: 'graceful stop', shutdownRequest: true },
        ctx(),
      );
      assert.match(r.output, /shutdown acknowledged/);
      assert.equal(r.metadata?.shutdownAcknowledged, true);
    });

    it('omits the acknowledgment when no shutdownRequest is sent', async () => {
      const r = await endConversationTool.execute({ reason: 'plain' }, ctx());
      assert.equal(r.output, 'Conversation ended: plain');
      assert.equal(r.metadata?.shutdownAcknowledged, undefined);
    });
  });

  // --- Backward-compatibility regression guards (pre-existing behavior) ---
  describe('backward compatibility', () => {
    it('base output and metadata shape unchanged', async () => {
      const r = await endConversationTool.execute({ reason: 'Abusive language' }, ctx());
      assert.equal(r.output, 'Conversation ended: Abusive language');
      assert.equal(r.metadata?.endConversation, true);
      assert.equal(r.metadata?.reason, 'Abusive language');
      assert.equal(r.isError, undefined);
    });

    it('defaults empty reason to Policy violation', async () => {
      const r = await endConversationTool.execute({ reason: '' }, ctx());
      assert.equal(r.output, 'Conversation ended: Policy violation');
      assert.equal(r.metadata?.reason, 'Policy violation');
    });

    it('still read-only and requires reason', () => {
      assert.equal(endConversationTool.isReadOnly, true);
      assert.deepEqual(endConversationTool.inputSchema.required, ['reason']);
    });
  });
});
