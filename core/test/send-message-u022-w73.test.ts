/**
 * U022 wave-73 additions to the teammate send_message subsystem. Each block maps to
 * a confirmed Claude-Code parity gap that was NOT yet implemented on the file:
 *
 *  - Age-based self-healing mailbox pruning (MAILBOX_ENTRY_TTL_MS + prune-on-append) —
 *    utils/swarm/permissionSync.ts `cleanupOldResolutions` (prune entries older than
 *    maxAgeMs so a never-draining recipient that keeps being written to cannot grow
 *    the buffer without bound). AMI's in-memory mailbox previously only capped the
 *    COUNT (MAX_MAILBOX); this adds an AGE bound at the single canonical append site.
 *  - Permission-related-response classifier (isPermissionRelatedResponse) +
 *    anti-starvation priority in drainMailboxPrioritized — utils/inProcessTeammateHelpers.ts
 *    `isPermissionRelatedResponse` (special-cases ordinary tool permissions AND
 *    sandbox/network-host permissions) so a permission decision routed back to a
 *    BLOCKED worker is drained ahead of peer chatter rather than starved behind it.
 *  - Auto-derived plain-text UI preview (deriveSummary) — tools/SendMessageTool/
 *    SendMessageTool.ts summary contract: every plain-text message must carry a short
 *    5-10 word preview. Claude hard-rejects a string message with no summary; AMI's
 *    superset keeps the send working and DEFAULTS a derived preview onto the stored
 *    message (so the UI/idle-summary path always has one) instead of failing closed.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  getMailbox,
  resetAllMailboxes,
  drainMailboxPrioritized,
  isPermissionRelatedResponse,
  deriveSummary,
  MAILBOX_ENTRY_TTL_MS,
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
// Age-based self-healing mailbox pruning
// ---------------------------------------------------------------------------
describe('U022-w73 age-based mailbox pruning', () => {
  it('exposes a generous positive TTL constant', () => {
    assert.ok(typeof MAILBOX_ENTRY_TTL_MS === 'number' && MAILBOX_ENTRY_TTL_MS > 0);
  });

  it('drops entries older than the TTL on the next append to that mailbox', async () => {
    await sendMessageTool.execute({ to: 'worker', content: 'stale one', summary: 's' }, ctx());
    const box = getMailbox('worker');
    assert.equal(box.length, 1);
    // Age the queued message well past the TTL.
    box[0].timestamp = Date.now() - MAILBOX_ENTRY_TTL_MS - 1;
    // A fresh append to the same mailbox self-heals the stale backlog.
    await sendMessageTool.execute({ to: 'worker', content: 'fresh one', summary: 's' }, ctx());
    const after = getMailbox('worker');
    assert.equal(after.length, 1, 'the stale entry is pruned, only the fresh one remains');
    assert.equal(after[0].content, 'fresh one');
  });

  it('keeps entries that are within the TTL', async () => {
    await sendMessageTool.execute({ to: 'worker', content: 'recent', summary: 's' }, ctx());
    const box = getMailbox('worker');
    box[0].timestamp = Date.now() - 1000; // 1s old, well within TTL
    await sendMessageTool.execute({ to: 'worker', content: 'newer', summary: 's' }, ctx());
    const after = getMailbox('worker');
    assert.equal(after.length, 2, 'a within-TTL entry is retained');
  });
});

// ---------------------------------------------------------------------------
// Permission-related-response classifier + priority drain
// ---------------------------------------------------------------------------
describe('U022-w73 isPermissionRelatedResponse classifier', () => {
  it('recognizes tool AND sandbox permission responses, rejects everything else', () => {
    assert.equal(isPermissionRelatedResponse('{"type":"permission_response","requestId":"r1","approve":true}'), true);
    assert.equal(isPermissionRelatedResponse('{"type":"sandbox_permission_response","requestId":"r1","host":"api.x","allow":true}'), true);
    // A permission REQUEST is not a response.
    assert.equal(isPermissionRelatedResponse('{"type":"permission_request","tool":"bash","description":"d"}'), false);
    assert.equal(isPermissionRelatedResponse('{"type":"shutdown_response","requestId":"r1","approve":true}'), false);
    assert.equal(isPermissionRelatedResponse('just chatter'), false);
    assert.equal(isPermissionRelatedResponse('{not json'), false);
  });
});

describe('U022-w73 permission-response anti-starvation in prioritized drain', () => {
  it('surfaces a permission decision ahead of peer chatter (after shutdown)', async () => {
    // Worker "w" has: peer chatter, a lead message, a forwarded permission decision
    // (sent by the lead → from = TEAM_LEAD_NAME), and a shutdown_request.
    await sendMessageTool.execute({ to: 'w', content: 'peer-one', summary: 's' }, ctx('p1'));
    await sendMessageTool.execute(
      { to: 'w', content: '{"type":"permission_response","requestId":"r1","approve":true}' },
      ctx(), // from = main (lead)
    );
    await sendMessageTool.execute({ to: 'w', content: 'from-lead', summary: 's' }, ctx()); // lead plain text
    await sendMessageTool.execute({ to: 'w', content: '{"type":"shutdown_request"}' }, ctx('p2'));

    const drained = drainMailboxPrioritized('w');
    const order = drained.map(m => {
      if (m.content.includes('shutdown_request')) return 'shutdown';
      if (m.content.includes('permission_response')) return 'perm';
      if (m.content === 'from-lead') return 'lead';
      return m.content;
    });
    // shutdown first, then the permission decision, then lead, then peers.
    assert.deepEqual(order, ['shutdown', 'perm', 'lead', 'peer-one']);
    assert.equal(getMailbox('w').length, 0);
  });

  it('preserves membership — same messages as a raw drain', async () => {
    await sendMessageTool.execute({ to: 'w', content: 'a', summary: 's' }, ctx('p1'));
    await sendMessageTool.execute(
      { to: 'w', content: '{"type":"sandbox_permission_response","requestId":"r1","host":"api.x","allow":true}' },
      ctx(),
    );
    const drained = drainMailboxPrioritized('w');
    assert.equal(drained.length, 2);
  });
});

// ---------------------------------------------------------------------------
// Auto-derived plain-text UI preview
// ---------------------------------------------------------------------------
describe('U022-w73 deriveSummary preview', () => {
  it('collapses whitespace and keeps a short word-bounded preview', () => {
    const s = deriveSummary('  refactor   the\n loader   module cleanly  ');
    assert.equal(s, 'refactor the loader module cleanly');
  });

  it('caps a long message to a bounded preview', () => {
    const long = Array.from({ length: 50 }, (_, i) => `word${i}`).join(' ');
    const s = deriveSummary(long);
    assert.ok(s.length <= 80, `preview must be bounded, got ${s.length}`);
    assert.ok(s.startsWith('word0 word1'));
  });

  it('returns empty for blank content', () => {
    assert.equal(deriveSummary('   \n  '), '');
  });

  it('populates the stored message summary when the model omitted one', async () => {
    await sendMessageTool.execute({ to: 'worker', content: 'tighten the retry backoff window' }, ctx());
    const box = getMailbox('worker');
    assert.equal(box[0].summary, 'tighten the retry backoff window');
  });

  it('does not override an explicit model-supplied summary', async () => {
    await sendMessageTool.execute({ to: 'worker', content: 'tighten the retry backoff window', summary: 'retry fix' }, ctx());
    const box = getMailbox('worker');
    assert.equal(box[0].summary, 'retry fix');
  });
});
