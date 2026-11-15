import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  mapChildToSubagentEvent,
  createSubagentRelay,
  type RelayMessage,
} from '../src/tools/subagent-relay';
import type { EngineEvent } from '../src/types';

const map = (e: EngineEvent) => mapChildToSubagentEvent(e, 'agent-1', 2);

/* ---------------------------------------------------------------- *
 * mapChildToSubagentEvent — relay-policy parity
 * ---------------------------------------------------------------- */
describe('mapChildToSubagentEvent — parity hardening', () => {
  // FIX 1: error events are surfaced live (previously dropped → returned null).
  it('relays error events as a flagged text activity', () => {
    const r = map({ type: 'error', error: 'boom failed' });
    assert.ok(r, 'error event must be relayed, not dropped');
    assert.equal(r.type, 'subagent_progress');
    assert.equal(r.type === 'subagent_progress' && r.activity, 'text');
    assert.equal(r.type === 'subagent_progress' && r.detail, '⚠ boom failed');
    assert.equal(r.type === 'subagent_progress' && r.nodeId, 'agent-1');
    assert.equal(r.type === 'subagent_progress' && r.depth, 2);
  });

  it('collapses whitespace inside a relayed error message', () => {
    const r = map({ type: 'error', error: 'line one\n   line two' });
    assert.equal(r?.type === 'subagent_progress' && r.detail, '⚠ line one line two');
  });

  it('drops an empty / whitespace-only error', () => {
    assert.equal(map({ type: 'error', error: '' }), null);
    assert.equal(map({ type: 'error', error: '   \n\t' }), null);
  });

  // FIX 2: whitespace-only text/thinking deltas are dropped (blank roster flicker)
  // while non-whitespace deltas still relay their RAW text (spacing preserved).
  it('drops whitespace-only text/thinking deltas', () => {
    assert.equal(map({ type: 'text_delta', text: '   ' }), null);
    assert.equal(map({ type: 'text_delta', text: '\n\t ' }), null);
    assert.equal(map({ type: 'thinking_delta', text: '  ' }), null);
  });

  it('preserves raw (untrimmed) text for a non-blank delta', () => {
    const r = map({ type: 'text_delta', text: ' hello ' });
    // The drop decision trims, but the relayed detail must keep the original spacing.
    assert.equal(r?.type === 'subagent_progress' && r.detail, ' hello ');
  });
});

/* ---------------------------------------------------------------- *
 * createSubagentRelay — routing / ordering / lifecycle / dead-letters
 * ---------------------------------------------------------------- */
describe('createSubagentRelay — message routing parity', () => {
  it('registers an agent as alive/pending and reports unknown otherwise', () => {
    const r = createSubagentRelay();
    assert.equal(r.state('a'), 'unknown');
    assert.equal(r.isAlive('a'), false);
    r.register('a');
    assert.equal(r.state('a'), 'pending');
    assert.equal(r.isAlive('a'), true);
  });

  // FIX 3 (dead-agent handling): send to unknown / finished recipients is rejected
  // and parked as a dead-letter, never silently delivered.
  it('rejects + dead-letters a send to an unknown recipient', () => {
    const r = createSubagentRelay();
    const res = r.send('ghost', 'hi');
    assert.equal(res.accepted, false);
    assert.equal(res.seq, null);
    assert.equal(res.reason, 'unknown-recipient');
    assert.equal(r.pending('ghost'), 0);
    assert.equal(r.deadLetters().length, 1);
    assert.equal(r.deadLetters()[0].body, 'hi');
  });

  it('rejects + dead-letters a send to a finished recipient', () => {
    const r = createSubagentRelay();
    r.register('a');
    r.complete('a');
    const res = r.send('a', 'too late');
    assert.equal(res.accepted, false);
    assert.equal(res.reason, 'recipient-finished');
    assert.equal(r.deadLetters().some((m) => m.body === 'too late'), true);
  });

  it('accepts a send to a live recipient and queues it', () => {
    const r = createSubagentRelay();
    r.register('a');
    const res = r.send('a', 'work', 'parent');
    assert.equal(res.accepted, true);
    assert.equal(typeof res.seq, 'number');
    assert.equal(r.pending('a'), 1);
    assert.equal(r.deadLetters().length, 0);
  });

  // FIX 4 (ordering): messages are delivered FIFO in monotonic sequence order,
  // and delivery drains the mailbox.
  it('delivers queued messages FIFO and clears the queue', () => {
    const r = createSubagentRelay();
    r.register('a');
    r.send('a', 'first');
    r.send('a', 'second');
    r.send('a', 'third');
    const out = r.deliver('a');
    assert.deepEqual(out.map((m) => m.body), ['first', 'second', 'third']);
    // strictly increasing sequence numbers fix global ordering
    assert.ok(out[0].seq < out[1].seq && out[1].seq < out[2].seq);
    assert.equal(r.pending('a'), 0);
    assert.deepEqual(r.deliver('a'), []); // drained
  });

  it('assigns a GLOBAL monotonic sequence across recipients', () => {
    const r = createSubagentRelay();
    r.register('a');
    r.register('b');
    const s1 = r.send('a', 'x').seq as number;
    const s2 = r.send('b', 'y').seq as number;
    const s3 = r.send('a', 'z').seq as number;
    assert.ok(s1 < s2 && s2 < s3, 'sequence must be monotonic across all recipients');
  });

  it('returns empty delivery for unknown / drained nodes without throwing', () => {
    const r = createSubagentRelay();
    assert.deepEqual(r.deliver('nobody'), []);
    assert.equal(r.pending('nobody'), 0);
  });

  // FIX 5 (completion notification + dropped-message handling): completing a node
  // transitions its state and flushes still-queued mail into dead-letters.
  it('flushes undelivered mail to dead-letters on completion', () => {
    const r = createSubagentRelay();
    r.register('a');
    r.send('a', 'unread-1');
    r.send('a', 'unread-2');
    const done = r.complete('a', 'completed');
    assert.equal(done.transitioned, true);
    assert.equal(done.state, 'completed');
    assert.deepEqual(done.dropped.map((m: RelayMessage) => m.body), ['unread-1', 'unread-2']);
    assert.equal(r.pending('a'), 0);
    assert.equal(r.state('a'), 'completed');
    const dl = r.deadLetters().map((m) => m.body);
    assert.deepEqual(dl, ['unread-1', 'unread-2']);
  });

  it('supports a failed terminal state', () => {
    const r = createSubagentRelay();
    r.register('a');
    const done = r.complete('a', 'failed');
    assert.equal(done.state, 'failed');
    assert.equal(r.isAlive('a'), false);
    assert.equal(r.state('a'), 'failed');
  });

  // FIX 6 (idempotent completion): re-completing (or completing an unknown node)
  // is a no-op — no spurious re-notification, no double dead-lettering.
  it('is idempotent: re-completing a finished node transitions nothing', () => {
    const r = createSubagentRelay();
    r.register('a');
    r.send('a', 'orphan');
    const first = r.complete('a');
    assert.equal(first.transitioned, true);
    assert.equal(first.dropped.length, 1);
    const second = r.complete('a', 'failed');
    assert.equal(second.transitioned, false);
    assert.equal(second.dropped.length, 0);
    assert.equal(r.state('a'), 'completed'); // not downgraded to failed
    assert.equal(r.deadLetters().length, 1); // not double-counted
  });

  it('completing an unknown node is a non-transition no-op', () => {
    const r = createSubagentRelay();
    const done = r.complete('ghost');
    assert.equal(done.transitioned, false);
    assert.equal(done.dropped.length, 0);
  });

  it('re-register re-arms a finished agent for new mail', () => {
    const r = createSubagentRelay();
    r.register('a');
    r.complete('a');
    assert.equal(r.isAlive('a'), false);
    r.register('a');
    assert.equal(r.isAlive('a'), true);
    assert.equal(r.send('a', 'again').accepted, true);
  });

  it('deadLetters() returns an immutable snapshot', () => {
    const r = createSubagentRelay();
    r.send('ghost', 'x');
    const snap = r.deadLetters();
    assert.equal(snap.length, 1);
    // mutating the snapshot must not affect the relay's internal store
    (snap as RelayMessage[]).push({ to: 'z', from: 'q', body: 'injected', seq: 999 });
    assert.equal(r.deadLetters().length, 1);
  });

  it('defaults the sender to "parent" when omitted', () => {
    const r = createSubagentRelay();
    r.register('a');
    r.send('a', 'hi');
    assert.equal(r.deliver('a')[0].from, 'parent');
  });
});
