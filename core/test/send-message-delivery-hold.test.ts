import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  readDeliveryHold,
  describeDeliveryHold,
  enqueueParkedDelivery,
  registerMailboxNotifier,
  resetAllMailboxes,
  getMailbox,
  pollMailbox,
  TEAM_LEAD_NAME,
} from '../src/tools/send-message';
import type { ToolContext, DeliveryHold } from '../src/types';

// ---------------------------------------------------------------------------
// Orca parity — honest delivery-hold receipt (describeMailboxDeliveryHold):
// a send receipt must state only what is TRUE at send time. readDeliveryHold
// reads the SAME gates send_message enforces and returns a typed DeliveryHold
// (busy / blocked / detached / failed-turn / unreachable) or null when the
// recipient is settled and delivery goes through now. The hold rides the
// send_message result metadata so a frontend (ListAgents) can surface it.
// ---------------------------------------------------------------------------

/** A ToolContext whose processManager reports the given per-id status + liveness. */
function ctx(
  statuses: Record<string, string> = {},
  active: Record<string, boolean> = {},
  agentId?: string,
): ToolContext {
  return {
    cwd: process.cwd(),
    agentId,
    abortSignal: new AbortController().signal,
    processManager: {
      getStatus: (id: string) => statuses[id],
      isActive: (id: string) => active[id] ?? true,
    },
  } as unknown as ToolContext;
}

describe('readDeliveryHold — typed receipt from the same gates', () => {
  beforeEach(() => resetAllMailboxes());
  afterEach(() => resetAllMailboxes());

  it('returns null (settled) when the recipient has a live eager-delivery notifier', () => {
    registerMailboxNotifier('worker', () => {});
    const hold = readDeliveryHold('worker', 'worker', ctx({ worker: 'running' }));
    assert.equal(hold, null, 'a listening recipient delivers now — no hold');
  });

  it('reports busy for a running recipient with no live notifier', () => {
    const hold = readDeliveryHold('worker', 'worker', ctx({ worker: 'running' }));
    assert.deepEqual(hold, { kind: 'busy' });
  });

  it('reports failed-turn for a failed recipient', () => {
    assert.deepEqual(
      readDeliveryHold('worker', 'worker', ctx({ worker: 'failed' })),
      { kind: 'failed-turn' },
    );
  });

  it('reports unreachable for a killed/completed recipient (name echoed)', () => {
    assert.deepEqual(
      readDeliveryHold('worker', 'worker', ctx({ worker: 'killed' })),
      { kind: 'unreachable', recipient: 'worker' },
    );
    assert.deepEqual(
      readDeliveryHold('gone', 'gone', ctx({ gone: 'completed' })),
      { kind: 'unreachable', recipient: 'gone' },
    );
  });

  it('reports blocked (shutting down) for a running-but-inactive recipient', () => {
    const hold = readDeliveryHold('worker', 'worker', ctx({ worker: 'running' }, { worker: false }));
    assert.deepEqual(hold, { kind: 'blocked', reason: 'shutting down' });
  });

  it('reports blocked when the recipient is awaiting plan approval', async () => {
    // Open a plan-approval boundary by submitting a plan from "planner".
    await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: JSON.stringify({ type: 'plan_approval_request', planContent: 'do X' }) },
      ctx({}, {}, 'planner'),
    );
    const hold = readDeliveryHold('planner', 'planner', ctx());
    assert.deepEqual(hold, { kind: 'blocked', reason: 'awaiting plan approval' });
  });

  it('reports detached for a known/notified teammate that is not running', () => {
    // Known via a notifier presence makes it reachable; but with no running status
    // and (for this check) a notifier means settled... so use a mailbox-owner that
    // is reachable-but-not-listening: seed a mailbox, no notifier, no status.
    pollMailbox('late'); // no-op; ensure clean
    // Make "late" reachable by giving it a queued mailbox entry, without a notifier.
    enqueueParkedDelivery('late', { from: 'x', to: 'late', content: 'seed', timestamp: Date.now() });
    const hold = readDeliveryHold('late', 'late', ctx());
    assert.deepEqual(hold, { kind: 'detached' }, 'reachable but not online yet → detached');
  });

  it('reports unreachable for an unknown name when a roster/live peer exists', () => {
    registerMailboxNotifier('realworker', () => {});
    const hold = readDeliveryHold('typpo', 'typpo', ctx());
    assert.deepEqual(hold, { kind: 'unreachable', recipient: 'typpo' });
  });

  it('returns null (no honest hold) for an unknown name with no team context at all', () => {
    const hold = readDeliveryHold('whoever', 'whoever', ctx());
    assert.equal(hold, null, 'no roster, no live peers → just queues, no hold to report');
  });
});

describe('describeDeliveryHold — honest one-line receipt', () => {
  it('renders each kind with a Queued. prefix and the reason', () => {
    const cases: [DeliveryHold, RegExp][] = [
      [{ kind: 'busy' }, /Queued\. Bob is busy/],
      [{ kind: 'blocked', reason: 'awaiting plan approval' }, /blocked \(awaiting plan approval\)/],
      [{ kind: 'detached' }, /not attached\/online yet/],
      [{ kind: 'failed-turn' }, /last turn failed/],
      [{ kind: 'unreachable', recipient: 'Bob' }, /No live agent answers to "Bob"/],
    ];
    for (const [hold, re] of cases) {
      const s = describeDeliveryHold('Bob', hold);
      assert.match(s, /^Queued\. /);
      assert.match(s, re);
    }
  });
});

describe('send_message surfaces the typed hold on result metadata', () => {
  beforeEach(() => resetAllMailboxes());
  afterEach(() => resetAllMailboxes());

  it('carries a busy hold on a DM to a running-but-not-listening recipient', async () => {
    const res = await sendMessageTool.execute(
      { to: 'worker', content: 'ping' },
      ctx({ worker: 'running' }),
    );
    assert.equal(res.isError, undefined);
    assert.deepEqual(res.metadata?.deliveryHold, { kind: 'busy' });
    assert.equal(getMailbox('worker').length, 1, 'still queued — never dropped');
  });

  it('carries NO hold when the recipient is listening (settled)', async () => {
    registerMailboxNotifier('worker', () => {});
    const res = await sendMessageTool.execute(
      { to: 'worker', content: 'ping' },
      ctx({ worker: 'running' }),
    );
    assert.equal(res.metadata?.deliveryHold, undefined, 'settled → no hold on the receipt');
  });

  it('carries a failed-turn hold even on the terminal hard-reject', async () => {
    const res = await sendMessageTool.execute(
      { to: 'worker', content: 'ping' },
      ctx({ worker: 'failed' }),
    );
    assert.equal(res.isError, true);
    assert.deepEqual(res.metadata?.deliveryHold, { kind: 'failed-turn' });
  });

  it('carries an unreachable hold on the terminal hard-reject for a killed agent', async () => {
    const res = await sendMessageTool.execute(
      { to: 'worker', content: 'ping' },
      ctx({ worker: 'killed' }),
    );
    assert.equal(res.isError, true);
    assert.deepEqual(res.metadata?.deliveryHold, { kind: 'unreachable', recipient: 'worker' });
  });

  it('carries a blocked hold on the shutting-down hard-reject', async () => {
    const res = await sendMessageTool.execute(
      { to: 'worker', content: 'ping' },
      ctx({ worker: 'running' }, { worker: false }),
    );
    assert.equal(res.isError, true);
    assert.deepEqual(res.metadata?.deliveryHold, { kind: 'blocked', reason: 'shutting down' });
  });

  it('does not add deliveryHold to a plain no-team-context send (regression)', async () => {
    const res = await sendMessageTool.execute({ to: 'solo', content: 'hi' }, ctx());
    assert.equal(res.isError, undefined);
    assert.equal(res.metadata, undefined, 'no hold, no warning, no metadata churn');
  });
});
