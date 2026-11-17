import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  resetAllMailboxes,
  registerMailboxNotifier,
} from '../src/tools/send-message';
import type { ToolContext } from '../src/types';

function ctxWithStatus(statuses: Record<string, string>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    processManager: {
      getStatus: (taskId: string) => statuses[taskId],
    },
  } as unknown as ToolContext;
}

function bareCtx(): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
  } as unknown as ToolContext;
}

// U022 / w70 — delivery-state affordance on the plain-text receipt.
// Mirrors Claude Code's call() branching (SendMessageTool.ts L800-874): a message
// to a RUNNING agent is queued for its next tool round, and a message to an agent
// that is actively listening (eager-delivery notifier registered) is delivered
// now. AMI surfaces which of the two happened so the sender knows whether the
// recipient will pick the message up immediately or on its next round.
describe('send_message delivery-state affordance (U022/w70)', () => {
  beforeEach(() => resetAllMailboxes());

  it('tells the sender a message to a running teammate is queued for its next tool round', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: 'please run the tests' },
      ctxWithStatus({ worker: 'running' }),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Message sent to "worker"/);
    assert.match(r.output, /queued for worker's next tool round/);
  });

  it('tells the sender a message to a listening teammate was delivered now', async () => {
    let notified = 0;
    registerMailboxNotifier('live', () => { notified++; });
    const r = await sendMessageTool.execute(
      { to: 'live', content: 'heads up' },
      bareCtx(),
    );
    assert.equal(r.isError, undefined);
    assert.equal(notified, 1, 'the eager-delivery notifier must have fired');
    assert.match(r.output, /was notified/);
    // A listening recipient takes precedence over the plain "queued" wording.
    assert.doesNotMatch(r.output, /next tool round/);
  });

  it('adds no delivery-state affordance for an untracked recipient', async () => {
    const r = await sendMessageTool.execute(
      { to: 'ghost', content: 'anyone there?' },
      bareCtx(),
    );
    assert.equal(r.isError, undefined);
    assert.doesNotMatch(r.output, /next tool round|was notified/);
  });

  it('does not add the plain-text affordance to a broadcast receipt', async () => {
    registerMailboxNotifier('a', () => {});
    registerMailboxNotifier('b', () => {});
    const r = await sendMessageTool.execute(
      { to: '*', content: 'team update' },
      bareCtx(),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Broadcast sent to 2 agent/);
    assert.doesNotMatch(r.output, /next tool round|was notified/);
  });

  it('does not add the plain-text affordance to a structured-message receipt', async () => {
    registerMailboxNotifier('worker', () => {});
    const r = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'shutdown_request', reason: 'done' }) },
      bareCtx(),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Shutdown request sent to worker/);
    assert.doesNotMatch(r.output, /next tool round|was notified/);
  });
});
