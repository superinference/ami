import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { sendMessageTool, resetAllMailboxes, getMailbox } from '../src/tools/send-message';
import { isTerminalTaskStatus } from '../src/process-manager';
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

describe('isTerminalTaskStatus', () => {
  it('is true only for completed/failed/killed', () => {
    for (const s of ['completed', 'failed', 'killed']) assert.ok(isTerminalTaskStatus(s), s);
    for (const s of ['running', undefined, '', 'pending']) assert.ok(!isTerminalTaskStatus(s), String(s));
  });
});

describe('send_message terminal-status guard (gap: no injecting into dead teammates)', () => {
  beforeEach(() => resetAllMailboxes());

  it('refuses to send to a completed teammate', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: 'hi' },
      ctxWithStatus({ worker: 'completed' }),
    );
    assert.ok(r.isError);
    assert.match(r.output, /completed/);
    assert.match(r.output, /no longer running/);
    assert.equal(getMailbox('worker').length, 0, 'nothing should be queued for a dead teammate');
  });

  it('refuses to send to a failed or killed teammate', async () => {
    const f = await sendMessageTool.execute({ to: 'w', content: 'x' }, ctxWithStatus({ w: 'failed' }));
    assert.ok(f.isError);
    const k = await sendMessageTool.execute({ to: 'w', content: 'x' }, ctxWithStatus({ w: 'killed' }));
    assert.ok(k.isError);
  });

  it('allows sending to a running teammate', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: 'hi' },
      ctxWithStatus({ worker: 'running' }),
    );
    assert.equal(r.isError, undefined);
    assert.equal(getMailbox('worker').length, 1);
  });

  it('allows sending to an untracked name (may be coming online)', async () => {
    const r = await sendMessageTool.execute(
      { to: 'newcomer', content: 'hi' },
      ctxWithStatus({}),
    );
    assert.equal(r.isError, undefined);
    assert.equal(getMailbox('newcomer').length, 1);
  });
});
