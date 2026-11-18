import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { sendMessageTool, resetAllMailboxes, getMailbox } from '../src/tools/send-message';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U023 — composite-liveness boundary guard before messaging a shutting-down
// teammate (gap: "Teammate liveness check (isActive) + boundary guard before
// messaging a dead/unknown agent", claudeRef swarm/backends/types.ts:L279-L300).
//
// The coarse terminal-status guard refuses only completed/failed/killed targets.
// A tracked agent that was SIGNALLED TO ABORT but whose status has not flipped
// yet still reads `running`, so a message would queue into a mailbox the aborting
// engine never drains — a silent orphan. send_message now consults the composite
// ProcessManager.isActive() (parity with Claude's InProcessBackend.isActive) and
// refuses a running-but-inactive target.
// ---------------------------------------------------------------------------

/** Mock context exposing getStatus + an isActive verdict per taskId. */
function ctx(statuses: Record<string, string>, active: Record<string, boolean>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    processManager: {
      getStatus: (taskId: string) => statuses[taskId],
      isActive: (taskId: string) => active[taskId] ?? false,
    },
  } as unknown as ToolContext;
}

describe('send_message composite-liveness guard (mock)', () => {
  beforeEach(() => resetAllMailboxes());

  it('refuses a running-but-inactive (aborting) teammate and queues nothing', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: 'hi' },
      ctx({ worker: 'running' }, { worker: false }),
    );
    assert.ok(r.isError, 'should be an error');
    assert.match(r.output, /shutting down/);
    assert.match(r.output, /no longer accepting messages/);
    assert.equal(getMailbox('worker').length, 0, 'nothing queued for an aborting teammate');
  });

  it('allows a running AND active teammate (regression)', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: 'hi' },
      ctx({ worker: 'running' }, { worker: true }),
    );
    assert.equal(r.isError, undefined);
    assert.equal(getMailbox('worker').length, 1);
  });

  it('allows a pending teammate even when isActive is false (it may be about to start)', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: 'hi' },
      ctx({ worker: 'pending' }, { worker: false }),
    );
    assert.equal(r.isError, undefined);
    assert.equal(getMailbox('worker').length, 1);
  });

  it('leaves an untracked name to the coming-online path (isActive not consulted)', async () => {
    const r = await sendMessageTool.execute(
      { to: 'newcomer', content: 'hi' },
      ctx({}, {}),
    );
    assert.equal(r.isError, undefined);
    assert.equal(getMailbox('newcomer').length, 1);
  });

  it('does not break a manager without isActive (running stays allowed)', async () => {
    const legacy = {
      cwd: process.cwd(),
      abortSignal: new AbortController().signal,
      processManager: { getStatus: () => 'running' },
    } as unknown as ToolContext;
    const r = await sendMessageTool.execute({ to: 'worker', content: 'hi' }, legacy);
    assert.equal(r.isError, undefined);
    assert.equal(getMailbox('worker').length, 1);
  });
});

describe('send_message composite-liveness guard (real ProcessManager integration)', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  beforeEach(() => {
    resetAllMailboxes();
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u023-'));
    pm = new ProcessManager(tmpDir);
  });
  afterEach(() => {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* best-effort */ }
  });

  it('refuses a registered agent whose abort signal fired (running, aborting) end-to-end', async () => {
    const ac = new AbortController();
    pm.registerAgent('worker', {
      command: 'agent',
      description: 'a worker',
      outputPath: path.join(tmpDir, 'worker.out'),
      abortFn: () => {},
      abortSignal: ac.signal,
    });
    // Live agent: message is accepted.
    assert.equal(pm.isActive('worker'), true);
    const ok = await sendMessageTool.execute(
      { to: 'worker', content: 'first' },
      { cwd: tmpDir, abortSignal: new AbortController().signal, processManager: pm } as unknown as ToolContext,
    );
    assert.equal(ok.isError, undefined);
    assert.equal(getMailbox('worker').length, 1);

    // Signal abort WITHOUT flipping status (status is still 'running'): the
    // composite liveness query now reports it inactive and the send is refused.
    ac.abort();
    assert.equal(pm.getStatus('worker'), 'running');
    assert.equal(pm.isActive('worker'), false);
    const refused = await sendMessageTool.execute(
      { to: 'worker', content: 'second' },
      { cwd: tmpDir, abortSignal: new AbortController().signal, processManager: pm } as unknown as ToolContext,
    );
    assert.ok(refused.isError, 'aborting agent must be refused');
    assert.match(refused.output, /shutting down/);
    assert.equal(getMailbox('worker').length, 1, 'no second orphan message queued');
  });
});
