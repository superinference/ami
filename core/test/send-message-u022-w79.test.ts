import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import type { ToolContext } from '../src/types';
import {
  sendMessageTool,
  resetAllMailboxes,
} from '../src/tools/send-message';
import { __resetLoggerForTest } from '../src/logger';

// U022 (w79) — Cross-session (uds:) local-peer capability made DISCOVERABLE +
// OBSERVABLE.
//
// Gap: references/claude/.../tools/SendMessageTool/prompt.ts:L6-L21 — Claude's
// prompt documents the cross-session targeting affordance (a reachable peer is
// alive; "no busy state; messages enqueue and drain at the receiver's next tool
// round") so the MODEL knows it can hand a message to an agent in a separate
// session. AMI already SUPPORTS `uds:<socket-path>` delivery in execute(), but the
// tool DESCRIPTION prose never told the model about it (only a terse schema line),
// so the capability was effectively undiscoverable. This wave (a) surfaces the
// capability + its liveness/reply contract in the description, and (b) extends the
// existing gated lifecycle trace to the cross-session transport so an out-of-process
// delivery (success or failure) is observable like an in-session one.
//
// Scope note: Claude's `bridge:` remote-control peers + `ListPeers` discovery need
// a peer registry/transport AMI does not have; only the uds: local-peer half (which
// AMI genuinely implements) is documented here — bridge: still returns an explicit
// "not available in this build" error.

function ctx(agentId?: string): ToolContext {
  return { agentId } as unknown as ToolContext;
}

describe('U022 w79 — cross-session capability documented in the tool description', () => {
  const d = sendMessageTool.description;

  it('documents the uds: cross-session local-peer address', () => {
    assert.match(d, /uds:<socket-path>/);
    assert.match(d, /separate session\/process/i);
  });

  it('documents the liveness/reply contract (no busy state; drains next tool round)', () => {
    assert.match(d, /no busy state/i);
    assert.match(d, /next tool round/i);
  });

  it('documents that only plain text crosses a session boundary', () => {
    assert.match(d, /plain text crosses/i);
    assert.match(d, /session-local/i);
  });
});

describe('U022 w79 — cross-session delivery is observable via the gated trace', () => {
  let captured: string[] = [];
  let savedEnv: Record<string, string | undefined> = {};

  beforeEach(() => {
    resetAllMailboxes();
    __resetLoggerForTest();
    captured = [];
    savedEnv = {
      AMI_DEBUG_TO_STDERR: process.env.AMI_DEBUG_TO_STDERR,
      AMI_DEBUG_LOG_LEVEL: process.env.AMI_DEBUG_LOG_LEVEL,
      AMI_DEBUG_COMPONENTS: process.env.AMI_DEBUG_COMPONENTS,
      AMI_DISABLE_DEBUG_LOG: process.env.AMI_DISABLE_DEBUG_LOG,
    };
    delete process.env.AMI_DEBUG_COMPONENTS;
    delete process.env.AMI_DISABLE_DEBUG_LOG;
    process.env.AMI_DEBUG_TO_STDERR = '1';
    mock.method(process.stderr, 'write', (chunk: unknown) => {
      captured.push(String(chunk));
      return true;
    });
  });

  afterEach(() => {
    mock.restoreAll();
    for (const [k, v] of Object.entries(savedEnv)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
    __resetLoggerForTest();
    resetAllMailboxes();
  });

  function out(): string { return captured.join(''); }

  it('emits a verbose trace when a cross-session delivery FAILS', async () => {
    process.env.AMI_DEBUG_LOG_LEVEL = 'verbose';
    const r = await sendMessageTool.execute(
      { to: 'uds:/nonexistent/u022-w79/missing.sock', content: 'hi' }, ctx('agent-9'));
    assert.ok(r.isError);
    const log = out();
    assert.match(log, /cross-session delivery failed/);
  });

  it('is OFF at the default (debug) level — no cross-session trace', async () => {
    process.env.AMI_DEBUG_LOG_LEVEL = 'debug';
    await sendMessageTool.execute(
      { to: 'uds:/nonexistent/u022-w79/missing.sock', content: 'quiet' }, ctx('agent-9'));
    assert.doesNotMatch(out(), /teammate-mailbox/);
  });
});
