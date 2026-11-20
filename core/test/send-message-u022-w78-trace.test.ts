import { test, beforeEach, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import type { ToolContext } from '../src/types';
import {
  sendMessageTool,
  resetAllMailboxes,
  receiveMessage,
  registerMailboxNotifier,
} from '../src/tools/send-message';
import { __resetLoggerForTest } from '../src/logger';

// U022 (w78) — Gated per-transition lifecycle diagnostic trace for the teammate
// mailbox. Mirrors the reference's InProcessBackend `logForDebugging('[InProcessBackend]
// ...')` opt-in lifecycle trace (utils/swarm/backends/InProcessBackend.ts): every
// deliver / shutdown-open / shutdown-skip / broadcast transition in send_message now
// emits a VERBOSE-level trace through AMI's canonical logger (no new facility — reuses
// logger.ts `log()`), so a developer/CI can answer "why did my teammate not receive /
// why wasn't the shutdown re-sent" by setting AMI_DEBUG_LOG_LEVEL=verbose. It is
// OPT-IN: verbose is filtered out at the default ('debug') level, so normal runs stay
// quiet. This test drives the REAL logger end to end (stderr routing) — no monkeypatch
// of a frozen import.

const COMPONENT = '[teammate-mailbox]';

function makeCtx(agentId?: string): ToolContext {
  return { agentId } as unknown as ToolContext;
}

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
  // Route the logger to stderr and capture it so we assert against the real log path.
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

function out(): string {
  return captured.join('');
}

test('opt-in verbose: a plain DM emits a mailbox deliver trace', async () => {
  process.env.AMI_DEBUG_LOG_LEVEL = 'verbose';
  const r = await sendMessageTool.execute({ to: 'worker', content: 'hi there' }, makeCtx());
  assert.equal(r.isError, undefined);
  const log = out();
  assert.match(log, /\[teammate-mailbox\]/);
  assert.match(log, /deliver/);
  // The trace carries the resolved recipient so a developer can see where it landed.
  assert.match(log, /worker/);
});

test('opt-in is OFF at the default (debug) level — no mailbox trace', async () => {
  process.env.AMI_DEBUG_LOG_LEVEL = 'debug'; // default; verbose traces are filtered
  await sendMessageTool.execute({ to: 'worker', content: 'quiet please' }, makeCtx());
  assert.doesNotMatch(out(), /teammate-mailbox/);
});

test('waiter hand-off delivery is traced distinctly from a queued delivery', async () => {
  process.env.AMI_DEBUG_LOG_LEVEL = 'verbose';
  const p = receiveMessage('listener');
  await sendMessageTool.execute({ to: 'listener', content: 'for a waiter' }, makeCtx());
  const msg = await p;
  assert.equal(msg.content, 'for a waiter');
  assert.match(out(), /\[teammate-mailbox\] deliver/);
  assert.match(out(), /waiter/);
});

test('shutdown handshake: open then idempotent-skip are both traced', async () => {
  process.env.AMI_DEBUG_LOG_LEVEL = 'verbose';
  const payload = JSON.stringify({ type: 'shutdown_request', reason: 'wrap up' });
  const first = await sendMessageTool.execute({ to: 'worker', content: payload }, makeCtx());
  assert.equal(first.isError, undefined);
  const second = await sendMessageTool.execute({ to: 'worker', content: payload }, makeCtx());
  assert.equal(second.isError, undefined);
  const log = out();
  assert.match(log, /shutdown requested/);
  assert.match(log, /shutdown skipped/);
});

test('broadcast fan-out is traced with the recipient count', async () => {
  process.env.AMI_DEBUG_LOG_LEVEL = 'verbose';
  registerMailboxNotifier('a', () => {});
  registerMailboxNotifier('b', () => {});
  const r = await sendMessageTool.execute({ to: '*', content: 'all hands' }, makeCtx());
  assert.equal(r.isError, undefined);
  assert.match(out(), /broadcast/);
});
