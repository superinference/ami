import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as net from 'node:net';
import * as path from 'node:path';
import {
  sendMessageTool,
  getMailbox,
  resetAllMailboxes,
  registerMailboxNotifier,
  parseStructuredMessage,
  generateRequestId,
  parseRequestId,
  parseAddress,
  sendToUdsSocket,
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
// U022-F1: Structured request IDs ({requestType}-{timestamp}@{agentId})
// ---------------------------------------------------------------------------

describe('U022 request-id helpers', () => {
  it('generateRequestId produces a parseable {type}-{timestamp}@{agentId} id', () => {
    const id = generateRequestId('shutdown', 'researcher');
    assert.match(id, /^shutdown-\d+@researcher$/);
    const parsed = parseRequestId(id);
    assert.ok(parsed);
    assert.equal(parsed!.requestType, 'shutdown');
    assert.equal(parsed!.agentId, 'researcher');
    assert.equal(typeof parsed!.timestamp, 'number');
    assert.ok(!Number.isNaN(parsed!.timestamp));
  });

  it('parseRequestId preserves a requestType that itself contains a dash', () => {
    // lastIndexOf('-') splits on the FINAL dash, so a multi-word type survives.
    const parsed = parseRequestId('plan-approval-1700000000000@team-lead@proj');
    assert.ok(parsed);
    assert.equal(parsed!.requestType, 'plan-approval');
    assert.equal(parsed!.timestamp, 1700000000000);
    assert.equal(parsed!.agentId, 'team-lead@proj');
  });

  it('parseRequestId is defensive: null on missing @, missing -, or NaN timestamp', () => {
    assert.equal(parseRequestId('noseparator'), null);        // no '@'
    assert.equal(parseRequestId('nodash@agent'), null);       // prefix has no '-'
    assert.equal(parseRequestId('shutdown-notanumber@a'), null); // NaN timestamp
  });

  it('auto-generates a requestId for a shutdown_request that omits one, and queues it to the recipient', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: '{"type":"shutdown_request","reason":"wrap up"}' },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /id: shutdown-\d+@worker/);
    // The queued payload must carry the generated id so the recipient can echo it.
    const box = getMailbox('worker');
    assert.equal(box.length, 1);
    const wire = parseStructuredMessage(box[0].content);
    assert.ok(wire && !('error' in wire) && wire.type === 'shutdown_request');
    assert.match((wire as { requestId: string }).requestId, /^shutdown-\d+@worker$/);
  });

  it('preserves a caller-supplied requestId verbatim (no regeneration)', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: '{"type":"shutdown_request","requestId":"my-custom-id","reason":"x"}' },
      ctx(),
    );
    assert.match(r.output, /id: my-custom-id/);
    assert.equal(getMailbox('worker')[0].content.includes('my-custom-id'), true);
  });

  it('annotates a shutdown_response output with the parsed request origin', async () => {
    const reqId = generateRequestId('shutdown', 'worker');
    const r = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: `{"type":"shutdown_response","requestId":"${reqId}","approve":false,"reason":"still busy"}` },
      ctx('agent-9'),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /\[re: shutdown request for worker\]/);
  });

  it('omits the origin annotation for a response whose id is not a structured id', async () => {
    const r = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: '{"type":"shutdown_response","requestId":"freeform","approve":true}' },
      ctx('agent-9'),
    );
    assert.equal(r.isError, undefined);
    assert.doesNotMatch(r.output, /\[re:/);
  });
});

// ---------------------------------------------------------------------------
// U022-F2: Address schemes (uds:/bridge:/other) + out-of-process UDS delivery
// ---------------------------------------------------------------------------

describe('U022 recipient address schemes', () => {
  it('parseAddress classifies uds:/bridge:/legacy-path/other', () => {
    assert.deepEqual(parseAddress('uds:/tmp/a.sock'), { scheme: 'uds', target: '/tmp/a.sock' });
    assert.deepEqual(parseAddress('bridge:session_123'), { scheme: 'bridge', target: 'session_123' });
    assert.deepEqual(parseAddress('/tmp/legacy.sock'), { scheme: 'uds', target: '/tmp/legacy.sock' });
    assert.deepEqual(parseAddress('worker'), { scheme: 'other', target: 'worker' });
  });

  it('rejects an empty scheme target', async () => {
    const r = await sendMessageTool.execute({ to: 'uds:', content: 'hi' }, ctx());
    assert.ok(r.isError);
    assert.match(r.output, /address target must not be empty/);
  });

  it('refuses to send a structured control message cross-session', async () => {
    const r = await sendMessageTool.execute(
      { to: 'uds:/tmp/x.sock', content: '{"type":"shutdown_request","requestId":"r1"}' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.match(r.output, /cannot be sent cross-session/);
  });

  it('reports a clear boundary for a bridge: target (no transport in this build)', async () => {
    const r = await sendMessageTool.execute({ to: 'bridge:sess_1', content: 'hello' }, ctx());
    assert.ok(r.isError);
    assert.match(r.output, /bridge delivery is not available/);
  });

  it('delivers plain text to a local UDS peer out-of-process', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u022-uds-'));
    const sockPath = path.join(dir, 'peer.sock');
    const received: string[] = [];
    const server = net.createServer((conn) => {
      conn.on('data', (buf) => received.push(buf.toString('utf8')));
    });
    await new Promise<void>((resolve) => server.listen(sockPath, resolve));
    try {
      const r = await sendMessageTool.execute(
        { to: `uds:${sockPath}`, content: 'cross-session hi', summary: 'greet' },
        ctx('agent-2'),
      );
      assert.equal(r.isError, undefined);
      assert.match(r.output, /delivered to local peer/);
      // The in-session mailbox must NOT be used for a cross-session send.
      assert.equal(getMailbox(`uds:${sockPath}`).length, 0);
      // Let the frame flush to the server.
      await new Promise((res) => setTimeout(res, 50));
      const payload = received.join('');
      assert.ok(payload.includes('cross-session hi'), payload);
      const frame = JSON.parse(payload.trim());
      assert.equal(frame.from, 'agent-2');
      assert.equal(frame.text, 'cross-session hi');
      assert.equal(frame.summary, 'greet');
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('reports a delivery failure when the UDS peer socket is absent', async () => {
    const r = await sendMessageTool.execute(
      { to: 'uds:/nonexistent/u022/missing.sock', content: 'hi' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.match(r.output, /failed to deliver to local peer/);
  });

  it('sendToUdsSocket rejects on an unreachable socket', async () => {
    await assert.rejects(() => sendToUdsSocket('/nonexistent/u022/none.sock', 'x'));
  });
});

// ---------------------------------------------------------------------------
// U022-F3: Broadcast reaches the full live roster (not just prior mailboxes)
// ---------------------------------------------------------------------------

describe('U022 broadcast roster', () => {
  it('reaches a teammate that only registered a notifier (never messaged yet)', async () => {
    let woke = false;
    registerMailboxNotifier('fresh-teammate', () => { woke = true; });
    const r = await sendMessageTool.execute({ to: '*', content: 'all hands' }, ctx());
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Broadcast sent to 1 agent/);
    assert.ok(/fresh-teammate/.test(r.output));
    // The message was actually queued into its (previously nonexistent) mailbox.
    assert.equal(getMailbox('fresh-teammate').length, 1);
    assert.equal(woke, true);
  });

  it('unions notifier-only and mailbox-owning recipients without duplicates', async () => {
    registerMailboxNotifier('a', () => {});
    await sendMessageTool.execute({ to: 'a', content: 'seed' }, ctx()); // a now has a mailbox AND a notifier
    registerMailboxNotifier('b', () => {}); // b has only a notifier
    const r = await sendMessageTool.execute({ to: '*', content: 'hi' }, ctx());
    assert.match(r.output, /Broadcast sent to 2 agent/);
    assert.ok(/\ba\b/.test(r.output) && /\bb\b/.test(r.output));
    assert.equal(getMailbox('b').length, 1);
  });

  it('excludes the sender case-insensitively', async () => {
    registerMailboxNotifier('Lead', () => {});
    registerMailboxNotifier('peer', () => {});
    // Sender identity resolves to 'lead' (differs only by case from the roster entry).
    const r = await sendMessageTool.execute({ to: '*', content: 'hi', from: 'lead' }, ctx());
    assert.match(r.output, /Broadcast sent to 1 agent/);
    assert.ok(/peer/.test(r.output));
    assert.ok(!/Lead/.test(r.output), r.output);
    assert.equal(getMailbox('Lead').length, 0);
  });
});
