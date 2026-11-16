import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient, MCP_STDERR_TAIL_LINES } from '../src/mcp/client';

after(() => { setTimeout(() => process.exit(0), 200); });

// A stdio MCP server that completes the handshake, then on tools/call writes a
// few lines to stderr and exits NON-ZERO without answering the call — so the
// pending callTool rejects with the close-built exit message.
const stderrExitServer = `
  const readline = require('readline');
  const rl = readline.createInterface({ input: process.stdin });
  rl.on('line', (line) => {
    try {
      const msg = JSON.parse(line);
      if (msg.method === 'initialize') {
        process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} } }) + '\\n');
      } else if (msg.method === 'tools/call') {
        for (let i = 0; i < 12; i++) process.stderr.write('L' + i + '\\n');
        process.exit(3);
      }
    } catch {}
  });
`;

// Handshakes, then on tools/call kills ITSELF with SIGKILL so the exit code is
// null and the message must fall back to naming the signal.
const signalExitServer = `
  const readline = require('readline');
  const rl = readline.createInterface({ input: process.stdin });
  rl.on('line', (line) => {
    try {
      const msg = JSON.parse(line);
      if (msg.method === 'initialize') {
        process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} } }) + '\\n');
      } else if (msg.method === 'tools/call') {
        process.kill(process.pid, 'SIGKILL');
      }
    } catch {}
  });
`;

// Handshakes, then on notifications/initialized pushes TWO server→client
// notifications in a single write: a notifications/message (emits 'log') and a
// notifications/resources/updated (emits 'resource-updated').
const twoNotificationsServer = `
  const readline = require('readline');
  const rl = readline.createInterface({ input: process.stdin });
  rl.on('line', (line) => {
    try {
      const msg = JSON.parse(line);
      if (msg.method === 'initialize') {
        process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} } }) + '\\n');
      } else if (msg.method === 'notifications/initialized') {
        process.stdout.write(
          JSON.stringify({ jsonrpc: '2.0', method: 'notifications/message', params: { level: 'info', data: 'boom' } }) + '\\n' +
          JSON.stringify({ jsonrpc: '2.0', method: 'notifications/resources/updated', params: { uri: 'file://x' } }) + '\\n'
        );
      }
    } catch {}
  });
`;

// A stdio MCP server that never answers anything — used as an inert child for
// unit-level assertions that don't need a completed handshake.
const silentServer = `
  const readline = require('readline');
  readline.createInterface({ input: process.stdin }).on('line', () => {});
`;

// ---------------------------------------------------------------------------
// GAP: Signal-code fallback + retained stderr tail in the subprocess-exit
// message (parity with useSSHSession's 'signal ...' fallback + stderr tail).
// ---------------------------------------------------------------------------
describe('stdio exit message: stderr tail + signal fallback', () => {
  it('appends the retained stderr tail (bounded) and names the exit code', async () => {
    const client = new McpClient({ command: 'node', args: ['-e', stderrExitServer] });
    await client.connect();
    let message = '';
    await assert.rejects(
      () => client.callTool('slow'),
      (err: Error) => { message = err.message; return true; },
    );
    assert.match(message, /MCP server exited with code 3/);
    // Tail is bounded to the last MCP_STDERR_TAIL_LINES lines (12 emitted → L2..L11).
    assert.match(message, /Server stderr \(code 3\):/);
    assert.match(message, /L11/);
    assert.match(message, /L2/);
    assert.doesNotMatch(message, /\bL0\b/);
    assert.equal(client.stderrTail.length, MCP_STDERR_TAIL_LINES);
    assert.ok(client.stderrTail.every((l) => /^L\d+$/.test(l)));
    client.disconnect();
  });

  it('falls back to naming the signal when killed (exit code null)', async () => {
    const client = new McpClient({ command: 'node', args: ['-e', signalExitServer] });
    await client.connect();
    await assert.rejects(
      () => client.callTool('slow'),
      /MCP server exited with signal SIGKILL/,
    );
    client.disconnect();
  });
});

// ---------------------------------------------------------------------------
// GAP: ±25% jitter on the reconnect backoff (shared by reconnect() and the
// Streamable-HTTP GET-stream reconnect) to de-synchronize lockstep reconnects.
// ---------------------------------------------------------------------------
describe('reconnectDelay jitter', () => {
  const client = new McpClient({ command: 'node', args: ['-e', silentServer] });
  const delay = (attempt: number) =>
    (client as unknown as { reconnectDelay: (n: number) => number }).reconnectDelay(attempt);

  it('stays within ±25% of the capped exponential base and varies in both directions', () => {
    const base = 1000; // attempt 0 → min(1000*2^0, 30000)
    let below = false, above = false;
    for (let i = 0; i < 400; i++) {
      const d = delay(0);
      assert.ok(d >= base * 0.75 && d <= base * 1.25, `delay ${d} out of [${base * 0.75}, ${base * 1.25}]`);
      if (d < base) below = true;
      if (d > base) above = true;
    }
    assert.ok(below && above, 'jitter should produce values both below and above the base');
  });

  it('respects the 30s cap (still jittered) for large attempt counts', () => {
    for (let i = 0; i < 100; i++) {
      const d = delay(20); // base saturates at 30000
      assert.ok(d >= 30000 * 0.75 && d <= 30000 * 1.25, `capped delay ${d} out of bounds`);
    }
  });
});

// ---------------------------------------------------------------------------
// GAP: Per-notification error boundary — a throwing notification listener (or a
// malformed notification) is isolated and cannot crash/stall the read loop.
// ---------------------------------------------------------------------------
describe('per-notification error boundary', () => {
  it('isolates a throwing listener so the next notification in the burst still processes', async () => {
    const client = new McpClient({ command: 'node', args: ['-e', twoNotificationsServer] });
    const notifErrors: Array<{ method: string; err: Error }> = [];
    let resourceUpdated: string | null = null;
    client.on('log', () => { throw new Error('listener boom'); });
    client.on('notification-error', (method: string, err: Error) => { notifErrors.push({ method, err }); });
    const gotResource = new Promise<void>((resolve) => {
      client.on('resource-updated', (uri: string) => { resourceUpdated = uri; resolve(); });
    });
    await client.connect();
    await Promise.race([
      gotResource,
      new Promise<never>((_, rej) => setTimeout(() => rej(new Error('resource-updated never arrived — read loop stalled')), 2000)),
    ]);
    // The first notification's throwing 'log' listener was caught + surfaced...
    assert.ok(notifErrors.some((e) => e.method === 'notifications/message'), 'expected a notification-error for notifications/message');
    assert.match(notifErrors[0].err.message, /listener boom/);
    // ...and the SECOND notification in the same burst was still dispatched.
    assert.equal(resourceUpdated, 'file://x');
    client.disconnect();
  });

  it('surfaces a non-Error throw as an Error on the notification-error channel', () => {
    const client = new McpClient({ command: 'node', args: ['-e', silentServer] });
    const errs: Error[] = [];
    client.on('notification-error', (_m: string, err: Error) => errs.push(err));
    client.on('resource-updated', () => { throw 'string boom'; });
    (client as unknown as { handleNotification: (n: unknown) => void }).handleNotification({
      jsonrpc: '2.0', method: 'notifications/resources/updated', params: { uri: 'file://y' },
    });
    assert.equal(errs.length, 1);
    assert.ok(errs[0] instanceof Error);
    assert.match(errs[0].message, /string boom/);
    client.disconnect();
  });
});
