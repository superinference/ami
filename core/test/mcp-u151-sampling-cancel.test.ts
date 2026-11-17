/**
 * mcp-u151-sampling-cancel.test.ts — end-to-end wiring of the per-request abort
 * signal for a server-initiated sampling request.
 *
 * The common-layer sampling handler (createSamplingHandler) already accepts a
 * per-request `requestSignal` and aborts the model turn when it fires, but the
 * core MCP client/manager previously never created or forwarded such a signal:
 * `handleSampling` called `onSampling(params)` with no signal and did not register
 * the request in `_inboundAborts`, so a `notifications/cancelled` could not stop an
 * in-flight server-initiated model turn. These tests lock in the wiring:
 *   - McpManager forwards the per-request signal into the wired sampling handler.
 *   - McpClient.handleSampling creates an AbortController, forwards its signal to
 *     onSampling, aborts it on an inbound notifications/cancelled, and does NOT
 *     answer a cancelled request (MCP spec) — mirroring handleElicitation.
 *
 * RED (without the fix): the manager drops the 3rd arg (signal is undefined) and
 * handleSampling never aborts / still replies after cancellation.
 */
import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { McpClient } from '../src/mcp/client';
import { McpManager } from '../src/mcp/manager';

// --- client stdio harness (minimal, self-contained) ----------------------
const liveClients: McpClient[] = [];
afterEach(() => { while (liveClients.length) { try { liveClients.pop()!.disconnect(); } catch { /* ignore */ } } });
function track(c: McpClient): McpClient { liveClients.push(c); c.on('error', () => {}); return c; }

function stdioArgs(): string[] {
  const script = `
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    rl.on('line', (line) => {
      try {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{
            protocolVersion:'2024-11-05',
            serverInfo:{ name:'test-server', version:'1.0' },
            capabilities:{ tools:{}, resources:{}, prompts:{} } } }) + '\\n');
        }
      } catch {}
    });
    setInterval(() => {}, 60000);
  `;
  return ['-e', script];
}
async function connectedStdio(): Promise<McpClient> {
  const client = track(new McpClient({ command: 'node', args: stdioArgs(), requestTimeout: 5000, connectTimeout: 5000 }));
  await client.connect();
  assert.equal(client.state, 'ready');
  return client;
}
function feed(c: McpClient, obj: unknown): void {
  const proc = (c as unknown as { process: { stdout: NodeJS.EventEmitter } }).process;
  proc.stdout.emit('data', Buffer.from(JSON.stringify(obj) + '\n'));
}
function writeSpy(c: McpClient) {
  return mock.method((c as unknown as { writeMessage: (m: unknown) => void }), 'writeMessage');
}
function responses(spy: ReturnType<typeof writeSpy>): Array<Record<string, any>> {
  return spy.mock.calls.map((c) => c.arguments[0] as Record<string, any>);
}
const tick = (ms = 20) => new Promise((r) => setTimeout(r, ms));

// --- manager fake --------------------------------------------------------
class FakeClient extends EventEmitter {
  state = 'ready';
  serverInfo: any = { name: 'fake', version: '1' };
  tools: any[] = []; resources: any[] = []; _tools: any[] = []; uptime: number | null = 1;
  onElicitation?: (req: any, signal?: AbortSignal) => Promise<any>;
  onSampling?: (req: any, signal?: AbortSignal) => Promise<any>;
  async connect() {}
  async listTools() { return this._tools; }
  async listResources() { return this.resources; }
  async readResource() { return {}; }
  async callTool() { return { ok: true }; }
  async listPrompts() { return []; }
  async getPrompt() { return { messages: [] }; }
  async ping() {}
  async reconnect() {}
  disconnect() {}
}

describe('U151 — McpManager forwards the per-request sampling signal', () => {
  it('threads the AbortSignal from client.onSampling into the wired handler', async () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    mgr._createClient = (() => fake as any);
    mgr.addServer('sa', { command: 'x' });
    try {
      const seen: Array<{ name: string; signal?: AbortSignal }> = [];
      mgr.setSamplingHandler(async (name, _req, signal) => {
        seen.push({ name, signal });
        return { role: 'assistant', content: { type: 'text', text: name }, model: 'm' };
      });
      const ac = new AbortController();
      const result = await fake.onSampling!({ messages: [] }, ac.signal);
      assert.equal(result.content.text, 'sa');
      assert.equal(seen.length, 1);
      // RED without the fix: signal is dropped by the manager → undefined.
      assert.equal(seen[0].signal, ac.signal, 'manager must forward the per-request signal');
    } finally {
      mgr.disconnectAll();
    }
  });
});

describe('U151 — McpClient.handleSampling cancellation', () => {
  it('forwards an AbortSignal to onSampling on the happy path', async () => {
    const c = await connectedStdio();
    let received: AbortSignal | undefined;
    c.onSampling = async (_req, signal) => {
      received = signal;
      return { role: 'assistant', content: { type: 'text', text: 'hi' }, model: 'm', stopReason: 'endTurn' };
    };
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', id: 51, method: 'sampling/createMessage', params: { messages: [] } });
    await tick();
    assert.equal(responses(spy).find((m) => m.id === 51)?.result.content.text, 'hi');
    // RED without the fix: handleSampling passes no signal → received is undefined.
    assert.ok(received instanceof AbortSignal, 'a per-request AbortSignal is forwarded');
    assert.equal(received!.aborted, false);
  });

  it('aborts the forwarded signal and does NOT answer when the server cancels the request', async () => {
    const c = await connectedStdio();
    let aborted = false;
    // A handler that hangs until the signal fires, then rejects (as the real
    // sampling handler does on abort). Without forwarding it would hang forever.
    c.onSampling = (_req, signal) => new Promise((_resolve, reject) => {
      signal?.addEventListener('abort', () => { aborted = true; reject(new Error('aborted')); }, { once: true });
    });
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', id: 52, method: 'sampling/createMessage', params: { messages: [] } });
    await tick();
    assert.equal(aborted, false, 'still pending before cancellation');
    // Server abandons the request.
    feed(c, { jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId: 52 } });
    await tick();
    assert.equal(aborted, true, 'the forwarded signal aborts on notifications/cancelled');
    // Per MCP spec a cancelled request is NOT answered.
    assert.equal(responses(spy).filter((m) => m.id === 52).length, 0, 'no response to a cancelled sampling request');
  });

  it('still replies normally for an uncancelled request and errors when the handler throws', async () => {
    const c = await connectedStdio();
    c.onSampling = async () => { throw new Error('model down'); };
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', id: 53, method: 'sampling/createMessage', params: { messages: [] } });
    await tick();
    assert.equal(responses(spy).find((m) => m.id === 53)?.error.message, 'model down');
  });

  it('errors -32601 when no handler is wired (signal plumbing does not change refusal)', async () => {
    const c = await connectedStdio();
    const spy = writeSpy(c);
    feed(c, { jsonrpc: '2.0', id: 54, method: 'sampling/createMessage', params: { messages: [] } });
    await tick();
    assert.equal(responses(spy).find((m) => m.id === 54)?.error.code, -32601);
  });
});
