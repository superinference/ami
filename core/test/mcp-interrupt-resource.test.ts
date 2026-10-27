import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import { McpClient } from '../src/mcp/client';
import { McpManager } from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// Repro: "mcp-interrupt" — aborting a resource read must take effect PROMPTLY
// instead of stalling until requestTimeout. The abort signal must be threaded
// through mcp-resources tool -> manager.readResource -> client.readResource
// -> transport, mirroring the existing callTool path.
//
// RED (current source): manager.readResource has no signal param and
// client.readResource(uri) drops the signal, so a pending resources/read can
// only reject on requestTimeout (default 30s). The behavioral tests below hit
// a watchdog ('TIMED_OUT_no_prompt_abort') and the manager-forwarding test
// sees `undefined` instead of the AbortSignal.
// GREEN (after fix): abort rejects the read with 'MCP request aborted' almost
// immediately, and the manager forwards the caller's signal to the client.
// ---------------------------------------------------------------------------

/** A mock MCP HTTP server that connects normally but NEVER answers
 *  resources/read, keeping the response socket open. */
function createHangingResourceServer(pending: http.ServerResponse[]): http.Server {
  return http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      const msg = JSON.parse(body);
      if (msg.method === 'initialize') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          jsonrpc: '2.0', id: msg.id,
          result: {
            protocolVersion: '2024-11-05',
            serverInfo: { name: 'hang-mcp', version: '1.0.0' },
            capabilities: { resources: {} },
          },
        }));
        return;
      }
      if (msg.method?.startsWith('notifications/')) {
        res.writeHead(202);
        res.end();
        return;
      }
      if (msg.method === 'resources/read') {
        // Intentionally never respond — hold the connection open.
        pending.push(res);
        return;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }));
    });
  });
}

function listenOnRandom(server: http.Server): Promise<number> {
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address();
      resolve(typeof addr === 'object' ? addr!.port : 0);
    });
  });
}

function closeServer(server: http.Server): Promise<void> {
  return new Promise((resolve) => server.close(() => resolve()));
}

/** Race `promise` against a watchdog; the losing promise is pre-handled to
 *  avoid unhandled-rejection noise. Resolves/rejects with whichever wins. */
async function raceWithWatchdog<T>(promise: Promise<T>, ms = 2000): Promise<T> {
  promise.catch(() => { /* swallow if watchdog wins first */ });
  let timer: ReturnType<typeof setTimeout> | undefined;
  const watchdog = new Promise<T>((_, reject) => {
    timer = setTimeout(() => reject(new Error('TIMED_OUT_no_prompt_abort')), ms);
  });
  try {
    return await Promise.race([promise, watchdog]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

describe('MCP resource read — interrupt propagation (client/transport)', () => {
  let server: http.Server | undefined;
  let client: McpClient | undefined;
  const pending: http.ServerResponse[] = [];

  afterEach(async () => {
    for (const res of pending.splice(0)) { try { res.destroy(); } catch { /* ignore */ } }
    if (client) { client.disconnect(); client = undefined; }
    if (server) { await closeServer(server); server = undefined; }
  });

  it('rejects a mid-flight resource read PROMPTLY when the signal aborts', async () => {
    server = createHangingResourceServer(pending);
    const port = await listenOnRandom(server);
    // Large requestTimeout so the only fast path to rejection is the abort.
    client = new McpClient({ command: '', requestTimeout: 30000, connectTimeout: 5000 });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);

    const ac = new AbortController();
    const read = client.readResource('test://hang', ac.signal);
    setTimeout(() => ac.abort(), 50);

    await assert.rejects(
      raceWithWatchdog(read),
      (err: Error) => {
        assert.equal(
          err.message,
          'MCP request aborted',
          `expected prompt abort, got: ${err.message}`,
        );
        return true;
      },
    );
  });

  it('rejects immediately when the signal is already aborted at call time', async () => {
    server = createHangingResourceServer(pending);
    const port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 30000, connectTimeout: 5000 });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);

    const ac = new AbortController();
    ac.abort();
    await assert.rejects(
      raceWithWatchdog(client.readResource('test://hang', ac.signal)),
      (err: Error) => {
        assert.equal(err.message, 'MCP request aborted', `got: ${err.message}`);
        return true;
      },
    );
  });
});

describe('MCP resource read — manager forwards the abort signal', () => {
  it('threads options.signal through to client.readResource', async () => {
    const mgr = new McpManager();
    let recordedSignal: AbortSignal | undefined;
    let recordedUri: string | undefined;
    const fakeClient = {
      async readResource(uri: string, signal?: AbortSignal) {
        recordedUri = uri;
        recordedSignal = signal;
        return { contents: [{ text: 'ok' }] };
      },
    };
    // Inject a connected client without a live transport.
    (mgr as unknown as { clients: Map<string, unknown> }).clients.set('srv', fakeClient);

    const ac = new AbortController();
    const out = await mgr.readResource('srv', 'file:///a.txt', { signal: ac.signal }) as {
      contents: Array<{ text: string }>;
    };

    assert.equal(recordedUri, 'file:///a.txt');
    assert.strictEqual(recordedSignal, ac.signal,
      'manager must forward the caller AbortSignal to client.readResource');
    assert.equal(out.contents[0].text, 'ok');
  });
});
