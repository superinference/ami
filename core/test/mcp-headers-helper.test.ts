import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import { McpManager, resolveMcpServerHeaders } from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// H30/H31 (headersHelper): dynamic per-server request headers produced by an
// external shell command (git-credential-helper style), merged over the static
// `headers` (dynamic wins). A failing / malformed helper is ignored so it never
// blocks the connection. The helper is refreshed at connect time.
// ---------------------------------------------------------------------------

describe('resolveMcpServerHeaders (H30/H31)', () => {
  it('returns static headers (env-expanded) when no helper is set', async () => {
    process.env.__HH_TOKEN = 'abc';
    const out = await resolveMcpServerHeaders('s', { command: '', headers: { Authorization: 'Bearer ${__HH_TOKEN}' } });
    assert.deepEqual(out, { Authorization: 'Bearer abc' });
    delete process.env.__HH_TOKEN;
  });

  it('merges dynamic over static headers (dynamic wins)', async () => {
    const out = await resolveMcpServerHeaders(
      's',
      { command: '', url: 'https://x', headers: { Authorization: 'old', 'X-Static': '1' }, headersHelper: 'run' },
      async () => JSON.stringify({ Authorization: 'new', 'X-Dyn': '2' }),
    );
    assert.deepEqual(out, { Authorization: 'new', 'X-Static': '1', 'X-Dyn': '2' });
  });

  it('passes the server name and url to the helper env', async () => {
    let seenEnv: NodeJS.ProcessEnv = {};
    const out = await resolveMcpServerHeaders(
      'myserver',
      { command: '', url: 'https://api.example.com', headersHelper: 'run' },
      async (_cmd, env) => { seenEnv = env; return JSON.stringify({ A: 'b' }); },
    );
    assert.equal(seenEnv.AMI_MCP_SERVER_NAME, 'myserver');
    assert.equal(seenEnv.AMI_MCP_SERVER_URL, 'https://api.example.com');
    assert.deepEqual(out, { A: 'b' });
  });

  it('ignores a helper that throws (keeps static headers, never blocks)', async () => {
    const out = await resolveMcpServerHeaders(
      's',
      { command: '', headers: { Keep: '1' }, headersHelper: 'boom' },
      async () => { throw new Error('exit 1'); },
    );
    assert.deepEqual(out, { Keep: '1' });
  });

  it('rejects empty, non-object, array, and non-string-valued output', async () => {
    const base = { command: '', headers: { Keep: '1' }, headersHelper: 'run' };
    for (const bad of ['', '  ', '42', '"str"', '[1,2]', '{"a":1}', '{"a":true}', 'not json']) {
      const out = await resolveMcpServerHeaders('s', base, async () => bad);
      assert.deepEqual(out, { Keep: '1' }, `bad output ${JSON.stringify(bad)} must fall back to static`);
    }
  });
});

// Minimal fake client to verify connectServer applies resolved headers.
class FakeClient extends EventEmitter {
  onElicitation: unknown;
  onSampling: unknown;
  applied: Record<string, string> | null = null;
  setHeaders(h: Record<string, string>): void { this.applied = h; }
  async connect(): Promise<void> {}
  async connectHTTP(): Promise<void> {}
  async connectSSE(): Promise<void> {}
  async listTools(): Promise<unknown[]> { return []; }
  disconnect(): void {}
}

describe('connectServer applies headersHelper headers (H30/H31 wiring)', () => {
  let mgr: McpManager;
  beforeEach(() => { mgr = new McpManager(); });

  it('calls setHeaders with merged headers before an http connect', async () => {
    const fake = new FakeClient();
    mgr._createClient = (() => fake) as never;
    mgr.addServer('s', {
      command: '',
      transport: 'http',
      url: 'https://api.example.com',
      headers: { 'X-Static': '1' },
      // A shell command that exits non-zero prints nothing → the resolver falls
      // back to static headers, proving setHeaders was still invoked at connect.
      headersHelper: 'exit 1',
    });
    await mgr.connectServer('s');
    assert.ok(fake.applied, 'setHeaders must be called for a headersHelper server');
    assert.equal(fake.applied?.['X-Static'], '1');
  });

  it('does NOT call setHeaders for a server without a headersHelper', async () => {
    const fake = new FakeClient();
    mgr._createClient = (() => fake) as never;
    mgr.addServer('s', { command: '', transport: 'http', url: 'https://api.example.com', headers: { 'X-Static': '1' } });
    await mgr.connectServer('s');
    assert.equal(fake.applied, null);
  });
});
