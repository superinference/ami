import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  boundMcpResponse,
  validateMcpBufferSize,
  DEFAULT_MCP_MAX_RESPONSE_BYTES,
  MCP_MAX_STDIO_BUFFER,
  MCP_MAX_PAGES,
  MCP_TRANSPORTS,
  MCP_CLIENT_STATES,
  MCP_TERMINAL_ERROR_CODES,
  MCP_PROTOCOL_VERSION,
  McpClient,
  McpSessionExpiredError,
} from '../src/mcp/client';
import {
  matchMcpAutoAllow,
  McpManager,
  MCP_SERVER_STATES,
} from '../src/mcp/manager';

// ===========================================================================
// POV: MCP client.ts + manager.ts PURE parts — behavioral regression matrix.
// Every expectation is computed by calling the real code (no source-text reads).
// ===========================================================================

const MIN_BUFFER = 1024;
const MAX_BUFFER = 100 * 1024 * 1024;

describe('POV/mcp — boundMcpResponse (response-size DoS guard)', () => {
  it('passes content through untouched when under the cap', () => {
    for (const s of ['', 'a', 'hello world', 'x'.repeat(999)]) {
      const r = boundMcpResponse(s, 1024);
      assert.equal(r.content, s);
      assert.equal(r.truncated, false);
      assert.equal(r.originalBytes, Buffer.byteLength(s, 'utf-8'));
    }
  });

  it('treats a response exactly at the cap as not truncated', () => {
    const s = 'a'.repeat(1024);
    const r = boundMcpResponse(s, 1024);
    assert.equal(r.truncated, false);
    assert.equal(r.content, s);
    assert.equal(r.originalBytes, 1024);
  });

  it('truncates one byte over the cap and appends the size marker', () => {
    const s = 'a'.repeat(1025);
    const r = boundMcpResponse(s, 1024);
    assert.equal(r.truncated, true);
    assert.equal(r.originalBytes, 1025);
    assert.ok(r.content.startsWith('a'.repeat(1024)));
    assert.match(r.content, /\[Response truncated: 1025 bytes → 1024 bytes/);
    assert.match(r.content, /MCP_MAX_RESPONSE_BYTES/);
  });

  it('never splits a multi-byte UTF-8 char, across every byte offset in a char', () => {
    // '😀' is 4 UTF-8 bytes; vary the cap through a full 4-byte window so the
    // backtrack-to-boundary logic is exercised for each continuation-byte offset.
    const text = '😀'.repeat(50); // 200 bytes
    for (let cap = 10; cap <= 20; cap++) {
      const r = boundMcpResponse(text, cap);
      assert.equal(r.truncated, true);
      const prefix = r.content.split('\n\n[Response truncated')[0];
      // Re-encode the kept prefix: a clean boundary means a whole number of chars.
      assert.ok(!prefix.includes('�'), `cap=${cap} produced a replacement char`);
      assert.equal(Buffer.byteLength(prefix, 'utf-8') % 4, 0, `cap=${cap} split a char`);
    }
  });

  it('bounds a multi-megabyte DoS response to ~cap + marker', () => {
    const huge = 'a'.repeat(50 * 1024 * 1024); // 50 MiB
    const r = boundMcpResponse(huge); // default 10 MiB cap
    assert.equal(r.truncated, true);
    assert.equal(r.originalBytes, 50 * 1024 * 1024);
    const outBytes = Buffer.byteLength(r.content, 'utf-8');
    assert.ok(outBytes <= DEFAULT_MCP_MAX_RESPONSE_BYTES + 512, `output ${outBytes} not bounded`);
    assert.ok(outBytes < r.originalBytes);
  });

  it('with a zero cap, any non-empty content is fully truncated', () => {
    const r = boundMcpResponse('data', 0);
    assert.equal(r.truncated, true);
    assert.equal(r.originalBytes, 4);
    assert.ok(r.content.startsWith('\n\n[Response truncated'));
  });
});

describe('POV/mcp — validateMcpBufferSize (clamp to [1KB, 100MB])', () => {
  const cases: Array<[unknown, number]> = [
    [undefined, DEFAULT_MCP_MAX_RESPONSE_BYTES],
    [null, DEFAULT_MCP_MAX_RESPONSE_BYTES],
    [NaN, DEFAULT_MCP_MAX_RESPONSE_BYTES],
    [Infinity, DEFAULT_MCP_MAX_RESPONSE_BYTES],
    [-Infinity, DEFAULT_MCP_MAX_RESPONSE_BYTES],
    ['abc', DEFAULT_MCP_MAX_RESPONSE_BYTES],
    [{}, DEFAULT_MCP_MAX_RESPONSE_BYTES],
    [0, MIN_BUFFER],
    [-1, MIN_BUFFER],
    [1023, MIN_BUFFER],
    [1024, MIN_BUFFER],
    [1025, 1025],
    [5_000_000, 5_000_000],
    ['5000000', 5_000_000],
    ['5e6', 5_000_000],
    [MAX_BUFFER, MAX_BUFFER],
    [MAX_BUFFER + 1, MAX_BUFFER],
    [200_000_000, MAX_BUFFER],
  ];
  for (const [input, expected] of cases) {
    it(`maps ${JSON.stringify(input)} → ${expected}`, () => {
      assert.equal(validateMcpBufferSize(input), expected);
    });
  }

  it('result is always within [MIN, MAX] for arbitrary finite inputs', () => {
    for (const n of [1, 500, 1024, 2048, 10 ** 9, -5, 0.5, 1e12]) {
      const out = validateMcpBufferSize(n);
      assert.ok(out >= MIN_BUFFER && out <= MAX_BUFFER, `${n} → ${out} out of range`);
    }
  });
});

describe('POV/mcp — matchMcpAutoAllow (allow-rule glob matching)', () => {
  it('matches an exact pattern only', () => {
    assert.equal(matchMcpAutoAllow('mcp__kali__nmap', ['mcp__kali__nmap']), true);
    assert.equal(matchMcpAutoAllow('mcp__kali__nmap', ['mcp__kali__nikto']), false);
  });

  it('matches a trailing-* prefix pattern', () => {
    assert.equal(matchMcpAutoAllow('mcp__kali__nmap', ['mcp__kali__*']), true);
    assert.equal(matchMcpAutoAllow('mcp__other__x', ['mcp__kali__*']), false);
  });

  it('a bare "*" matches everything, including empty', () => {
    assert.equal(matchMcpAutoAllow('anything', ['*']), true);
    assert.equal(matchMcpAutoAllow('', ['*']), true);
  });

  it('empty pattern list matches nothing', () => {
    assert.equal(matchMcpAutoAllow('mcp__kali__nmap', []), false);
  });

  it('matches when ANY pattern in the list matches', () => {
    const patterns = ['foo', 'bar__*', 'baz'];
    assert.equal(matchMcpAutoAllow('bar__qux', patterns), true);
    assert.equal(matchMcpAutoAllow('baz', patterns), true);
    assert.equal(matchMcpAutoAllow('nope', patterns), false);
  });

  it('prefix "" (just "*" without a stem) via a one-char star pattern acts as prefix', () => {
    // 'a*' → prefix 'a'; matches any name starting with 'a' but not others.
    assert.equal(matchMcpAutoAllow('alpha', ['a*']), true);
    assert.equal(matchMcpAutoAllow('beta', ['a*']), false);
  });

  it('exact pattern does not match by prefix (no implicit globbing)', () => {
    assert.equal(matchMcpAutoAllow('mcp__kali__nmap', ['mcp__kali']), false);
  });
});

describe('POV/mcp — constant sets (membership + shape)', () => {
  it('MCP_TRANSPORTS is exactly the three supported transports', () => {
    assert.deepEqual([...MCP_TRANSPORTS], ['stdio', 'sse', 'http']);
  });

  it('MCP_CLIENT_STATES enumerates the client lifecycle', () => {
    assert.deepEqual([...MCP_CLIENT_STATES], ['disconnected', 'connecting', 'ready', 'error']);
  });

  it('MCP_SERVER_STATES enumerates the manager lifecycle', () => {
    assert.deepEqual([...MCP_SERVER_STATES], ['pending', 'connecting', 'ready', 'error', 'stopped', 'disabled']);
  });

  it('MCP_TERMINAL_ERROR_CODES lists only codes classifyError can produce', () => {
    assert.deepEqual([...MCP_TERMINAL_ERROR_CODES], [
      'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'EHOSTUNREACH', 'ECONNREFUSED', 'ESRCH', 'SPAWN_ERROR', 'UNKNOWN',
    ]);
  });

  it('client and server states share the live phases but differ at the edges', () => {
    const client = new Set<string>(MCP_CLIENT_STATES);
    const server = new Set<string>(MCP_SERVER_STATES);
    for (const shared of ['connecting', 'ready', 'error']) {
      assert.ok(client.has(shared) && server.has(shared), `${shared} missing`);
    }
    // 'disconnected' is a client-only transport state.
    assert.ok(client.has('disconnected') && !server.has('disconnected'));
    // pending/stopped/disabled are manager-only orchestration states.
    for (const only of ['pending', 'stopped', 'disabled']) {
      assert.ok(server.has(only) && !client.has(only), `${only} leaked`);
    }
  });

  it('bound constants are finite positive integers', () => {
    assert.equal(DEFAULT_MCP_MAX_RESPONSE_BYTES, 10 * 1024 * 1024);
    assert.equal(MCP_MAX_STDIO_BUFFER, 16 * 1024 * 1024);
    assert.equal(MCP_MAX_PAGES, 1000);
    assert.equal(MCP_PROTOCOL_VERSION, '2025-06-18');
    for (const n of [DEFAULT_MCP_MAX_RESPONSE_BYTES, MCP_MAX_STDIO_BUFFER, MCP_MAX_PAGES]) {
      assert.ok(Number.isInteger(n) && n > 0);
    }
    // The response cap must fit inside the configurable buffer ceiling.
    assert.ok(DEFAULT_MCP_MAX_RESPONSE_BYTES <= MAX_BUFFER);
  });
});

describe('POV/mcp — McpClient.classifyError (every arm maps into the code set)', () => {
  const client = new McpClient({ command: 'echo' });
  const terminalSet = new Set<string>(MCP_TERMINAL_ERROR_CODES);

  const withCode = (code: string, message = 'boom') => {
    const e = new Error(message) as NodeJS.ErrnoException;
    e.code = code;
    return e;
  };

  it('maps each known errno code to itself with human context', () => {
    for (const code of ['ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'EHOSTUNREACH', 'ECONNREFUSED', 'ESRCH']) {
      const r = client.classifyError(withCode(code));
      assert.equal(r.code, code);
      assert.ok(r.message.length > 0 && r.message !== 'boom', `${code} lacked context`);
      assert.ok(terminalSet.has(r.code));
    }
  });

  it('classifies spawn failures as SPAWN_ERROR (message-based, no code)', () => {
    const r = client.classifyError(new Error('spawn cat ENOENT'));
    assert.equal(r.code, 'SPAWN_ERROR');
    assert.ok(terminalSet.has(r.code));
  });

  it('classifies terminated/killed messages as ESRCH', () => {
    assert.equal(client.classifyError(new Error('process was terminated')).code, 'ESRCH');
    assert.equal(client.classifyError(new Error('child killed by signal')).code, 'ESRCH');
  });

  it('spawn wins over terminated when both words appear', () => {
    const r = client.classifyError(new Error('spawn failed, process terminated'));
    assert.equal(r.code, 'SPAWN_ERROR');
  });

  it('falls back to UNKNOWN (preserving the message) for unrecognized errors', () => {
    const r = client.classifyError(new Error('some novel failure'));
    assert.equal(r.code, 'UNKNOWN');
    assert.equal(r.message, 'some novel failure');
    assert.ok(terminalSet.has(r.code));
  });

  it('an unlisted errno with a plain message falls through to UNKNOWN', () => {
    const r = client.classifyError(withCode('ENOENT', 'file missing'));
    assert.equal(r.code, 'UNKNOWN');
    assert.ok(terminalSet.has(r.code));
  });

  it('always keeps the original error attached', () => {
    const orig = withCode('EPIPE');
    assert.equal(client.classifyError(orig).original, orig);
  });
});

describe('POV/mcp — McpClient initial state & defensive getters (no live server)', () => {
  it('starts disconnected with empty, independent collections', () => {
    const client = new McpClient({ command: 'echo', args: ['x'] });
    assert.equal(client.state, 'disconnected');
    assert.ok(MCP_CLIENT_STATES.includes(client.state));
    assert.equal(client.serverInfo, null);
    assert.equal(client.protocolVersion, null);
    assert.equal(client.uptime, null);
    assert.deepEqual(client.tools, []);
    assert.deepEqual(client.resources, []);
    assert.deepEqual(client.prompts, []);
    assert.deepEqual(client.serverCapabilities, {});
  });

  it('getters return fresh copies — mutating the result cannot corrupt internal state', () => {
    const client = new McpClient({ command: 'echo' });
    client.tools.push({ name: 'x', description: '', inputSchema: {} });
    client.resources.push({ uri: 'u', name: 'n' });
    client.prompts.push({ name: 'p' });
    (client.serverCapabilities as Record<string, unknown>).hacked = true;
    assert.deepEqual(client.tools, []);
    assert.deepEqual(client.resources, []);
    assert.deepEqual(client.prompts, []);
    assert.deepEqual(client.serverCapabilities, {});
  });

  it('hasCapability is false before any handshake', () => {
    const client = new McpClient({ command: 'echo' });
    assert.equal(client.hasCapability('sampling'), false);
    assert.equal(client.hasCapability('anything'), false);
  });

  it('getRoots derives file:// URIs from configured root paths', () => {
    const client = new McpClient({ command: 'echo', rootPaths: ['/home/user/proj', '/tmp'] });
    assert.deepEqual(client.getRoots(), [
      { uri: 'file:///home/user/proj', name: 'proj' },
      { uri: 'file:///tmp', name: 'tmp' },
    ]);
  });

  it('getRoots is empty with no configured roots', () => {
    assert.deepEqual(new McpClient({ command: 'echo' }).getRoots(), []);
  });

  it('operations reject while not ready, and disconnect is idempotent', async () => {
    const client = new McpClient({ command: 'echo' });
    await assert.rejects(() => client.listTools(), /not ready/);
    await assert.rejects(() => client.callTool('t'), /not ready/);
    client.disconnect();
    client.disconnect();
    assert.equal(client.state, 'disconnected');
  });
});

describe('POV/mcp — stdio read buffer DoS bound (newline-delimited framing)', () => {
  const freshClient = () => new McpClient({ command: '' });

  it('routes every complete newline-delimited message and retains only the partial tail', () => {
    const client = freshClient();
    const seen: string[] = [];
    let protoErr = 0;
    let parseErr = 0;
    client.on('notification', (m: string) => seen.push(m));
    client.on('protocol-error', () => protoErr++);
    client.on('parse-error', () => parseErr++);
    (client as unknown as { buffer: string }).buffer =
      JSON.stringify({ jsonrpc: '2.0', method: 'notifications/tools/list_changed' }) + '\n' +
      JSON.stringify({ jsonrpc: '2.0', method: 'notifications/prompts/list_changed' }) + '\n' +
      '{"jsonrpc":"2.0","method":"incomp';
    (client as unknown as { processBuffer(): void }).processBuffer();
    assert.deepEqual(seen, ['notifications/tools/list_changed', 'notifications/prompts/list_changed']);
    assert.equal(protoErr, 0);
    assert.equal(parseErr, 0);
    assert.equal((client as unknown as { buffer: string }).buffer, '{"jsonrpc":"2.0","method":"incomp');
  });

  it('emits parse-error for a malformed complete line without dropping the stream', () => {
    const client = freshClient();
    let parseErr = 0;
    const bad: string[] = [];
    client.on('parse-error', (l: string) => { parseErr++; bad.push(l); });
    (client as unknown as { buffer: string }).buffer = 'not json\n';
    (client as unknown as { processBuffer(): void }).processBuffer();
    assert.equal(parseErr, 1);
    assert.equal(bad[0], 'not json');
    assert.equal((client as unknown as { buffer: string }).buffer, '');
  });

  it('drops the buffer and signals protocol-error when a never-terminated line exceeds the cap', () => {
    const client = freshClient();
    const errs: string[] = [];
    client.on('protocol-error', (m: string) => errs.push(m));
    (client as unknown as { buffer: string }).buffer = 'x'.repeat(MCP_MAX_STDIO_BUFFER + 1);
    (client as unknown as { processBuffer(): void }).processBuffer();
    assert.equal(errs.length, 1);
    assert.match(errs[0], /exceeded 16777216 bytes/);
    assert.equal((client as unknown as { buffer: string }).buffer, '');
  });

  it('keeps an un-terminated remainder that is exactly at the cap', () => {
    const client = freshClient();
    let protoErr = 0;
    client.on('protocol-error', () => protoErr++);
    (client as unknown as { buffer: string }).buffer = 'z'.repeat(MCP_MAX_STDIO_BUFFER);
    (client as unknown as { processBuffer(): void }).processBuffer();
    assert.equal(protoErr, 0);
    assert.equal((client as unknown as { buffer: string }).buffer.length, MCP_MAX_STDIO_BUFFER);
  });

  it('stays bounded when oversized chunks accumulate without any newline', () => {
    const client = freshClient();
    let protoErr = 0;
    client.on('protocol-error', () => protoErr++);
    const feed = (chunk: string) => {
      const c = client as unknown as { buffer: string; processBuffer(): void };
      c.buffer += chunk;
      c.processBuffer();
    };
    const chunk = 'q'.repeat(5 * 1024 * 1024);
    for (let i = 0; i < 6; i++) feed(chunk); // 30 MiB, never a newline
    assert.ok(protoErr >= 1);
    assert.ok((client as unknown as { buffer: string }).buffer.length <= MCP_MAX_STDIO_BUFFER);
  });
});

describe('POV/mcp — McpSessionExpiredError shape', () => {
  it('is an Error with a dedicated name and default message', () => {
    const e = new McpSessionExpiredError();
    assert.ok(e instanceof Error);
    assert.equal(e.name, 'McpSessionExpiredError');
    assert.equal(e.message, 'MCP session expired');
  });

  it('accepts a custom message', () => {
    const e = new McpSessionExpiredError('token revoked');
    assert.equal(e.message, 'token revoked');
    assert.equal(e.name, 'McpSessionExpiredError');
  });
});

describe('POV/mcp — McpManager server-state transitions (no live server)', () => {
  it('pending → stopped → re-enabled, each a declared MCP_SERVER_STATE', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('alpha', { command: 'echo', args: ['hi'] });
    assert.equal(mgr.getServerState('alpha'), 'pending');
    assert.ok(MCP_SERVER_STATES.includes(mgr.getServerState('alpha')));
    assert.equal(mgr.isPending('alpha'), true);
    assert.equal(mgr.isDisabled('alpha'), false);

    return mgr.stopServer('alpha').then(() => {
      assert.equal(mgr.getServerState('alpha'), 'stopped');
      assert.equal(mgr.isPending('alpha'), true); // stopped (not disabled) is still startable
      // enableServer only lifts a 'disabled' state; a 'stopped' server is unchanged.
      mgr.enableServer('alpha');
      assert.equal(mgr.getServerState('alpha'), 'stopped');
      mgr.disconnectAll();
    });
  });

  it('pending → disabled → re-enabled to stopped', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('beta', { command: 'echo' });
    await mgr.stopServer('beta', true);
    assert.equal(mgr.getServerState('beta'), 'disabled');
    assert.equal(mgr.isDisabled('beta'), true);
    assert.equal(mgr.isPending('beta'), false);
    mgr.enableServer('beta');
    assert.equal(mgr.getServerState('beta'), 'stopped');
    assert.equal(mgr.isPending('beta'), true);
    mgr.disconnectAll();
  });

  it('unknown server defaults to the "pending" state', () => {
    const mgr = new McpManager();
    assert.equal(mgr.getServerState('ghost'), 'pending');
    assert.equal(mgr.isPending('ghost'), false);
    assert.equal(mgr.isDisabled('ghost'), false);
  });

  it('removeServer clears a pending registration', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('gamma', { command: 'echo' });
    assert.equal(mgr.getServerState('gamma'), 'pending');
    mgr.removeServer('gamma');
    assert.equal(mgr.getServerState('gamma'), 'pending'); // default-for-unknown
    assert.equal(mgr.listAllServers().some(s => s.name === 'gamma'), false);
    mgr.disconnectAll();
  });

  it('rejects a duplicate pending registration', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('dup', { command: 'echo' });
    assert.throws(() => mgr.addPendingServer('dup', { command: 'echo' }), /already registered/);
    mgr.disconnectAll();
  });

  it('listAllServers reports a pending server with zeroed counts', () => {
    const mgr = new McpManager();
    mgr.addPendingServer('solo', { command: 'echo' });
    const row = mgr.listAllServers().find(s => s.name === 'solo');
    assert.ok(row);
    assert.equal(row!.state, 'pending');
    assert.equal(row!.toolCount, 0);
    assert.equal(row!.resourceCount, 0);
    assert.equal(row!.serverInfo, null);
    assert.equal(row!.uptime, null);
    mgr.disconnectAll();
  });
});
