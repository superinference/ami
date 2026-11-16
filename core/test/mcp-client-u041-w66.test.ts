/**
 * mcp-client-u041-w66.test.ts — U041 (wave 66) additive parity for the MCP client.
 *
 * Two confirmed, previously-unimplemented gaps against core/core/src/mcp/client.ts:
 *
 *  1. JSON-RPC envelope validation before dispatch (parity: Claude's
 *     mcpWebSocketTransport validateJsonRpcMessage / zod-parsed inbound frames).
 *     A malformed frame pushed by an untrusted server (bare primitive, array, or an
 *     object with neither id nor method) must be rejected + surfaced as a
 *     `protocol-error`, not silently swallowed or routed into a throwing branch.
 *     RED (revert isValidJsonRpcMessage in routeMessage): the malformed object is
 *     silently dropped → no protocol-error → the count assertion fails.
 *
 *  2. 403-token-revoked full re-authorization (parity: Claude's also403Revoked in
 *     utils/http.ts). A 403 whose challenge/body marks the token revoked/invalid
 *     must drive onUnauthorized (full re-auth) + a single retry, NOT the
 *     insufficient_scope step-up (onForbidden). RED (revert the is403RevokedToken
 *     branch in sendHttpRequest): the client calls onForbidden instead; with a fake
 *     that declines non-step-up 403s the call rejects → the success assertion fails.
 */
import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import {
  McpClient,
  isValidJsonRpcMessage,
  is403RevokedToken,
} from '../src/mcp/client';

after(() => { setTimeout(() => process.exit(0), 200); });

// ---------------------------------------------------------------------------
// GAP: JSON-RPC envelope validator (unit)
// ---------------------------------------------------------------------------
describe('isValidJsonRpcMessage', () => {
  it('accepts a response envelope (jsonrpc + numeric id)', () => {
    assert.equal(isValidJsonRpcMessage({ jsonrpc: '2.0', id: 7, result: {} }), true);
  });

  it('accepts a response envelope with a string id', () => {
    assert.equal(isValidJsonRpcMessage({ jsonrpc: '2.0', id: 'abc', result: {} }), true);
  });

  it('accepts a notification envelope (jsonrpc + string method)', () => {
    assert.equal(isValidJsonRpcMessage({ jsonrpc: '2.0', method: 'notifications/message', params: {} }), true);
  });

  it('accepts a server→client request envelope (id + method)', () => {
    assert.equal(isValidJsonRpcMessage({ jsonrpc: '2.0', id: 3, method: 'ping' }), true);
  });

  it('rejects a non-object (bare primitive)', () => {
    assert.equal(isValidJsonRpcMessage(42), false);
    assert.equal(isValidJsonRpcMessage('x'), false);
    assert.equal(isValidJsonRpcMessage(true), false);
  });

  it('rejects null and arrays', () => {
    assert.equal(isValidJsonRpcMessage(null), false);
    assert.equal(isValidJsonRpcMessage([{ jsonrpc: '2.0', method: 'x' }]), false);
  });

  it('rejects a wrong/absent jsonrpc version', () => {
    assert.equal(isValidJsonRpcMessage({ jsonrpc: '1.0', method: 'x' }), false);
    assert.equal(isValidJsonRpcMessage({ method: 'x' }), false);
  });

  it('rejects an object with neither a usable id nor a method', () => {
    assert.equal(isValidJsonRpcMessage({ jsonrpc: '2.0', foo: 'bar' }), false);
    // id present but of the wrong type is not a usable id
    assert.equal(isValidJsonRpcMessage({ jsonrpc: '2.0', id: null }), false);
    assert.equal(isValidJsonRpcMessage({ jsonrpc: '2.0', method: 5 }), false);
  });
});

// ---------------------------------------------------------------------------
// GAP: envelope validation wired into the read loop (integration, red-green)
// ---------------------------------------------------------------------------
describe('routeMessage envelope validation', () => {
  it('emits protocol-error for a malformed JSON-RPC frame and still routes valid ones', () => {
    const client = new McpClient({ command: '' });
    const protocolErrors: string[] = [];
    const notifications: string[] = [];
    client.on('protocol-error', (m: string) => protocolErrors.push(m));
    client.on('notification', (m: string) => notifications.push(m));

    // A well-formed-JSON but NON-conforming envelope (no id, no method), followed by
    // a valid notification on the same read pass.
    (client as unknown as { buffer: string }).buffer =
      JSON.stringify({ jsonrpc: '2.0', foo: 'bar' }) + '\n' +
      JSON.stringify({ jsonrpc: '2.0', method: 'notifications/tools/list_changed' }) + '\n';
    (client as unknown as { processBuffer: () => void }).processBuffer();

    assert.equal(protocolErrors.length, 1, 'exactly one protocol-error for the malformed frame');
    assert.match(protocolErrors[0], /malformed JSON-RPC/);
    assert.ok(notifications.includes('notifications/tools/list_changed'), 'the valid notification still routes');
  });

  it('rejects a bare-array frame fed through the buffer without throwing', () => {
    const client = new McpClient({ command: '' });
    let protocolErrors = 0;
    let parseErrors = 0;
    client.on('protocol-error', () => protocolErrors++);
    client.on('parse-error', () => parseErrors++);

    (client as unknown as { buffer: string }).buffer = JSON.stringify([1, 2, 3]) + '\n';
    assert.doesNotThrow(() => (client as unknown as { processBuffer: () => void }).processBuffer());
    assert.equal(protocolErrors, 1);
    assert.equal(parseErrors, 0);
  });
});

// ---------------------------------------------------------------------------
// GAP: 403-token-revoked re-auth (unit)
// ---------------------------------------------------------------------------
describe('is403RevokedToken', () => {
  it('detects a revoked/invalid token from the WWW-Authenticate challenge', () => {
    assert.equal(is403RevokedToken('Bearer error="invalid_token"', ''), true);
    assert.equal(is403RevokedToken('Bearer error="invalid_token", error_description="token revoked"', ''), true);
  });

  it('detects a revoked/invalid token from the response body', () => {
    assert.equal(is403RevokedToken(null, '{"error":"invalid_token"}'), true);
    assert.equal(is403RevokedToken(null, 'The access token has been revoked'), true);
  });

  it('returns false for an insufficient_scope step-up (not a revocation)', () => {
    assert.equal(is403RevokedToken('Bearer error="insufficient_scope", scope="admin"', ''), false);
    assert.equal(is403RevokedToken('Bearer error="access_denied"', 'forbidden'), false);
    assert.equal(is403RevokedToken(null, ''), false);
  });
});

// ---------------------------------------------------------------------------
// GAP: 403-token-revoked re-auth (integration, red-green)
// ---------------------------------------------------------------------------
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

describe('sendHttpRequest — 403 token-revoked full re-auth', () => {
  let server: http.Server;
  let client: McpClient;

  afterEach(async () => {
    if (client) client.disconnect();
    if (server) await closeServer(server);
  });

  it('drives onUnauthorized (not onForbidden) and retries once on a revoked-token 403', async () => {
    let toolCalls = 0;
    server = http.createServer((req, res) => {
      // Decline the server→client GET stream so we stay POST-only.
      if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { protocolVersion: '2025-06-18', serverInfo: { name: 's', version: '1' }, capabilities: {} },
          }));
          return;
        }
        if (typeof msg.method === 'string' && msg.method.startsWith('notifications/')) { res.writeHead(202); res.end(); return; }
        if (msg.method === 'tools/call') {
          toolCalls++;
          if (toolCalls === 1) {
            // First attempt: the bearer was revoked server-side.
            res.writeHead(403, { 'Content-Type': 'application/json', 'WWW-Authenticate': 'Bearer error="invalid_token"' });
            res.end(JSON.stringify({ error: 'invalid_token', error_description: 'token has been revoked' }));
            return;
          }
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { content: [{ type: 'text', text: 'ok' }] } }));
          return;
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }));
      });
    });
    const port = await listenOnRandom(server);

    let unauthorizedCalls = 0;
    let forbiddenCalls = 0;
    let token = 'AT1';
    const oauth = {
      authorizationHeader: async () => `Bearer ${token}`,
      onUnauthorized: async () => { unauthorizedCalls++; token = 'AT2'; return true; },
      // A revoked token is NOT a scope step-up: a correct client must never route
      // here. A real onForbidden declines a non-insufficient_scope 403 (returns false).
      onForbidden: async () => { forbiddenCalls++; return false; },
    };

    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000, oauth });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);

    const result = await client.callTool('do-thing', {}) as { content: Array<{ type: string; text: string }> };
    assert.deepEqual(result.content, [{ type: 'text', text: 'ok' }]);
    assert.equal(unauthorizedCalls, 1, 'a revoked-token 403 must drive a FULL re-authorization');
    assert.equal(forbiddenCalls, 0, 'a revoked-token 403 must NOT be treated as a scope step-up');
    assert.equal(toolCalls, 2, 'the tool call is retried exactly once after re-auth');
  });

  it('rejects when re-authorization after a revoked-token 403 fails', async () => {
    server = http.createServer((req, res) => {
      if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2025-06-18', serverInfo: { name: 's', version: '1' }, capabilities: {} } }));
          return;
        }
        if (typeof msg.method === 'string' && msg.method.startsWith('notifications/')) { res.writeHead(202); res.end(); return; }
        if (msg.method === 'tools/call') {
          res.writeHead(403, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'invalid_token' }));
          return;
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }));
      });
    });
    const port = await listenOnRandom(server);

    const oauth = {
      authorizationHeader: async () => 'Bearer AT1',
      onUnauthorized: async () => false, // re-auth fails
      onForbidden: async () => false,
    };
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000, oauth });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);

    await assert.rejects(() => client.callTool('do-thing', {}), /token revoked/);
  });
});
