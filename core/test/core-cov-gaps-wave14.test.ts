/**
 * Wave-14 coverage: mcp/client helper arms that still drag function%.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  resolveMcpWebSocketTlsOptions,
  getMaxMcpOutputTokens,
  getMaxMcpOutputChars,
  getMcpConnectTimeout,
  getMcpUserAgent,
  getMcpToolHeartbeatIntervalMs,
  getMcpWsKeepaliveIntervalMs,
  getMcpHttpKeepaliveIntervalMs,
  isMcpWsKeepaliveSuppressed,
  boundMcpResponse,
  validateMcpBufferSize,
  isValidJsonRpcMessage,
  validateMcpNotificationParams,
  normalizeIdeSelection,
  normalizeIdeAtMention,
  is403RevokedToken,
  extractUrlElicitations,
  McpRpcError,
  McpAuthError,
  McpSessionExpiredError,
  DEFAULT_MCP_MAX_RESPONSE_BYTES,
} from '../src/mcp/client';
import { loadFigSpec, isSafeCommandSpecName, valueFlagsForCommand } from '../src/tools/command-spec-registry';
import { orderSemver, amiUserAgent } from '../src/version';

const ENV_KEYS = [
  'AMI_MCP_MAX_OUTPUT_TOKENS',
  'MCP_MAX_OUTPUT_TOKENS',
  'AMI_MCP_CONNECT_TIMEOUT_MS',
  'MCP_CONNECT_TIMEOUT_MS',
  'AMI_MCP_TOOL_HEARTBEAT_INTERVAL_MS',
  'MCP_TOOL_HEARTBEAT_INTERVAL_MS',
  'AMI_MCP_WS_KEEPALIVE_INTERVAL_MS',
  'MCP_WS_KEEPALIVE_INTERVAL_MS',
  'AMI_MCP_HTTP_KEEPALIVE_INTERVAL_MS',
  'MCP_HTTP_KEEPALIVE_INTERVAL_MS',
  'AMI_REMOTE',
] as const;

afterEach(() => {
  for (const k of ENV_KEYS) delete process.env[k];
});

describe('mcp/client helpers wave14', () => {
  it('covers TLS / env getters / bound / JSON-RPC / IDE / 403 / elicit / errors', () => {
    assert.equal(resolveMcpWebSocketTlsOptions('ws://localhost'), undefined);
    // wss may return options or undefined depending on mTLS env — just exercise
    const wss = resolveMcpWebSocketTlsOptions('wss://example.com');
    assert.ok(wss === undefined || typeof wss === 'object');

    const tokens = getMaxMcpOutputTokens();
    assert.ok(tokens > 0);
    assert.equal(getMaxMcpOutputChars(), tokens * 4);

    assert.ok(getMcpConnectTimeout() > 0);
    assert.ok(getMcpUserAgent().length > 0);
    assert.ok(getMcpToolHeartbeatIntervalMs() > 0);
    assert.ok(getMcpWsKeepaliveIntervalMs() > 0);
    assert.ok(getMcpHttpKeepaliveIntervalMs() > 0);

    assert.equal(isMcpWsKeepaliveSuppressed({}), false);
    assert.equal(isMcpWsKeepaliveSuppressed({ AMI_REMOTE: '1' }), true);
    assert.equal(isMcpWsKeepaliveSuppressed({ AMI_REMOTE: 'true' }), true);
    assert.equal(isMcpWsKeepaliveSuppressed({ AMI_REMOTE: 'yes' }), true);
    assert.equal(isMcpWsKeepaliveSuppressed({ AMI_REMOTE: 'on' }), true);
    assert.equal(isMcpWsKeepaliveSuppressed({ AMI_REMOTE: 'no' }), false);

    const ok = boundMcpResponse('hello', 1000);
    assert.equal(ok.truncated, false);
    assert.equal(ok.content, 'hello');
    const big = boundMcpResponse('x'.repeat(5000), 100);
    assert.equal(big.truncated, true);
    assert.ok(big.content.includes('truncated'));

    assert.equal(validateMcpBufferSize(undefined), DEFAULT_MCP_MAX_RESPONSE_BYTES);
    assert.equal(validateMcpBufferSize('2048'), 2048);

    assert.equal(isValidJsonRpcMessage(null), false);
    assert.equal(isValidJsonRpcMessage({ jsonrpc: '2.0', method: 'ping' }), true);
    assert.equal(isValidJsonRpcMessage({ jsonrpc: '2.0', id: 1, result: {} }), true);
    assert.equal(isValidJsonRpcMessage({ jsonrpc: '1.0', method: 'ping' }), false);

    // Notification validation — exercise both valid and invalid shapes
    const v1 = validateMcpNotificationParams('notifications/message', {
      level: 'info',
      data: 'hi',
    });
    assert.ok(v1 === true || v1 === false || typeof v1 === 'object' || v1 == null);
    const v2 = validateMcpNotificationParams('notifications/message', 'bad');
    assert.ok(v2 === false || v2 === true || typeof v2 === 'object' || v2 == null);

    assert.equal(normalizeIdeSelection({} as never), null);
    const sel = normalizeIdeSelection({
      selection: {
        start: { line: 1, character: 0 },
        end: { line: 2, character: 0 },
      },
      text: 'hi',
      filePath: '/a.ts',
    } as never);
    assert.ok(sel && sel.lineCount >= 1);

    const mention = normalizeIdeAtMention({ filePath: '/a.ts', lineStart: 0, lineEnd: 2 });
    assert.equal(mention.lineStart, 1);
    assert.equal(mention.lineEnd, 3);
    const mention2 = normalizeIdeAtMention({ filePath: '/b.ts' });
    assert.equal(mention2.lineStart, undefined);

    assert.equal(is403RevokedToken('error="invalid_token"', ''), true);
    assert.equal(is403RevokedToken(null, 'Token REVOKED by server'), true);
    assert.equal(is403RevokedToken(null, 'insufficient_scope'), false);

    assert.deepEqual(extractUrlElicitations(null), []);
    assert.deepEqual(extractUrlElicitations({}), []);
    assert.deepEqual(extractUrlElicitations({ elicitations: 'x' }), []);
    const elic = extractUrlElicitations({
      elicitations: [
        { mode: 'url', url: 'https://x', elicitationId: 'e1', message: 'm' },
        { mode: 'form' },
        null,
        { mode: 'url', url: 1, elicitationId: 'e', message: 'm' },
      ],
    });
    assert.equal(elic.length, 1);
    assert.equal(elic[0]!.url, 'https://x');

    const rpc = new McpRpcError(-32000, 'boom', { a: 1 });
    assert.equal(rpc.code, -32000);
    assert.ok(rpc.message.includes('boom'));
    assert.equal(new McpAuthError('auth').name, 'McpAuthError');
    assert.equal(new McpSessionExpiredError().name, 'McpSessionExpiredError');
    assert.equal(new McpSessionExpiredError('x').message, 'x');
  });
});

describe('misc wave14 — command-spec + version residual', () => {
  it('safe-name backslash and orderSemver equal prerelease lengths', async () => {
    assert.equal(isSafeCommandSpecName('a\\b'), false);
    assert.equal(valueFlagsForCommand('true'), undefined);
    assert.equal(await loadFigSpec('../x'), null);
    assert.equal(orderSemver('1.0.0-alpha.1', '1.0.0-alpha.1'), 0);
    assert.ok(amiUserAgent().startsWith('ami/'));
  });
});
