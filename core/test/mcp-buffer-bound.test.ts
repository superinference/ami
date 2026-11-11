import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  boundMcpResponse,
  validateMcpBufferSize,
  DEFAULT_MCP_MAX_RESPONSE_BYTES,
  MCP_MAX_STDIO_BUFFER,
  McpClient,
} from '../src/mcp/client';

describe('boundMcpResponse', () => {
  it('passes through small responses unchanged', () => {
    const r = boundMcpResponse('hello world');
    assert.equal(r.content, 'hello world');
    assert.equal(r.truncated, false);
    assert.equal(r.originalBytes, Buffer.byteLength('hello world'));
  });

  it('passes through response exactly at limit', () => {
    const text = 'a'.repeat(1024);
    const r = boundMcpResponse(text, 1024);
    assert.equal(r.content, text);
    assert.equal(r.truncated, false);
  });

  it('truncates response over limit', () => {
    const text = 'a'.repeat(2000);
    const r = boundMcpResponse(text, 1000);
    assert.equal(r.truncated, true);
    assert.ok(r.content.length < text.length);
    assert.ok(r.content.includes('[Response truncated'));
    assert.equal(r.originalBytes, Buffer.byteLength(text));
  });

  it('preserves UTF-8 boundary on truncation', () => {
    // Multi-byte chars: each emoji is 4 bytes
    const emoji = '😀';
    const text = emoji.repeat(100); // 400 bytes
    const r = boundMcpResponse(text, 200);
    assert.equal(r.truncated, true);
    // Should not have broken UTF-8
    const decoded = Buffer.from(r.content.split('\n\n[Response truncated')[0]).toString('utf-8');
    assert.ok(!decoded.includes('�'), 'should not contain replacement character');
  });

  it('uses default max when no limit specified', () => {
    const text = 'a'.repeat(100);
    const r = boundMcpResponse(text);
    assert.equal(r.truncated, false);
    assert.equal(r.content, text);
  });

  it('truncation message includes sizes', () => {
    const text = 'x'.repeat(5000);
    const r = boundMcpResponse(text, 1000);
    assert.ok(r.content.includes('5000'));
    assert.ok(r.content.includes('1000'));
  });

  it('handles empty string', () => {
    const r = boundMcpResponse('');
    assert.equal(r.content, '');
    assert.equal(r.truncated, false);
    assert.equal(r.originalBytes, 0);
  });
});

describe('validateMcpBufferSize', () => {
  it('returns valid number as-is', () => {
    assert.equal(validateMcpBufferSize(5_000_000), 5_000_000);
  });

  it('returns default for undefined', () => {
    assert.equal(validateMcpBufferSize(undefined), DEFAULT_MCP_MAX_RESPONSE_BYTES);
  });

  it('returns default for non-numeric string', () => {
    assert.equal(validateMcpBufferSize('abc'), DEFAULT_MCP_MAX_RESPONSE_BYTES);
  });

  it('clamps to 1KB minimum', () => {
    assert.equal(validateMcpBufferSize(0), 1024);
    assert.equal(validateMcpBufferSize(100), 1024);
  });

  it('clamps to 100MB maximum', () => {
    assert.equal(validateMcpBufferSize(200_000_000), 100 * 1024 * 1024);
  });

  it('parses numeric string', () => {
    assert.equal(validateMcpBufferSize('5000000'), 5_000_000);
  });

  it('returns default for NaN', () => {
    assert.equal(validateMcpBufferSize(NaN), DEFAULT_MCP_MAX_RESPONSE_BYTES);
  });

  it('returns default for negative number', () => {
    assert.equal(validateMcpBufferSize(-1), 1024);
  });
});

describe('DEFAULT_MCP_MAX_RESPONSE_BYTES', () => {
  it('is 10MB', () => {
    assert.equal(DEFAULT_MCP_MAX_RESPONSE_BYTES, 10 * 1024 * 1024);
  });
});

// ===========================================================================
// L31 — stdio read buffer is bounded (memory-exhaustion DoS backstop)
// ===========================================================================
describe('MCP stdio read buffer bound (L31)', () => {
  it('exposes a 16 MiB cap', () => {
    assert.equal(MCP_MAX_STDIO_BUFFER, 16 * 1024 * 1024);
  });

  it('parses normal newline-delimited messages and keeps the buffer empty', () => {
    const client = new McpClient({ command: '' });
    const notifications: Array<[string, unknown]> = [];
    let protocolErrors = 0;
    let parseErrors = 0;
    client.on('notification', (method: string, params: unknown) => notifications.push([method, params]));
    client.on('protocol-error', () => protocolErrors++);
    client.on('parse-error', () => parseErrors++);

    // Two complete JSON-RPC notifications plus an incomplete trailing fragment.
    (client as any).buffer =
      JSON.stringify({ jsonrpc: '2.0', method: 'notifications/message', params: { level: 'info', data: 'a' } }) + '\n' +
      JSON.stringify({ jsonrpc: '2.0', method: 'notifications/message', params: { level: 'info', data: 'b' } }) + '\n' +
      '{"jsonrpc":"2.0","method":"partial"';
    (client as any).processBuffer();

    assert.equal(notifications.length, 2);
    assert.equal(protocolErrors, 0);
    assert.equal(parseErrors, 0);
    // The incomplete fragment (well under the cap) is retained for the next chunk.
    assert.equal((client as any).buffer, '{"jsonrpc":"2.0","method":"partial"');
  });

  it('drops the buffer and emits protocol-error when the un-terminated remainder exceeds the cap', () => {
    const client = new McpClient({ command: '' });
    const errors: string[] = [];
    let parseErrors = 0;
    client.on('protocol-error', (msg: string) => errors.push(msg));
    client.on('parse-error', () => parseErrors++);

    // One giant line with NO newline — classic never-terminates DoS.
    (client as any).buffer = 'x'.repeat(MCP_MAX_STDIO_BUFFER + 1);
    (client as any).processBuffer();

    assert.equal(errors.length, 1, 'exactly one protocol-error emitted');
    assert.match(errors[0], /exceeded 16777216 bytes/);
    assert.match(errors[0], /dropped \d+ buffered bytes/);
    // Buffer is reset — bounded, no unbounded growth.
    assert.equal((client as any).buffer, '');
    // The oversized fragment is not treated as a parseable line.
    assert.equal(parseErrors, 0);
  });

  it('still routes complete messages on the same pass that overflows', () => {
    const client = new McpClient({ command: '' });
    const notifications: string[] = [];
    let protocolErrors = 0;
    client.on('notification', (method: string) => notifications.push(method));
    client.on('protocol-error', () => protocolErrors++);

    // A valid complete message, then an oversized un-terminated remainder.
    (client as any).buffer =
      JSON.stringify({ jsonrpc: '2.0', method: 'notifications/tools/list_changed' }) + '\n' +
      'y'.repeat(MCP_MAX_STDIO_BUFFER + 1);
    (client as any).processBuffer();

    assert.ok(notifications.includes('notifications/tools/list_changed'), 'complete message still parsed');
    assert.equal(protocolErrors, 1);
    assert.equal((client as any).buffer, '');
  });

  it('does not drop an un-terminated remainder at or below the cap', () => {
    const client = new McpClient({ command: '' });
    let protocolErrors = 0;
    client.on('protocol-error', () => protocolErrors++);

    // Exactly at the cap with no newline — retained, not dropped.
    (client as any).buffer = 'z'.repeat(MCP_MAX_STDIO_BUFFER);
    (client as any).processBuffer();

    assert.equal(protocolErrors, 0);
    assert.equal((client as any).buffer.length, MCP_MAX_STDIO_BUFFER);
  });

  it('bounds accumulation driven through the stdout data handler', () => {
    const client = new McpClient({ command: '' });
    let protocolErrors = 0;
    client.on('protocol-error', () => protocolErrors++);

    // Simulate the real stdout 'data' accumulation path across chunks.
    const feed = (chunk: string) => {
      (client as any).buffer += chunk;
      (client as any).processBuffer();
    };
    const chunk = 'q'.repeat(4 * 1024 * 1024); // 4 MiB, no newline
    feed(chunk);
    feed(chunk);
    feed(chunk);
    feed(chunk);
    feed(chunk); // 20 MiB total, never a newline → must have been bounded

    assert.ok(protocolErrors >= 1, 'overflow was signalled');
    assert.ok((client as any).buffer.length <= MCP_MAX_STDIO_BUFFER, 'buffer stayed bounded');
  });
});
