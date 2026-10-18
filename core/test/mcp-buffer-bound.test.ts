import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { boundMcpResponse, validateMcpBufferSize, DEFAULT_MCP_MAX_RESPONSE_BYTES } from '../src/mcp/client';

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
