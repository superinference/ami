import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';

describe('MCP description length env var', () => {
  const envKey = 'SUPERINFERENCE_MAX_MCP_DESCRIPTION_LENGTH';
  let savedVal: string | undefined;

  beforeEach(() => {
    savedVal = process.env[envKey];
    delete process.env[envKey];
  });

  afterEach(() => {
    if (savedVal !== undefined) process.env[envKey] = savedVal;
    else delete process.env[envKey];
  });

  it('default truncation length is 2048', async () => {
    const { McpManager } = await import('../src/mcp/manager');
    const manager = new McpManager();
    const short = 'a'.repeat(100);
    assert.equal(manager.truncateDescription(short), short);

    const long = 'b'.repeat(3000);
    const truncated = manager.truncateDescription(long);
    assert.equal(truncated.length, 2048);
    assert.ok(truncated.endsWith('...'));
  });

  it('respects custom max length from env', async () => {
    process.env[envKey] = '500';
    // Clear module cache to pick up new env value
    // The function reads env at call time, so this should work
    const { McpManager } = await import('../src/mcp/manager');
    const manager = new McpManager();
    const long = 'c'.repeat(1000);
    const truncated = manager.truncateDescription(long);
    assert.equal(truncated.length, 500);
    assert.ok(truncated.endsWith('...'));
  });

  it('ignores invalid env values', async () => {
    process.env[envKey] = 'not-a-number';
    const { McpManager } = await import('../src/mcp/manager');
    const manager = new McpManager();
    const long = 'd'.repeat(3000);
    const truncated = manager.truncateDescription(long);
    assert.equal(truncated.length, 2048);
  });

  it('allows explicit maxLength parameter to override', async () => {
    const { McpManager } = await import('../src/mcp/manager');
    const manager = new McpManager();
    const long = 'e'.repeat(3000);
    const truncated = manager.truncateDescription(long, 100);
    assert.equal(truncated.length, 100);
    assert.ok(truncated.endsWith('...'));
  });

  it('does not truncate when text fits', async () => {
    const { McpManager } = await import('../src/mcp/manager');
    const manager = new McpManager();
    const short = 'f'.repeat(50);
    assert.equal(manager.truncateDescription(short), short);
  });
});
