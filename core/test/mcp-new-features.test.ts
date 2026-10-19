/**
 * Tests for MCP new features:
 * - McpManager.truncateDescription (description length limiting)
 * - McpManager.getToolAnnotations (readOnly/destructive hints)
 * - McpClient.onToolProgress (callback registration)
 * - McpClient states (disconnected → connecting → ready → error)
 * - McpManager health check start/stop
 * - MCP tool call timeout (configurable via SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS)
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { McpManager, findMcpConfigPaths, isTransientConnectError } from '../src/mcp/manager';
import { McpClient } from '../src/mcp/client';
import { DEFAULT_MCP_TOOL_TIMEOUT_MS, getMcpToolTimeout } from '../src/tools/mcp-tool';

let manager: McpManager;

beforeEach(() => {
  manager = new McpManager();
});

// ---------------------------------------------------------------------------
// truncateDescription
// ---------------------------------------------------------------------------

describe('McpManager — truncateDescription', () => {
  it('returns short descriptions unchanged', () => {
    assert.equal(manager.truncateDescription('hello'), 'hello');
  });

  it('truncates at default maxLength (2048)', () => {
    const long = 'a'.repeat(3000);
    const result = manager.truncateDescription(long);
    assert.equal(result.length, 2048);
    assert.ok(result.endsWith('...'));
  });

  it('truncates at custom maxLength', () => {
    const long = 'a'.repeat(100);
    const result = manager.truncateDescription(long, 50);
    assert.equal(result.length, 50);
    assert.ok(result.endsWith('...'));
  });

  it('handles empty string', () => {
    assert.equal(manager.truncateDescription(''), '');
  });

  it('handles exact-length string', () => {
    const exact = 'a'.repeat(2048);
    assert.equal(manager.truncateDescription(exact), exact);
  });
});

// ---------------------------------------------------------------------------
// getToolAnnotations
// ---------------------------------------------------------------------------

describe('McpManager — getToolAnnotations', () => {
  it('marks read-only tools', () => {
    const ann = manager.getToolAnnotations('getUser');
    assert.equal(ann.readOnlyHint, true);
  });

  it('marks list tools as read-only', () => {
    const ann = manager.getToolAnnotations('listFiles');
    assert.equal(ann.readOnlyHint, true);
  });

  it('marks search tools as read-only', () => {
    const ann = manager.getToolAnnotations('searchDocs');
    assert.equal(ann.readOnlyHint, true);
  });

  it('marks destructive tools', () => {
    const ann = manager.getToolAnnotations('deleteUser');
    assert.equal(ann.destructiveHint, true);
  });

  it('marks remove tools as destructive', () => {
    const ann = manager.getToolAnnotations('removeEntry');
    assert.equal(ann.destructiveHint, true);
  });

  it('returns no hints for neutral tools', () => {
    const ann = manager.getToolAnnotations('updateProfile');
    assert.equal(ann.readOnlyHint, undefined);
    assert.equal(ann.destructiveHint, undefined);
  });

  it('is case-insensitive', () => {
    const ann = manager.getToolAnnotations('GetUser');
    assert.equal(ann.readOnlyHint, true);
  });
});

// ---------------------------------------------------------------------------
// McpClient — onToolProgress
// ---------------------------------------------------------------------------

describe('McpClient — onToolProgress', () => {
  it('registers a progress callback', () => {
    const client = new McpClient({ command: 'echo' });
    let called = false;
    client.onToolProgress('call-1', (_progress) => {
      called = true;
    });
    assert.ok(!called);
  });
});

// ---------------------------------------------------------------------------
// McpClient — state
// ---------------------------------------------------------------------------

describe('McpClient — initial state', () => {
  it('starts in disconnected state', () => {
    const client = new McpClient({ command: 'echo' });
    assert.equal(client.state, 'disconnected');
  });
});

// ---------------------------------------------------------------------------
// McpManager — health check lifecycle
// ---------------------------------------------------------------------------

describe('McpManager — health checks', () => {
  it('can start and stop health checks without error', () => {
    manager.startHealthChecks();
    manager.stopHealthChecks();
  });

  it('can stop health checks when none are running', () => {
    manager.stopHealthChecks();
  });
});

// ---------------------------------------------------------------------------
// findMcpConfigPaths
// ---------------------------------------------------------------------------

describe('findMcpConfigPaths', () => {
  it('returns an array', () => {
    const paths = findMcpConfigPaths('/tmp/nonexistent');
    assert.ok(Array.isArray(paths));
  });
});

// ---------------------------------------------------------------------------
// isTransientConnectError — MCP startup retry classification
// ---------------------------------------------------------------------------

describe('isTransientConnectError', () => {
  it('returns true for ECONNREFUSED', () => {
    const err = Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' });
    assert.ok(isTransientConnectError(err));
  });

  it('returns true for ECONNRESET', () => {
    const err = Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' });
    assert.ok(isTransientConnectError(err));
  });

  it('returns true for ETIMEDOUT', () => {
    const err = Object.assign(new Error('connect ETIMEDOUT'), { code: 'ETIMEDOUT' });
    assert.ok(isTransientConnectError(err));
  });

  it('returns true for EPIPE', () => {
    const err = Object.assign(new Error('write EPIPE'), { code: 'EPIPE' });
    assert.ok(isTransientConnectError(err));
  });

  it('returns true for EAI_AGAIN (DNS)', () => {
    const err = Object.assign(new Error('getaddrinfo EAI_AGAIN'), { code: 'EAI_AGAIN' });
    assert.ok(isTransientConnectError(err));
  });

  it('returns true for message-only socket hang up', () => {
    assert.ok(isTransientConnectError(new Error('socket hang up')));
  });

  it('returns true for connect ENOENT in message', () => {
    assert.ok(isTransientConnectError(new Error('connect ENOENT /var/run/mcp.sock')));
  });

  it('returns false for auth errors', () => {
    assert.equal(isTransientConnectError(new Error('401 Unauthorized')), false);
  });

  it('returns false for generic errors', () => {
    assert.equal(isTransientConnectError(new Error('Invalid configuration')), false);
  });

  it('returns false for McpSessionExpiredError', () => {
    const { McpSessionExpiredError } = require('../src/mcp/client');
    assert.equal(isTransientConnectError(new McpSessionExpiredError('expired')), false);
  });

  it('returns false for non-error values', () => {
    assert.equal(isTransientConnectError('string error'), false);
    assert.equal(isTransientConnectError(42), false);
    assert.equal(isTransientConnectError(null), false);
  });
});

// ---------------------------------------------------------------------------
// MCP tool call timeout
// ---------------------------------------------------------------------------

describe('MCP tool call timeout', () => {
  const envKey = 'SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS';
  let savedEnv: string | undefined;

  beforeEach(() => {
    savedEnv = process.env[envKey];
  });

  afterEach(() => {
    if (savedEnv === undefined) {
      delete process.env[envKey];
    } else {
      process.env[envKey] = savedEnv;
    }
  });

  it('DEFAULT_MCP_TOOL_TIMEOUT_MS is 5 minutes (300000ms)', () => {
    assert.equal(DEFAULT_MCP_TOOL_TIMEOUT_MS, 300_000);
  });

  it('getMcpToolTimeout returns default when env var is unset', () => {
    delete process.env[envKey];
    assert.equal(getMcpToolTimeout(), 300_000);
  });

  it('getMcpToolTimeout returns default when env var is empty', () => {
    process.env[envKey] = '';
    assert.equal(getMcpToolTimeout(), 300_000);
  });

  it('getMcpToolTimeout reads a custom value from env', () => {
    process.env[envKey] = '60000';
    assert.equal(getMcpToolTimeout(), 60_000);
  });

  it('getMcpToolTimeout ignores non-numeric values', () => {
    process.env[envKey] = 'abc';
    assert.equal(getMcpToolTimeout(), 300_000);
  });

  it('getMcpToolTimeout ignores zero', () => {
    process.env[envKey] = '0';
    assert.equal(getMcpToolTimeout(), 300_000);
  });

  it('getMcpToolTimeout ignores negative values', () => {
    process.env[envKey] = '-1000';
    assert.equal(getMcpToolTimeout(), 300_000);
  });

  it('getMcpToolTimeout ignores Infinity', () => {
    process.env[envKey] = 'Infinity';
    assert.equal(getMcpToolTimeout(), 300_000);
  });

  it('source code uses Promise.race with timeout for tool calls', () => {
    const src = fs.readFileSync(
      path.join(__dirname, '..', 'src', 'tools', 'mcp-tool.ts'), 'utf-8',
    );
    assert.ok(src.includes('Promise.race'), 'must use Promise.race for timeout');
    assert.ok(src.includes('getMcpToolTimeout()'), 'must call getMcpToolTimeout()');
    assert.ok(src.includes('timed out after'), 'must include descriptive timeout message');
    assert.ok(src.includes('SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS'), 'must reference env var');
  });
});

describe('MCP server receives SUPERINFERENCE_PROJECT_DIR', () => {
  it('addServer enriches env with project directory from rootPaths', () => {
    const src = fs.readFileSync(
      path.resolve(__dirname, '..', 'src', 'mcp', 'manager.ts'), 'utf-8',
    );
    assert.ok(
      src.includes('SUPERINFERENCE_PROJECT_DIR') && src.includes('rootPaths[0]'),
      'addServer must pass SUPERINFERENCE_PROJECT_DIR from rootPaths to MCP client env',
    );
  });

  it('enrichedEnv spreads config.env and adds project dir', () => {
    const src = fs.readFileSync(
      path.resolve(__dirname, '..', 'src', 'mcp', 'manager.ts'), 'utf-8',
    );
    const enrichSection = src.slice(src.indexOf('enrichedEnv'), src.indexOf('enrichedEnv') + 200);
    assert.ok(enrichSection.includes('config.env'), 'must spread config.env into enrichedEnv');
    assert.ok(enrichSection.includes('SUPERINFERENCE_PROJECT_DIR'), 'must set SUPERINFERENCE_PROJECT_DIR');
  });
});
