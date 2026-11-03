/**
 * Tests for MCP new features:
 * - McpManager.truncateDescription (description length limiting)
 * - McpManager.getToolAnnotations (readOnly/destructive hints)
 * - McpClient.onToolProgress (callback registration)
 * - McpClient states (disconnected → connecting → ready → error)
 * - McpManager health check start/stop
 * - MCP tool call timeout (configurable via SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS)
 */

import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { McpManager, findMcpConfigPaths, isTransientConnectError } from '../src/mcp/manager';
import { McpClient } from '../src/mcp/client';
import { DEFAULT_MCP_TOOL_TIMEOUT_MS, getMcpToolTimeout, extractMaxResultSizeChars } from '../src/tools/mcp-tool';

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

describe('MCP capability discovery retry', () => {
  // Behavior-based (ESM-safe): assert the list methods route through the retry
  // wrapper and callTool does not, by mocking the seams on a ready client — not
  // by scraping source text. The methods paginate via collectPaginated, which
  // itself calls sendWithRetry, so the retry guarantee is preserved.
  function readyClient(): McpClient {
    const c = new McpClient({ command: 'node', args: [] });
    (c as unknown as { _state: string })._state = 'ready';
    return c;
  }

  it('list methods route through sendWithRetry; callTool goes direct (no auto-retry)', async () => {
    const c = readyClient();
    // Return a shape carrying every list key so each method is satisfied.
    const retry = mock.method(
      c as unknown as { sendWithRetry: (m: string) => Promise<unknown> },
      'sendWithRetry',
      async () => ({ tools: [], resources: [], prompts: [] }),
    );
    const auto = mock.method(
      c as unknown as { sendRequestAuto: (m: string) => Promise<unknown> },
      'sendRequestAuto',
      async () => ({ content: [] }),
    );
    await c.listTools();
    await c.listResources();
    await c.listPrompts();
    const retried = retry.mock.calls.map((call) => (call.arguments as unknown[])[0]);
    assert.ok(retried.includes('tools/list'));
    assert.ok(retried.includes('resources/list'));
    assert.ok(retried.includes('prompts/list'));
    await c.callTool('x', {});
    assert.ok(!retry.mock.calls.some((call) => (call.arguments as unknown[])[0] === 'tools/call'));
    assert.ok(auto.mock.calls.some((call) => (call.arguments as unknown[])[0] === 'tools/call'));
  });

  it('sendWithRetry retries a transient connect error then succeeds', async () => {
    const c = readyClient();
    let calls = 0;
    mock.method(
      c as unknown as { sendRequestAuto: (m: string, p: object) => Promise<unknown> },
      'sendRequestAuto',
      async () => {
        calls++;
        if (calls === 1) throw new Error('socket hang up'); // transient per isTransientConnectError
        return { tools: [{ name: 't', description: '', inputSchema: {} }] };
      },
    );
    const tools = await c.listTools();
    assert.equal(tools.length, 1);
    assert.equal(calls, 2); // one retry
  });
});

describe('MCP maxResultSizeChars annotation', () => {
  it('extracts maxResultSizeChars from _meta.anthropic/maxResultSizeChars', () => {
    const result = { _meta: { 'anthropic/maxResultSizeChars': 250000 }, content: 'data' };
    assert.equal(extractMaxResultSizeChars(result), 250000);
  });

  it('extracts maxResultSizeChars from _meta.maxResultSizeChars', () => {
    const result = { _meta: { maxResultSizeChars: 100000 }, content: 'data' };
    assert.equal(extractMaxResultSizeChars(result), 100000);
  });

  it('caps at 500000', () => {
    const result = { _meta: { 'anthropic/maxResultSizeChars': 999999 } };
    assert.equal(extractMaxResultSizeChars(result), 500000);
  });

  it('returns undefined for missing _meta', () => {
    assert.equal(extractMaxResultSizeChars({ content: 'data' }), undefined);
  });

  it('returns undefined for non-object result', () => {
    assert.equal(extractMaxResultSizeChars('string'), undefined);
    assert.equal(extractMaxResultSizeChars(null), undefined);
  });

  it('returns undefined for non-positive values', () => {
    assert.equal(extractMaxResultSizeChars({ _meta: { maxResultSizeChars: 0 } }), undefined);
    assert.equal(extractMaxResultSizeChars({ _meta: { maxResultSizeChars: -100 } }), undefined);
  });
});
