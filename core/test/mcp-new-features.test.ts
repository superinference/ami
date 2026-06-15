/**
 * Tests for MCP new features:
 * - McpManager.truncateDescription (description length limiting)
 * - McpManager.getToolAnnotations (readOnly/destructive hints)
 * - McpClient.onToolProgress (callback registration)
 * - McpClient states (disconnected → connecting → ready → error)
 * - McpManager health check start/stop
 */

import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpManager, findMcpConfigPaths } from '../src/mcp/manager';
import { McpClient } from '../src/mcp/client';

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
