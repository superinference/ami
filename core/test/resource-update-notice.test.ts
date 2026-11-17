/**
 * resource-update-notice.test.ts — MCP resource-update provenance notices.
 *
 * Covers the pure formatter (single-resource vs whole-list vs reason suffix) and
 * the drainable buffer attached to a REAL McpManager (an EventEmitter), proving
 * the buffer wires to the exact manager event names (`resource-updated` /
 * `resources-changed`) the engine relies on and surfaces each update exactly once.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';

import {
  formatResourceUpdateNotice,
  createMcpResourceNoticeBuffer,
} from '../src/mcp/resource-update-notice';
import { McpManager } from '../src/mcp';

describe('formatResourceUpdateNotice', () => {
  it('formats a single-resource update with server provenance', () => {
    assert.equal(
      formatResourceUpdateNotice({ server: 'files', uri: 'file:///notes.md' }),
      'MCP resource updated — files: file:///notes.md',
    );
  });

  it('formats a whole-list change (empty uri) without a target', () => {
    assert.equal(
      formatResourceUpdateNotice({ server: 'files', uri: '', reason: 'list_changed' }),
      'MCP resources changed — files · list_changed',
    );
  });

  it('appends an optional reason suffix to a single-resource update', () => {
    assert.equal(
      formatResourceUpdateNotice({ server: 'db', uri: 'db://rows', reason: 'invalidated' }),
      'MCP resource updated — db: db://rows · invalidated',
    );
  });

  it('trims whitespace-only uri down to a list-style line', () => {
    assert.equal(
      formatResourceUpdateNotice({ server: 'x', uri: '   ' }),
      'MCP resources changed — x',
    );
  });
});

describe('createMcpResourceNoticeBuffer', () => {
  it('buffers resource-updated as a single-resource notice and drains it once', () => {
    const src = new EventEmitter();
    const buf = createMcpResourceNoticeBuffer(src);
    assert.equal(buf.size, 0);
    assert.deepEqual(buf.drain(), []);

    src.emit('resource-updated', 'files', 'file:///a.txt');
    assert.equal(buf.size, 1);
    assert.deepEqual(buf.drain(), [{ server: 'files', uri: 'file:///a.txt' }]);
    // Exactly once — a second drain is empty.
    assert.deepEqual(buf.drain(), []);
    assert.equal(buf.size, 0);
  });

  it('buffers resources-changed as a list-change notice with reason', () => {
    const src = new EventEmitter();
    const buf = createMcpResourceNoticeBuffer(src);
    src.emit('resources-changed', 'tools');
    assert.deepEqual(buf.drain(), [{ server: 'tools', uri: '', reason: 'list_changed' }]);
  });

  it('preserves arrival order across both event kinds', () => {
    const src = new EventEmitter();
    const buf = createMcpResourceNoticeBuffer(src);
    src.emit('resource-updated', 's1', 'u1');
    src.emit('resources-changed', 's2');
    src.emit('resource-updated', 's3', 'u3');
    assert.deepEqual(buf.drain(), [
      { server: 's1', uri: 'u1' },
      { server: 's2', uri: '', reason: 'list_changed' },
      { server: 's3', uri: 'u3' },
    ]);
  });

  it('coerces a non-string uri to empty', () => {
    const src = new EventEmitter();
    const buf = createMcpResourceNoticeBuffer(src);
    src.emit('resource-updated', 'srv', undefined as unknown as string);
    assert.deepEqual(buf.drain(), [{ server: 'srv', uri: '' }]);
  });

  it('attaches to a real McpManager and captures its emitted events (integration)', () => {
    const mgr = new McpManager();
    const buf = createMcpResourceNoticeBuffer(mgr);
    // The manager re-emits client notifications as these exact event names
    // (manager.ts: resource-updated(server, uri), resources-changed(server)).
    mgr.emit('resource-updated', 'remote', 'res://x');
    mgr.emit('resources-changed', 'remote');
    assert.deepEqual(buf.drain(), [
      { server: 'remote', uri: 'res://x' },
      { server: 'remote', uri: '', reason: 'list_changed' },
    ]);
    mgr.disconnectAll();
  });
});
