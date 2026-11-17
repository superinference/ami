/**
 * connection-notice.test.ts — MCP connection-change provenance notices (U006).
 *
 * Covers the pure formatter (lost / recovered / failed, with/without detail) and the
 * drainable buffer attached to a REAL McpManager (an EventEmitter), proving the buffer
 * wires to the exact manager event names (`server-state-changed` / `reconnect-failed`)
 * the engine relies on, classifies only USER-RELEVANT transitions (no noise on the
 * initial connect / transient churn), and surfaces each change exactly once.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';

import {
  formatMcpConnectionChange,
  createMcpConnectionNoticeBuffer,
} from '../src/mcp/connection-notice';
import { McpManager } from '../src/mcp';

describe('formatMcpConnectionChange', () => {
  it('formats a lost connection with its raw-state detail', () => {
    assert.equal(
      formatMcpConnectionChange({ server: 'docs', state: 'lost', detail: 'stopped' }),
      'MCP server connection lost — docs (stopped)',
    );
  });

  it('formats a recovery with no detail', () => {
    assert.equal(
      formatMcpConnectionChange({ server: 'docs', state: 'recovered' }),
      'MCP server reconnected — docs',
    );
  });

  it('formats a reconnection failure with its attempt count', () => {
    assert.equal(
      formatMcpConnectionChange({ server: 'db', state: 'failed', detail: 'after 5 attempts' }),
      'MCP server reconnection failed — db (after 5 attempts)',
    );
  });

  it('omits a whitespace-only detail suffix', () => {
    assert.equal(
      formatMcpConnectionChange({ server: 'x', state: 'lost', detail: '   ' }),
      'MCP server connection lost — x',
    );
  });
});

describe('createMcpConnectionNoticeBuffer', () => {
  it('does NOT notify on the initial connect (no prior ready state)', () => {
    const src = new EventEmitter();
    const buf = createMcpConnectionNoticeBuffer(src);
    src.emit('server-state-changed', 'srv', 'connecting');
    src.emit('server-state-changed', 'srv', 'ready');
    assert.equal(buf.size, 0);
    assert.deepEqual(buf.drain(), []);
  });

  it('buffers a drop (ready → non-ready) exactly once, with the raw state as detail', () => {
    const src = new EventEmitter();
    const buf = createMcpConnectionNoticeBuffer(src);
    src.emit('server-state-changed', 'srv', 'ready');
    src.emit('server-state-changed', 'srv', 'stopped');
    // A further non-ready churn while already lost does not re-buffer.
    src.emit('server-state-changed', 'srv', 'connecting');
    assert.deepEqual(buf.drain(), [{ server: 'srv', state: 'lost', detail: 'stopped' }]);
    assert.deepEqual(buf.drain(), []);
  });

  it('buffers a recovery only after a real drop', () => {
    const src = new EventEmitter();
    const buf = createMcpConnectionNoticeBuffer(src);
    src.emit('server-state-changed', 'srv', 'ready');
    src.emit('server-state-changed', 'srv', 'stopped'); // lost
    src.emit('server-state-changed', 'srv', 'ready');    // recovered
    assert.deepEqual(buf.drain(), [
      { server: 'srv', state: 'lost', detail: 'stopped' },
      { server: 'srv', state: 'recovered' },
    ]);
  });

  it('buffers reconnect-failed as a failed notice with the attempt count (singular/plural)', () => {
    const src = new EventEmitter();
    const buf = createMcpConnectionNoticeBuffer(src);
    src.emit('reconnect-failed', 'srv', 1);
    src.emit('reconnect-failed', 'srv', 3);
    assert.deepEqual(buf.drain(), [
      { server: 'srv', state: 'failed', detail: 'after 1 attempt' },
      { server: 'srv', state: 'failed', detail: 'after 3 attempts' },
    ]);
  });

  it('tracks lost/recovered state independently per server', () => {
    const src = new EventEmitter();
    const buf = createMcpConnectionNoticeBuffer(src);
    src.emit('server-state-changed', 'a', 'ready');
    src.emit('server-state-changed', 'b', 'ready');
    src.emit('server-state-changed', 'a', 'stopped'); // a lost
    src.emit('server-state-changed', 'b', 'ready');    // b still ready — no recovery (never lost)
    src.emit('server-state-changed', 'a', 'ready');    // a recovered
    assert.deepEqual(buf.drain(), [
      { server: 'a', state: 'lost', detail: 'stopped' },
      { server: 'a', state: 'recovered' },
    ]);
  });

  it('attaches to a real McpManager and captures its emitted events (integration)', () => {
    const mgr = new McpManager();
    const buf = createMcpConnectionNoticeBuffer(mgr);
    // The manager re-emits connection lifecycle as these exact event names
    // (manager.ts: server-state-changed(server, state), reconnect-failed(server, attempts)).
    mgr.emit('server-state-changed', 'remote', 'ready');
    mgr.emit('server-state-changed', 'remote', 'needs-auth');
    mgr.emit('reconnect-failed', 'remote', 2);
    assert.deepEqual(buf.drain(), [
      { server: 'remote', state: 'lost', detail: 'needs-auth' },
      { server: 'remote', state: 'failed', detail: 'after 2 attempts' },
    ]);
    mgr.disconnectAll();
  });
});
