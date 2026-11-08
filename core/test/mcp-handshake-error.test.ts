import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import { McpManager, isHandshakeFailure, handshakeError } from '../src/mcp/manager';
import { MCP_PROTOCOL_VERSION } from '../src/mcp/client';

// M3: AMI does NOT negotiate backwards compatibility with legacy MCP servers.
// When an old/non-compliant server fails the handshake, the error must clearly
// name the protocol-version incompatibility instead of a generic failure.

describe('isHandshakeFailure (M3)', () => {
  it('treats a process-exit during handshake as a handshake failure', () => {
    assert.equal(isHandshakeFailure(new Error('MCP server exited with code 1')), true);
  });
  it('treats an initialize/protocol error as a handshake failure', () => {
    assert.equal(isHandshakeFailure(new Error('initialize failed: unknown method')), true);
    assert.equal(isHandshakeFailure(new Error('unsupported protocol version 2025-06-18')), true);
    assert.equal(isHandshakeFailure(new Error('bad protocolVersion')), true);
  });
  it('does NOT mislabel a transient network error as a handshake failure', () => {
    assert.equal(isHandshakeFailure(new Error('connect ECONNREFUSED 127.0.0.1:9')), false);
    assert.equal(isHandshakeFailure(new Error('socket hang up')), false);
  });
  it('does not match an unrelated runtime error', () => {
    assert.equal(isHandshakeFailure(new Error('tool threw TypeError')), false);
  });
});

describe('handshakeError (M3)', () => {
  it('names the protocol version AMI speaks and preserves the original error', () => {
    const e = handshakeError('legacy-srv', new Error('MCP server exited with code 1'));
    assert.match(e.message, /legacy-srv/);
    assert.match(e.message, new RegExp(MCP_PROTOCOL_VERSION));
    assert.match(e.message, /older or non-compliant/);
    assert.match(e.message, /Original error: MCP server exited with code 1/);
  });
});

// Minimal fake client to drive connectServer deterministically (no subprocess).
class FakeClient extends EventEmitter {
  onElicitation: unknown;
  onSampling: unknown;
  constructor(private failMsg: string) { super(); }
  async connect(): Promise<void> { throw new Error(this.failMsg); }
  async connectHTTP(): Promise<void> { throw new Error(this.failMsg); }
  async connectSSE(): Promise<void> { throw new Error(this.failMsg); }
  async listTools(): Promise<unknown[]> { return []; }
  disconnect(): void {}
}

describe('connectServer surfaces a clear handshake error (M3)', () => {
  let mgr: McpManager;
  beforeEach(() => { mgr = new McpManager(); });

  it('wraps a legacy-server handshake failure with actionable messaging', async () => {
    mgr._createClient = (() => new FakeClient('MCP server exited with code 1')) as never;
    mgr.addServer('legacy', { command: 'legacy-mcp' });
    await assert.rejects(
      () => mgr.connectServer('legacy'),
      (e: Error) => {
        assert.match(e.message, /failed the MCP handshake/);
        assert.match(e.message, new RegExp(MCP_PROTOCOL_VERSION));
        return true;
      },
    );
  });
});
