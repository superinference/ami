import { describe, it, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import {
  McpManager,
  MAX_MCP_RECONNECT_ATTEMPTS,
  MCP_RECONNECT_BASE_DELAY_MS,
  MAX_MCP_RECONNECT_DELAY_MS,
} from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// H44: automatic reconnect uses bounded retries + exponential backoff, re-checks
// isDisabled before each attempt, and emits 'reconnect-failed' when exhausted.
// ---------------------------------------------------------------------------

class FakeClient extends EventEmitter {
  state = 'ready';
  _tools: any[] = [];
  behavior: Record<string, any> = {};
  async reconnect() { if (this.behavior.reconnect) return this.behavior.reconnect(); }
  async listTools() { return this._tools; }
  disconnect() {}
}

/** Flush pending microtasks so an async setTimeout body settles before the next tick. */
async function flush() { for (let i = 0; i < 5; i++) await Promise.resolve(); }

describe('scheduleReconnect backoff (H44)', () => {
  it('retries with exponential backoff then emits reconnect-failed after the cap', async () => {
    mock.timers.enable({ apis: ['setTimeout'] });
    try {
      const mgr = new McpManager();
      const fake = new FakeClient();
      let attempts = 0;
      fake.behavior.reconnect = async () => { attempts++; throw new Error('boom'); };
      (mgr as any).clients.set('rf', fake);

      const failed: Array<[string, number]> = [];
      mgr.on('reconnect-failed', (name: string, n: number) => failed.push([name, n]));

      (mgr as any).scheduleReconnect('rf');

      // Drive through every backoff step. A generous tick fires whatever timer is
      // currently scheduled; flush() lets the async reconnect reject + reschedule.
      for (let i = 0; i < MAX_MCP_RECONNECT_ATTEMPTS + 1; i++) {
        mock.timers.tick(MAX_MCP_RECONNECT_DELAY_MS);
        await flush();
      }

      assert.equal(attempts, MAX_MCP_RECONNECT_ATTEMPTS, 'exactly MAX attempts made');
      assert.deepEqual(failed, [['rf', MAX_MCP_RECONNECT_ATTEMPTS]], 'reconnect-failed emitted once at the cap');
      assert.equal((mgr as any).reconnectAttempts.has('rf'), false, 'attempt counter cleared on give-up');
    } finally {
      mock.timers.reset();
    }
  });

  it('clears the attempt counter and refreshes tools on a successful reconnect', async () => {
    mock.timers.enable({ apis: ['setTimeout'] });
    try {
      const mgr = new McpManager();
      const fake = new FakeClient();
      fake._tools = [{ name: 't', description: '', inputSchema: {} }];
      let calls = 0;
      fake.behavior.reconnect = async () => { calls++; /* succeeds */ };
      (mgr as any).clients.set('rc', fake);

      (mgr as any).scheduleReconnect('rc');
      mock.timers.tick(MCP_RECONNECT_BASE_DELAY_MS);
      await flush();

      assert.equal(calls, 1);
      assert.equal((mgr as any).reconnectAttempts.has('rc'), false);
      assert.equal(mgr.getToolsForServer('rc').length, 1);
    } finally {
      mock.timers.reset();
    }
  });

  it('skips the reconnect when the server was disabled while the timer was pending', async () => {
    mock.timers.enable({ apis: ['setTimeout'] });
    try {
      const mgr = new McpManager();
      const fake = new FakeClient();
      let calls = 0;
      fake.behavior.reconnect = async () => { calls++; };
      (mgr as any).clients.set('dz', fake);
      // Mark disabled so isDisabled('dz') is true when the timer fires.
      (mgr as any).pendingServers.set('dz', { name: 'dz', config: {}, state: 'disabled' });

      (mgr as any).scheduleReconnect('dz');
      mock.timers.tick(MCP_RECONNECT_BASE_DELAY_MS);
      await flush();

      assert.equal(calls, 0, 'disabled server is not reconnected');
    } finally {
      mock.timers.reset();
    }
  });
});
