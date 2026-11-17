/**
 * U018 (w85) — MCP tool-polling schema + scheduler (parity: Claude Code's
 * `mcp-polling-update`).
 *
 * Parity gap (UserResourceUpdateMessage.tsx:L34-43): Claude surfaces a second
 * inbound MCP update kind — a periodically re-invoked ("polled") tool whose output
 * changed — as an `<mcp-polling-update type="tool" server="…" tool="…"><reason>…`
 * block. AMI had NO polling concept at all.
 *
 * This suite covers the core building block {@link McpToolPoller} (change detection,
 * baseline, overlap guard, error handling, timer scheduling, register/unregister/
 * stopAll), the {@link createMcpToolPollingNoticeBuffer} drain contract,
 * {@link formatToolPollingNotice}, and the {@link McpManager} wiring (config
 * `pollTools` → register on `ready`, emit `tool-polling-update`, unregister on
 * close/remove/disconnectAll).
 *
 * RED on revert: before `tool-polling.ts` existed the imports throw
 * (`McpToolPoller is not a constructor` / undefined), and `McpServerConfig.pollTools`
 * is unknown; the manager never registers a poll nor emits `tool-polling-update`.
 */
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import {
  McpToolPoller,
  createMcpToolPollingNoticeBuffer,
  formatToolPollingNotice,
  TOOL_POLLING_CHANGED_REASON,
  type McpToolPollingUpdate,
  type PollTimerHandle,
} from '../src/mcp/tool-polling';
import { McpManager } from '../src/mcp/manager';
import type { McpServerConfig } from '../src/mcp/manager';

describe('McpToolPoller — change detection', () => {
  it('fires only when a polled tool\'s output changes (first poll is a baseline)', async () => {
    let value = 'a';
    const updates: McpToolPollingUpdate[] = [];
    const poller = new McpToolPoller({
      call: async () => value,
      onUpdate: (u) => updates.push(u),
    });
    poller.register('srv', [{ tool: 't', intervalMs: 1000 }]);

    await poller.poll('srv', 't'); // baseline — no emit
    assert.deepEqual(updates, []);
    await poller.poll('srv', 't'); // unchanged — no emit
    assert.deepEqual(updates, []);
    value = 'b';
    await poller.poll('srv', 't'); // changed — emit
    assert.deepEqual(updates, [{ server: 'srv', tool: 't', reason: TOOL_POLLING_CHANGED_REASON }]);
    await poller.poll('srv', 't'); // stable at 'b' again — no further emit
    assert.equal(updates.length, 1);
    poller.stopAll();
  });

  it('passes the spec args to the tool call', async () => {
    const seen: Array<Record<string, unknown> | undefined> = [];
    const poller = new McpToolPoller({
      call: async (_s, _t, args) => { seen.push(args); return 1; },
      onUpdate: () => {},
    });
    poller.register('srv', [{ tool: 't', intervalMs: 1000, args: { q: 'x' } }]);
    await poller.poll('srv', 't');
    assert.deepEqual(seen, [{ q: 'x' }]);
    poller.stopAll();
  });

  it('a failed poll call does not emit, does not update the baseline, and reports onError', async () => {
    let mode: 'ok1' | 'throw' | 'ok2' = 'ok1';
    const updates: McpToolPollingUpdate[] = [];
    const errors: Array<[string, string]> = [];
    const poller = new McpToolPoller({
      call: async () => {
        if (mode === 'throw') throw new Error('boom');
        return mode === 'ok1' ? 'A' : 'B';
      },
      onUpdate: (u) => updates.push(u),
      onError: (s, t, err) => errors.push([`${s}:${t}`, (err as Error).message]),
    });
    poller.register('srv', [{ tool: 't', intervalMs: 1000 }]);
    await poller.poll('srv', 't'); // baseline 'A'
    mode = 'throw';
    await poller.poll('srv', 't'); // throws → onError, baseline stays 'A'
    assert.deepEqual(errors, [['srv:t', 'boom']]);
    assert.deepEqual(updates, []);
    mode = 'ok2';
    await poller.poll('srv', 't'); // 'B' !== baseline 'A' → emit (baseline was preserved across the error)
    assert.deepEqual(updates, [{ server: 'srv', tool: 't', reason: TOOL_POLLING_CHANGED_REASON }]);
    poller.stopAll();
  });

  it('guards against overlapping async polls of the same tool', async () => {
    let calls = 0;
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const poller = new McpToolPoller({
      call: async () => { calls++; await gate; return calls; },
      onUpdate: () => {},
    });
    poller.register('srv', [{ tool: 't', intervalMs: 1000 }]);
    const p1 = poller.poll('srv', 't');
    const p2 = poller.poll('srv', 't'); // in-flight → skipped
    release();
    await Promise.all([p1, p2]);
    assert.equal(calls, 1);
    poller.stopAll();
  });

  it('poll() on an unknown (server,tool) is a no-op', async () => {
    const updates: McpToolPollingUpdate[] = [];
    const poller = new McpToolPoller({ call: async () => 1, onUpdate: (u) => updates.push(u) });
    await poller.poll('nope', 'nope');
    assert.deepEqual(updates, []);
  });
});

describe('McpToolPoller — registration & scheduling', () => {
  it('skips specs with a non-positive / non-finite interval or empty tool name', () => {
    const poller = new McpToolPoller({ call: async () => 1, onUpdate: () => {} });
    poller.register('srv', [
      { tool: 't1', intervalMs: 0 },
      { tool: 't2', intervalMs: -5 },
      { tool: 't3', intervalMs: Number.NaN },
      { tool: '', intervalMs: 1000 },
      { tool: 't5', intervalMs: 1000 },
    ]);
    assert.equal(poller.size, 1);
    poller.stopAll();
  });

  it('register replaces a server\'s prior specs (no duplicate timers on reconnect)', () => {
    const cleared: PollTimerHandle[] = [];
    let n = 0;
    const poller = new McpToolPoller({
      call: async () => 1,
      onUpdate: () => {},
      setTimer: () => ++n,
      clearTimer: (h) => cleared.push(h),
    });
    poller.register('srv', [{ tool: 'a', intervalMs: 1000 }, { tool: 'b', intervalMs: 1000 }]);
    assert.equal(poller.size, 2);
    poller.register('srv', [{ tool: 'c', intervalMs: 1000 }]);
    assert.equal(poller.size, 1);
    assert.equal(cleared.length, 2); // both prior timers cleared
    poller.stopAll();
  });

  it('register(server, undefined/[]) clears and registers nothing', () => {
    const poller = new McpToolPoller({ call: async () => 1, onUpdate: () => {} });
    poller.register('srv', [{ tool: 'a', intervalMs: 1000 }]);
    poller.register('srv', undefined);
    assert.equal(poller.size, 0);
    poller.register('srv', []);
    assert.equal(poller.size, 0);
  });

  it('the injected timer schedules a poll cycle that emits on change', async () => {
    let scheduled: (() => void) | undefined;
    let value = 1;
    const updates: McpToolPollingUpdate[] = [];
    const poller = new McpToolPoller({
      call: async () => value,
      onUpdate: (u) => updates.push(u),
      setTimer: (fn) => { scheduled = fn; return 1; },
      clearTimer: () => {},
    });
    poller.register('srv', [{ tool: 't', intervalMs: 5 }]);
    assert.equal(typeof scheduled, 'function');
    scheduled!(); await Promise.resolve(); await Promise.resolve(); // baseline
    value = 2;
    scheduled!(); await Promise.resolve(); await Promise.resolve(); // change → emit
    assert.equal(updates.length, 1);
    poller.stopAll();
  });

  it('unregister stops only the named server; stopAll clears everything', () => {
    const poller = new McpToolPoller({ call: async () => 1, onUpdate: () => {} });
    poller.register('a', [{ tool: 't', intervalMs: 1000 }]);
    poller.register('b', [{ tool: 't', intervalMs: 1000 }]);
    assert.equal(poller.size, 2);
    poller.unregister('a');
    assert.equal(poller.size, 1);
    poller.stopAll();
    assert.equal(poller.size, 0);
  });

  it('default serialize handles non-JSON-serializable results without throwing', async () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    const updates: McpToolPollingUpdate[] = [];
    let result: unknown = circular;
    const poller = new McpToolPoller({ call: async () => result, onUpdate: (u) => updates.push(u) });
    poller.register('srv', [{ tool: 't', intervalMs: 1000 }]);
    await poller.poll('srv', 't'); // baseline via String() fallback
    result = { different: true };
    await poller.poll('srv', 't'); // changed
    assert.equal(updates.length, 1);
    poller.stopAll();
  });
});

describe('createMcpToolPollingNoticeBuffer', () => {
  it('buffers tool-polling-update events and drains them exactly once', () => {
    const src = new EventEmitter();
    const buf = createMcpToolPollingNoticeBuffer(src);
    assert.equal(buf.size, 0);
    assert.deepEqual(buf.drain(), []); // empty drain short-circuits
    src.emit('tool-polling-update', 'srv', 'a', 'tool output changed');
    src.emit('tool-polling-update', 'srv', 'b');
    assert.equal(buf.size, 2);
    assert.deepEqual(buf.drain(), [
      { server: 'srv', tool: 'a', reason: 'tool output changed' },
      { server: 'srv', tool: 'b' },
    ]);
    assert.equal(buf.size, 0);
    assert.deepEqual(buf.drain(), []);
  });
});

describe('formatToolPollingNotice', () => {
  it('renders server:tool with an optional reason suffix', () => {
    assert.equal(formatToolPollingNotice({ server: 's', tool: 't' }), 'MCP tool polled — s: t');
    assert.equal(
      formatToolPollingNotice({ server: 's', tool: 't', reason: 'tool output changed' }),
      'MCP tool polled — s: t · tool output changed',
    );
  });
});

// --- Manager wiring -------------------------------------------------------

class PollFakeClient extends EventEmitter {
  state = 'idle';
  serverInfo: unknown = { name: 'srv', version: '1' };
  instructions: string | null = null;
  uptime: number | null = 1;
  tools: Array<{ name: string; description?: string }> = [{ name: 't' }];
  toolResult: unknown = 'v0';
  onElicitation: unknown;
  onSampling: unknown;
  async connect(): Promise<void> { this.state = 'ready'; this.emit('ready'); }
  async connectHTTP(): Promise<void> { return this.connect(); }
  async connectSSE(): Promise<void> { return this.connect(); }
  async connectWebSocket(): Promise<void> { return this.connect(); }
  async listTools(): Promise<Array<{ name: string; description?: string }>> { return this.tools; }
  async listResources(): Promise<unknown[]> { return []; }
  async listResourceTemplates(): Promise<unknown[]> { return []; }
  async listPrompts(): Promise<unknown[]> { return []; }
  async callTool(): Promise<unknown> { return this.toolResult; }
  disconnect(): void { this.state = 'stopped'; this.emit('close', 0, 1); }
  setHeaders(): void {}
  async revokeAuth(): Promise<void> {}
}

describe('McpManager — pollTools wiring', () => {
  let mgr: McpManager;
  afterEach(() => { mgr?.disconnectAll(); });

  async function connectedManager(pollTools: McpServerConfig['pollTools']): Promise<{ mgr: McpManager; client: PollFakeClient }> {
    mgr = new McpManager();
    const client = new PollFakeClient();
    mgr._createClient = (() => client) as never;
    const cfg: McpServerConfig = { command: 'node', pollTools };
    mgr.addServer('srv', cfg);
    await mgr.ensureConnected('srv');
    return { mgr, client };
  }

  it('registers configured polls on ready and emits tool-polling-update on a change', async () => {
    const { mgr, client } = await connectedManager([{ tool: 't', intervalMs: 999999 }]);
    const poller = (mgr as unknown as { _toolPoller: McpToolPoller })._toolPoller;
    assert.equal(poller.size, 1);

    const events: Array<[string, string, string | undefined]> = [];
    mgr.on('tool-polling-update', (s: string, t: string, r?: string) => events.push([s, t, r]));

    await poller.poll('srv', 't'); // baseline 'v0'
    assert.deepEqual(events, []);
    client.toolResult = 'v1';
    await poller.poll('srv', 't'); // changed → manager re-emits
    assert.deepEqual(events, [['srv', 't', TOOL_POLLING_CHANGED_REASON]]);
  });

  it('registers nothing when the server has no pollTools', async () => {
    const { mgr } = await connectedManager(undefined);
    const poller = (mgr as unknown as { _toolPoller: McpToolPoller })._toolPoller;
    assert.equal(poller.size, 0);
  });

  it('unregisters the server\'s polls when the transport closes', async () => {
    const { mgr, client } = await connectedManager([{ tool: 't', intervalMs: 999999 }]);
    const poller = (mgr as unknown as { _toolPoller: McpToolPoller })._toolPoller;
    assert.equal(poller.size, 1);
    client.emit('close', 0, 1);
    assert.equal(poller.size, 0);
  });

  it('removeServer stops the server\'s polls', async () => {
    const { mgr } = await connectedManager([{ tool: 't', intervalMs: 999999 }]);
    const poller = (mgr as unknown as { _toolPoller: McpToolPoller })._toolPoller;
    assert.equal(poller.size, 1);
    mgr.removeServer('srv');
    assert.equal(poller.size, 0);
  });

  it('disconnectAll tears down all polling timers', async () => {
    const { mgr } = await connectedManager([{ tool: 't', intervalMs: 999999 }]);
    const poller = (mgr as unknown as { _toolPoller: McpToolPoller })._toolPoller;
    assert.equal(poller.size, 1);
    mgr.disconnectAll();
    assert.equal(poller.size, 0);
  });
});
