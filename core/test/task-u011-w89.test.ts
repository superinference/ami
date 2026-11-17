import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  taskTool,
  resetAgentRegistry,
  normalizeMcpServerState,
  readMcpServerStates,
  ensureRequiredMcpServersReady,
  REQUIRED_MCP_POLL_INTERVAL_MS,
  REQUIRED_MCP_MAX_WAIT_MS,
} from '../src/tools/task';
import type { ToolContext, ToolDefinition } from '../src/types';

// ---------------------------------------------------------------------------
// gap U011 (w89): Required-MCP-server readiness poll before spawn, with
// early-exit on a failed server. Parity: Claude AgentTool.tsx:L367-410.
// ---------------------------------------------------------------------------

describe('normalizeMcpServerState', () => {
  it('maps fully-ready states to connected', () => {
    assert.equal(normalizeMcpServerState('ready'), 'connected');
    assert.equal(normalizeMcpServerState('connected'), 'connected');
  });
  it('maps still-connecting states to pending', () => {
    assert.equal(normalizeMcpServerState('pending'), 'pending');
    assert.equal(normalizeMcpServerState('connecting'), 'pending');
  });
  it('maps every terminal non-ready state to failed (fail-fast)', () => {
    for (const s of ['error', 'needs-auth', 'disconnected', 'stopped', 'disabled', 'whatever']) {
      assert.equal(normalizeMcpServerState(s), 'failed', `state ${s}`);
    }
  });
});

describe('readMcpServerStates', () => {
  it('returns {} when no manager is wired', () => {
    assert.deepEqual(readMcpServerStates({} as unknown as ToolContext), {});
  });
  it('returns {} when listAllServers is not a function', () => {
    const ctx = { _mcpManager: { listAllServers: 123 } } as unknown as ToolContext;
    assert.deepEqual(readMcpServerStates(ctx), {});
  });
  it('returns {} when listAllServers throws', () => {
    const ctx = { _mcpManager: { listAllServers: () => { throw new Error('boom'); } } } as unknown as ToolContext;
    assert.deepEqual(readMcpServerStates(ctx), {});
  });
  it('normalizes each listed server state and skips malformed entries', () => {
    const ctx = {
      _mcpManager: {
        listAllServers: () => [
          { name: 'db', state: 'ready' },
          { name: 'search', state: 'connecting' },
          { name: 'broken', state: 'error' },
          { name: 123 as unknown as string, state: 'ready' }, // malformed name
          null,
        ],
      },
    } as unknown as ToolContext;
    assert.deepEqual(readMcpServerStates(ctx), { db: 'connected', search: 'pending', broken: 'failed' });
  });
});

describe('ensureRequiredMcpServersReady', () => {
  const label = 'db-agent';
  const neverSleep = async () => { throw new Error('should not poll'); };

  it('is ready with no requirements (undefined / empty) without polling', async () => {
    assert.deepEqual(await ensureRequiredMcpServersReady(undefined, () => ({}), label, { sleep: neverSleep }), { ready: true });
    assert.deepEqual(await ensureRequiredMcpServersReady([], () => ({}), label, { sleep: neverSleep }), { ready: true });
  });

  it('is immediately ready when all required servers are connected (no poll)', async () => {
    const res = await ensureRequiredMcpServersReady(['db'], () => ({ db: 'connected' }), label, { sleep: neverSleep });
    assert.deepEqual(res, { ready: true });
  });

  it('fails fast (no poll) when a required server is already failed', async () => {
    const res = await ensureRequiredMcpServersReady(['db'], () => ({ db: 'failed', notes: 'connected' }), label, { sleep: neverSleep });
    assert.equal(res.ready, false);
    if (res.ready) return;
    assert.match(res.error, /requires MCP server\(s\) matching: db/);
    assert.match(res.error, /Connected MCP servers: notes/);
    assert.match(res.error, /\/mcp/);
  });

  it('fails fast (no poll) when a required server is missing, reporting none connected', async () => {
    const res = await ensureRequiredMcpServersReady(['db'], () => ({}), label, { sleep: neverSleep });
    assert.equal(res.ready, false);
    if (res.ready) return;
    assert.match(res.error, /matching: db/);
    assert.match(res.error, /Connected MCP servers: none/);
  });

  it('polls a pending server and succeeds once it connects', async () => {
    let tick = 0;
    const states: Record<string, string> = { db: 'pending' };
    const res = await ensureRequiredMcpServersReady(['db'], () => states, label, {
      sleep: async () => { tick++; if (tick >= 2) states.db = 'connected'; },
      now: () => tick * REQUIRED_MCP_POLL_INTERVAL_MS,
      maxWaitMs: 10 * REQUIRED_MCP_POLL_INTERVAL_MS,
    });
    assert.deepEqual(res, { ready: true });
    assert.equal(tick, 2); // polled twice, then satisfied
  });

  it('early-exits the poll loop the moment a pending required server fails', async () => {
    let tick = 0;
    const states: Record<string, string> = { db: 'pending' };
    const res = await ensureRequiredMcpServersReady(['db'], () => states, label, {
      sleep: async () => { tick++; if (tick >= 1) states.db = 'failed'; },
      now: () => tick * REQUIRED_MCP_POLL_INTERVAL_MS,
      maxWaitMs: 100 * REQUIRED_MCP_POLL_INTERVAL_MS,
    });
    assert.equal(res.ready, false);
    assert.equal(tick, 1); // did NOT burn the whole ceiling
  });

  it('errors when a pending server never resolves before the deadline', async () => {
    let tick = 0;
    const res = await ensureRequiredMcpServersReady(['db'], () => ({ db: 'pending' }), label, {
      sleep: async () => { tick++; },
      now: () => tick * REQUIRED_MCP_POLL_INTERVAL_MS,
      maxWaitMs: 3 * REQUIRED_MCP_POLL_INTERVAL_MS,
    });
    assert.equal(res.ready, false);
    if (res.ready) return;
    assert.match(res.error, /matching: db/);
  });

  it('exposes the real poll ceiling + interval constants', () => {
    assert.equal(REQUIRED_MCP_MAX_WAIT_MS, 30_000);
    assert.equal(REQUIRED_MCP_POLL_INTERVAL_MS, 500);
  });
});

// ---------------------------------------------------------------------------
// End-to-end through taskTool.execute: the gate blocks / allows a real spawn.
// ---------------------------------------------------------------------------

function skillManagerWith(agent: Record<string, unknown>) {
  return {
    getAgent: (n: string) => (n === agent.name ? agent : null),
    listAgents: () => [agent],
  } as unknown as ToolContext['_skillManager'];
}

async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext>,
): Promise<{ isError?: boolean; output: string; factoryCalled: boolean }> {
  let factoryCalled = false;
  async function* fakeSubmit() {
    yield { type: 'text_delta', text: 'done' } as never;
  }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c) => {
      factoryCalled = true;
      void (c.tools as ToolDefinition[]);
      return { submit: fakeSubmit };
    },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, factoryCalled };
}

describe('taskTool required-MCP-server spawn gate (E2E)', () => {
  beforeEach(() => resetAgentRegistry());

  it('blocks the spawn with a guided error when a required server is unavailable', async () => {
    const res = await runTask(
      { description: 'query db', prompt: 'look it up', subagent_type: 'db-agent' },
      {
        _skillManager: skillManagerWith({ name: 'db-agent', requiredMcpServers: ['postgres'] }),
        _mcpManager: { listAllServers: () => [{ name: 'files', state: 'ready' }] } as unknown as ToolContext['_mcpManager'],
      },
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /requires MCP server\(s\) matching: postgres/);
    assert.match(res.output, /Connected MCP servers: files/);
    assert.match(res.output, /\/mcp/);
    assert.equal(res.factoryCalled, false, 'sub-engine must NOT be built when a required server is missing');
  });

  it('allows the spawn when the required server is connected', async () => {
    const res = await runTask(
      { description: 'query db', prompt: 'look it up', subagent_type: 'db-agent' },
      {
        _skillManager: skillManagerWith({ name: 'db-agent', requiredMcpServers: ['postgres'] }),
        _mcpManager: { listAllServers: () => [{ name: 'postgres', state: 'ready' }] } as unknown as ToolContext['_mcpManager'],
      },
    );
    assert.notEqual(res.isError, true);
    assert.equal(res.factoryCalled, true, 'sub-engine must be built once requirements are satisfied');
  });

  it('does not gate an agent without requiredMcpServers', async () => {
    const res = await runTask(
      { description: 'plain', prompt: 'go', subagent_type: 'plain-agent' },
      { _skillManager: skillManagerWith({ name: 'plain-agent' }) },
    );
    assert.notEqual(res.isError, true);
    assert.equal(res.factoryCalled, true);
  });
});
