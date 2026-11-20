import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { createMcpConnectTool } from '../src/tools/mcp-connect';

// ---------------------------------------------------------------------------
// U268 — Cancellable MCP connect: active teardown of the half-open connection
// on abort (the reference-parity transport.close()/inProcessServer.close()).
//
// The tool already races ensureConnected() against the abort signal. This suite
// locks in the ADDITIVE behavior: when the race is settled by an abort, the tool
// actively tears the in-flight connection down via the manager (stopServer) so
// the transport / spawned process / container / socket is released promptly,
// instead of abandoning the ensureConnected() promise (which leaves the manager
// connecting in the background until its internal 120s timeout fires).
// ---------------------------------------------------------------------------

function baseContext(signal: AbortSignal, mcpManager: unknown) {
  return {
    cwd: '/tmp',
    abortSignal: signal,
    filesRead: new Set<string>(),
    _mcpManager: mcpManager,
  } as any;
}

describe('MCP connect abort teardown (U268)', () => {
  it('tears down the in-flight connection via stopServer when aborted mid-connect', async () => {
    const ac = new AbortController();
    const stopped: string[] = [];
    const mockMcpManager = {
      // Never resolves on its own — only the abort race can settle the connect.
      ensureConnected: () => new Promise<void>(() => { /* hang */ }),
      getToolsForServer: () => [{ name: 'nmap_scan' }],
      stopServer: async (name: string) => { stopped.push(name); },
    };
    const tool = createMcpConnectTool('kali', 'Start kali');
    const p = tool.execute({}, baseContext(ac.signal, mockMcpManager));
    setImmediate(() => ac.abort());
    const result = await p;

    assert.equal(result.isError, true);
    assert.ok(result.output.includes('cancelled'));
    assert.equal((result.metadata as any)?.status, 'aborted');
    // The half-open connection was actively released.
    assert.deepEqual(stopped, ['kali']);
  });

  it('does NOT tear down when the connect succeeds', async () => {
    const ac = new AbortController();
    let stopCalled = false;
    const mockMcpManager = {
      ensureConnected: async () => {},
      getToolsForServer: () => [{ name: 'nmap_scan' }],
      stopServer: async () => { stopCalled = true; },
    };
    const tool = createMcpConnectTool('kali', 'Start kali');
    const result = await tool.execute({}, baseContext(ac.signal, mockMcpManager));

    assert.ok(!result.isError);
    assert.equal(stopCalled, false, 'a successful connect must not be torn down');
  });

  it('does NOT tear down on a genuine connection failure (non-abort)', async () => {
    const ac = new AbortController();
    let stopCalled = false;
    const mockMcpManager = {
      ensureConnected: async () => { throw new Error('No container runtime'); },
      getToolsForServer: () => [],
      stopServer: async () => { stopCalled = true; },
    };
    const tool = createMcpConnectTool('kali', 'Start kali');
    const result = await tool.execute({}, baseContext(ac.signal, mockMcpManager));

    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Failed'));
    assert.equal((result.metadata as any)?.status, 'error');
    assert.equal(stopCalled, false, 'a hard failure already cleans up in the manager; no extra teardown');
  });

  it('does NOT tear down when the signal was already aborted before connecting', async () => {
    const ac = new AbortController();
    ac.abort();
    let stopCalled = false;
    let connectCalled = false;
    const mockMcpManager = {
      ensureConnected: async () => { connectCalled = true; },
      getToolsForServer: () => [],
      stopServer: async () => { stopCalled = true; },
    };
    const tool = createMcpConnectTool('kali', 'Start kali');
    const result = await tool.execute({}, baseContext(ac.signal, mockMcpManager));

    assert.equal(connectCalled, false, 'must short-circuit before connecting');
    assert.equal(stopCalled, false, 'nothing was started, so nothing to tear down');
    assert.equal((result.metadata as any)?.status, 'aborted');
  });

  it('still returns the aborted result if teardown itself throws (best-effort)', async () => {
    const ac = new AbortController();
    const mockMcpManager = {
      ensureConnected: () => new Promise<void>(() => {}),
      getToolsForServer: () => [],
      stopServer: async () => { throw new Error('teardown blew up'); },
    };
    const tool = createMcpConnectTool('kali', 'Start kali');
    const p = tool.execute({}, baseContext(ac.signal, mockMcpManager));
    setImmediate(() => ac.abort());
    const result = await p;

    assert.equal(result.isError, true);
    assert.ok(result.output.includes('cancelled'));
    assert.equal((result.metadata as any)?.status, 'aborted');
  });

  it('still returns the aborted result when the manager exposes no stopServer', async () => {
    const ac = new AbortController();
    const mockMcpManager = {
      ensureConnected: () => new Promise<void>(() => {}),
      getToolsForServer: () => [],
      // no stopServer — minimal manager shape must not crash the tool
    };
    const tool = createMcpConnectTool('kali', 'Start kali');
    const p = tool.execute({}, baseContext(ac.signal, mockMcpManager));
    setImmediate(() => ac.abort());
    const result = await p;

    assert.equal(result.isError, true);
    assert.equal((result.metadata as any)?.status, 'aborted');
  });
});
