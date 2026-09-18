import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { createMcpTool } from '../src/tools/mcp-tool';

function makeContext(mcpManager: any): any {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    filesRead: new Set(),
    _mcpManager: mcpManager,
  };
}

// ---------------------------------------------------------------------------
// Auto-connect when server is pending (gap #1)
// ---------------------------------------------------------------------------

describe('MCP tool auto-connect on execute', () => {
  it('calls ensureConnected when isPending returns true', async () => {
    let ensureConnectedCalled = false;
    const mock = {
      isPending: (name: string) => {
        assert.equal(name, 'kali');
        return true;
      },
      ensureConnected: async (name: string) => {
        assert.equal(name, 'kali');
        ensureConnectedCalled = true;
      },
      callTool: async () => 'result',
    };
    const tool = createMcpTool('kali', 'nmap_scan', 'Scan ports', {});
    await tool.execute({}, makeContext(mock));
    assert.equal(ensureConnectedCalled, true);
  });

  it('returns error if auto-connect fails', async () => {
    const mock = {
      isPending: () => true,
      ensureConnected: async () => { throw new Error('No container runtime found'); },
      callTool: async () => { throw new Error('should not be called'); },
    };
    const tool = createMcpTool('kali', 'nmap_scan', 'Scan ports', {});
    const result = await tool.execute({}, makeContext(mock));
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Failed to start MCP server'));
    assert.ok(result.output.includes('No container runtime found'));
  });

  it('proceeds normally when isPending returns false', async () => {
    let callToolCalled = false;
    const mock = {
      isPending: () => false,
      ensureConnected: async () => { throw new Error('should not be called'); },
      callTool: async () => { callToolCalled = true; return 'ok'; },
    };
    const tool = createMcpTool('kali', 'nmap_scan', 'Scan ports', {});
    const result = await tool.execute({}, makeContext(mock));
    assert.equal(callToolCalled, true);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('ok'));
  });

  it('works when isPending is not defined (optional chaining)', async () => {
    let callToolCalled = false;
    const mock = {
      callTool: async () => { callToolCalled = true; return 'result'; },
    };
    const tool = createMcpTool('kali', 'nmap_scan', 'Scan ports', {});
    const result = await tool.execute({}, makeContext(mock));
    assert.equal(callToolCalled, true);
    assert.ok(!result.isError);
  });

  it('calls callTool after successful auto-connect', async () => {
    const callOrder: string[] = [];
    const mock = {
      isPending: () => true,
      ensureConnected: async () => { callOrder.push('ensureConnected'); },
      callTool: async (name: string, args: any) => {
        callOrder.push('callTool');
        assert.equal(name, 'kali:nmap_scan');
        return { result: 'scan complete' };
      },
    };
    const tool = createMcpTool('kali', 'nmap_scan', 'Scan ports', {});
    const result = await tool.execute({ target: '127.0.0.1' }, makeContext(mock));
    assert.deepEqual(callOrder, ['ensureConnected', 'callTool']);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('scan complete'));
  });

  it('passes input args to callTool after auto-connect', async () => {
    let receivedArgs: any = null;
    const mock = {
      isPending: () => true,
      ensureConnected: async () => {},
      callTool: async (_name: string, args: any) => {
        receivedArgs = args;
        return 'done';
      },
    };
    const tool = createMcpTool('kali', 'nmap_scan', 'Scan ports', {});
    await tool.execute({ target: '10.0.0.1', ports: '1-1000' }, makeContext(mock));
    assert.deepEqual(receivedArgs, { target: '10.0.0.1', ports: '1-1000' });
  });

  it('returns MCP not initialized when no manager', async () => {
    const tool = createMcpTool('kali', 'nmap_scan', 'Scan ports', {});
    const result = await tool.execute({}, makeContext(null));
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('MCP not initialized'));
  });
});
