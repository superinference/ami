import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import { createMcpConnectTool } from '../src/tools/mcp-connect';

const SRC_PATH = path.resolve(__dirname, '../src/tools/mcp-connect.ts');
const SRC = fs.readFileSync(SRC_PATH, 'utf-8');

// ---------------------------------------------------------------------------
// createMcpConnectTool factory
// ---------------------------------------------------------------------------

describe('createMcpConnectTool', () => {
  it('is exported as a function', () => {
    assert.equal(typeof createMcpConnectTool, 'function');
  });

  it('returns a tool definition with correct name pattern', () => {
    const tool = createMcpConnectTool('kali', 'Start kali MCP server');
    assert.equal(tool.name, 'mcp__kali__connect');
  });

  it('normalizes server name in tool name', () => {
    const tool = createMcpConnectTool('my-server', 'Test');
    assert.equal(tool.name, 'mcp__my_server__connect');
  });

  it('includes description about connecting', () => {
    const tool = createMcpConnectTool('kali', 'Start kali MCP server');
    assert.ok(tool.description.includes('Connect'));
    assert.ok(tool.description.includes('kali'));
  });

  it('has inputSchema with type object', () => {
    const tool = createMcpConnectTool('kali', 'Start kali');
    assert.equal((tool.inputSchema as any).type, 'object');
  });

  it('is marked as readOnly', () => {
    const tool = createMcpConnectTool('kali', 'Start kali');
    assert.equal(tool.isReadOnly, true);
  });

  it('has execute function', () => {
    const tool = createMcpConnectTool('kali', 'Start kali');
    assert.equal(typeof tool.execute, 'function');
  });
});

// ---------------------------------------------------------------------------
// Tool execution
// ---------------------------------------------------------------------------

describe('MCP connect tool execution', () => {
  it('returns error when MCP manager is not initialized', async () => {
    const tool = createMcpConnectTool('kali', 'Start kali');
    const result = await tool.execute({}, { cwd: '/tmp', abortSignal: new AbortController().signal, filesRead: new Set() } as any);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('MCP not initialized'));
  });

  it('calls ensureConnected on the MCP manager', async () => {
    let connectCalled = false;
    const mockMcpManager = {
      ensureConnected: async (name: string) => {
        connectCalled = true;
        assert.equal(name, 'kali');
      },
      getToolsForServer: () => [{ name: 'nmap_scan' }, { name: 'sqlmap_scan' }],
    };
    const tool = createMcpConnectTool('kali', 'Start kali');
    const result = await tool.execute({}, {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      filesRead: new Set(),
      _mcpManager: mockMcpManager,
    } as any);
    assert.equal(connectCalled, true);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Connected'));
    assert.ok(result.output.includes('2 tools'));
    assert.ok(result.output.includes('nmap_scan'));
    assert.ok(result.output.includes('sqlmap_scan'));
  });

  it('returns error on connection failure', async () => {
    const mockMcpManager = {
      ensureConnected: async () => { throw new Error('No container runtime'); },
      getToolsForServer: () => [],
    };
    const tool = createMcpConnectTool('kali', 'Start kali');
    const result = await tool.execute({}, {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      filesRead: new Set(),
      _mcpManager: mockMcpManager,
    } as any);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Failed'));
    assert.ok(result.output.includes('No container runtime'));
  });
});

// ---------------------------------------------------------------------------
// Source code structure
// ---------------------------------------------------------------------------

describe('mcp-connect.ts structure', () => {
  it('imports ToolDefinition and ToolResult', () => {
    assert.ok(SRC.includes('ToolDefinition'));
    assert.ok(SRC.includes('ToolResult'));
  });

  it('exports createMcpConnectTool function', () => {
    assert.ok(SRC.includes('export function createMcpConnectTool'));
  });

  it('calls ensureConnected on mcpManager', () => {
    assert.ok(SRC.includes('ensureConnected'));
  });

  it('calls getToolsForServer after connecting', () => {
    assert.ok(SRC.includes('getToolsForServer'));
  });

  it('handles errors gracefully', () => {
    assert.ok(SRC.includes('catch'));
    assert.ok(SRC.includes('isError: true'));
  });
});
