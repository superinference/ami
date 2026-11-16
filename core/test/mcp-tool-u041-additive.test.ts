import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { createMcpTool } from '../src/tools/mcp-tool';

// ---------------------------------------------------------------------------
// GAP U041-36: MCP tool results are capped by the token budget
// (MAX_MCP_OUTPUT_TOKENS x 4 chars) when the server carries no explicit
// maxResultSizeChars hint — instead of the 10 MB byte ceiling.
// ---------------------------------------------------------------------------

let tmpCwd: string;
before(() => { tmpCwd = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-tool-u041-')); });
after(() => { fs.rmSync(tmpCwd, { recursive: true, force: true }); });
afterEach(() => { delete process.env.MAX_MCP_OUTPUT_TOKENS; });

function makeContext(mcpManager: any): any {
  return {
    cwd: tmpCwd,
    abortSignal: new AbortController().signal,
    filesRead: new Set(),
    _mcpManager: mcpManager,
  };
}

class MockManager extends EventEmitter {
  callTool: (name: string, input: unknown) => Promise<unknown>;
  constructor(callTool: (name: string, input: unknown) => Promise<unknown>) {
    super();
    this.callTool = callTool;
  }
}

describe('token-budget default output cap', () => {
  it('truncates an oversized result to the MAX_MCP_OUTPUT_TOKENS char budget when no _meta hint', async () => {
    // 10 tokens -> 40 chars. A 200-char result must be truncated.
    process.env.MAX_MCP_OUTPUT_TOKENS = '10';
    const big = 'y'.repeat(200);
    const mgr = new MockManager(async () => big);
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.equal((res.metadata as any)?.mcpTruncated, true, 'result should be truncated by the token budget');
    assert.ok((res.metadata as any)?.mcpOriginalBytes >= 200, 'original byte count is recorded');
  });

  it('does not truncate a small result under the default token budget', async () => {
    delete process.env.MAX_MCP_OUTPUT_TOKENS; // default 25000 tokens -> 100000 chars
    const mgr = new MockManager(async () => 'short result');
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.equal(res.output, 'short result');
    assert.equal(res.metadata, undefined);
  });

  it('still honors an explicit server maxResultSizeChars hint over the token default', async () => {
    process.env.MAX_MCP_OUTPUT_TOKENS = '100000'; // huge token budget
    const mgr = new MockManager(async () => ({ _meta: { maxResultSizeChars: 10 }, blob: 'z'.repeat(500) }));
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.equal((res.metadata as any)?.mcpTruncated, true, 'explicit hint wins and truncates');
  });
});
