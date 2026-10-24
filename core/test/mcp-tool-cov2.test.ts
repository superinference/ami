import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  createMcpTool,
  qualifiedMcpToolName,
  getMcpToolTimeout,
  extractMaxResultSizeChars,
  processMcpContent,
  DEFAULT_MCP_TOOL_TIMEOUT_MS,
  MAX_MCP_TOOL_NAME_LEN,
} from '../src/tools/mcp-tool';

// ---------------------------------------------------------------------------
// ESM-safe behavioural coverage for core/core/src/tools/mcp-tool.ts
//
// No source-text reads, no fs-namespace monkeypatching, no __dirname fixture
// coupling. Every manager is an in-memory mock; file spill goes to a fresh
// temp dir created with fs.mkdtempSync.
// ---------------------------------------------------------------------------

let tmpCwd: string;

before(() => {
  tmpCwd = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-tool-cov2-'));
});

after(() => {
  fs.rmSync(tmpCwd, { recursive: true, force: true });
  delete process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS;
});

function makeContext(mcpManager: any, extra: Record<string, unknown> = {}): any {
  return {
    cwd: tmpCwd,
    abortSignal: new AbortController().signal,
    filesRead: new Set(),
    _mcpManager: mcpManager,
    ...extra,
  };
}

/** Event-emitting mock manager so the elicitation listener wiring is exercised. */
class MockManager extends EventEmitter {
  callTool: (name: string, input: unknown) => Promise<unknown>;
  constructor(callTool: (name: string, input: unknown) => Promise<unknown>) {
    super();
    this.callTool = callTool;
  }
}

// ---------------------------------------------------------------------------
// getMcpToolTimeout
// ---------------------------------------------------------------------------

describe('getMcpToolTimeout', () => {
  it('returns the default when the env var is unset or empty', () => {
    delete process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS;
    assert.equal(getMcpToolTimeout(), DEFAULT_MCP_TOOL_TIMEOUT_MS);
    process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS = '';
    assert.equal(getMcpToolTimeout(), DEFAULT_MCP_TOOL_TIMEOUT_MS);
  });

  it('reads a positive finite custom value', () => {
    process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS = '1234';
    assert.equal(getMcpToolTimeout(), 1234);
  });

  it('falls back to the default for non-finite, zero, or negative values', () => {
    for (const bad of ['abc', 'Infinity', '0', '-5']) {
      process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS = bad;
      assert.equal(getMcpToolTimeout(), DEFAULT_MCP_TOOL_TIMEOUT_MS, `bad=${bad}`);
    }
    delete process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS;
  });
});

// ---------------------------------------------------------------------------
// qualifiedMcpToolName
// ---------------------------------------------------------------------------

describe('qualifiedMcpToolName', () => {
  it('normalizes illegal characters in server and tool names', () => {
    const name = qualifiedMcpToolName('my-server.v2', 'do/thing!');
    assert.equal(name, 'mcp__my_server_v2__do_thing_');
  });

  it('keeps names at or under the limit intact', () => {
    const name = qualifiedMcpToolName('s', 'a'.repeat(50));
    assert.ok(name.length <= MAX_MCP_TOOL_NAME_LEN);
    assert.equal(name, 'mcp__s__' + 'a'.repeat(50));
  });

  it('truncates over-long names with a disambiguating hash suffix', () => {
    const a = qualifiedMcpToolName('s', 'z'.repeat(300) + 'A');
    const b = qualifiedMcpToolName('s', 'z'.repeat(300) + 'B');
    assert.ok(a.length <= MAX_MCP_TOOL_NAME_LEN);
    assert.ok(b.length <= MAX_MCP_TOOL_NAME_LEN);
    assert.notEqual(a, b);
  });
});

// ---------------------------------------------------------------------------
// extractMaxResultSizeChars
// ---------------------------------------------------------------------------

describe('extractMaxResultSizeChars', () => {
  it('reads the anthropic/-namespaced meta key', () => {
    assert.equal(extractMaxResultSizeChars({ _meta: { 'anthropic/maxResultSizeChars': 4096 } }), 4096);
  });

  it('reads the bare meta key', () => {
    assert.equal(extractMaxResultSizeChars({ _meta: { maxResultSizeChars: 777 } }), 777);
  });

  it('caps the value at 500000', () => {
    assert.equal(extractMaxResultSizeChars({ _meta: { maxResultSizeChars: 9_000_000 } }), 500_000);
  });

  it('returns undefined for non-objects, arrays, missing meta, and bad values', () => {
    assert.equal(extractMaxResultSizeChars('str'), undefined);
    assert.equal(extractMaxResultSizeChars(null), undefined);
    assert.equal(extractMaxResultSizeChars([{ _meta: { maxResultSizeChars: 10 } }]), undefined);
    assert.equal(extractMaxResultSizeChars({ nope: 1 }), undefined);
    assert.equal(extractMaxResultSizeChars({ _meta: {} }), undefined);
    assert.equal(extractMaxResultSizeChars({ _meta: { maxResultSizeChars: 0 } }), undefined);
    assert.equal(extractMaxResultSizeChars({ _meta: { maxResultSizeChars: -1 } }), undefined);
    assert.equal(extractMaxResultSizeChars({ _meta: { maxResultSizeChars: 'big' } }), undefined);
  });
});

// ---------------------------------------------------------------------------
// processMcpContent
// ---------------------------------------------------------------------------

describe('processMcpContent', () => {
  it('returns null when there is no content array', () => {
    assert.equal(processMcpContent(null), null);
    assert.equal(processMcpContent('plain'), null);
    assert.equal(processMcpContent([1, 2]), null);
    assert.equal(processMcpContent({ nope: true }), null);
    assert.equal(processMcpContent({ content: 'not-array' }), null);
  });

  it('returns null when the content array yields no renderable parts', () => {
    assert.equal(processMcpContent({ content: [] }), null);
    assert.equal(processMcpContent({ content: [{ type: 'unknown' }] }), null);
    // text item whose text is not a string is skipped
    assert.equal(processMcpContent({ content: [{ type: 'text', text: 42 }] }), null);
    // image item whose data is not a string is skipped
    assert.equal(processMcpContent({ content: [{ type: 'image', data: 123 }] }), null);
    // resource item with a falsy resource is skipped
    assert.equal(processMcpContent({ content: [{ type: 'resource', resource: null }] }), null);
  });

  it('renders a supported image and emits mcpImages metadata', () => {
    const out = processMcpContent({ content: [{ type: 'image', data: 'aGVsbG8=', mimeType: 'image/webp' }] });
    assert.ok(out);
    assert.match(out!.text, /\[Image: image\/webp, \d+ bytes base64\]/);
    const imgs = out!.metadata!.mcpImages as Array<{ mimeType: string; data: string }>;
    assert.equal(imgs[0].mimeType, 'image/webp');
    assert.equal(imgs[0].data, 'aGVsbG8=');
  });

  it('defaults a missing image mimeType to image/png', () => {
    const out = processMcpContent({ content: [{ type: 'image', data: 'YWJj' }] });
    assert.match(out!.text, /image\/png/);
  });

  it('flags an unsupported image type and adds no metadata', () => {
    const out = processMcpContent({ content: [{ type: 'image', data: 'YWJj', mimeType: 'image/tiff' }] });
    assert.equal(out!.text, '[Unsupported image type: image/tiff]');
    assert.equal(out!.metadata, undefined);
  });

  it('inlines a text resource verbatim', () => {
    const out = processMcpContent({ content: [{ type: 'resource', resource: { uri: 'file:///a.txt', text: 'inline body' } }] });
    assert.equal(out!.text, 'inline body');
  });

  it('decodes a text-ish MIME base64 blob', () => {
    const blob = Buffer.from('decoded json', 'utf-8').toString('base64');
    const out = processMcpContent({ content: [{ type: 'resource', resource: { mimeType: 'application/json', blob } }] });
    assert.equal(out!.text, 'decoded json');
  });

  it('reports a binary resource blob by size with its uri and mime', () => {
    const out = processMcpContent({ content: [{ type: 'resource', resource: { uri: 'file:///x.bin', mimeType: 'application/pdf', blob: 'YmluYXJ5' } }] });
    assert.match(out!.text, /\[Binary resource: file:\/\/\/x\.bin \(application\/pdf, \d+ bytes\)\]/);
  });

  it('reports a binary blob with unknown-type fallback when no mime given', () => {
    const out = processMcpContent({ content: [{ type: 'resource', resource: { blob: 'YmluYXJ5' } }] });
    assert.match(out!.text, /\[Binary resource: unknown \(unknown type, \d+ bytes\)\]/);
  });

  it('reports a resource with neither text nor blob', () => {
    const out = processMcpContent({ content: [{ type: 'resource', resource: { uri: 'res://thing' } }] });
    assert.equal(out!.text, '[Resource: res://thing]');
    const anon = processMcpContent({ content: [{ type: 'resource', resource: {} }] });
    assert.equal(anon!.text, '[Resource: unknown]');
  });

  it('joins multiple heterogeneous parts with newlines', () => {
    const out = processMcpContent({ content: [
      { type: 'text', text: 'alpha' },
      { type: 'resource', resource: { text: 'beta' } },
    ] });
    assert.equal(out!.text, 'alpha\nbeta');
  });
});

// ---------------------------------------------------------------------------
// createMcpTool.execute — guard branches
// ---------------------------------------------------------------------------

describe('createMcpTool.execute guards', () => {
  it('errors when no manager is present', async () => {
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(null));
    assert.equal(res.isError, true);
    assert.match(res.output, /MCP not initialized/);
  });

  it('errors when the server is disabled', async () => {
    const mgr = { isDisabled: (n: string) => n === 'srv', callTool: async () => 'x' };
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.equal(res.isError, true);
    assert.match(res.output, /is disabled/);
    assert.match(res.output, /\/mcp start srv/);
  });

  it('auto-connects a pending server, then calls the tool', async () => {
    const order: string[] = [];
    const mgr = {
      isPending: () => true,
      ensureConnected: async () => { order.push('ensure'); },
      callTool: async () => { order.push('call'); return 'ok'; },
    };
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.deepEqual(order, ['ensure', 'call']);
    assert.equal(res.isError, undefined);
  });

  it('errors when auto-connect fails', async () => {
    const mgr = {
      isPending: () => true,
      ensureConnected: async () => { throw new Error('no runtime'); },
      callTool: async () => 'unused',
    };
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.equal(res.isError, true);
    assert.match(res.output, /Failed to start MCP server 'srv': no runtime/);
  });

  it('stringifies a thrown non-Error during auto-connect', async () => {
    const mgr = {
      isPending: () => true,
      ensureConnected: async () => { throw 'string blow-up'; },
      callTool: async () => 'unused',
    };
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.equal(res.isError, true);
    assert.match(res.output, /string blow-up/);
  });
});

// ---------------------------------------------------------------------------
// createMcpTool.execute — result shaping
// ---------------------------------------------------------------------------

describe('createMcpTool.execute result shaping', () => {
  it('passes the qualified server:tool name and raw input to callTool', async () => {
    let seen: { name?: string; input?: unknown } = {};
    const mgr = new MockManager(async (name, input) => { seen = { name, input }; return 'done'; });
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    await tool.execute({ a: 1 }, makeContext(mgr));
    assert.equal(seen.name, 'srv:tl');
    assert.deepEqual(seen.input, { a: 1 });
  });

  it('returns a string result unchanged', async () => {
    const mgr = new MockManager(async () => 'hello string');
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.equal(res.output, 'hello string');
    assert.equal(res.metadata, undefined);
  });

  it('JSON-stringifies a non-content object result', async () => {
    const mgr = new MockManager(async () => ({ k: 'v' }));
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.match(res.output, /"k": "v"/);
  });

  it('surfaces processed content metadata (images) when not truncated', async () => {
    const mgr = new MockManager(async () => ({ content: [{ type: 'image', data: 'aGk=', mimeType: 'image/png' }] }));
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.match(res.output, /\[Image: image\/png/);
    assert.ok((res.metadata as any).mcpImages, 'image metadata should be surfaced');
    assert.equal((res.metadata as any).mcpTruncated, undefined);
  });

  it('marks truncation metadata when the result exceeds maxResultSizeChars', async () => {
    const mgr = new MockManager(async () => ({ _meta: { maxResultSizeChars: 10 }, blob: 'y'.repeat(500) }));
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.equal((res.metadata as any).mcpTruncated, true);
    assert.ok((res.metadata as any).mcpOriginalBytes > 10);
  });

  it('merges truncation metadata with processed image metadata', async () => {
    const mgr = new MockManager(async () => ({
      _meta: { maxResultSizeChars: 5 },
      content: [{ type: 'image', data: 'aGVsbG8gd29ybGQ=', mimeType: 'image/png' }],
    }));
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    const meta = res.metadata as any;
    assert.equal(meta.mcpTruncated, true);
    assert.ok(meta.mcpImages, 'image metadata should survive the merge');
  });
});

// ---------------------------------------------------------------------------
// createMcpTool.execute — error & timeout paths
// ---------------------------------------------------------------------------

describe('createMcpTool.execute errors and timeouts', () => {
  it('returns a generic MCP tool error when callTool rejects', async () => {
    const mgr = new MockManager(async () => { throw new Error('tool exploded'); });
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.equal(res.isError, true);
    assert.match(res.output, /MCP tool error: tool exploded/);
  });

  it('stringifies a thrown non-Error from callTool', async () => {
    const mgr = new MockManager(async () => { throw 'bare rejection'; });
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.equal(res.isError, true);
    assert.match(res.output, /MCP tool error: bare rejection/);
  });

  it('returns a generic timeout error when callTool never resolves', async () => {
    process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS = '20';
    const mgr = new MockManager(() => new Promise(() => {}));
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.equal(res.isError, true);
    assert.match(res.output, /timed out/);
    assert.doesNotMatch(res.output, /no user was available/);
    delete process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS;
  });

  it('attributes a detached-mode timeout to elicitation (elicitation/create)', async () => {
    process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS = '20';
    const mgr = new MockManager(function (this: MockManager) {
      this.emit('notification', 'srv', 'elicitation/create');
      return new Promise(() => {});
    } as any);
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr, { detachedMode: true }));
    assert.equal(res.isError, true);
    assert.match(res.output, /no user was available/);
    assert.match(res.output, /Do not retry/);
    delete process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS;
  });

  it('attributes a detached-mode timeout to elicitation (notifications/elicitation)', async () => {
    process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS = '20';
    const mgr = new MockManager(function (this: MockManager) {
      this.emit('notification', 'srv', 'notifications/elicitation');
      return new Promise(() => {});
    } as any);
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr, { detachedMode: true }));
    assert.equal(res.isError, true);
    assert.match(res.output, /no user was available/);
    delete process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS;
  });

  it('ignores notifications from other servers or non-elicitation methods', async () => {
    process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS = '20';
    const mgr = new MockManager(function (this: MockManager) {
      // wrong server with an elicitation method, and matching server with a non-elicitation method
      this.emit('notification', 'other', 'elicitation/create');
      this.emit('notification', 'srv', 'notifications/progress');
      return new Promise(() => {});
    } as any);
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr, { detachedMode: true }));
    assert.equal(res.isError, true);
    // gotElicitation stays false → generic timeout even in detached mode
    assert.match(res.output, /timed out/);
    assert.doesNotMatch(res.output, /no user was available/);
    delete process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS;
  });

  it('gives a generic timeout even with elicitation when not in detached mode', async () => {
    process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS = '20';
    const mgr = new MockManager(function (this: MockManager) {
      this.emit('notification', 'srv', 'elicitation/create');
      return new Promise(() => {});
    } as any);
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr, { detachedMode: false }));
    assert.equal(res.isError, true);
    assert.match(res.output, /timed out/);
    assert.doesNotMatch(res.output, /no user was available/);
    delete process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS;
  });

  it('removes its notification listener after a successful call', async () => {
    const mgr = new MockManager(async () => 'ok');
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    await tool.execute({}, makeContext(mgr));
    assert.equal(mgr.listenerCount('notification'), 0);
  });
});
