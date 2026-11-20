import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  createMcpTool,
  getMcpToolTimeout,
  inferCompactSchema,
  processMcpContent,
  DEFAULT_MCP_TOOL_TIMEOUT_MS,
} from '../src/tools/mcp-tool';
import {
  persistBinaryContent,
  getBinaryBlobSavedMessage,
} from '../src/tools/mcp-output-storage';
import { extensionForMimeType } from '../src/tools/web-fetch';

// ---------------------------------------------------------------------------
// U071 — additive MCP tool-result parity:
//   * inferCompactSchema (compact jq-friendly type signature)
//   * schema-enriched structuredContent format
//   * binary resource-blob persistence in processMcpContent
//   * standard MCP_TOOL_TIMEOUT env knob
//   * shared mcp-output-storage helpers
// ESM-safe: in-memory mocks, fs.mkdtempSync temp dirs, no source-text reads.
// ---------------------------------------------------------------------------

let tmpCwd: string;

before(() => {
  tmpCwd = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-tool-u071-'));
});

after(() => {
  fs.rmSync(tmpCwd, { recursive: true, force: true });
});

afterEach(() => {
  delete process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS;
  delete process.env.MCP_TOOL_TIMEOUT;
});

function makeContext(mcpManager: unknown): any {
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

// ---------------------------------------------------------------------------
// inferCompactSchema
// ---------------------------------------------------------------------------

describe('inferCompactSchema', () => {
  it('renders primitive, null and typeof signatures', () => {
    assert.equal(inferCompactSchema(null), 'null');
    assert.equal(inferCompactSchema(42), 'number');
    assert.equal(inferCompactSchema('hi'), 'string');
    assert.equal(inferCompactSchema(true), 'boolean');
    assert.equal(inferCompactSchema(undefined), 'undefined');
  });

  it('samples the first array element and renders an empty array as []', () => {
    assert.equal(inferCompactSchema([]), '[]');
    assert.equal(inferCompactSchema([1, 2, 3]), '[number]');
    assert.equal(inferCompactSchema([{ id: 1, name: 'x' }]), '[{id: number, name: string}]');
  });

  it('renders a nested object signature, collapsing past the depth budget', () => {
    // depth=2: items -> array (depth 1) -> element object (depth 0) collapses to {...}.
    assert.equal(
      inferCompactSchema({ title: 'x', items: [{ id: 1, name: 'y' }] }),
      '{title: string, items: [{...}]}',
    );
    // One level shallower stays fully expanded.
    assert.equal(inferCompactSchema([{ id: 1, name: 'y' }]), '[{id: number, name: string}]');
  });

  it('collapses objects deeper than the depth budget to {...}', () => {
    // depth=2: top object -> props at depth 1 -> nested object at depth 0 collapses.
    assert.equal(inferCompactSchema({ a: { b: { c: 1 } } }), '{a: {b: {...}}}');
  });

  it('caps an object at 10 keys and appends a continuation marker', () => {
    const big: Record<string, number> = {};
    for (let i = 0; i < 12; i++) big[`k${i}`] = i;
    const sig = inferCompactSchema(big);
    assert.ok(sig.endsWith(', ...}'), `expected continuation marker, got ${sig}`);
    // exactly 10 rendered props
    assert.equal((sig.match(/: number/g) || []).length, 10);
  });

  it('does not append a continuation marker at exactly 10 keys', () => {
    const exactly: Record<string, number> = {};
    for (let i = 0; i < 10; i++) exactly[`k${i}`] = i;
    assert.ok(!inferCompactSchema(exactly).includes(', ...}'));
  });
});

// ---------------------------------------------------------------------------
// structuredContent format carries the schema
// ---------------------------------------------------------------------------

describe('processMcpContent structuredContent schema', () => {
  it('attaches a compact schema to the structuredContent format', () => {
    const out = processMcpContent({ structuredContent: { title: 'x', count: 3 } });
    assert.ok(out);
    assert.equal(out!.format, 'JSON with schema: {title: string, count: number}');
  });
});

// ---------------------------------------------------------------------------
// MCP_TOOL_TIMEOUT standard env knob
// ---------------------------------------------------------------------------

describe('getMcpToolTimeout MCP_TOOL_TIMEOUT parity', () => {
  it('honors the standard MCP_TOOL_TIMEOUT env var', () => {
    process.env.MCP_TOOL_TIMEOUT = '7000';
    assert.equal(getMcpToolTimeout(), 7000);
  });

  it('prefers AMI SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS over MCP_TOOL_TIMEOUT', () => {
    process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS = '5000';
    process.env.MCP_TOOL_TIMEOUT = '7000';
    assert.equal(getMcpToolTimeout(), 5000);
  });

  it('falls back to the default for an invalid MCP_TOOL_TIMEOUT', () => {
    process.env.MCP_TOOL_TIMEOUT = 'not-a-number';
    assert.equal(getMcpToolTimeout(), DEFAULT_MCP_TOOL_TIMEOUT_MS);
  });
});

// ---------------------------------------------------------------------------
// mcp-output-storage shared helpers
// ---------------------------------------------------------------------------

describe('mcp-output-storage', () => {
  it('persists binary bytes to the tool-results dir with a mime-derived extension', () => {
    const bytes = Buffer.from('%PDF-1.4 fake pdf bytes');
    const res = persistBinaryContent(tmpCwd, bytes, 'application/pdf');
    assert.ok(!('error' in res), 'expected success');
    if ('error' in res) return;
    assert.equal(res.ext, 'pdf');
    assert.equal(res.size, bytes.length);
    assert.ok(res.filepath.endsWith('.pdf'));
    assert.deepEqual(fs.readFileSync(res.filepath), bytes);
  });

  it('falls back to a .bin extension for an unknown mime type', () => {
    const res = persistBinaryContent(tmpCwd, Buffer.from([0, 1, 2]), 'application/x-weird');
    assert.ok(!('error' in res));
    if ('error' in res) return;
    assert.equal(res.ext, 'bin');
  });

  it('returns an error (not a throw) when the results dir cannot be created', () => {
    // Point the config dir under a regular file so mkdirSync fails with ENOTDIR.
    const file = path.join(tmpCwd, 'a-file');
    fs.writeFileSync(file, 'x');
    const prev = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = path.join(file, 'under');
    try {
      const res = persistBinaryContent(tmpCwd, Buffer.from('y'), 'image/png');
      assert.ok('error' in res, 'expected an error result, not a throw');
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
      else process.env.SUPERINFERENCE_DATA_DIR = prev;
    }
  });

  it('builds a human-readable saved-blob message and reuses extensionForMimeType', () => {
    const msg = getBinaryBlobSavedMessage('/tmp/x.png', 'image/png', 2048, '[src] ');
    assert.equal(msg, '[src] Binary content (image/png, 2.0 KB) saved to /tmp/x.png');
    assert.equal(getBinaryBlobSavedMessage('/tmp/x', undefined, 3, 'S '), 'S Binary content (unknown type, 3 B) saved to /tmp/x');
    assert.equal(extensionForMimeType('image/jpeg'), 'jpg');
  });
});

// ---------------------------------------------------------------------------
// processMcpContent binary-blob persistence
// ---------------------------------------------------------------------------

describe('processMcpContent binary resource persistence', () => {
  const blob = Buffer.from('binary-xlsx-bytes-here').toString('base64');

  it('persists a binary resource blob and emits a saved-to-disk message when cwd is given', () => {
    const out = processMcpContent({
      content: [{ type: 'resource', resource: { uri: 'res://sheet', mimeType: 'application/pdf', blob } }],
    }, tmpCwd);
    assert.ok(out);
    assert.match(out!.text, /\[Resource from MCP at res:\/\/sheet\] Binary content \(application\/pdf, .* B\) saved to .*\.pdf/);
    const saved = out!.text.split('saved to ')[1].trim();
    assert.ok(fs.existsSync(saved), 'the blob file should exist on disk');
  });

  it('falls back to a size-only placeholder when no cwd is available', () => {
    const out = processMcpContent({
      content: [{ type: 'resource', resource: { uri: 'res://sheet', mimeType: 'application/pdf', blob } }],
    });
    assert.ok(out);
    // FUNCTIONAL CHANGE (U069/w74): size reported human-readably via formatSize.
    assert.match(out!.text, /\[Binary resource: res:\/\/sheet \(application\/pdf, [\d.]+ (B|KB|MB|GB|TB)\)\]/);
  });
});

// ---------------------------------------------------------------------------
// E2E: a tool call returning a binary resource blob persists it end-to-end.
// ---------------------------------------------------------------------------

describe('createMcpTool.execute binary blob e2e', () => {
  it('saves a binary blob returned by an MCP tool call and surfaces the path', async () => {
    const blob = Buffer.from('PK\u0003\u0004 zip-bytes').toString('base64');
    const mgr = new MockManager(async () => ({
      content: [{ type: 'resource', resource: { uri: 'res://report.zip', mimeType: 'application/zip', blob } }],
    }));
    const tool = createMcpTool('srv', 'export', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.ok(!res.isError, res.output);
    assert.match(res.output, /Binary content \(application\/zip, .*\) saved to .*\.zip/);
    const saved = res.output.split('saved to ')[1].split(/\s/)[0];
    assert.ok(fs.existsSync(saved));
  });
});

// ---------------------------------------------------------------------------
// E2E: an inlined text resource returned by a tool call is labeled with the
// originating server + resource URI (the reference parity, client.ts:L2526-2534).
// ---------------------------------------------------------------------------

describe('createMcpTool.execute resource provenance e2e', () => {
  it('prefixes an inlined text resource with its originating server and uri', async () => {
    const mgr = new MockManager(async () => ({
      content: [{ type: 'resource', resource: { uri: 'res://notes.md', mimeType: 'text/markdown', text: '# release notes' } }],
    }));
    const tool = createMcpTool('knowledge-base', 'fetch', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.ok(!res.isError, res.output);
    // Both the server name and the resource uri reach the model-visible output,
    // and the original resource body is preserved after the provenance prefix.
    assert.match(res.output, /\[Resource from knowledge-base at res:\/\/notes\.md\] # release notes/);
  });

  it('labels a persisted binary resource blob with the originating server', async () => {
    const blob = Buffer.from('%PDF-1.4 fake').toString('base64');
    const mgr = new MockManager(async () => ({
      content: [{ type: 'resource', resource: { uri: 'res://report.pdf', mimeType: 'application/pdf', blob } }],
    }));
    const tool = createMcpTool('reporting', 'export', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.ok(!res.isError, res.output);
    assert.match(res.output, /\[Resource from reporting at res:\/\/report\.pdf\] Binary content \(application\/pdf, .*\) saved to .*\.pdf/);
  });
});
