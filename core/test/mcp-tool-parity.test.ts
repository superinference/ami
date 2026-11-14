import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { createMcpTool, extractMcpErrorText } from '../src/tools/mcp-tool';

// ---------------------------------------------------------------------------
// Parity coverage for createMcpTool: annotation-driven read-only (H29),
// untrusted-metadata Unicode sanitization (H28), isError:true surfacing (H33),
// and abort/cancellation wiring into the underlying call (H27).
// ---------------------------------------------------------------------------

let tmpCwd: string;

before(() => {
  tmpCwd = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-tool-parity-'));
});
after(() => {
  fs.rmSync(tmpCwd, { recursive: true, force: true });
});

class MockManager extends EventEmitter {
  calls: Array<{ name: string; input: unknown; options: unknown }> = [];
  impl: (name: string, input: unknown, options: unknown) => Promise<unknown>;
  constructor(impl: (name: string, input: unknown, options: unknown) => Promise<unknown>) {
    super();
    this.impl = impl;
  }
  callTool(name: string, input: unknown, options: unknown) {
    this.calls.push({ name, input, options });
    return this.impl(name, input, options);
  }
}

function makeContext(mcpManager: any, extra: Record<string, unknown> = {}): any {
  return {
    cwd: tmpCwd,
    abortSignal: new AbortController().signal,
    filesRead: new Set(),
    _mcpManager: mcpManager,
    ...extra,
  };
}

describe('createMcpTool annotations (H29)', () => {
  it('is read-only + concurrency-safe when readOnlyHint is true', () => {
    const tool = createMcpTool('srv', 'get_thing', 'desc', {}, { readOnlyHint: true });
    assert.equal(tool.isReadOnly, true);
    assert.equal(tool.isConcurrencySafe, true);
    assert.equal(tool.isMcp, true);
  });
  it('defaults to not-read-only when no annotation is present', () => {
    const tool = createMcpTool('srv', 'do_thing', 'desc', {});
    assert.equal(tool.isReadOnly, false);
    assert.equal(tool.isConcurrencySafe, false);
  });
  it('is not read-only when readOnlyHint is explicitly false', () => {
    const tool = createMcpTool('srv', 'do_thing', 'desc', {}, { readOnlyHint: false });
    assert.equal(tool.isReadOnly, false);
  });
});

describe('createMcpTool unicode sanitization (H28)', () => {
  it('strips invisible unicode from description, name, and schema strings', () => {
    const zw = '​'; // zero-width space
    const tool = createMcpTool(
      'srv',
      `do${zw}thing`,
      `a ${zw}description`,
      { type: 'object', properties: { q: { type: 'string', description: `field${zw}help` } } },
    );
    assert.ok(!tool.description.includes(zw), 'description sanitized');
    assert.ok(!tool.name.includes(zw), 'name sanitized');
    const schema = tool.inputSchema as any;
    assert.equal(schema.properties.q.description, 'fieldhelp');
  });
});

describe('createMcpTool isError surfacing (H33)', () => {
  it('extractMcpErrorText pulls the first text content part', () => {
    assert.equal(
      extractMcpErrorText({ isError: true, content: [{ type: 'text', text: 'boom' }] }),
      'boom',
    );
  });
  it('extractMcpErrorText falls back to a legacy error field', () => {
    assert.equal(extractMcpErrorText({ isError: true, error: 'legacy' }), 'legacy');
  });
  it('execute returns isError:true for an error result instead of success', async () => {
    const mgr = new MockManager(async () => ({ isError: true, content: [{ type: 'text', text: 'server failed' }] }));
    const tool = createMcpTool('srv', 'do_thing', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.equal(res.isError, true);
    assert.equal(res.output, 'server failed');
  });
  it('execute returns a normal success for a non-error result', async () => {
    const mgr = new MockManager(async () => ({ content: [{ type: 'text', text: 'ok' }] }));
    const tool = createMcpTool('srv', 'do_thing', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    assert.notEqual(res.isError, true);
    assert.equal(res.output, 'ok');
  });
});

describe('createMcpTool abort wiring (H27)', () => {
  it('forwards the abort signal to the manager callTool', async () => {
    const ac = new AbortController();
    const mgr = new MockManager(async () => ({ content: [{ type: 'text', text: 'ok' }] }));
    const tool = createMcpTool('srv', 'do_thing', 'desc', {});
    await tool.execute({}, makeContext(mgr, { abortSignal: ac.signal }));
    assert.equal((mgr.calls[0].options as any).signal, ac.signal);
  });
  it('rejects the call when the signal aborts mid-flight', async () => {
    const ac = new AbortController();
    const mgr = new MockManager(() => new Promise(() => {/* never resolves */}));
    const tool = createMcpTool('srv', 'do_thing', 'desc', {});
    const p = tool.execute({}, makeContext(mgr, { abortSignal: ac.signal }));
    ac.abort();
    const res = await p;
    assert.equal(res.isError, true);
    assert.match(res.output, /abort/i);
  });
  it('returns an error immediately when the signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const mgr = new MockManager(() => new Promise(() => {/* never resolves */}));
    const tool = createMcpTool('srv', 'do_thing', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr, { abortSignal: ac.signal }));
    assert.equal(res.isError, true);
    assert.match(res.output, /abort/i);
  });
});
