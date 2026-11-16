// Unit U088 — MCP resource content parity: binary-blob interception (decode +
// persist + saved-to-disk message), the normalized structured `contents[]` shape
// ({uri, mimeType, text, blobSavedTo}), and the per-resource description/server
// fields in the list output — mirroring Claude's ReadMcpResourceTool /
// ListMcpResourcesTool behavior.
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { listMcpResourcesTool, readMcpResourceTool } from '../src/tools/mcp-resources';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: os.tmpdir(), abortSignal: new AbortController().signal, ...overrides } as ToolContext;
}
function withManager(impl: Record<string, unknown>): ToolContext {
  return ctx({ _mcpManager: impl as unknown as ToolContext['_mcpManager'] });
}

describe('U088 — ReadMcpResource binary-blob interception + saved-to-disk message', () => {
  it('decodes a base64 blob, persists raw bytes to disk, and returns a blobSavedTo path', async () => {
    const payload = Buffer.from('%PDF-1.7 fake pdf bytes', 'utf8');
    const result = {
      contents: [{ uri: 'file://doc.pdf', mimeType: 'application/pdf', blob: payload.toString('base64') }],
    };
    const res = await readMcpResourceTool.execute!(
      { server: 'docs', uri: 'file://doc.pdf' },
      withManager({ readResource: async () => result }),
    );
    assert.equal(res.isError, undefined);
    const contents = (res.metadata as { contents: Array<{ blobSavedTo?: string; mimeType?: string; uri?: string; text?: string }> }).contents;
    assert.equal(contents.length, 1);
    const entry = contents[0];
    // The blob was NOT inlined: a file path is returned instead.
    assert.ok(entry.blobSavedTo, 'blobSavedTo path is present');
    assert.equal(entry.mimeType, 'application/pdf');
    // The raw bytes on disk round-trip exactly (not re-encoded / not stringified).
    const onDisk = fs.readFileSync(entry.blobSavedTo!);
    assert.ok(onDisk.equals(payload), 'persisted bytes equal the decoded blob');
    // Persisted with a mime-derived extension so native tools can open it.
    assert.equal(path.extname(entry.blobSavedTo!), '.pdf');
    // A legible, source-tagged saved-to-disk message replaces the base64 inline.
    assert.match(entry.text ?? '', /\[Resource from docs at file:\/\/doc\.pdf\] /);
    assert.match(entry.text ?? '', /Binary content \(application\/pdf, \d/);
    assert.match(entry.text ?? '', /saved to /);
    // The base64 of the payload never leaks into the model-facing output.
    assert.ok(!res.output.includes(payload.toString('base64')), 'base64 is not inlined');
    fs.rmSync(entry.blobSavedTo!, { force: true });
  });

  it('returns a normalized structured contents[] shape of {uri, mimeType, text} for text resources', async () => {
    const result = {
      contents: [
        { uri: 'res://a', mimeType: 'text/plain', text: 'alpha' },
        { uri: 'res://b', mimeType: 'text/markdown', text: 'beta' },
      ],
    };
    const res = await readMcpResourceTool.execute!(
      { server: 's', uri: 'res://a' },
      withManager({ readResource: async () => result }),
    );
    const contents = (res.metadata as { contents: Array<{ uri?: string; mimeType?: string; text?: string }> }).contents;
    assert.deepEqual(contents, [
      { uri: 'res://a', mimeType: 'text/plain', text: 'alpha' },
      { uri: 'res://b', mimeType: 'text/markdown', text: 'beta' },
    ]);
    // Rendered output is the pretty-printed structured JSON (not an opaque blob).
    assert.match(res.output, /"contents"/);
    assert.match(res.output, /"uri": "res:\/\/a"/);
  });

  it('special-cases empty resource content', async () => {
    const res = await readMcpResourceTool.execute!(
      { server: 's', uri: 'res://empty' },
      withManager({ readResource: async () => ({ contents: [] }) }),
    );
    assert.match(res.output, /returned no content/);
  });
});

describe('U088 — ListMcpResources surfaces per-resource description and server', () => {
  it('includes the description and a [server] prefix when listing across all servers', async () => {
    const resources = [
      { uri: 'res://1', name: 'one', mimeType: 'text/plain', description: 'the first thing', server: 'alpha' },
      { uri: 'res://2', name: 'two', mimeType: 'text/csv', description: 'the second thing', server: 'beta' },
    ];
    const res = await listMcpResourcesTool.execute!({}, withManager({ listResources: async () => resources }));
    assert.equal(res.isError, undefined);
    assert.match(res.output, /\[alpha\] res:\/\/1 — one \(text\/plain\): the first thing/);
    assert.match(res.output, /\[beta\] res:\/\/2 — two \(text\/csv\): the second thing/);
  });

  it('omits the [server] prefix when scoped to a single server', async () => {
    const resources = [{ uri: 'res://1', name: 'one', mimeType: 'text/plain', description: 'desc', server: 'alpha' }];
    const res = await listMcpResourcesTool.execute!(
      { server: 'alpha' },
      withManager({ listServers: () => [{ name: 'alpha', state: 'ready' }], listResources: async () => resources }),
    );
    assert.ok(!/\[alpha\]/.test(res.output), 'server prefix is omitted when scoped');
    // The description is still surfaced even in scoped mode.
    assert.match(res.output, /res:\/\/1 — one \(text\/plain\): desc/);
  });
});
