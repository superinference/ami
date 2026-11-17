import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  createMcpTool,
  formatMcpProgressLine,
  budgetMcpImages,
  IMAGE_TOKEN_ESTIMATE,
  MCP_PROGRESS_BAR_WIDTH,
} from '../src/tools/mcp-tool';
import { parseFileImagesOutput, FILE_IMAGES_KINDS } from '../src/tools/file-read';

// ---------------------------------------------------------------------------
// U071 / w76 — additive MCP tool-result parity:
//   * In-flight MCP progress hints forwarded via context.onProgress
//     (started → N% bar / message)  — parity services/mcp/client.ts:1846-1892,
//     tools/MCPTool/UI.tsx:L57-90
//   * Image-aware output budgeting + compress-to-fit with skip-on-failure, and
//     first-class multimodal forwarding via the shared file_images protocol
//     — parity utils/mcpValidation.ts:L113-L149, services/mcp/client.ts:L2503-L2563
// ESM-safe: in-memory EventEmitter mocks, fs.mkdtempSync temp dirs.
// ---------------------------------------------------------------------------

let tmpCwd: string;

before(() => {
  tmpCwd = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-tool-u071-w76-'));
});

after(() => {
  fs.rmSync(tmpCwd, { recursive: true, force: true });
});

function makeContext(mcpManager: unknown, extra: Record<string, unknown> = {}): any {
  return {
    cwd: tmpCwd,
    abortSignal: new AbortController().signal,
    filesRead: new Set(),
    _mcpManager: mcpManager,
    ...extra,
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
// formatMcpProgressLine
// ---------------------------------------------------------------------------

describe('formatMcpProgressLine', () => {
  it('renders a determinate bar + rounded percent when a percentage is known', () => {
    const line = formatMcpProgressLine({ percent: 50 });
    const filled = Math.round((50 / 100) * MCP_PROGRESS_BAR_WIDTH);
    assert.equal(line, `[${'█'.repeat(filled)}${'░'.repeat(MCP_PROGRESS_BAR_WIDTH - filled)}] 50%`);
  });

  it('appends a server message after the percentage when present', () => {
    assert.match(formatMcpProgressLine({ percent: 25, message: 'indexing' }), /] 25% indexing$/);
  });

  it('clamps an out-of-range percentage to 0..100 and rounds', () => {
    assert.match(formatMcpProgressLine({ percent: 140 }), /] 100%$/);
    assert.match(formatMcpProgressLine({ percent: -10 }), /] 0%$/);
    assert.match(formatMcpProgressLine({ percent: 33.6 }), /] 34%$/);
  });

  it('falls back to a Processing… line when only a message/count is known', () => {
    assert.equal(formatMcpProgressLine({ message: 'working' }), 'Processing… working');
    assert.equal(formatMcpProgressLine({}), 'Processing…');
    // A non-finite percent is treated as unknown.
    assert.equal(formatMcpProgressLine({ percent: NaN, message: 'x' }), 'Processing… x');
  });
});

// ---------------------------------------------------------------------------
// budgetMcpImages
// ---------------------------------------------------------------------------

describe('budgetMcpImages', () => {
  it('compresses/keeps small images as first-class base64 blocks', async () => {
    const blocks = await budgetMcpImages([{ mimeType: 'image/png', data: 'aGk=' }], 100_000);
    assert.equal(blocks.length, 1);
    assert.equal(blocks[0].type, 'image');
    assert.equal(blocks[0].source.type, 'base64');
    assert.equal(blocks[0].source.media_type, 'image/png');
    assert.ok(blocks[0].source.data.length > 0);
  });

  it('charges IMAGE_TOKEN_ESTIMATE per image and drops the overflowing tail', async () => {
    const imgs = [
      { mimeType: 'image/png', data: 'aGk=' },
      { mimeType: 'image/png', data: 'aGk=' },
    ];
    // Budget for exactly one image: the second overflows and is dropped.
    const blocks = await budgetMcpImages(imgs, IMAGE_TOKEN_ESTIMATE);
    assert.equal(blocks.length, 1);
  });

  it('drops all images when the budget is below one image estimate', async () => {
    const blocks = await budgetMcpImages([{ mimeType: 'image/png', data: 'aGk=' }], IMAGE_TOKEN_ESTIMATE - 1);
    assert.equal(blocks.length, 0);
  });

  it('skips an empty-data block without consuming budget', async () => {
    const blocks = await budgetMcpImages([
      { mimeType: 'image/png', data: '' },
      { mimeType: 'image/png', data: 'aGk=' },
    ], IMAGE_TOKEN_ESTIMATE);
    // The empty one is skipped (no budget charge), so the real one still fits.
    assert.equal(blocks.length, 1);
  });

  it('skips an un-compressible oversized image instead of emitting a bad block', async () => {
    // ~6.5 MB of zeros: not a valid image and > the 5 MB base64 API ceiling, so
    // compressImageBuffer throws ImageResizeError and the block is skipped.
    const oversized = Buffer.alloc(6_500_000).toString('base64');
    const blocks = await budgetMcpImages([{ mimeType: 'image/png', data: oversized }], 100_000);
    assert.equal(blocks.length, 0);
  });

  it('defaults the budget to the configured MCP output-token cap', async () => {
    const blocks = await budgetMcpImages([{ mimeType: 'image/png', data: 'aGk=' }]);
    assert.equal(blocks.length, 1);
  });
});

// ---------------------------------------------------------------------------
// FILE_IMAGES_KINDS gained the 'mcp' kind
// ---------------------------------------------------------------------------

describe('file_images mcp kind', () => {
  it('includes mcp so parseFileImagesOutput accepts MCP-sourced image envelopes', () => {
    assert.ok((FILE_IMAGES_KINDS as readonly string[]).includes('mcp'));
    const env = JSON.stringify({
      type: 'file_images',
      kind: 'mcp',
      text: 'some text',
      images: [{ type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'aGk=' } }],
    });
    const parsed = parseFileImagesOutput(env);
    assert.ok(parsed);
    assert.equal(parsed!.kind, 'mcp');
    assert.equal(parsed!.images.length, 1);
  });
});

// ---------------------------------------------------------------------------
// createMcpTool.execute — in-flight progress forwarding (F1, end-to-end)
// ---------------------------------------------------------------------------

describe('createMcpTool.execute progress forwarding', () => {
  it('emits a started hint and forwards server progress for THIS call via onProgress', async () => {
    const mgr = new MockManager(async function (this: unknown, name) {
      // Server pushes a determinate progress update mid-call (routed by the client
      // as a `tool-progress` event keyed by the <server>:<tool> name), plus one for
      // a DIFFERENT call that must be ignored.
      mgr.emit('tool-progress', { name: 'other:tool', percent: 99 });
      mgr.emit('tool-progress', { name, percent: 40, message: 'halfway' });
      return 'ok';
    });
    const progress: string[] = [];
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr, { onProgress: (d: string) => progress.push(d) }));
    assert.equal(res.output, 'ok');
    assert.ok(progress.includes('Running tl…'), 'a started hint is emitted');
    assert.ok(progress.some(p => /] 40% halfway$/.test(p)), 'the matching progress update is forwarded');
    assert.ok(!progress.some(p => /99%/.test(p)), 'a progress update for a different call is ignored');
  });

  it('detaches the progress listener after the call settles', async () => {
    const mgr = new MockManager(async () => 'ok');
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    await tool.execute({}, makeContext(mgr, { onProgress: () => {} }));
    assert.equal(mgr.listenerCount('tool-progress'), 0, 'no leaked tool-progress listener');
  });

  it('detaches the progress listener even when the call rejects', async () => {
    const mgr = new MockManager(async () => { throw new Error('boom'); });
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr, { onProgress: () => {} }));
    assert.equal(res.isError, true);
    assert.equal(mgr.listenerCount('tool-progress'), 0, 'listener detached on failure');
  });
});

// ---------------------------------------------------------------------------
// createMcpTool.execute — first-class image forwarding (F2, end-to-end)
// ---------------------------------------------------------------------------

describe('createMcpTool.execute image forwarding', () => {
  it('forwards a result image as a compressed first-class file_images block', async () => {
    const mgr = new MockManager(async () => ({
      content: [
        { type: 'text', text: 'chart below' },
        { type: 'image', data: 'aGVsbG8=', mimeType: 'image/png' },
      ],
    }));
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    const parsed = parseFileImagesOutput(res.output);
    assert.ok(parsed, 'output is a file_images envelope');
    assert.equal(parsed!.kind, 'mcp');
    assert.equal(parsed!.images.length, 1);
    assert.equal(parsed!.images[0].source.media_type, 'image/png');
    assert.match(parsed!.text, /chart below/);
    assert.equal(res.metadata, undefined);
  });

  it('falls back to the text path when every image is un-compressible', async () => {
    const oversized = Buffer.alloc(6_500_000).toString('base64');
    const mgr = new MockManager(async () => ({
      content: [
        { type: 'text', text: 'just text then a bad image' },
        { type: 'image', data: oversized, mimeType: 'image/png' },
      ],
    }));
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute({}, makeContext(mgr));
    // No file_images envelope — the un-compressible image was skipped and the
    // result degraded to the text path (placeholder retained).
    assert.equal(parseFileImagesOutput(res.output), null);
    assert.match(res.output, /just text then a bad image/);
  });
});
