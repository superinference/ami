import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { processMcpContent } from '../src/tools/mcp-tool';

// ---------------------------------------------------------------------------
// U069 / w74 — MCP inline content size reporting is HUMAN-READABLE (formatSize),
// matching the saved-blob message convention, instead of a raw `N bytes` count.
// ---------------------------------------------------------------------------

describe('processMcpContent human-readable inline sizes', () => {
  // ~3 KiB of base64 => > 2 KiB raw => renders in KB, never a raw "bytes" count.
  const bigB64 = 'A'.repeat(3072);

  it('renders image size as KB, not raw bytes', () => {
    const out = processMcpContent({ content: [{ type: 'image', mimeType: 'image/png', data: bigB64 }] });
    assert.ok(out);
    assert.match(out.text, /\[Image: image\/png, [\d.]+ KB base64\]/);
    assert.ok(!/\d+ bytes base64/.test(out.text), `raw bytes leaked: ${out.text}`);
  });

  it('renders audio size as KB, not raw bytes', () => {
    const out = processMcpContent({ content: [{ type: 'audio', mimeType: 'audio/mpeg', data: bigB64 }] });
    assert.ok(out);
    assert.match(out.text, /\[Audio: audio\/mpeg, [\d.]+ KB base64\]/);
    assert.ok(!/\d+ bytes base64/.test(out.text));
  });

  it('renders a binary resource size human-readably', () => {
    const out = processMcpContent({
      content: [{ type: 'resource', resource: { uri: 'file:///x.bin', mimeType: 'application/octet-stream', blob: bigB64 } }],
    });
    assert.ok(out);
    // Either inlined with a formatted size, or persisted via the saved-blob
    // message (also formatSize-based) — neither path emits a raw "N bytes" count.
    assert.ok(!/\d+ bytes\)/.test(out.text), `raw bytes leaked: ${out.text}`);
  });
});
