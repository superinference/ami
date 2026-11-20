// Unit U088 — MCP resource blob persistence: mime→extension parity with the reference's
// `extensionForMimeType`. The on-disk extension must come from a conservative
// allowlist (so native tools open the file correctly) and default to `bin` for
// ANYTHING not in the allowlist — never a garbage extension derived from an
// untrusted mimeType subtype. Exercised end-to-end through readMcpResourceTool so
// the real decode→persist→blobSavedTo path is covered.
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { readMcpResourceTool } from '../src/tools/mcp-resources';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return { cwd: os.tmpdir(), abortSignal: new AbortController().signal } as ToolContext;
}
function withManager(impl: Record<string, unknown>): ToolContext {
  return { ...ctx(), _mcpManager: impl as unknown as ToolContext['_mcpManager'] } as ToolContext;
}

const written: string[] = [];
after(() => {
  for (const f of written) fs.rmSync(f, { force: true });
});

async function extFor(mimeType: string): Promise<string> {
  const payload = Buffer.from('binary-bytes-for-' + mimeType, 'utf8');
  const res = await readMcpResourceTool.execute!(
    { server: 's', uri: 'file://blob' },
    withManager({ readResource: async () => ({ contents: [{ uri: 'file://blob', mimeType, blob: payload.toString('base64') }] }) }),
  );
  const contents = (res.metadata as { contents: Array<{ blobSavedTo?: string }> }).contents;
  const file = contents[0].blobSavedTo!;
  assert.ok(file, `blobSavedTo present for ${mimeType}`);
  written.push(file);
  return path.extname(file).replace(/^\./, '');
}

describe('U088 — mime→extension allowlist parity (Claude extensionForMimeType)', () => {
  it('maps office document mime types to their canonical extensions', async () => {
    assert.equal(await extFor('application/vnd.openxmlformats-officedocument.wordprocessingml.document'), 'docx');
    assert.equal(await extFor('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'), 'xlsx');
    assert.equal(await extFor('application/vnd.openxmlformats-officedocument.presentationml.presentation'), 'pptx');
    assert.equal(await extFor('application/msword'), 'doc');
    assert.equal(await extFor('application/vnd.ms-excel'), 'xls');
  });

  it('maps audio/video/text mime types added for parity', async () => {
    assert.equal(await extFor('audio/mpeg'), 'mp3');
    assert.equal(await extFor('audio/wav'), 'wav');
    assert.equal(await extFor('audio/ogg'), 'ogg');
    assert.equal(await extFor('video/mp4'), 'mp4');
    assert.equal(await extFor('video/webm'), 'webm');
    assert.equal(await extFor('text/markdown'), 'md');
  });

  it('forces bin for any mime type not in the allowlist (no garbage subtype extension)', async () => {
    assert.equal(await extFor('application/x-weird-custom-thing'), 'bin');
    assert.equal(await extFor('application/octet-stream'), 'bin');
    assert.equal(await extFor('chemical/x-pdb'), 'bin');
  });

  it('strips charset/boundary params before matching and still lands a known ext', async () => {
    assert.equal(await extFor('text/csv; charset=utf-8'), 'csv');
    assert.equal(await extFor('application/pdf;version=1.7'), 'pdf');
  });
});
