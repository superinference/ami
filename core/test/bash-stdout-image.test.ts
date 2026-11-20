import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { bashTool, maybeBuildShellImageOutput } from '../src/tools/bash';
import { parseImageToolOutput } from '../src/tools/file-read';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// A valid 1x1 PNG (transparent) — small enough to pass every size/dimension cap.
const PNG_1x1 =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==';

// ---------------------------------------------------------------------------
// maybeBuildShellImageOutput — surface a base64 image data URI from stdout as a
// real image tool_result instead of a truncated/corrupt text blob.
// Parity: the reference BashTool utils isImageOutput / buildImageToolResult.
// ---------------------------------------------------------------------------

describe('maybeBuildShellImageOutput', () => {
  it('returns null for ordinary text output', async () => {
    assert.equal(await maybeBuildShellImageOutput('hello world'), null);
    assert.equal(await maybeBuildShellImageOutput(''), null);
    // A data URI embedded mid-text (not the whole output) must NOT be treated as an image.
    assert.equal(await maybeBuildShellImageOutput(`log line\ndata:image/png;base64,${PNG_1x1}`), null);
  });

  it('builds an engine-parseable image block for a bare PNG data URI', async () => {
    const out = await maybeBuildShellImageOutput(`data:image/png;base64,${PNG_1x1}`);
    assert.ok(out, 'expected a non-null image output');
    const parsed = JSON.parse(out!);
    assert.equal(parsed.type, 'image');
    assert.equal(parsed.source.type, 'base64');
    assert.match(parsed.source.media_type, /^image\//);
    assert.ok(parsed.source.data.length > 0);
    // The engine consumes exactly this shape via parseImageToolOutput.
    const roundTrip = parseImageToolOutput(out!);
    assert.ok(roundTrip && roundTrip.length === 1);
  });

  it('tolerates whitespace/newlines inside the base64 payload', async () => {
    const wrapped = PNG_1x1.replace(/(.{20})/g, '$1\n');
    const out = await maybeBuildShellImageOutput(`data:image/png;base64,\n${wrapped}\n`);
    assert.ok(out);
    assert.equal(JSON.parse(out!).type, 'image');
  });

  it('omits an oversized image instead of decoding it (OOM guard)', async () => {
    const huge = `data:image/png;base64,${'A'.repeat(28 * 1024 * 1024)}`;
    const out = await maybeBuildShellImageOutput(huge);
    assert.ok(out && out.startsWith('[Image output omitted'), out ?? 'null');
  });
});

// ---------------------------------------------------------------------------
// End-to-end through bashTool.execute: a command whose stdout is an image data
// URI yields the image JSON output (so the engine renders it as an image), while
// ordinary commands still return text.
// ---------------------------------------------------------------------------

describe('bashTool.execute – stdout image data URI', () => {
  it('emits an image block when a command prints a data URI', async () => {
    const result = await bashTool.execute(
      { command: `printf '%s' 'data:image/png;base64,${PNG_1x1}'` },
      ctx(),
    );
    assert.equal(result.isError, false, result.output);
    const parsed = parseImageToolOutput(result.output);
    assert.ok(parsed && parsed.length === 1, `expected image output, got: ${result.output.slice(0, 80)}`);
  });

  it('still returns plain text for a non-image command', async () => {
    const result = await bashTool.execute({ command: 'echo not-an-image' }, ctx());
    assert.equal(result.isError, false);
    assert.equal(parseImageToolOutput(result.output), null);
    assert.ok(result.output.includes('not-an-image'));
  });
});
