/**
 * U208 (w101) — Structured spill metadata + background-source discriminator.
 *
 * Parity: Claude Code's ExecResult carries structured `{ outputFilePath,
 * outputFileSize }` when stdout overflows inline to a file (ShellCommand.ts
 * #handleExit), and distinguishes a user-requested background (`backgroundedByUser`)
 * from an assistant auto-background. AMI previously exposed the spill file only via
 * the banner text (callers had to re-parse it) and tagged ONLY the auto-background
 * case. This unit adds:
 *   - `spillToolTextWithMeta` returning the on-disk path/size structurally;
 *   - bash foreground ToolResult.metadata.{outputFilePath,outputFileSize};
 *   - bash run_in_background ToolResult.metadata.backgroundedByUser.
 *
 * RED-GREEN: drop the `outputFilePath` metadata spread in bash.ts and the
 * "surfaces outputFilePath/outputFileSize" test fails; drop `backgroundedByUser`
 * and the "tags an explicit background" test fails.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { spillToolText, spillToolTextWithMeta } from '../src/tools/spill-output';
import { projectStateDir } from '../src/reference-journal';
import { bashTool } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, ...overrides };
}

// ---------------------------------------------------------------------------
// spillToolTextWithMeta — structured on-disk path/size
// ---------------------------------------------------------------------------
describe('U208 w101 spillToolTextWithMeta — structured spill metadata', () => {
  let tmpDir: string;
  afterEach(() => { if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns text verbatim with NO path/size when under the limit', () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u208w101-'));
    const text = 'short';
    const res = spillToolTextWithMeta(tmpDir, 'test', text, 1000);
    assert.equal(res.text, text);
    assert.equal(res.outputFilePath, undefined);
    assert.equal(res.outputFileSize, undefined);
  });

  it('returns verbatim with no meta for a non-finite limit (hard opt-out)', () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u208w101-'));
    const text = 'z'.repeat(5000);
    const res = spillToolTextWithMeta(tmpDir, 'test', text, Infinity);
    assert.equal(res.text, text);
    assert.equal(res.outputFilePath, undefined);
  });

  it('exposes the on-disk path and byte size on a real spill', () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u208w101-'));
    const prev = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = tmpDir;
    try {
      const text = 'x'.repeat(5000);
      const res = spillToolTextWithMeta(tmpDir, 'big', text, 1000);
      assert.ok(res.text.length < text.length, 'display text is bounded');
      assert.ok(res.text.includes('persisted to'));
      // The structured fields point at the real file without re-parsing the banner.
      assert.ok(res.outputFilePath, 'outputFilePath is set on a real spill');
      assert.equal(res.outputFileSize, Buffer.byteLength(text));
      assert.ok(fs.existsSync(res.outputFilePath!), 'the spill file exists on disk');
      assert.equal(fs.readFileSync(res.outputFilePath!, 'utf-8'), text);
      // The structured path is exactly the banner's path (one canonical location).
      assert.ok(res.text.includes(res.outputFilePath!));
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
      else process.env.SUPERINFERENCE_DATA_DIR = prev;
    }
  });

  it('counts BYTES not chars for a multi-byte payload', () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u208w101-'));
    const prev = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = tmpDir;
    try {
      // 2000 × '€' (3 bytes each in UTF-8) = 2000 chars but 6000 bytes.
      const text = '€'.repeat(2000);
      const res = spillToolTextWithMeta(tmpDir, 'mb', text, 500);
      assert.ok(res.outputFilePath, 'spilled');
      assert.equal(res.outputFileSize, Buffer.byteLength(text));
      assert.notEqual(res.outputFileSize, text.length, 'byte size differs from char length');
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
      else process.env.SUPERINFERENCE_DATA_DIR = prev;
    }
  });

  it('records NO path/size when the spill write fails (banner-only)', () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u208w101-'));
    const roData = fs.mkdtempSync(path.join(os.tmpdir(), 'u208w101-nowrite-'));
    fs.chmodSync(roData, 0o555);
    const prev = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = roData;
    try {
      const text = 'HEAD' + 'x'.repeat(5000) + 'TAIL';
      const res = spillToolTextWithMeta(tmpDir, 'fail', text, 1000);
      assert.ok(res.text.length < text.length, 'still truncated');
      assert.ok(res.text.includes('could not persist'), 'banner reports the write failure');
      assert.equal(res.outputFilePath, undefined, 'no structured path on failure');
      assert.equal(res.outputFileSize, undefined);
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
      else process.env.SUPERINFERENCE_DATA_DIR = prev;
      fs.chmodSync(roData, 0o755);
      fs.rmSync(roData, { recursive: true, force: true });
    }
  });

  it('spillToolText wrapper returns exactly the .text of the structured result', () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u208w101-'));
    const prev = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = tmpDir;
    try {
      const text = 'q'.repeat(4000);
      const full = spillToolTextWithMeta(tmpDir, 'wrap', text, 1000);
      const thin = spillToolText(tmpDir, 'wrap', text, 1000);
      assert.equal(thin, full.text);
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
      else process.env.SUPERINFERENCE_DATA_DIR = prev;
    }
  });
});

// ---------------------------------------------------------------------------
// bash integration — ToolResult.metadata carries the new structured fields
// ---------------------------------------------------------------------------
describe('U208 w101 bashTool — structured result metadata', () => {
  it('surfaces outputFilePath/outputFileSize when stdout overflows the cap', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u208w101-bash-'));
    const prev = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = dir;
    try {
      // A tiny inline cap forces even modest stdout to spill to disk.
      const result = await bashTool.execute(
        { command: 'printf "%05000d" 0' },
        ctx({ cwd: dir, outputBudget: { bashMaxChars: 500 } }),
      );
      assert.match(result.output, /persisted to/, 'stdout was spilled');
      const meta = result.metadata as Record<string, unknown>;
      assert.ok(typeof meta.outputFilePath === 'string', 'outputFilePath is in metadata');
      assert.ok(typeof meta.outputFileSize === 'number', 'outputFileSize is in metadata');
      assert.ok((meta.outputFileSize as number) > 500, 'size reflects the full output');
      assert.ok(fs.existsSync(meta.outputFilePath as string), 'the referenced file exists');
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
      else process.env.SUPERINFERENCE_DATA_DIR = prev;
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('does NOT set outputFilePath when stdout fits inline', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u208w101-bash-'));
    try {
      const result = await bashTool.execute(
        { command: 'echo hello' },
        ctx({ cwd: dir, outputBudget: { bashMaxChars: 10000 } }),
      );
      const meta = (result.metadata ?? {}) as Record<string, unknown>;
      assert.equal(meta.outputFilePath, undefined, 'no spill → no structured path');
      assert.equal(meta.outputFileSize, undefined);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('tags an explicit run_in_background with backgroundedByUser (not auto)', async () => {
    const pm = {
      spawn: () => 'task-u208',
      get: () => ({ pid: 4242, outputPath: '/tmp/task-u208.out' }),
    };
    const result = await bashTool.execute(
      { command: 'npm run dev', run_in_background: true },
      ctx({ processManager: pm } as Partial<ToolContext>),
    );
    assert.equal(result.isError, false);
    const meta = result.metadata as Record<string, unknown>;
    assert.equal(meta.backgroundedByUser, true, 'explicit background is user-initiated');
    assert.equal(meta.backgrounded, true);
    assert.equal(meta.taskId, 'task-u208');
    // It is NOT the assistant auto-background path.
    assert.equal(meta.assistantAutoBackgrounded, undefined);
  });
});
