import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  fileReadTool,
  parsePageRange,
  classifyPdfError,
  coerceReadLineParam,
  encodeImageFile,
  ImageResizeError,
} from '../src/tools/file-read';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-file-read-c-'));
  getFileCache(tmpDir).clear();
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Gap: open-ended PDF page range "N-" (page N onward)
// RED before fix: parsePageRange("3-") returned an "Invalid page range" error
// because the anchored regex required a digit after the dash.
// ---------------------------------------------------------------------------
describe('parsePageRange – open-ended "N-"', () => {
  it('parses "3-" as a 20-page window from page 3 (no error)', () => {
    const r = parsePageRange('3-');
    assert.equal(r.error, undefined);
    assert.equal(r.start, 3);
    assert.equal(r.end, 22); // 3 .. 3+19 (per-request 20-page cap)
    assert.equal(r.openEnded, true);
  });

  it('parses a padded "  7-  " open-ended range', () => {
    const r = parsePageRange('  7-  ');
    assert.equal(r.error, undefined);
    assert.equal(r.start, 7);
    assert.equal(r.end, 26);
  });

  it('rejects open-ended "0-" (page must be >= 1)', () => {
    const r = parsePageRange('0-');
    assert.ok(r.error);
    assert.ok(r.error.includes('>= 1'));
  });

  it('still rejects a plain trailing-dash with other junk', () => {
    const r = parsePageRange('3-x');
    assert.ok(r.error);
    assert.ok(r.error.includes('Invalid page range'));
  });

  it('still parses a bounded range and still caps at 20 pages', () => {
    assert.equal(parsePageRange('3-7').end, 7);
    assert.ok(parsePageRange('1-25').error?.includes('max 20'));
  });
});

// ---------------------------------------------------------------------------
// Gap: password-protected PDF detection with actionable error
// RED before fix: every PDF failure collapsed to "Error reading PDF: <msg>".
// ---------------------------------------------------------------------------
describe('classifyPdfError – password/encryption', () => {
  it('maps a password error to an actionable message', () => {
    const out = classifyPdfError('PDFParse: file is password protected');
    assert.ok(out.includes('password-protected or encrypted'));
    assert.ok(out.includes('unprotected'));
    assert.ok(!out.includes('Error reading PDF'));
  });

  it('maps an encryption error to the same actionable message', () => {
    assert.ok(classifyPdfError('The document is encrypted').includes('password-protected or encrypted'));
  });

  it('keeps the generic "Error reading PDF" wording for other failures', () => {
    const out = classifyPdfError('Invalid xref table');
    assert.ok(out.includes('Error reading PDF'));
    assert.ok(out.includes('Invalid xref table'));
  });
});

// ---------------------------------------------------------------------------
// Gap: offset/limit schema validation + string coercion
// RED before fix: offset/limit were raw-cast (`as number`) with no validation,
// so limit:0 / offset:-1 / stringified numbers flowed straight into arithmetic.
// ---------------------------------------------------------------------------
describe('coerceReadLineParam', () => {
  it('returns {} (default) when omitted', () => {
    assert.deepEqual(coerceReadLineParam(undefined, { name: 'offset', min: 0 }), {});
    assert.deepEqual(coerceReadLineParam(null, { name: 'limit', min: 1 }), {});
  });

  it('accepts a plain number', () => {
    assert.deepEqual(coerceReadLineParam(5, { name: 'offset', min: 0 }), { value: 5 });
  });

  it('coerces a numeric string', () => {
    assert.deepEqual(coerceReadLineParam('42', { name: 'limit', min: 1 }), { value: 42 });
  });

  it('rejects a non-numeric string', () => {
    const r = coerceReadLineParam('abc', { name: 'limit', min: 1 });
    assert.ok(r.error?.includes('must be a number'));
  });

  it('rejects a fractional value', () => {
    const r = coerceReadLineParam(2.5, { name: 'offset', min: 0 });
    assert.ok(r.error?.includes('must be an integer'));
  });

  it('rejects below the minimum (limit:0, offset:-1)', () => {
    assert.ok(coerceReadLineParam(0, { name: 'limit', min: 1 }).error?.includes('>= 1'));
    assert.ok(coerceReadLineParam(-1, { name: 'offset', min: 0 }).error?.includes('>= 0'));
  });

  it('file_read rejects limit:0 with a corrective error (not an empty window)', async () => {
    const fp = path.join(tmpDir, 'f.txt');
    fs.writeFileSync(fp, 'a\nb\nc\n');
    const res = await fileReadTool.execute({ file_path: fp, limit: 0 }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('limit'));
  });

  it('file_read accepts a stringified offset/limit and reads the right window', async () => {
    const fp = path.join(tmpDir, 'nums.txt');
    fs.writeFileSync(fp, Array.from({ length: 20 }, (_, i) => `row${i}`).join('\n') + '\n');
    const res = await fileReadTool.execute(
      { file_path: fp, offset: '2', limit: '3' } as Record<string, unknown>,
      ctx(),
    );
    assert.ok(!res.isError);
    assert.ok(res.output.includes('row2'));
    assert.ok(res.output.includes('row4'));
    assert.ok(!res.output.includes('row5'));
  });
});

// ---------------------------------------------------------------------------
// Gap: distinct empty-file vs offset-past-end warnings
// RED before fix: both collapsed to the misleading "(empty file)".
// ---------------------------------------------------------------------------
describe('fileReadTool – offset past end vs empty file', () => {
  it('reports the real line count when a range offset overshoots a non-empty file', async () => {
    const fp = path.join(tmpDir, 'short.txt');
    fs.writeFileSync(fp, 'l1\nl2\nl3\n');
    const res = await fileReadTool.execute({ file_path: fp, offset: 50, limit: 10 }, ctx());
    assert.ok(!res.isError);
    assert.ok(res.output.includes('shorter than the provided offset (50)'));
    assert.ok(res.output.includes('3 lines'));
    assert.ok(!res.output.includes('(empty file)'));
  });

  it('still returns "(empty file)" for a genuinely empty file via the range path', async () => {
    const fp = path.join(tmpDir, 'empty.txt');
    fs.writeFileSync(fp, '');
    const res = await fileReadTool.execute({ file_path: fp, offset: 0, limit: 10 }, ctx());
    assert.equal(res.output, '(empty file)');
  });
});

// ---------------------------------------------------------------------------
// Gap: empty (0-byte) image buffer guard
// RED before fix: a 0-byte image slipped through and produced an empty base64
// payload instead of an actionable error.
// ---------------------------------------------------------------------------
describe('encodeImageFile / file_read – empty image guard', () => {
  it('encodeImageFile throws ImageResizeError for a 0-byte image', async () => {
    const fp = path.join(tmpDir, 'empty.png');
    fs.writeFileSync(fp, Buffer.alloc(0));
    await assert.rejects(encodeImageFile(fp), (err: unknown) => {
      assert.ok(err instanceof ImageResizeError);
      assert.ok((err as Error).message.includes('empty'));
      return true;
    });
  });

  it('file_read returns an actionable error (not an empty image block) for a 0-byte image', async () => {
    const fp = path.join(tmpDir, 'empty2.png');
    fs.writeFileSync(fp, Buffer.alloc(0));
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.toLowerCase().includes('empty'));
    assert.ok(!res.output.includes('"data":""'));
  });
});
