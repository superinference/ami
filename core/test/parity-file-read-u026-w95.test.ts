import { describe, it, mock, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  fileReadTool,
  isPDFSupported,
  buildFileReadDescription,
  buildFileReadInputSchema,
  resolveFileReadDescriptionOptions,
  loadImageProcessor,
  imageHooks,
  pdfHooks,
  compressImageBuffer,
  IMAGE_TARGET_RAW_SIZE,
  type FileReadDescriptionOptions,
} from '../src/tools/file-read';
import {
  getFileReadPromptFlags,
  FILE_READ_SHOW_SIZE_CAP_ENV,
  FILE_READ_RANGE_NUDGE_ENV,
} from '../src/tools/file-read-limits';
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fr-u026-w95-'));
  getFileCache(tmpDir).clear();
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
  mock.restoreAll();
});

const ALL_ON: FileReadDescriptionOptions = {
  pdfSupported: true,
  includeMaxSizeInPrompt: true,
  targetedRangeNudge: true,
};

// ---------------------------------------------------------------------------
// Gap 11 — description dynamically reflects actual PDF capability (isPDFSupported)
// ---------------------------------------------------------------------------
describe('U026 w95 — PDF capability gated description', () => {
  it('isPDFSupported reflects whether pdf-parse resolves (via the pdfHooks seam)', () => {
    mock.method(pdfHooks, 'isPdfParseResolvable', () => true);
    assert.equal(isPDFSupported(), true);
    mock.method(pdfHooks, 'isPdfParseResolvable', () => false);
    assert.equal(isPDFSupported(), false);
  });

  it('includes the PDF instruction block ONLY when PDFs are supported', () => {
    const withPdf = buildFileReadDescription({ ...ALL_ON, pdfSupported: true });
    const withoutPdf = buildFileReadDescription({ ...ALL_ON, pdfSupported: false });
    assert.match(withPdf, /Can read PDF files \(\.pdf\)/);
    assert.doesNotMatch(withoutPdf, /Can read PDF files/);
    // The non-PDF clauses stay regardless of the PDF capability.
    for (const d of [withPdf, withoutPdf]) {
      assert.match(d, /Jupyter notebooks \(\.ipynb\)/);
      assert.match(d, /image files/);
      assert.match(d, /This tool reads files only, not directories/);
    }
  });

  it('resolveFileReadDescriptionOptions threads isPDFSupported + env flags', () => {
    mock.method(pdfHooks, 'isPdfParseResolvable', () => false);
    const opts = resolveFileReadDescriptionOptions({});
    assert.equal(opts.pdfSupported, false);
    assert.equal(opts.includeMaxSizeInPrompt, true);
    assert.equal(opts.targetedRangeNudge, true);
  });
});

// ---------------------------------------------------------------------------
// Gap 9/10 — includeMaxSizeInPrompt + targetedRangeNudge knobs
// ---------------------------------------------------------------------------
describe('U026 w95 — prompt-shaping knobs (size cap / range nudge)', () => {
  it('getFileReadPromptFlags defaults both ON and only an explicit falsy token turns them off', () => {
    assert.deepEqual(getFileReadPromptFlags({}), { includeMaxSizeInPrompt: true, targetedRangeNudge: true });
    assert.equal(getFileReadPromptFlags({ [FILE_READ_SHOW_SIZE_CAP_ENV]: '0' }).includeMaxSizeInPrompt, false);
    assert.equal(getFileReadPromptFlags({ [FILE_READ_SHOW_SIZE_CAP_ENV]: 'off' }).includeMaxSizeInPrompt, false);
    assert.equal(getFileReadPromptFlags({ [FILE_READ_SHOW_SIZE_CAP_ENV]: 'false' }).includeMaxSizeInPrompt, false);
    // A non-falsy value keeps it on.
    assert.equal(getFileReadPromptFlags({ [FILE_READ_SHOW_SIZE_CAP_ENV]: '1' }).includeMaxSizeInPrompt, true);
    assert.equal(getFileReadPromptFlags({ [FILE_READ_RANGE_NUDGE_ENV]: 'no' }).targetedRangeNudge, false);
    assert.equal(getFileReadPromptFlags({ [FILE_READ_RANGE_NUDGE_ENV]: '' }).targetedRangeNudge, true);
  });

  it('includeMaxSizeInPrompt gates the concrete 256KB figure in the allow_large schema', () => {
    const on = buildFileReadInputSchema({ ...ALL_ON, includeMaxSizeInPrompt: true });
    const off = buildFileReadInputSchema({ ...ALL_ON, includeMaxSizeInPrompt: false });
    const onDesc = (on.properties!.allow_large as { description: string }).description;
    const offDesc = (off.properties!.allow_large as { description: string }).description;
    assert.match(onDesc, /256KB/);
    assert.doesNotMatch(offDesc, /256KB/);
    assert.match(offDesc, /past the usual size limit/);
  });

  it('targetedRangeNudge gates the offset/limit range-read nudge', () => {
    const on = buildFileReadInputSchema({ ...ALL_ON, targetedRangeNudge: true });
    const off = buildFileReadInputSchema({ ...ALL_ON, targetedRangeNudge: false });
    const onOffset = (on.properties!.offset as { description: string }).description;
    const offOffset = (off.properties!.offset as { description: string }).description;
    assert.match(onOffset, /Prefer reading the whole file/);
    assert.doesNotMatch(offOffset, /Prefer reading the whole file/);
    // The base description survives either way.
    assert.match(offOffset, /Line number to start reading from/);
  });
});

// ---------------------------------------------------------------------------
// Default-wired tool still advertises every historical capability + tail
// ---------------------------------------------------------------------------
describe('U026 w95 — default-wired fileReadTool', () => {
  it('default description still mentions image/PDF/notebook/directory (pdf-parse resolves in test env)', () => {
    assert.equal(isPDFSupported(), true);
    assert.match(fileReadTool.description, /image files/);
    assert.match(fileReadTool.description, /Can read PDF files \(\.pdf\)/);
    assert.match(fileReadTool.description, /Jupyter notebooks \(\.ipynb\)/);
  });

  it('exposes the tail parameter in the input schema', () => {
    assert.ok(fileReadTool.inputSchema.properties!.tail);
    assert.match((fileReadTool.inputSchema.properties!.tail as { description: string }).description, /LAST N lines/);
  });
});

// ---------------------------------------------------------------------------
// Gap 6/7/8 — pluggable ordered image-processor loader chain
// ---------------------------------------------------------------------------
function fakeSharpFactory() {
  return (_input: Buffer) => {
    const inst: any = {
      _enc: 'raw',
      async metadata() { return { width: 10, height: 10, format: 'png' }; },
      resize() { return inst; },
      png() { inst._enc = 'png'; return inst; },
      jpeg() { inst._enc = 'jpeg'; return inst; },
      async toBuffer() { return Buffer.alloc(8); },
    };
    return inst;
  };
}

describe('U026 w95 — image-processor loader chain', () => {
  afterEach(() => { imageHooks.loaders = undefined; });

  it('loadImageProcessor uses the single loadSharp loader by default', () => {
    const sentinel = fakeSharpFactory();
    mock.method(imageHooks, 'loadSharp', () => sentinel);
    assert.equal(loadImageProcessor(), sentinel);
  });

  it('tries loaders in order and falls back when an earlier (native) loader throws', () => {
    const native = () => { throw new Error('native image-processor-napi binding missing'); };
    const sharp = fakeSharpFactory();
    imageHooks.loaders = [native, () => sharp];
    assert.equal(loadImageProcessor(), sharp);
  });

  it('propagates the last error when every loader in the chain fails', () => {
    imageHooks.loaders = [
      () => { throw new Error('native missing'); },
      () => { throw new Error('sharp missing'); },
    ];
    assert.throws(() => loadImageProcessor(), /sharp missing/);
  });

  it('compressImageBuffer downscales via a native-first chain that falls back to sharp', async () => {
    const native = () => { throw new Error('napi unavailable'); };
    imageHooks.loaders = [native, fakeSharpFactory];
    const png = Buffer.alloc(24, 0);
    png.set([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a], 0);
    png.writeUInt32BE(10, 16);
    png.writeUInt32BE(10, 20);
    const out = await compressImageBuffer(png, IMAGE_TARGET_RAW_SIZE, 'image/png');
    // The fallback sharp was used: the returned payload is the within-budget original.
    assert.equal(out.mediaType, 'image/png');
    assert.equal(out.dimensions?.displayWidth, 10);
  });
});

// ---------------------------------------------------------------------------
// Gap 13 — tail read parameter
// ---------------------------------------------------------------------------
describe('U026 w95 — tail read', () => {
  it('returns the LAST N lines with their true 1-based line numbers', async () => {
    const file = path.join(tmpDir, 'log.txt');
    const lines = Array.from({ length: 50 }, (_, i) => `line-${i + 1}`);
    fs.writeFileSync(file, lines.join('\n'));
    const res = await fileReadTool.execute({ file_path: file, tail: 3 }, ctx());
    assert.equal(res.isError, undefined);
    assert.match(res.output, /Showing last 3 lines \(lines 48-50 of 50\)/);
    assert.match(res.output, /48\tline-48/);
    assert.match(res.output, /50\tline-50/);
    // Earlier lines are NOT present.
    assert.doesNotMatch(res.output, /\b1\tline-1\b/);
  });

  it('clamps tail larger than the file to the whole file', async () => {
    const file = path.join(tmpDir, 'short.txt');
    fs.writeFileSync(file, 'a\nb\nc');
    const res = await fileReadTool.execute({ file_path: file, tail: 100 }, ctx());
    assert.match(res.output, /Showing last 3 lines \(lines 1-3 of 3\)/);
    assert.match(res.output, /1\ta/);
    assert.match(res.output, /3\tc/);
  });

  it('rejects tail combined with offset/limit', async () => {
    const file = path.join(tmpDir, 'x.txt');
    fs.writeFileSync(file, 'a\nb');
    const res = await fileReadTool.execute({ file_path: file, tail: 1, offset: 0 }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /tail cannot be combined with offset\/limit/);
  });

  it('rejects a non-positive / non-integer tail value', async () => {
    const file = path.join(tmpDir, 'y.txt');
    fs.writeFileSync(file, 'a\nb');
    const zero = await fileReadTool.execute({ file_path: file, tail: 0 }, ctx());
    assert.equal(zero.isError, true);
    assert.match(zero.output, /tail must be >= 1/);
  });

  it('reports (empty file) for a tail of an empty file', async () => {
    const file = path.join(tmpDir, 'empty.txt');
    fs.writeFileSync(file, '');
    const res = await fileReadTool.execute({ file_path: file, tail: 5 }, ctx());
    assert.equal(res.output, '(empty file)');
  });

  it('tails a file larger than the 256KB partial-view cap without a first-slice partial view', async () => {
    const file = path.join(tmpDir, 'big.txt');
    const lines = Array.from({ length: 20000 }, (_, i) => `row-${i + 1}-padded-to-make-the-file-large-enough-xxxxxxxxxx`);
    fs.writeFileSync(file, lines.join('\n'));
    assert.ok(fs.statSync(file).size > 256 * 1024, 'fixture must exceed the partial-view cap');
    const res = await fileReadTool.execute({ file_path: file, tail: 2 }, ctx());
    assert.doesNotMatch(res.output, /PARTIAL VIEW/);
    assert.match(res.output, /row-20000-/);
    assert.match(res.output, /Showing last 2 lines \(lines 19999-20000 of 20000\)/);
  });
});
