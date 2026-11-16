import { describe, it, mock, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  fileReadTool,
  compressImageBuffer,
  encodeImageFile,
  imageHooks,
  IMAGE_MAX_WIDTH,
  isBinaryByExtension,
  KNOWN_BINARY_EXTENSIONS,
} from '../src/tools/file-read';
import {
  normalizePathForComparison,
  _pathCompareHooks,
  isCaseInsensitiveFs,
} from '../src/tools/tool-utils';
import { __resetLoggerForTest } from '../src/logger';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fr-u026-w69-'));
  getFileCache(tmpDir).clear();
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

/** Minimal PNG header (8-byte sig + IHDR width/height big-endian at 16/20). */
function pngHeader(width: number, height: number, pad = 0): Buffer {
  const buf = Buffer.alloc(Math.max(24, 24 + pad), 0);
  buf.set([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a], 0);
  buf.writeUInt32BE(width, 16);
  buf.writeUInt32BE(height, 20);
  return buf;
}

function fakeSharp(
  meta: { width: number; height: number; format: string },
  sizes: { raw?: number; png?: number; jpeg?: number },
) {
  return (_input: Buffer) => {
    const inst: any = {
      _enc: 'raw',
      async metadata() { return meta; },
      resize() { return inst; },
      png() { inst._enc = 'png'; return inst; },
      jpeg() { inst._enc = 'jpeg'; return inst; },
      async toBuffer() {
        const len = inst._enc === 'png' ? (sizes.png ?? 10)
          : inst._enc === 'jpeg' ? (sizes.jpeg ?? 10)
          : (sizes.raw ?? 10);
        return Buffer.alloc(len);
      },
    };
    return inst;
  };
}

// ---------------------------------------------------------------------------
// GAP U026: broadened centralized binary-extension set. The informative
// "[Binary file: <ext> — <name>. Use bash ...]" affordance must now cover the
// extra categories Claude's centralized BINARY_EXTENSIONS includes (fonts/video
// variants/archives/bytecode/design/db/lock-data) that AMI previously omitted.
// RED before fix: these extensions were NOT in KNOWN_BINARY_EXTENSIONS, so a
// text-content file with one of them was line-numbered and returned as text
// instead of the binary affordance.
// ---------------------------------------------------------------------------
describe('U026-w69: broadened binary-extension coverage', () => {
  const newlyCovered = [
    '.tif', '.tiff', '.wmv', '.flv', '.m4v', '.mpeg', '.mpg', '.aiff',
    '.tgz', '.z', '.obj', '.app', '.ear', '.rlib', '.idx', '.eps',
    '.max', '.lockb', '.data',
  ];

  for (const ext of newlyCovered) {
    it(`treats ${ext} as a known binary extension`, () => {
      assert.equal(isBinaryByExtension(ext), true, `${ext} should be binary-by-extension`);
      assert.ok(KNOWN_BINARY_EXTENSIONS.has(ext));
    });

    it(`emits the informative binary affordance for a ${ext} file with text content`, async () => {
      // Benign TEXT content: before the broadening this file read as text (no
      // NUL / control density), so the binary affordance is the discriminator.
      const p = path.join(tmpDir, `sample${ext}`);
      fs.writeFileSync(p, 'hello world\nsecond line\n');
      const res = await fileReadTool.execute({ file_path: p }, ctx());
      assert.equal(res.isError, undefined);
      assert.ok(
        res.output.includes(`[Binary file: ${ext} — sample${ext}. Use bash to inspect binary files.]`),
        `expected informative binary affordance, got: ${res.output.slice(0, 120)}`,
      );
    });
  }

  it('still reads an ordinary text extension as text (regression)', async () => {
    const p = path.join(tmpDir, 'ok.ts');
    fs.writeFileSync(p, 'const a = 1;\n');
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('const a = 1;'));
    assert.ok(!res.output.includes('Use bash to inspect binary files'));
  });
});

// ---------------------------------------------------------------------------
// GAP U026: NFC Unicode normalization of the path used for containment/equality
// comparisons. The shared normalizePathForComparison now folds to NFC so a
// decomposed (NFD) path component — as macOS/HFS+ realpath returns — compares
// equal to the composed (NFC) form. RED before fix: no .normalize('NFC'), so the
// two Unicode-equivalent spellings compared UNEQUAL.
// ---------------------------------------------------------------------------
describe('U026-w69: NFC-normalizing path comparison', () => {
  afterEach(() => { _pathCompareHooks.platform = process.platform; });

  it('folds NFD and NFC spellings of the same name to one key (case-sensitive fs)', () => {
    _pathCompareHooks.platform = 'linux';
    assert.equal(isCaseInsensitiveFs(), false);
    const nfc = '/repo/café/file.ts';        // é as U+00E9 (composed)
    const nfd = '/repo/café/file.ts';       // e + U+0301 (decomposed)
    assert.notEqual(nfc, nfd, 'inputs must differ before normalization');
    assert.equal(normalizePathForComparison(nfc), normalizePathForComparison(nfd));
  });

  it('still folds on a case-insensitive fs (NFC + lowercase)', () => {
    _pathCompareHooks.platform = 'darwin';
    assert.equal(isCaseInsensitiveFs(), true);
    const nfc = '/Repo/Café/File.TS';
    const nfd = '/Repo/Café/File.TS';
    assert.equal(normalizePathForComparison(nfc), normalizePathForComparison(nfd));
    assert.equal(normalizePathForComparison(nfc), '/repo/café/file.ts');
  });

  it('is a no-op for plain ASCII paths (regression)', () => {
    _pathCompareHooks.platform = 'linux';
    assert.equal(normalizePathForComparison('/Foo/../Foo/Bar.TS'), '/Foo/Bar.TS');
  });

  it('a symlink to a file inside cwd still reads (containment wiring intact)', async () => {
    const real = path.join(tmpDir, 'real.txt');
    fs.writeFileSync(real, 'line one\nline two\n');
    const link = path.join(tmpDir, 'link.txt');
    fs.symlinkSync('real.txt', link);
    const res = await fileReadTool.execute({ file_path: link }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('line one'));
  });
});

// ---------------------------------------------------------------------------
// GAP U026 (wrong-ancestor path fallback): when the IMMEDIATE parent directory
// does not exist (a misspelled directory component), the not-found error now
// walks up to the nearest existing ancestor and suggests the corrected path via
// suggestPathCorrection's 'missing-dir' recovery. RED before fix: the not-found
// handler only called suggestSimilarPaths (which returns [] when the parent dir
// is missing), so no "Did you mean" was offered.
// ---------------------------------------------------------------------------
describe('U026-w69: wrong-ancestor (misspelled directory) recovery', () => {
  it('suggests the corrected directory when the parent folder is misspelled', async () => {
    fs.mkdirSync(path.join(tmpDir, 'source'));
    fs.writeFileSync(path.join(tmpDir, 'source', 'foo.ts'), 'export const x = 1;\n');
    // Model rooted the path at a misspelled directory component ("sorce").
    const wrong = path.join(tmpDir, 'sorce', 'foo.ts');
    const res = await fileReadTool.execute({ file_path: wrong }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Did you mean:'), `expected a suggestion, got: ${res.output}`);
    assert.ok(res.output.includes(path.join(tmpDir, 'source', 'foo.ts')));
  });

  it('a plain missing file in an existing dir keeps the sibling suggestion path', async () => {
    fs.writeFileSync(path.join(tmpDir, 'readme.md'), '# hi\n');
    const res = await fileReadTool.execute({ file_path: path.join(tmpDir, 'readme.mdx') }, ctx());
    assert.equal(res.isError, true);
    // sibling same-stem suggestion still works; cwd note always present
    assert.ok(res.output.includes('your current working directory is'));
  });
});

// ---------------------------------------------------------------------------
// GAP U026: image-processor degradation + resize progress breadcrumbs. The
// compressor now emits debug/warn log lines so an operator can trace a degraded
// or slow image read. RED before fix: no such log lines existed.
// ---------------------------------------------------------------------------
describe('U026-w69: image-processor logging', () => {
  let logFile: string;
  const prevEnv: Record<string, string | undefined> = {};

  beforeEach(() => {
    for (const k of ['AMI_DEBUG_FILE', 'AMI_DISABLE_DEBUG_LOG', 'AMI_DEBUG_TO_STDERR', 'AMI_DEBUG_COMPONENTS', 'AMI_LOG_LEVEL']) {
      prevEnv[k] = process.env[k];
      delete process.env[k];
    }
    logFile = path.join(tmpDir, 'debug.log');
    process.env.AMI_DEBUG_FILE = logFile;
    __resetLoggerForTest();
  });

  afterEach(() => {
    for (const k of Object.keys(prevEnv)) {
      if (prevEnv[k] === undefined) delete process.env[k];
      else process.env[k] = prevEnv[k];
    }
    __resetLoggerForTest();
  });

  function readLog(): string {
    try { return fs.readFileSync(logFile, 'utf-8'); } catch { return ''; }
  }

  it('warns when the image processor is unavailable (sharp throws)', async () => {
    mock.method(imageHooks, 'loadSharp', (() => { throw new Error('no sharp'); }) as any);
    const raw = pngHeader(100, 100, 10);
    const out = await compressImageBuffer(raw, 5, 'image/png');
    assert.equal(out.base64, raw.toString('base64'));
    const logged = readLog();
    assert.ok(logged.includes('Image processor unavailable or failed'), `log missing warn: ${logged}`);
    assert.ok(logged.includes('[WARN]'));
  });

  it('emits a resize breadcrumb when downscaling to the pixel cap', async () => {
    mock.method(imageHooks, 'loadSharp', (() => fakeSharp({ width: 4000, height: 1000, format: 'jpeg' }, { raw: 50 })) as any);
    const out = await compressImageBuffer(Buffer.alloc(100), 80, 'image/jpeg');
    assert.equal(out.dimensions?.displayWidth, IMAGE_MAX_WIDTH);
    assert.ok(readLog().includes(`Resizing image to ${IMAGE_MAX_WIDTH}x`));
  });

  it('logs the aggressive last-resort JPEG compression step', async () => {
    mock.method(imageHooks, 'loadSharp', (() => fakeSharp({ width: 3000, height: 3000, format: 'png' }, { raw: 1000, png: 1000, jpeg: 1000 })) as any);
    await compressImageBuffer(Buffer.alloc(100), 80, 'image/png');
    const logged = readLog();
    assert.ok(logged.includes('compressing aggressively with JPEG'));
    assert.ok(logged.includes('JPEG-compressed image buffer size:'));
  });

  it('warns when BMP→PNG transcode is unavailable during a file read encode', async () => {
    mock.method(imageHooks, 'loadSharp', (() => { throw new Error('no sharp'); }) as any);
    // "BM" magic → image/bmp; the transcode try/catch fires the warn.
    const bmp = Buffer.from([0x42, 0x4d, ...new Array(30).fill(0)]);
    const p = path.join(tmpDir, 'clip.bmp');
    fs.writeFileSync(p, bmp);
    await encodeImageFile(p).catch(() => { /* size guard may reject; we only assert the warn */ });
    assert.ok(readLog().includes('BMP transcode unavailable'));
  });
});
