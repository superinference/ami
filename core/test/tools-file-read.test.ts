import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileReadTool, isImagePath, parseImageToolOutput, encodeImageFile } from '../src/tools/file-read';
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-file-read-'));
  // Clear file cache for the temp dir so each test starts fresh
  getFileCache(tmpDir).clear();
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('fileReadTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(fileReadTool.name, 'file_read');
  });

  it('is read-only and concurrency-safe', () => {
    assert.equal(fileReadTool.isReadOnly, true);
    assert.equal(fileReadTool.isConcurrencySafe, true);
  });

  it('schema requires file_path', () => {
    assert.ok(fileReadTool.inputSchema.required?.includes('file_path'));
  });
});

// ---------------------------------------------------------------------------
// Basic file reading
// ---------------------------------------------------------------------------

describe('fileReadTool – basic reading', () => {
  it('reads a simple text file with line numbers', async () => {
    const file = path.join(tmpDir, 'hello.txt');
    fs.writeFileSync(file, 'hello\nworld\n');

    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('hello'));
    assert.ok(result.output.includes('world'));
    assert.ok(result.output.includes('File:'));
    assert.ok(result.output.includes('3 lines')); // 'hello\nworld\n' splits to 3 lines
  });

  it('returns error for empty file_path', async () => {
    const result = await fileReadTool.execute({ file_path: '' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must not be empty'));
  });

  it('returns error for non-existent file', async () => {
    const result = await fileReadTool.execute({ file_path: path.join(tmpDir, 'no-such-file.txt') }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found') || result.output.includes('not readable'));
  });

  it('returns error when path is a directory', async () => {
    const result = await fileReadTool.execute({ file_path: tmpDir }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('directory'));
  });

  it('returns (empty file) for a zero-length file', async () => {
    const file = path.join(tmpDir, 'empty.txt');
    fs.writeFileSync(file, '');

    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(result.output.includes('empty file'));
  });
});

// ---------------------------------------------------------------------------
// Relative paths
// ---------------------------------------------------------------------------

describe('fileReadTool – relative paths', () => {
  it('resolves relative path against cwd', async () => {
    const file = path.join(tmpDir, 'rel.txt');
    fs.writeFileSync(file, 'relative content\n');

    const result = await fileReadTool.execute({ file_path: 'rel.txt' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('relative content'));
  });
});

// ---------------------------------------------------------------------------
// Offset and limit
// ---------------------------------------------------------------------------

describe('fileReadTool – offset and limit', () => {
  it('respects offset (0-based line number)', async () => {
    const file = path.join(tmpDir, 'lines.txt');
    fs.writeFileSync(file, 'line1\nline2\nline3\nline4\nline5\n');

    const result = await fileReadTool.execute(
      { file_path: file, offset: 2, limit: 2 },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('line3'));
    assert.ok(result.output.includes('line4'));
    assert.ok(result.output.includes('Showing'));
    // Should NOT include line1 or line2
    const lines = result.output.split('\n');
    const contentLines = lines.filter(l => /\t/.test(l));
    assert.equal(contentLines.length, 2);
  });

  it('defaults to offset=0 and limit=2000', async () => {
    const file = path.join(tmpDir, 'small.txt');
    fs.writeFileSync(file, 'one\ntwo\nthree\n');

    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('one'));
    assert.ok(result.output.includes('two'));
    assert.ok(result.output.includes('three'));
  });

  it('handles offset beyond file length', async () => {
    const file = path.join(tmpDir, 'short.txt');
    fs.writeFileSync(file, 'only line\n');

    const result = await fileReadTool.execute(
      { file_path: file, offset: 100 },
      ctx(),
    );
    assert.ok(!result.isError);
    // No content lines should be shown
  });
});

// ---------------------------------------------------------------------------
// Binary detection
// ---------------------------------------------------------------------------

describe('fileReadTool – binary detection', () => {
  it('returns [Binary file] for files with null bytes', async () => {
    const file = path.join(tmpDir, 'binary.bin');
    const buf = Buffer.alloc(100);
    buf[50] = 0; // null byte
    buf.write('text', 0);
    fs.writeFileSync(file, buf);

    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(result.output.includes('Binary file'));
  });
});

// ---------------------------------------------------------------------------
// Image files
// ---------------------------------------------------------------------------

describe('fileReadTool – image files', () => {
  for (const ext of ['.png', '.jpg', '.jpeg', '.gif', '.webp']) {
    it(`returns base64 for ${ext} files`, async () => {
      const file = path.join(tmpDir, `image${ext}`);
      fs.writeFileSync(file, Buffer.from('fake image data'));

      const result = await fileReadTool.execute({ file_path: file }, ctx());
      assert.ok(!result.isError);
      const parsed = JSON.parse(result.output);
      assert.equal(parsed.type, 'image');
      assert.equal(parsed.source.type, 'base64');
      assert.ok(parsed.source.data.length > 0);
    });
  }
});

// ---------------------------------------------------------------------------
// File cache
// ---------------------------------------------------------------------------

describe('fileReadTool – file cache', () => {
  it('returns short summary on second read of unchanged file', async () => {
    const file = path.join(tmpDir, 'cached.txt');
    fs.writeFileSync(file, 'content\n');

    // First read — full content
    const r1 = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(r1.output.includes('content'));

    // Second read — should show cached summary
    const r2 = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(r2.output.includes('unchanged'));
  });
});

// ---------------------------------------------------------------------------
// Line number formatting
// ---------------------------------------------------------------------------

describe('fileReadTool – line number formatting', () => {
  it('uses 1-based line numbers', async () => {
    const file = path.join(tmpDir, 'nums.txt');
    fs.writeFileSync(file, 'first\nsecond\nthird\n');

    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('1\tfirst'));
    assert.ok(result.output.includes('2\tsecond'));
    assert.ok(result.output.includes('3\tthird'));
  });
});

// ---------------------------------------------------------------------------
// Read-before-write tracking (filesRead)
// ---------------------------------------------------------------------------

describe('fileReadTool – filesRead tracking', () => {
  it('adds resolved path to filesRead set after successful read', async () => {
    const file = path.join(tmpDir, 'tracked.txt');
    fs.writeFileSync(file, 'content\n');
    const filesRead = new Set<string>();

    await fileReadTool.execute({ file_path: file }, ctx({ filesRead }));
    assert.ok(filesRead.has(file));
  });

  it('adds path for image files', async () => {
    const file = path.join(tmpDir, 'img.png');
    fs.writeFileSync(file, Buffer.from('fake'));
    const filesRead = new Set<string>();

    await fileReadTool.execute({ file_path: file }, ctx({ filesRead }));
    assert.ok(filesRead.has(file));
  });

  it('does not track binary files (tracking moved to after successful processing)', async () => {
    const file = path.join(tmpDir, 'bin.dat');
    const buf = Buffer.alloc(100);
    buf[50] = 0;
    fs.writeFileSync(file, buf);
    const filesRead = new Set<string>();

    await fileReadTool.execute({ file_path: file }, ctx({ filesRead }));
    assert.ok(!filesRead.has(file));
  });

  it('does not crash when filesRead is undefined', async () => {
    const file = path.join(tmpDir, 'notrack.txt');
    fs.writeFileSync(file, 'data\n');

    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(!result.isError);
  });

  it('does not add path for non-existent files', async () => {
    const filesRead = new Set<string>();
    await fileReadTool.execute(
      { file_path: path.join(tmpDir, 'ghost.txt') },
      ctx({ filesRead }),
    );
    assert.equal(filesRead.size, 0);
  });

  it('resolves relative paths before adding to filesRead', async () => {
    const file = path.join(tmpDir, 'rel-track.txt');
    fs.writeFileSync(file, 'data\n');
    const filesRead = new Set<string>();

    await fileReadTool.execute({ file_path: 'rel-track.txt' }, ctx({ filesRead }));
    assert.ok(filesRead.has(file));
  });
});

describe('fileReadTool – suite logs', () => {
  it('returns the failing test from a go test json file and does not page the passes', async () => {
    const file = path.join(tmpDir, 'go-test-results.json');
    const lines: string[] = [];
    for (let i = 0; i < 4000; i++) {
      lines.push(`{"Action":"pass","Test":"TestOk${i}","Output":"${'p'.repeat(80)}"}`);
    }
    lines.splice(
      2000,
      0,
      '{"Action":"output","Test":"TestBLPModel","Output":"panic: runtime error: invalid memory address or nil pointer dereference"}',
      '{"Action":"fail","Test":"TestBLPModel"}',
    );
    fs.writeFileSync(file, lines.join('\n'));
    assert.ok(fs.statSync(file).size > 256 * 1024);

    const full = await fileReadTool.execute({ file_path: file }, ctx());
    assert.equal(full.isError, undefined);
    assert.equal(full.output.includes('TestOk0'), false);
    assert.equal(full.output.includes('"Action":"pass"'), false);
    assert.match(full.output, /TestBLPModel/);
    assert.match(full.output, /does not enter the prompt/);
    assert.ok(full.output.length < 20_000, `output length ${full.output.length}`);

    const paged = await fileReadTool.execute({ file_path: file, offset: 0, limit: 50 }, ctx());
    assert.equal(paged.output.includes('TestOk0'), false);
    assert.match(paged.output, /TestBLPModel/);
  });
});

describe('image attachment helpers', () => {
  it('isImagePath recognizes supported image extensions (case-insensitive) and rejects others', () => {
    for (const p of ['/a/b.png', 'shot.JPG', './x.jpeg', 'y.gif', 'z.webp']) {
      assert.equal(isImagePath(p), true, p);
    }
    for (const p of ['/a/b.txt', 'movie.mp4', 'doc.pdf', 'noext', 'a.svg']) {
      assert.equal(isImagePath(p), false, p);
    }
  });

  it('parseImageToolOutput returns ImageContent[] for file_read image JSON and null otherwise', () => {
    const imageJson = JSON.stringify({
      type: 'image',
      source: { type: 'base64', media_type: 'image/png', data: 'QUJD' },
    });
    const parsed = parseImageToolOutput(imageJson);
    assert.ok(parsed, 'should parse image JSON');
    assert.equal(parsed!.length, 1);
    assert.equal(parsed![0].type, 'image');
    assert.equal(parsed![0].source.media_type, 'image/png');
    assert.equal(parsed![0].source.data, 'QUJD');

    // Non-image outputs must not be treated as images.
    assert.equal(parseImageToolOutput('just some text output'), null);
    assert.equal(parseImageToolOutput('{"type":"text","value":"hi"}'), null);
    assert.equal(parseImageToolOutput('{"type":"image"'), null); // malformed JSON
    assert.equal(parseImageToolOutput(''), null);
  });

  it('encodeImageFile reads a small image file and returns base64 + media type', async () => {
    const p = path.join(tmpDir, 'pixel.png');
    const bytes = Buffer.from('fake-png-bytes');
    fs.writeFileSync(p, bytes);
    const { media_type, data } = await encodeImageFile(p);
    assert.equal(media_type, 'image/png');
    assert.equal(data, bytes.toString('base64'));
  });
});

describe('Read tool — size/regular-file guards + allow_large + large PDF', () => {
  it('refuses an over-128MB image instead of loading it into memory', async () => {
    const p = path.join(tmpDir, 'huge.png');
    const fd = fs.openSync(p, 'w');
    fs.ftruncateSync(fd, 130 * 1024 * 1024); // 130MB sparse
    fs.closeSync(fd);
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /128 MiB/);
  });

  it('refuses a non-regular file (unix socket) named like an image — fails fast, no hang', async () => {
    // A unix domain socket with an image extension is a clean discriminator:
    // it is not a directory (so the directory guard does not intercept it) and
    // readFile on a socket errors fast — unlike a FIFO which would block. With
    // the !isFile() guard the specific "not a regular file" message is produced.
    const net = require('net');
    const p = path.join(tmpDir, 'sock.png');
    const server = net.createServer();
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(p, () => resolve());
    });
    try {
      const res = await fileReadTool.execute({ file_path: p }, ctx());
      assert.equal(res.isError, true);
      assert.match(res.output, /not a regular file|pipe|device/i);
    } finally {
      await new Promise<void>(resolve => server.close(() => resolve()));
    }
  });

  it('refuses a non-regular file (FIFO) named like an image', async () => {
    const p = path.join(tmpDir, 'pipe.png');
    try {
      require('child_process').execSync(`mkfifo ${JSON.stringify(p)}`);
    } catch { return; /* mkfifo unavailable — skip */ }
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /not a regular file|pipe|device/i);
  });

  it('advertises allow_large in the input schema so the model can pass it', () => {
    const prop = (fileReadTool.inputSchema.properties as Record<string, unknown>).allow_large as { type?: string } | undefined;
    assert.ok(prop, 'allow_large must be declared in inputSchema.properties');
    assert.equal(prop.type, 'boolean');
  });

  it('allow_large reads a text file past the 256KB cap', async () => {
    const p = path.join(tmpDir, 'big.txt');
    fs.writeFileSync(p, 'HEADER_SENTINEL\n' + 'x'.repeat(300 * 1024));
    const refused = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(refused.isError, true);
    assert.match(refused.output, /exceeds 256KB limit/);
    const allowed = await fileReadTool.execute({ file_path: p, allow_large: true }, ctx());
    assert.ok(!allowed.isError, `allow_large should read, got ${JSON.stringify(allowed).slice(0, 200)}`);
    assert.match(allowed.output, /HEADER_SENTINEL/);
  });

  it('a large PDF with no pages is not refused with the misleading size hint', async () => {
    const p = path.join(tmpDir, 'big.pdf');
    fs.writeFileSync(p, '%PDF-1.4\n' + 'x'.repeat(300 * 1024));
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    // Whatever the PDF handler returns, it must NOT be the generic size-limit hint.
    assert.ok(!res.output.includes('exceeds 256KB limit'), `PDF should not hit the size guard: ${res.output.slice(0, 160)}`);
  });
});
