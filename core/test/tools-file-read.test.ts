import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileReadTool, isImagePath, parseImageToolOutput, encodeImageFile, isImageExtension, validateImageFile, isBinaryByExtension, checkBinaryContent } from '../src/tools/file-read';
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

// ---------------------------------------------------------------------------
// Symlink target validation
// ---------------------------------------------------------------------------

describe('fileReadTool – symlink target validation', () => {
  it('allows reading a symlink to a regular file in the workspace', async () => {
    const target = path.join(tmpDir, 'real.txt');
    fs.writeFileSync(target, 'real content\n');
    const link = path.join(tmpDir, 'link.txt');
    fs.symlinkSync(target, link);

    const result = await fileReadTool.execute({ file_path: link }, ctx());
    assert.ok(!result.isError, `expected success, got: ${result.output}`);
    assert.ok(result.output.includes('real content'));
  });

  it('blocks a symlink targeting /etc/passwd (credential path)', async () => {
    const link = path.join(tmpDir, 'sneaky-passwd.txt');
    try {
      fs.symlinkSync('/etc/passwd', link);
    } catch { return; /* cannot create symlink — skip */ }
    const result = await fileReadTool.execute({ file_path: link }, ctx());
    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('sensitive file') || result.output.includes('outside the workspace'),
      `expected credential/workspace block, got: ${result.output}`,
    );
  });

  it('blocks a symlink targeting /etc/shadow (credential path)', async () => {
    const link = path.join(tmpDir, 'sneaky-shadow.txt');
    try {
      fs.symlinkSync('/etc/shadow', link);
    } catch { return; /* cannot create symlink — skip */ }
    const result = await fileReadTool.execute({ file_path: link }, ctx());
    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('sensitive file') || result.output.includes('outside the workspace'),
      `expected credential/workspace block, got: ${result.output}`,
    );
  });

  it('blocks a symlink targeting ~/.ssh/id_rsa (credential path)', async () => {
    const sshDir = path.join(tmpDir, '.ssh');
    fs.mkdirSync(sshDir);
    const fakeKey = path.join(sshDir, 'id_rsa');
    fs.writeFileSync(fakeKey, 'fake-key-content');
    // Create the symlink in a subdirectory so the target is technically inside
    // the workspace — this tests the isCredentialPath check specifically
    const subDir = path.join(tmpDir, 'sub');
    fs.mkdirSync(subDir);
    const link = path.join(subDir, 'key-link.txt');
    fs.symlinkSync(fakeKey, link);

    const result = await fileReadTool.execute({ file_path: link }, ctx());
    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('sensitive file'),
      `expected credential block, got: ${result.output}`,
    );
  });

  it('blocks a dangling symlink (target does not exist)', async () => {
    const link = path.join(tmpDir, 'dangling.txt');
    try {
      fs.symlinkSync(path.join(tmpDir, 'no-such-target.txt'), link);
    } catch { return; /* cannot create symlink — skip */ }
    const result = await fileReadTool.execute({ file_path: link }, ctx());
    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('not found') || result.output.includes('not readable'),
      `expected not-found error for dangling symlink, got: ${result.output}`,
    );
  });

  it('blocks a symlink resolving outside the workspace', async () => {
    const link = path.join(tmpDir, 'escape.txt');
    try {
      fs.symlinkSync('/usr/share/common-licenses/GPL-3', link);
    } catch { return; /* cannot create symlink — skip */ }
    const result = await fileReadTool.execute({ file_path: link }, ctx());
    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('outside the workspace') || result.output.includes('resolves outside'),
      `expected workspace escape block, got: ${result.output}`,
    );
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

  it('treats empty pages parameter as omitted (no error)', async () => {
    const p = path.join(tmpDir, 'empty-pages.pdf');
    fs.writeFileSync(p, '%PDF-1.4\n' + 'x'.repeat(100));
    const res = await fileReadTool.execute({ file_path: p, pages: '' }, ctx());
    // Should NOT return "Invalid page range" error
    assert.ok(!res.output.includes('Invalid page range'), `empty pages should be treated as omitted: ${res.output.slice(0, 200)}`);
  });

  it('treats whitespace-only pages parameter as omitted', async () => {
    const p = path.join(tmpDir, 'ws-pages.pdf');
    fs.writeFileSync(p, '%PDF-1.4\n' + 'x'.repeat(100));
    const res = await fileReadTool.execute({ file_path: p, pages: '   ' }, ctx());
    assert.ok(!res.output.includes('Invalid page range'), `whitespace-only pages should be treated as omitted: ${res.output.slice(0, 200)}`);
  });
});

// ---------------------------------------------------------------------------
// PDF text extraction (pdf-parse v2 API)
// ---------------------------------------------------------------------------

function makeMinimalPdf(text = 'Hello World'): Buffer {
  const stream = `BT /F1 12 Tf 100 700 Td (${text}) Tj ET`;
  const streamLen = Buffer.byteLength(stream);
  const lines = [
    '%PDF-1.0',
    '1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj',
    '2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj',
    '3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Contents 4 0 R/Resources<</Font<</F1<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>>>>>>>endobj',
    `4 0 obj<</Length ${streamLen}>>stream`,
    stream,
    'endstream',
    'endobj',
  ];
  const body = lines.join('\n') + '\n';
  const xrefOffset = body.length;
  const xref = ['xref', '0 5', '0000000000 65535 f '];
  for (let i = 1; i <= 4; i++) {
    const pos = body.indexOf(`${i} 0 obj`);
    xref.push(String(pos).padStart(10, '0') + ' 00000 n ');
  }
  const trailer = ['trailer<</Size 5/Root 1 0 R>>', 'startxref', String(xrefOffset), '%%EOF'];
  return Buffer.from(body + xref.join('\n') + '\n' + trailer.join('\n'));
}

describe('Read tool — PDF text extraction (v2 API)', () => {
  let hasPdfParse = false;
  try { require('pdf-parse'); hasPdfParse = true; } catch {}

  it('extracts text from a valid PDF (requires pdf-parse)', { skip: !hasPdfParse }, async () => {
    const p = path.join(tmpDir, 'valid.pdf');
    fs.writeFileSync(p, makeMinimalPdf('SuperInference Test'));
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, false);
    assert.ok(res.output.includes('SuperInference Test'), `Should contain extracted text but got: ${res.output.slice(0, 200)}`);
    assert.ok(res.output.includes('1 pages'), `Should report page count but got: ${res.output.slice(0, 200)}`);
  });

  it('returns error for corrupted PDF', async () => {
    const p = path.join(tmpDir, 'corrupt.pdf');
    fs.writeFileSync(p, '%PDF-1.4\ngarbage data that is not valid PDF structure');
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Error reading PDF'), `Should report PDF error but got: ${res.output.slice(0, 200)}`);
  });

  it('tracks the file in filesRead (requires pdf-parse)', { skip: !hasPdfParse }, async () => {
    const p = path.join(tmpDir, 'tracked.pdf');
    fs.writeFileSync(p, makeMinimalPdf('Tracked'));
    const context = ctx();
    context.filesRead = new Set();
    await fileReadTool.execute({ file_path: p }, context);
    assert.ok(context.filesRead.has(p), 'PDF file should be tracked in filesRead');
  });
});

// ---------------------------------------------------------------------------
// isImageExtension
// ---------------------------------------------------------------------------

describe('isImageExtension', () => {
  it('recognizes common image extensions', () => {
    assert.equal(isImageExtension('photo.png'), true);
    assert.equal(isImageExtension('photo.jpg'), true);
    assert.equal(isImageExtension('photo.jpeg'), true);
    assert.equal(isImageExtension('photo.gif'), true);
    assert.equal(isImageExtension('photo.webp'), true);
    assert.equal(isImageExtension('photo.svg'), true);
  });

  it('is case-insensitive', () => {
    assert.equal(isImageExtension('photo.PNG'), true);
    assert.equal(isImageExtension('photo.JPG'), true);
  });

  it('rejects non-image extensions', () => {
    assert.equal(isImageExtension('file.ts'), false);
    assert.equal(isImageExtension('file.txt'), false);
    assert.equal(isImageExtension('file.json'), false);
  });
});

// ---------------------------------------------------------------------------
// validateImageFile
// ---------------------------------------------------------------------------

describe('validateImageFile', () => {
  it('returns valid for a regular file', () => {
    const p = path.join(tmpDir, 'img.png');
    fs.writeFileSync(p, Buffer.from([0x89, 0x50, 0x4e, 0x47]));
    const result = validateImageFile(p);
    assert.equal(result.valid, true);
  });

  it('returns error for non-existent file', () => {
    const result = validateImageFile(path.join(tmpDir, 'nope.png'));
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('not found') || result.error!.includes('not accessible'));
  });

  it('returns error for directory', () => {
    const result = validateImageFile(tmpDir);
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('Not a regular file'));
  });
});

// ---------------------------------------------------------------------------
// isBinaryByExtension
// ---------------------------------------------------------------------------

describe('isBinaryByExtension', () => {
  it('detects binary extensions', () => {
    assert.equal(isBinaryByExtension('.exe'), true);
    assert.equal(isBinaryByExtension('.zip'), true);
    assert.equal(isBinaryByExtension('.sqlite'), true);
    assert.equal(isBinaryByExtension('.mp4'), true);
    assert.equal(isBinaryByExtension('.wasm'), true);
  });

  it('is case-insensitive', () => {
    assert.equal(isBinaryByExtension('.EXE'), true);
    assert.equal(isBinaryByExtension('.ZIP'), true);
  });

  it('rejects text extensions', () => {
    assert.equal(isBinaryByExtension('.ts'), false);
    assert.equal(isBinaryByExtension('.txt'), false);
    assert.equal(isBinaryByExtension('.json'), false);
    assert.equal(isBinaryByExtension('.md'), false);
  });
});

// ---------------------------------------------------------------------------
// checkBinaryContent
// ---------------------------------------------------------------------------

describe('checkBinaryContent', () => {
  it('returns false for text file', async () => {
    const p = path.join(tmpDir, 'text.txt');
    fs.writeFileSync(p, 'Hello, world!');
    assert.equal(await checkBinaryContent(p), false);
  });

  it('returns true for file with many null bytes', async () => {
    const p = path.join(tmpDir, 'binary.bin');
    const buf = Buffer.alloc(1000);
    buf.fill(0, 0, 100);
    buf.write('some text', 100);
    fs.writeFileSync(p, buf);
    assert.equal(await checkBinaryContent(p), true);
  });

  it('returns false for empty file', async () => {
    const p = path.join(tmpDir, 'empty.txt');
    fs.writeFileSync(p, '');
    assert.equal(await checkBinaryContent(p), false);
  });
});
