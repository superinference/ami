import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  fileReadTool,
  classifyPdfError,
  classifyPdfErrorReason,
  type PdfErrorReason,
  DOCUMENT_EXTENSIONS,
  isPDFExtension,
  unwrapDefault,
} from '../src/tools/file-read';
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fr-u026-w68-'));
  getFileCache(tmpDir).clear();
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// GAP U026#38/#40/#41/#0: structured PDF error reason taxonomy. classifyPdfError
// now switches on a machine-readable PdfErrorReason so each cause yields a
// distinct message. RED before fix: classifyPdfErrorReason did not exist.
// ---------------------------------------------------------------------------
describe('U026-w68: classifyPdfErrorReason taxonomy', () => {
  const cases: Array<[string, PdfErrorReason]> = [
    ['PDFParse: file is password protected', 'password_protected'],
    ['The document is encrypted', 'password_protected'],
    ['file stream is damaged', 'corrupted'],
    ['The PDF appears to be corrupt', 'corrupted'],
    ['PDF file is empty (0 bytes)', 'empty'],
    ['zero-length document', 'empty'],
    ['PDF too large (40 MB) — exceeds the 20 MB limit', 'too_large'],
    ['pdftoppm is not installed', 'unavailable'],
    ['Cannot find module pdf-parse', 'unavailable'],
    ['Invalid xref table', 'unknown'],
    ['some other parser failure', 'unknown'],
  ];
  for (const [msg, reason] of cases) {
    it(`classifies ${JSON.stringify(msg)} as ${reason}`, () => {
      assert.equal(classifyPdfErrorReason(msg), reason);
    });
  }

  it('password classification wins over corrupted when both words appear', () => {
    // password branch is evaluated first (parity with Claude's ordering)
    assert.equal(classifyPdfErrorReason('encrypted and corrupt'), 'password_protected');
  });
});

describe('U026-w68: classifyPdfError messages per reason', () => {
  it('password_protected → unprotected-version guidance', () => {
    assert.ok(classifyPdfError('encrypted').includes('password-protected or encrypted'));
  });
  it('corrupted → corrupted/invalid message', () => {
    assert.ok(classifyPdfError('file is damaged').includes('corrupted or invalid'));
  });
  it('empty → 0-byte message', () => {
    const out = classifyPdfError('PDF file is empty (0 bytes)');
    assert.ok(out.includes('empty (0 bytes)'));
    assert.ok(!out.includes('Error reading PDF'));
  });
  it('too_large → surfaces the original size message', () => {
    const out = classifyPdfError('too large — exceeds the 20 MB limit');
    assert.ok(out.startsWith('Error: '));
    assert.ok(out.includes('20 MB'));
  });
  it('unavailable → support-unavailable message', () => {
    assert.ok(classifyPdfError('pdftoppm is not installed').includes('PDF support is unavailable'));
  });
  it('REGRESSION: a benign "Invalid xref table" stays the generic reader error', () => {
    const out = classifyPdfError('Invalid xref table');
    assert.ok(out.includes('Error reading PDF'));
    assert.ok(out.includes('Invalid xref table'));
  });
});

// ---------------------------------------------------------------------------
// GAP U026#39: 0-byte PDF → distinct "empty" error (not the %PDF- header error).
// RED before fix (isError true but): the message was the %PDF- header complaint;
// now it is the empty-file classification.
// ---------------------------------------------------------------------------
describe('U026-w68: 0-byte PDF empty guard', () => {
  it('reports a 0-byte .pdf as empty, not "missing %PDF- header"', async () => {
    const p = path.join(tmpDir, 'empty.pdf');
    fs.writeFileSync(p, '');
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('empty (0 bytes)'), res.output);
    assert.ok(!res.output.includes('%PDF- header'), 'empty guard must precede the header probe');
  });

  it('a non-empty non-PDF-magic .pdf still reports the header error (discriminator)', async () => {
    const p = path.join(tmpDir, 'fake.pdf');
    fs.writeFileSync(p, 'this is not a pdf at all');
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('%PDF- header'), res.output);
  });
});

// ---------------------------------------------------------------------------
// GAP U026#42: DOCUMENT_EXTENSIONS set + isPDFExtension() normalized membership.
// RED before fix: neither export existed.
// ---------------------------------------------------------------------------
describe('U026-w68: DOCUMENT_EXTENSIONS / isPDFExtension', () => {
  it('DOCUMENT_EXTENSIONS stores the dot-less extension', () => {
    assert.ok(DOCUMENT_EXTENSIONS.has('pdf'));
    assert.ok(!DOCUMENT_EXTENSIONS.has('.pdf'));
  });
  it('isPDFExtension normalizes a leading dot and case', () => {
    assert.ok(isPDFExtension('.pdf'));
    assert.ok(isPDFExtension('pdf'));
    assert.ok(isPDFExtension('.PDF'));
    assert.ok(isPDFExtension('Pdf'));
  });
  it('isPDFExtension rejects non-PDF extensions', () => {
    assert.ok(!isPDFExtension('.txt'));
    assert.ok(!isPDFExtension('png'));
    assert.ok(!isPDFExtension(''));
  });
});

// ---------------------------------------------------------------------------
// GAP U026#23: ESM/CJS interop unwrapping of a dynamic-imported module.
// RED before fix: unwrapDefault did not exist, and imageHooks.loadSharp assumed
// the CJS callable shape.
// ---------------------------------------------------------------------------
describe('U026-w68: unwrapDefault (ESM/CJS interop)', () => {
  it('returns a bare function unchanged (CJS shape)', () => {
    const fn = () => 'ok';
    assert.equal(unwrapDefault(fn), fn);
  });
  it('unwraps a { default } wrapper (ESM shape)', () => {
    const fn = () => 'ok';
    assert.equal(unwrapDefault({ default: fn }), fn);
  });
  it('returns a plain object without a default unchanged', () => {
    const obj = { create: () => 1 };
    assert.equal(unwrapDefault(obj), obj);
  });
});

// ---------------------------------------------------------------------------
// GAP U026#44: stream resource cleanup on mid-read error. The range reader now
// wraps the for-await loop in try/finally so the stream is always torn down.
// Behavior-preserving: a normal range read returns the expected window, and an
// abort while streaming rejects cleanly instead of hanging / leaking.
// ---------------------------------------------------------------------------
describe('U026-w68: readFileInRange cleanup (try/finally)', () => {
  it('a normal range read returns the requested window (finally runs on success)', async () => {
    const p = path.join(tmpDir, 'lines.txt');
    fs.writeFileSync(p, Array.from({ length: 50 }, (_, i) => `line ${i + 1}`).join('\n'));
    const res = await fileReadTool.execute({ file_path: p, offset: 10, limit: 3 }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('line 11'));
    assert.ok(res.output.includes('line 13'));
    assert.ok(!res.output.includes('line 14'));
  });

  it('aborting while streaming rejects without hanging (cleanup path)', async () => {
    const p = path.join(tmpDir, 'big-range.txt');
    fs.writeFileSync(p, Array.from({ length: 5000 }, (_, i) => `row ${i}`).join('\n'));
    const ac = new AbortController();
    const promise = fileReadTool.execute(
      { file_path: p, offset: 0, limit: 4000 },
      ctx({ abortSignal: ac.signal }),
    );
    // Abort after execute's synchronous up-front check but before the stream
    // finishes; the stream's error propagates through the readline iterator and
    // the finally block tears the stream down.
    ac.abort();
    await assert.rejects(promise);
  });
});
