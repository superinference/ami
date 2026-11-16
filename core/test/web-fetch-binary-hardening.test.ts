/**
 * U072 — web_fetch binary/PDF hardening (core-only, additive parity):
 *  1. PDF parser is resolved from OUR install location, never the untrusted cwd.
 *  2. Binary spill extension comes from a fixed-vocabulary whitelist (not the
 *     verbatim, attacker-controlled Content-Type subtype).
 *  3. Binary saved-content note reports a human-readable size (formatSize).
 *  4. A disk/permission failure persisting binary bytes is surfaced as a clear
 *     tool error, not a generic fetch failure.
 *
 * Everything drives the REAL webFetchTool.execute via the `_webFetchHttpGet`
 * seam + `_allowLocalhostForTesting`; no socket is ever opened.
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as nodePath from 'path';
import { webFetchTool, extensionForMimeType } from '../src/tools/web-fetch';
import type { ToolContext } from '../src/types';

type HttpGetResult = {
  body: string;
  bodyBuffer?: Buffer;
  statusCode: number;
  contentType: string;
  finalUrl: string;
  truncated?: boolean;
};

function mockGet(resp: Partial<HttpGetResult>) {
  return async (url: string): Promise<HttpGetResult> => ({
    body: '',
    statusCode: 200,
    contentType: 'text/plain',
    finalUrl: url,
    ...resp,
  });
}

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// Isolate from a globally-set kill switch + config-dir overrides.
let saved: Array<[string, string | undefined]>;
before(() => {
  saved = [
    ['CLAUDE_CODE_DISABLE_WEB_FETCH', process.env.CLAUDE_CODE_DISABLE_WEB_FETCH],
    ['SUPERINFERENCE_DISABLE_WEB_FETCH', process.env.SUPERINFERENCE_DISABLE_WEB_FETCH],
    ['SUPERINFERENCE_DATA_DIR', process.env.SUPERINFERENCE_DATA_DIR],
    ['CLAUDE_CONFIG_DIR', process.env.CLAUDE_CONFIG_DIR],
  ];
  delete process.env.CLAUDE_CODE_DISABLE_WEB_FETCH;
  delete process.env.SUPERINFERENCE_DISABLE_WEB_FETCH;
});
after(() => {
  for (const [k, v] of saved) {
    if (v === undefined) delete process.env[k]; else process.env[k] = v;
  }
});

// ---------------------------------------------------------------------------
// Gap: conservative mime-type -> extension whitelist (extensionForMimeType)
// ---------------------------------------------------------------------------
describe('extensionForMimeType — fixed-vocabulary whitelist', () => {
  it('maps known types to their proper extension', () => {
    assert.equal(extensionForMimeType('application/pdf'), 'pdf');
    assert.equal(extensionForMimeType('image/png'), 'png');
    assert.equal(extensionForMimeType('image/jpeg'), 'jpg');
    assert.equal(extensionForMimeType('application/zip'), 'zip');
  });
  it('ignores charset/boundary params and is case-insensitive', () => {
    assert.equal(extensionForMimeType('IMAGE/PNG; charset=utf-8'), 'png');
    assert.equal(extensionForMimeType('application/json;boundary=x'), 'json');
  });
  it('falls back to bin for unknown / missing / hostile types', () => {
    assert.equal(extensionForMimeType(undefined), 'bin');
    assert.equal(extensionForMimeType(''), 'bin');
    assert.equal(extensionForMimeType('application/octet-stream'), 'bin');
    // A hostile subtype must never leak path separators into the extension.
    assert.equal(extensionForMimeType('image/png/../../etc/passwd'), 'bin');
  });
});

// ---------------------------------------------------------------------------
// Gap: binary spill uses whitelist extension + human-readable size
// ---------------------------------------------------------------------------
describe('web_fetch — binary spill naming & size', () => {
  it('names an octet-stream spill .bin (not the verbatim subtype) and reports a human size', async () => {
    const tmp = fs.mkdtempSync(nodePath.join(os.tmpdir(), 'u072-bin-'));
    process.env.SUPERINFERENCE_DATA_DIR = tmp;
    try {
      const bytes = Buffer.alloc(2048, 0x41); // 2 KiB -> "2.0 KB"
      const r = await webFetchTool.execute(
        { url: 'https://bin.example/a' },
        ctx({
          cwd: tmp,
          _allowLocalhostForTesting: true,
          _webFetchHttpGet: mockGet({ body: bytes.toString('binary'), bodyBuffer: bytes, contentType: 'application/octet-stream' }),
        }),
      );
      assert.equal(r.isError, undefined);
      // Whitelist extension: .bin, NOT ".octet-stream" (old verbatim behavior).
      const m = r.output.match(/saved to (\S+)\]/);
      assert.ok(m, r.output);
      assert.ok(m![1].endsWith('.bin'), `expected .bin extension, got ${m![1]}`);
      assert.ok(!/\.octet-stream/.test(r.output), 'must not use verbatim subtype as extension');
      // Human-readable size, NOT "2048 bytes".
      assert.match(r.output, /\(application\/octet-stream, 2\.0 KB\)/);
      assert.ok(!/\d+ bytes\)/.test(r.output), 'must not report a raw byte count');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('surfaces a clear tool error when binary bytes cannot be persisted to disk', async () => {
    // Point the data dir at a regular FILE so projectStateDir's mkdir fails (ENOTDIR).
    const tmpDir = fs.mkdtempSync(nodePath.join(os.tmpdir(), 'u072-wf-'));
    const blockFile = nodePath.join(tmpDir, 'not-a-dir');
    fs.writeFileSync(blockFile, 'x');
    process.env.SUPERINFERENCE_DATA_DIR = blockFile;
    try {
      const bytes = Buffer.from([0x00, 0xff, 0x10]);
      const r = await webFetchTool.execute(
        { url: 'https://bin.example/b' },
        ctx({
          cwd: tmpDir,
          _allowLocalhostForTesting: true,
          _webFetchHttpGet: mockGet({ body: bytes.toString('binary'), bodyBuffer: bytes, contentType: 'application/octet-stream' }),
        }),
      );
      assert.equal(r.isError, true);
      assert.match(r.output, /could not save binary content/);
      // Not a generic "Error fetching ..." message.
      assert.ok(!/^Error fetching/.test(r.output), r.output);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Gap: PDF parser must NOT be require()'d from the untrusted process.cwd()
// RED on revert (createRequire(path.join(process.cwd(),'_'))): the planted
// node_modules/pdf-parse executes and writes the sentinel. GREEN with the fix
// (createRequire(import.meta.url)): the cwd module is never loaded.
// ---------------------------------------------------------------------------
describe('web_fetch — PDF parser is not loaded from the untrusted cwd', () => {
  it('never executes a pdf-parse planted in the working-directory node_modules', async () => {
    const tmp = fs.mkdtempSync(nodePath.join(os.tmpdir(), 'u072-pdf-cwd-'));
    const sentinel = nodePath.join(tmp, 'PWNED');
    const pkgDir = nodePath.join(tmp, 'node_modules', 'pdf-parse');
    fs.mkdirSync(pkgDir, { recursive: true });
    fs.writeFileSync(nodePath.join(pkgDir, 'package.json'), JSON.stringify({ name: 'pdf-parse', version: '9.9.9', main: 'index.js' }));
    // Malicious module: writes the sentinel the instant it is loaded.
    fs.writeFileSync(
      nodePath.join(pkgDir, 'index.js'),
      `require('fs').writeFileSync(${JSON.stringify(sentinel)}, 'pwned');\nmodule.exports = { PDFParse: class {} };\n`,
    );
    process.env.SUPERINFERENCE_DATA_DIR = tmp;
    const origCwd = process.cwd();
    process.chdir(tmp);
    try {
      await webFetchTool.execute(
        { url: 'https://pdf.example/doc' },
        ctx({
          cwd: tmp,
          _allowLocalhostForTesting: true,
          _webFetchHttpGet: mockGet({ body: '%PDF-1.4 fake pdf body', contentType: 'application/pdf' }),
        }),
      );
      assert.equal(fs.existsSync(sentinel), false, 'cwd-resident pdf-parse must NOT be executed');
    } finally {
      process.chdir(origCwd);
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});
