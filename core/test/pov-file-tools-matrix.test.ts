import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  safeReadFile,
  safeLstat,
  FILE_ENCODINGS,
  detectEncoding,
  decodeBuffer,
  encodeText,
  type FileEncoding,
} from '../src/tools/tool-utils';
import { fuzzyFindAndReplace } from '../src/tools/fuzzy-match';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

// POV: core file tools — tool-utils (safeReadFile / safeLstat / encodings) and
// file-edit apply behavior through the real fuzzy strategies. Every expected
// value is computed by calling the real code or reading the real file back
// from disk; no source-text .includes assertions.

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

/** Write a file with the given raw bytes and return its absolute path. */
function fixture(name: string, data: Buffer | string): string {
  const p = path.join(tmpDir, name);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, data);
  return p;
}

/** Read the on-disk bytes back as decoded text (ground truth for assertions). */
function readBack(p: string): { text: string; encoding: FileEncoding } {
  return decodeBuffer(fs.readFileSync(p));
}

beforeEach(() => {
  tmpDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-file-tools-')));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// FILE_ENCODINGS constant + encode/decode round-trips
// ---------------------------------------------------------------------------

describe('FILE_ENCODINGS + encode/decode round-trips', () => {
  it('lists exactly the four supported encodings in order', () => {
    assert.deepEqual([...FILE_ENCODINGS], ['utf-8', 'utf-8-bom', 'utf-16le', 'utf-16be']);
  });

  it('round-trips every encoding losslessly through encodeText -> decodeBuffer', () => {
    const sample = 'héllo — wörld\nsecond line\té';
    for (const enc of FILE_ENCODINGS) {
      const buf = encodeText(sample, enc);
      const decoded = decodeBuffer(buf);
      assert.equal(decoded.text, sample, `text mismatch for ${enc}`);
      assert.equal(decoded.encoding, enc, `encoding mismatch for ${enc}`);
    }
  });

  it('detectEncoding recognizes each BOM and defaults to utf-8', () => {
    assert.equal(detectEncoding(Buffer.from('plain ascii')), 'utf-8');
    assert.equal(detectEncoding(encodeText('x', 'utf-8-bom')), 'utf-8-bom');
    assert.equal(detectEncoding(encodeText('x', 'utf-16le')), 'utf-16le');
    assert.equal(detectEncoding(encodeText('x', 'utf-16be')), 'utf-16be');
  });

  it('detectEncoding handles short/empty buffers without a BOM', () => {
    assert.equal(detectEncoding(Buffer.alloc(0)), 'utf-8');
    assert.equal(detectEncoding(Buffer.from([0xEF])), 'utf-8');
    assert.equal(detectEncoding(Buffer.from([0xFF])), 'utf-8');
  });
});

// ---------------------------------------------------------------------------
// safeReadFile — encodings, missing file, empty file, containment
// ---------------------------------------------------------------------------

describe('safeReadFile', () => {
  it('reads a plain UTF-8 file with utf-8 encoding', () => {
    const p = fixture('plain.txt', 'alpha\nbeta\n');
    const r = safeReadFile(p, tmpDir);
    assert.equal(r.error, undefined);
    assert.equal(r.content, 'alpha\nbeta\n');
    assert.equal(r.encoding, 'utf-8');
  });

  it('strips the BOM from a UTF-8-BOM file and reports utf-8-bom', () => {
    const p = fixture('bom.txt', encodeText('bom content', 'utf-8-bom'));
    const r = safeReadFile(p, tmpDir);
    assert.equal(r.content, 'bom content');
    assert.equal(r.encoding, 'utf-8-bom');
  });

  it('decodes a UTF-16LE file', () => {
    const text = 'utf16 ☃ sample';
    const p = fixture('le.txt', encodeText(text, 'utf-16le'));
    const r = safeReadFile(p, tmpDir);
    assert.equal(r.content, text);
    assert.equal(r.encoding, 'utf-16le');
  });

  it('decodes a UTF-16BE file (byte-swapped)', () => {
    const text = 'big endian ✓';
    const p = fixture('be.txt', encodeText(text, 'utf-16be'));
    const r = safeReadFile(p, tmpDir);
    assert.equal(r.content, text);
    assert.equal(r.encoding, 'utf-16be');
  });

  it('reads an empty file as empty utf-8 content', () => {
    const p = fixture('empty.txt', '');
    const r = safeReadFile(p, tmpDir);
    assert.equal(r.content, '');
    assert.equal(r.encoding, 'utf-8');
  });

  it('errors on a missing file', () => {
    const r = safeReadFile(path.join(tmpDir, 'does-not-exist.txt'), tmpDir);
    assert.equal(r.content, undefined);
    assert.ok(r.error?.isError);
    assert.ok(r.error!.output.includes('cannot read'));
  });

  it('errors on a path outside the workspace', () => {
    const outside = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-outside-')));
    try {
      fs.writeFileSync(path.join(outside, 'secret.txt'), 'secret');
      const r = safeReadFile(path.join(outside, 'secret.txt'), tmpDir);
      assert.ok(r.error?.isError);
      assert.ok(r.error!.output.includes('outside the workspace'));
    } finally {
      fs.rmSync(outside, { recursive: true, force: true });
    }
  });

  it('allows reading from a permission-granted allowedRoot outside cwd', () => {
    const granted = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-granted-')));
    try {
      const gp = path.join(granted, 'resource.txt');
      fs.writeFileSync(gp, 'granted body');
      const r = safeReadFile(gp, tmpDir, [granted]);
      assert.equal(r.error, undefined);
      assert.equal(r.content, 'granted body');
    } finally {
      fs.rmSync(granted, { recursive: true, force: true });
    }
  });

  it('blocks a symlink pointing outside the workspace', () => {
    const outside = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-symtarget-')));
    try {
      const target = path.join(outside, 'target.txt');
      fs.writeFileSync(target, 'escaped');
      const link = path.join(tmpDir, 'link.txt');
      fs.symlinkSync(target, link);
      const r = safeReadFile(link, tmpDir);
      assert.ok(r.error?.isError);
      assert.ok(r.error!.output.includes('symlink'));
    } finally {
      fs.rmSync(outside, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// safeLstat — regular file, directory, symlink (no follow), missing, outside
// ---------------------------------------------------------------------------

describe('safeLstat', () => {
  it('stats a regular file', () => {
    const p = fixture('reg.txt', 'data');
    const r = safeLstat(p, tmpDir);
    assert.equal(r.error, undefined);
    assert.ok(r.stat!.isFile());
    assert.equal(r.stat!.size, 4);
  });

  it('stats a directory', () => {
    const d = path.join(tmpDir, 'sub');
    fs.mkdirSync(d);
    const r = safeLstat(d, tmpDir);
    assert.ok(r.stat!.isDirectory());
  });

  it('does NOT follow a symlink (reports it as a symlink)', () => {
    const real = fixture('real.txt', 'x');
    const link = path.join(tmpDir, 'sl.txt');
    fs.symlinkSync(real, link);
    const r = safeLstat(link, tmpDir);
    assert.ok(r.stat!.isSymbolicLink());
    assert.ok(!r.stat!.isFile());
  });

  it('errors on a missing path', () => {
    const r = safeLstat(path.join(tmpDir, 'nope'), tmpDir);
    assert.ok(r.error?.isError);
    assert.ok(r.error!.output.includes('cannot stat'));
  });

  it('errors on a path outside the workspace', () => {
    const r = safeLstat('/etc/hostname', tmpDir);
    assert.ok(r.error?.isError);
    assert.ok(r.error!.output.includes('outside the workspace'));
  });

  it('resolves a relative path against cwd', () => {
    fixture('rel.txt', 'hi');
    const r = safeLstat('rel.txt', tmpDir);
    assert.equal(r.error, undefined);
    assert.ok(r.stat!.isFile());
  });
});

// ---------------------------------------------------------------------------
// file-edit apply — exact / fuzzy strategies via fileEditTool.execute,
// asserting the REAL applied bytes on disk (expected computed from real code).
// ---------------------------------------------------------------------------

describe('fileEditTool.execute – exact unique replace', () => {
  it('applies an exact unique match and writes the expected content', async () => {
    const before = 'line one\nTARGET value\nline three\n';
    const p = fixture('exact.txt', before);
    const expected = fuzzyFindAndReplace(before, 'TARGET value', 'REPLACED value');
    assert.equal(expected.strategy, 'exact');

    const r = await fileEditTool.execute(
      { file_path: p, old_string: 'TARGET value', new_string: 'REPLACED value' },
      ctx(),
    );
    assert.ok(!r.isError, r.output);
    assert.equal(readBack(p).text, expected.newContent);
    assert.equal(readBack(p).text, 'line one\nREPLACED value\nline three\n');
  });
});

describe('fileEditTool.execute – whitespace/indentation fuzzy strategy', () => {
  it('matches a multi-line block whose indentation differs (non-exact strategy)', async () => {
    // File indents the body with 6 spaces; old_string uses 4 — so the exact
    // byte sequence never appears and a line-based fuzzy strategy must win.
    const before = 'if (x) {\n      doThing();\n}\n';
    const oldStr = 'if (x) {\n    doThing();';
    const newStr = 'if (x) {\n    doOther();';
    const p = fixture('indent.ts', before);

    // Ground truth: the real fuzzy engine on the normalized (LF) content.
    const expected = fuzzyFindAndReplace(before, oldStr, newStr);
    assert.notEqual(expected.strategy, 'exact');
    assert.notEqual(expected.strategy, null);
    assert.ok(expected.newContent);

    const r = await fileEditTool.execute(
      { file_path: p, old_string: oldStr, new_string: newStr },
      ctx(),
    );
    assert.ok(!r.isError, r.output);
    assert.equal(readBack(p).text, expected.newContent);
    // Strategy note surfaces a non-exact strategy name.
    assert.ok(/matched via \w+ strategy/.test(r.output), r.output);
  });
});

describe('fileEditTool.execute – ambiguous match', () => {
  it('refuses when old_string matches multiple locations', async () => {
    const before = 'dup\nkeep\ndup\n';
    const p = fixture('ambig.txt', before);
    const r = await fileEditTool.execute(
      { file_path: p, old_string: 'dup', new_string: 'changed' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.ok(/matches \d+ locations/.test(r.output));
    // File is left unchanged.
    assert.equal(readBack(p).text, before);
  });

  it('replace_all rewrites every occurrence', async () => {
    const before = 'dup\nkeep\ndup\n';
    const p = fixture('all.txt', before);
    const r = await fileEditTool.execute(
      { file_path: p, old_string: 'dup', new_string: 'X', replace_all: true },
      ctx(),
    );
    assert.ok(!r.isError, r.output);
    assert.equal(readBack(p).text, 'X\nkeep\nX\n');
    assert.ok(r.output.includes('Successfully replaced 2 occurrences'));
  });
});

describe('fileEditTool.execute – not found', () => {
  it('errors with a hint when old_string is absent and leaves the file intact', async () => {
    const before = 'alpha\nbeta\ngamma\n';
    const p = fixture('nomatch.txt', before);
    const r = await fileEditTool.execute(
      { file_path: p, old_string: 'nonexistent string here', new_string: 'x' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('not found'));
    assert.equal(readBack(p).text, before);
  });
});

describe('fileEditTool.execute – edge cases', () => {
  it('empty old_string activates write/create mode', async () => {
    const p = path.join(tmpDir, 'created.txt');
    const r = await fileEditTool.execute(
      { file_path: p, old_string: '', new_string: 'brand new body\n' },
      ctx(),
    );
    assert.ok(!r.isError, r.output);
    assert.equal(readBack(p).text, 'brand new body\n');
  });

  it('preserves CRLF line endings after an edit', async () => {
    const before = 'first\r\nedit-me\r\nlast\r\n';
    const p = fixture('crlf.txt', before);
    const r = await fileEditTool.execute(
      { file_path: p, old_string: 'edit-me', new_string: 'edited' },
      ctx(),
    );
    assert.ok(!r.isError, r.output);
    const raw = fs.readFileSync(p, 'latin1');
    assert.ok(raw.includes('\r\n'), 'CRLF endings should be preserved');
    assert.equal(readBack(p).text, 'first\r\nedited\r\nlast\r\n');
  });

  it('edits unicode content correctly', async () => {
    const before = 'emoji 🚀 here\nsnowman ☃ there\n';
    const p = fixture('unicode.txt', before);
    const r = await fileEditTool.execute(
      { file_path: p, old_string: 'snowman ☃ there', new_string: 'snowman ☃ replaced ✨' },
      ctx(),
    );
    assert.ok(!r.isError, r.output);
    assert.equal(readBack(p).text, 'emoji 🚀 here\nsnowman ☃ replaced ✨\n');
  });

  it('rejects identical old_string and new_string', async () => {
    const before = 'same\n';
    const p = fixture('same.txt', before);
    const r = await fileEditTool.execute(
      { file_path: p, old_string: 'same', new_string: 'same' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('identical'));
    assert.equal(readBack(p).text, before);
  });

  it('preserves BOM encoding across an edit', async () => {
    const before = 'bomline one\nbomline two\n';
    const p = fixture('bom-edit.txt', encodeText(before, 'utf-8-bom'));
    const r = await fileEditTool.execute(
      { file_path: p, old_string: 'bomline two', new_string: 'bomline TWO' },
      ctx(),
    );
    assert.ok(!r.isError, r.output);
    const back = readBack(p);
    assert.equal(back.encoding, 'utf-8-bom');
    assert.equal(back.text, 'bomline one\nbomline TWO\n');
  });

  it('applies sequential edits in order', async () => {
    const before = 'one\ntwo\nthree\n';
    const p = fixture('seq.txt', before);
    const r = await fileEditTool.execute(
      {
        file_path: p,
        edits: [
          { old_string: 'one', new_string: '1' },
          { old_string: 'three', new_string: '3' },
        ],
      },
      ctx(),
    );
    assert.ok(!r.isError, r.output);
    assert.equal(readBack(p).text, '1\ntwo\n3\n');
    assert.ok(r.output.includes('2/2 edits applied'));
  });

  it('errors for sequential edits that all fail, leaving file intact', async () => {
    const before = 'content\n';
    const p = fixture('seqfail.txt', before);
    const r = await fileEditTool.execute(
      {
        file_path: p,
        edits: [{ old_string: 'missing-a', new_string: 'x' }],
      },
      ctx(),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('No edits applied'));
    assert.equal(readBack(p).text, before);
  });
});
