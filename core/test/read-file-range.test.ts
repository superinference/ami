import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  readFileInRange,
  FileTooLargeError,
  FAST_PATH_MAX_SIZE,
  readFileRange,
  tailFile,
} from '../src/tools/read-file-range';

let tmpDir: string;

before(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-rfr-'));
});
after(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function write(name: string, content: string | Buffer): string {
  const p = path.join(tmpDir, name);
  fs.writeFileSync(p, content);
  return p;
}

// ---------------------------------------------------------------------------
// Fast path (small files) — selection, accurate totals, BOM, CRLF
// ---------------------------------------------------------------------------
describe('readFileInRange — fast path', () => {
  it('selects a middle range and reports accurate totalLines (no trailing phantom line)', async () => {
    const p = write('lines.txt', 'a\nb\nc\nd\ne\n');
    const r = await readFileInRange(p, 1, 2);
    assert.equal(r.content, 'b\nc');
    assert.equal(r.lineCount, 2);
    // file ends with a newline → 5 real lines, NOT 6 (no phantom trailing line)
    assert.equal(r.totalLines, 5);
    assert.equal(r.totalBytes, Buffer.byteLength('a\nb\nc\nd\ne\n'));
    assert.equal(r.readBytes, Buffer.byteLength('b\nc'));
    assert.equal(r.truncatedByBytes, undefined);
  });

  it('counts a final unterminated line (no trailing newline)', async () => {
    const p = write('no-nl.txt', 'a\nb\nc');
    const r = await readFileInRange(p);
    assert.equal(r.content, 'a\nb\nc');
    assert.equal(r.totalLines, 3);
    assert.equal(r.lineCount, 3);
  });

  it('counts an out-of-range final unterminated line (accurate totalLines)', async () => {
    const p = write('no-nl2.txt', 'a\nb\nc');
    const r = await readFileInRange(p, 0, 1); // only want line 0
    assert.equal(r.content, 'a');
    assert.equal(r.lineCount, 1);
    assert.equal(r.totalLines, 3); // b and c still counted though discarded
  });

  it('strips a leading UTF-8 BOM', async () => {
    const p = write('bom.txt', '﻿hello\nworld\n');
    const r = await readFileInRange(p, 0, 1);
    assert.equal(r.content, 'hello');
  });

  it('normalizes CRLF to LF', async () => {
    const p = write('crlf.txt', 'x\r\ny\r\n');
    const r = await readFileInRange(p);
    assert.equal(r.content, 'x\ny');
    assert.equal(r.totalLines, 2);
  });

  it('returns nothing for an offset past the end but still reports totalLines', async () => {
    const p = write('short.txt', 'one\ntwo\n');
    const r = await readFileInRange(p, 50, 10);
    assert.equal(r.content, '');
    assert.equal(r.lineCount, 0);
    assert.equal(r.totalLines, 2);
  });

  it('treats an empty file as zero lines', async () => {
    const p = write('empty.txt', '');
    const r = await readFileInRange(p);
    assert.equal(r.content, '');
    assert.equal(r.lineCount, 0);
    assert.equal(r.totalLines, 0);
    assert.equal(r.totalBytes, 0);
  });

  it('renders a single blank line (file is just a newline) as one empty line', async () => {
    const p = write('blank.txt', '\n');
    const r = await readFileInRange(p);
    assert.equal(r.content, '');
    assert.equal(r.lineCount, 1);
    assert.equal(r.totalLines, 1);
  });
});

// ---------------------------------------------------------------------------
// Fast path — maxBytes throw mode + truncate mode
// ---------------------------------------------------------------------------
describe('readFileInRange — byte budget (fast path)', () => {
  it('throws FileTooLargeError when file size exceeds maxBytes (default throw mode)', async () => {
    const p = write('big-ish.txt', 'x'.repeat(5000));
    await assert.rejects(
      () => readFileInRange(p, 0, undefined, 100),
      (err: unknown) => {
        assert.ok(err instanceof FileTooLargeError);
        assert.equal((err as FileTooLargeError).maxSizeBytes, 100);
        assert.ok((err as Error).message.includes('exceeds the maximum allowed size'));
        return true;
      },
    );
  });

  it('does not throw in throw mode when file size is within maxBytes', async () => {
    const p = write('within.txt', 'a\nb\n');
    const r = await readFileInRange(p, 0, undefined, 1000);
    assert.equal(r.content, 'a\nb');
  });

  it('clips selected output at the last complete line that fits (truncate mode)', async () => {
    const p = write('trunc.txt', 'aaaa\nbbbb\ncccc\n');
    // budget fits "aaaa" (4) + sep(1) + "bbbb" (4) = 9; adding cccc would exceed.
    const r = await readFileInRange(p, 0, undefined, 9, undefined, { truncateOnByteLimit: true });
    assert.equal(r.content, 'aaaa\nbbbb');
    assert.equal(r.truncatedByBytes, true);
    assert.equal(r.lineCount, 2);
    // totalLines still accurate
    assert.equal(r.totalLines, 3);
  });

  it('does not set truncatedByBytes when the whole selection fits the byte budget', async () => {
    const p = write('fits.txt', 'aa\nbb\n');
    const r = await readFileInRange(p, 0, undefined, 1000, undefined, { truncateOnByteLimit: true });
    assert.equal(r.content, 'aa\nbb');
    assert.equal(r.truncatedByBytes, undefined);
  });
});

// ---------------------------------------------------------------------------
// Directory + abort
// ---------------------------------------------------------------------------
describe('readFileInRange — guards', () => {
  it('throws EISDIR for a directory', async () => {
    await assert.rejects(() => readFileInRange(tmpDir), /EISDIR/);
  });

  it('rejects immediately when the signal is already aborted', async () => {
    const p = write('abort.txt', 'a\nb\n');
    const ac = new AbortController();
    ac.abort();
    await assert.rejects(() => readFileInRange(p, 0, 1, undefined, ac.signal));
  });
});

// ---------------------------------------------------------------------------
// Streaming path (files >= 10 MB) — bounded memory, accurate totals, byte budget
// ---------------------------------------------------------------------------
describe('readFileInRange — streaming path (large files)', () => {
  let bigMulti: string;
  let bigSingle: string;
  let bigLines: number;

  before(() => {
    // ~11 MB multi-line file: forces the streaming path (> FAST_PATH_MAX_SIZE).
    const unit = 'abcdefghij\n'; // 11 bytes
    const repeats = Math.ceil((FAST_PATH_MAX_SIZE + 1_000_000) / unit.length);
    bigLines = repeats; // each unit contributes one terminated line
    bigMulti = write('big-multi.txt', unit.repeat(repeats));
    // ~11 MB single-line file (no newlines): bounded-memory discard stress.
    bigSingle = write('big-single.txt', 'Z'.repeat(FAST_PATH_MAX_SIZE + 1_000_000));
  });

  it('reads a middle window from a large file via the streaming path', async () => {
    const r = await readFileInRange(bigMulti, 100, 3);
    assert.equal(r.content, 'abcdefghij\nabcdefghij\nabcdefghij');
    assert.equal(r.lineCount, 3);
    // accurate totalLines: scanned to EOF even though we only kept 3 lines
    assert.equal(r.totalLines, bigLines);
  });

  it('reads the full large file in throw mode when within maxBytes', async () => {
    const stat = fs.statSync(bigMulti);
    const r = await readFileInRange(bigMulti, 0, undefined, stat.size + 10);
    assert.equal(r.lineCount, bigLines);
    assert.equal(r.totalLines, bigLines);
  });

  it('throws FileTooLargeError when streamed bytes exceed maxBytes (throw mode)', async () => {
    await assert.rejects(
      () => readFileInRange(bigMulti, 0, undefined, 1_000_000),
      (err: unknown) => err instanceof FileTooLargeError,
    );
  });

  it('clips at a line boundary and flags truncatedByBytes (streaming truncate mode)', async () => {
    // budget for ~5 lines of 10 chars + separators.
    const r = await readFileInRange(bigMulti, 0, undefined, 54, undefined, { truncateOnByteLimit: true });
    assert.equal(r.truncatedByBytes, true);
    // every kept line is a complete 10-char unit (clean boundary, no mid-line cut)
    for (const line of r.content.split('\n')) {
      assert.equal(line, 'abcdefghij');
    }
    assert.ok(r.readBytes <= 54);
    assert.equal(r.totalLines, bigLines); // still scanned to EOF
  });

  it('bounds memory on a huge single line: truncate mode discards the overflowing fragment', async () => {
    const r = await readFileInRange(bigSingle, 0, 1, 100, undefined, { truncateOnByteLimit: true });
    // the single >10MB line cannot fit a 100-byte budget → discarded, flagged
    assert.equal(r.truncatedByBytes, true);
    assert.equal(r.content, '');
    assert.equal(r.totalLines, 1);
  });

  it('counts a huge out-of-range single line without accumulating it', async () => {
    const r = await readFileInRange(bigSingle, 5, 1);
    assert.equal(r.content, '');
    assert.equal(r.lineCount, 0);
    assert.equal(r.totalLines, 1); // the one (unterminated) line is counted
  });

  it('strips a leading UTF-8 BOM on the streaming path', async () => {
    const p = write('big-bom.txt', '﻿first\n' + 'pad-line\n'.repeat(FAST_PATH_MAX_SIZE / 9 + 1));
    const r = await readFileInRange(p, 0, 1);
    assert.equal(r.content, 'first');
  });

  it('emits the final unterminated in-range line on the streaming path (end handler)', async () => {
    const r = await readFileInRange(bigSingle, 0, 1);
    assert.equal(r.lineCount, 1);
    assert.equal(r.content.length, FAST_PATH_MAX_SIZE + 1_000_000);
    assert.equal(r.totalLines, 1);
  });

  describe('streaming end-handler byte budget', () => {
    let noNl: string;
    let size: number;
    before(() => {
      // >10 MB of 10-char terminated lines, then a final UNTERMINATED "LAST" line.
      const unit = 'abcdefghij\n';
      const repeats = Math.ceil((FAST_PATH_MAX_SIZE + 1_000_000) / unit.length);
      noNl = write('big-no-nl.txt', unit.repeat(repeats) + 'LAST');
      size = fs.statSync(noNl).size; // == 11*repeats + 4
    });

    it('pushes the final unterminated line when it fits the budget (truncate mode)', async () => {
      const r = await readFileInRange(noNl, 0, undefined, size, undefined, { truncateOnByteLimit: true });
      assert.equal(r.truncatedByBytes, undefined);
      assert.ok(r.content.endsWith('\nLAST'));
    });

    it('drops the final unterminated line when it overflows the budget at EOF (truncate mode)', async () => {
      // budget fits every terminated line (size-5) but not the final "LAST".
      const r = await readFileInRange(noNl, 0, undefined, size - 5, undefined, { truncateOnByteLimit: true });
      assert.equal(r.truncatedByBytes, true);
      assert.ok(!r.content.includes('LAST'));
    });
  });
});

// ---------------------------------------------------------------------------
// Byte-oriented bounded readers: readFileRange / tailFile
// ---------------------------------------------------------------------------
describe('readFileRange — byte-offset partial read', () => {
  it('reads up to maxBytes from the given byte offset and reports bytesTotal', async () => {
    const p = write('bytes.txt', '0123456789');
    const r = await readFileRange(p, 3, 4);
    assert.ok(r);
    assert.equal(r!.content, '3456');
    assert.equal(r!.bytesRead, 4);
    assert.equal(r!.bytesTotal, 10);
  });

  it('clamps bytesToRead to what remains after the offset (maxBytes larger than the rest)', async () => {
    const p = write('clamp.txt', '0123456789');
    const r = await readFileRange(p, 7, 1000);
    assert.ok(r);
    assert.equal(r!.content, '789');
    assert.equal(r!.bytesRead, 3);
    assert.equal(r!.bytesTotal, 10);
  });

  it('returns null when the file is smaller than or equal to the offset', async () => {
    const p = write('short.txt', 'abc');
    assert.equal(await readFileRange(p, 3, 10), null); // size === offset
    assert.equal(await readFileRange(p, 100, 10), null); // size < offset
  });

  it('reads from offset 0 like a bounded head read', async () => {
    const p = write('head.txt', 'hello world');
    const r = await readFileRange(p, 0, 5);
    assert.ok(r);
    assert.equal(r!.content, 'hello');
    assert.equal(r!.bytesRead, 5);
    assert.equal(r!.bytesTotal, 11);
  });

  it('decodes an aligned multi-byte UTF-8 range without corruption', async () => {
    // 'é' is 2 bytes (0xC3 0xA9); read exactly those two bytes from offset 1.
    const p = write('utf8.txt', Buffer.from('aé', 'utf8')); // bytes: 61 C3 A9
    const r = await readFileRange(p, 1, 2);
    assert.ok(r);
    assert.equal(r!.content, 'é');
    assert.equal(r!.bytesRead, 2);
    assert.equal(r!.bytesTotal, 3);
  });
});

describe('tailFile — last-maxBytes read', () => {
  it('reads only the last maxBytes of a file', async () => {
    const p = write('tail.txt', '0123456789');
    const r = await tailFile(p, 4);
    assert.equal(r.content, '6789');
    assert.equal(r.bytesRead, 4);
    assert.equal(r.bytesTotal, 10);
  });

  it('returns the whole file when it is smaller than maxBytes', async () => {
    const p = write('tail-small.txt', 'abc');
    const r = await tailFile(p, 1000);
    assert.equal(r.content, 'abc');
    assert.equal(r.bytesRead, 3);
    assert.equal(r.bytesTotal, 3);
  });

  it('returns an empty result for a 0-byte file', async () => {
    const p = write('empty.txt', '');
    const r = await tailFile(p, 100);
    assert.equal(r.content, '');
    assert.equal(r.bytesRead, 0);
    assert.equal(r.bytesTotal, 0);
  });

  it('tails a large file without loading the whole thing (bounded content)', async () => {
    const big = 'x'.repeat(5_000_000) + 'TAILMARKER';
    const p = write('big.log', big);
    const r = await tailFile(p, 16);
    assert.equal(r.content, 'xxxxxxTAILMARKER');
    assert.equal(r.bytesRead, 16);
    assert.equal(r.bytesTotal, big.length);
  });
});
