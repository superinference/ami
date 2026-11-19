/**
 * Output bounding for one tool result: offset paging + explicit (never-silent)
 * truncation, on both the character and byte axes (Orca-parity tool-output-limits).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  TOOL_LIST_PAGE_SIZE,
  TOOL_OUTPUT_MAX_CHARS,
  TOOL_TEXT_PAGE_CHARS,
  TOOL_DOWNLOAD_MAX_BYTES,
  BOUND_NOTE_PREFIX,
  pageOfText,
  truncationNote,
  capToolOutput,
  utf8SafeSliceEnd,
  pageOfBytes,
  byteTruncationNote,
  capToolOutputBytes,
  formatMegabytes,
  boundToolOutput,
} from '../src/tool-output-limits';

describe('tool-output-limits — constants', () => {
  it('exposes the Orca-parity caps', () => {
    assert.equal(TOOL_LIST_PAGE_SIZE, 20);
    assert.equal(TOOL_OUTPUT_MAX_CHARS, 20_000);
    assert.equal(TOOL_TEXT_PAGE_CHARS, 16_000);
    assert.equal(TOOL_DOWNLOAD_MAX_BYTES, 2_900_000);
    assert.equal(BOUND_NOTE_PREFIX, 'AMI');
  });
});

describe('pageOfText', () => {
  it('pages long text by offset and says where the next page starts', () => {
    assert.deepEqual(pageOfText('abcdef', 0, 4), { text: 'abcd', total: 6, nextOffset: 4 });
    assert.deepEqual(pageOfText('abcdef', 4, 4), { text: 'ef', total: 6 });
    assert.deepEqual(pageOfText('abc', 99, 4), { text: '', total: 3 });
  });

  it('clamps a negative offset to the start and floors fractional inputs', () => {
    assert.deepEqual(pageOfText('abcdef', -5, 2.9), { text: 'ab', total: 6, nextOffset: 2 });
  });

  it('treats a non-positive size as a single char page', () => {
    assert.deepEqual(pageOfText('abc', 0, 0), { text: 'a', total: 3, nextOffset: 1 });
  });

  it('uses the default page size when size is omitted', () => {
    const text = 'x'.repeat(TOOL_TEXT_PAGE_CHARS + 5);
    const page = pageOfText(text);
    assert.equal(page.text.length, TOOL_TEXT_PAGE_CHARS);
    assert.equal(page.nextOffset, TOOL_TEXT_PAGE_CHARS);
  });
});

describe('truncationNote / capToolOutput', () => {
  it('cuts an answer over the limit with a marker that says so and how to see less', () => {
    assert.equal(capToolOutput('short', 'hint', 10), 'short');
    assert.equal(
      capToolOutput('x'.repeat(12), 'Use --offset.', 10),
      `${'x'.repeat(10)}\n[AMI: truncated at 10 of 12 characters. Use --offset.]`,
    );
  });

  it('keeps content exactly at the cap untouched (never silently annotated)', () => {
    assert.equal(capToolOutput('x'.repeat(10), 'hint', 10), 'x'.repeat(10));
  });

  it('builds the note standalone with the default cap', () => {
    assert.equal(
      truncationNote(50000, 'page it'),
      '[AMI: truncated at 20000 of 50000 characters. page it]',
    );
  });
});

describe('utf8SafeSliceEnd', () => {
  it('returns 0 for a non-positive budget', () => {
    assert.equal(utf8SafeSliceEnd('abc', 0), 0);
    assert.equal(utf8SafeSliceEnd('abc', -1), 0);
  });

  it('returns the full length when the whole string fits', () => {
    assert.equal(utf8SafeSliceEnd('abc', 100), 3);
  });

  it('never splits a multibyte codepoint', () => {
    // '€' is 3 UTF-8 bytes. A 2-byte budget must take nothing; a 3-byte budget takes it.
    assert.equal(utf8SafeSliceEnd('€x', 2), 0);
    assert.equal(utf8SafeSliceEnd('€x', 3), 1);
    assert.equal(utf8SafeSliceEnd('€x', 4), 2);
  });
});

describe('pageOfBytes / capToolOutputBytes', () => {
  it('pages by bytes without splitting a multibyte char and reports the byte total', () => {
    const text = '€€€'; // 9 bytes, 3 chars
    const page = pageOfBytes(text, 0, 4); // only one full '€' (3 bytes) fits under 4
    assert.deepEqual(page, { text: '€', totalBytes: 9, nextOffset: 1 });
    const rest = pageOfBytes(text, page.nextOffset, 100);
    assert.deepEqual(rest, { text: '€€', totalBytes: 9 });
  });

  it('caps byte output with a megabyte-labelled note and leaves small content alone', () => {
    assert.equal(capToolOutputBytes('tiny', 'hint', 1000), 'tiny');
    const big = 'a'.repeat(50);
    const capped = capToolOutputBytes(big, 'download less', 10);
    assert.equal(capped, `${'a'.repeat(10)}\n${byteTruncationNote(50, 'download less', 10)}`);
  });
});

describe('formatMegabytes', () => {
  it('formats and trims a trailing .0', () => {
    assert.equal(formatMegabytes(2_900_000), '2.9 MB');
    assert.equal(formatMegabytes(1_000_000), '1 MB');
    assert.equal(formatMegabytes(1_500_000), '1.5 MB');
  });
});

describe('boundToolOutput — end-to-end bounding', () => {
  it('returns short content unchanged, not truncated, no next page', () => {
    const r = boundToolOutput('hello');
    assert.equal(r.text, 'hello');
    assert.equal(r.truncated, false);
    assert.equal(r.total, 5);
    assert.equal(r.nextOffset, undefined);
  });

  it('appends an explicit next-page note when the source has more (never silent)', () => {
    const text = 'x'.repeat(100);
    const r = boundToolOutput(text, { offset: 0, pageChars: 40, max: 1000 });
    assert.equal(r.nextOffset, 40);
    assert.ok(r.text.includes('[AMI: 100 characters total; next page at offset 40.]'));
    assert.equal(r.truncated, false);
  });

  it('marks truncated when a single page still exceeds the char cap', () => {
    const text = 'y'.repeat(30);
    const r = boundToolOutput(text, { pageChars: 30, max: 10, hint: 'see less' });
    assert.equal(r.truncated, true);
    assert.ok(r.text.includes('[AMI: truncated at 10 of 30 characters. see less]'));
  });
});
