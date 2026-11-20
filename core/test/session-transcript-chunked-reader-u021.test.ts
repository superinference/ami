/**
 * U021 — forward chunked, compact-boundary-truncating resume reader
 * (parity with the reference's sessionStoragePortable readTranscriptForLoad +
 * TRANSCRIPT_READ_CHUNK_SIZE):
 *
 *  - TRANSCRIPT_READ_CHUNK_SIZE = 1 MiB knob exists alongside the pre-existing
 *    SKIP_PRECOMPACT_THRESHOLD = 5 MiB.
 *  - readTranscriptForLoad reads the transcript fd in chunks (handling lines that
 *    straddle a chunk seam and multi-byte UTF-8 split across the seam), strips
 *    attribution-snapshot lines keeping only the most-recent one appended at EOF,
 *    and — when boundary truncation is enabled — drops the pre-compaction prefix at
 *    the last non-preserved compact_boundary (keeping it when preservedSegment).
 *  - loadMessagesFromTranscriptPath is wired onto the chunked reader and only
 *    applies boundary truncation for files at/above SKIP_PRECOMPACT_THRESHOLD, so a
 *    large auto-compacted session resumes to its post-compaction thread while a
 *    small session is unchanged.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  readTranscriptForLoad,
  loadMessagesFromTranscriptPath,
  TRANSCRIPT_READ_CHUNK_SIZE,
  SKIP_PRECOMPACT_THRESHOLD,
} from '../src/session';

const tmpDirs: string[] = [];
function tmpFile(contents: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-transcript-'));
  tmpDirs.push(dir);
  const p = path.join(dir, 'transcript.jsonl');
  fs.writeFileSync(p, contents);
  return p;
}
after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

const msg = (uuid: string, parentUuid: string | undefined, text: string) =>
  JSON.stringify({ role: 'user', uuid, parentUuid, content: text });
const snapshot = (id: string) =>
  JSON.stringify({ type: 'attribution-snapshot', messageId: id, surface: 'cli', fileStates: {} });
const boundary = (preserved = false) =>
  JSON.stringify({ type: 'system', subtype: 'compact_boundary', compactMetadata: { preservedSegment: preserved } });

describe('U021 TRANSCRIPT_READ_CHUNK_SIZE knob', () => {
  it('is 1 MiB and distinct from the 5 MiB skip-precompact threshold', () => {
    assert.equal(TRANSCRIPT_READ_CHUNK_SIZE, 1024 * 1024);
    assert.equal(SKIP_PRECOMPACT_THRESHOLD, 5 * 1024 * 1024);
    assert.ok(TRANSCRIPT_READ_CHUNK_SIZE < SKIP_PRECOMPACT_THRESHOLD);
  });
});

describe('U021 readTranscriptForLoad', () => {
  it('returns every line for a boundaryless file (no truncation configured)', () => {
    const p = tmpFile([msg('a', undefined, 'one'), msg('b', 'a', 'two')].join('\n') + '\n');
    const r = readTranscriptForLoad(p, { applyBoundaryTruncation: true });
    assert.equal(r.lines.length, 2);
    assert.equal(r.boundaryStartOffset, 0);
    assert.equal(r.hasPreservedSegment, false);
  });

  it('keeps only the most-recent attribution-snapshot, appended at EOF', () => {
    const p = tmpFile([
      snapshot('old'),
      msg('a', undefined, 'one'),
      snapshot('mid'),
      msg('b', 'a', 'two'),
      snapshot('newest'),
    ].join('\n') + '\n');
    const r = readTranscriptForLoad(p, {});
    // two data lines + exactly one snapshot (the newest), appended last.
    assert.equal(r.lines.length, 3);
    assert.ok(r.lines[0].includes('"content":"one"'));
    assert.ok(r.lines[1].includes('"content":"two"'));
    const snaps = r.lines.filter(l => l.includes('attribution-snapshot'));
    assert.equal(snaps.length, 1);
    assert.ok(snaps[0].includes('"messageId":"newest"'));
    assert.equal(r.lines[r.lines.length - 1], snaps[0]);
  });

  it('truncates the pre-compaction prefix at a non-preserved boundary', () => {
    const lines = [
      msg('a', undefined, 'pre1'),
      msg('b', 'a', 'pre2'),
      boundary(false),
      msg('c', 'b', 'post1'),
      msg('d', 'c', 'post2'),
    ];
    const p = tmpFile(lines.join('\n') + '\n');
    const r = readTranscriptForLoad(p, { applyBoundaryTruncation: true });
    // pre1/pre2 are gone; the boundary line + the two post lines survive.
    assert.ok(!r.lines.some(l => l.includes('pre1')));
    assert.ok(!r.lines.some(l => l.includes('pre2')));
    assert.ok(r.lines.some(l => l.includes('post1')));
    assert.ok(r.lines.some(l => l.includes('post2')));
    assert.equal(r.hasPreservedSegment, false);
    assert.ok(r.boundaryStartOffset > 0);
  });

  it('keeps the earlier output when the boundary preserves a segment', () => {
    const lines = [
      msg('a', undefined, 'pre1'),
      boundary(true),
      msg('c', 'a', 'post1'),
    ];
    const p = tmpFile(lines.join('\n') + '\n');
    const r = readTranscriptForLoad(p, { applyBoundaryTruncation: true });
    assert.ok(r.lines.some(l => l.includes('pre1')));
    assert.ok(r.lines.some(l => l.includes('post1')));
    assert.equal(r.hasPreservedSegment, true);
    assert.equal(r.boundaryStartOffset, 0);
  });

  it('does NOT truncate when boundary truncation is disabled', () => {
    const lines = [msg('a', undefined, 'pre1'), boundary(false), msg('c', 'a', 'post1')];
    const p = tmpFile(lines.join('\n') + '\n');
    const r = readTranscriptForLoad(p, { applyBoundaryTruncation: false });
    assert.ok(r.lines.some(l => l.includes('pre1')));
    assert.ok(r.lines.some(l => l.includes('post1')));
    assert.equal(r.boundaryStartOffset, 0);
  });

  it('reassembles lines straddling a chunk seam (tiny chunk size)', () => {
    const lines = [
      msg('a', undefined, 'first-message-with-some-length'),
      boundary(false),
      msg('c', 'a', 'second-message-also-long'),
      snapshot('snap'),
    ];
    const p = tmpFile(lines.join('\n') + '\n');
    // chunkSize=7 forces every line across many seams.
    const r = readTranscriptForLoad(p, { applyBoundaryTruncation: true, chunkSize: 7 });
    assert.ok(!r.lines.some(l => l.includes('first-message')));
    assert.ok(r.lines.some(l => l.includes('second-message-also-long')));
    const snaps = r.lines.filter(l => l.includes('attribution-snapshot'));
    assert.equal(snaps.length, 1);
    // the surviving post line must be valid JSON (not torn at a seam).
    const post = r.lines.find(l => l.includes('second-message-also-long'))!;
    assert.doesNotThrow(() => JSON.parse(post));
  });

  it('reassembles a multi-byte UTF-8 char split across a chunk seam', () => {
    // '🚀' is 4 UTF-8 bytes; a naive per-chunk toString would corrupt it.
    const line = msg('a', undefined, 'rocket-🚀-tail');
    const p = tmpFile(line + '\n');
    const r = readTranscriptForLoad(p, { chunkSize: 3 });
    assert.equal(r.lines.length, 1);
    const parsed = JSON.parse(r.lines[0]) as { content: string };
    assert.ok(parsed.content.includes('🚀'));
  });

  it('handles a final line with no trailing newline', () => {
    const p = tmpFile([msg('a', undefined, 'one'), msg('b', 'a', 'two')].join('\n')); // no trailing \n
    const r = readTranscriptForLoad(p, {});
    assert.equal(r.lines.length, 2);
    assert.ok(r.lines[1].includes('two'));
  });

  it('retains a trailing attribution-snapshot with no trailing newline', () => {
    const p = tmpFile(msg('a', undefined, 'one') + '\n' + snapshot('tail')); // snapshot is final, unterminated
    const r = readTranscriptForLoad(p, {});
    assert.equal(r.lines.length, 2);
    assert.ok(r.lines[1].includes('"messageId":"tail"'));
  });

  it('ignores a non-boundary system line whose content merely mentions the marker', () => {
    const decoy = JSON.stringify({ role: 'user', uuid: 'x', content: 'talking about "compact_boundary" here' });
    const p = tmpFile([decoy, msg('y', 'x', 'real')].join('\n') + '\n');
    const r = readTranscriptForLoad(p, { applyBoundaryTruncation: true });
    // The decoy is NOT a real boundary, so nothing is truncated.
    assert.equal(r.lines.length, 2);
    assert.ok(r.lines.some(l => l.includes('"uuid":"x"')));
    assert.equal(r.boundaryStartOffset, 0);
  });

  it('returns empty for a missing, empty, or oversized file', () => {
    assert.deepEqual(readTranscriptForLoad('/no/such/file.jsonl', {}).lines, []);
    const empty = tmpFile('');
    assert.deepEqual(readTranscriptForLoad(empty, {}).lines, []);
    const small = tmpFile(msg('a', undefined, 'x') + '\n');
    assert.deepEqual(readTranscriptForLoad(small, { fileSize: 200 * 1024 * 1024 }).lines, []);
  });
});

describe('U021 loadMessagesFromTranscriptPath wired onto the chunked reader', () => {
  it('leaves a small session (below the threshold) unchanged by boundary truncation', () => {
    // A boundary present but file < 5 MiB → truncation NOT applied → full chain.
    const lines = [
      msg('a', undefined, 'pre1'),
      msg('b', 'a', 'pre2'),
      boundary(false),
      msg('c', 'b', 'post1'),
      msg('d', 'c', 'post2'),
    ];
    const p = tmpFile(lines.join('\n') + '\n');
    const msgs = loadMessagesFromTranscriptPath(p);
    assert.equal(msgs.length, 4); // whole parentUuid chain preserved
  });

  it('truncates the pre-compaction prefix for a session at/above the threshold', () => {
    // Pad the pre-compaction region past 5 MiB so truncation engages, then assert
    // the resumed chain is only the post-compaction messages.
    const pre: string[] = [msg('a', undefined, 'pre1'), msg('b', 'a', 'pre2')];
    // Filler messages (NOT in the resumed leaf's ancestor chain) only inflate size.
    const fillerPayload = 'x'.repeat(1024);
    for (let i = 0; i < 5200; i++) {
      pre.push(JSON.stringify({ role: 'user', uuid: `f${i}`, content: fillerPayload }));
    }
    const all = [
      ...pre,
      boundary(false),
      msg('c', 'b', 'post1'),
      msg('d', 'c', 'post2'),
    ];
    const p = tmpFile(all.join('\n') + '\n');
    assert.ok(fs.statSync(p).size >= SKIP_PRECOMPACT_THRESHOLD, 'fixture must exceed the threshold');

    const msgs = loadMessagesFromTranscriptPath(p);
    // c.parentUuid=b, but b's line was truncated away → chain resolves to [c, d].
    assert.equal(msgs.length, 2);
    assert.equal((msgs[0] as { uuid?: string }).uuid, 'c');
    assert.equal((msgs[1] as { uuid?: string }).uuid, 'd');
    assert.ok(!msgs.some(m => (m as { content?: string }).content === 'pre1'));
  });
});
