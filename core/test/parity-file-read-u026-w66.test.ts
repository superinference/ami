import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileReadTool, classifyPdfError } from '../src/tools/file-read';
import { memoryFreshnessReminder } from '../src/memory';
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fr-u026-w66-'));
  getFileCache(tmpDir).clear();
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// GAP: file-type-aware bytes/token ratio for the read token-budget guard.
// Dense JSON (~2 chars/token) must be counted tighter than generic prose
// (~4 chars/token) so an oversized JSON payload cannot slip past the 25k-token
// read budget. RED before fix: both paths used Math.ceil(len/4), so the JSON
// file below (estimate 4 ≈ 20k < 25k) was NOT truncated.
// ---------------------------------------------------------------------------
describe('U026-w66: file-type-aware token budget (full read)', () => {
  // ~80 KB of content: at ~4 chars/token ≈ 20k tokens (under the 25k budget),
  // at ~2 chars/token ≈ 40k tokens (over the budget).
  const line = 'x'.repeat(78);
  const body = Array.from({ length: 1000 }, () => line).join('\n'); // ~79 KB

  it('truncates a dense .json file whose /2 estimate exceeds the budget', async () => {
    const p = path.join(tmpDir, 'big.json');
    fs.writeFileSync(p, body);
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(
      res.output.includes('truncated: file exceeds'),
      'dense JSON over the /2-ratio budget must be truncated',
    );
  });

  it('does NOT truncate the identical content as .txt (generic /4 ratio stays under budget)', async () => {
    const p = path.join(tmpDir, 'big.txt');
    fs.writeFileSync(p, body);
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(
      !res.output.includes('truncated: file exceeds'),
      'generic text under the /4-ratio budget must NOT be truncated — discriminates the fix',
    );
  });
});

describe('U026-w66: file-type-aware token budget (range read)', () => {
  const longLine = 'y'.repeat(900);
  const body = Array.from({ length: 200 }, () => longLine).join('\n');

  it('truncates a dense .jsonl range whose /2 estimate exceeds the budget', async () => {
    const p = path.join(tmpDir, 'big.jsonl');
    fs.writeFileSync(p, body);
    const res = await fileReadTool.execute({ file_path: p, offset: 0, limit: 100 }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(
      res.output.includes('truncated: exceeds'),
      'dense JSONL range over the /2-ratio budget must be truncated',
    );
  });

  it('does NOT truncate the identical .txt range (generic /4 ratio stays under budget)', async () => {
    const p = path.join(tmpDir, 'big-range.txt');
    fs.writeFileSync(p, body);
    const res = await fileReadTool.execute({ file_path: p, offset: 0, limit: 100 }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(
      !res.output.includes('truncated: exceeds'),
      'generic text range under the /4-ratio budget must NOT be truncated — discriminates the fix',
    );
  });
});

// ---------------------------------------------------------------------------
// GAP: structured PDF error classification — a damaged/corrupt PDF gets its own
// actionable message distinct from the generic reader error. RED before fix:
// classifyPdfError only branched on password/encryption.
// ---------------------------------------------------------------------------
describe('U026-w66: classifyPdfError — corrupted/damaged branch', () => {
  it('maps a "damaged" failure to the corrupted message', () => {
    const out = classifyPdfError('PDFParse: file stream is damaged');
    assert.ok(out.includes('corrupted or invalid'));
    assert.ok(!out.includes('Error reading PDF'));
  });

  it('maps a "corrupt"/"corrupted" failure to the corrupted message', () => {
    assert.ok(classifyPdfError('The PDF appears to be corrupt').includes('corrupted or invalid'));
    assert.ok(classifyPdfError('corrupted object table').includes('corrupted or invalid'));
  });

  it('keeps password classification ahead of the corrupted branch', () => {
    assert.ok(classifyPdfError('encrypted').includes('password-protected or encrypted'));
  });

  it('REGRESSION: a benign "Invalid xref table" stays the generic error (bare "invalid" excluded)', () => {
    const out = classifyPdfError('Invalid xref table');
    assert.ok(out.includes('Error reading PDF'));
    assert.ok(out.includes('Invalid xref table'));
    assert.ok(!out.includes('corrupted or invalid'));
  });
});

// ---------------------------------------------------------------------------
// GAP: per-read memory staleness note wrapped in <system-reminder> tags for
// file_read-style callers. RED before fix: the note was a plain [Memory file …]
// bracket with no system-reminder wrapper.
// ---------------------------------------------------------------------------
describe('U026-w66: memoryFreshnessReminder (system-reminder wrapping)', () => {
  it('wraps a stale note in <system-reminder> tags', () => {
    const fiveDaysAgo = Date.now() - 5 * 24 * 60 * 60 * 1000;
    const note = memoryFreshnessReminder(fiveDaysAgo);
    assert.ok(note.startsWith('<system-reminder>'));
    assert.ok(note.includes('</system-reminder>\n'));
    assert.match(note, /last modified 5 days ago/);
  });

  it('returns "" for a fresh (<= 1 day) file', () => {
    assert.equal(memoryFreshnessReminder(Date.now()), '');
  });

  it('file_read of a stale memory file prepends the wrapped reminder', async () => {
    const memDir = path.join(tmpDir, '.ami', 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    const p = path.join(memDir, 'old.md');
    fs.writeFileSync(p, '# notes\nsome stored observation\n');
    const fiveDaysAgo = (Date.now() - 5 * 24 * 60 * 60 * 1000) / 1000;
    fs.utimesSync(p, fiveDaysAgo, fiveDaysAgo);
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(
      res.output.startsWith('<system-reminder>Memory file — last modified'),
      'the staleness note must be wrapped in system-reminder tags — discriminates the fix',
    );
    assert.match(res.output, /last modified 5 days ago/);
  });

  it('file_read of a fresh memory file adds no staleness note', async () => {
    const memDir = path.join(tmpDir, '.ami', 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    const p = path.join(memDir, 'fresh.md');
    fs.writeFileSync(p, '# notes\nfresh observation\n');
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(!res.output.includes('Memory file — last modified'));
  });
});
