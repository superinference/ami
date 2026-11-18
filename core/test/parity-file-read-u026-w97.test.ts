import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  estimateReadTokensForFileType,
  sliceToReadTokenBudget,
  estimateTokensForFileType,
  estimateTextTokens,
  utf8ByteLength,
} from '../src/token-estimator';
import { fileReadTool } from '../src/tools/file-read';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U026-w97 — GAP: accurate token-budget enforcement for the Read tool
// (limit-bypass hardening). The read token-budget guard previously estimated
// tokens by UTF-16 code units (chars/ratio), which under-counts CJK/multibyte
// content 3-4x (one code unit is 3-4 UTF-8 bytes), so a file that is really far
// over the token budget could slip its WHOLE payload into the prompt. The fix
// routes detection AND truncation through a byte-aware, file-type-aware estimate
// (estimateReadTokensForFileType / sliceToReadTokenBudget), closing the bypass
// while leaving pure-ASCII reads byte-for-code-unit identical.
// ---------------------------------------------------------------------------

describe('U026-w97: byte-aware read token estimate (unit)', () => {
  it('equals the old code-unit estimate for pure ASCII (no regression)', () => {
    for (const s of ['', 'a', 'abcd', 'abcde', 'hello world', 'x'.repeat(401)]) {
      assert.equal(
        estimateReadTokensForFileType(s, '.ts'),
        estimateTokensForFileType(s, '.ts'),
        `ascii generic s.len=${s.length}`,
      );
      assert.equal(
        estimateReadTokensForFileType(s, '.json'),
        estimateTokensForFileType(s, '.json'),
        `ascii json s.len=${s.length}`,
      );
    }
  });

  it('counts CJK content by UTF-8 bytes, strictly above the old under-count', () => {
    const cjk = '你'.repeat(100); // 100 code units, 300 UTF-8 bytes
    // Old estimate: ceil(100/4) = 25 (the under-count that let it slip the budget).
    assert.equal(estimateTokensForFileType(cjk, '.txt'), 25);
    // Byte-aware: ceil(300/4) = 75.
    assert.equal(estimateReadTokensForFileType(cjk, '.txt'), 75);
    assert.ok(estimateReadTokensForFileType(cjk, '.txt') > estimateTokensForFileType(cjk, '.txt'));
    // Composes with the dense-JSON ratio (2 bytes/token): ceil(300/2) = 150.
    assert.equal(estimateReadTokensForFileType(cjk, '.json'), 150);
  });

  it('astral emoji (4 UTF-8 bytes) are counted above their 2-code-unit length', () => {
    const emoji = '😀'.repeat(10); // 20 code units, 40 UTF-8 bytes
    assert.equal(utf8ByteLength(emoji), 40);
    assert.equal(estimateReadTokensForFileType(emoji, '.txt'), estimateTextTokens(emoji, 4));
    assert.equal(estimateReadTokensForFileType(emoji, '.txt'), 10); // ceil(40/4)
  });
});

describe('U026-w97: sliceToReadTokenBudget (unit)', () => {
  it('keeps the historical maxTokens*ratio char slice for ASCII', () => {
    const ascii = 'x'.repeat(1000);
    assert.equal(sliceToReadTokenBudget(ascii, 10, '.txt'), ascii.slice(0, 40)); // 10*4 bytes
    assert.equal(sliceToReadTokenBudget(ascii, 10, '.json'), ascii.slice(0, 20)); // 10*2 bytes
  });

  it('returns the text unchanged when it already fits', () => {
    assert.equal(sliceToReadTokenBudget('short', 100, '.txt'), 'short');
  });

  it('empty text and non-positive budget yield an empty slice', () => {
    assert.equal(sliceToReadTokenBudget('', 10, '.txt'), '');
    assert.equal(sliceToReadTokenBudget('abc', 0, '.txt'), '');
    assert.equal(sliceToReadTokenBudget('abc', -1, '.txt'), '');
  });

  it('cuts CJK on a whole-character boundary (no partial multibyte / replacement char)', () => {
    const cjk = '你'.repeat(100); // each '你' is 3 UTF-8 bytes
    // Budget 10 tokens * 4 bytes = 40 bytes = 13.33 chars → must keep whole chars only.
    const kept = sliceToReadTokenBudget(cjk, 10, '.txt');
    assert.ok(!kept.includes('�'), 'no replacement character');
    // 40 bytes / 3 bytes per char = 13 whole characters.
    assert.equal(kept.length, 13);
    assert.ok(utf8ByteLength(kept) <= 40, 'kept slice stays within the byte budget');
    // The kept slice must itself be within the token budget (the char-slice bug kept ~3x).
    assert.ok(estimateReadTokensForFileType(kept, '.txt') <= 10);
  });
});

// ---------------------------------------------------------------------------
// e2e (real fileReadTool.execute): a CJK-dense file whose OLD chars/4 estimate
// is UNDER the budget but whose byte-aware estimate is OVER must now truncate.
// RED on revert (code-unit estimate): the file was returned whole — the bypass.
// ---------------------------------------------------------------------------
describe('U026-w97: Read-tool CJK token-budget bypass (e2e red-green)', () => {
  let tmpDir: string;
  function ctx(overrides?: Partial<ToolContext>): ToolContext {
    return {
      cwd: tmpDir,
      abortSignal: new AbortController().signal,
      ...overrides,
    } as ToolContext;
  }
  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fr-u026-w97-'));
    getFileCache(tmpDir).clear();
  });
  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('truncates a CJK file that the old code-unit estimate let slip the budget (full read)', async () => {
    // 400 CJK chars (one line): old estimate ≈ (header+400)/4 ≈ 120 tokens < 200;
    // byte-aware ≈ header/4 + 400*3/4 = 320+ tokens > 200 → must truncate.
    const p = path.join(tmpDir, 'notes.txt');
    fs.writeFileSync(p, '文'.repeat(400));
    const res = await fileReadTool.execute(
      { file_path: p },
      ctx({ outputBudget: { fileReadMaxTokens: 200 } }),
    );
    assert.equal(res.isError, undefined);
    assert.ok(
      res.output.includes('truncated: file exceeds 200 token budget'),
      'CJK content over the byte-aware budget must be truncated (bypass closed)',
    );
  });

  it('does NOT truncate the same-sized ASCII file at the same budget (discriminates the fix)', async () => {
    // 400 ASCII chars: byte length == code-unit length, so ≈ 105 tokens < 200.
    const p = path.join(tmpDir, 'notes.txt');
    fs.writeFileSync(p, 'a'.repeat(400));
    const res = await fileReadTool.execute(
      { file_path: p },
      ctx({ outputBudget: { fileReadMaxTokens: 200 } }),
    );
    assert.equal(res.isError, undefined);
    assert.ok(
      !res.output.includes('truncated: file exceeds'),
      'equal-length ASCII stays under budget — proves the discriminator is multibyte density',
    );
  });

  it('truncates a CJK range read over the byte-aware budget', async () => {
    const p = path.join(tmpDir, 'lines.txt');
    fs.writeFileSync(p, Array.from({ length: 50 }, () => '漢'.repeat(20)).join('\n'));
    const res = await fileReadTool.execute(
      { file_path: p, offset: 0, limit: 50 },
      ctx({ outputBudget: { fileReadMaxTokens: 60 } }),
    );
    assert.equal(res.isError, undefined);
    assert.ok(
      res.output.includes('truncated: exceeds 60 token budget'),
      'CJK range over the byte-aware budget must be truncated',
    );
    assert.ok(!res.output.includes('�'), 'truncated slice has no replacement character');
  });
});
