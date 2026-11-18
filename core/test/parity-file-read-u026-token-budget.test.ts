import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileReadTool, applyReadTokenBudget } from '../src/tools/file-read';
import {
  getFileReadErrorOnBudget,
  FILE_READ_ERROR_ON_BUDGET_ENV,
  FILE_READ_MAX_TOKENS_ENV,
} from '../src/tools/file-read-limits';
import { fileReadTokenBudgetMessage } from '../src/token-estimator';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

// =============================================================================
// U026 — GAP (FileReadTool.ts:L755-772): two-stage token budgeting / error vs
// silent truncation. AMI already does a byte-aware, file-type-aware rough
// estimate; this adds the Claude-parity OPT-IN to HARD-ERROR (with an actionable
// offset/limit-or-search nudge) instead of silently truncating a file mid-content.
// =============================================================================

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

beforeEach(() => {
  tmpDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-fr-u026-budget-')));
  getFileCache(tmpDir).clear();
  delete process.env[FILE_READ_ERROR_ON_BUDGET_ENV];
  delete process.env[FILE_READ_MAX_TOKENS_ENV];
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
  delete process.env[FILE_READ_ERROR_ON_BUDGET_ENV];
  delete process.env[FILE_READ_MAX_TOKENS_ENV];
});

describe('U026 fileReadTokenBudgetMessage (unit)', () => {
  it('names both the count and cap and offers offset/limit AND search remedies', () => {
    const msg = fileReadTokenBudgetMessage(50_000, 25_000);
    assert.match(msg, /50000 tokens/);
    assert.match(msg, /maximum allowed tokens \(25000\)/);
    assert.match(msg, /offset and limit/);
    assert.match(msg, /search for/);
  });
});

describe('U026 getFileReadErrorOnBudget (resolver, default OFF)', () => {
  it('defaults OFF with a clean env', () => {
    assert.equal(getFileReadErrorOnBudget({}), false);
  });
  it('turns ON only for an explicitly truthy token', () => {
    for (const v of ['1', 'true', 'TRUE', 'yes', 'on', 'On']) {
      assert.equal(getFileReadErrorOnBudget({ [FILE_READ_ERROR_ON_BUDGET_ENV]: v }), true, `"${v}" → on`);
    }
  });
  it('stays OFF for empty / falsy / unrelated tokens', () => {
    for (const v of ['', '0', 'false', 'no', 'off', 'maybe', '  ']) {
      assert.equal(getFileReadErrorOnBudget({ [FILE_READ_ERROR_ON_BUDGET_ENV]: v }), false, `"${v}" → off`);
    }
  });
});

describe('U026 applyReadTokenBudget (helper, pure)', () => {
  it('returns the output unchanged when within budget (either mode)', () => {
    for (const err of [false, true]) {
      const r = applyReadTokenBudget('hello', 3, 100, '.txt', err, '[trunc]');
      assert.equal(r.output, 'hello');
      assert.equal(r.isError, undefined);
    }
  });
  it('TRUNCATES with the note when over budget and errorOnBudget=false', () => {
    const big = 'x'.repeat(4000);
    const r = applyReadTokenBudget(big, 1000, 10, '.txt', false, '\n[NOTE]');
    assert.equal(r.isError, undefined);
    assert.ok(r.output.endsWith('\n[NOTE]'));
    assert.ok(r.output.length < big.length, 'content was cut to the budget');
  });
  it('ERRORS with the actionable message when over budget and errorOnBudget=true', () => {
    const big = 'x'.repeat(4000);
    const r = applyReadTokenBudget(big, 1000, 10, '.txt', true, '\n[NOTE]');
    assert.equal(r.isError, true);
    assert.match(r.output, /^Error: File content \(1000 tokens\) exceeds/);
    assert.match(r.output, /offset and limit|search for/);
    assert.ok(!r.output.includes('[NOTE]'), 'the truncation note is NOT emitted on the error path');
  });
});

describe('U026 fileReadTool over-budget behavior (integration)', () => {
  // A small file whose read output is well over a lowered token budget.
  function writeBig(name = 'big.txt'): string {
    const p = path.join(tmpDir, name);
    fs.writeFileSync(p, Array.from({ length: 80 }, (_, i) => `line ${i} ${'y'.repeat(40)}`).join('\n'));
    return p;
  }

  it('DEFAULT: silently truncates an over-budget full read (AMI differentiator, regression lock)', async () => {
    const p = writeBig();
    process.env[FILE_READ_MAX_TOKENS_ENV] = '10';
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, undefined);
    assert.match(res.output, /truncated: file exceeds 10 token budget/);
  });

  it('ERROR MODE: a full read over budget ERRORS with the offset/limit-or-search nudge', async () => {
    const p = writeBig();
    process.env[FILE_READ_MAX_TOKENS_ENV] = '10';
    process.env[FILE_READ_ERROR_ON_BUDGET_ENV] = '1';
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, true, 'over-budget read errors instead of truncating');
    assert.match(res.output, /exceeds maximum allowed tokens \(10\)/);
    assert.match(res.output, /offset and limit/);
    assert.match(res.output, /search for/);
    // The silent-truncation note must NOT appear, and no cyber-risk reminder is appended.
    assert.ok(!res.output.includes('truncated: file exceeds'), 'no truncation note on error');
    assert.ok(!res.output.includes('<system-reminder>'), 'error short-circuits later appends');
  });

  it('ERROR MODE: an over-budget RANGE read errors', async () => {
    const p = writeBig();
    process.env[FILE_READ_MAX_TOKENS_ENV] = '10';
    process.env[FILE_READ_ERROR_ON_BUDGET_ENV] = '1';
    const res = await fileReadTool.execute({ file_path: p, offset: 0, limit: 80 }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /exceeds maximum allowed tokens/);
  });

  it('ERROR MODE: an over-budget TAIL read errors', async () => {
    const p = writeBig();
    process.env[FILE_READ_MAX_TOKENS_ENV] = '10';
    process.env[FILE_READ_ERROR_ON_BUDGET_ENV] = '1';
    const res = await fileReadTool.execute({ file_path: p, tail: 80 }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /exceeds maximum allowed tokens/);
  });

  it('ERROR MODE: an over-budget NOTEBOOK read errors', async () => {
    const p = path.join(tmpDir, 'nb.ipynb');
    const cells = Array.from({ length: 40 }, (_, i) => ({
      cell_type: 'code',
      source: [`print(${i}) # ${'z'.repeat(60)}`],
      outputs: [],
    }));
    fs.writeFileSync(p, JSON.stringify({ cells, metadata: {} }));
    process.env[FILE_READ_MAX_TOKENS_ENV] = '10';
    process.env[FILE_READ_ERROR_ON_BUDGET_ENV] = '1';
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /exceeds maximum allowed tokens/);
  });

  it('ERROR MODE: a within-budget read is unaffected (no false positive)', async () => {
    const p = path.join(tmpDir, 'tiny.txt');
    fs.writeFileSync(p, 'one\ntwo\nthree\n');
    process.env[FILE_READ_ERROR_ON_BUDGET_ENV] = '1';
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, undefined);
    assert.match(res.output, /three/);
  });
});
