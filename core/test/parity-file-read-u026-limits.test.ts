import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  getFileReadingLimits,
  DEFAULT_FILE_READ_MAX_TOKENS,
  DEFAULT_FILE_READ_MAX_SIZE_BYTES,
  FILE_READ_MAX_TOKENS_ENV,
  FILE_READ_MAX_SIZE_BYTES_ENV,
} from '../src/tools/file-read-limits';
import { fileReadTool } from '../src/tools/file-read';
import { getFileCache } from '../src/file-cache';
import * as logger from '../src/logger';
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
  tmpDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-fr-u026-lim-')));
  getFileCache(tmpDir).clear();
  delete process.env[FILE_READ_MAX_TOKENS_ENV];
  delete process.env[FILE_READ_MAX_SIZE_BYTES_ENV];
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
  delete process.env[FILE_READ_MAX_TOKENS_ENV];
  delete process.env[FILE_READ_MAX_SIZE_BYTES_ENV];
  mock.restoreAll();
});

// ---------------------------------------------------------------------------
// GAP (limits.ts): pure resolver — defaults, env override + validation,
// per-field defensive validation of a configurable limits object, precedence.
// ---------------------------------------------------------------------------
describe('U026 getFileReadingLimits (pure resolver)', () => {
  it('returns the hardcoded defaults with no overrides and a clean env', () => {
    const l = getFileReadingLimits(undefined, {});
    assert.equal(l.maxTokens, DEFAULT_FILE_READ_MAX_TOKENS);
    assert.equal(l.maxSizeBytes, DEFAULT_FILE_READ_MAX_SIZE_BYTES);
  });

  it('honors a valid env override (positive integer) for both caps', () => {
    const l = getFileReadingLimits(undefined, {
      [FILE_READ_MAX_TOKENS_ENV]: '500',
      [FILE_READ_MAX_SIZE_BYTES_ENV]: '4096',
    });
    assert.equal(l.maxTokens, 500);
    assert.equal(l.maxSizeBytes, 4096);
  });

  it('rejects malformed/zero/negative env values and falls through to default', () => {
    for (const bad of ['', 'abc', '0', '-5', '  ', 'NaN']) {
      const l = getFileReadingLimits(undefined, { [FILE_READ_MAX_TOKENS_ENV]: bad });
      assert.equal(l.maxTokens, DEFAULT_FILE_READ_MAX_TOKENS, `env="${bad}" must fall through`);
    }
  });

  it('accepts a valid per-call override object', () => {
    const l = getFileReadingLimits({ maxTokens: 123, maxSizeBytes: 777 }, {});
    assert.equal(l.maxTokens, 123);
    assert.equal(l.maxSizeBytes, 777);
  });

  it('per-field defensive validation: 0/negative/NaN/Infinity/non-number override → default (no zero/NaN cap)', () => {
    const bads: unknown[] = [0, -1, NaN, Infinity, -Infinity, '5', null, {}, undefined];
    for (const bad of bads) {
      const l = getFileReadingLimits(
        { maxTokens: bad as number, maxSizeBytes: bad as number },
        {},
      );
      assert.equal(l.maxTokens, DEFAULT_FILE_READ_MAX_TOKENS, `maxTokens override ${String(bad)} rejected`);
      assert.equal(l.maxSizeBytes, DEFAULT_FILE_READ_MAX_SIZE_BYTES, `maxSizeBytes override ${String(bad)} rejected`);
      assert.ok(l.maxTokens > 0 && Number.isFinite(l.maxTokens));
      assert.ok(l.maxSizeBytes > 0 && Number.isFinite(l.maxSizeBytes));
    }
  });

  it('env override wins over a per-call override (precedence: env > override > default)', () => {
    const l = getFileReadingLimits(
      { maxTokens: 10, maxSizeBytes: 10 },
      { [FILE_READ_MAX_TOKENS_ENV]: '9999', [FILE_READ_MAX_SIZE_BYTES_ENV]: '8888' },
    );
    assert.equal(l.maxTokens, 9999);
    assert.equal(l.maxSizeBytes, 8888);
  });

  it('a field with no env and an invalid override independently falls to default while the other honors env', () => {
    const l = getFileReadingLimits(
      { maxTokens: 42 },
      { [FILE_READ_MAX_SIZE_BYTES_ENV]: '4096' },
    );
    assert.equal(l.maxTokens, 42); // valid override, no env
    assert.equal(l.maxSizeBytes, 4096); // env, no override
  });
});

// ---------------------------------------------------------------------------
// INTEGRATION: the env token override truncates a read the default would not.
// RED before wiring: file-read.ts used a hardcoded MAX_FILE_READ_TOKENS=25000,
// so a tiny file was never truncated regardless of the env knob.
// ---------------------------------------------------------------------------
describe('U026 file_read token-budget override (integration)', () => {
  it('env AMI_FILE_READ_MAX_OUTPUT_TOKENS truncates a small file the default keeps whole', async () => {
    const p = path.join(tmpDir, 'small.txt');
    const body = Array.from({ length: 60 }, (_, i) => `line ${i} xxxxxxxxxxxxxxxxxxxx`).join('\n');
    fs.writeFileSync(p, body);

    // Default budget: not truncated.
    const whole = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(whole.isError, undefined);
    assert.ok(!whole.output.includes('truncated: file exceeds'), 'default budget keeps the small file whole');

    // Lowered env budget: truncated.
    process.env[FILE_READ_MAX_TOKENS_ENV] = '10';
    getFileCache(tmpDir).clear();
    const capped = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(capped.isError, undefined);
    assert.ok(
      capped.output.includes('truncated: file exceeds 10 token budget'),
      'env-lowered budget truncates and names the effective cap',
    );
  });

  it('per-call context outputBudget.fileReadMaxTokens truncates when no env is set', async () => {
    const p = path.join(tmpDir, 'ctx.txt');
    fs.writeFileSync(p, Array.from({ length: 60 }, (_, i) => `row ${i} yyyyyyyyyyyyyyyy`).join('\n'));
    const res = await fileReadTool.execute(
      { file_path: p },
      ctx({ outputBudget: { fileReadMaxTokens: 10 } }),
    );
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('truncated: file exceeds 10 token budget'));
  });

  it('env token override beats a per-call context override (precedence end-to-end)', async () => {
    const p = path.join(tmpDir, 'prec.txt');
    fs.writeFileSync(p, Array.from({ length: 60 }, (_, i) => `z ${i} zzzzzzzzzzz`).join('\n'));
    process.env[FILE_READ_MAX_TOKENS_ENV] = '100000'; // very high: nothing truncated
    const res = await fileReadTool.execute(
      { file_path: p },
      ctx({ outputBudget: { fileReadMaxTokens: 5 } }), // would truncate if it won
    );
    assert.equal(res.isError, undefined);
    assert.ok(!res.output.includes('truncated: file exceeds'), 'env (100000) overrides the context cap (5)');
  });
});

// ---------------------------------------------------------------------------
// INTEGRATION: the maxSizeBytes pre-read gate is overridable (per-context).
// ---------------------------------------------------------------------------
describe('U026 file_read pre-read size gate override (integration)', () => {
  it('context fileReadMaxSizeBytes trips the partial-view gate below 256KB', async () => {
    const p = path.join(tmpDir, 'sizey.txt');
    // ~2KB file, well under the 256KB default but over a 100-byte override.
    fs.writeFileSync(p, Array.from({ length: 80 }, (_, i) => `content line number ${i}`).join('\n'));

    const whole = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(whole.isError, undefined);
    assert.ok(!whole.output.includes('PARTIAL VIEW'), 'default 256KB gate keeps a 2KB file whole');

    getFileCache(tmpDir).clear();
    const partial = await fileReadTool.execute(
      { file_path: p },
      ctx({ outputBudget: { fileReadMaxSizeBytes: 100 } }),
    );
    assert.equal(partial.isError, undefined);
    assert.ok(partial.output.includes('PARTIAL VIEW'), 'lowered size gate returns a partial first-slice view');
  });

  it('env AMI_FILE_READ_MAX_SIZE_BYTES also drives the size gate', async () => {
    const p = path.join(tmpDir, 'envsize.txt');
    fs.writeFileSync(p, Array.from({ length: 80 }, (_, i) => `env size line ${i}`).join('\n'));
    process.env[FILE_READ_MAX_SIZE_BYTES_ENV] = '100';
    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('PARTIAL VIEW'));
  });
});

// ---------------------------------------------------------------------------
// GAP (fileRead.ts:L81-85): transparency logging when a read follows a symlink.
// RED before fix: the symlink-validation branch read through the link silently
// with no debug record, so no "Reading through symlink" line was ever emitted.
// ---------------------------------------------------------------------------
describe('U026 symlink read transparency logging', () => {
  it('emits a debug "Reading through symlink" record when the read follows a symlink', async () => {
    const target = path.join(tmpDir, 'real.txt');
    fs.writeFileSync(target, 'hello from the real file\n');
    const link = path.join(tmpDir, 'alias.txt');
    fs.symlinkSync(target, link);

    const lines: string[] = [];
    mock.method(logger.fsHooks, 'appendFileSync', (_f: string, line: string) => {
      lines.push(String(line));
    });

    const res = await fileReadTool.execute({ file_path: link }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('hello from the real file'), 'read returns the target content');

    const symlinkLine = lines.find(l => l.includes('Reading through symlink'));
    assert.ok(symlinkLine, 'a symlink-read transparency line is logged');
    assert.ok(symlinkLine!.includes(link) && symlinkLine!.includes(target), 'line records link -> target');
  });

  it('does NOT emit a symlink line for a plain (non-symlink) read', async () => {
    const p = path.join(tmpDir, 'plain.txt');
    fs.writeFileSync(p, 'no symlink here\n');

    const lines: string[] = [];
    mock.method(logger.fsHooks, 'appendFileSync', (_f: string, line: string) => {
      lines.push(String(line));
    });

    const res = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(!lines.some(l => l.includes('Reading through symlink')), 'no symlink line for a regular file');
  });
});
