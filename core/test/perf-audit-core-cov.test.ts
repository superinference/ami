// Coverage for the behavior-preserving performance-audit optimizations in core/:
// exercises the NEW branches/parameters added by those changes so the optimized
// paths are measured (not just the pre-existing fast paths).

import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { boundedHookAppend, MAX_HOOK_COMMAND_OUTPUT_BYTES } from '../src/hooks';
import { truncateContent } from '../src/analytics';
import { ProcessManager } from '../src/process-manager';

describe('perf-audit — boundedHookAppend incremental byte count', () => {
  it('reuses a caller-supplied currentBytes instead of re-measuring (below cap)', () => {
    // currentBytes provided → the function must NOT re-measure `current`; pass a
    // deliberately-wrong `current` to prove the supplied count is authoritative.
    const r = boundedHookAppend('IGNORED', 'xyz', 100, 10);
    assert.equal(r.text, 'IGNOREDxyz');
    assert.equal(r.truncated, false);
    assert.equal(r.bytes, 13); // 10 (supplied) + 3 (chunk)
  });

  it('short-circuits when the supplied currentBytes already meets the cap', () => {
    const r = boundedHookAppend('abc', 'more', 5, 5);
    assert.equal(r.text, 'abc');
    assert.equal(r.truncated, true);
    assert.equal(r.bytes, 5);
  });

  it('truncates the chunk at the remaining budget and reports resulting bytes', () => {
    const r = boundedHookAppend('', 'abcdef', 3, 0);
    assert.equal(r.text, 'abc');
    assert.equal(r.truncated, true);
    assert.equal(r.bytes, 3);
  });

  it('threads bytes forward across successive calls (streaming simulation)', () => {
    let text = '';
    let bytes = 0;
    for (const chunk of ['aa', 'bb', 'cc']) {
      const r = boundedHookAppend(text, chunk, MAX_HOOK_COMMAND_OUTPUT_BYTES, bytes);
      text = r.text; bytes = r.bytes;
    }
    assert.equal(text, 'aabbcc');
    assert.equal(bytes, 6);
  });
});

describe('perf-audit — truncateContent binary-search cut', () => {
  it('returns the value unchanged when within the byte cap', () => {
    const r = truncateContent('hello', 100);
    assert.equal(r.truncated, false);
    assert.equal(r.value, 'hello');
    assert.equal(r.originalLength, 5);
  });

  it('cuts multi-byte content on a byte boundary that stays <= maxSize', () => {
    // '€' is 3 UTF-8 bytes. With a 7-byte cap, at most 2 euro signs (6 bytes) fit.
    const s = '€€€€';
    const r = truncateContent(s, 7);
    assert.equal(r.truncated, true);
    assert.ok(Buffer.byteLength(r.value.replace(/…\[TRUNCATED.*$/, ''), 'utf8') <= 7);
    assert.ok(r.value.startsWith('€€'));
    assert.ok(r.value.includes('[TRUNCATED'));
    assert.equal(r.originalLength, 4);
  });

  it('matches the char-by-char reference cut for ASCII overflow', () => {
    const s = 'abcdefghij';
    const r = truncateContent(s, 4);
    assert.equal(r.value, 'abcd…[TRUNCATED 10 chars]');
  });
});

describe('perf-audit — getOutputTail bounded read', () => {
  let dir: string;
  let pm: ProcessManager;
  before(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'perf-got-')); pm = new ProcessManager(dir); });
  after(() => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ } });

  it('returns null for an unknown task', () => {
    assert.equal(pm.getOutputTail('nope', 0), null);
  });
});
