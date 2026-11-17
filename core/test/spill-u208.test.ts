import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  resolveTaskMaxChars,
  DEFAULT_TASK_MAX_CHARS,
  TASK_MAX_OUTPUT_UPPER_LIMIT,
  spillToolText,
} from '../src/tools/spill-output';
import { resolveBashMaxOutput, BASH_MAX_OUTPUT_UPPER_LIMIT } from '../src/tools/bash';
import { isSafeEnvVar } from '../src/env-protection';

// ---------------------------------------------------------------------------
// U208 gap-7: always-on default cap on subagent/task output
// (parity: Claude Code getMaxTaskOutputLength / TASK_MAX_OUTPUT_DEFAULT)
// ---------------------------------------------------------------------------
describe('U208 resolveTaskMaxChars — always-on bounded task cap', () => {
  const SAVE = process.env.TASK_MAX_OUTPUT_LENGTH;
  afterEach(() => {
    if (SAVE === undefined) delete process.env.TASK_MAX_OUTPUT_LENGTH;
    else process.env.TASK_MAX_OUTPUT_LENGTH = SAVE;
  });

  it('applies the built-in default when no budget and no env var are set', () => {
    delete process.env.TASK_MAX_OUTPUT_LENGTH;
    // The core regression: subagent output must be bounded out of the box. The old
    // code returned `undefined`/verbatim with no config; this must now be a finite cap.
    assert.equal(resolveTaskMaxChars(undefined), DEFAULT_TASK_MAX_CHARS);
    assert.equal(DEFAULT_TASK_MAX_CHARS, 32_000);
  });

  it('honors a valid TASK_MAX_OUTPUT_LENGTH env override', () => {
    process.env.TASK_MAX_OUTPUT_LENGTH = '5000';
    assert.equal(resolveTaskMaxChars(undefined), 5000);
  });

  it('caps an absurd TASK_MAX_OUTPUT_LENGTH at the enforced upper bound', () => {
    process.env.TASK_MAX_OUTPUT_LENGTH = '99999999';
    assert.equal(resolveTaskMaxChars(undefined), TASK_MAX_OUTPUT_UPPER_LIMIT);
  });

  it('falls back to the default on an invalid/non-positive env value', () => {
    process.env.TASK_MAX_OUTPUT_LENGTH = 'nonsense';
    assert.equal(resolveTaskMaxChars(undefined), DEFAULT_TASK_MAX_CHARS);
    process.env.TASK_MAX_OUTPUT_LENGTH = '-10';
    assert.equal(resolveTaskMaxChars(undefined), DEFAULT_TASK_MAX_CHARS);
  });

  it('lets an explicit positive budget win over env/default', () => {
    process.env.TASK_MAX_OUTPUT_LENGTH = '5000';
    assert.equal(resolveTaskMaxChars(12_000), 12_000);
  });

  it('clamps an explicit over-large budget to the upper bound', () => {
    delete process.env.TASK_MAX_OUTPUT_LENGTH;
    assert.equal(resolveTaskMaxChars(5_000_000), TASK_MAX_OUTPUT_UPPER_LIMIT);
  });

  it('ignores a non-positive/non-finite explicit budget and falls through', () => {
    delete process.env.TASK_MAX_OUTPUT_LENGTH;
    assert.equal(resolveTaskMaxChars(0), DEFAULT_TASK_MAX_CHARS);
    assert.equal(resolveTaskMaxChars(-1), DEFAULT_TASK_MAX_CHARS);
    assert.equal(resolveTaskMaxChars(Number.NaN), DEFAULT_TASK_MAX_CHARS);
  });

  it('registers TASK_MAX_OUTPUT_LENGTH as a safe (non-stripped) tuning knob', () => {
    // The knob is useless if env-protection strips it before the tool reads it.
    assert.equal(isSafeEnvVar('TASK_MAX_OUTPUT_LENGTH'), true);
  });
});

// ---------------------------------------------------------------------------
// U208 gap-7 E2E: the resolved cap actually bounds a chatty subagent transcript.
// This mirrors the real task.ts path: resolveTaskMaxChars -> spillToolText.
// ---------------------------------------------------------------------------
describe('U208 task output is bounded end-to-end with no config', () => {
  let tmpDir: string;
  const SAVE = process.env.TASK_MAX_OUTPUT_LENGTH;
  afterEach(() => {
    if (SAVE === undefined) delete process.env.TASK_MAX_OUTPUT_LENGTH;
    else process.env.TASK_MAX_OUTPUT_LENGTH = SAVE;
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('truncates an oversized transcript using the always-on default', () => {
    delete process.env.TASK_MAX_OUTPUT_LENGTH;
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u208-task-'));
    const prevData = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = tmpDir;
    try {
      // A chatty subagent returning ~100k chars with NO budget/env configured.
      const transcript = 'x'.repeat(100_000);
      const cap = resolveTaskMaxChars(undefined); // what task.ts now always computes
      const out = spillToolText(tmpDir, 'task', transcript, cap);
      // Old behavior (uncapped) returned the full 100k verbatim; now it is bounded.
      assert.ok(out.length < transcript.length, 'transcript must be bounded, not verbatim');
      assert.ok(out.length <= cap + 500, 'bounded within the cap + banner');
      assert.match(out, /persisted to .+ — truncated/);
    } finally {
      if (prevData === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
      else process.env.SUPERINFERENCE_DATA_DIR = prevData;
    }
  });
});

// ---------------------------------------------------------------------------
// U208 gap-6: bash output cap gains an enforced upper bound (DoS guard) and
// consolidates on the canonical validateBoundedIntEnvVar.
// ---------------------------------------------------------------------------
describe('U208 resolveBashMaxOutput — enforced upper bound', () => {
  const SAVE = process.env.BASH_MAX_OUTPUT_LENGTH;
  afterEach(() => {
    if (SAVE === undefined) delete process.env.BASH_MAX_OUTPUT_LENGTH;
    else process.env.BASH_MAX_OUTPUT_LENGTH = SAVE;
  });

  it('uses the generous default when the knob is unset', () => {
    delete process.env.BASH_MAX_OUTPUT_LENGTH;
    assert.equal(resolveBashMaxOutput(), BASH_MAX_OUTPUT_UPPER_LIMIT);
  });

  it('honors a valid in-range value verbatim', () => {
    process.env.BASH_MAX_OUTPUT_LENGTH = '12345';
    assert.equal(resolveBashMaxOutput(), 12345);
  });

  it('caps an absurd value at the enforced upper bound (DoS guard)', () => {
    process.env.BASH_MAX_OUTPUT_LENGTH = '999999999';
    // The core regression: the old code returned 999999999 verbatim.
    assert.equal(resolveBashMaxOutput(), BASH_MAX_OUTPUT_UPPER_LIMIT);
  });

  it('falls back to the default on an invalid value', () => {
    process.env.BASH_MAX_OUTPUT_LENGTH = 'not-a-number';
    assert.equal(resolveBashMaxOutput(), BASH_MAX_OUTPUT_UPPER_LIMIT);
  });
});
