import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  resolveBashMaxOutput,
  resolveBashTimeout,
  resolveAssistantBlockingBudget,
  resolveBackgroundTimeoutMs,
} from '../src/tools/bash';

/**
 * U030 gap: route bash.ts's own numeric env knobs through the canonical
 * `validateBoundedIntEnvVar` so a mistaken/hostile value is CAPPED to a sane
 * upper limit (resource-exhaustion / hang DoS guard), not honored verbatim.
 * These assert the upper-limit cap (the invalid/NaN rejection is already locked
 * by bash-u030-w69/w70). Each cap case is RED before the fix (the old
 * `Number(raw)||DEFAULT` / `parseEnvMs` reads applied no ceiling).
 */

// Mirrors the module-private ceilings in bash.ts.
const ABSOLUTE_MAX_OUTPUT_LENGTH = 50_000_000;
const HARD_MAX_TIMEOUT_MS = 3_600_000; // 1 hour
const MAX_BG_TIMEOUT_MS = 2 * 60 * 60 * 1000; // 2 hours

describe('U030: bounded-int capping of bash env knobs', () => {
  const saved = {
    out: process.env.BASH_MAX_OUTPUT_LENGTH,
    maxT: process.env.BASH_MAX_TIMEOUT_MS,
    defT: process.env.BASH_DEFAULT_TIMEOUT_MS,
    budget: process.env.BASH_ASSISTANT_BLOCKING_BUDGET_MS,
    bg: process.env.SUPERINFERENCE_BACKGROUND_COMMAND_TIMEOUT_MS,
  };
  afterEach(() => {
    const restore = (k: string, v: string | undefined) => {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    };
    restore('BASH_MAX_OUTPUT_LENGTH', saved.out);
    restore('BASH_MAX_TIMEOUT_MS', saved.maxT);
    restore('BASH_DEFAULT_TIMEOUT_MS', saved.defT);
    restore('BASH_ASSISTANT_BLOCKING_BUDGET_MS', saved.budget);
    restore('SUPERINFERENCE_BACKGROUND_COMMAND_TIMEOUT_MS', saved.bg);
  });

  it('caps an out-of-range BASH_MAX_OUTPUT_LENGTH to the absolute ceiling', () => {
    // RED on revert: the old read returned Math.max(MIN, 999_999_999) verbatim,
    // forcing the tool to buffer ~1GB of output per call.
    process.env.BASH_MAX_OUTPUT_LENGTH = String(ABSOLUTE_MAX_OUTPUT_LENGTH + 1);
    assert.equal(resolveBashMaxOutput(), ABSOLUTE_MAX_OUTPUT_LENGTH);
  });

  it('still honors an in-range BASH_MAX_OUTPUT_LENGTH verbatim', () => {
    process.env.BASH_MAX_OUTPUT_LENGTH = '12345';
    assert.equal(resolveBashMaxOutput(), 12345);
  });

  it('caps BASH_MAX_TIMEOUT_MS to the 1-hour hard ceiling', () => {
    // RED on revert: parseEnvMs returned 10× the hard max, so a huge per-call
    // timeout clamped only to that inflated ceiling (≈10h blocking budget).
    process.env.BASH_MAX_TIMEOUT_MS = String(HARD_MAX_TIMEOUT_MS * 10);
    delete process.env.BASH_DEFAULT_TIMEOUT_MS;
    assert.equal(resolveBashTimeout(Number.MAX_SAFE_INTEGER), HARD_MAX_TIMEOUT_MS);
  });

  it('still honors an in-range BASH_MAX_TIMEOUT_MS as the per-call ceiling', () => {
    process.env.BASH_MAX_TIMEOUT_MS = '500';
    process.env.BASH_DEFAULT_TIMEOUT_MS = '100';
    assert.equal(resolveBashTimeout(99999), 500);
  });

  it('caps the assistant blocking budget to the hard ceiling', () => {
    // RED on revert: resolveAssistantBlockingBudget returned the raw huge value.
    process.env.BASH_ASSISTANT_BLOCKING_BUDGET_MS = String(HARD_MAX_TIMEOUT_MS * 5);
    assert.equal(resolveAssistantBlockingBudget(), HARD_MAX_TIMEOUT_MS);
  });

  it('returns undefined for an unset / invalid assistant blocking budget', () => {
    delete process.env.BASH_ASSISTANT_BLOCKING_BUDGET_MS;
    assert.equal(resolveAssistantBlockingBudget(), undefined);
    process.env.BASH_ASSISTANT_BLOCKING_BUDGET_MS = 'abc';
    assert.equal(resolveAssistantBlockingBudget(), undefined);
  });

  it('caps the background-command timeout env knob to the 2-hour ceiling', () => {
    process.env.SUPERINFERENCE_BACKGROUND_COMMAND_TIMEOUT_MS = String(MAX_BG_TIMEOUT_MS * 10);
    assert.equal(resolveBackgroundTimeoutMs(undefined), MAX_BG_TIMEOUT_MS);
  });
});
