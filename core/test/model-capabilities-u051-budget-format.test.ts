import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { formatTokenCount } from '../src/token-format';
import {
  getModelSwitchWarning,
  getModelCapabilities,
  claudeReasoningCapabilities,
  resolveThinkingBudget,
  getModelMaxOutputTokens,
  clearDynamicCapabilities,
} from '../src/model-capabilities';

// ---------------------------------------------------------------------------
// U051 — gap: "human readable token counts in user facing warnings"
// (references/claude/.../utils/format.ts). getModelSwitchWarning must render
// token/window figures via the canonical compact formatter, not raw integers.
// ---------------------------------------------------------------------------

describe('U051: formatTokenCount (canonical compact token formatter)', () => {
  it('renders compact notation with a fixed decimal and lowercase units', () => {
    assert.equal(formatTokenCount(900), '900');
    assert.equal(formatTokenCount(1_321), '1.3k');
    assert.equal(formatTokenCount(70_000), '70.0k');
    assert.equal(formatTokenCount(1_000_000), '1.0M');
    assert.equal(formatTokenCount(2_500_000_000), '2.5b');
  });

  it('degrades non-finite counts to "0" so a user never sees NaN/Infinity', () => {
    assert.equal(formatTokenCount(NaN), '0');
    assert.equal(formatTokenCount(Infinity), '0');
    assert.equal(formatTokenCount(-Infinity), '0');
  });
});

describe('U051: getModelSwitchWarning uses human-readable compact counts', () => {
  it('formats the critical warning figures compactly', () => {
    // opus-4 window 200k → deepseek-chat window 64k; 100k tokens exceed target.
    const warning = getModelSwitchWarning('claude-opus-4', 'deepseek-chat', 100_000);
    assert.ok(warning);
    assert.ok(warning.startsWith('Critical'));
    assert.ok(warning.includes(`${formatTokenCount(100_000)} tokens`)); // "100.0k tokens"
    assert.ok(warning.includes(formatTokenCount(64_000)));              // "64.0k"
    // No raw integer leaks into the user-facing string.
    assert.ok(!/\b100000\b/.test(warning));
    assert.ok(!/\b64000\b/.test(warning));
  });

  it('formats the near-limit warning figures compactly', () => {
    const warning = getModelSwitchWarning('claude-opus-4', 'deepseek-chat', 60_000);
    assert.ok(warning);
    assert.ok(warning.startsWith('Warning'));
    assert.ok(warning.includes(formatTokenCount(60_000)));
    assert.ok(!/\b60000\b/.test(warning));
  });

  it('formats the informational note figures compactly', () => {
    const warning = getModelSwitchWarning('claude-opus-4', 'deepseek-chat', 10_000);
    assert.ok(warning);
    assert.ok(warning.startsWith('Note'));
    assert.ok(warning.includes(formatTokenCount(64_000)));   // target window
    assert.ok(warning.includes(formatTokenCount(200_000)));  // current window
    assert.ok(!/\b200000\b/.test(warning));
  });
});

// ---------------------------------------------------------------------------
// U051 — gap: "thinking budget tied to the model's actual output cap"
// (references/claude/.../utils/context.ts getMaxThinkingTokensForModel).
// claudeReasoningCapabilities must derive maxThinkingBudget from the model's
// real output ceiling (upperLimit - 1) instead of a flat 128k for every
// Opus/Sonnet model.
// ---------------------------------------------------------------------------

describe('U051: thinking budget is tied to the model output ceiling', () => {
  beforeEach(() => clearDynamicCapabilities());

  it('derives maxThinkingBudget = output upperLimit - 1 for small-output families', () => {
    // Opus 4.0 caps output at 32k → a 128k thinking budget is impossible.
    const caps = claudeReasoningCapabilities('claude-opus-4');
    assert.ok(caps);
    assert.equal(getModelMaxOutputTokens('claude-opus-4').upperLimit, 32_000);
    assert.equal(caps!.maxThinkingBudget, 31_999);
    assert.notEqual(caps!.maxThinkingBudget, 128_000, 'must no longer be a flat 128k');
  });

  it('keeps a 128k-class budget for 128k-output flagships (no regression)', () => {
    for (const id of ['claude-opus-4-6', 'claude-opus-4-7', 'claude-opus-4-8',
                      'claude-opus-5', 'claude-opus-5-5', 'claude-sonnet-4-6',
                      'claude-sonnet-5']) {
      const caps = getModelCapabilities(id);
      assert.ok(caps, `${id} should be a reasoning model`);
      assert.equal(
        caps!.maxThinkingBudget,
        getModelMaxOutputTokens(id).upperLimit - 1,
        `${id} thinking budget must track its output ceiling`,
      );
      assert.equal(caps!.maxThinkingBudget, 127_999, `${id} should keep the 128k-class ceiling`);
    }
  });

  it('is strictly below the output cap for every Opus/Sonnet version', () => {
    for (const id of ['claude-opus-4', 'claude-opus-4-5', 'claude-opus-4-6',
                      'claude-sonnet-4', 'claude-sonnet-4-6', 'claude-opus-5']) {
      const caps = getModelCapabilities(id)!;
      assert.ok(
        caps.maxThinkingBudget < getModelMaxOutputTokens(id).upperLimit,
        `${id}: thinking must be strictly below the output cap`,
      );
    }
  });

  it('propagates the per-model ceiling through resolveThinkingBudget("max")', () => {
    // Opus 4.0 (32k output) must resolve to a far smaller max-thinking budget
    // than Opus 4.6 (128k output).
    const small = resolveThinkingBudget('max', 'claude-opus-4');
    const big = resolveThinkingBudget('max', 'claude-opus-4-6');
    assert.equal(small, 31_999 - 8192);
    assert.equal(big, 127_999 - 8192);
    assert.ok(small < big);
  });
});
