/**
 * U006 (w86) — core/core/src/types.ts parity additions.
 *
 * Covers:
 *  - LSP-recommendation ignored-count gating: MAX_LSP_RECOMMENDATION_IGNORED_COUNT,
 *    isLspRecommendationDisabled, and validate/sanitize of lspRecommendationIgnoredCount
 *    (parity: utils/plugins/lspRecommendation.ts).
 *  - shouldEnablePromptSuggestion default resolution (parity:
 *    services/PromptSuggestion/promptSuggestion.ts).
 *  - StreamingToolProgress schema carried on ToolResult (parity: Shell.ts/TaskOutput
 *    onProgress + PowerShell/BashModeProgress UI timer fields).
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  MAX_LSP_RECOMMENDATION_IGNORED_COUNT,
  isLspRecommendationDisabled,
  shouldEnablePromptSuggestion,
  validateEngineConfig,
  sanitizeEngineConfig,
} from '../src/types';
import type { EngineConfig, ToolResult, StreamingToolProgress } from '../src/types';
import { classifyToolResultState } from '../src/message-display';

function cfg(overrides: Partial<EngineConfig> = {}): EngineConfig {
  return {
    provider: { apiKey: 'k', baseUrl: 'http://localhost', model: 'm' },
    cwd: '/tmp',
    ...overrides,
  };
}

describe('LSP recommendation ignored-count gating', () => {
  it('MAX constant matches Claude (5)', () => {
    assert.equal(MAX_LSP_RECOMMENDATION_IGNORED_COUNT, 5);
  });

  it('isLspRecommendationDisabled: explicit off-switch wins', () => {
    assert.equal(isLspRecommendationDisabled({ lspRecommendationDisabled: true }), true);
    assert.equal(isLspRecommendationDisabled({ lspRecommendationDisabled: false }), false);
  });

  it('isLspRecommendationDisabled: auto-disables at/after MAX ignores', () => {
    assert.equal(isLspRecommendationDisabled({ lspRecommendationIgnoredCount: 4 }), false);
    assert.equal(isLspRecommendationDisabled({ lspRecommendationIgnoredCount: 5 }), true);
    assert.equal(isLspRecommendationDisabled({ lspRecommendationIgnoredCount: 9 }), true);
  });

  it('isLspRecommendationDisabled: undefined count defaults to 0 (enabled)', () => {
    assert.equal(isLspRecommendationDisabled({}), false);
  });

  it('validateEngineConfig flags a non-integer / negative ignored count', () => {
    assert.equal(validateEngineConfig(cfg({ lspRecommendationIgnoredCount: 3 })).valid, true);
    const neg = validateEngineConfig(cfg({ lspRecommendationIgnoredCount: -1 }));
    assert.equal(neg.valid, false);
    assert.ok(neg.warnings.some((w) => w.includes('lspRecommendationIgnoredCount')));
    const frac = validateEngineConfig(cfg({ lspRecommendationIgnoredCount: 2.5 }));
    assert.equal(frac.valid, false);
    assert.ok(frac.warnings.some((w) => w.includes('lspRecommendationIgnoredCount')));
  });

  it('sanitizeEngineConfig clamps negatives to 0 and rounds', () => {
    assert.equal(sanitizeEngineConfig(cfg({ lspRecommendationIgnoredCount: -4 })).lspRecommendationIgnoredCount, 0);
    assert.equal(sanitizeEngineConfig(cfg({ lspRecommendationIgnoredCount: 3.6 })).lspRecommendationIgnoredCount, 4);
    assert.equal(sanitizeEngineConfig(cfg({})).lspRecommendationIgnoredCount, undefined);
  });
});

describe('shouldEnablePromptSuggestion', () => {
  it('env override wins over everything', () => {
    assert.equal(shouldEnablePromptSuggestion({ envOverride: true, nonInteractive: true, configEnabled: false }), true);
    assert.equal(shouldEnablePromptSuggestion({ envOverride: false, nonInteractive: false, configEnabled: true }), false);
  });

  it('non-interactive sessions are off when no env override', () => {
    assert.equal(shouldEnablePromptSuggestion({ nonInteractive: true, configEnabled: true }), false);
  });

  it('config toggle decides when interactive, defaulting ON', () => {
    assert.equal(shouldEnablePromptSuggestion({ configEnabled: true }), true);
    assert.equal(shouldEnablePromptSuggestion({ configEnabled: false }), false);
    assert.equal(shouldEnablePromptSuggestion({}), true);
    assert.equal(shouldEnablePromptSuggestion(), true);
  });
});

describe('StreamingToolProgress on ToolResult', () => {
  it('carries the consolidated shell/task progress schema and does not change result state', () => {
    const progress: StreamingToolProgress = {
      lastLines: 'tail\n',
      allLines: 'head\ntail\n',
      totalLines: 2,
      totalBytes: 10,
      isIncomplete: false,
      elapsedTimeSeconds: 3,
      timeoutMs: 120_000,
    };
    const result: ToolResult = { output: 'done', progress };
    assert.equal(result.progress?.totalLines, 2);
    assert.equal(result.progress?.isIncomplete, false);
    assert.equal(result.progress?.elapsedTimeSeconds, 3);
    // Progress is purely informational — a non-error result stays 'success'.
    assert.equal(classifyToolResultState(result), 'success');
  });

  it('optional timer fields may be omitted (Shell/TaskOutput callback shape)', () => {
    const progress: StreamingToolProgress = {
      lastLines: '',
      allLines: '',
      totalLines: 0,
      totalBytes: 0,
      isIncomplete: true,
    };
    const result: ToolResult = { output: '', isError: true, progress };
    assert.equal(result.progress?.isIncomplete, true);
    assert.equal(result.progress?.elapsedTimeSeconds, undefined);
    assert.equal(classifyToolResultState(result), 'error');
  });
});
