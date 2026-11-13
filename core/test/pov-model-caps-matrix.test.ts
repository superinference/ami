import { describe, it, before, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  getModelCapabilities,
  parseClaudeVersion,
  claudeReasoningCapabilities,
  clearDynamicCapabilities,
  type ThinkingLevel,
} from '../src/model-capabilities';
import { buildThinkingOptions } from '../src/provider';

// ---------------------------------------------------------------------------
// POV: EXHAUSTIVE guard for the #24 class (adaptive vs legacy thinking).
//
// The single invariant under test — adaptive thinking is supported IFF the
// Claude version is major>=5 OR (major==4 AND minor>=6). Pre-4.6 Opus/Sonnet
// (incl. Vertex sonnet-4-5 snapshots) MUST stay on the legacy
// { type:'enabled', budget_tokens } shape or they regress to an HTTP-400
// ("adaptive thinking is not supported on this model"). We compute every
// expectation from that first-principles rule, never guess, and drive the real
// functions on a big model-id matrix.
// ---------------------------------------------------------------------------

/** First-principles classifier — the spec, not the implementation. */
function expectAdaptive(major: number, minor: number): boolean {
  return major >= 5 || (major === 4 && minor >= 6);
}

// The dynamic capability registry is a module-global Map consulted FIRST by
// getModelCapabilities. Nothing registers it in this suite, but clear it so a
// stray registration from another file loaded in the same process can never
// pollute these assertions.
before(() => clearDynamicCapabilities());
beforeEach(() => clearDynamicCapabilities());

const THINK_HIGH = { enabled: true, level: 'high' as ThinkingLevel };

// Helper: reach into the anthropic providerOptions block (or undefined).
function anthropicThinking(modelId: string, thinking = THINK_HIGH): Record<string, unknown> | undefined {
  const opts = buildThinkingOptions(modelId, thinking) as {
    providerOptions?: { anthropic?: { thinking?: Record<string, unknown>; effort?: unknown } };
  };
  return opts.providerOptions?.anthropic;
}

describe('pov-model-caps-matrix: parseClaudeVersion family/major/minor + date safety', () => {
  const cases: Array<[string, { family: string; major: number; minor: number } | null]> = [
    ['claude-opus-4', { family: 'opus', major: 4, minor: 0 }],
    ['claude-opus-4-1', { family: 'opus', major: 4, minor: 1 }],
    ['claude-opus-4-5', { family: 'opus', major: 4, minor: 5 }],
    ['claude-opus-4-6', { family: 'opus', major: 4, minor: 6 }],
    ['claude-opus-4-7', { family: 'opus', major: 4, minor: 7 }],
    ['claude-opus-4-8', { family: 'opus', major: 4, minor: 8 }],
    ['claude-sonnet-4', { family: 'sonnet', major: 4, minor: 0 }],
    ['claude-sonnet-4-5', { family: 'sonnet', major: 4, minor: 5 }],
    ['claude-sonnet-4-6', { family: 'sonnet', major: 4, minor: 6 }],
    ['claude-opus-5', { family: 'opus', major: 5, minor: 0 }],
    ['claude-opus-5-5', { family: 'opus', major: 5, minor: 5 }],
    ['claude-sonnet-5', { family: 'sonnet', major: 5, minor: 0 }],
    ['claude-sonnet-5-5', { family: 'sonnet', major: 5, minor: 5 }],
    ['claude-fable-5-2', { family: 'fable', major: 5, minor: 2 }],
    ['claude-haiku-4-5', { family: 'haiku', major: 4, minor: 5 }],
    // Dated snapshots — the 8-digit date segment must NOT be read as the minor.
    ['claude-sonnet-4-5-20251022', { family: 'sonnet', major: 4, minor: 5 }],
    ['claude-sonnet-4-20250514', { family: 'sonnet', major: 4, minor: 0 }],
    ['claude-opus-4-6-20260101', { family: 'opus', major: 4, minor: 6 }],
    // @-dated snapshot form: the '5@20251022' segment is not a bare 1-2 digit
    // run, so the minor is NOT parsed from it (falls back to 0). Date-safe, and
    // we never misread the date as a huge minor.
    ['claude-sonnet-4-5@20251022', { family: 'sonnet', major: 4, minor: 0 }],
    // Provider/gateway-prefixed — the match is unanchored.
    ['openrouter/anthropic/claude-sonnet-4-5', { family: 'sonnet', major: 4, minor: 5 }],
    ['openrouter/anthropic/claude-sonnet-4-6', { family: 'sonnet', major: 4, minor: 6 }],
    ['vertex/claude-opus-5-5', { family: 'opus', major: 5, minor: 5 }],
    // Non-claude → null.
    ['gpt-4o', null],
    ['gemini-2.5-pro', null],
    ['o3', null],
    ['', null],
  ];

  for (const [id, expected] of cases) {
    it(`parses ${JSON.stringify(id)}`, () => {
      assert.deepEqual(parseClaudeVersion(id), expected);
    });
  }

  it('non-string input is null (no throw)', () => {
    assert.equal(parseClaudeVersion(undefined as unknown as string), null);
    assert.equal(parseClaudeVersion(null as unknown as string), null);
    assert.equal(parseClaudeVersion(42 as unknown as string), null);
  });

  it('a date segment is NEVER read as the minor version (Vertex regression guard)', () => {
    // The exact bug class: an 8-digit date looks like a huge minor. Assert the
    // minor stays the bare major (0) and never becomes the date.
    const v = parseClaudeVersion('claude-sonnet-4-20250514');
    assert.ok(v);
    assert.equal(v!.minor, 0);
    assert.notEqual(v!.minor, 20250514);
    // And the dated 4-5 snapshot keeps minor 5, not the date.
    const v2 = parseClaudeVersion('claude-sonnet-4-5-20251022');
    assert.equal(v2!.minor, 5);
  });
});

describe('pov-model-caps-matrix: adaptive classification (major>=5 OR (major==4 && minor>=6))', () => {
  // Opus/Sonnet reasoning models — each classified against the first-principles rule.
  const reasoning: Array<[string, number, number]> = [
    ['claude-opus-4', 4, 0],
    ['claude-opus-4-1', 4, 1],
    ['claude-opus-4-5', 4, 5],
    ['claude-opus-4-6', 4, 6],
    ['claude-opus-4-7', 4, 7],
    ['claude-opus-4-8', 4, 8],
    ['claude-sonnet-4', 4, 0],
    ['claude-sonnet-4-5', 4, 5],
    ['claude-sonnet-4-6', 4, 6],
    ['claude-opus-5', 5, 0],
    ['claude-opus-5-5', 5, 5],
    ['claude-sonnet-5', 5, 0],
    ['claude-sonnet-5-5', 5, 5],
    ['claude-sonnet-4-5-20251022', 4, 5],
    ['claude-sonnet-4-20250514', 4, 0],
    ['openrouter/anthropic/claude-sonnet-4-5', 4, 5],
    ['openrouter/anthropic/claude-sonnet-4-6', 4, 6],
  ];

  for (const [id, major, minor] of reasoning) {
    const want = expectAdaptive(major, minor);
    it(`${id} → supportsAdaptiveThinking === ${want}`, () => {
      const caps = claudeReasoningCapabilities(id);
      assert.ok(caps, `expected reasoning caps for ${id}`);
      assert.equal(caps!.reasoning, true);
      assert.equal(caps!.supportsAdaptiveThinking, want);
      assert.equal(caps!.requiresTemperatureOne, true);
      assert.equal(caps!.defaultThinkingLevel, 'medium');
      assert.equal(caps!.maxThinkingBudget, 128000);
      // getModelCapabilities must agree with claudeReasoningCapabilities.
      const viaGet = getModelCapabilities(id);
      assert.deepEqual(viaGet, caps);
    });
  }

  it('Vertex sonnet-4-5 (bare + dated + prefixed) is NEVER adaptive — the adaptive-400 guard', () => {
    for (const id of [
      'claude-sonnet-4-5',
      'claude-sonnet-4-5-20251022',
      'openrouter/anthropic/claude-sonnet-4-5',
      'claude-opus-4-5',
      'claude-opus-4',
      'claude-sonnet-4',
    ]) {
      assert.equal(
        claudeReasoningCapabilities(id)!.supportsAdaptiveThinking,
        false,
        `${id} must stay legacy (adaptive would 400)`,
      );
    }
  });

  it('non-reasoning Claude families (haiku, fable) return null caps', () => {
    for (const id of ['claude-haiku-4-5', 'claude-haiku-3-5', 'claude-fable-5-2', 'claude-mythos-6']) {
      assert.equal(claudeReasoningCapabilities(id), null, `${id} should be non-reasoning`);
      assert.equal(getModelCapabilities(id), null, `${id} should be non-reasoning via getModelCapabilities`);
    }
  });

  it('non-claude ids return null from claudeReasoningCapabilities', () => {
    for (const id of ['gpt-4o', 'gemini-2.5-pro', 'o3', '']) {
      assert.equal(claudeReasoningCapabilities(id), null);
    }
  });
});

describe('pov-model-caps-matrix: property sweep over major/minor', () => {
  it('every opus/sonnet major 3..7 × minor 0..9 obeys the adaptive rule', () => {
    for (const family of ['opus', 'sonnet']) {
      for (let major = 3; major <= 7; major++) {
        for (let minor = 0; minor <= 9; minor++) {
          const id = `claude-${family}-${major}-${minor}`;
          const caps = claudeReasoningCapabilities(id);
          assert.ok(caps, `expected caps for ${id}`);
          assert.equal(
            caps!.supportsAdaptiveThinking,
            expectAdaptive(major, minor),
            `${id} adaptive mismatch`,
          );
        }
      }
    }
  });
});

describe('pov-model-caps-matrix: buildThinkingOptions emits the correct shape', () => {
  it('ADAPTIVE models emit { type:"adaptive" } + effort, no budgetTokens', () => {
    for (const id of [
      'claude-opus-4-6',
      'claude-opus-4-7',
      'claude-opus-4-8',
      'claude-sonnet-4-6',
      'claude-opus-5',
      'claude-opus-5-5',
      'claude-sonnet-5',
      'claude-sonnet-5-5',
    ]) {
      const a = anthropicThinking(id);
      assert.ok(a, `${id} should emit anthropic providerOptions`);
      assert.deepEqual(a!.thinking, { type: 'adaptive' });
      assert.equal(a!.effort, 'high'); // level 'high' maps 1:1
      assert.ok(!('budgetTokens' in (a!.thinking as Record<string, unknown>)), `${id} adaptive must not carry budgetTokens`);
    }
  });

  it('adaptive effort maps 1:1 from the thinking level, defaulting to high', () => {
    const map: Array<[ThinkingLevel, string]> = [
      ['low', 'low'],
      ['medium', 'medium'],
      ['high', 'high'],
      ['max', 'max'],
      ['off', 'high'], // 'off' is not in the effort map → default 'high'
    ];
    for (const [level, effort] of map) {
      // level 'off' still passes enabled:true here to exercise the effort default.
      const a = anthropicThinking('claude-opus-5', { enabled: true, level });
      assert.equal(a!.effort, effort, `level ${level} → effort ${effort}`);
      assert.deepEqual(a!.thinking, { type: 'adaptive' });
    }
  });

  it('PRE-4.6 Opus/Sonnet emit legacy { type:"enabled", budgetTokens } with NO effort', () => {
    for (const id of [
      'claude-opus-4',
      'claude-opus-4-1',
      'claude-opus-4-5',
      'claude-sonnet-4',
      'claude-sonnet-4-5',
      'claude-sonnet-4-5-20251022',
      'claude-sonnet-4-20250514',
      'openrouter/anthropic/claude-sonnet-4-5',
    ]) {
      const a = anthropicThinking(id);
      assert.ok(a, `${id} should emit anthropic providerOptions`);
      const thinking = a!.thinking as { type: string; budgetTokens: number };
      assert.equal(thinking.type, 'enabled', `${id} must use legacy enabled shape`);
      assert.equal(typeof thinking.budgetTokens, 'number');
      assert.ok(
        thinking.budgetTokens >= 1024 && thinking.budgetTokens <= 24000,
        `${id} budgetTokens ${thinking.budgetTokens} out of [1024,24000]`,
      );
      assert.ok(!('effort' in a!), `${id} legacy shape must NOT carry effort`);
    }
  });

  it('legacy budgetTokens is clamped into [1024, 24000] across levels', () => {
    const id = 'claude-sonnet-4-5';
    const cases: Array<[ThinkingLevel, number]> = [
      ['off', 1024], // resolveThinkingBudget 0 → max(1024, min(0,24000)) = 1024
      ['low', 4096], // 4096 within range
      ['medium', 10240], // 10240 within range
      ['high', 24000], // 32768 → clamped to 24000
      ['max', 24000], // large → clamped to 24000
    ];
    for (const [level, expected] of cases) {
      const a = anthropicThinking(id, { enabled: true, level });
      const thinking = a!.thinking as { budgetTokens: number };
      assert.equal(thinking.budgetTokens, expected, `level ${level}`);
      assert.ok(thinking.budgetTokens >= 1024 && thinking.budgetTokens <= 24000);
    }
  });

  it('explicit budgetTokens is honored then clamped', () => {
    const id = 'claude-sonnet-4-5';
    // Huge explicit budget clamps to 24000.
    assert.equal(
      (anthropicThinking(id, { enabled: true, level: 'high', budgetTokens: 999999 })!.thinking as { budgetTokens: number }).budgetTokens,
      24000,
    );
    // Tiny explicit budget floors at 1024.
    assert.equal(
      (anthropicThinking(id, { enabled: true, level: 'high', budgetTokens: 1 })!.thinking as { budgetTokens: number }).budgetTokens,
      1024,
    );
    // In-range explicit budget passes through.
    assert.equal(
      (anthropicThinking(id, { enabled: true, level: 'high', budgetTokens: 5000 })!.thinking as { budgetTokens: number }).budgetTokens,
      5000,
    );
  });

  it('NON-reasoning models emit NOTHING odd (empty object, no anthropic block)', () => {
    for (const id of [
      'claude-haiku-4-5',
      'claude-fable-5-2',
      'gpt-4o',
      'gpt-4o-mini',
      'gpt-4-turbo',
    ]) {
      const opts = buildThinkingOptions(id, THINK_HIGH);
      assert.deepEqual(opts, {}, `${id} must emit {}`);
    }
  });

  it('thinking disabled → always {} even for adaptive/legacy models', () => {
    for (const id of ['claude-opus-5', 'claude-sonnet-4-5', 'gpt-4o', 'o3', 'gemini-2.5-pro']) {
      assert.deepEqual(buildThinkingOptions(id, { enabled: false, level: 'high' }), {});
      assert.deepEqual(buildThinkingOptions(id, undefined), {});
    }
  });

  it('non-claude reasoning models NEVER emit an anthropic adaptive/enabled block', () => {
    // o3 → openai reasoningEffort; gemini-2.5-pro → google thinkingConfig.
    // Neither must ever carry an anthropic.thinking block (that would be the #24 bug
    // leaking onto the wrong provider).
    for (const id of ['o3', 'o1', 'gpt-5.6-luna', 'gemini-2.5-pro', 'gemini-3']) {
      const opts = buildThinkingOptions(id, THINK_HIGH) as {
        providerOptions?: { anthropic?: unknown };
      };
      assert.equal(opts.providerOptions?.anthropic, undefined, `${id} must not emit anthropic options`);
    }
  });

  it('every ADAPTIVE id in the matrix never produces a legacy enabled block, and vice-versa', () => {
    const adaptiveIds = ['claude-opus-4-6', 'claude-opus-4-8', 'claude-sonnet-4-6', 'claude-opus-5', 'claude-sonnet-5-5'];
    const legacyIds = ['claude-opus-4', 'claude-opus-4-5', 'claude-sonnet-4', 'claude-sonnet-4-5'];
    for (const id of adaptiveIds) {
      const t = anthropicThinking(id)!.thinking as { type: string };
      assert.equal(t.type, 'adaptive', `${id} must be adaptive`);
    }
    for (const id of legacyIds) {
      const t = anthropicThinking(id)!.thinking as { type: string };
      assert.equal(t.type, 'enabled', `${id} must be legacy enabled`);
    }
  });
});
