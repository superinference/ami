/**
 * U051 (w76) — model-capabilities parity additions:
 *  - getModelSwitchWarning: actionable `/compact` remediation command in the
 *    Critical/Warning branches (parity with references/claude .../components/TokenWarning.tsx:169).
 *  - getAvailableContextUpgrade: forward larger-context-variant descriptor
 *    {alias,name,multiplier} (parity with references/claude .../utils/model/contextWindowUpgradeCheck.ts:L9-L47).
 *  - deriveMaxThinkingBudget: a completion cap learned from a live /models payload
 *    (warmed from the on-disk cache) bounds the thinking budget, not just output
 *    (parity with the reference's getMaxThinkingTokensForModel deriving from the live cap).
 *  - lookupOutputTokenRange: longest-key-first specificity ordering so a newer minor
 *    version is not shadowed by a shorter family key regardless of table order
 *    (parity with sortForMatching, .../utils/model/modelCapabilities.ts:L53-L58).
 */
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  getModelSwitchWarning,
  getAvailableContextUpgrade,
  getModelCapabilities,
  getModelMaxOutputTokens,
  clearDynamicCapabilities,
  writeModelCapabilitiesCache,
  warmModelCapabilitiesFromCache,
} from '../src/model-capabilities';

// ---------------------------------------------------------------------------
// gap: actionable /compact remediation in getModelSwitchWarning
// ---------------------------------------------------------------------------
describe('U051(w76): getModelSwitchWarning surfaces a concrete /compact command', () => {
  it('names /compact in the Critical branch (tokens exceed target window)', () => {
    const warning = getModelSwitchWarning('claude-opus-4', 'deepseek-chat', 100_000);
    assert.ok(warning);
    assert.ok(warning.startsWith('Critical'));
    // Red-green discriminator: the pre-fix string ended "...before switching." with
    // no runnable command. The fix appends the exact affordance.
    assert.match(warning, /run \/compact first\./);
  });

  it('names /compact in the near-limit Warning branch', () => {
    const targetWindow = 64_000; // deepseek-chat window
    const warning = getModelSwitchWarning('claude-opus-4', 'deepseek-chat', targetWindow * 0.85);
    assert.ok(warning);
    assert.ok(warning.startsWith('Warning'));
    assert.match(warning, /run \/compact first\./);
  });

  it('leaves the informational Note branch without a command (no action needed)', () => {
    const warning = getModelSwitchWarning('claude-opus-4', 'deepseek-chat', 10_000);
    assert.ok(warning);
    assert.ok(warning.startsWith('Note'));
    assert.doesNotMatch(warning, /\/compact/);
  });
});

// ---------------------------------------------------------------------------
// gap: getAvailableContextUpgrade — forward larger-context descriptor + multiplier
// ---------------------------------------------------------------------------
describe('U051(w76): getAvailableContextUpgrade', () => {
  it('returns the opus [1m] descriptor for a 200k opus model (default 5x)', () => {
    const up = getAvailableContextUpgrade('claude-opus-4-8');
    assert.deepEqual(up, { alias: 'opus[1m]', name: 'Opus 1M', multiplier: 5 });
  });

  it('derives an exact multiplier from the resolved base window', () => {
    // 1M / 200k = 5x
    assert.equal(getAvailableContextUpgrade('claude-opus-4-8', 200_000)?.multiplier, 5);
    // 1M / 500k = 2x
    assert.equal(getAvailableContextUpgrade('claude-sonnet-4', 500_000)?.multiplier, 2);
    // 1M / 900k rounds to 1 but is floored at 2 (always a real gain to advertise)
    assert.equal(getAvailableContextUpgrade('claude-sonnet-4', 900_000)?.multiplier, 2);
  });

  it('names the variant after the family', () => {
    assert.equal(getAvailableContextUpgrade('claude-sonnet-4')?.name, 'Sonnet 1M');
    assert.equal(getAvailableContextUpgrade('claude-sonnet-4')?.alias, 'sonnet[1m]');
  });

  it('returns null when the model already carries the [1m] marker', () => {
    assert.equal(getAvailableContextUpgrade('opus[1m]'), null);
    assert.equal(getAvailableContextUpgrade('claude-opus-4-8[1m]'), null);
  });

  it('returns null when already on a >=1M window', () => {
    assert.equal(getAvailableContextUpgrade('claude-opus-5', 1_000_000), null);
  });

  it('returns null for a family with no 1M variant and for empty/non-string ids', () => {
    assert.equal(getAvailableContextUpgrade('claude-haiku-4-5'), null);
    assert.equal(getAvailableContextUpgrade('gpt-4o'), null);
    assert.equal(getAvailableContextUpgrade(''), null);
    assert.equal(getAvailableContextUpgrade(undefined), null);
    assert.equal(getAvailableContextUpgrade(null), null);
  });
});

// ---------------------------------------------------------------------------
// gap: a live/cached output cap bounds the thinking budget (not just output)
// ---------------------------------------------------------------------------
describe('U051(w76): a cached output cap bounds the derived thinking budget', () => {
  let dir: string;
  const PRIOR = process.env.SUPERINFERENCE_MODEL_CACHE_DIR;
  afterEach(() => {
    clearDynamicCapabilities();
    if (PRIOR === undefined) delete process.env.SUPERINFERENCE_MODEL_CACHE_DIR;
    else process.env.SUPERINFERENCE_MODEL_CACHE_DIR = PRIOR;
    if (dir) fs.rmSync(dir, { recursive: true, force: true });
  });

  it('caps maxThinkingBudget at (advertised output cap - 1) for a warmed Claude model', () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mcap-'));
    process.env.SUPERINFERENCE_MODEL_CACHE_DIR = dir;
    clearDynamicCapabilities();
    // Persist then warm an advertised output cap well BELOW the static 128k ladder.
    writeModelCapabilitiesCache(dir, [{ id: 'claude-opus-4-8', maxOutputTokens: 50_000 }]);
    const warmed = warmModelCapabilitiesFromCache(dir);
    assert.ok(warmed >= 1);
    const caps = getModelCapabilities('claude-opus-4-8');
    assert.ok(caps);
    // Red-green discriminator: without the fix deriveMaxThinkingBudget would use the
    // static ladder upperLimit (128k → 127999); the live cap now bounds it to 49999.
    assert.equal(caps.maxThinkingBudget, 49_999);
    // The output upperLimit is bound by the same learned cap.
    assert.equal(getModelMaxOutputTokens('claude-opus-4-8').upperLimit, 50_000);
  });
});

// ---------------------------------------------------------------------------
// gap: output-cap lookup is longest-key-first (structural specificity)
// ---------------------------------------------------------------------------
describe('U051(w76): output-cap lookup prefers the most specific id', () => {
  it('resolves a newer minor version to its own cap, not a shorter family prefix', () => {
    // `claude-opus-4-8` must resolve to the 128k-ceiling 4.8 row, never be shadowed
    // by the shorter `claude-opus-4` (4.0 → 32k) key regardless of table order.
    assert.equal(getModelMaxOutputTokens('claude-opus-4-8').upperLimit, 128_000);
    // The bare 4.0 id still resolves to the 32k row.
    assert.equal(getModelMaxOutputTokens('claude-opus-4').upperLimit, 32_000);
    // A dated snapshot of 3.5 sonnet keeps its 8192 cap, not the shorter 3-sonnet key.
    assert.equal(getModelMaxOutputTokens('claude-3-5-sonnet-20241022').upperLimit, 8_192);
  });
});
