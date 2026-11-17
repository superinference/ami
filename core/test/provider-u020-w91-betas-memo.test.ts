// ===========================================================================
// provider-u020-w91-betas-memo.test.ts
//
// Parity unit U020 / w91 — memoized per-(model, settings) Anthropic beta
// assembly with an explicit cache-clear (core/core/src/provider.ts).
//
// Gap: Claude Code memoizes beta assembly per model id (getAllModelBetas /
// getModelBetas, lodash memoize) and exposes clearBetasCaches() to invalidate it
// when provider/settings/experiment state changes (utils/betas.ts L234, L430-434,
// auth.ts L1240). AMI recomputed its beta set inline on every request with no
// memoization and no invalidation hook. This covers the added memoization data
// structure (computed once per model+settings, reused), the cachedModelBetaSetCount
// diagnostic, clearBetasCaches(), and — critically — that memoization did NOT
// change the computed result, ordering, or env-freshness.
//
// RED without the memo: cachedModelBetaSetCount / clearBetasCaches do not exist
// (import + type errors) and the "cache grows then clears" / "count stays bounded
// on repeat" assertions have no backing structure to satisfy.
//
// ESM-coverage-safe: no source-text assertions, no mocked globals, env saved +
// restored per block.
// ===========================================================================
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  computeAnthropicBetas,
  computeBedrockBetaExtraBody,
  clearBetasCaches,
  cachedModelBetaSetCount,
  INTERLEAVED_THINKING_BETA,
  TOKEN_EFFICIENT_TOOLS_BETA,
  STRUCTURED_OUTPUTS_BETA,
  PROMPT_CACHING_SCOPE_BETA,
  EXTENDED_CACHE_TTL_BETA,
  ONE_MILLION_CONTEXT_BETA,
  PROMPT_CACHE_SCOPE_ENV,
} from '../src/provider';

const BETA_ENV = [
  'ANTHROPIC_BETAS',
  'CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS',
  'SUPERINFERENCE_DISABLE_EXPERIMENTAL_BETAS',
  'DISABLE_INTERLEAVED_THINKING',
  'SUPERINFERENCE_DISABLE_INTERLEAVED_THINKING',
  'SUPERINFERENCE_TOKEN_EFFICIENT_TOOLS',
  'CLAUDE_CODE_TOKEN_EFFICIENT_TOOLS',
  'SUPERINFERENCE_DISABLE_1M_CONTEXT',
  'SUPERINFERENCE_STRUCTURED_OUTPUTS',
  'CLAUDE_CODE_STRUCTURED_OUTPUTS',
  PROMPT_CACHE_SCOPE_ENV,
] as const;

function saveEnv(): Record<string, string | undefined> {
  const saved: Record<string, string | undefined> = {};
  for (const k of BETA_ENV) { saved[k] = process.env[k]; delete process.env[k]; }
  return saved;
}
function restoreEnv(saved: Record<string, string | undefined>): void {
  for (const k of BETA_ENV) {
    if (saved[k] === undefined) delete process.env[k];
    else process.env[k] = saved[k];
  }
}

const SONNET = 'claude-sonnet-4-6';

describe('computeAnthropicBetas memoization (U020/w91)', () => {
  let saved: Record<string, string | undefined>;
  afterEach(() => { restoreEnv(saved); clearBetasCaches(); });

  it('memoizes the request-independent base set: repeat calls add ONE cache entry per (model,settings)', () => {
    saved = saveEnv();
    clearBetasCaches();
    assert.equal(cachedModelBetaSetCount(), 0);

    const first = computeAnthropicBetas(SONNET);
    assert.equal(cachedModelBetaSetCount(), 1, 'first call primes exactly one entry');

    // Repeat calls (incl. varying the per-request opts) reuse the SAME base entry.
    const second = computeAnthropicBetas(SONNET);
    computeAnthropicBetas(SONNET, { cacheTtl: '1h', includeCacheTtlBeta: true });
    computeAnthropicBetas(SONNET, { cacheTtl: '5m', includeCacheTtlBeta: true });
    assert.equal(cachedModelBetaSetCount(), 1, 'opts do not create new base entries');
    assert.deepEqual(second, first);
  });

  it('keys the cache by model: a different model id adds a new entry', () => {
    saved = saveEnv();
    clearBetasCaches();
    computeAnthropicBetas(SONNET);
    computeAnthropicBetas('claude-opus-4-1');
    assert.equal(cachedModelBetaSetCount(), 2);
  });

  it('clearBetasCaches() drops every memoized entry', () => {
    saved = saveEnv();
    clearBetasCaches();
    computeAnthropicBetas(SONNET);
    computeAnthropicBetas('claude-opus-4-1');
    assert.ok(cachedModelBetaSetCount() >= 2);
    clearBetasCaches();
    assert.equal(cachedModelBetaSetCount(), 0);
  });

  it('is env-fresh: a settings change yields a NEW result without an explicit clear (no stale entry served)', () => {
    saved = saveEnv();
    clearBetasCaches();

    assert.ok(!computeAnthropicBetas(SONNET).includes(TOKEN_EFFICIENT_TOOLS_BETA));
    // Flip an env knob WITHOUT clearing — the fingerprint changes, so the memo
    // must recompute rather than serve the stale (no-token) entry.
    process.env.SUPERINFERENCE_TOKEN_EFFICIENT_TOOLS = '1';
    assert.ok(computeAnthropicBetas(SONNET).includes(TOKEN_EFFICIENT_TOOLS_BETA));
    // Two distinct settings generations are now cached for the same model.
    assert.equal(cachedModelBetaSetCount(), 2);
  });

  it('memoization preserves the exact computed set + ordering (no behavior change)', () => {
    saved = saveEnv();
    clearBetasCaches();
    process.env.SUPERINFERENCE_TOKEN_EFFICIENT_TOOLS = '1';
    process.env[PROMPT_CACHE_SCOPE_ENV] = 'global';
    process.env.ANTHROPIC_BETAS = 'custom-op-beta-2026';

    const betas = computeAnthropicBetas(SONNET, { cacheTtl: '1h', includeCacheTtlBeta: true });
    // Extended-cache (request-specific) must lead; base order: scope, 1M-context,
    // interleaved, token-efficient, then the operator escape-hatch beta.
    assert.deepEqual(betas, [
      EXTENDED_CACHE_TTL_BETA,
      PROMPT_CACHING_SCOPE_BETA,
      ONE_MILLION_CONTEXT_BETA,
      INTERLEAVED_THINKING_BETA,
      TOKEN_EFFICIENT_TOOLS_BETA,
      'custom-op-beta-2026',
    ]);
    // No duplicates even across repeated calls that reuse the cached base.
    const again = computeAnthropicBetas(SONNET, { cacheTtl: '1h', includeCacheTtlBeta: true });
    assert.deepEqual(again, betas);
  });

  it('disableBetas short-circuits before touching the cache', () => {
    saved = saveEnv();
    clearBetasCaches();
    assert.deepEqual(computeAnthropicBetas(SONNET, { disableBetas: true }), []);
    assert.equal(cachedModelBetaSetCount(), 0, 'kill-switch path never primes the cache');
  });

  it('structured-outputs gating still flows through the memoized base', () => {
    saved = saveEnv();
    clearBetasCaches();
    process.env.SUPERINFERENCE_STRUCTURED_OUTPUTS = '1';
    const betas = computeAnthropicBetas('claude-opus-4-1');
    assert.ok(betas.includes(STRUCTURED_OUTPUTS_BETA));
  });

  it('computeBedrockBetaExtraBody reuses the memoized base (shares one cache entry)', () => {
    saved = saveEnv();
    clearBetasCaches();
    const body = computeBedrockBetaExtraBody('claude-opus-4-1');
    assert.ok(body && Array.isArray(body.anthropic_beta));
    // The header path for the same model+settings must hit the already-primed entry.
    computeAnthropicBetas('claude-opus-4-1');
    assert.equal(cachedModelBetaSetCount(), 1);
  });

  it('a mutation of the returned array does not corrupt the cached base', () => {
    saved = saveEnv();
    clearBetasCaches();
    const first = computeAnthropicBetas(SONNET);
    first.push('tampered-beta');
    const second = computeAnthropicBetas(SONNET);
    assert.ok(!second.includes('tampered-beta'), 'cache must be isolated from caller mutation');
  });
});
