// ===========================================================================
// model-advisor-support-u051.test.ts
//
// Unit U051 — per-model capability gate for a secondary advisor/architect model.
//
// Mirrors the reference's modelSupportsAdvisor
// (references/claude/claude-code-main/src/utils/advisor.ts:L88-L96) and the
// no-arg advisor status "(inactive)" state gated on the current base model
// (references/claude/claude-code-main/src/commands/advisor.ts:L31-L36):
// the secondary model only runs when the current MAIN-LOOP model is
// advisor-capable (Opus/Sonnet 4.6+). AMI applies the same version-aware family
// match so a non-capable base model reports its configured architect model as
// INACTIVE instead of silently doing nothing.
//
// Gaps covered (all suggestedAmiTarget core/core/src/model-capabilities.ts):
//  - commands/advisor.ts "configured but inactive state gated on the current base mode"
//  - commands/advisor.ts "base model capability gate inactive state for the secondary"
//  - utils/advisor.ts "per model capability gating for advisor support modelSupportsAdvisor"
// ===========================================================================
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { modelSupportsAdvisor } from '../src/model-capabilities';

describe('modelSupportsAdvisor — per-model advisor-capability gate (U051)', () => {
  it('is true for the Opus/Sonnet 4.6+ class and the 5.x line', () => {
    assert.equal(modelSupportsAdvisor('claude-opus-4-6'), true);
    assert.equal(modelSupportsAdvisor('claude-sonnet-4-6'), true);
    assert.equal(modelSupportsAdvisor('claude-opus-4-6-20251101'), true);
    assert.equal(modelSupportsAdvisor('claude-sonnet-4-7'), true);
    assert.equal(modelSupportsAdvisor('claude-opus-5'), true);
    assert.equal(modelSupportsAdvisor('claude-sonnet-5-5'), true);
    // unanchored — gateway prefixes still resolve the family/version
    assert.equal(modelSupportsAdvisor('openrouter/anthropic/claude-opus-4-6'), true);
  });

  it('is false for pre-4.6 Opus/Sonnet (base model cannot drive an advisor)', () => {
    assert.equal(modelSupportsAdvisor('claude-opus-4-5'), false);
    assert.equal(modelSupportsAdvisor('claude-opus-4-1'), false);
    assert.equal(modelSupportsAdvisor('claude-opus-4'), false);
    assert.equal(modelSupportsAdvisor('claude-sonnet-4-5'), false);
    assert.equal(modelSupportsAdvisor('claude-sonnet-4'), false);
    assert.equal(modelSupportsAdvisor('claude-3-5-sonnet'), false);
  });

  it('is false for Haiku, non-Claude ids, and empty/non-string input', () => {
    assert.equal(modelSupportsAdvisor('claude-haiku-4-6'), false);
    assert.equal(modelSupportsAdvisor('gpt-6'), false);
    assert.equal(modelSupportsAdvisor('gemini-3'), false);
    assert.equal(modelSupportsAdvisor(''), false);
    // @ts-expect-error — exercise the non-string guard
    assert.equal(modelSupportsAdvisor(undefined), false);
    // @ts-expect-error — exercise the non-string guard
    assert.equal(modelSupportsAdvisor(null), false);
  });
});
