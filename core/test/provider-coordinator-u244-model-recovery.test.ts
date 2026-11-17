import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { ProviderCoordinator } from '../src/provider-coordinator';
import type { ProviderConfig, ProviderSubsystem } from '../src/types';

// ---------------------------------------------------------------------------
// U244 — Model-recovery notification (fallback → primary).
//
// Claude Code announces BOTH directions of a capacity-driven model swap: the
// demotion to a fallback model AND the return to the primary once demand eases
// (query.ts recordError/recordSuccess recovery path). AMI already surfaces the
// demotion (engine emits provider_changed on recordError), but recordSuccess
// silently reverted to the primary with no signal. recordSuccess now returns a
// ModelRecovery describing the fallback → primary transition on the success that
// completes the cooldown, so the engine can emit the mirror notification.
// ---------------------------------------------------------------------------

function makeSubsystem(overrides?: Partial<ProviderSubsystem>): ProviderSubsystem {
  const primary: ProviderConfig = {
    baseUrl: 'https://api.openai.com/v1',
    apiKey: 'sk-test-key',
    model: 'gpt-4o',
  };
  return { primary, ...overrides };
}

describe('ProviderCoordinator — model-recovery notification (U244)', () => {
  it('returns null on success while still on the primary model', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: 'gpt-3.5-turbo' }));
    assert.equal(coord.recordSuccess(), null);
  });

  it('returns null for the successes that keep us on the fallback (cooldown not yet met)', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: 'gpt-3.5-turbo' }));
    coord.recordError();
    assert.equal(coord.getConfig().model, 'gpt-3.5-turbo');
    // Cooldown is 3; the first two successes do NOT recover yet.
    assert.equal(coord.recordSuccess(), null);
    assert.equal(coord.recordSuccess(), null);
    assert.equal(coord.getConfig().model, 'gpt-3.5-turbo');
  });

  it('returns the fallback → primary transition on the success that completes the cooldown', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: 'gpt-3.5-turbo' }));
    coord.recordError();
    coord.recordSuccess();
    coord.recordSuccess();
    const recovery = coord.recordSuccess();
    assert.deepEqual(recovery, { from: 'gpt-3.5-turbo', to: 'gpt-4o' });
    // And the coordinator is genuinely back on the primary.
    assert.equal(coord.getConfig().model, 'gpt-4o');
    assert.equal(coord.getState().currentModel, 'gpt-4o');
  });

  it('reports the multi-step fallback model it was actually on as the recovery source', () => {
    const coord = new ProviderCoordinator(makeSubsystem({
      fallbackModels: ['gpt-4o-mini', 'gpt-3.5-turbo'],
    }));
    coord.recordError(); // → gpt-4o-mini
    coord.recordError(); // → gpt-3.5-turbo
    assert.equal(coord.getConfig().model, 'gpt-3.5-turbo');
    coord.recordSuccess();
    coord.recordSuccess();
    const recovery = coord.recordSuccess();
    assert.deepEqual(recovery, { from: 'gpt-3.5-turbo', to: 'gpt-4o' });
  });

  it('never recovers (and so never emits) while the fallback is sticky', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: 'gpt-3.5-turbo' }));
    coord.setStickyFallback(true);
    coord.recordError();
    for (let i = 0; i < 10; i++) {
      assert.equal(coord.recordSuccess(), null);
    }
    assert.equal(coord.getConfig().model, 'gpt-3.5-turbo');
  });

  it("falls back to 'unknown' as the recovery target when the primary has no model id", () => {
    const sub = makeSubsystem({ fallbackModel: 'gpt-3.5-turbo' });
    sub.primary.model = undefined;
    const coord = new ProviderCoordinator(sub);
    coord.recordError();
    coord.recordSuccess();
    coord.recordSuccess();
    const recovery = coord.recordSuccess();
    assert.deepEqual(recovery, { from: 'gpt-3.5-turbo', to: 'unknown' });
  });
});
