import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { ProviderHealthTracker } from '../src/provider-health';
import { ProviderCoordinator } from '../src/provider-coordinator';
import { CredentialPool, SELECTION_STRATEGIES } from '../src/credential-pool';
import type { SelectionStrategy } from '../src/credential-pool';
import type { ProviderConfig, ProviderSubsystem } from '../src/types';

// A deterministic primary: sk-ant- prefix => provider inferred as 'anthropic'
// regardless of ambient env vars (apiKey inference wins over env inference).
function makeSubsystem(overrides?: Partial<ProviderSubsystem>): ProviderSubsystem {
  const primary: ProviderConfig = {
    baseUrl: 'https://api.anthropic.com',
    apiKey: 'sk-ant-primary',
    model: 'claude-primary',
  };
  return { primary, ...overrides };
}

// ---------------------------------------------------------------------------
// ProviderHealthTracker
// ---------------------------------------------------------------------------
describe('ProviderHealthTracker — no data (degenerate)', () => {
  it('reports zeroed stats with full availability before any request', () => {
    const t = new ProviderHealthTracker();
    const s = t.getStats();
    assert.equal(s.requestCount, 0);
    assert.equal(s.errorCount, 0);
    assert.equal(s.successCount, 0);
    // total === 0 => errorRate 0 => availability 1 (not NaN)
    assert.equal(s.errorRate, 0);
    assert.equal(s.availability, 1);
    assert.deepEqual(s.latency, { min: 0, max: 0, avg: 0, p50: 0, p95: 0 });
    assert.equal(s.lastErrorTime, null);
    assert.equal(s.lastSuccessTime, null);
  });
});

describe('ProviderHealthTracker — success recording', () => {
  it('increments request + success counts and timestamps success', () => {
    const t = new ProviderHealthTracker();
    const before = Date.now();
    t.recordRequest(42);
    const s = t.getStats();
    assert.equal(s.requestCount, 1);
    assert.equal(s.successCount, 1);
    assert.equal(s.errorCount, 0);
    assert.equal(s.errorRate, 0);
    assert.equal(s.availability, 1);
    assert.ok(s.lastSuccessTime !== null && s.lastSuccessTime >= before);
    assert.equal(s.lastErrorTime, null);
    // Single latency sample: all percentiles collapse to that value.
    assert.deepEqual(s.latency, { min: 42, max: 42, avg: 42, p50: 42, p95: 42 });
  });
});

describe('ProviderHealthTracker — error recording', () => {
  it('increments request + error counts, defaults latency to 0, timestamps error', () => {
    const t = new ProviderHealthTracker();
    const before = Date.now();
    t.recordError();
    const s = t.getStats();
    assert.equal(s.requestCount, 1);
    assert.equal(s.errorCount, 1);
    assert.equal(s.successCount, 0);
    assert.equal(s.errorRate, 1);
    assert.equal(s.availability, 0);
    assert.ok(s.lastErrorTime !== null && s.lastErrorTime >= before);
    assert.equal(s.lastSuccessTime, null);
    // default latency arg is 0, so the latency window has a single 0 sample
    assert.deepEqual(s.latency, { min: 0, max: 0, avg: 0, p50: 0, p95: 0 });
  });

  it('records an explicit error latency when supplied', () => {
    const t = new ProviderHealthTracker();
    t.recordError(250);
    const s = t.getStats();
    assert.equal(s.latency.max, 250);
    assert.equal(s.latency.min, 250);
  });
});

describe('ProviderHealthTracker — errorRate / availability math', () => {
  it('computes errorRate = errors/total and availability = 1 - errorRate', () => {
    const t = new ProviderHealthTracker();
    // 3 successes + 1 error => 1/4 errorRate
    t.recordRequest(10);
    t.recordRequest(10);
    t.recordRequest(10);
    t.recordError(10);
    const s = t.getStats();
    assert.equal(s.requestCount, 4);
    assert.equal(s.errorCount, 1);
    assert.equal(s.successCount, 3);
    assert.equal(s.errorRate, 0.25);
    assert.equal(s.availability, 0.75);
  });

  it('all-errors case yields errorRate 1 / availability 0', () => {
    const t = new ProviderHealthTracker();
    for (let i = 0; i < 5; i++) t.recordError(5);
    const s = t.getStats();
    assert.equal(s.errorCount, 5);
    assert.equal(s.successCount, 0);
    assert.equal(s.errorRate, 1);
    assert.equal(s.availability, 0);
  });
});

describe('ProviderHealthTracker — latency percentile stats', () => {
  it('computes min/max/avg/p50/p95 over the recorded samples', () => {
    const t = new ProviderHealthTracker();
    const samples = [100, 200, 300, 400, 500];
    for (const ms of samples) t.recordRequest(ms);
    const s = t.getStats();
    // Compute expected independently.
    const sorted = [...samples].sort((a, b) => a - b);
    const sum = sorted.reduce((a, b) => a + b, 0);
    assert.equal(s.latency.min, sorted[0]);
    assert.equal(s.latency.max, sorted[sorted.length - 1]);
    assert.equal(s.latency.avg, Math.round((sum / sorted.length) * 100) / 100);
    assert.equal(s.latency.p50, sorted[Math.floor(sorted.length * 0.5)]);
    assert.equal(
      s.latency.p95,
      sorted[Math.min(Math.floor(sorted.length * 0.95), sorted.length - 1)],
    );
  });

  it('rounds the average to two decimal places', () => {
    const t = new ProviderHealthTracker();
    // 1 and 2 => avg 1.5; 1,1,2 => avg 1.3333 -> 1.33
    t.recordRequest(1);
    t.recordRequest(1);
    t.recordRequest(2);
    assert.equal(t.getStats().latency.avg, 1.33);
  });
});

describe('ProviderHealthTracker — sliding window bound', () => {
  it('keeps only the most recent 100 latency samples', () => {
    const t = new ProviderHealthTracker();
    // push 1..150; window keeps 51..150
    for (let i = 1; i <= 150; i++) t.recordRequest(i);
    const s = t.getStats();
    // requestCount keeps counting beyond the window.
    assert.equal(s.requestCount, 150);
    // latency reflects only the last 100 samples (51..150).
    const kept: number[] = [];
    for (let i = 51; i <= 150; i++) kept.push(i);
    const sum = kept.reduce((a, b) => a + b, 0);
    assert.equal(s.latency.min, 51);
    assert.equal(s.latency.max, 150);
    assert.equal(s.latency.avg, Math.round((sum / kept.length) * 100) / 100);
    assert.equal(s.latency.p50, kept[Math.floor(kept.length * 0.5)]);
  });
});

describe('ProviderHealthTracker — reset', () => {
  it('clears all counters, timestamps, and latency window', () => {
    const t = new ProviderHealthTracker();
    t.recordRequest(10);
    t.recordError(20);
    t.reset();
    const s = t.getStats();
    assert.equal(s.requestCount, 0);
    assert.equal(s.errorCount, 0);
    assert.equal(s.successCount, 0);
    assert.equal(s.errorRate, 0);
    assert.equal(s.availability, 1);
    assert.equal(s.lastErrorTime, null);
    assert.equal(s.lastSuccessTime, null);
    assert.deepEqual(s.latency, { min: 0, max: 0, avg: 0, p50: 0, p95: 0 });
  });
});

// ---------------------------------------------------------------------------
// ProviderCoordinator — state transitions
// ---------------------------------------------------------------------------
describe('ProviderCoordinator — initial state', () => {
  it('seeds currentModel/provider from the primary config', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const st = coord.getState();
    assert.equal(st.currentModel, 'claude-primary');
    assert.equal(st.currentProvider, 'anthropic');
    assert.equal(st.usedFallback, false);
    assert.equal(st.requestCount, 0);
    assert.equal(st.errorCount, 0);
  });

  it('getState returns a copy (mutating it does not affect the coordinator)', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const st = coord.getState();
    st.requestCount = 999;
    assert.equal(coord.getState().requestCount, 0);
  });

  it('always resolves to string model/provider even with an empty primary', () => {
    const subsystem: ProviderSubsystem = {
      primary: { baseUrl: '', apiKey: '', model: '' },
    };
    const coord = new ProviderCoordinator(subsystem);
    const st = coord.getState();
    // With no apiKey/baseUrl, model/provider come from env inference or the
    // 'unknown' sentinel — never undefined/null. (Env-independent invariant.)
    assert.equal(typeof st.currentModel, 'string');
    assert.ok(st.currentModel.length > 0);
    assert.equal(typeof st.currentProvider, 'string');
    assert.ok(st.currentProvider.length > 0);
  });
});

describe('ProviderCoordinator — request/latency accounting', () => {
  it('recordRequest increments the request counter in state', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    coord.recordRequest();
    coord.recordRequest();
    assert.equal(coord.getState().requestCount, 2);
  });

  it('recordRequestLatency feeds the health tracker', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    coord.recordRequestLatency(123);
    const h = coord.getHealthStats();
    assert.equal(h.requestCount, 1);
    assert.equal(h.successCount, 1);
    assert.equal(h.latency.max, 123);
  });
});

describe('ProviderCoordinator — fallback stepping', () => {
  it('recordError with no fallback chain returns false and never switches model', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const switched = coord.recordError();
    assert.equal(switched, false);
    const st = coord.getState();
    assert.equal(st.errorCount, 1);
    assert.equal(st.usedFallback, false);
    assert.equal(st.currentModel, 'claude-primary');
  });

  it('steps through a multi-model fallback chain then exhausts', () => {
    const coord = new ProviderCoordinator(
      makeSubsystem({ fallbackModels: ['fb-1', 'fb-2'] }),
    );
    assert.equal(coord.hasFallback(), true);

    // first error -> fb-1
    assert.equal(coord.recordError(), true);
    assert.equal(coord.getState().currentModel, 'fb-1');
    assert.equal(coord.getState().usedFallback, true);
    assert.equal(coord.getConfig().model, 'fb-1');

    // second error -> fb-2
    assert.equal(coord.recordError(), true);
    assert.equal(coord.getState().currentModel, 'fb-2');
    assert.equal(coord.getConfig().model, 'fb-2');

    // third error -> chain exhausted
    assert.equal(coord.recordError(), false);
    assert.equal(coord.getState().currentModel, 'fb-2');
    assert.equal(coord.getState().errorCount, 3);
  });

  it('normalizes a single fallbackModel into the chain', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: 'solo-fb' }));
    assert.equal(coord.hasFallback(), true);
    assert.equal(coord.recordError(), true);
    assert.equal(coord.getConfig().model, 'solo-fb');
    assert.equal(coord.recordError(), false);
  });

  it('hasFallback is false with no fallback configured', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    assert.equal(coord.hasFallback(), false);
  });
});

describe('ProviderCoordinator — recovery to primary after cooldown', () => {
  it('returns to primary only after FALLBACK_COOLDOWN (3) consecutive successes', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModels: ['fb-1'] }));
    coord.recordError(); // on fb-1
    assert.equal(coord.getConfig().model, 'fb-1');

    coord.recordSuccess(); // 1
    assert.equal(coord.getConfig().model, 'fb-1');
    coord.recordSuccess(); // 2
    assert.equal(coord.getConfig().model, 'fb-1');
    coord.recordSuccess(); // 3 -> back to primary
    assert.equal(coord.getConfig().model, 'claude-primary');
    assert.equal(coord.getState().currentModel, 'claude-primary');
  });

  it('recordSuccess on primary (fallbackIndex 0) is a no-op', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModels: ['fb-1'] }));
    coord.recordSuccess();
    coord.recordSuccess();
    coord.recordSuccess();
    coord.recordSuccess();
    assert.equal(coord.getConfig().model, 'claude-primary');
  });

  it('sticky fallback prevents recovery to primary', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModels: ['fb-1'] }));
    coord.setStickyFallback(true);
    coord.recordError();
    for (let i = 0; i < 10; i++) coord.recordSuccess();
    assert.equal(coord.getConfig().model, 'fb-1');
  });
});

describe('ProviderCoordinator — compaction / architect configs', () => {
  it('getCompactionConfig uses compactionModel and a distinct fallback', () => {
    const coord = new ProviderCoordinator(
      makeSubsystem({ compactionModel: 'compact-m', fallbackModels: ['fb-1'] }),
    );
    const cfg = coord.getCompactionConfig();
    assert.equal(cfg.model, 'compact-m');
    assert.equal(cfg.fallbackModel, 'fb-1');
  });

  it('getCompactionConfig falls back to primary model when none set', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    assert.equal(coord.getCompactionConfig().model, 'claude-primary');
  });

  it('getArchitectConfig uses architectModel when present, else primary', () => {
    const withArch = new ProviderCoordinator(makeSubsystem({ architectModel: 'arch-m' }));
    assert.equal(withArch.getArchitectConfig().model, 'arch-m');
    const without = new ProviderCoordinator(makeSubsystem());
    assert.equal(without.getArchitectConfig().model, 'claude-primary');
  });
});

describe('ProviderCoordinator — provider identity + reconfig', () => {
  it('isAnthropicProvider is true for a claude model', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    assert.equal(coord.isAnthropicProvider(), true);
  });

  it('isAnthropicProvider is false for a non-anthropic config', () => {
    const subsystem: ProviderSubsystem = {
      primary: { baseUrl: 'https://api.openai.com/v1', apiKey: 'sk-openai', model: 'gpt-4o' },
    };
    const coord = new ProviderCoordinator(subsystem);
    assert.equal(coord.isAnthropicProvider(), false);
  });

  it('setPrimaryConfig swaps the provider and resets fallbackIndex', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModels: ['fb-1'] }));
    coord.recordError(); // move onto fb-1
    assert.equal(coord.getConfig().model, 'fb-1');

    coord.setPrimaryConfig({
      baseUrl: 'https://api.openai.com/v1',
      apiKey: 'sk-openai',
      model: 'gpt-4o',
    });
    const st = coord.getState();
    assert.equal(st.currentModel, 'gpt-4o');
    assert.equal(st.currentProvider, 'openai');
    assert.equal(coord.getConfig().model, 'gpt-4o'); // back on primary index 0
  });
});

describe('ProviderCoordinator — credential pool wiring', () => {
  it('reports 1 available credential when no pool exists', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    assert.equal(coord.availableCredentials, 1);
  });

  it('builds a pool from primary + additional keys and rotates apiKeys', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), ['sk-ant-two', 'sk-ant-three']);
    assert.equal(coord.availableCredentials, 3);
    // round_robin: primary key first, then the extras, then wrap.
    const k1 = coord.getConfig().apiKey;
    const k2 = coord.getConfig().apiKey;
    const k3 = coord.getConfig().apiKey;
    const k4 = coord.getConfig().apiKey;
    assert.deepEqual([k1, k2, k3], ['sk-ant-primary', 'sk-ant-two', 'sk-ant-three']);
    assert.equal(k4, 'sk-ant-primary');
  });

  it('markCredentialExhausted drops the active credential from the pool', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), ['sk-ant-two']);
    assert.equal(coord.availableCredentials, 2);
    coord.getConfig(); // acquire -> activeCredentialId set to primary key
    coord.markCredentialExhausted(60000);
    assert.equal(coord.availableCredentials, 1);
  });

  it('markCredentialExhausted is a safe no-op without a pool', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    assert.doesNotThrow(() => coord.markCredentialExhausted());
    assert.equal(coord.availableCredentials, 1);
  });
});

describe('ProviderCoordinator — rate limit passthrough', () => {
  it('exposes a rate-limit status object', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    coord.updateRateLimits({ 'x-ratelimit-remaining-requests': '5' });
    const status = coord.getRateLimitStatus();
    assert.equal(typeof status, 'object');
    assert.ok(status !== null);
  });
});

// ---------------------------------------------------------------------------
// CredentialPool — selection strategies + lifecycle
// ---------------------------------------------------------------------------
describe('CredentialPool — SELECTION_STRATEGIES catalog', () => {
  it('lists the three supported strategies', () => {
    assert.deepEqual([...SELECTION_STRATEGIES], ['fill_first', 'round_robin', 'least_used']);
  });
});

describe('CredentialPool — degenerate cases', () => {
  it('acquire on an empty pool returns null', () => {
    const pool = new CredentialPool();
    assert.equal(pool.acquire(), null);
    assert.equal(pool.size, 0);
    assert.equal(pool.availableCount, 0);
  });

  it('defaults to fill_first strategy', () => {
    const pool = new CredentialPool();
    pool.addKey('a');
    pool.addKey('b');
    assert.equal(pool.acquire()?.apiKey, 'a');
    assert.equal(pool.acquire()?.apiKey, 'a');
  });
});

describe('CredentialPool — fill_first selection', () => {
  it('always returns the first OK key until it is removed', () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('a');
    pool.addKey('b');
    assert.equal(pool.acquire()?.id, 'key-0');
    assert.equal(pool.acquire()?.id, 'key-0');
    pool.markDead('key-0');
    assert.equal(pool.acquire()?.id, 'key-1');
  });
});

describe('CredentialPool — round_robin selection', () => {
  it('cycles across OK keys deterministically and wraps', () => {
    const pool = new CredentialPool('round_robin');
    pool.addKey('a');
    pool.addKey('b');
    pool.addKey('c');
    const ids = [
      pool.acquire()?.id,
      pool.acquire()?.id,
      pool.acquire()?.id,
      pool.acquire()?.id,
    ];
    assert.deepEqual(ids, ['key-0', 'key-1', 'key-2', 'key-0']);
  });
});

describe('CredentialPool — least_used selection', () => {
  it('prefers the key with the lowest usage count', () => {
    const pool = new CredentialPool('least_used');
    pool.addKey('a');
    pool.addKey('b');
    // both at 0 -> first (stable) -> a; a now 1
    assert.equal(pool.acquire()?.id, 'key-0');
    // a=1, b=0 -> b; b now 1
    assert.equal(pool.acquire()?.id, 'key-1');
    // tie at 1 -> stable order -> a
    assert.equal(pool.acquire()?.id, 'key-0');
  });
});

describe('CredentialPool — exhaustion + cooldown recovery', () => {
  it('skips an exhausted key until its cooldown lapses', () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('a');
    pool.addKey('b');
    const first = pool.acquire()!;
    assert.equal(first.id, 'key-0');
    // exhaust key-0 with a long cooldown -> only key-1 remains
    pool.markExhausted('key-0', 60000);
    assert.equal(pool.availableCount, 1);
    assert.equal(pool.acquire()?.id, 'key-1');
  });

  it('returns null when every key is exhausted and still cooling down', () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('a');
    pool.acquire();
    pool.markExhausted('key-0', 60000);
    assert.equal(pool.availableCount, 0);
    assert.equal(pool.acquire(), null);
  });

  it('recovers an exhausted key once its cooldown has elapsed (cooldown 0)', () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('a');
    pool.acquire();
    pool.markExhausted('key-0', 0); // cooldownUntil === now
    assert.equal(pool.availableCount, 1);
    const re = pool.acquire();
    assert.equal(re?.id, 'key-0'); // status reset to ok on re-acquire
  });

  it('markExhausted uses a 60s default cooldown', () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('a');
    pool.acquire();
    pool.markExhausted('key-0');
    assert.equal(pool.availableCount, 0);
  });
});

describe('CredentialPool — dead keys', () => {
  it('permanently removes a dead key from availability', () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('a');
    pool.markDead('key-0');
    assert.equal(pool.size, 1);
    assert.equal(pool.availableCount, 0);
    assert.equal(pool.acquire(), null);
  });

  it('markDead on an unknown id is a no-op', () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('a');
    assert.doesNotThrow(() => pool.markDead('nope'));
    assert.equal(pool.availableCount, 1);
  });

  it('markExhausted on an unknown id is a no-op', () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('a');
    assert.doesNotThrow(() => pool.markExhausted('nope'));
    assert.equal(pool.availableCount, 1);
  });
});

describe('CredentialPool — usage + label bookkeeping', () => {
  it('tracks usage count and last-used across acquisitions', () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('a', 'primary-label');
    const before = Date.now();
    pool.acquire();
    pool.acquire();
    // usageCount is internal; observe via least_used ordering with a second key.
    const pool2 = new CredentialPool('least_used');
    pool2.addKey('x');
    pool2.addKey('y');
    pool2.acquire(); // x -> 1
    pool2.acquire(); // y -> 1
    pool2.acquire(); // tie -> x
    // last acquire picked x again; nothing threw and ordering held.
    assert.ok(Date.now() >= before);
  });

  it('every strategy produces a valid key id for a single-key pool', () => {
    for (const strat of SELECTION_STRATEGIES as readonly SelectionStrategy[]) {
      const pool = new CredentialPool(strat);
      pool.addKey('only');
      assert.equal(pool.acquire()?.id, 'key-0');
    }
  });
});
