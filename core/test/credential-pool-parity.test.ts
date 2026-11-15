import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as util from 'node:util';
import {
  CredentialPool,
  DEFAULT_COOLDOWN_MS,
  MAX_COOLDOWN_MS,
} from '../src/credential-pool';

describe('CredentialPool parity: input validation (addKey)', () => {
  it('rejects an empty key', () => {
    const pool = new CredentialPool();
    assert.throws(() => pool.addKey(''), /non-empty/);
    assert.equal(pool.size, 0);
  });

  it('rejects a whitespace-only key', () => {
    const pool = new CredentialPool();
    assert.throws(() => pool.addKey('   \t\n '), /non-empty/);
    assert.equal(pool.size, 0);
  });

  it('trims surrounding whitespace from an accepted key', () => {
    const pool = new CredentialPool();
    pool.addKey('  sk-trimmed  ');
    assert.equal(pool.acquire()!.apiKey, 'sk-trimmed');
  });
});

describe('CredentialPool parity: exponential escalation', () => {
  it('escalates the cooldown on repeated exhaustion of the same key', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const { id } = pool.acquire()!;

    const before1 = Date.now();
    pool.markExhausted(id, 1000);
    const after1 = Date.now();
    // first exhaustion: base * 2^0 = 1000ms
    const json1 = pool.toJSON();
    const cd1 = json1.entries[0]!.cooldownUntil;
    assert.ok(cd1 >= before1 + 1000 && cd1 <= after1 + 1000);
    assert.equal(json1.entries[0]!.consecutiveExhaustions, 1);

    const before2 = Date.now();
    pool.markExhausted(id, 1000);
    const after2 = Date.now();
    // second exhaustion: base * 2^1 = 2000ms
    const cd2 = pool.toJSON().entries[0]!.cooldownUntil;
    assert.ok(cd2 >= before2 + 2000 && cd2 <= after2 + 2000);
    assert.equal(pool.toJSON().entries[0]!.consecutiveExhaustions, 2);
  });

  it('escalate:false keeps a flat cooldown across repeats', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const { id } = pool.acquire()!;
    pool.markExhausted(id, { cooldownMs: 500, escalate: false });
    pool.markExhausted(id, { cooldownMs: 500, escalate: false });
    const base = Date.now();
    const cd = pool.toJSON().entries[0]!.cooldownUntil;
    // flat 500ms (± scheduling slack), not 1000ms
    assert.ok(cd <= base + 600);
  });

  it('markSuccess resets the escalation streak and revives an exhausted key', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const { id } = pool.acquire()!;
    pool.markExhausted(id, 1000);
    pool.markExhausted(id, 1000); // streak = 2
    pool.markSuccess(id);
    assert.equal(pool.toJSON().entries[0]!.consecutiveExhaustions, 0);
    // revived from exhausted -> ok, cooldown cleared
    assert.equal(pool.availableCount, 1);
    assert.equal(pool.acquire()!.apiKey, 'sk-1');

    // next exhaustion starts from the base again (streak back to 1)
    const before = Date.now();
    pool.markExhausted(id, 1000);
    const cd = pool.toJSON().entries[0]!.cooldownUntil;
    assert.ok(cd <= before + 1000 + 50);
  });

  it('markSuccess on an ok key only clears the counter (no status change)', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const { id } = pool.acquire()!;
    pool.markSuccess(id); // already ok
    assert.equal(pool.toJSON().entries[0]!.status, 'ok');
    assert.equal(pool.toJSON().entries[0]!.consecutiveExhaustions, 0);
  });

  it('markSuccess on an unknown id is a no-op', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    pool.markSuccess('key-404');
    assert.equal(pool.availableCount, 1);
  });
});

describe('CredentialPool parity: server-directed cooldown', () => {
  it('uses an explicit retryAfterMs verbatim (no escalation)', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const { id } = pool.acquire()!;
    pool.markExhausted(id, { retryAfterMs: 5000 });
    pool.markExhausted(id, { retryAfterMs: 5000 });
    const before = Date.now();
    const cd = pool.toJSON().entries[0]!.cooldownUntil;
    // server hint is authoritative -> stays ~5000ms even on the 2nd call
    assert.ok(cd >= before + 4900 && cd <= before + 5100);
  });

  it('honors an absolute resetAt timestamp', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const { id } = pool.acquire()!;
    const resetAt = Date.now() + 7000;
    pool.markExhausted(id, { resetAt });
    const cd = pool.toJSON().entries[0]!.cooldownUntil;
    assert.ok(cd >= resetAt - 50 && cd <= resetAt + 50);
  });

  it('a resetAt in the past clamps the cooldown to now (never negative)', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const { id } = pool.acquire()!;
    const before = Date.now();
    pool.markExhausted(id, { resetAt: Date.now() - 10000 });
    const cd = pool.toJSON().entries[0]!.cooldownUntil;
    assert.ok(cd >= before && cd <= Date.now());
    // immediately re-acquirable
    assert.equal(pool.availableCount, 1);
  });
});

describe('CredentialPool parity: clamping untrusted durations', () => {
  it('clamps a huge server retryAfterMs to the default MAX_COOLDOWN_MS', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const { id } = pool.acquire()!;
    const before = Date.now();
    pool.markExhausted(id, { retryAfterMs: Number.MAX_SAFE_INTEGER });
    const cd = pool.toJSON().entries[0]!.cooldownUntil;
    assert.ok(cd <= before + MAX_COOLDOWN_MS + 50);
    assert.ok(cd >= before + MAX_COOLDOWN_MS - 50);
  });

  it('honors an explicit maxCooldownMs ceiling', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const { id } = pool.acquire()!;
    const before = Date.now();
    pool.markExhausted(id, { retryAfterMs: 999999999, maxCooldownMs: 2000 });
    const cd = pool.toJSON().entries[0]!.cooldownUntil;
    assert.ok(cd <= before + 2000 + 50 && cd >= before + 2000 - 50);
  });
});

describe('CredentialPool parity: cooldown jitter', () => {
  it('adds bounded jitter using an injected rng', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const { id } = pool.acquire()!;
    const before = Date.now();
    // rng=1 -> floor(0.999... * jitterMs); use 0.5 for a stable midpoint.
    pool.markExhausted(id, { cooldownMs: 1000, jitterMs: 400, rng: () => 0.5 });
    const cd = pool.toJSON().entries[0]!.cooldownUntil;
    // base 1000 + jitter floor(0.5*400)=200
    assert.ok(cd >= before + 1200 && cd <= before + 1200 + 50);
  });

  it('keeps jitter within [0, jitterMs) when using the default rng', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const { id } = pool.acquire()!;
    const before = Date.now();
    pool.markExhausted(id, { cooldownMs: 1000, jitterMs: 300 });
    const cd = pool.toJSON().entries[0]!.cooldownUntil;
    assert.ok(cd >= before + 1000 && cd < before + 1000 + 300 + 50);
  });
});

describe('CredentialPool parity: nextAvailableAt backpressure', () => {
  it('returns null for an empty pool', () => {
    const pool = new CredentialPool();
    assert.equal(pool.nextAvailableAt(), null);
  });

  it('returns null when every credential is dead', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    pool.markDead('key-0');
    assert.equal(pool.nextAvailableAt(), null);
  });

  it('returns ~now when a credential is currently available', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const at = pool.nextAvailableAt()!;
    assert.ok(at <= Date.now() && at >= Date.now() - 50);
  });

  it('returns the soonest cooldownUntil when all keys are cooling down', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    pool.addKey('sk-2');
    pool.markExhausted('key-0', { retryAfterMs: 10000 });
    pool.markExhausted('key-1', { retryAfterMs: 3000 });
    const at = pool.nextAvailableAt()!;
    const soonest = Math.min(
      pool.toJSON().entries[0]!.cooldownUntil,
      pool.toJSON().entries[1]!.cooldownUntil,
    );
    assert.equal(at, soonest);
  });
});

describe('CredentialPool parity: serialization redaction', () => {
  it('JSON.stringify never leaks the raw key', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-super-secret-value-123', 'prod');
    const serialized = JSON.stringify(pool);
    assert.ok(!serialized.includes('sk-super-secret-value-123'));
    assert.ok(serialized.includes('[REDACTED]'));
    assert.ok(serialized.includes('prod')); // non-secret metadata preserved
  });

  it('util.inspect never leaks the raw key', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-super-secret-value-123');
    const dumped = util.inspect(pool);
    assert.ok(!dumped.includes('sk-super-secret-value-123'));
    assert.ok(dumped.includes('[REDACTED]'));
  });
});

describe('CredentialPool parity: exported constants', () => {
  it('exposes named cooldown constants (no inline magic numbers)', () => {
    assert.equal(DEFAULT_COOLDOWN_MS, 60000);
    assert.equal(MAX_COOLDOWN_MS, 3600000);
  });
});
