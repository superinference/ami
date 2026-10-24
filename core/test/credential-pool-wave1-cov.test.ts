import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { CredentialPool } from '../src/credential-pool';

// Wave 1 coverage: exercises the uncovered branches in credential-pool.ts
// (markExhausted/markDead no-op on unknown id, availableCount cooldown
// short-circuit for exhausted entries, and acquire's default strategy).
describe('CredentialPool wave1 coverage', () => {
  it('markExhausted on an unknown id is a no-op (if-entry false branch)', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    // Nonexistent id must not throw and must not alter the live key.
    pool.markExhausted('key-999', 60000);
    assert.equal(pool.availableCount, 1);
    assert.equal(pool.acquire()!.apiKey, 'sk-1');
  });

  it('markDead on an unknown id is a no-op (if-entry false branch)', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    pool.markDead('key-nope');
    // The real key is untouched and still acquirable.
    assert.equal(pool.availableCount, 1);
    assert.ok(pool.acquire());
  });

  it('availableCount counts an exhausted key whose cooldown has expired', async () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const cred = pool.acquire()!;
    pool.markExhausted(cred.id, 1); // 1ms cooldown
    // While cooling down the key is not available...
    pool.markExhausted(cred.id, 999999999);
    assert.equal(pool.availableCount, 0);
    // ...re-exhaust with a tiny cooldown, then wait it out so the
    // `now >= e.cooldownUntil` branch in availableCount is taken (true).
    pool.markExhausted(cred.id, 1);
    await new Promise(r => setTimeout(r, 10));
    assert.equal(pool.availableCount, 1);
  });

  it('availableCount excludes an exhausted key still within cooldown (false branch)', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    pool.addKey('sk-2');
    const cred = pool.acquire()!;
    pool.markExhausted(cred.id, 999999999);
    // Only the untouched key is counted.
    assert.equal(pool.availableCount, 1);
  });

  it('acquire flips an exhausted-but-cooled key back to ok and reuses it', async () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const first = pool.acquire()!;
    pool.markExhausted(first.id, 1);
    await new Promise(r => setTimeout(r, 10));
    // acquire must take the `e.status === 'exhausted' && now >= cooldownUntil`
    // true branch, reset status to ok, and return the same key.
    const second = pool.acquire()!;
    assert.equal(second.apiKey, 'sk-1');
    assert.equal(second.id, first.id);
  });

  it('acquire uses the explicit default strategy for an unknown value', () => {
    // Force the switch default arm with a strategy outside the union.
    const pool = new CredentialPool('bogus' as unknown as 'fill_first');
    pool.addKey('sk-a');
    pool.addKey('sk-b');
    assert.equal(pool.acquire()!.apiKey, 'sk-a');
    assert.equal(pool.acquire()!.apiKey, 'sk-a');
  });

  it('acquire returns null on an empty pool', () => {
    const pool = new CredentialPool();
    assert.equal(pool.acquire(), null);
    assert.equal(pool.availableCount, 0);
    assert.equal(pool.size, 0);
  });

  it('availableCount evaluates every filter branch in one mixed-state call', async () => {
    const pool = new CredentialPool('round_robin');
    pool.addKey('ok');          // key-0 -> stays ok
    pool.addKey('dead');        // key-1 -> dead
    pool.addKey('cooled');      // key-2 -> exhausted then cooled
    pool.addKey('cooling');     // key-3 -> exhausted, long cooldown
    pool.markDead('key-1');
    pool.markExhausted('key-2', 1);
    pool.markExhausted('key-3', 999999999);
    await new Promise(r => setTimeout(r, 10));
    // ok (key-0) + cooled (key-2) are available; dead + cooling are not.
    assert.equal(pool.availableCount, 2);
  });

  it('markExhausted applies the default 60000ms cooldown when omitted', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const cred = pool.acquire()!;
    // Call with a single argument so the `cooldownMs = 60000` default branch runs.
    pool.markExhausted(cred.id);
    // Default cooldown is far in the future, so the key is unavailable now.
    assert.equal(pool.availableCount, 0);
    assert.equal(pool.acquire(), null);
  });

  it('round_robin wraps its index after keys are removed from the ok set', () => {
    const pool = new CredentialPool('round_robin');
    pool.addKey('sk-1');
    pool.addKey('sk-2');
    pool.addKey('sk-3');
    assert.equal(pool.acquire()!.apiKey, 'sk-1');
    assert.equal(pool.acquire()!.apiKey, 'sk-2');
    // Kill sk-3 so the ok set shrinks; roundRobinIndex (now 2) must wrap via modulo.
    pool.markDead('key-2');
    assert.equal(pool.acquire()!.apiKey, 'sk-1');
  });
});
