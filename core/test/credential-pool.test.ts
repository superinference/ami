import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { CredentialPool } from '../src/credential-pool';

describe('CredentialPool', () => {
  it('acquires single key', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-test-1', 'key1');
    const cred = pool.acquire();
    assert.ok(cred);
    assert.equal(cred.apiKey, 'sk-test-1');
  });

  it('fill_first strategy uses first key repeatedly', () => {
    const pool = new CredentialPool('fill_first');
    pool.addKey('sk-1');
    pool.addKey('sk-2');
    assert.equal(pool.acquire()!.apiKey, 'sk-1');
    assert.equal(pool.acquire()!.apiKey, 'sk-1');
  });

  it('round_robin strategy cycles through keys', () => {
    const pool = new CredentialPool('round_robin');
    pool.addKey('sk-1');
    pool.addKey('sk-2');
    pool.addKey('sk-3');
    assert.equal(pool.acquire()!.apiKey, 'sk-1');
    assert.equal(pool.acquire()!.apiKey, 'sk-2');
    assert.equal(pool.acquire()!.apiKey, 'sk-3');
    assert.equal(pool.acquire()!.apiKey, 'sk-1');
  });

  it('least_used strategy picks least used', () => {
    const pool = new CredentialPool('least_used');
    pool.addKey('sk-1');
    pool.addKey('sk-2');
    const first = pool.acquire()!;
    assert.equal(first.apiKey, 'sk-1');
    const second = pool.acquire()!;
    assert.equal(second.apiKey, 'sk-2');
    const third = pool.acquire()!;
    assert.ok(['sk-1', 'sk-2'].includes(third.apiKey));
  });

  it('marks key as exhausted with cooldown', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    pool.addKey('sk-2');
    const cred = pool.acquire()!;
    pool.markExhausted(cred.id, 60000);
    const next = pool.acquire()!;
    assert.equal(next.apiKey, 'sk-2');
  });

  it('marks key as dead (permanent)', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    pool.addKey('sk-2');
    const cred = pool.acquire()!;
    pool.markDead(cred.id);
    assert.equal(pool.availableCount, 1);
  });

  it('returns null when all keys exhausted', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const cred = pool.acquire()!;
    pool.markExhausted(cred.id, 999999999);
    assert.equal(pool.acquire(), null);
  });

  it('recovers exhausted key after cooldown', async () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    const cred = pool.acquire()!;
    pool.markExhausted(cred.id, 1); // 1ms cooldown
    await new Promise(r => setTimeout(r, 10));
    const recovered = pool.acquire();
    assert.ok(recovered);
    assert.equal(recovered!.apiKey, 'sk-1');
  });

  it('tracks size and available count', () => {
    const pool = new CredentialPool();
    assert.equal(pool.size, 0);
    pool.addKey('sk-1');
    pool.addKey('sk-2');
    assert.equal(pool.size, 2);
    assert.equal(pool.availableCount, 2);
    pool.markDead(pool.acquire()!.id);
    assert.equal(pool.availableCount, 1);
  });

  it('returns null when all keys are dead', () => {
    const pool = new CredentialPool();
    pool.addKey('sk-1');
    pool.addKey('sk-2');
    pool.markDead(pool.acquire()!.id);
    pool.markDead(pool.acquire()!.id);
    assert.equal(pool.acquire(), null);
    assert.equal(pool.availableCount, 0);
  });
});
