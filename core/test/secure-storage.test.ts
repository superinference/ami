// Unit tests for the core secure-storage building blocks: the Linux libsecret
// (`secret-tool`) backend + the `secret-tool` availability probe. Every path is
// exercised through the injectable runner seam, so nothing spawns a real keyring.

import { describe, it, afterEach, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

import {
  createLibsecretStorage,
  isSecretToolAvailable,
  resetSecretToolAvailableCache,
  isLibsecretPreferred,
  defaultSecretToolRunner,
  LIBSECRET_CACHE_TTL_MS,
  type SecretToolRunResult,
  type SecretToolRunner,
} from '../src/secure-storage';

/** A scripted `secret-tool` runner recording every invocation. */
function makeRunner(
  handler: (args: string[], input?: string) => SecretToolRunResult,
): { run: SecretToolRunner; calls: Array<{ args: string[]; input?: string }> } {
  const calls: Array<{ args: string[]; input?: string }> = [];
  const run: SecretToolRunner = (args, input) => {
    calls.push({ args, input });
    return handler(args, input);
  };
  return { run, calls };
}

const strOpts = (run: SecretToolRunner, extra: Record<string, unknown> = {}) => ({
  service: 'AMI test',
  account: 'tester',
  serialize: (s: string) => s,
  deserialize: (s: string) => s,
  run,
  ...extra,
});

describe('core secure-storage: createLibsecretStorage', () => {
  it('reads a stored secret via `secret-tool lookup` and deserializes it', () => {
    const { run, calls } = makeRunner(() => ({ status: 0, stdout: 'my-secret\n' }));
    const store = createLibsecretStorage(strOpts(run));
    assert.equal(store.name, 'libsecret');
    assert.equal(store.read(), 'my-secret');
    assert.deepEqual(calls[0].args, ['lookup', 'service', 'AMI test', 'account', 'tester']);
  });

  it('returns null on a lookup miss (non-zero exit, no output)', () => {
    const { run } = makeRunner(() => ({ status: 1, stdout: '' }));
    assert.equal(createLibsecretStorage(strOpts(run)).read(), null);
  });

  it('serves the TTL cache on a second read without re-spawning', () => {
    let hits = 0;
    const { run } = makeRunner(() => {
      hits += 1;
      return { status: 0, stdout: 'cached' };
    });
    const store = createLibsecretStorage(strOpts(run));
    assert.equal(store.read(), 'cached');
    assert.equal(store.read(), 'cached');
    assert.equal(hits, 1);
  });

  it('re-reads after the TTL window expires', () => {
    const values = ['first', 'second'];
    let i = 0;
    const { run } = makeRunner(() => ({ status: 0, stdout: values[i++] }));
    const store = createLibsecretStorage(strOpts(run, { cacheTtlMs: 0 }));
    assert.equal(store.read(), 'first');
    assert.equal(store.read(), 'second');
  });

  it('serves the stale value when a refresh fails (stale-while-error)', () => {
    let first = true;
    const { run } = makeRunner(() => {
      if (first) {
        first = false;
        return { status: 0, stdout: 'warm' };
      }
      return { status: 1, stdout: '' };
    });
    const store = createLibsecretStorage(strOpts(run, { cacheTtlMs: 0 }));
    assert.equal(store.read(), 'warm');
    // Second read fails but the warm value is served + re-cached, not null.
    assert.equal(store.read(), 'warm');
  });

  it('treats a deserialize throw as a read failure (falls through to null)', () => {
    const { run } = makeRunner(() => ({ status: 0, stdout: 'not-json' }));
    const store = createLibsecretStorage({
      service: 'AMI',
      account: 'u',
      serialize: (o: { v: number }) => JSON.stringify(o),
      deserialize: (raw) => JSON.parse(raw) as { v: number },
      run,
    });
    assert.equal(store.read(), null);
  });

  it('writes the secret via stdin (never argv) and caches on success', () => {
    const { run, calls } = makeRunner(() => ({ status: 0, stdout: '' }));
    const store = createLibsecretStorage(strOpts(run, { label: 'My Label' }));
    const res = store.update('top-secret');
    assert.deepEqual(res, { success: true });
    assert.deepEqual(calls[0].args, [
      'store', '--label', 'My Label', 'service', 'AMI test', 'account', 'tester',
    ]);
    assert.equal(calls[0].input, 'top-secret');
    assert.ok(!calls[0].args.includes('top-secret'), 'secret must not appear in argv');
    // Successful write primes the cache (no further lookup spawn).
    assert.equal(store.read(), 'top-secret');
    assert.equal(calls.length, 1);
  });

  it('defaults the keyring label to the service name', () => {
    const { run, calls } = makeRunner(() => ({ status: 0, stdout: '' }));
    createLibsecretStorage(strOpts(run)).update('x');
    assert.equal(calls[0].args[2], 'AMI test');
  });

  it('reports failure when the store subprocess exits non-zero', () => {
    const { run } = makeRunner(() => ({ status: 5, stdout: '' }));
    assert.deepEqual(createLibsecretStorage(strOpts(run)).update('x'), { success: false });
  });

  it('reports failure when the runner throws on write', () => {
    const run: SecretToolRunner = () => {
      throw new Error('spawn boom');
    };
    assert.deepEqual(createLibsecretStorage(strOpts(run)).update('x'), { success: false });
  });

  it('deletes via `secret-tool clear` and reports success/failure', () => {
    const ok = makeRunner(() => ({ status: 0, stdout: '' }));
    const store = createLibsecretStorage(strOpts(ok.run));
    assert.equal(store.delete(), true);
    assert.deepEqual(ok.calls[0].args, ['clear', 'service', 'AMI test', 'account', 'tester']);

    const bad = makeRunner(() => ({ status: 1, stdout: '' }));
    assert.equal(createLibsecretStorage(strOpts(bad.run)).delete(), false);
  });

  it('returns false when the runner throws on delete', () => {
    const run: SecretToolRunner = () => {
      throw new Error('boom');
    };
    assert.equal(createLibsecretStorage(strOpts(run)).delete(), false);
  });

  it('invalidates the read cache on delete so the next read re-spawns', () => {
    let hits = 0;
    const { run } = makeRunner((args) => {
      if (args[0] === 'lookup') hits += 1;
      return { status: 0, stdout: 'v' };
    });
    const store = createLibsecretStorage(strOpts(run));
    assert.equal(store.read(), 'v');
    store.delete();
    assert.equal(store.read(), 'v');
    assert.equal(hits, 2);
  });

  it('exposes a sane default cache TTL', () => {
    assert.equal(LIBSECRET_CACHE_TTL_MS, 30_000);
  });

  it('invalidate() drops the cache so the next read re-fetches a rotated secret', () => {
    // Parity: clearLegacyApiKeyPrefetch() — an out-of-band rotation must not be
    // shadowed by a value cached within the TTL window. A long TTL means only an
    // explicit invalidate() can force the re-read before it elapses.
    const values = ['old-secret', 'rotated-secret'];
    let i = 0;
    let lookups = 0;
    const { run } = makeRunner((args) => {
      if (args[0] === 'lookup') lookups += 1;
      return { status: 0, stdout: values[Math.min(i++, values.length - 1)] };
    });
    const store = createLibsecretStorage(strOpts(run, { cacheTtlMs: 60_000 }));
    assert.equal(store.read(), 'old-secret');
    // Within the TTL window the cache shadows the rotated value...
    assert.equal(store.read(), 'old-secret');
    assert.equal(lookups, 1);
    // ...until an explicit invalidate() forces a through-read.
    store.invalidate();
    assert.equal(store.read(), 'rotated-secret');
    assert.equal(lookups, 2);
  });

  it('invalidate() is idempotent and never throws on an empty cache', () => {
    const { run } = makeRunner(() => ({ status: 1, stdout: '' }));
    const store = createLibsecretStorage(strOpts(run));
    assert.doesNotThrow(() => {
      store.invalidate();
      store.invalidate();
    });
  });
});

describe('core secure-storage: isSecretToolAvailable', () => {
  beforeEach(resetSecretToolAvailableCache);
  afterEach(resetSecretToolAvailableCache);

  it('is true when `secret-tool --version` exits 0', () => {
    const { run, calls } = makeRunner(() => ({ status: 0, stdout: '0.21' }));
    assert.equal(isSecretToolAvailable(run), true);
    assert.deepEqual(calls[0].args, ['--version']);
  });

  it('is false when the CLI is missing (status null)', () => {
    const { run } = makeRunner(() => ({ status: null, stdout: '' }));
    assert.equal(isSecretToolAvailable(run), false);
  });

  it('is false when the runner throws', () => {
    const run: SecretToolRunner = () => {
      throw new Error('ENOENT');
    };
    assert.equal(isSecretToolAvailable(run), false);
  });

  it('caches the probe for the process lifetime', () => {
    let hits = 0;
    const { run } = makeRunner(() => {
      hits += 1;
      return { status: 0, stdout: '' };
    });
    assert.equal(isSecretToolAvailable(run), true);
    assert.equal(isSecretToolAvailable(run), true);
    assert.equal(hits, 1);
  });

  it('re-probes after the cache is reset', () => {
    let hits = 0;
    const { run } = makeRunner(() => {
      hits += 1;
      return { status: 0, stdout: '' };
    });
    isSecretToolAvailable(run);
    resetSecretToolAvailableCache();
    isSecretToolAvailable(run);
    assert.equal(hits, 2);
  });

  it('uses the real spawn runner by default (returns a boolean, never throws)', () => {
    assert.equal(typeof isSecretToolAvailable(), 'boolean');
  });
});

describe('core secure-storage: isLibsecretPreferred', () => {
  it('is off by default (no env set)', () => {
    assert.equal(isLibsecretPreferred({}), false);
  });

  it('is on for the AMI-native "1"/"true" flag', () => {
    assert.equal(isLibsecretPreferred({ SUPERINFERENCE_SECURE_STORAGE_LIBSECRET: '1' }), true);
    assert.equal(isLibsecretPreferred({ SUPERINFERENCE_SECURE_STORAGE_LIBSECRET: 'true' }), true);
  });

  it('honors the cross-framework alias', () => {
    assert.equal(isLibsecretPreferred({ AMI_SECURE_STORAGE_LIBSECRET: '1' }), true);
  });

  it('is off for any other value', () => {
    assert.equal(isLibsecretPreferred({ SUPERINFERENCE_SECURE_STORAGE_LIBSECRET: '0' }), false);
    assert.equal(isLibsecretPreferred({ SUPERINFERENCE_SECURE_STORAGE_LIBSECRET: 'yes' }), false);
  });

  it('reads process.env by default (returns a boolean)', () => {
    assert.equal(typeof isLibsecretPreferred(), 'boolean');
  });
});

describe('core secure-storage: defaultSecretToolRunner', () => {
  it('shapes a spawnSync result into { status, stdout } without throwing', () => {
    const res = defaultSecretToolRunner(['--version']);
    assert.ok(typeof res.status === 'number' || res.status === null);
    assert.equal(typeof res.stdout, 'string');
  });
});
