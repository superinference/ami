import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  RespawnThrottle,
  CrashLoopError,
  DEFAULT_RESPAWN_MAX_ATTEMPTS,
  DEFAULT_RESPAWN_WINDOW_MS,
  type RespawnAdmission,
} from '../src/respawn-throttle';

function assertRefused(a: RespawnAdmission): asserts a is Extract<RespawnAdmission, { allowed: false }> {
  assert.equal(a.allowed, false);
}

test('RespawnThrottle: admits up to maxAttempts inside the window, then refuses with crash_loop', () => {
  let now = 1_000;
  const t = new RespawnThrottle({ maxAttempts: 3, windowMs: 10_000, now: () => now });
  assert.equal(t.admit().allowed, true);
  assert.equal(t.admit().allowed, true);
  assert.equal(t.admit().allowed, true);
  const refused = t.admit();
  assertRefused(refused);
  assert.equal(refused.reason, 'crash_loop');
  assert.equal(refused.attemptsInWindow, 3);
  // oldest attempt was at t=1000, window 10000 → retry after full window from now.
  assert.equal(refused.retryAfterMs, 10_000);
});

test('RespawnThrottle: a refusal does not record an attempt, so the window drains purely by time', () => {
  let now = 0;
  const t = new RespawnThrottle({ maxAttempts: 2, windowMs: 1_000, now: () => now });
  t.admit();
  t.admit();
  now = 500;
  assertRefused(t.admit()); // still full, refused
  // Advance past the window: the two original attempts age out, a slot frees.
  now = 1_001;
  assert.equal(t.admit().allowed, true);
});

test('RespawnThrottle: retryAfterMs shrinks as the oldest attempt ages', () => {
  let now = 0;
  const t = new RespawnThrottle({ maxAttempts: 1, windowMs: 1_000, now: () => now });
  t.admit();
  now = 600;
  const a = t.admit();
  assertRefused(a);
  assert.equal(a.retryAfterMs, 400); // 1000 - 600
});

test('RespawnThrottle: reset clears the window (proven recovery)', () => {
  let now = 0;
  const t = new RespawnThrottle({ maxAttempts: 2, windowMs: 10_000, now: () => now });
  t.admit();
  t.admit();
  assertRefused(t.admit());
  t.reset();
  assert.equal(t.admit().allowed, true);
  assert.equal(t.attemptsInWindow(), 1);
});

test('RespawnThrottle: attemptsInWindow evicts aged attempts', () => {
  let now = 0;
  const t = new RespawnThrottle({ maxAttempts: 5, windowMs: 100, now: () => now });
  t.admit();
  t.admit();
  assert.equal(t.attemptsInWindow(), 2);
  now = 200;
  assert.equal(t.attemptsInWindow(), 0);
});

test('RespawnThrottle: defaults are applied when no options given', () => {
  const t = new RespawnThrottle();
  for (let i = 0; i < DEFAULT_RESPAWN_MAX_ATTEMPTS; i++) {
    assert.equal(t.admit().allowed, true);
  }
  assertRefused(t.admit());
  assert.equal(DEFAULT_RESPAWN_WINDOW_MS, 60_000);
});

test('RespawnThrottle: rejects invalid options', () => {
  assert.throws(() => new RespawnThrottle({ maxAttempts: 0 }), RangeError);
  assert.throws(() => new RespawnThrottle({ maxAttempts: 2.5 }), RangeError);
  assert.throws(() => new RespawnThrottle({ windowMs: 0 }), RangeError);
  assert.throws(() => new RespawnThrottle({ windowMs: -1 }), RangeError);
});

test('CrashLoopError: carries code, counts, and a human message', () => {
  const admission = { allowed: false as const, reason: 'crash_loop' as const, attemptsInWindow: 4, retryAfterMs: 2_500 };
  const err = new CrashLoopError(admission, 'MCP server "foo"');
  assert.equal(err.code, 'crash_loop');
  assert.equal(err.name, 'CrashLoopError');
  assert.equal(err.attemptsInWindow, 4);
  assert.equal(err.retryAfterMs, 2_500);
  assert.match(err.message, /MCP server "foo"/);
  assert.match(err.message, /4 times/);
  assert.match(err.message, /3s/); // ceil(2500/1000)
  // default subject
  assert.match(new CrashLoopError(admission).message, /The process has failed/);
});
