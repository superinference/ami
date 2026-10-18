import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isSensitiveEnvVar,
  canAccessEnv,
  SENSITIVE_ENV_VARS,
  EnvAccessGuard,
} from '../src/env-access-guard';

describe('isSensitiveEnvVar', () => {
  it('detects API_KEY suffix', () => {
    assert.equal(isSensitiveEnvVar('MY_API_KEY'), true);
  });

  it('detects PASSWORD', () => {
    assert.equal(isSensitiveEnvVar('DB_PASSWORD'), true);
  });

  it('is case-insensitive', () => {
    assert.equal(isSensitiveEnvVar('my_api_key'), true);
    assert.equal(isSensitiveEnvVar('Secret_Key'), true);
  });

  it('returns false for PATH', () => {
    assert.equal(isSensitiveEnvVar('PATH'), false);
  });

  it('returns false for HOME', () => {
    assert.equal(isSensitiveEnvVar('HOME'), false);
  });

  it('detects ACCESS_TOKEN', () => {
    assert.equal(isSensitiveEnvVar('GITHUB_ACCESS_TOKEN'), true);
  });

  it('detects PRIVATE_KEY', () => {
    assert.equal(isSensitiveEnvVar('SSH_PRIVATE_KEY'), true);
  });

  it('detects CREDENTIAL', () => {
    assert.equal(isSensitiveEnvVar('AWS_CREDENTIAL'), true);
  });
});

describe('SENSITIVE_ENV_VARS', () => {
  it('contains expected entries', () => {
    assert.ok(SENSITIVE_ENV_VARS.has('API_KEY'));
    assert.ok(SENSITIVE_ENV_VARS.has('SECRET_KEY'));
    assert.ok(SENSITIVE_ENV_VARS.has('ACCESS_TOKEN'));
    assert.ok(SENSITIVE_ENV_VARS.has('PRIVATE_KEY'));
    assert.ok(SENSITIVE_ENV_VARS.has('PASSWORD'));
    assert.ok(SENSITIVE_ENV_VARS.has('CREDENTIAL'));
    assert.ok(SENSITIVE_ENV_VARS.has('AUTH_TOKEN'));
  });

  it('has 7 entries', () => {
    assert.equal(SENSITIVE_ENV_VARS.size, 7);
  });
});

describe('canAccessEnv', () => {
  it('allows normal get', () => {
    assert.equal(canAccessEnv('HOME', 'get', 'user'), true);
  });

  it('blocks user reading org plugin (prepend) env', () => {
    assert.equal(canAccessEnv('MY_VAR', 'get', 'user', 'prepend'), false);
  });

  it('blocks user reading org plugin (append) env', () => {
    assert.equal(canAccessEnv('MY_VAR', 'get', 'user', 'append'), false);
  });

  it('allows engine reading any env', () => {
    assert.equal(canAccessEnv('MY_VAR', 'get', 'engine', 'prepend'), true);
  });

  it('allows builtin reading any env', () => {
    assert.equal(canAccessEnv('MY_VAR', 'get', 'builtin', 'prepend'), true);
  });

  it('allows set operations', () => {
    assert.equal(canAccessEnv('MY_VAR', 'set', 'user'), true);
  });

  it('allows delete operations', () => {
    assert.equal(canAccessEnv('MY_VAR', 'delete', 'user'), true);
  });

  it('allows get without readerTier', () => {
    assert.equal(canAccessEnv('MY_VAR', 'get', 'user'), true);
  });

  it('allows prepend tier get of prepend reader', () => {
    assert.equal(canAccessEnv('MY_VAR', 'get', 'prepend', 'prepend'), true);
  });
});

describe('EnvAccessGuard', () => {
  it('check records event', () => {
    const guard = new EnvAccessGuard();
    guard.check('HOME', 'get', 'user');
    assert.equal(guard.getEvents().length, 1);
  });

  it('check returns true for allowed access', () => {
    const guard = new EnvAccessGuard();
    assert.equal(guard.check('HOME', 'get', 'user'), true);
  });

  it('check returns false for blocked access', () => {
    const guard = new EnvAccessGuard();
    assert.equal(guard.check('MY_VAR', 'get', 'user', 'prepend'), false);
  });

  it('getEvents returns all events', () => {
    const guard = new EnvAccessGuard();
    guard.check('A', 'get', 'user');
    guard.check('B', 'set', 'engine');
    assert.equal(guard.getEvents().length, 2);
  });

  it('getBlockedEvents filters correctly', () => {
    const guard = new EnvAccessGuard();
    guard.check('HOME', 'get', 'user');
    guard.check('MY_VAR', 'get', 'user', 'prepend');
    const blocked = guard.getBlockedEvents();
    assert.equal(blocked.length, 1);
    assert.equal(blocked[0].variable, 'MY_VAR');
  });

  it('getBlockedCount returns correct count', () => {
    const guard = new EnvAccessGuard();
    guard.check('HOME', 'get', 'user');
    guard.check('X', 'get', 'user', 'prepend');
    guard.check('Y', 'get', 'user', 'append');
    assert.equal(guard.getBlockedCount(), 2);
  });

  it('safeGetEnv redacts sensitive vars', () => {
    const original = process.env.TEST_API_KEY;
    process.env.TEST_API_KEY = 'secret123';
    try {
      const guard = new EnvAccessGuard();
      const result = guard.safeGetEnv('TEST_API_KEY');
      assert.equal(result.redacted, true);
      assert.equal(result.value, '[REDACTED]');
    } finally {
      if (original === undefined) delete process.env.TEST_API_KEY;
      else process.env.TEST_API_KEY = original;
    }
  });

  it('safeGetEnv returns value for safe vars', () => {
    const guard = new EnvAccessGuard();
    const result = guard.safeGetEnv('HOME');
    assert.equal(result.redacted, false);
    assert.equal(typeof result.value, 'string');
  });

  it('safeGetEnv redacts undefined sensitive var', () => {
    const guard = new EnvAccessGuard();
    const result = guard.safeGetEnv('NONEXISTENT_API_KEY');
    assert.equal(result.redacted, true);
    assert.equal(result.value, undefined);
  });

  it('clear resets events', () => {
    const guard = new EnvAccessGuard();
    guard.check('A', 'get', 'user');
    guard.check('B', 'get', 'user', 'prepend');
    guard.clear();
    assert.equal(guard.getEvents().length, 0);
    assert.equal(guard.getBlockedCount(), 0);
  });

  it('capacity is enforced', () => {
    const guard = new EnvAccessGuard(3);
    guard.check('A', 'get', 'user');
    guard.check('B', 'get', 'user');
    guard.check('C', 'get', 'user');
    guard.check('D', 'get', 'user');
    assert.equal(guard.getEvents().length, 3);
  });

  it('event contains correct fields', () => {
    const guard = new EnvAccessGuard();
    guard.check('MY_VAR', 'set', 'engine');
    const ev = guard.getEvents()[0];
    assert.equal(ev.variable, 'MY_VAR');
    assert.equal(ev.operation, 'set');
    assert.equal(ev.callerTier, 'engine');
    assert.equal(ev.allowed, true);
    assert.equal(typeof ev.timestamp, 'number');
  });
});
