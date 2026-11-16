import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isSensitiveEnvVar,
  canAccessEnv,
  SENSITIVE_ENV_VARS,
  EnvAccessGuard,
  classifyEnvValue,
  classifyEnvName,
  classifyEnvSecret,
  redactionMarker,
  redactSecretValue,
  REDACT_MIN_LENGTH,
  REDACT_PREVIEW_PREFIX,
  REDACT_PREVIEW_SUFFIX,
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
    assert.ok(SENSITIVE_ENV_VARS.has('PASSPHRASE'));
  });

  it('has 8 entries', () => {
    assert.equal(SENSITIVE_ENV_VARS.size, 8);
  });

  it('flags the mTLS client-key passphrase env var as sensitive', () => {
    assert.equal(isSensitiveEnvVar('AMI_CLIENT_KEY_PASSPHRASE'), true);
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
      assert.equal(result.value, '[REDACTED_API_KEY]');
      assert.equal(result.redactedAs, 'api_key');
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

describe('isSensitiveEnvVar — broadened name coverage (gaps 1560/2581)', () => {
  it('redacts bare TOKEN/SECRET family names the compound list missed', () => {
    assert.equal(isSensitiveEnvVar('GITHUB_TOKEN'), true);
    assert.equal(isSensitiveEnvVar('NPM_TOKEN'), true);
    assert.equal(isSensitiveEnvVar('SLACK_TOKEN'), true);
    assert.equal(isSensitiveEnvVar('NPM_SECRET'), true);
    assert.equal(isSensitiveEnvVar('DB_SECRET'), true);
  });

  it('redacts bare KEY / PASSWD and provider-prefixed secret names', () => {
    assert.equal(isSensitiveEnvVar('OPENAI_KEY'), true);
    assert.equal(isSensitiveEnvVar('SSH_KEY'), true);
    assert.equal(isSensitiveEnvVar('MYSQL_PASSWD'), true);
    assert.equal(isSensitiveEnvVar('AWS_SECRET_ACCESS_KEY'), true);
  });

  it('still returns false for genuinely non-secret names', () => {
    assert.equal(isSensitiveEnvVar('PATH'), false);
    assert.equal(isSensitiveEnvVar('HOME'), false);
    assert.equal(isSensitiveEnvVar('NODE_ENV'), false);
    assert.equal(isSensitiveEnvVar('MONKEY_BUSINESS'), false);
  });
});

describe('classifyEnvValue — value-shape detection (gap 129)', () => {
  it('detects AWS access-key IDs', () => {
    assert.equal(classifyEnvValue('AKIAIOSFODNN7EXAMPLE'), 'aws_key');
    assert.equal(classifyEnvValue('ASIAIOSFODNN7EXAMPLE'), 'aws_key');
  });

  it('detects GCP API keys', () => {
    assert.equal(classifyEnvValue('AIzaSyA1234567890abcdefghijklmnopqrstuv'), 'gcp_key');
  });

  it('detects GCP service-account emails', () => {
    assert.equal(
      classifyEnvValue('svc@my-proj.iam.gserviceaccount.com'),
      'gcp_service_account',
    );
  });

  it('detects Anthropic / OpenAI style keys', () => {
    assert.equal(classifyEnvValue('sk-ant-api03-ABCDEFGHIJKLMNOPQRST'), 'api_key');
    assert.equal(classifyEnvValue('sk-ABCDEFGHIJKLMNOPQRSTUVWX'), 'api_key');
  });

  it('detects GitHub classic and fine-grained tokens', () => {
    assert.equal(classifyEnvValue('ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'), 'token');
    assert.equal(classifyEnvValue('github_pat_11ABCDE0123456789_abcdefghij'), 'token');
  });

  it('detects Slack tokens', () => {
    assert.equal(classifyEnvValue('xoxb-1234567890-abcdefghij'), 'token');
  });

  it('detects bearer auth headers', () => {
    assert.equal(classifyEnvValue('Bearer abcdef0123456789'), 'token');
  });

  it('detects x-api-key headers', () => {
    assert.equal(classifyEnvValue('X-Api-Key: anything'), 'api_key');
  });

  it('returns undefined for non-secret-shaped values', () => {
    assert.equal(classifyEnvValue('just-a-plain-string'), undefined);
    assert.equal(classifyEnvValue(''), undefined);
  });
});

describe('classifyEnvName — name-based classification (gap 2582)', () => {
  it('classifies AWS, GCP, token, and api_key families', () => {
    assert.equal(classifyEnvName('AWS_SECRET_ACCESS_KEY'), 'aws_key');
    assert.equal(classifyEnvName('GOOGLE_API_KEY'), 'gcp_key');
    assert.equal(classifyEnvName('GCP_CREDENTIAL'), 'gcp_key');
    assert.equal(classifyEnvName('GITHUB_TOKEN'), 'token');
    assert.equal(classifyEnvName('MY_API_KEY'), 'api_key');
  });

  it('falls back to generic for sensitive names without a provider/key cue', () => {
    assert.equal(classifyEnvName('DB_PASSWORD'), 'generic');
    assert.equal(classifyEnvName('APP_PASSPHRASE'), 'generic');
  });

  it('returns undefined for non-sensitive names', () => {
    assert.equal(classifyEnvName('PATH'), undefined);
  });
});

describe('classifyEnvSecret — value precedence over name', () => {
  it('value shape wins even under an innocuous name', () => {
    assert.equal(classifyEnvSecret('MY_CONFIG', 'sk-ant-api03-ABCDEFGHIJKLMNOP'), 'api_key');
    assert.equal(classifyEnvSecret('X', 'AKIAIOSFODNN7EXAMPLE'), 'aws_key');
  });

  it('falls back to name classification when the value is not secret-shaped', () => {
    assert.equal(classifyEnvSecret('GITHUB_TOKEN', 'plain-looking'), 'token');
  });

  it('classifies by name when the value is undefined', () => {
    assert.equal(classifyEnvSecret('MY_API_KEY', undefined), 'api_key');
  });

  it('returns undefined when neither name nor value is sensitive', () => {
    assert.equal(classifyEnvSecret('PATH', '/usr/bin'), undefined);
  });
});

describe('EnvAccessGuard.safeGetEnv — value-shape redaction + typed labels', () => {
  it('redacts a secret-shaped value under an innocuously-named var (gap 129)', () => {
    const name = `TEST_INNOCENT_${process.pid}`;
    const original = process.env[name];
    process.env[name] = 'sk-ant-api03-ABCDEFGHIJKLMNOPQRST';
    try {
      const guard = new EnvAccessGuard();
      const result = guard.safeGetEnv(name);
      assert.equal(result.redacted, true);
      assert.equal(result.value, '[REDACTED_API_KEY]');
      assert.equal(result.redactedAs, 'api_key');
      assert.ok(!String(result.value).includes('sk-ant'), 'secret must not leak');
    } finally {
      if (original === undefined) delete process.env[name];
      else process.env[name] = original;
    }
  });

  it('classifies the redacted value by provider via redactedAs (gap 2582)', () => {
    const name = `TEST_AWS_CFG_${process.pid}`;
    const original = process.env[name];
    process.env[name] = 'AKIAIOSFODNN7EXAMPLE';
    try {
      const guard = new EnvAccessGuard();
      const result = guard.safeGetEnv(name);
      assert.equal(result.value, '[REDACTED_AWS_KEY]');
      assert.equal(result.redactedAs, 'aws_key');
    } finally {
      if (original === undefined) delete process.env[name];
      else process.env[name] = original;
    }
  });

  it('redacts an absent sensitive var as undefined with a class label', () => {
    const name = `TEST_ABSENT_TOKEN_${process.pid}`;
    delete process.env[name];
    const guard = new EnvAccessGuard();
    const result = guard.safeGetEnv(name);
    assert.equal(result.redacted, true);
    assert.equal(result.value, undefined);
    assert.equal(result.redactedAs, 'token');
  });

  it('emits a typed class-specific marker in the value (gap: typed labels)', () => {
    const name = `TEST_TYPED_TOKEN_${process.pid}`;
    const original = process.env[name];
    process.env[name] = 'ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    try {
      const guard = new EnvAccessGuard();
      const result = guard.safeGetEnv(name);
      assert.equal(result.value, '[REDACTED_TOKEN]');
      assert.equal(result.redactedAs, 'token');
    } finally {
      if (original === undefined) delete process.env[name];
      else process.env[name] = original;
    }
  });

  it('passes through a non-secret value unchanged', () => {
    const name = `TEST_PLAIN_${process.pid}`;
    const original = process.env[name];
    process.env[name] = 'visible-value';
    try {
      const guard = new EnvAccessGuard();
      const result = guard.safeGetEnv(name);
      assert.equal(result.redacted, false);
      assert.equal(result.value, 'visible-value');
      assert.equal(result.redactedAs, undefined);
    } finally {
      if (original === undefined) delete process.env[name];
      else process.env[name] = original;
    }
  });
});

describe('redactionMarker — typed, self-describing labels (gap: typed secret labels)', () => {
  it('maps each class to a [REDACTED_<CLASS>] label matching the reference', () => {
    assert.equal(redactionMarker('api_key'), '[REDACTED_API_KEY]');
    assert.equal(redactionMarker('aws_key'), '[REDACTED_AWS_KEY]');
    assert.equal(redactionMarker('gcp_key'), '[REDACTED_GCP_KEY]');
    assert.equal(redactionMarker('gcp_service_account'), '[REDACTED_GCP_SERVICE_ACCOUNT]');
    assert.equal(redactionMarker('token'), '[REDACTED_TOKEN]');
    assert.equal(redactionMarker('generic'), '[REDACTED_GENERIC]');
  });
});

describe('redactSecretValue — threshold + partial-reveal preview (gap: length-threshold params)', () => {
  it('exposes the reference threshold and slice defaults', () => {
    assert.equal(REDACT_MIN_LENGTH, 16);
    assert.equal(REDACT_PREVIEW_PREFIX, 8);
    assert.equal(REDACT_PREVIEW_SUFFIX, 4);
  });

  it('full-masks to the typed marker by default (no preview)', () => {
    assert.equal(
      redactSecretValue('sk-ant-api03-ABCDEFGHIJKLMNOPQRST', 'api_key'),
      '[REDACTED_API_KEY]',
    );
  });

  it('previews a long value as prefix(8)...suffix(4) without leaking the body', () => {
    const value = 'sk-ant-api03-ABCDEFGHIJKLMNOPQRST'; // >= REDACT_MIN_LENGTH
    const preview = redactSecretValue(value, 'api_key', true);
    assert.equal(preview, 'sk-ant-a...QRST');
    assert.equal(preview, `${value.slice(0, 8)}...${value.slice(-4)}`);
    assert.ok(!preview.includes('api03'), 'secret body must not leak');
  });

  it('full-masks a short value even with preview (too short to reveal safely)', () => {
    const value = 'short123'; // < REDACT_MIN_LENGTH (8 chars)
    assert.ok(value.length < REDACT_MIN_LENGTH);
    assert.equal(redactSecretValue(value, 'token', true), '[REDACTED_TOKEN]');
  });

  it('full-masks a value exactly one below the threshold with preview', () => {
    const value = 'a'.repeat(REDACT_MIN_LENGTH - 1);
    assert.equal(redactSecretValue(value, 'generic', true), '[REDACTED_GENERIC]');
  });
});

describe('EnvAccessGuard.safeGetEnv — opt-in preview (gap: partial reveal)', () => {
  it('returns a prefix...suffix preview when preview is requested', () => {
    const name = `TEST_PREVIEW_${process.pid}`;
    const original = process.env[name];
    const secret = 'sk-ant-api03-ABCDEFGHIJKLMNOPQRST';
    process.env[name] = secret;
    try {
      const guard = new EnvAccessGuard();
      const result = guard.safeGetEnv(name, { preview: true });
      assert.equal(result.redacted, true);
      assert.equal(result.redactedAs, 'api_key');
      assert.equal(result.value, `${secret.slice(0, 8)}...${secret.slice(-4)}`);
    } finally {
      if (original === undefined) delete process.env[name];
      else process.env[name] = original;
    }
  });

  it('keeps full masking as the default when preview is omitted', () => {
    const name = `TEST_NOPREVIEW_${process.pid}`;
    const original = process.env[name];
    process.env[name] = 'sk-ant-api03-ABCDEFGHIJKLMNOPQRST';
    try {
      const guard = new EnvAccessGuard();
      assert.equal(guard.safeGetEnv(name).value, '[REDACTED_API_KEY]');
    } finally {
      if (original === undefined) delete process.env[name];
      else process.env[name] = original;
    }
  });
});
