import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { sanitizeEnvForSubprocess, SENSITIVE_ENV_SET } from '../src/tools/bash';

// ---------------------------------------------------------------------------
// sanitizeEnvForSubprocess — unit tests
// ---------------------------------------------------------------------------

describe('sanitizeEnvForSubprocess', () => {
  // ----- Removal of known sensitive vars -----

  it('removes AWS_SECRET_ACCESS_KEY', () => {
    const env = { PATH: '/usr/bin', AWS_SECRET_ACCESS_KEY: 'secret123' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.AWS_SECRET_ACCESS_KEY, undefined);
  });

  it('removes AWS_ACCESS_KEY_ID', () => {
    const env = { PATH: '/usr/bin', AWS_ACCESS_KEY_ID: 'AKIAIOSFODNN7EXAMPLE' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.AWS_ACCESS_KEY_ID, undefined);
  });

  it('removes AWS_SESSION_TOKEN', () => {
    const env = { PATH: '/usr/bin', AWS_SESSION_TOKEN: 'token123' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.AWS_SESSION_TOKEN, undefined);
  });

  it('removes GITHUB_TOKEN', () => {
    const env = { PATH: '/usr/bin', GITHUB_TOKEN: 'ghp_abc123' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.GITHUB_TOKEN, undefined);
  });

  it('removes GH_TOKEN', () => {
    const env = { PATH: '/usr/bin', GH_TOKEN: 'ghp_abc123' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.GH_TOKEN, undefined);
  });

  it('removes ANTHROPIC_API_KEY', () => {
    const env = { PATH: '/usr/bin', ANTHROPIC_API_KEY: 'sk-ant-api03-xxx' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.ANTHROPIC_API_KEY, undefined);
  });

  it('removes OPENAI_API_KEY', () => {
    const env = { PATH: '/usr/bin', OPENAI_API_KEY: 'sk-xxx' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.OPENAI_API_KEY, undefined);
  });

  it('removes GOOGLE_APPLICATION_CREDENTIALS', () => {
    const env = { PATH: '/usr/bin', GOOGLE_APPLICATION_CREDENTIALS: '/path/to/creds.json' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.GOOGLE_APPLICATION_CREDENTIALS, undefined);
  });

  it('removes GOOGLE_API_KEY', () => {
    const env = { PATH: '/usr/bin', GOOGLE_API_KEY: 'AIza_xxx' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.GOOGLE_API_KEY, undefined);
  });

  it('removes DATABASE_URL', () => {
    const env = { PATH: '/usr/bin', DATABASE_URL: 'postgres://user:pass@host/db' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.DATABASE_URL, undefined);
  });

  it('removes REDIS_URL', () => {
    const env = { PATH: '/usr/bin', REDIS_URL: 'redis://host:6379' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.REDIS_URL, undefined);
  });

  it('removes NPM_TOKEN', () => {
    const env = { PATH: '/usr/bin', NPM_TOKEN: 'npm_xxx' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.NPM_TOKEN, undefined);
  });

  it('removes PYPI_TOKEN', () => {
    const env = { PATH: '/usr/bin', PYPI_TOKEN: 'pypi-xxx' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.PYPI_TOKEN, undefined);
  });

  it('removes GITHUB_APP_PRIVATE_KEY', () => {
    const env = { PATH: '/usr/bin', GITHUB_APP_PRIVATE_KEY: '-----BEGIN RSA...' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.GITHUB_APP_PRIVATE_KEY, undefined);
  });

  // ----- Pattern-based removal -----

  it('removes vars matching *_PASSWORD', () => {
    const env = { PATH: '/usr/bin', DB_PASSWORD: 'secret', MY_PASSWORD: 'secret2' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.DB_PASSWORD, undefined);
    assert.equal(result.MY_PASSWORD, undefined);
  });

  it('removes vars matching *_SECRET', () => {
    const env = { PATH: '/usr/bin', CLIENT_SECRET: 's3cret', MY_SECRET: 'val' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.CLIENT_SECRET, undefined);
    assert.equal(result.MY_SECRET, undefined);
  });

  it('removes vars matching *_PRIVATE_KEY', () => {
    const env = { PATH: '/usr/bin', SSH_PRIVATE_KEY: 'key-data' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.SSH_PRIVATE_KEY, undefined);
  });

  it('removes vars matching *_API_KEY', () => {
    const env = { PATH: '/usr/bin', STRIPE_API_KEY: 'sk_test_xxx' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.STRIPE_API_KEY, undefined);
  });

  it('removes vars matching *_CREDENTIALS', () => {
    const env = { PATH: '/usr/bin', APP_CREDENTIALS: 'cred-val' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.APP_CREDENTIALS, undefined);
  });

  // ----- Safe vars preserved -----

  it('keeps PATH', () => {
    const env = { PATH: '/usr/bin:/usr/local/bin', ANTHROPIC_API_KEY: 'x' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.PATH, '/usr/bin:/usr/local/bin');
  });

  it('keeps HOME', () => {
    const env = { HOME: '/home/user', OPENAI_API_KEY: 'x' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.HOME, '/home/user');
  });

  it('keeps USER', () => {
    const env = { USER: 'testuser' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.USER, 'testuser');
  });

  it('keeps SHELL', () => {
    const env = { SHELL: '/bin/bash' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.SHELL, '/bin/bash');
  });

  it('keeps TERM', () => {
    const env = { TERM: 'xterm-256color' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.TERM, 'xterm-256color');
  });

  it('keeps LANG', () => {
    const env = { LANG: 'en_US.UTF-8' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.LANG, 'en_US.UTF-8');
  });

  it('keeps LC_* vars', () => {
    const env = { LC_ALL: 'C', LC_CTYPE: 'UTF-8' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.LC_ALL, 'C');
    assert.equal(result.LC_CTYPE, 'UTF-8');
  });

  it('keeps DISPLAY', () => {
    const env = { DISPLAY: ':0' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.DISPLAY, ':0');
  });

  it('keeps XDG_* vars', () => {
    const env = { XDG_RUNTIME_DIR: '/run/user/1000', XDG_DATA_HOME: '/home/user/.local/share' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.XDG_RUNTIME_DIR, '/run/user/1000');
    assert.equal(result.XDG_DATA_HOME, '/home/user/.local/share');
  });

  it('keeps TMPDIR', () => {
    const env = { TMPDIR: '/tmp' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.TMPDIR, '/tmp');
  });

  it('keeps EDITOR and VISUAL (overridden by scrubEnv)', () => {
    // sanitizeEnvForSubprocess only strips secrets, doesn't add overrides
    const env = { EDITOR: 'vim', VISUAL: 'code' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.EDITOR, 'vim');
    assert.equal(result.VISUAL, 'code');
  });

  it('keeps SUPERINFERENCE_* vars', () => {
    const env = { SUPERINFERENCE_DEBUG: '1', SUPERINFERENCE_CWD: '/test' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.SUPERINFERENCE_DEBUG, '1');
    assert.equal(result.SUPERINFERENCE_CWD, '/test');
  });

  it('keeps NODE_PATH', () => {
    const env = { NODE_PATH: '/usr/lib/node_modules' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.NODE_PATH, '/usr/lib/node_modules');
  });

  it('keeps NODE_OPTIONS', () => {
    const env = { NODE_OPTIONS: '--max-old-space-size=4096' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.NODE_OPTIONS, '--max-old-space-size=4096');
  });

  it('keeps AWS_REGION (non-sensitive AWS var)', () => {
    const env = { AWS_REGION: 'us-east-1', AWS_SECRET_ACCESS_KEY: 'xxx' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.AWS_REGION, 'us-east-1');
    assert.equal(result.AWS_SECRET_ACCESS_KEY, undefined);
  });

  it('keeps AWS_DEFAULT_REGION', () => {
    const env = { AWS_DEFAULT_REGION: 'eu-west-1' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.AWS_DEFAULT_REGION, 'eu-west-1');
  });

  // ----- Edge cases -----

  it('empty env returns empty object', () => {
    const result = sanitizeEnvForSubprocess({});
    assert.deepEqual(result, {});
  });

  it('env with only safe vars returns all of them', () => {
    const env = { PATH: '/usr/bin', HOME: '/home/user', TERM: 'xterm' };
    const result = sanitizeEnvForSubprocess(env);
    assert.deepEqual(result, env);
  });

  it('env with only sensitive vars returns empty object', () => {
    const env = { ANTHROPIC_API_KEY: 'x', OPENAI_API_KEY: 'y', DB_PASSWORD: 'z' };
    const result = sanitizeEnvForSubprocess(env);
    assert.deepEqual(result, {});
  });

  it('case-insensitive check on pattern suffixes', () => {
    const env = { my_api_key: 'val', My_Password: 'val2' };
    const result = sanitizeEnvForSubprocess(env);
    assert.equal(result.my_api_key, undefined);
    assert.equal(result.My_Password, undefined);
  });

  it('does not modify the input object', () => {
    const env = { PATH: '/usr/bin', ANTHROPIC_API_KEY: 'key' };
    sanitizeEnvForSubprocess(env);
    assert.equal(env.ANTHROPIC_API_KEY, 'key');
    assert.equal(env.PATH, '/usr/bin');
  });
});

// ---------------------------------------------------------------------------
// SENSITIVE_ENV_SET — explicit list membership
// ---------------------------------------------------------------------------

describe('SENSITIVE_ENV_SET', () => {
  it('contains GH_TOKEN', () => {
    assert.ok(SENSITIVE_ENV_SET.has('GH_TOKEN'));
  });

  it('contains PYPI_TOKEN', () => {
    assert.ok(SENSITIVE_ENV_SET.has('PYPI_TOKEN'));
  });

  it('contains GITHUB_APP_PRIVATE_KEY', () => {
    assert.ok(SENSITIVE_ENV_SET.has('GITHUB_APP_PRIVATE_KEY'));
  });

  it('contains GH_ENTERPRISE_TOKEN', () => {
    assert.ok(SENSITIVE_ENV_SET.has('GH_ENTERPRISE_TOKEN'));
  });

  it('contains GITHUB_ENTERPRISE_TOKEN', () => {
    assert.ok(SENSITIVE_ENV_SET.has('GITHUB_ENTERPRISE_TOKEN'));
  });

  it('contains ANTHROPIC_AUTH_TOKEN', () => {
    assert.ok(SENSITIVE_ENV_SET.has('ANTHROPIC_AUTH_TOKEN'));
  });

  it('contains NODE_AUTH_TOKEN', () => {
    assert.ok(SENSITIVE_ENV_SET.has('NODE_AUTH_TOKEN'));
  });

  it('contains YARN_NPM_AUTH_TOKEN', () => {
    assert.ok(SENSITIVE_ENV_SET.has('YARN_NPM_AUTH_TOKEN'));
  });

  it('contains YARN_NPM_AUTH_IDENT', () => {
    assert.ok(SENSITIVE_ENV_SET.has('YARN_NPM_AUTH_IDENT'));
  });

  it('contains COREPACK_NPM_TOKEN', () => {
    assert.ok(SENSITIVE_ENV_SET.has('COREPACK_NPM_TOKEN'));
  });

  it('contains COREPACK_NPM_PASSWORD', () => {
    assert.ok(SENSITIVE_ENV_SET.has('COREPACK_NPM_PASSWORD'));
  });

  it('contains COREPACK_NPM_USERNAME', () => {
    assert.ok(SENSITIVE_ENV_SET.has('COREPACK_NPM_USERNAME'));
  });

  it('contains BUN_CONFIG_TOKEN', () => {
    assert.ok(SENSITIVE_ENV_SET.has('BUN_CONFIG_TOKEN'));
  });
});
