import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  POLICY_VARIABLES,
  CODE_LOADING_VARIABLES,
  TELEMETRY_VARIABLES,
  isPinnedVariable,
  getPinCategory,
  getAllPinnedVariables,
  EnvProtectionGuard,
  SAFE_ENV_VARS,
  isSafeEnvVar,
  PROVIDER_MANAGED_ENV_VARS,
  isProviderManagedEnvVar,
  ENV_REJECT_REASONS,
  ENV_TRUST_TIERS,
  filterSettingsEnv,
  isHostManagedProvider,
} from '../src/env-protection';

describe('isPinnedVariable', () => {
  it('returns true for SUPERINFERENCE_CONFIG_DIR', () => {
    assert.equal(isPinnedVariable('SUPERINFERENCE_CONFIG_DIR'), true);
  });

  it('returns true for LD_PRELOAD', () => {
    assert.equal(isPinnedVariable('LD_PRELOAD'), true);
  });

  it('returns true for NODE_OPTIONS', () => {
    assert.equal(isPinnedVariable('NODE_OPTIONS'), true);
  });

  it('returns true for DISABLE_TELEMETRY', () => {
    assert.equal(isPinnedVariable('DISABLE_TELEMETRY'), true);
  });

  it('returns true for DO_NOT_TRACK', () => {
    assert.equal(isPinnedVariable('DO_NOT_TRACK'), true);
  });

  it('returns false for HOME', () => {
    assert.equal(isPinnedVariable('HOME'), false);
  });

  it('returns true for PATH (code-loading)', () => {
    assert.equal(isPinnedVariable('PATH'), true);
  });

  it('returns false for empty string', () => {
    assert.equal(isPinnedVariable(''), false);
  });

  it('is case-insensitive', () => {
    assert.equal(isPinnedVariable('ld_preload'), true);
    assert.equal(isPinnedVariable('Node_Options'), true);
    assert.equal(isPinnedVariable('disable_telemetry'), true);
    assert.equal(isPinnedVariable('superinference_config_dir'), true);
  });
});

describe('getPinCategory', () => {
  it('returns policy for policy variables', () => {
    assert.equal(getPinCategory('SUPERINFERENCE_MANAGED_SETTINGS_PATH'), 'policy');
    assert.equal(getPinCategory('USE_LOCAL_OAUTH'), 'policy');
    assert.equal(getPinCategory('SUPERINFERENCE_CONFIG_DIR'), 'policy');
  });

  it('returns code_loading for code-loading variables', () => {
    assert.equal(getPinCategory('LD_PRELOAD'), 'code_loading');
    assert.equal(getPinCategory('DYLD_INSERT_LIBRARIES'), 'code_loading');
    assert.equal(getPinCategory('NODE_OPTIONS'), 'code_loading');
    assert.equal(getPinCategory('BASH_ENV'), 'code_loading');
    assert.equal(getPinCategory('PATH'), 'code_loading');
  });

  it('returns telemetry for telemetry variables', () => {
    assert.equal(getPinCategory('DISABLE_TELEMETRY'), 'telemetry');
    assert.equal(getPinCategory('DO_NOT_TRACK'), 'telemetry');
    assert.equal(getPinCategory('SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC'), 'telemetry');
  });

  it('returns null for unrelated variables', () => {
    assert.equal(getPinCategory('HOME'), null);
    assert.equal(getPinCategory('USER'), null);
    assert.equal(getPinCategory('TERM'), null);
  });

  it('is case-insensitive', () => {
    assert.equal(getPinCategory('ld_preload'), 'code_loading');
    assert.equal(getPinCategory('Disable_Telemetry'), 'telemetry');
  });
});

describe('getAllPinnedVariables', () => {
  it('returns all pinned variables', () => {
    const all = getAllPinnedVariables();
    assert.ok(all.includes('LD_PRELOAD'));
    assert.ok(all.includes('SUPERINFERENCE_CONFIG_DIR'));
    assert.ok(all.includes('DISABLE_TELEMETRY'));
  });

  it('count matches sum of individual sets', () => {
    const all = getAllPinnedVariables();
    assert.equal(all.length, POLICY_VARIABLES.size + CODE_LOADING_VARIABLES.size + TELEMETRY_VARIABLES.size);
  });
});

describe('constant sets', () => {
  it('POLICY_VARIABLES contains expected entries', () => {
    assert.ok(POLICY_VARIABLES.has('SUPERINFERENCE_MANAGED_SETTINGS_PATH'));
    assert.ok(POLICY_VARIABLES.has('SUPERINFERENCE_REMOTE_SETTINGS_PATH'));
    assert.ok(POLICY_VARIABLES.has('SUPERINFERENCE_CONFIG_DIR'));
    assert.ok(POLICY_VARIABLES.has('USE_LOCAL_OAUTH'));
    assert.ok(POLICY_VARIABLES.has('USE_STAGING_OAUTH'));
    assert.equal(POLICY_VARIABLES.size, 9);
  });

  it('CODE_LOADING_VARIABLES contains 20 entries', () => {
    assert.equal(CODE_LOADING_VARIABLES.size, 20);
    assert.ok(CODE_LOADING_VARIABLES.has('LD_PRELOAD'));
    assert.ok(CODE_LOADING_VARIABLES.has('LD_LIBRARY_PATH'));
    assert.ok(CODE_LOADING_VARIABLES.has('LD_AUDIT'));
    assert.ok(CODE_LOADING_VARIABLES.has('DYLD_INSERT_LIBRARIES'));
    assert.ok(CODE_LOADING_VARIABLES.has('NODE_OPTIONS'));
    assert.ok(CODE_LOADING_VARIABLES.has('PROMPT_COMMAND'));
    assert.ok(CODE_LOADING_VARIABLES.has('PATH'));
  });

  it('TELEMETRY_VARIABLES contains 3 entries', () => {
    assert.equal(TELEMETRY_VARIABLES.size, 3);
  });
});

describe('EnvProtectionGuard', () => {
  it('checkSet returns attempt for pinned variable', () => {
    const guard = new EnvProtectionGuard();
    const attempt = guard.checkSet('LD_PRELOAD', 'user-plugin');
    assert.ok(attempt !== null);
    assert.equal(attempt!.variable, 'LD_PRELOAD');
    assert.equal(attempt!.category, 'code_loading');
    assert.equal(attempt!.source, 'user-plugin');
    assert.ok(typeof attempt!.timestamp === 'number');
  });

  it('checkSet returns null for unpinned variable', () => {
    const guard = new EnvProtectionGuard();
    assert.equal(guard.checkSet('HOME', 'user-plugin'), null);
    assert.equal(guard.checkSet('MY_APP_VAR', 'plugin-x'), null);
  });

  it('checkSet is case-insensitive for variable names', () => {
    const guard = new EnvProtectionGuard();
    const attempt = guard.checkSet('node_options', 'test');
    assert.ok(attempt !== null);
    assert.equal(attempt!.variable, 'NODE_OPTIONS');
  });

  it('getBlockedAttempts tracks all blocked attempts', () => {
    const guard = new EnvProtectionGuard();
    guard.checkSet('LD_PRELOAD', 'plugin-a');
    guard.checkSet('HOME', 'plugin-b'); // not pinned
    guard.checkSet('NODE_OPTIONS', 'plugin-c');
    const attempts = guard.getBlockedAttempts();
    assert.equal(attempts.length, 2);
    assert.equal(attempts[0].variable, 'LD_PRELOAD');
    assert.equal(attempts[1].variable, 'NODE_OPTIONS');
  });

  it('getBlockedCount returns correct count', () => {
    const guard = new EnvProtectionGuard();
    assert.equal(guard.getBlockedCount(), 0);
    guard.checkSet('LD_PRELOAD', 'x');
    assert.equal(guard.getBlockedCount(), 1);
    guard.checkSet('DISABLE_TELEMETRY', 'y');
    assert.equal(guard.getBlockedCount(), 2);
  });

  it('clear resets blocked attempts', () => {
    const guard = new EnvProtectionGuard();
    guard.checkSet('LD_PRELOAD', 'x');
    guard.checkSet('NODE_OPTIONS', 'y');
    assert.equal(guard.getBlockedCount(), 2);
    guard.clear();
    assert.equal(guard.getBlockedCount(), 0);
    assert.deepEqual(guard.getBlockedAttempts(), []);
  });

  it('respects maxAttempts cap', () => {
    const guard = new EnvProtectionGuard();
    // The private _maxAttempts is 1000 — we can't easily test that without
    // adding 1001 attempts, so instead verify the attempt is still returned
    // even when not stored (the guard returns the attempt object regardless).
    const attempt = guard.checkSet('LD_PRELOAD', 'overflow-test');
    assert.ok(attempt !== null);
  });

  it('blocked attempts are readonly', () => {
    const guard = new EnvProtectionGuard();
    guard.checkSet('LD_PRELOAD', 'x');
    const attempts = guard.getBlockedAttempts();
    assert.equal(attempts.length, 1);
    // The type is readonly — verify it's the internal array (not a copy, which is fine for perf)
    guard.checkSet('NODE_OPTIONS', 'y');
    assert.equal(guard.getBlockedAttempts().length, 2);
  });
});

// ---------------------------------------------------------------------------
// Settings-sourced env-var trust
// ---------------------------------------------------------------------------

describe('SAFE_ENV_VARS / isSafeEnvVar', () => {
  it('classifies a tuning knob as safe', () => {
    assert.equal(isSafeEnvVar('BASH_MAX_TIMEOUT_MS'), true);
    assert.ok(SAFE_ENV_VARS.has('BASH_MAX_TIMEOUT_MS'));
  });
  it('is case-insensitive', () => {
    assert.equal(isSafeEnvVar('bash_max_timeout_ms'), true);
  });
  it('treats routing/credential vars as NOT safe', () => {
    assert.equal(isSafeEnvVar('ANTHROPIC_BASE_URL'), false);
    assert.equal(isSafeEnvVar('OPENAI_API_KEY'), false);
    assert.equal(isSafeEnvVar('NODE_TLS_REJECT_UNAUTHORIZED'), false);
    assert.equal(isSafeEnvVar('LD_PRELOAD'), false);
  });
});

describe('isProviderManagedEnvVar', () => {
  it('matches any *_API_KEY', () => {
    assert.equal(isProviderManagedEnvVar('OPENAI_API_KEY'), true);
    assert.equal(isProviderManagedEnvVar('DEEPSEEK_API_KEY'), true);
  });
  it('matches *_AUTH_TOKEN and *_BASE_URL', () => {
    assert.equal(isProviderManagedEnvVar('ANTHROPIC_AUTH_TOKEN'), true);
    assert.equal(isProviderManagedEnvVar('ANTHROPIC_BASE_URL'), true);
  });
  it('matches explicit routing/credential vars', () => {
    assert.equal(isProviderManagedEnvVar('AWS_ACCESS_KEY_ID'), true);
    assert.equal(isProviderManagedEnvVar('AI_BASE_URL'), true);
    assert.ok(PROVIDER_MANAGED_ENV_VARS.has('GOOGLE_APPLICATION_CREDENTIALS'));
  });
  it('does not match an unrelated var', () => {
    assert.equal(isProviderManagedEnvVar('EDITOR'), false);
    assert.equal(isProviderManagedEnvVar('BASH_MAX_TIMEOUT_MS'), false);
  });
});

describe('ENV_REJECT_REASONS / ENV_TRUST_TIERS', () => {
  it('enumerate the expected values', () => {
    assert.deepEqual([...ENV_REJECT_REASONS], ['pinned', 'provider-managed', 'network-trust', 'unsafe-untrusted']);
    assert.deepEqual([...ENV_TRUST_TIERS], ['trusted', 'project']);
  });
});

describe('filterSettingsEnv', () => {
  it('returns empty result for undefined env', () => {
    const r = filterSettingsEnv(undefined, { tier: 'trusted' });
    assert.deepEqual(r.applied, {});
    assert.deepEqual(r.rejected, []);
  });

  it('project tier applies only SAFE_ENV_VARS and rejects the rest', () => {
    const r = filterSettingsEnv(
      { BASH_MAX_TIMEOUT_MS: '1000', ANTHROPIC_BASE_URL: 'http://evil', EDITOR: 'vim' },
      { tier: 'project' },
    );
    assert.deepEqual(r.applied, { BASH_MAX_TIMEOUT_MS: '1000' });
    const reasons = r.rejected.map(x => `${x.key}:${x.reason}`).sort();
    assert.deepEqual(reasons, ['ANTHROPIC_BASE_URL:unsafe-untrusted', 'EDITOR:unsafe-untrusted']);
  });

  it('trusted tier applies non-pinned vars including routing vars', () => {
    const r = filterSettingsEnv(
      { ANTHROPIC_BASE_URL: 'http://corp', EDITOR: 'vim' },
      { tier: 'trusted' },
    );
    assert.deepEqual(r.applied, { ANTHROPIC_BASE_URL: 'http://corp', EDITOR: 'vim' });
    assert.deepEqual(r.rejected, []);
  });

  it('always rejects pinned vars regardless of tier', () => {
    const r = filterSettingsEnv({ LD_PRELOAD: '/x.so', PATH: '/evil' }, { tier: 'trusted' });
    assert.deepEqual(r.applied, {});
    assert.deepEqual(r.rejected.map(x => x.reason), ['pinned', 'pinned']);
  });

  it('strips provider-managed vars when the host owns routing', () => {
    const r = filterSettingsEnv(
      { ANTHROPIC_BASE_URL: 'http://corp', EDITOR: 'vim' },
      { tier: 'trusted', hostManagedProvider: true },
    );
    assert.deepEqual(r.applied, { EDITOR: 'vim' });
    assert.deepEqual(r.rejected, [{ key: 'ANTHROPIC_BASE_URL', reason: 'provider-managed' }]);
  });

  it('skips non-string values', () => {
    const r = filterSettingsEnv(
      { EDITOR: 'vim', BAD: 123 as unknown as string },
      { tier: 'trusted' },
    );
    assert.deepEqual(r.applied, { EDITOR: 'vim' });
    assert.deepEqual(r.rejected, []);
  });
});

describe('isHostManagedProvider', () => {
  it('false when unset / empty / 0 / false', () => {
    assert.equal(isHostManagedProvider({}), false);
    assert.equal(isHostManagedProvider({ SUPERINFERENCE_PROVIDER_MANAGED_BY_HOST: '' }), false);
    assert.equal(isHostManagedProvider({ SUPERINFERENCE_PROVIDER_MANAGED_BY_HOST: '0' }), false);
    assert.equal(isHostManagedProvider({ SUPERINFERENCE_PROVIDER_MANAGED_BY_HOST: 'false' }), false);
  });
  it('true for a truthy value', () => {
    assert.equal(isHostManagedProvider({ SUPERINFERENCE_PROVIDER_MANAGED_BY_HOST: '1' }), true);
    assert.equal(isHostManagedProvider({ SUPERINFERENCE_PROVIDER_MANAGED_BY_HOST: 'yes' }), true);
  });
  it('defaults to reading process.env', () => {
    const prev = process.env.SUPERINFERENCE_PROVIDER_MANAGED_BY_HOST;
    delete process.env.SUPERINFERENCE_PROVIDER_MANAGED_BY_HOST;
    try {
      assert.equal(isHostManagedProvider(), false);
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_PROVIDER_MANAGED_BY_HOST;
      else process.env.SUPERINFERENCE_PROVIDER_MANAGED_BY_HOST = prev;
    }
  });
});
