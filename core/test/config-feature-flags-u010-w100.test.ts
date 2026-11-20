import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  featureFlagEnvName,
  isFeatureEnabled,
  subsystemEnabled,
  isWorkflowScriptsEnabled,
  isCronEnabled,
  isLspEnabled,
  isTeammatesEnabled,
  WORKFLOW_SCRIPTS_ENV,
  CRON_ENV,
  LSP_ENV,
  AGENT_TEAMS_ENV,
  WORKFLOW_SCRIPTS_FEATURE,
  createDefaultTools,
} from '../src/tools/index';
import { applyConfigFeatureFlags, applyConfigStartupKnobs } from '../src/config';

// U010 (w100) — generic feature() capability gate + config.features bridge.
// Parity: the reference's `features` map / `feature()` conditional task registration.

describe('featureFlagEnvName — normalization', () => {
  it('upper-cases and collapses non-alnum runs to a single underscore', () => {
    assert.equal(featureFlagEnvName('workflow-scripts'), 'SUPERINFERENCE_FEATURE_WORKFLOW_SCRIPTS');
    assert.equal(featureFlagEnvName('WORKFLOW_SCRIPTS'), 'SUPERINFERENCE_FEATURE_WORKFLOW_SCRIPTS');
    assert.equal(featureFlagEnvName('Monitor Tool'), 'SUPERINFERENCE_FEATURE_MONITOR_TOOL');
    assert.equal(featureFlagEnvName('  bash.classifier  '), 'SUPERINFERENCE_FEATURE_BASH_CLASSIFIER');
  });

  it('uses the Claude-compatible prefix when requested', () => {
    assert.equal(featureFlagEnvName('cron', 'CLAUDE_CODE'), 'CLAUDE_CODE_FEATURE_CRON');
  });
});

describe('isFeatureEnabled — resolution precedence', () => {
  it('defaults to the supplied default when no env is set', () => {
    assert.equal(isFeatureEnabled('CRON', {}), true);
    assert.equal(isFeatureEnabled('CRON', {}, false), false);
  });

  it('AMI-native env override decides via truthy/falsey tokens', () => {
    assert.equal(isFeatureEnabled('CRON', { SUPERINFERENCE_FEATURE_CRON: '0' }), false);
    assert.equal(isFeatureEnabled('CRON', { SUPERINFERENCE_FEATURE_CRON: 'off' }, true), false);
    assert.equal(isFeatureEnabled('CRON', { SUPERINFERENCE_FEATURE_CRON: '1' }, false), true);
  });

  it('empty/whitespace env is ignored (falls through to default)', () => {
    assert.equal(isFeatureEnabled('CRON', { SUPERINFERENCE_FEATURE_CRON: '   ' }, false), false);
  });

  it('Claude-compatible alias is honored when the native name is unset', () => {
    assert.equal(isFeatureEnabled('CRON', { CLAUDE_CODE_FEATURE_CRON: '0' }), false);
  });

  it('AMI-native name wins over the Claude alias', () => {
    assert.equal(
      isFeatureEnabled('CRON', { SUPERINFERENCE_FEATURE_CRON: '1', CLAUDE_CODE_FEATURE_CRON: '0' }),
      true,
    );
  });
});

describe('subsystemEnabled — dedicated env beats generic feature flag', () => {
  it('dedicated env (when set) wins over the feature flag', () => {
    assert.equal(
      subsystemEnabled('0', 'CRON', { SUPERINFERENCE_FEATURE_CRON: '1' }),
      false,
    );
    assert.equal(
      subsystemEnabled('1', 'CRON', { SUPERINFERENCE_FEATURE_CRON: '0' }),
      true,
    );
  });

  it('falls back to the feature flag when the dedicated env is unset/empty', () => {
    assert.equal(subsystemEnabled(undefined, 'CRON', { SUPERINFERENCE_FEATURE_CRON: '0' }), false);
    assert.equal(subsystemEnabled('   ', 'CRON', { SUPERINFERENCE_FEATURE_CRON: '0' }), false);
  });

  it('defaults ON when neither signal is present', () => {
    assert.equal(subsystemEnabled(undefined, 'CRON', {}), true);
  });
});

describe('subsystem gates honor the generic feature flag', () => {
  it('each subsystem is disabled by its generic SUPERINFERENCE_FEATURE_<NAME> flag', () => {
    assert.equal(isWorkflowScriptsEnabled({ SUPERINFERENCE_FEATURE_WORKFLOW_SCRIPTS: '0' }), false);
    assert.equal(isCronEnabled({ SUPERINFERENCE_FEATURE_CRON: 'false' }), false);
    assert.equal(isLspEnabled({ SUPERINFERENCE_FEATURE_LSP: 'off' }), false);
    assert.equal(isTeammatesEnabled({ SUPERINFERENCE_FEATURE_AGENT_TEAMS: '0' }, []), false);
  });

  it('the dedicated env var still overrides the feature flag', () => {
    // dedicated enables even though the generic flag disables
    assert.equal(
      isWorkflowScriptsEnabled({ [WORKFLOW_SCRIPTS_ENV]: '1', SUPERINFERENCE_FEATURE_WORKFLOW_SCRIPTS: '0' }),
      true,
    );
    assert.equal(
      isCronEnabled({ [CRON_ENV]: '1', SUPERINFERENCE_FEATURE_CRON: '0' }),
      true,
    );
    assert.equal(
      isLspEnabled({ [LSP_ENV]: '1', SUPERINFERENCE_FEATURE_LSP: '0' }),
      true,
    );
    assert.equal(
      isTeammatesEnabled({ [AGENT_TEAMS_ENV]: '1', SUPERINFERENCE_FEATURE_AGENT_TEAMS: '0' }, []),
      true,
    );
  });

  it('all gates still default ON with no signals', () => {
    assert.equal(isWorkflowScriptsEnabled({}), true);
    assert.equal(isCronEnabled({}), true);
    assert.equal(isLspEnabled({}), true);
    assert.equal(isTeammatesEnabled({}, []), true);
  });
});

describe('applyConfigFeatureFlags — config.features → env bridge', () => {
  it('bridges a boolean feature entry into the native env var', () => {
    const env: NodeJS.ProcessEnv = {};
    applyConfigFeatureFlags({ features: { WORKFLOW_SCRIPTS: false, CRON: true } }, env);
    assert.equal(env.SUPERINFERENCE_FEATURE_WORKFLOW_SCRIPTS, '0');
    assert.equal(env.SUPERINFERENCE_FEATURE_CRON, '1');
  });

  it('does not clobber an already-set env override (env wins)', () => {
    const env: NodeJS.ProcessEnv = { SUPERINFERENCE_FEATURE_CRON: '1' };
    applyConfigFeatureFlags({ features: { CRON: false } }, env);
    assert.equal(env.SUPERINFERENCE_FEATURE_CRON, '1');
  });

  it('does not clobber an already-set Claude-compatible alias', () => {
    const env: NodeJS.ProcessEnv = { CLAUDE_CODE_FEATURE_CRON: '1' };
    applyConfigFeatureFlags({ features: { CRON: false } }, env);
    assert.equal(env.SUPERINFERENCE_FEATURE_CRON, undefined);
  });

  it('ignores non-boolean entries and a missing map', () => {
    const env: NodeJS.ProcessEnv = {};
    applyConfigFeatureFlags({ features: { WEIRD: 'yes' as unknown as boolean } }, env);
    assert.equal(env.SUPERINFERENCE_FEATURE_WEIRD, undefined);
    applyConfigFeatureFlags(null, env);
    applyConfigFeatureFlags({}, env);
    assert.deepEqual(env, {});
  });

  it('normalizes the feature key the same way the gate reads it', () => {
    const env: NodeJS.ProcessEnv = {};
    applyConfigFeatureFlags({ features: { 'workflow-scripts': false } }, env);
    // The gate reads SUPERINFERENCE_FEATURE_WORKFLOW_SCRIPTS, so the disable takes effect.
    assert.equal(isWorkflowScriptsEnabled(env), false);
  });
});

describe('applyConfigStartupKnobs includes the feature-flag bridge', () => {
  it('a disabled feature in config.features reaches the env through the aggregator', () => {
    const env: NodeJS.ProcessEnv = {};
    applyConfigStartupKnobs({ features: { CRON: false } }, env);
    assert.equal(env.SUPERINFERENCE_FEATURE_CRON, '0');
    assert.equal(isCronEnabled(env), false);
  });
});

describe('end-to-end — config.features gates tool registration', () => {
  const savedArgv = process.argv;
  afterEach(() => {
    process.argv = savedArgv;
  });

  function toolNames(env: NodeJS.ProcessEnv): Set<string> {
    const saved = process.env;
    process.env = env;
    try {
      return new Set(createDefaultTools('/tmp').getAll().map((t) => t.name));
    } finally {
      process.env = saved;
    }
  }

  it('config.features disabling WORKFLOW_SCRIPTS drops the workflow tool (others stay)', () => {
    process.argv = ['node', 'test'];
    const env: NodeJS.ProcessEnv = {};
    applyConfigStartupKnobs({ features: { [WORKFLOW_SCRIPTS_FEATURE]: false } }, env);
    const names = toolNames(env);
    assert.ok(!names.has('workflow'), 'workflow tool should be dropped by config.features');
    assert.ok(names.has('lsp'), 'other gated subsystems remain');
    assert.ok(names.has('cron_list'), 'cron subsystem remains');
  });

  it('config.features enabling (true) keeps the subsystem registered', () => {
    process.argv = ['node', 'test'];
    const env: NodeJS.ProcessEnv = {};
    applyConfigStartupKnobs({ features: { [WORKFLOW_SCRIPTS_FEATURE]: true } }, env);
    assert.ok(toolNames(env).has('workflow'), 'workflow tool stays registered');
  });
});
