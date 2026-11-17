import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  scanConfigRiskSignals,
  hasAnyRiskSignal,
  scanConfigRiskSignalsBySource,
  detectConfigEscalationVectors,
} from '../src/safe-mode';

describe('scanConfigRiskSignals — structured per-capability detectors', () => {
  it('returns an all-empty breakdown for non-object / empty settings', () => {
    for (const bad of [null, undefined, 'x' as unknown, 42 as unknown, [] as unknown]) {
      const s = scanConfigRiskSignals(bad as Record<string, unknown>);
      assert.deepEqual(s, { commandHelpers: [], hooks: false, bashPermissions: [], dangerousEnvVars: [], mcpServers: [] });
      assert.equal(hasAnyRiskSignal(s), false);
    }
  });

  it('flags only concretely-set credential/command helpers', () => {
    const s = scanConfigRiskSignals({
      apiKeyHelper: '/bin/get-key',
      awsAuthRefresh: 'aws sso login',
      gcpCommands: 'gcloud auth',
      otelHeadersHelper: 'x',
      // not set → excluded:
      awsCredentialExport: '',
      gcloudAuthRefresh: false,
    });
    assert.deepEqual(s.commandHelpers, ['apiKeyHelper', 'awsAuthRefresh', 'gcpCommands', 'otelHeadersHelper']);
  });

  it('detects hooks only when the object is non-empty', () => {
    assert.equal(scanConfigRiskSignals({ hooks: { preToolUse: [{}] } }).hooks, true);
    assert.equal(scanConfigRiskSignals({ hooks: {} }).hooks, false);
    assert.equal(scanConfigRiskSignals({ hooks: null }).hooks, false);
    assert.equal(scanConfigRiskSignals({ hooks: 'nope' }).hooks, false);
  });

  it('detects bash-granting permission allow rules (Claude + AMI tool names)', () => {
    const s = scanConfigRiskSignals({
      permissions: { allow: ['Bash', 'Bash(git:*)', 'bash foo', 'Read', 'file_edit', 42] },
    });
    assert.deepEqual(s.bashPermissions, ['Bash', 'Bash(git:*)', 'bash foo']);
  });

  it('ignores permissions that are not an object with a string[] allow', () => {
    assert.deepEqual(scanConfigRiskSignals({ permissions: [] }).bashPermissions, []);
    assert.deepEqual(scanConfigRiskSignals({ permissions: 'x' }).bashPermissions, []);
    assert.deepEqual(scanConfigRiskSignals({ permissions: { allow: 'Bash' } }).bashPermissions, []);
    assert.deepEqual(scanConfigRiskSignals({ permissions: {} }).bashPermissions, []);
  });

  it('flags non-allowlisted env vars, case-insensitively, and never the safe ones', () => {
    const s = scanConfigRiskSignals({ env: { PATH: '/evil', node_options: '--require x', NODE_ENV: 'production', TERM: 'xterm' } });
    assert.deepEqual(s.dangerousEnvVars, ['PATH', 'node_options']);
    assert.deepEqual(scanConfigRiskSignals({ env: 'x' }).dangerousEnvVars, []);
    assert.deepEqual(scanConfigRiskSignals({ env: ['PATH'] }).dangerousEnvVars, []);
  });

  it('enumerates declared MCP server names', () => {
    assert.deepEqual(scanConfigRiskSignals({ mcpServers: { foo: {}, bar: {} } }).mcpServers, ['foo', 'bar']);
    assert.deepEqual(scanConfigRiskSignals({ mcpServers: [] }).mcpServers, []);
    assert.deepEqual(scanConfigRiskSignals({ mcpServers: 'x' }).mcpServers, []);
  });
});

describe('hasAnyRiskSignal', () => {
  it('is true when any single category is present, false when all empty', () => {
    const base = { commandHelpers: [], hooks: false, bashPermissions: [], dangerousEnvVars: [], mcpServers: [] };
    assert.equal(hasAnyRiskSignal(base), false);
    assert.equal(hasAnyRiskSignal({ ...base, commandHelpers: ['apiKeyHelper'] }), true);
    assert.equal(hasAnyRiskSignal({ ...base, hooks: true }), true);
    assert.equal(hasAnyRiskSignal({ ...base, bashPermissions: ['Bash'] }), true);
    assert.equal(hasAnyRiskSignal({ ...base, dangerousEnvVars: ['PATH'] }), true);
    assert.equal(hasAnyRiskSignal({ ...base, mcpServers: ['x'] }), true);
  });
});

describe('scanConfigRiskSignalsBySource — per-source attribution', () => {
  it('keeps only sources carrying a signal, keyed by source file', () => {
    const out = scanConfigRiskSignalsBySource({
      '.ami/settings.json': { hooks: { preToolUse: [{}] } },
      '.ami/settings.local.json': { apiKeyHelper: '/bin/x' },
      '.ami/clean.json': { model: 'opus' },
      '.ami/empty.json': null,
    });
    assert.deepEqual(Object.keys(out).sort(), ['.ami/settings.json', '.ami/settings.local.json']);
    assert.equal(out['.ami/settings.json']!.hooks, true);
    assert.deepEqual(out['.ami/settings.local.json']!.commandHelpers, ['apiKeyHelper']);
  });

  it('returns an empty map when no source carries a signal', () => {
    assert.deepEqual(scanConfigRiskSignalsBySource({ a: { model: 'opus' }, b: null }), {});
  });
});

describe('detectConfigEscalationVectors — unchanged high-risk flat set', () => {
  it('flags credential/command helpers and dangerous env, derived from the scanner', () => {
    assert.deepEqual(detectConfigEscalationVectors({ apiKeyHelper: '/bin/get-key' }), ['apiKeyHelper']);
    assert.ok(detectConfigEscalationVectors({ awsCredentialExport: 'x' }).includes('awsCredentialExport'));
    assert.ok(detectConfigEscalationVectors({ env: { PATH: '/evil' } }).includes('env.PATH'));
    assert.deepEqual(detectConfigEscalationVectors({ apiKeyHelper: '' }), []);
    assert.deepEqual(detectConfigEscalationVectors(null), []);
    assert.deepEqual(detectConfigEscalationVectors({ model: 'opus' }), []);
  });

  it('does NOT escalate on the softer trust-gating signals (hooks / bash-allow / mcp)', () => {
    // These require trust but must not force safe mode unconditionally (they are
    // routed through scanConfigRiskSignals / the trust decision, not this flat set).
    assert.deepEqual(detectConfigEscalationVectors({ hooks: { preToolUse: [{}] } }), []);
    assert.deepEqual(detectConfigEscalationVectors({ permissions: { allow: ['Bash'] } }), []);
    assert.deepEqual(detectConfigEscalationVectors({ mcpServers: { foo: {} } }), []);
  });
});
