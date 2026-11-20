/**
 * U018 — resolvePluginLspEnvironment: variable substitution for plugin-contributed
 * LSP server configs (parity: the reference's resolvePluginLspEnvironment,
 * lspPluginIntegration.ts:L229-292).
 *
 * Covers the plugin-scoped tier (${CLAUDE_PLUGIN_ROOT}, ${CLAUDE_PLUGIN_DATA},
 * ${user_config.X}), the general ${ENV} tier with missing-var tracking, the
 * CLAUDE_PLUGIN_ROOT/DATA env injection with author-override precedence, and
 * immutability of the input config.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { resolvePluginLspEnvironment, type LspServerConfig } from '../src/lsp/config';

const PLUGIN = {
  pluginRoot: '/opt/plugins/mylsp',
  pluginDataDir: '/var/data/mylsp',
};

describe('resolvePluginLspEnvironment', () => {
  it('substitutes ${CLAUDE_PLUGIN_ROOT} in command and ${CLAUDE_PLUGIN_DATA} in args', () => {
    const cfg: LspServerConfig = {
      command: '${CLAUDE_PLUGIN_ROOT}/bin/server',
      args: ['--data', '${CLAUDE_PLUGIN_DATA}/cache', '--stdio'],
    };
    const { resolved } = resolvePluginLspEnvironment(cfg, PLUGIN);
    assert.equal(resolved.command, '/opt/plugins/mylsp/bin/server');
    assert.deepEqual(resolved.args, ['--data', '/var/data/mylsp/cache', '--stdio']);
  });

  it('injects CLAUDE_PLUGIN_ROOT and CLAUDE_PLUGIN_DATA into the server env', () => {
    const { resolved } = resolvePluginLspEnvironment({ command: 'srv' }, PLUGIN);
    assert.equal(resolved.env?.CLAUDE_PLUGIN_ROOT, '/opt/plugins/mylsp');
    assert.equal(resolved.env?.CLAUDE_PLUGIN_DATA, '/var/data/mylsp');
  });

  it('lets an author-declared env entry override the injected key', () => {
    const cfg: LspServerConfig = { command: 'srv', env: { CLAUDE_PLUGIN_ROOT: '/custom/root' } };
    const { resolved } = resolvePluginLspEnvironment(cfg, PLUGIN);
    assert.equal(resolved.env?.CLAUDE_PLUGIN_ROOT, '/custom/root');
    // The injected path keys are NOT run through expansion (literal passthrough).
    assert.equal(resolved.env?.CLAUDE_PLUGIN_DATA, '/var/data/mylsp');
  });

  it('resolves ${user_config.X} against the plugin userConfig values', () => {
    const cfg: LspServerConfig = {
      command: 'srv',
      args: ['--token', '${user_config.token}'],
      env: { TOKEN: '${user_config.token}' },
    };
    const { resolved } = resolvePluginLspEnvironment(cfg, { ...PLUGIN, userConfig: { token: 'abc123' } });
    assert.deepEqual(resolved.args, ['--token', 'abc123']);
    assert.equal(resolved.env?.TOKEN, 'abc123');
  });

  it('resolves an absent ${user_config.X} to an empty string', () => {
    const { resolved } = resolvePluginLspEnvironment(
      { command: 'srv', args: ['${user_config.missing}'] },
      PLUGIN,
    );
    assert.deepEqual(resolved.args, ['']);
  });

  it('expands general ${ENV} and tracks missing env vars', () => {
    const key = 'AMI_LSP_TEST_VAR_PRESENT';
    const missingKey = 'AMI_LSP_TEST_VAR_ABSENT';
    process.env[key] = 'present-value';
    delete process.env[missingKey];
    try {
      const cfg: LspServerConfig = {
        command: 'srv',
        args: [`\${${key}}`, `\${${missingKey}}`],
      };
      const { resolved, missingVars } = resolvePluginLspEnvironment(cfg, PLUGIN);
      assert.deepEqual(resolved.args, ['present-value', `\${${missingKey}}`]);
      assert.deepEqual(missingVars, [missingKey]);
    } finally {
      delete process.env[key];
    }
  });

  it('honors a ${VAR:-default} fallback without reporting it missing', () => {
    const missingKey = 'AMI_LSP_TEST_DEFAULTED';
    delete process.env[missingKey];
    const cfg: LspServerConfig = { command: `\${${missingKey}:-/fallback/bin}` };
    const { resolved, missingVars } = resolvePluginLspEnvironment(cfg, PLUGIN);
    assert.equal(resolved.command, '/fallback/bin');
    assert.deepEqual(missingVars, []);
  });

  it('resolves workspaceFolder through both tiers', () => {
    const cfg: LspServerConfig = { command: 'srv', workspaceFolder: '${CLAUDE_PLUGIN_ROOT}/ws' };
    const { resolved } = resolvePluginLspEnvironment(cfg, PLUGIN);
    assert.equal(resolved.workspaceFolder, '/opt/plugins/mylsp/ws');
  });

  it('de-duplicates repeated missing vars across fields', () => {
    const missingKey = 'AMI_LSP_TEST_DUP_MISSING';
    delete process.env[missingKey];
    const cfg: LspServerConfig = {
      command: `\${${missingKey}}`,
      args: [`\${${missingKey}}`],
      env: { A: `\${${missingKey}}` },
    };
    const { missingVars } = resolvePluginLspEnvironment(cfg, PLUGIN);
    assert.deepEqual(missingVars, [missingKey]);
  });

  it('does not mutate the input config', () => {
    const cfg: LspServerConfig = {
      command: '${CLAUDE_PLUGIN_ROOT}/bin',
      args: ['${CLAUDE_PLUGIN_DATA}'],
      env: { X: '1' },
    };
    const snapshot = JSON.parse(JSON.stringify(cfg));
    resolvePluginLspEnvironment(cfg, PLUGIN);
    assert.deepEqual(cfg, snapshot);
  });
});
