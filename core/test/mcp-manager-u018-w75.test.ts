import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  McpManager,
  substitutePluginMcpVars,
  validateMcpServerConfigShape,
  type McpServerConfig,
} from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// U018-w75 — additive MCP manager parity (the reference):
//   * Curated read/search allowlist in getToolAnnotations (classifyForCollapse.ts
//     SEARCH_TOOLS/READ_TOOLS) so a verb-less read op (`whoami`, `esql`, …) is
//     still classified readOnly, UNIONED with the verb-prefix heuristic.
//   * ${user_config.X} / ${CLAUDE_PLUGIN_ROOT} / ${CLAUDE_PLUGIN_DATA} plugin
//     substitution tier ahead of general ${ENV} expansion, plus CLAUDE_PLUGIN_ROOT
//     / CLAUDE_PLUGIN_DATA injection into a stdio server's launch env
//     (mcpPluginIntegration.ts:L475-521).
// ---------------------------------------------------------------------------

function writeUserConfig(servers: Record<string, unknown>): { dir: string; file: string } {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-u018-w75-'));
  const file = path.join(dir, 'mcp.json');
  fs.writeFileSync(file, JSON.stringify({ mcpServers: servers }));
  return { dir, file };
}

describe('getToolAnnotations curated read/search allowlist (classifyForCollapse parity)', () => {
  const mgr = new McpManager();

  it('classifies a verb-LESS read op in the curated allowlist as read-only', () => {
    // None of these contain a read/search verb TOKEN, so the prefix heuristic
    // alone leaves them unclassified; the curated allowlist rescues them.
    for (const tool of ['whoami', 'esql', 'explain', 'aggregate', 'count', 'recommend', 'atlassian_user_info']) {
      assert.equal(mgr.getToolAnnotations(`srv:${tool}`).readOnlyHint, true, `${tool} should be read-only`);
    }
  });

  it('classifies a curated search op (suffix verb) as read-only', () => {
    for (const tool of ['brave_web_search', 'people_search_exa', 'firecrawl_search']) {
      assert.equal(mgr.getToolAnnotations(`srv:${tool}`).readOnlyHint, true, `${tool} should be read-only`);
    }
  });

  it('still leaves a mutation tool NOT read-only (conservative default preserved)', () => {
    assert.equal(mgr.getToolAnnotations('srv:create_issue').readOnlyHint, undefined);
    assert.equal(mgr.getToolAnnotations('srv:send_message').readOnlyHint, undefined);
  });

  it('still classifies a verb-prefix read op via the heuristic (superset)', () => {
    assert.equal(mgr.getToolAnnotations('srv:get_user').readOnlyHint, true);
  });
});

describe('substitutePluginMcpVars (plugin substitution tier)', () => {
  it('substitutes ${user_config.X} from the provided values', () => {
    assert.equal(
      substitutePluginMcpVars('--port=${user_config.PORT}', { userConfig: { PORT: '8080' } }),
      '--port=8080',
    );
  });

  it('substitutes ${CLAUDE_PLUGIN_ROOT} and ${CLAUDE_PLUGIN_DATA}', () => {
    assert.equal(
      substitutePluginMcpVars('${CLAUDE_PLUGIN_ROOT}/bin --data ${CLAUDE_PLUGIN_DATA}', {
        pluginRoot: '/plug/root',
        pluginDataDir: '/plug/data',
      }),
      '/plug/root/bin --data /plug/data',
    );
  });

  it('leaves an unrelated ${VAR} reference untouched for the env tier', () => {
    assert.equal(
      substitutePluginMcpVars('${user_config.A}:${SOME_ENV}', { userConfig: { A: 'x' } }),
      'x:${SOME_ENV}',
    );
  });

  it('expands a missing user_config key / plugin dir to empty string (no stale placeholder leak)', () => {
    assert.equal(substitutePluginMcpVars('${user_config.MISSING}', { userConfig: {} }), '');
    assert.equal(substitutePluginMcpVars('${CLAUDE_PLUGIN_ROOT}', {}), '');
  });
});

describe('expandConfigEnv plugin substitution + stdio env injection (end-to-end)', () => {
  const TOUCHED = ['__W75_CLAUDE_PLUGIN_ROOT', 'CLAUDE_PLUGIN_ROOT'];
  afterEach(() => { for (const k of TOUCHED) delete process.env[k]; });

  it('substitutes ${user_config.X} and ${CLAUDE_PLUGIN_ROOT} and injects both plugin env keys for a stdio server', () => {
    const { dir, file } = writeUserConfig({
      bundled: {
        command: '${CLAUDE_PLUGIN_ROOT}/bin/server',
        args: ['--token', '${user_config.TOKEN}'],
        userConfig: { TOKEN: 'secret-123' },
        pluginRoot: '/installed/plug',
        pluginDataDir: '/data/plug',
      },
    });
    try {
      const mgr = new McpManager();
      mgr.loadFromConfig(file, { scope: 'user' });
      const cfg = mgr.getServerConfig('bundled') as McpServerConfig;
      assert.equal(cfg.command, '/installed/plug/bin/server');
      assert.deepEqual(cfg.args, ['--token', 'secret-123']);
      assert.equal(cfg.env?.CLAUDE_PLUGIN_ROOT, '/installed/plug');
      assert.equal(cfg.env?.CLAUDE_PLUGIN_DATA, '/data/plug');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('gives a plugin value precedence over a same-named process env var', () => {
    process.env.CLAUDE_PLUGIN_ROOT = '/evil/from/env';
    const { dir, file } = writeUserConfig({
      bundled: {
        command: 'server',
        args: ['${CLAUDE_PLUGIN_ROOT}/x'],
        pluginRoot: '/the/real/plug',
      },
    });
    try {
      const mgr = new McpManager();
      mgr.loadFromConfig(file, { scope: 'user' });
      const cfg = mgr.getServerConfig('bundled') as McpServerConfig;
      assert.deepEqual(cfg.args, ['/the/real/plug/x']);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('does NOT inject plugin env keys into a REMOTE (http) server', () => {
    const { dir, file } = writeUserConfig({
      remote: {
        transport: 'http',
        url: 'https://example.com/mcp',
        pluginRoot: '/installed/plug',
        pluginDataDir: '/data/plug',
      },
    });
    try {
      const mgr = new McpManager();
      mgr.loadFromConfig(file, { scope: 'user' });
      const cfg = mgr.getServerConfig('remote') as McpServerConfig;
      assert.equal(cfg.env?.CLAUDE_PLUGIN_ROOT, undefined);
      assert.equal(cfg.env?.CLAUDE_PLUGIN_DATA, undefined);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('leaves a config with no plugin vars unchanged (no spurious substitution)', () => {
    const { dir, file } = writeUserConfig({ plain: { command: 'echo', args: ['hi'] } });
    try {
      const mgr = new McpManager();
      mgr.loadFromConfig(file, { scope: 'user' });
      const cfg = mgr.getServerConfig('plain') as McpServerConfig;
      assert.equal(cfg.command, 'echo');
      assert.equal(cfg.env?.CLAUDE_PLUGIN_ROOT, undefined);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('validateMcpServerConfigShape — plugin config field validation', () => {
  it('accepts a valid userConfig/pluginRoot/pluginDataDir', () => {
    assert.deepEqual(
      validateMcpServerConfigShape({
        command: 'x',
        userConfig: { A: '1' },
        pluginRoot: '/p',
        pluginDataDir: '/d',
      }),
      [],
    );
  });

  it('rejects a non-object userConfig and non-string plugin dirs', () => {
    const issues = validateMcpServerConfigShape({
      command: 'x',
      userConfig: 'nope',
      pluginRoot: 5,
      pluginDataDir: [],
    });
    assert.ok(issues.some((i) => i.includes('userConfig')));
    assert.ok(issues.some((i) => i.includes('pluginRoot')));
    assert.ok(issues.some((i) => i.includes('pluginDataDir')));
  });

  it('rejects a userConfig object with a non-string value', () => {
    const issues = validateMcpServerConfigShape({ command: 'x', userConfig: { A: 5 } });
    assert.ok(issues.some((i) => i.includes('userConfig')));
  });
});
