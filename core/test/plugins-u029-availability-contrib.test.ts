import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  discoverPlugins,
  discoverPluginsDetailed,
  validatePluginManifest,
  isPluginAvailable,
  parsePluginSettings,
  type PluginBundle,
  type PluginRequirements,
} from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 — additive Claude-Code plugin parity for core/src/plugins.ts:
//  1. Declarative availability gate (`requires`) — Claude's isAvailable():
//     a bundle whose platform/env/bin prerequisites aren't met is OMITTED.
//  2. Inline MCP servers in the manifest `mcpServers` record/array
//     (PluginManifestMcpServerSchema).
//  3. Plugin-contributed LSP servers (`lspServers` file/record/array)
//     validated against the canonical LspServerConfig schema.
//  4. Output-styles plugin component (outputStylesDir + outputStylesPaths).
//  5. Allowlisted plugin settings (settings.json / manifest.settings, agent-only).
// Each is a red-green reproducer against the pre-fix plugins.ts.
// ---------------------------------------------------------------------------

let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'plugins-u029-'));
});
afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function makePlugin(name: string, manifest: Record<string, unknown>, extra?: (root: string) => void): string {
  const root = path.join(tmpDir, name);
  fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(path.join(root, '.ami-plugin', 'plugin.json'), JSON.stringify(manifest));
  extra?.(root);
  return root;
}

function find(name: string): PluginBundle | undefined {
  return discoverPlugins(tmpDir).find((b) => b.name === name);
}

// --- Feature 1: availability gate ------------------------------------------

describe('isPluginAvailable — declarative requires gate', () => {
  it('returns true when no requirements are declared', () => {
    assert.equal(isPluginAvailable(undefined), true);
    assert.equal(isPluginAvailable({}), true);
  });

  it('gates on platform allowlist', () => {
    assert.equal(isPluginAvailable({ platform: ['linux', 'darwin'] }, { platform: 'linux' }), true);
    assert.equal(isPluginAvailable({ platform: ['darwin'] }, { platform: 'linux' }), false);
    // empty list imposes no requirement
    assert.equal(isPluginAvailable({ platform: [] }, { platform: 'win32' }), true);
  });

  it('gates on required env vars being set and non-empty', () => {
    assert.equal(isPluginAvailable({ env: ['MY_TOKEN'] }, { env: { MY_TOKEN: 'x' } }), true);
    assert.equal(isPluginAvailable({ env: ['MY_TOKEN'] }, { env: { MY_TOKEN: '' } }), false);
    assert.equal(isPluginAvailable({ env: ['MY_TOKEN'] }, { env: {} }), false);
  });

  it('gates on a binary resolving on PATH (incl. explicit path form)', () => {
    const binDir = fs.mkdtempSync(path.join(os.tmpdir(), 'bin-'));
    const binPath = path.join(binDir, 'mytool');
    fs.writeFileSync(binPath, '#!/bin/sh\n');
    try {
      assert.equal(isPluginAvailable({ bin: ['mytool'] }, { env: { PATH: binDir } }), true);
      assert.equal(isPluginAvailable({ bin: ['missing-xyz'] }, { env: { PATH: binDir } }), false);
      // explicit path form bypasses PATH scanning
      assert.equal(isPluginAvailable({ bin: [binPath] }, { env: {} }), true);
      assert.equal(isPluginAvailable({ bin: [path.join(binDir, 'nope') ] }, { env: {} }), false);
    } finally {
      fs.rmSync(binDir, { recursive: true, force: true });
    }
  });

  it('discovery OMITS a bundle whose requires are not met (not surfaced as a failure)', () => {
    // Impossible platform → dropped entirely.
    makePlugin('gated', { name: 'gated', requires: { platform: ['definitely-not-a-platform'] } });
    makePlugin('open', { name: 'open' });
    const { bundles, failures } = discoverPluginsDetailed(tmpDir);
    assert.equal(bundles.find((b) => b.name === 'gated'), undefined, 'unavailable bundle must be omitted');
    assert.ok(bundles.find((b) => b.name === 'open'), 'available bundle must remain');
    assert.equal(failures.length, 0, 'omission is not a failure');
  });

  it('records the satisfied requires on a surviving bundle', () => {
    makePlugin('reqd', { name: 'reqd', requires: { platform: [process.platform] } });
    const b = find('reqd')!;
    assert.deepEqual(b.requires, { platform: [process.platform] } as PluginRequirements);
  });

  it('validatePluginManifest flags a malformed requires', () => {
    const r = validatePluginManifest({ name: 'p', requires: { platform: 'linux' } });
    assert.ok(r.errors.some((e) => e.path === 'requires.platform'));
    const r2 = validatePluginManifest({ name: 'p', requires: [] });
    assert.ok(r2.errors.some((e) => e.path === 'requires'));
  });
});

// --- Feature 2: inline MCP servers -----------------------------------------

describe('discovery — inline manifest mcpServers', () => {
  it('populates mcpServers from an inline record', () => {
    makePlugin('mcp-inline', { name: 'mcp-inline', mcpServers: { web: { command: 'serve', args: ['--port', '1'] } } });
    const b = find('mcp-inline')!;
    assert.deepEqual(b.mcpServers, { web: { command: 'serve', args: ['--port', '1'] } });
  });

  it('merges inline records from an array (later wins) and ignores path strings', () => {
    makePlugin('mcp-arr', { name: 'mcp-arr', mcpServers: [{ a: { command: 'x' } }, { b: { command: 'y' } }, 'conf/extra.json'] });
    const b = find('mcp-arr')!;
    assert.deepEqual(b.mcpServers, { a: { command: 'x' }, b: { command: 'y' } });
  });

  it('leaves mcpServers undefined when only a file path is declared', () => {
    makePlugin('mcp-path', { name: 'mcp-path', mcpServers: 'conf/servers.json' });
    const b = find('mcp-path')!;
    assert.equal(b.mcpServers, undefined);
  });

  it('still drops a bundle whose mcpServers path escapes via ".."', () => {
    makePlugin('mcp-escape2', { name: 'mcp-escape2', mcpServers: '../outside.json' });
    assert.equal(find('mcp-escape2'), undefined);
  });
});

// --- Feature 3: plugin LSP servers -----------------------------------------

describe('discovery — plugin-contributed LSP servers', () => {
  it('populates inline lspServers validated against the LspServerConfig schema', () => {
    makePlugin('lsp-inline', {
      name: 'lsp-inline',
      lspServers: { ts: { command: 'typescript-language-server', args: ['--stdio'], extensionToLanguage: { '.ts': 'typescript' } } },
    });
    const b = find('lsp-inline')!;
    assert.ok(b.lspServers);
    assert.equal(b.lspServers!.ts!.command, 'typescript-language-server');
    assert.deepEqual(b.lspServers!.ts!.args, ['--stdio']);
  });

  it('drops a malformed inline LSP server (lenient at discovery)', () => {
    makePlugin('lsp-bad', { name: 'lsp-bad', lspServers: { broken: { args: ['x'] } } });
    const b = find('lsp-bad')!;
    assert.equal(b.lspServers, undefined);
  });

  it('resolves a bundled .lsp.json file by convention', () => {
    makePlugin('lsp-file', { name: 'lsp-file' }, (root) => {
      fs.writeFileSync(path.join(root, '.lsp.json'), JSON.stringify({ py: { command: 'pylsp' } }));
    });
    const b = find('lsp-file')!;
    assert.ok(b.lspServersFile && b.lspServersFile.endsWith('.lsp.json'));
  });

  it('honors a manifest-declared lspServers file-path override', () => {
    makePlugin('lsp-override', { name: 'lsp-override', lspServers: 'conf/servers.lsp.json' }, (root) => {
      fs.mkdirSync(path.join(root, 'conf'), { recursive: true });
      fs.writeFileSync(path.join(root, 'conf', 'servers.lsp.json'), JSON.stringify({ py: { command: 'pylsp' } }));
    });
    const b = find('lsp-override')!;
    assert.ok(b.lspServersFile && b.lspServersFile.endsWith(path.join('conf', 'servers.lsp.json')));
  });

  it('validatePluginManifest flags an lspServers path escaping via ".."', () => {
    const r = validatePluginManifest({ name: 'p', lspServers: '../evil.json' });
    assert.ok(r.errors.some((e) => /contains "\.\."/.test(e.message)));
  });
});

// --- Feature 4: output styles ----------------------------------------------

describe('discovery — plugin output styles', () => {
  it('populates outputStylesDir from the output-styles convention', () => {
    makePlugin('os-plugin', { name: 'os-plugin' }, (root) => {
      fs.mkdirSync(path.join(root, 'output-styles'), { recursive: true });
      fs.writeFileSync(path.join(root, 'output-styles', 'terse.md'), '# Terse\nBe terse.');
    });
    const b = find('os-plugin')!;
    assert.ok(b.outputStylesDir && b.outputStylesDir.endsWith('output-styles'));
  });

  it('honors a manifest outputStyles override and outputStylesPaths list', () => {
    makePlugin('os-override', { name: 'os-override', outputStyles: 'styles', outputStylesPaths: ['extra-styles'] }, (root) => {
      fs.mkdirSync(path.join(root, 'styles'), { recursive: true });
      fs.mkdirSync(path.join(root, 'extra-styles'), { recursive: true });
    });
    const b = find('os-override')!;
    assert.ok(b.outputStylesDir && b.outputStylesDir.endsWith('styles'));
    assert.ok(b.outputStylesPaths && b.outputStylesPaths.some((p) => p.endsWith('extra-styles')));
  });

  it('leaves outputStylesDir undefined when the plugin ships none', () => {
    makePlugin('os-none', { name: 'os-none' });
    const b = find('os-none')!;
    assert.equal(b.outputStylesDir, undefined);
  });

  it('validatePluginManifest flags an outputStyles path escaping via ".."', () => {
    const r = validatePluginManifest({ name: 'p', outputStyles: '../../styles' });
    assert.ok(r.errors.some((e) => /contains "\.\."/.test(e.message)));
  });
});

// --- Feature 5: allowlisted plugin settings --------------------------------

describe('plugin settings — allowlisted parse + load', () => {
  it('parsePluginSettings keeps only the agent key', () => {
    assert.deepEqual(parsePluginSettings({ agent: 'reviewer', model: 'x', hooks: {} }), { agent: 'reviewer' });
    assert.equal(parsePluginSettings({ model: 'x' }), undefined);
    assert.equal(parsePluginSettings({ agent: '' }), undefined);
    assert.equal(parsePluginSettings('nope'), undefined);
  });

  it('loads settings.json over manifest.settings at discovery', () => {
    makePlugin('set-file', { name: 'set-file', settings: { agent: 'manifest-agent' } }, (root) => {
      fs.writeFileSync(path.join(root, 'settings.json'), JSON.stringify({ agent: 'file-agent', secret: 'nope' }));
    });
    const b = find('set-file')!;
    assert.deepEqual(b.settings, { agent: 'file-agent' });
  });

  it('falls back to manifest.settings when no settings.json exists', () => {
    makePlugin('set-manifest', { name: 'set-manifest', settings: { agent: 'manifest-agent' } });
    const b = find('set-manifest')!;
    assert.deepEqual(b.settings, { agent: 'manifest-agent' });
  });

  it('leaves settings undefined with no allowlisted setting', () => {
    makePlugin('set-none', { name: 'set-none', settings: { model: 'gpt' } });
    const b = find('set-none')!;
    assert.equal(b.settings, undefined);
  });

  it('validatePluginManifest errors on a non-string agent and warns on stripped keys', () => {
    const r = validatePluginManifest({ name: 'p', settings: { agent: 42, extra: 'x' } });
    assert.ok(r.errors.some((e) => e.path === 'settings.agent'));
    assert.ok(r.warnings.some((w) => w.path === 'settings.extra'));
  });
});
