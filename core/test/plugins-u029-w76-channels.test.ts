import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  discoverPlugins,
  validatePluginManifest,
  type PluginChannel,
} from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 w76 — plugin `channels` declaration (Claude parity:
// PluginManifestChannelsSchema, utils/plugins/schemas.ts L656-703). A channel
// binds an MCP server (by name, matching a key in the plugin's `mcpServers`)
// that emits notifications to inject messages into the conversation, and may
// declare per-channel `userConfig` to prompt the user for at enable time. The
// server↔mcpServers cross-check runs at LOAD time (the mcpServers field can be a
// file we have not read yet, so it is NOT a schema-parse error) — matching
// Claude's comment that the check lives in mcpPluginIntegration, not the schema.
// ---------------------------------------------------------------------------

let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u029-w76-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function writeManifest(name: string, manifest: Record<string, unknown>): string {
  const root = path.join(tmpDir, name);
  fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(path.join(root, '.ami-plugin', 'plugin.json'), JSON.stringify(manifest));
  return root;
}

describe('validatePluginManifest — channels', () => {
  it('accepts a well-formed channels array binding a declared inline mcpServers key', () => {
    const r = validatePluginManifest({
      name: 'chat',
      mcpServers: { telegram: { command: 'telegram-mcp' } },
      channels: [
        {
          server: 'telegram',
          displayName: 'Telegram',
          userConfig: {
            BOT_TOKEN: { type: 'string', title: 'Bot token', description: 'Telegram bot token', sensitive: true },
          },
        },
      ],
    });
    assert.equal(r.valid, true, JSON.stringify(r.errors));
  });

  it('rejects a non-array channels', () => {
    const r = validatePluginManifest({ name: 'p', channels: { server: 'x' } });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'channels'));
  });

  it('rejects a channel entry missing a non-empty server', () => {
    const r = validatePluginManifest({ name: 'p', channels: [{ displayName: 'X' }] });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'channels[0].server'));
  });

  it('rejects a channel whose server does not match any inline mcpServers key', () => {
    const r = validatePluginManifest({
      name: 'p',
      mcpServers: { telegram: { command: 'telegram-mcp' } },
      channels: [{ server: 'slack' }],
    });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'channels[0].server' && /mcpServers/.test(e.message)));
  });

  it('does NOT cross-check the server name when mcpServers is only a file path (deferred to load time)', () => {
    const r = validatePluginManifest({
      name: 'p',
      mcpServers: 'servers.mcp.json',
      channels: [{ server: 'slack' }],
    });
    assert.equal(r.valid, true, JSON.stringify(r.errors));
  });

  it('rejects a non-string displayName and a malformed userConfig option', () => {
    const r = validatePluginManifest({
      name: 'p',
      mcpServers: { s: { command: 'x' } },
      channels: [{ server: 's', displayName: 42, userConfig: { K: { type: 'string', title: '', description: 'd' } } }],
    });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'channels[0].displayName'));
    assert.ok(r.errors.some(e => e.path.startsWith('channels[0].userConfig.K')));
  });
});

describe('discoverPlugins — channels population', () => {
  it('populates bundle.channels with the well-formed entries', () => {
    writeManifest('chat', {
      name: 'chat',
      mcpServers: { telegram: { command: 'telegram-mcp' } },
      channels: [
        {
          server: 'telegram',
          displayName: 'Telegram',
          userConfig: {
            BOT_TOKEN: { type: 'string', title: 'Bot token', description: 'token', sensitive: true },
          },
        },
      ],
    });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    const channels = bundles[0]!.channels as PluginChannel[];
    assert.ok(Array.isArray(channels));
    assert.equal(channels.length, 1);
    assert.equal(channels[0]!.server, 'telegram');
    assert.equal(channels[0]!.displayName, 'Telegram');
    assert.equal(channels[0]!.userConfig?.BOT_TOKEN?.sensitive, true);
  });

  it('drops malformed channel entries (no server) and leaves channels undefined when none survive', () => {
    writeManifest('bad', {
      name: 'bad',
      channels: [{ displayName: 'no server' }, 'not-an-object'],
    });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0]!.channels, undefined);
  });

  it('omits channels entirely when the manifest declares none', () => {
    writeManifest('plain', { name: 'plain' });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0]!.channels, undefined);
  });
});
