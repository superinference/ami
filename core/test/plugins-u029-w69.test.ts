import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  loadCommands,
  discoverPlugins,
  validatePluginManifest,
  type CommandMetadata,
  type PluginCommandsMetadataContext,
} from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 w69 — `commandsMetadata` manifest object-mapping (Claude parity:
// CommandMetadataSchema + the loadPluginCommands metadata-override / inline-content
// loops, utils/plugins/schemas.ts L385-416, utils/plugins/loadPluginCommands.ts
// L516-668). An entry carries EXACTLY ONE of `source` (override a file-based
// command, matched by path) or `content` (inline markdown that IS the command),
// plus optional description/argumentHint/model/allowedTools overrides.
// ---------------------------------------------------------------------------

let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u029-w69-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('loadCommands with commandsMetadata', () => {
  it('overrides a file-based command matched by source and renames it to the metadata key', () => {
    const root = path.join(tmpDir, 'bundle');
    const cmdDir = path.join(root, 'commands');
    fs.mkdirSync(cmdDir, { recursive: true });
    fs.writeFileSync(path.join(cmdDir, 'readme.md'), '---\ndescription: original desc\n---\nDo the readme thing');

    const metadata: PluginCommandsMetadataContext = {
      root,
      entries: {
        about: {
          source: 'commands/readme.md',
          description: 'About this plugin',
          argumentHint: '[topic]',
          model: 'opus',
          allowedTools: ['Read', 'Bash'],
        },
      },
    };
    const cmds = loadCommands(cmdDir, undefined, undefined, metadata);
    assert.equal(cmds.length, 1);
    const cmd = cmds[0]!;
    // Renamed to the metadata KEY (Claude parity), namespace reset.
    assert.equal(cmd.name, 'about');
    assert.deepEqual(cmd.namespace, []);
    // Overrides applied; the body stays the command's prompt.
    assert.equal(cmd.description, 'About this plugin');
    assert.equal(cmd.argumentHint, '[topic]');
    assert.equal(cmd.model, 'opus');
    assert.deepEqual(cmd.allowedTools, ['Read', 'Bash']);
    assert.equal(cmd.prompt, 'Do the readme thing');
  });

  it('creates an inline-content command (no source file) under the metadata key', () => {
    const root = path.join(tmpDir, 'bundle2');
    const cmdDir = path.join(root, 'commands');
    fs.mkdirSync(cmdDir, { recursive: true });

    const metadata: PluginCommandsMetadataContext = {
      root,
      entries: {
        hello: {
          content: '---\ndescription: inline greeting\n---\nSay hello to $ARGUMENTS',
          argumentHint: '<name>',
        },
      },
    };
    const cmds = loadCommands(cmdDir, undefined, undefined, metadata);
    assert.equal(cmds.length, 1);
    const cmd = cmds[0]!;
    assert.equal(cmd.name, 'hello');
    assert.equal(cmd.prompt, 'Say hello to $ARGUMENTS');
    assert.equal(cmd.description, 'inline greeting');
    assert.equal(cmd.argumentHint, '<name>');
    assert.equal(cmd.source, 'plugin');
    assert.equal(cmd.type, 'prompt');
    // A virtual, non-on-disk source path marks it as having no file.
    assert.match(cmd.filePath, /^<inline:hello>$/);
  });

  it('skips an entry that declares both source and content (XOR)', () => {
    const root = path.join(tmpDir, 'bundle3');
    const cmdDir = path.join(root, 'commands');
    fs.mkdirSync(cmdDir, { recursive: true });
    const metadata: PluginCommandsMetadataContext = {
      root,
      entries: { bad: { source: 'commands/x.md', content: 'body' } as CommandMetadata },
    };
    assert.deepEqual(loadCommands(cmdDir, undefined, undefined, metadata), []);
  });

  it('skips an entry that declares neither source nor content (XOR)', () => {
    const root = path.join(tmpDir, 'bundle4');
    const cmdDir = path.join(root, 'commands');
    fs.mkdirSync(cmdDir, { recursive: true });
    const metadata: PluginCommandsMetadataContext = {
      root,
      entries: { empty: { description: 'nothing' } as CommandMetadata },
    };
    assert.deepEqual(loadCommands(cmdDir, undefined, undefined, metadata), []);
  });

  it('does not match a source path that escapes the plugin root', () => {
    const root = path.join(tmpDir, 'bundle5');
    const cmdDir = path.join(root, 'commands');
    fs.mkdirSync(cmdDir, { recursive: true });
    fs.writeFileSync(path.join(cmdDir, 'ok.md'), '---\n---\nbody');
    const metadata: PluginCommandsMetadataContext = {
      root,
      entries: { evil: { source: '../outside.md', description: 'nope' } },
    };
    const cmds = loadCommands(cmdDir, undefined, undefined, metadata);
    // The real `ok` command stays, unrenamed; the escaping entry matches nothing.
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.name, 'ok');
  });

  it('is a no-op when no metadata is passed (back-compat)', () => {
    const cmdDir = path.join(tmpDir, 'plain');
    fs.mkdirSync(cmdDir, { recursive: true });
    fs.writeFileSync(path.join(cmdDir, 'a.toml'), 'prompt = "do a"');
    const cmds = loadCommands(cmdDir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.name, 'a');
  });
});

describe('discoverPlugins parses commandsMetadata onto the bundle', () => {
  function makeBundle(name: string, manifest: Record<string, unknown>): void {
    const root = path.join(tmpDir, name);
    fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(path.join(root, '.ami-plugin', 'plugin.json'), JSON.stringify(manifest));
  }

  it('keeps well-formed entries and drops malformed ones', () => {
    makeBundle('p', {
      name: 'p',
      version: '1.0.0',
      commandsMetadata: {
        fromFile: { source: 'commands/x.md', description: 'd' },
        inline: { content: '---\n---\nbody' },
        bothIsBad: { source: 'a.md', content: 'b' },
        neitherIsBad: { description: 'd' },
        notAnObject: 'nope',
      },
    });
    const bundle = discoverPlugins(tmpDir).find(b => b.name === 'p')!;
    assert.ok(bundle.commandsMetadata);
    assert.deepEqual(Object.keys(bundle.commandsMetadata!).sort(), ['fromFile', 'inline']);
    assert.equal(bundle.commandsMetadata!.fromFile!.source, 'commands/x.md');
    assert.equal(bundle.commandsMetadata!.inline!.content, '---\n---\nbody');
  });

  it('accepts argument-hint as an alias for argumentHint', () => {
    makeBundle('q', {
      name: 'q',
      version: '1.0.0',
      commandsMetadata: { c: { content: 'body', 'argument-hint': '<x>' } },
    });
    const bundle = discoverPlugins(tmpDir).find(b => b.name === 'q')!;
    assert.equal(bundle.commandsMetadata!.c!.argumentHint, '<x>');
  });

  it('leaves commandsMetadata undefined when absent or empty', () => {
    makeBundle('none', { name: 'none', version: '1.0.0' });
    makeBundle('empty', { name: 'empty', version: '1.0.0', commandsMetadata: {} });
    const byName = new Map(discoverPlugins(tmpDir).map(b => [b.name, b]));
    assert.equal(byName.get('none')!.commandsMetadata, undefined);
    assert.equal(byName.get('empty')!.commandsMetadata, undefined);
  });
});

describe('validatePluginManifest validates commandsMetadata', () => {
  it('passes a well-formed source and content entry', () => {
    const r = validatePluginManifest({
      name: 'good',
      version: '1.0.0',
      commandsMetadata: {
        a: { source: 'commands/a.md', description: 'A', allowedTools: ['Read'] },
        b: { content: 'inline body', model: 'opus' },
      },
    });
    assert.equal(r.valid, true, JSON.stringify(r.errors));
  });

  it('errors when commandsMetadata is not an object', () => {
    const r = validatePluginManifest({ name: 'x', version: '1', commandsMetadata: ['a'] });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'commandsMetadata'));
  });

  it('errors on both source and content (XOR message)', () => {
    const r = validatePluginManifest({
      name: 'x',
      version: '1',
      commandsMetadata: { c: { source: 'a.md', content: 'b' } },
    });
    assert.equal(r.valid, false);
    const e = r.errors.find(e => e.path === 'commandsMetadata.c');
    assert.ok(e);
    assert.match(e!.message, /either "source" .* or "content" .* but not both/);
  });

  it('errors on neither source nor content', () => {
    const r = validatePluginManifest({
      name: 'x',
      version: '1',
      commandsMetadata: { c: { description: 'only' } },
    });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'commandsMetadata.c'));
  });

  it('flags a traversing source path', () => {
    const r = validatePluginManifest({
      name: 'x',
      version: '1',
      commandsMetadata: { c: { source: '../../etc/passwd' } },
    });
    assert.equal(r.valid, false);
    const e = r.errors.find(e => e.path === 'commandsMetadata.c.source');
    assert.ok(e);
    assert.match(e!.message, /\.\./);
  });

  it('flags a non-string field and a non-string-array allowedTools', () => {
    const r = validatePluginManifest({
      name: 'x',
      version: '1',
      commandsMetadata: { c: { content: 'b', description: 5, allowedTools: [1, 2] } },
    });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'commandsMetadata.c.description'));
    assert.ok(r.errors.some(e => e.path === 'commandsMetadata.c.allowedTools'));
  });

  it('errors when an entry value is not an object', () => {
    const r = validatePluginManifest({
      name: 'x',
      version: '1',
      commandsMetadata: { c: 'nope' },
    });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'commandsMetadata.c'));
  });
});
