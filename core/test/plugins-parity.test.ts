import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  loadCommands,
  discoverPlugins,
  validatePluginContents,
  type PluginBundle,
} from '../src/plugins';

// ---------------------------------------------------------------------------
// Additive Claude-Code plugin parity for core/src/plugins.ts:
//  1. multi-line TOML basic strings (""" … """) in command files;
//  2. full-line `#` TOML comments ignored by the command parser;
//  3. bundled MCP server config (.mcp.json) enumerated on the PluginBundle,
//     with a manifest `mcpServers` override, symlink-escape hardening, and JSON
//     validation in validatePluginContents.
// Each test is a red-green reproducer: it fails against the pre-fix plugins.ts
// and passes with the fix.
// ---------------------------------------------------------------------------

let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'plugins-parity-'));
});
afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function makePlugin(name: string, manifest: Record<string, unknown>): string {
  const root = path.join(tmpDir, name);
  fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(path.join(root, '.ami-plugin', 'plugin.json'), JSON.stringify(manifest));
  return root;
}

describe('loadCommands — multi-line TOML basic strings', () => {
  it('loads a """ multi-line prompt instead of dropping the command', () => {
    const dir = path.join(tmpDir, 'ml-cmds');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, 'review.toml'),
      ['description = "Deep review"', 'prompt = """', 'line one', 'line two', '"""', ''].join('\n'),
    );
    const cmds = loadCommands(dir);
    // RED (pre-fix): the """ opener parses to an empty prompt, so the command is
    // dropped entirely → cmds.length === 0.
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.prompt, 'line one\nline two\n');
    assert.equal(cmds[0]!.description, 'Deep review');
  });

  it('supports a multi-line string that opens and closes on the same line', () => {
    const dir = path.join(tmpDir, 'ml-same');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, 'c.toml'),
      ['prompt = """inline body"""'].join('\n'),
    );
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.prompt, 'inline body');
  });

  it('drops a command whose multi-line string is never closed', () => {
    const dir = path.join(tmpDir, 'ml-unterminated');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, 'c.toml'),
      ['prompt = """', 'dangling body', 'with no closing delimiter'].join('\n'),
    );
    // Unterminated → prompt is absent → command dropped (no crash).
    assert.deepEqual(loadCommands(dir), []);
  });
});

describe('loadCommands — full-line comments', () => {
  it('ignores a commented-out key and uses the real assignment below it', () => {
    const dir = path.join(tmpDir, 'cm-cmds');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, 'c.toml'),
      ['# prompt = "disabled old prompt"', 'description = "d"', 'prompt = "real prompt"'].join('\n'),
    );
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    // RED (pre-fix): indexOf matched inside the comment line → "disabled old prompt".
    assert.equal(cmds[0]!.prompt, 'real prompt');
  });

  it('does not match a key that is not at the start of a line', () => {
    const dir = path.join(tmpDir, 'cm-embedded');
    fs.mkdirSync(dir, { recursive: true });
    // `prompt = "x"` appears only as a substring inside another value; the only
    // real assignment is on its own line.
    fs.writeFileSync(
      path.join(dir, 'c.toml'),
      ['description = "see prompt = \\"x\\" in docs"', 'prompt = "the real one"'].join('\n'),
    );
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.prompt, 'the real one');
    assert.equal(cmds[0]!.description, 'see prompt = "x" in docs');
  });
});

describe('discoverPlugins — bundled MCP servers (.mcp.json)', () => {
  it('populates mcpServersFile from the root .mcp.json convention', () => {
    const root = makePlugin('mcp-plugin', { name: 'MCP' });
    fs.writeFileSync(path.join(root, '.mcp.json'), JSON.stringify({ mcpServers: { demo: {} } }));
    const bundles = discoverPlugins(tmpDir);
    const b = bundles.find((x: PluginBundle) => x.name === 'MCP')!;
    // RED (pre-fix): PluginBundle had no mcpServersFile field → undefined.
    assert.ok(b.mcpServersFile, 'mcpServersFile should be populated');
    assert.ok(b.mcpServersFile!.endsWith('.mcp.json'));
  });

  it('leaves mcpServersFile undefined when a plugin ships no .mcp.json', () => {
    makePlugin('no-mcp', { name: 'NOMCP' });
    const b = discoverPlugins(tmpDir).find((x: PluginBundle) => x.name === 'NOMCP')!;
    assert.equal(b.mcpServersFile, undefined);
  });

  it('honors a manifest-declared mcpServers path override', () => {
    const root = makePlugin('mcp-override', { name: 'OV', mcpServers: 'conf/servers.json' });
    fs.mkdirSync(path.join(root, 'conf'), { recursive: true });
    fs.writeFileSync(path.join(root, 'conf', 'servers.json'), '{}');
    // A plain root .mcp.json also exists to prove the override wins.
    fs.writeFileSync(path.join(root, '.mcp.json'), '{}');
    const b = discoverPlugins(tmpDir).find((x: PluginBundle) => x.name === 'OV')!;
    assert.ok(b.mcpServersFile!.endsWith(path.join('conf', 'servers.json')));
  });

  it('rejects an mcpServers manifest override that escapes via ".."', () => {
    makePlugin('mcp-escape', { name: 'ESC', mcpServers: '../outside.json' });
    // ".." in a component path is a hard manifest error → the bundle is dropped.
    assert.equal(discoverPlugins(tmpDir).length, 0);
  });

  it('rejects a symlink-escaped .mcp.json', () => {
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-escape-'));
    fs.writeFileSync(path.join(outside, 'evil.json'), '{}');
    const root = makePlugin('mcp-symlink', { name: 'SL' });
    try {
      fs.symlinkSync(path.join(outside, 'evil.json'), path.join(root, '.mcp.json'));
    } catch {
      fs.rmSync(outside, { recursive: true, force: true });
      return; // platform without symlink support
    }
    const b = discoverPlugins(tmpDir).find((x: PluginBundle) => x.name === 'SL')!;
    assert.equal(b.mcpServersFile, undefined, 'symlink-escaped .mcp.json must be rejected');
    fs.rmSync(outside, { recursive: true, force: true });
  });
});

describe('validatePluginContents — .mcp.json validation', () => {
  it('flags a corrupt .mcp.json as an error', () => {
    const root = makePlugin('bad-mcp', { name: 'BAD' });
    fs.writeFileSync(path.join(root, '.mcp.json'), 'not json {');
    const r = validatePluginContents(root);
    // RED (pre-fix): .mcp.json was never inspected → no such error.
    assert.ok(r.errors.some(e => e.path === '.mcp.json' && /Invalid MCP servers JSON/.test(e.message)));
    assert.equal(r.valid, false);
  });

  it('accepts a well-formed .mcp.json', () => {
    const root = makePlugin('good-mcp', { name: 'GOOD' });
    fs.writeFileSync(path.join(root, '.mcp.json'), JSON.stringify({ mcpServers: {} }));
    const r = validatePluginContents(root);
    assert.ok(!r.errors.some(e => e.path === '.mcp.json'));
  });
});
