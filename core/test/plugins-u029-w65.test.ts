import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  deriveCommandDescription,
  isNamespaceSafeName,
  loadCommands,
  discoverPlugins,
  DEFAULT_COMMAND_DESCRIPTION,
} from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 (w65) — two Claude-parity gaps in core/core/src/plugins.ts:
//   (gap) Description derived from the command body/prompt when none is declared
//         (parity: extractDescriptionFromMarkdown, utils/markdownConfigLoader.ts).
//   (gap) Plugin manifest name sanitized for namespacing: a name with spaces /
//         non-ASCII homographs falls back to the directory name at discovery
//         (parity: PluginManifestSchema name refinement + NON_ASCII guard).
// ---------------------------------------------------------------------------

describe('U029 w65 — deriveCommandDescription', () => {
  it('returns the first non-empty line verbatim', () => {
    assert.equal(deriveCommandDescription('Do the thing.\nmore detail'), 'Do the thing.');
  });

  it('skips leading blank lines', () => {
    assert.equal(deriveCommandDescription('\n\n   \nActual first line.'), 'Actual first line.');
  });

  it('strips a leading markdown header prefix', () => {
    assert.equal(deriveCommandDescription('# Deploy\nbody'), 'Deploy');
    assert.equal(deriveCommandDescription('### Nested header'), 'Nested header');
  });

  it('truncates past 100 characters with an ellipsis', () => {
    const long = 'x'.repeat(130);
    const out = deriveCommandDescription(long);
    assert.equal(out.length, 100);
    assert.ok(out.endsWith('...'));
    assert.equal(out, 'x'.repeat(97) + '...');
  });

  it('keeps a line exactly 100 chars long intact', () => {
    const exact = 'y'.repeat(100);
    assert.equal(deriveCommandDescription(exact), exact);
  });

  it('falls back to the default label when there is no non-empty line', () => {
    assert.equal(deriveCommandDescription('\n  \n\t\n'), DEFAULT_COMMAND_DESCRIPTION);
    assert.equal(deriveCommandDescription('', 'Custom fallback'), 'Custom fallback');
  });
});

describe('U029 w65 — isNamespaceSafeName', () => {
  it('accepts a kebab-case ASCII name', () => {
    assert.equal(isNamespaceSafeName('my-plugin'), true);
    assert.equal(isNamespaceSafeName('plugin_1.2'), true);
  });

  it('rejects a name with whitespace', () => {
    assert.equal(isNamespaceSafeName('my plugin'), false);
    assert.equal(isNamespaceSafeName('tab\tname'), false);
  });

  it('rejects a non-ASCII (homograph-risk) name', () => {
    assert.equal(isNamespaceSafeName('pаypal'), false); // Cyrillic 'а'
  });

  it('rejects path separators / traversal / NUL via isSafeSegment', () => {
    assert.equal(isNamespaceSafeName('a/b'), false);
    assert.equal(isNamespaceSafeName('..'), false);
    assert.equal(isNamespaceSafeName('a\0b'), false);
  });
});

describe('U029 w65 — loadCommands derives a description from the content', () => {
  let dir: string;
  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u029w65-cmd-')); });
  afterEach(() => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* best effort */ } });

  it('derives a TOML command description from its prompt when absent', () => {
    fs.writeFileSync(path.join(dir, 'go.toml'), 'prompt = "Run the full deploy pipeline"\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd!.description, 'Run the full deploy pipeline');
  });

  it('prefers an explicit TOML description over the derived one', () => {
    fs.writeFileSync(path.join(dir, 'go.toml'), 'description = "Deploy"\nprompt = "big long prompt body"\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd!.description, 'Deploy');
  });

  it('derives a markdown command description from its body when absent', () => {
    fs.writeFileSync(path.join(dir, 'review.md'), '---\n---\n# Review the diff\n\nLook carefully.\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd!.description, 'Review the diff');
  });
});

describe('U029 w65 — discovery sanitizes the plugin namespace name', () => {
  let dir: string;
  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u029w65-disc-')); });
  afterEach(() => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* best effort */ } });

  function writeManifest(entry: string, manifest: Record<string, unknown>): void {
    const root = path.join(dir, entry, '.ami-plugin');
    fs.mkdirSync(root, { recursive: true });
    fs.writeFileSync(path.join(root, 'plugin.json'), JSON.stringify(manifest));
  }

  it('keeps a valid namespace-safe manifest name', () => {
    writeManifest('dir-a', { name: 'custom-name', version: '1.0.0' });
    const [b] = discoverPlugins(dir);
    assert.equal(b!.name, 'custom-name');
  });

  it('falls back to the directory name when the manifest name has spaces', () => {
    writeManifest('spaced-dir', { name: 'My Plugin', version: '1.0.0' });
    const [b] = discoverPlugins(dir);
    assert.equal(b!.name, 'spaced-dir');
  });

  it('falls back to the directory name for a non-ASCII manifest name', () => {
    writeManifest('homoglyph-dir', { name: 'pаypal', version: '1.0.0' });
    const [b] = discoverPlugins(dir);
    assert.equal(b!.name, 'homoglyph-dir');
  });

  it('falls back to the directory name when the manifest name is not a string', () => {
    writeManifest('nonstr-dir', { name: 123, version: '1.0.0' });
    const [b] = discoverPlugins(dir);
    assert.equal(b!.name, 'nonstr-dir');
  });
});
