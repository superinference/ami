// POV: core skill/plugin building blocks — behavioral regression coverage for
// the path-safety validators, catalog validation, URL resolution, skill-arg
// sanitization/validation, the stance discriminator, and local plugin-bundle
// discovery + TOML command loading. Every assertion exercises the REAL exported
// function and checks an OBSERVABLE result; fixtures are built on disk with
// mkdtempSync and cleaned up in after().

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  isSafeSegment,
  isSafeRelativePath,
  validateCatalogIndex,
  resolveSkillUrl,
  SKILL_TRUST_TIERS,
} from '../src/skill-catalog';
import {
  sanitizeSkillArgs,
  validateSkillArgs,
  isStanceSkill,
  MAX_SKILL_ARG_NAMES,
  MAX_SKILL_ARG_NAME_LENGTH,
  MAX_SKILL_ARG_VALUE_LENGTH,
  MAX_SKILL_ARG_TOTAL_CHARS,
  type SkillDefinition,
} from '../src/skills';
import { discoverPlugins, loadCommands } from '../src/plugins';

// ===========================================================================
// skill-catalog.ts :: isSafeSegment
// ===========================================================================

describe('isSafeSegment', () => {
  it('accepts a plain filename segment', () => {
    assert.equal(isSafeSegment('SKILL.md'), true);
    assert.equal(isSafeSegment('my-skill'), true);
    assert.equal(isSafeSegment('a.b.c'), true);
  });

  it('rejects empty string', () => {
    assert.equal(isSafeSegment(''), false);
  });

  it('rejects dot and dot-dot traversal segments', () => {
    assert.equal(isSafeSegment('.'), false);
    assert.equal(isSafeSegment('..'), false);
  });

  it('rejects a NUL byte', () => {
    assert.equal(isSafeSegment('foo\0bar'), false);
  });

  it('rejects forward-slash and backslash separators', () => {
    assert.equal(isSafeSegment('a/b'), false);
    assert.equal(isSafeSegment('a\\b'), false);
  });

  it('rejects an over-length (>255) segment but accepts exactly 255', () => {
    assert.equal(isSafeSegment('x'.repeat(256)), false);
    assert.equal(isSafeSegment('x'.repeat(255)), true);
  });
});

// ===========================================================================
// skill-catalog.ts :: isSafeRelativePath
// ===========================================================================

describe('isSafeRelativePath', () => {
  it('accepts a simple nested relative path', () => {
    assert.equal(isSafeRelativePath('skills/foo/SKILL.md'), true);
    assert.equal(isSafeRelativePath('SKILL.md'), true);
  });

  it('rejects empty path', () => {
    assert.equal(isSafeRelativePath(''), false);
  });

  it('rejects an absolute path', () => {
    assert.equal(isSafeRelativePath('/etc/passwd'), false);
  });

  it('rejects a parent-traversal path', () => {
    assert.equal(isSafeRelativePath('../secret'), false);
    assert.equal(isSafeRelativePath('../../etc/passwd'), false);
  });

  it('rejects traversal that normalizes above root', () => {
    // a/../../b normalizes to ../b which starts with '..'
    assert.equal(isSafeRelativePath('a/../../b'), false);
  });

  it('accepts an internal traversal that stays within root after normalization', () => {
    // foo/bar/../baz normalizes to foo/baz — safe
    assert.equal(isSafeRelativePath('foo/bar/../baz'), true);
  });

  it('rejects a path containing a NUL segment', () => {
    assert.equal(isSafeRelativePath('foo/ba\0r'), false);
  });
});

// ===========================================================================
// skill-catalog.ts :: validateCatalogIndex
// ===========================================================================

describe('validateCatalogIndex', () => {
  it('rejects non-object input (null / array / primitive)', () => {
    for (const bad of [null, undefined, 42, 'str', [], true]) {
      const r = validateCatalogIndex(bad as unknown);
      assert.equal(r.valid, false);
      assert.ok(r.errors.length > 0);
      assert.equal(r.index, undefined);
    }
  });

  it('rejects object without a skills array', () => {
    const r = validateCatalogIndex({ name: 'x' });
    assert.equal(r.valid, false);
    assert.match(r.errors[0], /"skills" array/);
  });

  it('accepts a well-formed catalog and returns a normalized index', () => {
    const r = validateCatalogIndex({
      name: 'cat',
      description: 'desc',
      skills: [
        { name: 'alpha', version: '1.0.0', description: 'A', files: ['SKILL.md'] },
        { name: 'beta', files: ['skills/beta/SKILL.md', 'skills/beta/ref.md'] },
      ],
    });
    assert.equal(r.valid, true);
    assert.deepEqual(r.errors, []);
    assert.ok(r.index);
    assert.equal(r.index!.name, 'cat');
    assert.equal(r.index!.description, 'desc');
    assert.equal(r.index!.skills.length, 2);
    assert.equal(r.index!.skills[0].version, '1.0.0');
    assert.equal(r.index!.skills[1].version, undefined);
    assert.deepEqual(r.index!.skills[1].files, ['skills/beta/SKILL.md', 'skills/beta/ref.md']);
  });

  it('drops a non-string version/description to undefined', () => {
    const r = validateCatalogIndex({
      name: 123,
      description: { not: 'string' },
      skills: [{ name: 'a', version: 9, description: [], files: ['SKILL.md'] }],
    });
    assert.equal(r.valid, true);
    assert.equal(r.index!.name, undefined);
    assert.equal(r.index!.description, undefined);
    assert.equal(r.index!.skills[0].version, undefined);
    assert.equal(r.index!.skills[0].description, undefined);
  });

  it('records an error for a non-object skill entry but keeps valid ones', () => {
    const r = validateCatalogIndex({
      skills: [null, { name: 'ok', files: ['SKILL.md'] }],
    });
    assert.equal(r.valid, true);
    assert.equal(r.index!.skills.length, 1);
    assert.ok(r.errors.some(e => /skills\[0\]/.test(e)));
  });

  it('records an error for a missing/empty name', () => {
    const r = validateCatalogIndex({
      skills: [{ name: '   ', files: ['SKILL.md'] }],
    });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => /empty "name"/.test(e)));
  });

  it('records an error for an unsafe (traversal) skill name', () => {
    const r = validateCatalogIndex({
      skills: [{ name: '../evil', files: ['SKILL.md'] }],
    });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => /unsafe skill name/.test(e)));
  });

  it('records an error for missing/empty files array', () => {
    const r1 = validateCatalogIndex({ skills: [{ name: 'a' }] });
    assert.equal(r1.valid, false);
    assert.ok(r1.errors.some(e => /missing or empty "files"/.test(e)));
    const r2 = validateCatalogIndex({ skills: [{ name: 'a', files: [] }] });
    assert.equal(r2.valid, false);
  });

  it('rejects an unsafe file path but keeps safe files in the same entry', () => {
    const r = validateCatalogIndex({
      skills: [{ name: 'mix', files: ['good.md', '../bad.md', 42] }],
    });
    // One safe file remains, so the entry survives and the index is valid.
    assert.equal(r.valid, true);
    assert.deepEqual(r.index!.skills[0].files, ['good.md']);
    assert.ok(r.errors.some(e => /unsafe file path/.test(e)));
    assert.ok(r.errors.some(e => /file entry must be a string/.test(e)));
  });

  it('drops an entry whose files are all unsafe', () => {
    const r = validateCatalogIndex({
      skills: [{ name: 'allbad', files: ['../a', '/b'] }],
    });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => /no valid file paths/.test(e)));
    assert.equal(r.index, undefined);
  });

  it('is valid with an empty skills array (no entries, no errors)', () => {
    const r = validateCatalogIndex({ skills: [] });
    assert.equal(r.valid, true);
    assert.deepEqual(r.errors, []);
    assert.deepEqual(r.index!.skills, []);
  });
});

// ===========================================================================
// skill-catalog.ts :: resolveSkillUrl
// ===========================================================================

describe('resolveSkillUrl', () => {
  it('passes through an http(s) URL, trimming trailing slashes', () => {
    assert.equal(resolveSkillUrl('https://example.com/cat/'), 'https://example.com/cat');
    assert.equal(resolveSkillUrl('http://example.com/x///'), 'http://example.com/x');
    assert.equal(resolveSkillUrl('  https://example.com/y  '), 'https://example.com/y');
  });

  it('expands owner/repo shorthand to raw.githubusercontent main', () => {
    assert.equal(
      resolveSkillUrl('acme/catalog'),
      'https://raw.githubusercontent.com/acme/catalog/main',
    );
  });

  it('appends a subpath for owner/repo/sub/path shorthand', () => {
    assert.equal(
      resolveSkillUrl('acme/catalog/skills/index.json'),
      'https://raw.githubusercontent.com/acme/catalog/main/skills/index.json',
    );
  });

  it('returns a single bare token unchanged (no slash to split)', () => {
    assert.equal(resolveSkillUrl('justone'), 'justone');
  });
});

// ===========================================================================
// skill-catalog.ts :: SKILL_TRUST_TIERS constant
// ===========================================================================

describe('SKILL_TRUST_TIERS', () => {
  it('contains the four known tiers', () => {
    assert.deepEqual([...SKILL_TRUST_TIERS].sort(), ['core', 'installed', 'project', 'user']);
  });
});

// ===========================================================================
// skills.ts :: sanitizeSkillArgs
// ===========================================================================

describe('sanitizeSkillArgs', () => {
  it('passes through a small well-formed arg map unchanged', () => {
    const out = sanitizeSkillArgs({ a: '1', b: 'two' });
    assert.deepEqual(out, { a: '1', b: 'two' });
  });

  it('caps the number of arg names to MAX_SKILL_ARG_NAMES', () => {
    const input: Record<string, string> = {};
    for (let i = 0; i < MAX_SKILL_ARG_NAMES + 20; i++) input[`k${i}`] = 'v';
    const out = sanitizeSkillArgs(input);
    assert.ok(Object.keys(out).length <= MAX_SKILL_ARG_NAMES);
  });

  it('truncates an over-length key name', () => {
    const longKey = 'k'.repeat(MAX_SKILL_ARG_NAME_LENGTH + 50);
    const out = sanitizeSkillArgs({ [longKey]: 'v' });
    const resultKey = Object.keys(out)[0];
    assert.equal(resultKey.length, MAX_SKILL_ARG_NAME_LENGTH);
  });

  it('truncates an over-length value', () => {
    const bigVal = 'x'.repeat(MAX_SKILL_ARG_VALUE_LENGTH + 500);
    const out = sanitizeSkillArgs({ k: bigVal });
    assert.equal(out.k.length, MAX_SKILL_ARG_VALUE_LENGTH);
  });

  it('coerces a non-string value via String()', () => {
    const out = sanitizeSkillArgs({ n: 42 as unknown as string });
    assert.equal(out.n, '42');
  });

  it('stops adding args once total char budget is exceeded', () => {
    // Each value is just under the per-value cap; many of them blow the total cap.
    const input: Record<string, string> = {};
    const chunk = 'y'.repeat(MAX_SKILL_ARG_VALUE_LENGTH);
    for (let i = 0; i < 40; i++) input[`k${i}`] = chunk;
    const out = sanitizeSkillArgs(input);
    const total = Object.entries(out).reduce((n, [k, v]) => n + k.length + v.length, 0);
    assert.ok(total <= MAX_SKILL_ARG_TOTAL_CHARS);
    // Should have stopped well before all 40 keys made it in.
    assert.ok(Object.keys(out).length < 40);
  });

  it('returns an empty object for empty input', () => {
    assert.deepEqual(sanitizeSkillArgs({}), {});
  });
});

// ===========================================================================
// skills.ts :: validateSkillArgs
// ===========================================================================

describe('validateSkillArgs', () => {
  it('reports no warnings for normal args and is always valid', () => {
    const r = validateSkillArgs({ a: '1', b: 'two' });
    assert.equal(r.valid, true);
    assert.deepEqual(r.warnings, []);
  });

  it('warns when there are too many arg names', () => {
    const input: Record<string, string> = {};
    for (let i = 0; i < MAX_SKILL_ARG_NAMES + 5; i++) input[`k${i}`] = 'v';
    const r = validateSkillArgs(input);
    assert.equal(r.valid, true);
    assert.ok(r.warnings.some(w => /Too many arguments/.test(w)));
  });

  it('warns on an over-length argument name', () => {
    const longKey = 'k'.repeat(MAX_SKILL_ARG_NAME_LENGTH + 1);
    const r = validateSkillArgs({ [longKey]: 'v' });
    assert.ok(r.warnings.some(w => /Argument name too long/.test(w)));
  });

  it('warns on an over-length argument value', () => {
    const bigVal = 'x'.repeat(MAX_SKILL_ARG_VALUE_LENGTH + 1);
    const r = validateSkillArgs({ k: bigVal });
    assert.ok(r.warnings.some(w => /Argument value too long/.test(w)));
  });

  it('warns when total argument size exceeds the limit', () => {
    const input: Record<string, string> = {};
    const chunk = 'z'.repeat(MAX_SKILL_ARG_VALUE_LENGTH);
    for (let i = 0; i < 20; i++) input[`k${i}`] = chunk;
    const r = validateSkillArgs(input);
    assert.ok(r.warnings.some(w => /Total argument size/.test(w)));
  });

  it('coerces a non-string value before measuring length', () => {
    const r = validateSkillArgs({ n: 12345 as unknown as string });
    assert.equal(r.valid, true);
    assert.deepEqual(r.warnings, []);
  });
});

// ===========================================================================
// skills.ts :: isStanceSkill
// ===========================================================================

function makeSkill(overrides: Partial<SkillDefinition>): SkillDefinition {
  return {
    name: 'test',
    description: 'd',
    content: 'body',
    filePath: '<builtin>',
    ...overrides,
  };
}

describe('isStanceSkill', () => {
  it('is a stance when it carries non-empty autoAllowPatterns', () => {
    assert.equal(isStanceSkill(makeSkill({ autoAllowPatterns: ['nmap*'] })), true);
  });

  it('is a stance when it declares MCP servers', () => {
    assert.equal(
      isStanceSkill(makeSkill({ mcpServers: { kali: { command: 'x' } as never } })),
      true,
    );
  });

  it('is a stance when it carries mcpAutoAllowPatterns', () => {
    assert.equal(isStanceSkill(makeSkill({ mcpAutoAllowPatterns: ['mcp__kali__*'] })), true);
  });

  it('is a stance when it sets a defaultThinkingLevel', () => {
    assert.equal(isStanceSkill(makeSkill({ defaultThinkingLevel: 'high' as never })), true);
  });

  it('is NOT a stance for a plain skill', () => {
    assert.equal(isStanceSkill(makeSkill({})), false);
  });

  it('is NOT a stance when stance arrays/objects are empty', () => {
    assert.equal(
      isStanceSkill(makeSkill({ autoAllowPatterns: [], mcpServers: {}, mcpAutoAllowPatterns: [] })),
      false,
    );
  });

  it('is NOT a stance for a disable-model-invocation workflow doc', () => {
    assert.equal(isStanceSkill(makeSkill({ disableModelInvocation: true })), false);
  });
});

// ===========================================================================
// plugins.ts :: discoverPlugins + loadCommands (on-disk fixtures)
// ===========================================================================

describe('discoverPlugins + loadCommands', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pov-plugins-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  function writeManifest(bundleRoot: string, manifestDir: string, manifest: Record<string, unknown>) {
    const dir = path.join(bundleRoot, manifestDir);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'plugin.json'), JSON.stringify(manifest));
  }

  it('returns [] for a nonexistent directory', () => {
    assert.deepEqual(discoverPlugins(path.join(tmpDir, 'does-not-exist')), []);
  });

  it('returns [] for an empty directory', () => {
    assert.deepEqual(discoverPlugins(tmpDir), []);
  });

  it('discovers an .ami-plugin bundle with name/version from the manifest', () => {
    const root = path.join(tmpDir, 'bundle-a');
    writeManifest(root, '.ami-plugin', { name: 'alpha', version: '2.1.0' });
    fs.mkdirSync(path.join(root, 'skills'));
    fs.mkdirSync(path.join(root, 'commands'));
    const found = discoverPlugins(tmpDir);
    assert.equal(found.length, 1);
    assert.equal(found[0].name, 'alpha');
    assert.equal(found[0].version, '2.1.0');
    assert.ok(found[0].skillsDir && found[0].skillsDir.endsWith('skills'));
    assert.ok(found[0].commandsDir && found[0].commandsDir.endsWith('commands'));
    // No agents/ dir or hooks file present -> undefined
    assert.equal(found[0].agentsDir, undefined);
    assert.equal(found[0].hooksFile, undefined);
  });

  it('discovers a .claude-plugin bundle as a fallback manifest', () => {
    const root = path.join(tmpDir, 'bundle-cc');
    writeManifest(root, '.claude-plugin', { name: 'ccplug', version: '0.3.0' });
    const found = discoverPlugins(tmpDir);
    assert.equal(found.length, 1);
    assert.equal(found[0].name, 'ccplug');
    assert.equal(found[0].version, '0.3.0');
  });

  it('discovers both an .ami-plugin AND a .claude-plugin bundle in the same dir', () => {
    const amiRoot = path.join(tmpDir, 'ami-one');
    writeManifest(amiRoot, '.ami-plugin', { name: 'ami-one', version: '1.0.0' });
    const ccRoot = path.join(tmpDir, 'cc-two');
    writeManifest(ccRoot, '.claude-plugin', { name: 'cc-two', version: '1.1.0' });
    const found = discoverPlugins(tmpDir);
    const names = found.map(b => b.name).sort();
    assert.deepEqual(names, ['ami-one', 'cc-two']);
  });

  it('prefers .ami-plugin over .claude-plugin when both exist in one bundle', () => {
    const root = path.join(tmpDir, 'bundle-both');
    writeManifest(root, '.ami-plugin', { name: 'ami-wins', version: '9.9.9' });
    writeManifest(root, '.claude-plugin', { name: 'cc-loses', version: '0.0.1' });
    const found = discoverPlugins(tmpDir);
    assert.equal(found.length, 1);
    assert.equal(found[0].name, 'ami-wins');
    assert.equal(found[0].version, '9.9.9');
  });

  it('falls back to directory name and 0.0.0 when manifest omits name/version', () => {
    const root = path.join(tmpDir, 'nameless');
    writeManifest(root, '.ami-plugin', {});
    const found = discoverPlugins(tmpDir);
    assert.equal(found.length, 1);
    assert.equal(found[0].name, 'nameless');
    assert.equal(found[0].version, '0.0.0');
  });

  it('skips a directory without any plugin manifest', () => {
    fs.mkdirSync(path.join(tmpDir, 'not-a-plugin'));
    fs.writeFileSync(path.join(tmpDir, 'not-a-plugin', 'readme.txt'), 'hi');
    assert.deepEqual(discoverPlugins(tmpDir), []);
  });

  it('skips a plain file sitting alongside bundles', () => {
    fs.writeFileSync(path.join(tmpDir, 'loose.txt'), 'x');
    const root = path.join(tmpDir, 'bundle-x');
    writeManifest(root, '.ami-plugin', { name: 'x', version: '1.0.0' });
    const found = discoverPlugins(tmpDir);
    assert.equal(found.length, 1);
    assert.equal(found[0].name, 'x');
  });

  it('skips a bundle with malformed JSON manifest without failing discovery', () => {
    const badRoot = path.join(tmpDir, 'bad');
    fs.mkdirSync(path.join(badRoot, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(path.join(badRoot, '.ami-plugin', 'plugin.json'), '{ not json');
    const goodRoot = path.join(tmpDir, 'good');
    writeManifest(goodRoot, '.ami-plugin', { name: 'good', version: '1.0.0' });
    const found = discoverPlugins(tmpDir);
    assert.equal(found.length, 1);
    assert.equal(found[0].name, 'good');
  });

  it('resolves hooks.json at the bundle root', () => {
    const root = path.join(tmpDir, 'with-hooks');
    writeManifest(root, '.ami-plugin', { name: 'h', version: '1.0.0' });
    fs.writeFileSync(path.join(root, 'hooks.json'), '{}');
    const found = discoverPlugins(tmpDir);
    assert.ok(found[0].hooksFile && found[0].hooksFile.endsWith('hooks.json'));
  });

  it('resolves hooks/hooks.json via the Claude Code convention fallback', () => {
    const root = path.join(tmpDir, 'with-hooks-subdir');
    writeManifest(root, '.ami-plugin', { name: 'h2', version: '1.0.0' });
    fs.mkdirSync(path.join(root, 'hooks'));
    fs.writeFileSync(path.join(root, 'hooks', 'hooks.json'), '{}');
    const found = discoverPlugins(tmpDir);
    assert.ok(found[0].hooksFile && found[0].hooksFile.endsWith(path.join('hooks', 'hooks.json')));
  });

  it('exposes agentsDir when an agents/ directory exists', () => {
    const root = path.join(tmpDir, 'with-agents');
    writeManifest(root, '.ami-plugin', { name: 'a', version: '1.0.0' });
    fs.mkdirSync(path.join(root, 'agents'));
    const found = discoverPlugins(tmpDir);
    assert.ok(found[0].agentsDir && found[0].agentsDir.endsWith('agents'));
  });

  // --- loadCommands -------------------------------------------------------

  it('loadCommands returns [] for a nonexistent dir', () => {
    assert.deepEqual(loadCommands(path.join(tmpDir, 'nope')), []);
  });

  it('loadCommands returns [] for a dir with no .toml files', () => {
    const dir = path.join(tmpDir, 'cmds-empty');
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(dir, 'readme.md'), 'x');
    assert.deepEqual(loadCommands(dir), []);
  });

  it('loadCommands parses a well-formed TOML command (description + prompt)', () => {
    const dir = path.join(tmpDir, 'cmds');
    fs.mkdirSync(dir);
    fs.writeFileSync(
      path.join(dir, 'deploy.toml'),
      'description = "Deploy the app"\nprompt = "Run the deploy steps"\n',
    );
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0].name, 'deploy');
    assert.equal(cmds[0].description, 'Deploy the app');
    assert.equal(cmds[0].prompt, 'Run the deploy steps');
    assert.ok(cmds[0].filePath.endsWith('deploy.toml'));
  });

  it('loadCommands decodes backslash escapes (\\n, \\t, \\\\, \\") in values', () => {
    const dir = path.join(tmpDir, 'cmds-esc');
    fs.mkdirSync(dir);
    // On disk: prompt = "line1\nline2\ttab\\slash\"q"
    fs.writeFileSync(
      path.join(dir, 'esc.toml'),
      'description = "d"\nprompt = "line1\\nline2\\ttab\\\\slash\\"q"\n',
    );
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0].prompt, 'line1\nline2\ttab\\slash"q');
  });

  it('loadCommands skips a TOML file missing the prompt field', () => {
    const dir = path.join(tmpDir, 'cmds-nop');
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(dir, 'half.toml'), 'description = "only desc"\n');
    assert.deepEqual(loadCommands(dir), []);
  });

  it('loadCommands skips a TOML file missing the description field', () => {
    const dir = path.join(tmpDir, 'cmds-nod');
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(dir, 'half.toml'), 'prompt = "only prompt"\n');
    assert.deepEqual(loadCommands(dir), []);
  });

  it('loadCommands loads multiple commands and ignores non-.toml files', () => {
    const dir = path.join(tmpDir, 'cmds-multi');
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(dir, 'a.toml'), 'description = "A"\nprompt = "pa"\n');
    fs.writeFileSync(path.join(dir, 'b.toml'), 'description = "B"\nprompt = "pb"\n');
    fs.writeFileSync(path.join(dir, 'notes.txt'), 'ignore me');
    const cmds = loadCommands(dir);
    assert.deepEqual(cmds.map(c => c.name).sort(), ['a', 'b']);
  });

  it('loadCommands handles a value with no closing quote by returning what it has', () => {
    const dir = path.join(tmpDir, 'cmds-open');
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(dir, 'open.toml'), 'description = "ok"\nprompt = "unterminated value\n');
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0].prompt, 'unterminated value');
  });

  it('loadCommands returns [] when the dir contains only files that fail parsing', () => {
    const dir = path.join(tmpDir, 'cmds-bad');
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(dir, 'empty.toml'), '');
    assert.deepEqual(loadCommands(dir), []);
  });

  // --- integration: discovered commandsDir feeds loadCommands -------------

  it('a discovered bundle commandsDir can be passed straight to loadCommands', () => {
    const root = path.join(tmpDir, 'full-bundle');
    writeManifest(root, '.ami-plugin', { name: 'full', version: '1.0.0' });
    const cmdDir = path.join(root, 'commands');
    fs.mkdirSync(cmdDir);
    fs.writeFileSync(path.join(cmdDir, 'hi.toml'), 'description = "say hi"\nprompt = "Hello"\n');
    const bundle = discoverPlugins(tmpDir)[0];
    assert.ok(bundle.commandsDir);
    const cmds = loadCommands(bundle.commandsDir!);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0].name, 'hi');
    assert.equal(cmds[0].prompt, 'Hello');
  });
});
