import { test } from 'node:test';
import * as assert from 'node:assert/strict';

import { validatePluginManifest } from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 w66 — manifest `dependencies` validation + richer component-path forms.
//
// (1) `dependencies` (Claude parity: DependencyRefSchema, utils/plugins/schemas.ts
//     L1367) declares plugins that must be enabled first. AMI's resolver
//     (common/plugin-dependencies.ts) already consumes them, but the manifest
//     VALIDATOR never flagged a malformed entry — an author typo (a number, an
//     object with no name) was silently dropped. These tests lock the validation
//     + its actionable messages.
//
// (2) Component-path fields (skills/agents/commands/hooks/mcpServers) may be
//     declared as a string, an array of strings, or (commands) an object mapping
//     name → { source, ... } (Claude parity: schemas.ts L429-497). AMI previously
//     validated ONLY the string form (`typeof val !== 'string'` → skip), so an
//     array or record entry containing `..` / an absolute path slipped past the
//     traversal gate. These tests lock traversal validation across all forms.
// ---------------------------------------------------------------------------

// --- dependencies ----------------------------------------------------------

test('dependencies: an array of bare-string refs passes', () => {
  const r = validatePluginManifest({
    name: 'p',
    version: '1.0.0',
    dependencies: ['base-plugin', 'other@market'],
  });
  assert.equal(r.valid, true, JSON.stringify(r.errors));
});

test('dependencies: object refs { name } / { name, marketplace } pass', () => {
  const r = validatePluginManifest({
    name: 'p',
    dependencies: [{ name: 'base' }, { name: 'dep', marketplace: 'official' }],
  });
  assert.equal(r.valid, true, JSON.stringify(r.errors));
});

test('dependencies: a non-array is rejected with an actionable message', () => {
  const r = validatePluginManifest({ name: 'p', dependencies: 'base-plugin' });
  assert.equal(r.valid, false);
  const e = r.errors.find(e => e.path === 'dependencies');
  assert.ok(e, 'expected a dependencies error');
  assert.match(e!.message, /must be an array/);
});

test('dependencies: a non-string / nameless entry is rejected', () => {
  const r = validatePluginManifest({ name: 'p', dependencies: ['ok', 42, {}] });
  assert.equal(r.valid, false);
  const errs = r.errors.filter(e => e.path.startsWith('dependencies['));
  assert.equal(errs.length, 2, JSON.stringify(r.errors));
  assert.ok(errs.some(e => e.path === 'dependencies[1]'));
  assert.ok(errs.some(e => e.path === 'dependencies[2]'));
});

test('dependencies: an object with a non-string marketplace is rejected', () => {
  const r = validatePluginManifest({
    name: 'p',
    dependencies: [{ name: 'dep', marketplace: 7 }],
  });
  assert.equal(r.valid, false);
  const e = r.errors.find(e => e.path === 'dependencies[0].marketplace');
  assert.ok(e, JSON.stringify(r.errors));
  assert.match(e!.message, /must be a string/);
});

test('dependencies: an empty-string ref is rejected', () => {
  const r = validatePluginManifest({ name: 'p', dependencies: ['  '] });
  assert.equal(r.valid, false);
  assert.ok(r.errors.some(e => e.path === 'dependencies[0]'));
});

// --- component-path richer forms -------------------------------------------

test('component path: a string[] with a clean relative entry passes', () => {
  const r = validatePluginManifest({
    name: 'p',
    commands: ['./extra/deploy.toml', 'docs/guide.md'],
  });
  assert.equal(r.valid, true, JSON.stringify(r.errors));
});

test('component path: a `..` entry inside a string[] is flagged (was a traversal hole)', () => {
  const r = validatePluginManifest({
    name: 'p',
    commands: ['ok.toml', '../../../etc/evil.toml'],
  });
  assert.equal(r.valid, false);
  const e = r.errors.find(e => e.path === 'commands[1]');
  assert.ok(e, JSON.stringify(r.errors));
  assert.match(e!.message, /\.\./);
  assert.match(e!.message, /escape/);
});

test('component path: an absolute entry inside a string[] is flagged', () => {
  const r = validatePluginManifest({ name: 'p', skills: ['/abs/skill'] });
  assert.equal(r.valid, false);
  const e = r.errors.find(e => e.path === 'skills[0]');
  assert.ok(e, JSON.stringify(r.errors));
  assert.match(e!.message, /must be relative/);
});

test('component path: a non-string array entry is flagged', () => {
  const r = validatePluginManifest({ name: 'p', agents: ['ok.md', 123] });
  assert.equal(r.valid, false);
  const e = r.errors.find(e => e.path === 'agents[1]');
  assert.ok(e, JSON.stringify(r.errors));
  assert.match(e!.message, /must be a string/);
});

test('component path: a `..` in a record (commands object) source is flagged', () => {
  const r = validatePluginManifest({
    name: 'p',
    commands: {
      about: { source: './cmds/about.md' },
      evil: { source: '../../secrets.md' },
    },
  });
  assert.equal(r.valid, false);
  const e = r.errors.find(e => e.path === 'commands.evil.source');
  assert.ok(e, JSON.stringify(r.errors));
  assert.match(e!.message, /escape/);
});

test('component path: a clean record (commands object) source passes', () => {
  const r = validatePluginManifest({
    name: 'p',
    commands: { about: { source: './cmds/about.md', description: 'About' } },
  });
  assert.equal(r.valid, true, JSON.stringify(r.errors));
});
