import { test } from 'node:test';
import * as assert from 'node:assert/strict';

import { validatePluginManifest } from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 — plugin-manifest metadata validation + corrective, example-bearing
// messages (the reference parity: PluginManifestMetadataSchema / PluginAuthorSchema,
// utils/plugins/schemas.ts L251-L320).
//
// AMI's validatePluginManifest previously type-checked only name/version and the
// component paths; every other metadata field (description/author/homepage/
// repository/license/keywords) was accepted with any value, and a name with a
// space (legal for isSafeSegment, which only blocks slashes/`..`/NUL) was never
// flagged. These tests lock the new validation + Claude's actionable messages.
// ---------------------------------------------------------------------------

test('name with a space is rejected with a kebab-case corrective message', () => {
  const r = validatePluginManifest({ name: 'my plugin', version: '1.0.0' });
  assert.equal(r.valid, false);
  const e = r.errors.find(e => e.path === 'name');
  assert.ok(e, 'expected a name error');
  assert.match(e!.message, /cannot contain spaces/);
  assert.match(e!.message, /kebab-case/);
  assert.match(e!.message, /my-plugin/);
});

test('a valid kebab-case name with full metadata passes', () => {
  const r = validatePluginManifest({
    name: 'my-plugin',
    version: '1.2.3',
    description: 'Does a thing',
    author: { name: 'Jane Dev', email: 'jane@example.com', url: 'https://example.com' },
    homepage: 'https://example.com/plugin',
    repository: 'https://github.com/jane/my-plugin',
    license: 'MIT',
    keywords: ['cli', 'git'],
  });
  assert.equal(r.valid, true, JSON.stringify(r.errors));
  assert.equal(r.errors.length, 0);
});

test('a bare-string author is accepted', () => {
  const r = validatePluginManifest({ name: 'p', author: 'Jane Dev' });
  assert.equal(r.valid, true, JSON.stringify(r.errors));
});

test('non-string description is rejected with an example', () => {
  const r = validatePluginManifest({ name: 'p', description: 123 });
  const e = r.errors.find(e => e.path === 'description');
  assert.ok(e, 'expected a description error');
  assert.match(e!.message, /must be a string/);
});

test('author object missing a name is rejected', () => {
  const r = validatePluginManifest({ name: 'p', author: { email: 'x@y.z' } });
  const e = r.errors.find(e => e.path === 'author.name');
  assert.ok(e, 'expected an author.name error');
  assert.match(e!.message, /cannot be empty/);
});

test('author object with a non-string email/url is rejected', () => {
  const r = validatePluginManifest({ name: 'p', author: { name: 'Jane', email: 5, url: {} } });
  assert.ok(r.errors.some(e => e.path === 'author.email'));
  assert.ok(r.errors.some(e => e.path === 'author.url'));
});

test('an author that is an array (not an object) is rejected', () => {
  const r = validatePluginManifest({ name: 'p', author: ['Jane'] });
  const e = r.errors.find(e => e.path === 'author');
  assert.ok(e, 'expected an author error');
  assert.match(e!.message, /string or an object/);
});

test('an empty-string author is rejected', () => {
  const r = validatePluginManifest({ name: 'p', author: '   ' });
  assert.ok(r.errors.some(e => e.path === 'author'));
});

test('a non-URL homepage is rejected with a URL example', () => {
  const r = validatePluginManifest({ name: 'p', homepage: 'not a url' });
  const e = r.errors.find(e => e.path === 'homepage');
  assert.ok(e, 'expected a homepage error');
  assert.match(e!.message, /must be a valid URL/);
  assert.match(e!.message, /https:\/\/example\.com/);
});

test('a scheme-less homepage (example.com) is rejected', () => {
  const r = validatePluginManifest({ name: 'p', homepage: 'example.com' });
  assert.ok(r.errors.some(e => e.path === 'homepage'));
});

test('non-string repository and license are rejected', () => {
  const r = validatePluginManifest({ name: 'p', repository: 7, license: ['MIT'] });
  assert.ok(r.errors.some(e => e.path === 'repository'));
  const lic = r.errors.find(e => e.path === 'license');
  assert.ok(lic, 'expected a license error');
  assert.match(lic!.message, /SPDX/);
});

test('keywords must be a string array', () => {
  const bad = validatePluginManifest({ name: 'p', keywords: 'cli' });
  assert.ok(bad.errors.some(e => e.path === 'keywords'));
  const mixed = validatePluginManifest({ name: 'p', keywords: ['cli', 3] });
  assert.ok(mixed.errors.some(e => e.path === 'keywords'));
  const ok = validatePluginManifest({ name: 'p', keywords: [] });
  assert.ok(!ok.errors.some(e => e.path === 'keywords'));
});
