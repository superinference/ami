// Parity unit U104 (cont.) — core building blocks added to skill-catalog.ts:
//   - validateInstalledSkillManifest: defensive schema validation of the on-disk
//     installed-skills cache, with schemaVersion mismatch invalidation
//   - INSTALLED_MANIFEST_SCHEMA_VERSION stamp
//   - compareSemver / isAmiVersionCompatible: minimum-AMI-version floor
//   - SkillCatalogEntry.requires parsed by validateCatalogIndex

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  validateInstalledSkillManifest,
  INSTALLED_MANIFEST_SCHEMA_VERSION,
  compareSemver,
  isAmiVersionCompatible,
  validateCatalogIndex,
} from '../src/skill-catalog';

// ---------------------------------------------------------------------------
// validateInstalledSkillManifest
// ---------------------------------------------------------------------------

describe('validateInstalledSkillManifest', () => {
  it('accepts a well-formed manifest and sanitizes skills', () => {
    const m = validateInstalledSkillManifest({
      url: 'https://example.com/skills',
      catalogName: 'cat',
      catalogVersion: '2.0.0',
      owner: 'Acme',
      schemaVersion: INSTALLED_MANIFEST_SCHEMA_VERSION,
      fetchedAt: '2026-01-01T00:00:00Z',
      skills: [
        { name: 'a', version: '1.0.0', category: 'dev', tags: ['x', 42, '', 'y'] },
        { name: 'b' },
      ],
    });
    assert.ok(m);
    assert.equal(m!.url, 'https://example.com/skills');
    assert.equal(m!.catalogVersion, '2.0.0');
    assert.equal(m!.owner, 'Acme');
    assert.equal(m!.schemaVersion, INSTALLED_MANIFEST_SCHEMA_VERSION);
    assert.equal(m!.skills.length, 2);
    assert.equal(m!.skills[0].category, 'dev');
    assert.deepEqual(m!.skills[0].tags, ['x', 'y']);
  });

  it('accepts a legacy manifest with no schemaVersion (implicit baseline)', () => {
    const m = validateInstalledSkillManifest({
      url: 'https://example.com/skills',
      fetchedAt: '',
      skills: [{ name: 'legacy' }],
    });
    assert.ok(m);
    assert.equal(m!.schemaVersion, undefined);
    assert.equal(m!.skills[0].name, 'legacy');
  });

  it('INVALIDATES a manifest whose schemaVersion does not match (mismatch invalidation)', () => {
    const m = validateInstalledSkillManifest({
      url: 'https://example.com/skills',
      schemaVersion: INSTALLED_MANIFEST_SCHEMA_VERSION + 1,
      fetchedAt: '',
      skills: [{ name: 'a' }],
    });
    assert.equal(m, null);
  });

  it('invalidates a manifest with a non-numeric schemaVersion stamp', () => {
    assert.equal(
      validateInstalledSkillManifest({ url: 'u', schemaVersion: 'nope', fetchedAt: '', skills: [] }),
      null,
    );
  });

  it('drops skill entries with unsafe names (traversal defense)', () => {
    const m = validateInstalledSkillManifest({
      url: 'u',
      fetchedAt: '',
      skills: [{ name: '../escape' }, { name: 'ok' }, { name: 'a/b' }, { foo: 1 }],
    });
    assert.ok(m);
    assert.deepEqual(m!.skills.map(s => s.name), ['ok']);
  });

  it('returns null for structurally invalid input', () => {
    assert.equal(validateInstalledSkillManifest(null), null);
    assert.equal(validateInstalledSkillManifest('str'), null);
    assert.equal(validateInstalledSkillManifest([]), null);
    assert.equal(validateInstalledSkillManifest({ skills: [] }), null); // no url
    assert.equal(validateInstalledSkillManifest({ url: 'u' }), null); // no skills
    assert.equal(validateInstalledSkillManifest({ url: 'u', skills: 'nope' }), null);
  });

  it('defaults fetchedAt to empty string when absent/malformed', () => {
    const m = validateInstalledSkillManifest({ url: 'u', fetchedAt: 5, skills: [] });
    assert.ok(m);
    assert.equal(m!.fetchedAt, '');
  });
});

// ---------------------------------------------------------------------------
// compareSemver
// ---------------------------------------------------------------------------

describe('compareSemver', () => {
  it('orders by major, minor, patch', () => {
    assert.equal(compareSemver('1.0.0', '2.0.0'), -1);
    assert.equal(compareSemver('2.0.0', '1.9.9'), 1);
    assert.equal(compareSemver('1.2.3', '1.2.3'), 0);
    assert.equal(compareSemver('1.2.0', '1.2.1'), -1);
  });

  it('ignores prerelease/build suffixes and pads missing segments', () => {
    assert.equal(compareSemver('1.2.3-beta', '1.2.3'), 0);
    assert.equal(compareSemver('1.2', '1.2.0'), 0);
    assert.equal(compareSemver('1', '1.0.1'), -1);
    assert.equal(compareSemver('2.0.0+build', '2.0.0'), 0);
  });

  it('treats malformed segments as 0 (graceful fallback, no throw)', () => {
    assert.equal(compareSemver('garbage', '0.0.0'), 0);
    assert.equal(compareSemver('1.x.y', '1.0.0'), 0);
  });
});

// ---------------------------------------------------------------------------
// isAmiVersionCompatible
// ---------------------------------------------------------------------------

describe('isAmiVersionCompatible', () => {
  it('imposes no floor when required is absent/empty (graceful default)', () => {
    assert.equal(isAmiVersionCompatible(undefined, '0.1.0'), true);
    assert.equal(isAmiVersionCompatible('', '0.1.0'), true);
    assert.equal(isAmiVersionCompatible('   ', '0.1.0'), true);
  });

  it('passes when current >= required', () => {
    assert.equal(isAmiVersionCompatible('0.7.0', '0.7.4'), true);
    assert.equal(isAmiVersionCompatible('0.7.4', '0.7.4'), true);
  });

  it('fails when current < required', () => {
    assert.equal(isAmiVersionCompatible('1.0.0', '0.7.4'), false);
    assert.equal(isAmiVersionCompatible('0.8.0', '0.7.4'), false);
  });
});

// ---------------------------------------------------------------------------
// validateCatalogIndex — requires field
// ---------------------------------------------------------------------------

describe('validateCatalogIndex requires field', () => {
  it('parses a string requires field on a skill entry', () => {
    const r = validateCatalogIndex({
      skills: [{ name: 'x', files: ['SKILL.md'], requires: '1.2.0' }],
    });
    assert.equal(r.valid, true);
    assert.equal(r.index!.skills[0].requires, '1.2.0');
  });

  it('drops a non-string requires field', () => {
    const r = validateCatalogIndex({
      skills: [{ name: 'x', files: ['SKILL.md'], requires: 99 }],
    });
    assert.equal(r.valid, true);
    assert.equal(r.index!.skills[0].requires, undefined);
  });
});
