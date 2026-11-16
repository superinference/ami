// Parity unit U104 — additions to core/core/src/skill-catalog.ts:
//   - isValidCatalogName (kebab-case / no-whitespace / reserved-name rejection)
//   - isValidGitRef + resolveSkillUrl `#ref` fragment support
//   - category/tags on SkillCatalogEntry, owner/version on SkillCatalogIndex
//   - isSkillSourceAllowed (supply-chain allow/deny host policy)

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  isValidCatalogName,
  isValidGitRef,
  resolveSkillUrl,
  validateCatalogIndex,
  isSkillSourceAllowed,
  MAX_TAGS_PER_SKILL,
} from '../src/skill-catalog';

// ---------------------------------------------------------------------------
// isValidCatalogName
// ---------------------------------------------------------------------------

describe('isValidCatalogName', () => {
  it('accepts a kebab-case name', () => {
    assert.equal(isValidCatalogName('my-skill'), true);
    assert.equal(isValidCatalogName('skill_2'), true);
  });

  it('rejects a name with whitespace (kebab-case enforcement)', () => {
    assert.equal(isValidCatalogName('my skill'), false);
    assert.equal(isValidCatalogName('tab\tname'), false);
    assert.equal(isValidCatalogName('line\nname'), false);
  });

  it('rejects reserved loader names regardless of case', () => {
    assert.equal(isValidCatalogName('inline'), false);
    assert.equal(isValidCatalogName('BUILTIN'), false);
  });

  it('rejects path-traversal / separators (delegates to isSafeSegment)', () => {
    assert.equal(isValidCatalogName('..'), false);
    assert.equal(isValidCatalogName('a/b'), false);
    assert.equal(isValidCatalogName('%2e%2e'), false);
  });

  it('rejects a non-string input', () => {
    assert.equal(isValidCatalogName(42 as unknown as string), false);
    assert.equal(isValidCatalogName(undefined as unknown as string), false);
  });
});

// ---------------------------------------------------------------------------
// isValidGitRef
// ---------------------------------------------------------------------------

describe('isValidGitRef', () => {
  it('accepts simple branch/tag/commit refs', () => {
    assert.equal(isValidGitRef('main'), true);
    assert.equal(isValidGitRef('v1.2.3'), true);
    assert.equal(isValidGitRef('feature/new-thing'), true);
    assert.equal(isValidGitRef('a1b2c3d'), true);
  });

  it('rejects traversal, empty segments and trailing separators', () => {
    assert.equal(isValidGitRef('..'), false);
    assert.equal(isValidGitRef('a/../b'), false);
    assert.equal(isValidGitRef('a//b'), false);
    assert.equal(isValidGitRef('trailing/'), false);
    assert.equal(isValidGitRef('trailing.'), false);
  });

  it('rejects whitespace, unsafe chars and empty/over-long refs', () => {
    assert.equal(isValidGitRef('has space'), false);
    assert.equal(isValidGitRef('bad~ref'), false);
    assert.equal(isValidGitRef('-leading-dash'), false);
    assert.equal(isValidGitRef(''), false);
    assert.equal(isValidGitRef('a'.repeat(256)), false);
  });
});

// ---------------------------------------------------------------------------
// resolveSkillUrl — #ref fragment support
// ---------------------------------------------------------------------------

describe('resolveSkillUrl with #ref', () => {
  it('applies a valid ref to a GitHub shorthand instead of main', () => {
    assert.equal(
      resolveSkillUrl('owner/repo#v2.0.0'),
      'https://raw.githubusercontent.com/owner/repo/v2.0.0',
    );
  });

  it('applies a ref with a subpath', () => {
    assert.equal(
      resolveSkillUrl('owner/repo/catalog#release/2024'),
      'https://raw.githubusercontent.com/owner/repo/release/2024/catalog',
    );
  });

  it('falls back to main when the ref is unsafe', () => {
    assert.equal(
      resolveSkillUrl('owner/repo#../../etc'),
      'https://raw.githubusercontent.com/owner/repo/main',
    );
  });

  it('still defaults to main with no fragment', () => {
    assert.equal(
      resolveSkillUrl('owner/repo'),
      'https://raw.githubusercontent.com/owner/repo/main',
    );
  });

  it('strips a fragment from an absolute URL (ref not spliced)', () => {
    assert.equal(
      resolveSkillUrl('https://example.com/catalog#frag'),
      'https://example.com/catalog',
    );
  });
});

// ---------------------------------------------------------------------------
// validateCatalogIndex — new metadata fields
// ---------------------------------------------------------------------------

describe('validateCatalogIndex metadata', () => {
  it('rejects a skill name containing a space', () => {
    const r = validateCatalogIndex({ skills: [{ name: 'bad name', files: ['SKILL.md'] }] });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => /unsafe skill name/.test(e)));
  });

  it('parses per-skill category and tags', () => {
    const r = validateCatalogIndex({
      skills: [
        {
          name: 'my-skill',
          files: ['SKILL.md'],
          category: 'productivity',
          tags: ['cli', 'search', 42, '', 'cli'],
        },
      ],
    });
    assert.equal(r.valid, true);
    const s = r.index!.skills[0];
    assert.equal(s.category, 'productivity');
    assert.deepEqual(s.tags, ['cli', 'search', 'cli']);
  });

  it('drops a non-array tags field and non-string category', () => {
    const r = validateCatalogIndex({
      skills: [{ name: 'my-skill', files: ['SKILL.md'], category: 7, tags: 'nope' }],
    });
    assert.equal(r.valid, true);
    assert.equal(r.index!.skills[0].category, undefined);
    assert.equal(r.index!.skills[0].tags, undefined);
  });

  it('caps tags at MAX_TAGS_PER_SKILL', () => {
    const many = Array.from({ length: MAX_TAGS_PER_SKILL + 50 }, (_, i) => `t${i}`);
    const r = validateCatalogIndex({ skills: [{ name: 'x', files: ['x.md'], tags: many }] });
    assert.equal(r.valid, true);
    assert.equal(r.index!.skills[0].tags!.length, MAX_TAGS_PER_SKILL);
  });

  it('parses top-level catalog version and owner (string)', () => {
    const r = validateCatalogIndex({
      version: '3.1.0',
      owner: 'Acme Corp',
      skills: [{ name: 'x', files: ['x.md'] }],
    });
    assert.equal(r.valid, true);
    assert.equal(r.index!.version, '3.1.0');
    assert.equal(r.index!.owner, 'Acme Corp');
  });

  it('reads version from metadata.version and owner from {name}/author alias', () => {
    const r = validateCatalogIndex({
      metadata: { version: '9.9.9' },
      owner: { name: 'Owner Name', email: 'x@y.z' },
      skills: [{ name: 'x', files: ['x.md'] }],
    });
    assert.equal(r.index!.version, '9.9.9');
    assert.equal(r.index!.owner, 'Owner Name');

    const r2 = validateCatalogIndex({ author: 'Author Only', skills: [{ name: 'x', files: ['x.md'] }] });
    assert.equal(r2.index!.owner, 'Author Only');
  });

  it('leaves version/owner undefined when absent or malformed', () => {
    const r = validateCatalogIndex({ version: 5, owner: ['arr'], skills: [{ name: 'x', files: ['x.md'] }] });
    assert.equal(r.index!.version, undefined);
    assert.equal(r.index!.owner, undefined);
  });
});

// ---------------------------------------------------------------------------
// isSkillSourceAllowed — supply-chain allow/deny host policy
// ---------------------------------------------------------------------------

describe('isSkillSourceAllowed', () => {
  it('allows everything with no policy or an empty policy', () => {
    assert.equal(isSkillSourceAllowed('https://evil.test/x'), true);
    assert.equal(isSkillSourceAllowed('https://evil.test/x', {}), true);
    assert.equal(isSkillSourceAllowed('https://evil.test/x', { allow: [], deny: [] }), true);
  });

  it('blocks a denied host (deny takes precedence over allow)', () => {
    assert.equal(
      isSkillSourceAllowed('https://evil.test/x', { deny: ['evil.test'] }),
      false,
    );
    assert.equal(
      isSkillSourceAllowed('https://evil.test/x', { allow: ['evil.test'], deny: ['evil.test'] }),
      false,
    );
  });

  it('with a non-empty allow list, only matching hosts pass', () => {
    const policy = { allow: ['raw.githubusercontent.com'] };
    assert.equal(isSkillSourceAllowed('https://raw.githubusercontent.com/o/r/main', policy), true);
    assert.equal(isSkillSourceAllowed('https://elsewhere.test/x', policy), false);
  });

  it('supports *.suffix wildcard patterns (incl. apex)', () => {
    const policy = { allow: ['*.example.com'] };
    assert.equal(isSkillSourceAllowed('https://cdn.example.com/x', policy), true);
    assert.equal(isSkillSourceAllowed('https://example.com/x', policy), true);
    assert.equal(isSkillSourceAllowed('https://evil-example.com/x', policy), false);
  });

  it('is case-insensitive on the hostname', () => {
    assert.equal(isSkillSourceAllowed('https://EVIL.Test/x', { deny: ['evil.test'] }), false);
  });

  it('fails closed on an unparseable URL when a policy is active', () => {
    assert.equal(isSkillSourceAllowed('not a url', { allow: ['x.test'] }), false);
  });
});
