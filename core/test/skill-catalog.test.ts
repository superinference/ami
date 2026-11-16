import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  isSafeSegment,
  isSafeRelativePath,
  validateCatalogIndex,
  resolveSkillUrl,
  MAX_CATALOG_SKILLS,
  MAX_FILES_PER_SKILL,
} from '../src/skill-catalog';

// ---------------------------------------------------------------------------
// isSafeSegment
// ---------------------------------------------------------------------------

describe('isSafeSegment', () => {
  it('rejects empty string', () => {
    assert.equal(isSafeSegment(''), false);
  });

  it('rejects "."', () => {
    assert.equal(isSafeSegment('.'), false);
  });

  it('rejects ".."', () => {
    assert.equal(isSafeSegment('..'), false);
  });

  it('rejects strings containing null bytes', () => {
    assert.equal(isSafeSegment('foo\0bar'), false);
  });

  it('rejects strings with forward slash', () => {
    assert.equal(isSafeSegment('a/b'), false);
  });

  it('rejects strings with backslash', () => {
    assert.equal(isSafeSegment('a\\b'), false);
  });

  it('rejects strings longer than 255 characters', () => {
    assert.equal(isSafeSegment('a'.repeat(256)), false);
  });

  it('accepts exactly 255 characters', () => {
    assert.equal(isSafeSegment('a'.repeat(255)), true);
  });

  it('accepts normal hyphenated name', () => {
    assert.equal(isSafeSegment('my-skill'), true);
  });

  it('accepts underscored name', () => {
    assert.equal(isSafeSegment('code_review'), true);
  });

  it('accepts name with dots (not just "." or "..")', () => {
    assert.equal(isSafeSegment('v1.0'), true);
  });

  it('accepts single character name', () => {
    assert.equal(isSafeSegment('a'), true);
  });

  it('accepts name with numbers', () => {
    assert.equal(isSafeSegment('skill123'), true);
  });

  it('accepts name with spaces (no slash/backslash/null)', () => {
    assert.equal(isSafeSegment('my skill'), true);
  });

  // --- URL-encoded traversal rejection (decodeURIComponent re-check) ---

  it('rejects percent-encoded ".." (%2e%2e)', () => {
    assert.equal(isSafeSegment('%2e%2e'), false);
  });

  it('rejects percent-encoded forward slash (%2f)', () => {
    assert.equal(isSafeSegment('a%2fb'), false);
  });

  it('rejects percent-encoded backslash (%5c)', () => {
    assert.equal(isSafeSegment('a%5cb'), false);
  });

  it('rejects double percent-encoded ".." (%252e%252e)', () => {
    assert.equal(isSafeSegment('%252e%252e'), false);
  });

  it('rejects percent-encoded null byte (%00)', () => {
    assert.equal(isSafeSegment('foo%00bar'), false);
  });

  it('accepts a literal percent that is not valid encoding (100%.md)', () => {
    // decodeURIComponent throws on "100%.md"; the raw form is still safe.
    assert.equal(isSafeSegment('100%.md'), true);
  });

  // --- NFKC Unicode-normalization traversal rejection ---

  it('rejects fullwidth ".." (U+FF0E U+FF0E) that NFKC-normalizes to ".."', () => {
    assert.equal(isSafeSegment('．．'), false);
  });

  it('rejects fullwidth solidus (U+FF0F) that NFKC-normalizes to "/"', () => {
    assert.equal(isSafeSegment('a／b'), false);
  });

  it('accepts an accented name whose NFKC form is still safe', () => {
    assert.equal(isSafeSegment('café'), true);
  });
});

// ---------------------------------------------------------------------------
// isSafeRelativePath
// ---------------------------------------------------------------------------

describe('isSafeRelativePath', () => {
  it('rejects empty string', () => {
    assert.equal(isSafeRelativePath(''), false);
  });

  it('rejects absolute path', () => {
    assert.equal(isSafeRelativePath('/etc/passwd'), false);
  });

  it('rejects path starting with ".."', () => {
    assert.equal(isSafeRelativePath('../../etc/passwd'), false);
  });

  it('rejects path with ".." segments in the middle', () => {
    assert.equal(isSafeRelativePath('a/../../b'), false);
  });

  it('rejects bare ".." path', () => {
    assert.equal(isSafeRelativePath('..'), false);
  });

  it('rejects path that normalizes to ".."', () => {
    assert.equal(isSafeRelativePath('a/../..'), false);
  });

  it('accepts simple filename', () => {
    assert.equal(isSafeRelativePath('SKILL.md'), true);
  });

  it('accepts single-depth nested path', () => {
    assert.equal(isSafeRelativePath('references/guide.md'), true);
  });

  it('accepts multi-depth nested path', () => {
    assert.equal(isSafeRelativePath('a/b/c.md'), true);
  });

  it('accepts path that has ".." sibling cancel (stays within root)', () => {
    // a/../b normalizes to just "b"
    assert.equal(isSafeRelativePath('a/../b'), true);
  });

  it('rejects path with null byte in a segment', () => {
    assert.equal(isSafeRelativePath('foo\0bar'), false);
  });

  it('rejects a segment with percent-encoded separator (a%2fb)', () => {
    assert.equal(isSafeRelativePath('a%2fb'), false);
  });

  it('rejects a percent-encoded traversal segment (%2e%2e/x)', () => {
    assert.equal(isSafeRelativePath('%2e%2e/x'), false);
  });

  it('rejects a fullwidth-dot traversal segment (．．/x)', () => {
    assert.equal(isSafeRelativePath('．．/x'), false);
  });
});

// ---------------------------------------------------------------------------
// validateCatalogIndex
// ---------------------------------------------------------------------------

describe('validateCatalogIndex', () => {
  it('rejects null', () => {
    const result = validateCatalogIndex(null);
    assert.equal(result.valid, false);
    assert.ok(result.errors.length > 0);
    assert.match(result.errors[0], /JSON object/);
  });

  it('rejects an array', () => {
    const result = validateCatalogIndex([]);
    assert.equal(result.valid, false);
    assert.match(result.errors[0], /JSON object/);
  });

  it('rejects a string', () => {
    const result = validateCatalogIndex('hello');
    assert.equal(result.valid, false);
    assert.match(result.errors[0], /JSON object/);
  });

  it('rejects a number', () => {
    const result = validateCatalogIndex(42);
    assert.equal(result.valid, false);
    assert.match(result.errors[0], /JSON object/);
  });

  it('rejects undefined', () => {
    const result = validateCatalogIndex(undefined);
    assert.equal(result.valid, false);
    assert.match(result.errors[0], /JSON object/);
  });

  it('rejects object without skills array', () => {
    const result = validateCatalogIndex({ name: 'catalog' });
    assert.equal(result.valid, false);
    assert.match(result.errors[0], /"skills" array/);
  });

  it('rejects object where skills is not an array', () => {
    const result = validateCatalogIndex({ skills: 'not-array' });
    assert.equal(result.valid, false);
    assert.match(result.errors[0], /"skills" array/);
  });

  it('accepts empty skills array (valid with zero skills)', () => {
    const result = validateCatalogIndex({ skills: [] });
    assert.equal(result.valid, true);
    assert.equal(result.errors.length, 0);
    assert.ok(result.index);
    assert.equal(result.index!.skills.length, 0);
  });

  it('rejects skills entries that are not objects', () => {
    const result = validateCatalogIndex({ skills: ['not-object', 123] });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /must be an object/.test(e)));
  });

  it('rejects null skills entry', () => {
    const result = validateCatalogIndex({ skills: [null] });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /must be an object/.test(e)));
  });

  it('rejects skills with missing name', () => {
    const result = validateCatalogIndex({
      skills: [{ files: ['SKILL.md'] }],
    });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /missing or empty "name"/.test(e)));
  });

  it('rejects skills with empty string name', () => {
    const result = validateCatalogIndex({
      skills: [{ name: '  ', files: ['SKILL.md'] }],
    });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /missing or empty "name"/.test(e)));
  });

  it('rejects skills with non-string name', () => {
    const result = validateCatalogIndex({
      skills: [{ name: 42, files: ['SKILL.md'] }],
    });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /missing or empty "name"/.test(e)));
  });

  it('rejects skills with unsafe name (directory traversal)', () => {
    const result = validateCatalogIndex({
      skills: [{ name: '../hack', files: ['SKILL.md'] }],
    });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /unsafe skill name/.test(e)));
  });

  it('rejects skills with unsafe name containing slash', () => {
    const result = validateCatalogIndex({
      skills: [{ name: 'a/b', files: ['SKILL.md'] }],
    });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /unsafe skill name/.test(e)));
  });

  it('rejects skills with empty files array', () => {
    const result = validateCatalogIndex({
      skills: [{ name: 'my-skill', files: [] }],
    });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /missing or empty "files" array/.test(e)));
  });

  it('rejects skills with missing files property', () => {
    const result = validateCatalogIndex({
      skills: [{ name: 'my-skill' }],
    });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /missing or empty "files" array/.test(e)));
  });

  it('rejects skills with non-array files', () => {
    const result = validateCatalogIndex({
      skills: [{ name: 'my-skill', files: 'single-file.md' }],
    });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /missing or empty "files" array/.test(e)));
  });

  it('rejects skills with unsafe file paths', () => {
    const result = validateCatalogIndex({
      skills: [{ name: 'my-skill', files: ['../../etc/passwd'] }],
    });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /unsafe file path/.test(e)));
  });

  it('reports error for non-string file entries', () => {
    const result = validateCatalogIndex({
      skills: [{ name: 'my-skill', files: [42] }],
    });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /file entry must be a string/.test(e)));
  });

  it('rejects skill when all file paths are unsafe (no valid files remain)', () => {
    const result = validateCatalogIndex({
      skills: [{ name: 'my-skill', files: ['/abs/path', '../traversal'] }],
    });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /no valid file paths/.test(e)));
  });

  it('accepts valid catalog with a single skill', () => {
    const result = validateCatalogIndex({
      skills: [{ name: 'my-skill', files: ['SKILL.md'] }],
    });
    assert.equal(result.valid, true);
    assert.ok(result.index);
    assert.equal(result.index!.skills.length, 1);
    assert.equal(result.index!.skills[0].name, 'my-skill');
    assert.deepEqual(result.index!.skills[0].files, ['SKILL.md']);
  });

  it('accepts valid catalog with multiple skills', () => {
    const result = validateCatalogIndex({
      skills: [
        { name: 'skill-a', files: ['SKILL.md', 'references/guide.md'] },
        { name: 'skill-b', files: ['main.md'] },
      ],
    });
    assert.equal(result.valid, true);
    assert.ok(result.index);
    assert.equal(result.index!.skills.length, 2);
    assert.equal(result.index!.skills[0].name, 'skill-a');
    assert.deepEqual(result.index!.skills[0].files, ['SKILL.md', 'references/guide.md']);
    assert.equal(result.index!.skills[1].name, 'skill-b');
  });

  it('preserves optional fields: version and description on skills', () => {
    const result = validateCatalogIndex({
      skills: [
        {
          name: 'my-skill',
          version: '1.2.3',
          description: 'A cool skill',
          files: ['SKILL.md'],
        },
      ],
    });
    assert.equal(result.valid, true);
    assert.ok(result.index);
    const skill = result.index!.skills[0];
    assert.equal(skill.version, '1.2.3');
    assert.equal(skill.description, 'A cool skill');
  });

  it('preserves optional catalog-level name and description', () => {
    const result = validateCatalogIndex({
      name: 'my-catalog',
      description: 'A collection of skills',
      skills: [{ name: 'x', files: ['x.md'] }],
    });
    assert.equal(result.valid, true);
    assert.ok(result.index);
    assert.equal(result.index!.name, 'my-catalog');
    assert.equal(result.index!.description, 'A collection of skills');
  });

  it('sets version/description to undefined when non-string', () => {
    const result = validateCatalogIndex({
      name: 42,
      description: true,
      skills: [
        { name: 'my-skill', version: 99, description: false, files: ['SKILL.md'] },
      ],
    });
    assert.equal(result.valid, true);
    assert.ok(result.index);
    assert.equal(result.index!.name, undefined);
    assert.equal(result.index!.description, undefined);
    assert.equal(result.index!.skills[0].version, undefined);
    assert.equal(result.index!.skills[0].description, undefined);
  });

  it('handles mixed valid/invalid entries — valid ones survive', () => {
    const result = validateCatalogIndex({
      skills: [
        { name: '../bad', files: ['SKILL.md'] },         // unsafe name
        { name: 'good', files: ['SKILL.md'] },            // valid
        { name: 'also-bad', files: [] },                  // empty files
        { name: 'also-good', files: ['reference.md'] },   // valid
      ],
    });
    assert.equal(result.valid, true);
    assert.ok(result.index);
    assert.equal(result.index!.skills.length, 2);
    assert.equal(result.index!.skills[0].name, 'good');
    assert.equal(result.index!.skills[1].name, 'also-good');
    assert.ok(result.errors.length > 0); // errors recorded for bad entries
  });

  it('returns valid: false when ALL entries are invalid', () => {
    const result = validateCatalogIndex({
      skills: [
        { name: '../bad', files: ['SKILL.md'] },
        { name: 'no-files' },
      ],
    });
    assert.equal(result.valid, false);
    assert.equal(result.errors.length, 2);
    assert.equal(result.index, undefined);
  });

  it('rejects a catalog whose skills array exceeds MAX_CATALOG_SKILLS', () => {
    const skills = Array.from({ length: MAX_CATALOG_SKILLS + 1 }, (_, i) => ({
      name: `s${i}`,
      files: ['SKILL.md'],
    }));
    const result = validateCatalogIndex({ skills });
    assert.equal(result.valid, false);
    assert.equal(result.index, undefined);
    assert.ok(result.errors.some(e => /maximum of .* skills/.test(e)));
  });

  it('accepts a catalog at exactly MAX_CATALOG_SKILLS', () => {
    const skills = Array.from({ length: MAX_CATALOG_SKILLS }, (_, i) => ({
      name: `s${i}`,
      files: ['SKILL.md'],
    }));
    const result = validateCatalogIndex({ skills });
    assert.equal(result.valid, true);
    assert.equal(result.index!.skills.length, MAX_CATALOG_SKILLS);
  });

  it('rejects a single skill whose files array exceeds MAX_FILES_PER_SKILL', () => {
    const files = Array.from({ length: MAX_FILES_PER_SKILL + 1 }, (_, i) => `f${i}.md`);
    const result = validateCatalogIndex({ skills: [{ name: 'big', files }] });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /maximum of .* files/.test(e)));
  });

  it('accepts a skill with exactly MAX_FILES_PER_SKILL files', () => {
    const files = Array.from({ length: MAX_FILES_PER_SKILL }, (_, i) => `f${i}.md`);
    const result = validateCatalogIndex({ skills: [{ name: 'big', files }] });
    assert.equal(result.valid, true);
    assert.equal(result.index!.skills[0].files.length, MAX_FILES_PER_SKILL);
  });

  it('rejects a catalog entry whose name is a percent-encoded traversal', () => {
    const result = validateCatalogIndex({
      skills: [{ name: '%2e%2e', files: ['SKILL.md'] }],
    });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(e => /unsafe skill name/.test(e)));
  });

  it('filters unsafe file paths but keeps safe ones in same skill', () => {
    const result = validateCatalogIndex({
      skills: [
        {
          name: 'mixed-files',
          files: ['good.md', '../../bad.md', 'also-good.md'],
        },
      ],
    });
    assert.equal(result.valid, true);
    assert.ok(result.index);
    assert.deepEqual(result.index!.skills[0].files, ['good.md', 'also-good.md']);
    assert.ok(result.errors.some(e => /unsafe file path/.test(e)));
  });

  it('retains an executable list of declared safe files', () => {
    const result = validateCatalogIndex({
      skills: [
        {
          name: 'has-script',
          files: ['SKILL.md', 'bin/run.sh', 'bin/setup.sh'],
          executable: ['bin/run.sh', 'bin/setup.sh'],
        },
      ],
    });
    assert.equal(result.valid, true);
    assert.ok(result.index);
    assert.deepEqual(result.index!.skills[0].executable, ['bin/run.sh', 'bin/setup.sh']);
  });

  it('leaves executable undefined when absent', () => {
    const result = validateCatalogIndex({
      skills: [{ name: 'plain', files: ['SKILL.md'] }],
    });
    assert.equal(result.valid, true);
    assert.equal(result.index!.skills[0].executable, undefined);
  });

  it('dedupes repeated executable entries', () => {
    const result = validateCatalogIndex({
      skills: [
        { name: 'dup', files: ['SKILL.md', 'run.sh'], executable: ['run.sh', 'run.sh'] },
      ],
    });
    assert.equal(result.valid, true);
    assert.deepEqual(result.index!.skills[0].executable, ['run.sh']);
  });

  it('drops an executable entry for an unknown/undeclared file and records an error', () => {
    const result = validateCatalogIndex({
      skills: [
        { name: 'bad-exec', files: ['SKILL.md'], executable: ['ghost.sh'] },
      ],
    });
    assert.equal(result.valid, true);
    assert.equal(result.index!.skills[0].executable, undefined);
    assert.ok(result.errors.some(e => /executable path "ghost\.sh" is not a declared file/.test(e)));
  });

  it('drops a non-string executable entry and records an error', () => {
    const result = validateCatalogIndex({
      skills: [
        { name: 'bad-exec-type', files: ['run.sh'], executable: [42] },
      ],
    });
    assert.equal(result.valid, true);
    assert.equal(result.index!.skills[0].executable, undefined);
    assert.ok(result.errors.some(e => /executable entry must be a string/.test(e)));
  });

  it('rejects a non-array executable field with an error', () => {
    const result = validateCatalogIndex({
      skills: [
        { name: 'bad-exec-shape', files: ['run.sh'], executable: 'run.sh' },
      ],
    });
    assert.equal(result.valid, true);
    assert.equal(result.index!.skills[0].executable, undefined);
    assert.ok(result.errors.some(e => /"executable" must be an array/.test(e)));
  });
});

// ---------------------------------------------------------------------------
// resolveSkillUrl
// ---------------------------------------------------------------------------

describe('resolveSkillUrl', () => {
  it('returns https URL unchanged', () => {
    assert.equal(
      resolveSkillUrl('https://example.com/catalog'),
      'https://example.com/catalog',
    );
  });

  it('strips trailing slashes from https URL', () => {
    assert.equal(
      resolveSkillUrl('https://example.com/catalog///'),
      'https://example.com/catalog',
    );
  });

  it('returns http URL unchanged', () => {
    assert.equal(
      resolveSkillUrl('http://localhost:8080/skills'),
      'http://localhost:8080/skills',
    );
  });

  it('strips trailing slashes from http URL', () => {
    assert.equal(
      resolveSkillUrl('http://localhost:8080/skills/'),
      'http://localhost:8080/skills',
    );
  });

  it('resolves owner/repo to raw.githubusercontent.com', () => {
    assert.equal(
      resolveSkillUrl('myorg/my-skills'),
      'https://raw.githubusercontent.com/myorg/my-skills/main',
    );
  });

  it('resolves owner/repo/path to raw.githubusercontent.com with subpath', () => {
    assert.equal(
      resolveSkillUrl('myorg/my-skills/catalog/v2'),
      'https://raw.githubusercontent.com/myorg/my-skills/main/catalog/v2',
    );
  });

  it('resolves owner/repo/single-file to raw.githubusercontent.com', () => {
    assert.equal(
      resolveSkillUrl('owner/repo/index.json'),
      'https://raw.githubusercontent.com/owner/repo/main/index.json',
    );
  });

  it('handles plain string (no slash, not a URL) as-is', () => {
    assert.equal(resolveSkillUrl('just-a-name'), 'just-a-name');
  });

  it('trims whitespace from input', () => {
    assert.equal(
      resolveSkillUrl('  https://example.com  '),
      'https://example.com',
    );
  });

  it('trims whitespace from plain string', () => {
    assert.equal(resolveSkillUrl('  plain  '), 'plain');
  });

  it('trims whitespace before GitHub shorthand resolution', () => {
    assert.equal(
      resolveSkillUrl('  org/repo  '),
      'https://raw.githubusercontent.com/org/repo/main',
    );
  });
});
