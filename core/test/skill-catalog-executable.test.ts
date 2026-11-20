// Parity unit U104 — executable-file declarations on a catalog entry.
// validateCatalogIndex must retain only executable paths that correspond to a
// declared, safety-validated file; everything else is dropped with an error.
// Mirrors the reference's archive exec-bit preservation (officialMarketplaceGcs
// parseZipModes → chmod when mode & 0o111), declared per-file in the catalog
// since AMI fetches skill files individually over HTTP.

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { validateCatalogIndex } from '../src/skill-catalog';

describe('validateCatalogIndex executable', () => {
  it('retains executable paths that are declared files', () => {
    const r = validateCatalogIndex({
      skills: [
        {
          name: 'tool-skill',
          files: ['SKILL.md', 'hooks/run.sh', 'bin/do.sh'],
          executable: ['hooks/run.sh', 'bin/do.sh'],
        },
      ],
    });
    assert.equal(r.valid, true);
    assert.deepEqual(r.index!.skills[0].executable, ['hooks/run.sh', 'bin/do.sh']);
  });

  it('leaves executable undefined when absent', () => {
    const r = validateCatalogIndex({
      skills: [{ name: 'plain', files: ['SKILL.md'] }],
    });
    assert.equal(r.valid, true);
    assert.equal(r.index!.skills[0].executable, undefined);
  });

  it('drops an executable path that is not a declared file (with an error)', () => {
    const r = validateCatalogIndex({
      skills: [
        { name: 'x', files: ['SKILL.md'], executable: ['SKILL.md', 'not-declared.sh'] },
      ],
    });
    assert.equal(r.valid, true);
    assert.deepEqual(r.index!.skills[0].executable, ['SKILL.md']);
    assert.ok(r.errors.some(e => /executable path "not-declared\.sh" is not a declared file/.test(e)));
  });

  it('drops a non-string executable entry (with an error) and collapses duplicates', () => {
    const r = validateCatalogIndex({
      skills: [
        {
          name: 'x',
          files: ['a.sh'],
          executable: ['a.sh', 'a.sh', 42],
        },
      ],
    });
    assert.equal(r.valid, true);
    assert.deepEqual(r.index!.skills[0].executable, ['a.sh']);
    assert.ok(r.errors.some(e => /executable entry must be a string/.test(e)));
  });

  it('flags a non-array executable field and leaves it undefined', () => {
    const r = validateCatalogIndex({
      skills: [{ name: 'x', files: ['a.sh'], executable: 'a.sh' }],
    });
    assert.equal(r.valid, true);
    assert.equal(r.index!.skills[0].executable, undefined);
    assert.ok(r.errors.some(e => /"executable" must be an array/.test(e)));
  });

  it('leaves executable undefined when every entry is dropped', () => {
    const r = validateCatalogIndex({
      skills: [{ name: 'x', files: ['SKILL.md'], executable: ['ghost.sh'] }],
    });
    assert.equal(r.valid, true);
    assert.equal(r.index!.skills[0].executable, undefined);
  });
});
