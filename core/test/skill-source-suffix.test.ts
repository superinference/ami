/**
 * Unit tests for `formatSkillSourceSuffix` — the shared source-annotation formatter
 * (Claude parity: `formatDescriptionWithSource`). Pure, no I/O.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { formatSkillSourceSuffix } from '../src/skill-catalog';

describe('formatSkillSourceSuffix', () => {
  it('names the originating plugin bundle when present (wins over tier)', () => {
    assert.equal(formatSkillSourceSuffix({ tier: 'installed', bundleName: 'ponytail' }), '(ponytail)');
    // bundle wins even for a project-tier skill contributed by a plugin.
    assert.equal(formatSkillSourceSuffix({ tier: 'project', bundleName: 'my-plugin' }), '(my-plugin)');
  });

  it('maps the core tier to the bundled annotation', () => {
    assert.equal(formatSkillSourceSuffix({ tier: 'core' }), '(bundled)');
  });

  it('annotates installed/user/project tiers with their own name', () => {
    assert.equal(formatSkillSourceSuffix({ tier: 'installed' }), '(installed)');
    assert.equal(formatSkillSourceSuffix({ tier: 'user' }), '(user)');
    assert.equal(formatSkillSourceSuffix({ tier: 'project' }), '(project)');
  });

  it('returns an empty string for an absent/unknown source (no suffix)', () => {
    assert.equal(formatSkillSourceSuffix(null), '');
    assert.equal(formatSkillSourceSuffix(undefined), '');
    assert.equal(formatSkillSourceSuffix({ tier: 'bogus' as never }), '');
  });
});
