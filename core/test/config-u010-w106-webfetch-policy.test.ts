/**
 * U010 / w106 — persistent `web_fetch` domain policy config knob (parity gap [18]).
 *
 * A new `webFetch?: { allowedDomains?: string[]; blockedDomains?: string[] }` field in
 * ProjectConfig lets an operator pin a durable allow/deny-list once (vs the per-REQUEST
 * allowed_domains/blocked_domains tool params). These tests cover the config.ts half:
 * the field is a recognized known key and its nested arrays are value-validated.
 *
 * RED before the config.ts change: `webFetch` is an UNKNOWN key (findUnknownConfigKeys
 * flags it) and its nested arrays are NOT validated (no issue for a bad shape).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateProjectConfigValues,
  findUnknownConfigKeys,
  KNOWN_CONFIG_KEYS,
} from '../src/config';
import type { ProjectConfig } from '../src/config';

describe('webFetch domain-policy config knob — recognition (gap [18])', () => {
  it('webFetch is a recognized known config key (not an unknown-key typo)', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('webFetch'), 'webFetch must be a known config key');
    const cfg: ProjectConfig = { webFetch: { allowedDomains: ['example.com'] } };
    assert.deepEqual(findUnknownConfigKeys(cfg), [], 'webFetch must not be flagged unknown');
  });

  it('accepts a valid webFetch object with allow + block string[] arrays', () => {
    const cfg: ProjectConfig = {
      webFetch: { allowedDomains: ['docs.rs', 'github.com/anthropics'], blockedDomains: ['evil.test'] },
    };
    assert.deepEqual(validateProjectConfigValues(cfg), []);
  });

  it('accepts an empty webFetch object and absent sub-fields', () => {
    assert.deepEqual(validateProjectConfigValues({ webFetch: {} }), []);
    assert.deepEqual(validateProjectConfigValues({ webFetch: { blockedDomains: [] } }), []);
  });
});

describe('webFetch domain-policy config knob — validation (gap [18])', () => {
  it('flags a non-object webFetch value with a type issue', () => {
    const issues = validateProjectConfigValues({ webFetch: 'nope' as unknown as ProjectConfig['webFetch'] });
    assert.equal(issues.length, 1);
    assert.equal(issues[0]!.field, 'webFetch');
    assert.match(issues[0]!.message, /must be a object/);
  });

  it('flags webFetch.allowedDomains when it is not a string[]', () => {
    const issues = validateProjectConfigValues({
      webFetch: { allowedDomains: [1, 2] as unknown as string[] },
    });
    const issue = issues.find(i => i.field === 'webFetch.allowedDomains');
    assert.ok(issue, 'expected a webFetch.allowedDomains issue');
    assert.match(issue!.message, /must be a string\[\]/);
  });

  it('flags webFetch.blockedDomains when it is a non-array', () => {
    const issues = validateProjectConfigValues({
      webFetch: { blockedDomains: 'evil.test' as unknown as string[] },
    });
    const issue = issues.find(i => i.field === 'webFetch.blockedDomains');
    assert.ok(issue, 'expected a webFetch.blockedDomains issue');
    assert.match(issue!.message, /must be a string\[\]/);
  });

  it('does not re-validate nested arrays when the outer value is the wrong type', () => {
    const issues = validateProjectConfigValues({ webFetch: 42 as unknown as ProjectConfig['webFetch'] });
    // Exactly the outer type issue — no spurious nested webFetch.* issues.
    assert.deepEqual(issues.map(i => i.field), ['webFetch']);
  });
});
