import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { parseSemver, orderSemver, isSemverNewer } from '../src/version';

// UNS008 — `{ loose: true }` parsing option: prerelease / build-metadata aware
// version parsing + precedence ordering (parity with the reference's
// utils/semver.ts order()/{loose:true}). Distinct from skill-catalog's
// prerelease-AGNOSTIC compareSemver (release-floor) — this honors SemVer §11.
describe('parseSemver (loose, prerelease/build-metadata aware)', () => {
  it('parses a full major.minor.patch release', () => {
    assert.deepEqual(parseSemver('1.2.3'), {
      major: 1, minor: 2, patch: 3, prerelease: [], build: [],
    });
  });

  it('loosely strips a leading v / = and surrounding whitespace', () => {
    assert.deepEqual(parseSemver('  v1.2.3 '), {
      major: 1, minor: 2, patch: 3, prerelease: [], build: [],
    });
    assert.deepEqual(parseSemver('=2.0.0'), {
      major: 2, minor: 0, patch: 0, prerelease: [], build: [],
    });
  });

  it('defaults missing minor/patch to 0 (loose partial versions)', () => {
    assert.deepEqual(parseSemver('1'), {
      major: 1, minor: 0, patch: 0, prerelease: [], build: [],
    });
    assert.deepEqual(parseSemver('1.4'), {
      major: 1, minor: 4, patch: 0, prerelease: [], build: [],
    });
  });

  it('captures dot-separated prerelease identifiers (numeric → number)', () => {
    assert.deepEqual(parseSemver('1.0.0-alpha.1'), {
      major: 1, minor: 0, patch: 0, prerelease: ['alpha', 1], build: [],
    });
  });

  it('captures build metadata and keeps it separate from prerelease', () => {
    assert.deepEqual(parseSemver('1.0.0-rc.2+build.5'), {
      major: 1, minor: 0, patch: 0, prerelease: ['rc', 2], build: ['build', '5'],
    });
    assert.deepEqual(parseSemver('2.0.0+exp.sha.5114f85'), {
      major: 2, minor: 0, patch: 0, prerelease: [], build: ['exp', 'sha', '5114f85'],
    });
  });

  it('returns null for unparseable / non-numeric core / malformed input', () => {
    assert.equal(parseSemver('garbage'), null);
    assert.equal(parseSemver('1.x.0'), null);
    assert.equal(parseSemver(''), null);
    assert.equal(parseSemver('  '), null);
    assert.equal(parseSemver('1.2.3.4'), null); // too many core segments
    assert.equal(parseSemver('1.0.0-'), null);  // empty prerelease identifier
    assert.equal(parseSemver(undefined as unknown as string), null);
  });
});

describe('orderSemver (SemVer §11 precedence)', () => {
  it('orders by major.minor.patch', () => {
    assert.equal(orderSemver('1.0.0', '2.0.0'), -1);
    assert.equal(orderSemver('2.1.0', '2.0.9'), 1);
    assert.equal(orderSemver('1.2.3', '1.2.3'), 0);
  });

  it('a release outranks a prerelease of the same core version', () => {
    assert.equal(orderSemver('1.0.0', '1.0.0-rc.1'), 1);
    assert.equal(orderSemver('1.0.0-rc.1', '1.0.0'), -1);
  });

  it('orders prereleases per spec (numeric < alphanumeric, field count)', () => {
    assert.equal(orderSemver('1.0.0-alpha', '1.0.0-alpha.1'), -1);
    assert.equal(orderSemver('1.0.0-alpha.1', '1.0.0-alpha.beta'), -1);
    assert.equal(orderSemver('1.0.0-alpha.beta', '1.0.0-beta'), -1);
    assert.equal(orderSemver('1.0.0-beta', '1.0.0-beta.2'), -1);
    assert.equal(orderSemver('1.0.0-beta.2', '1.0.0-beta.11'), -1); // numeric, not lexical
    assert.equal(orderSemver('1.0.0-rc.1', '1.0.0-rc.1'), 0);
  });

  it('ignores build metadata for precedence', () => {
    assert.equal(orderSemver('1.0.0+build.1', '1.0.0+build.2'), 0);
    assert.equal(orderSemver('1.0.0-rc.1+a', '1.0.0-rc.1+b'), 0);
  });

  it('degrades unparseable operands to 0.0.0 rather than throwing', () => {
    assert.equal(orderSemver('garbage', '0.0.0'), 0);
    assert.equal(orderSemver('1.0.0', 'nope'), 1);
  });
});

describe('isSemverNewer', () => {
  it('is true only when the candidate strictly outranks the base', () => {
    assert.equal(isSemverNewer('2.0.0', '1.2.3'), true);
    assert.equal(isSemverNewer('1.2.3', '1.2.3'), false);
    assert.equal(isSemverNewer('1.2.2', '1.2.3'), false);
    // prerelease precedence: a final release is newer than its own prerelease
    assert.equal(isSemverNewer('1.0.0', '1.0.0-rc.1'), true);
    assert.equal(isSemverNewer('1.0.0-rc.1', '1.0.0'), false);
    assert.equal(isSemverNewer('1.0.0-rc.2', '1.0.0-rc.1'), true);
  });
});
