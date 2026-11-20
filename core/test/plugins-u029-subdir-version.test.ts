import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as crypto from 'crypto';

import {
  normalizePluginSubdirPath,
  hashPluginSubdirPath,
} from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 — git-subdir path hashing folded into a plugin's version/cache key
// (parity: the reference's calculatePluginVersion git-subdir branch, pluginVersioning.ts
// L63-88). Two plugins at different subdirs of the SAME monorepo commit share a
// SHA; the path hash disambiguates their versions/cache keys so they never
// collide at cache/<m>/<p>/<sha>/.
// ---------------------------------------------------------------------------

describe('normalizePluginSubdirPath — byte-for-byte parity normalization', () => {
  it('converts backslashes to forward slashes', () => {
    assert.equal(normalizePluginSubdirPath('plugins\\foo\\bar'), 'plugins/foo/bar');
  });

  it('strips exactly ONE leading "./"', () => {
    assert.equal(normalizePluginSubdirPath('./foo/bar'), 'foo/bar');
    // Only one is stripped — a second "./" is part of the path.
    assert.equal(normalizePluginSubdirPath('././foo'), './foo');
  });

  it('strips ALL trailing slashes', () => {
    assert.equal(normalizePluginSubdirPath('foo/bar///'), 'foo/bar');
  });

  it('applies backslash → leading-"./" → trailing-slash in order', () => {
    assert.equal(normalizePluginSubdirPath('.\\foo\\bar\\'), 'foo/bar');
  });

  it('leaves an already-normalized path unchanged', () => {
    assert.equal(normalizePluginSubdirPath('plugins/foo'), 'plugins/foo');
  });
});

describe('hashPluginSubdirPath — deterministic 8-char sha256 digest', () => {
  it('matches a known sha256 vector (first 8 hex of the normalized path)', () => {
    assert.equal(hashPluginSubdirPath('plugins/foo'), 'cbcac51a');
    assert.equal(hashPluginSubdirPath('foo/bar'), 'cc5d46bd');
  });

  it('uses the NORMALIZED path as the digest input (parity invariant)', () => {
    // A `\`-separated, `./`-prefixed, trailing-slashed spelling of the same path
    // must hash identically to its canonical form — the normalization is what
    // keeps the cache key stable across platforms/inputs.
    assert.equal(hashPluginSubdirPath('.\\plugins\\foo\\'), 'cbcac51a');
    assert.equal(hashPluginSubdirPath('plugins/foo'), hashPluginSubdirPath('./plugins/foo/'));
  });

  it('is exactly 8 lowercase hex chars', () => {
    const h = hashPluginSubdirPath('some/deep/plugin/path');
    assert.match(h, /^[0-9a-f]{8}$/);
  });

  it('reproduces crypto.createHash(sha256)(norm).digest(hex)[:8] exactly', () => {
    const p = 'packages/tools/linter';
    const expected = crypto.createHash('sha256').update(p).digest('hex').substring(0, 8);
    assert.equal(hashPluginSubdirPath(p), expected);
  });

  it('distinguishes two different subdirs (the collision the hash prevents)', () => {
    assert.notEqual(hashPluginSubdirPath('monorepo/plugin-a'), hashPluginSubdirPath('monorepo/plugin-b'));
  });
});
