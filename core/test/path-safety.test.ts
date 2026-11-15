import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  assertPathWithin,
  canonicalizeExisting,
  PathTraversalError,
} from '../src/path-safety';

function errno(code: string): NodeJS.ErrnoException {
  const e = new Error(code) as NodeJS.ErrnoException;
  e.code = code;
  return e;
}

const tmpRoots: string[] = [];
function mkTmp(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'psafe-'));
  tmpRoots.push(dir);
  return dir;
}

after(() => {
  for (const dir of tmpRoots) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// ==========================================================================
// Backward-compatible lexical containment (parity with the original guard).
// ==========================================================================

describe('assertPathWithin — lexical containment (preserved behavior)', () => {
  it('allows a path nested inside the base dir', () => {
    assert.doesNotThrow(() => assertPathWithin('/base/dir/file.json', '/base/dir'));
    assert.doesNotThrow(() => assertPathWithin('/base/dir/a/b/c.json', '/base/dir'));
  });

  it('allows the base dir itself', () => {
    assert.doesNotThrow(() => assertPathWithin('/base/dir', '/base/dir'));
  });

  it('resolves relative segments before comparing', () => {
    assert.doesNotThrow(() => assertPathWithin('/base/dir/sub/../file.json', '/base/dir'));
  });

  it('rejects a parent-traversal escape with the original message', () => {
    assert.throws(
      () => assertPathWithin('/base/dir/../evil.json', '/base/dir'),
      /Path traversal blocked/,
    );
  });

  it('rejects a sibling-prefix directory that is not actually contained', () => {
    assert.throws(
      () => assertPathWithin('/base/dir-sibling/file.json', '/base/dir'),
      /Path traversal blocked/,
    );
  });

  it('rejects an unrelated absolute path', () => {
    assert.throws(
      () => assertPathWithin('/etc/passwd', '/base/dir'),
      /Path traversal blocked/,
    );
  });

  it('out-of-base rejection is a PathTraversalError with reason "out-of-base"', () => {
    assert.throws(
      () => assertPathWithin('/etc/passwd', '/base/dir'),
      (err: unknown) =>
        err instanceof PathTraversalError &&
        err instanceof Error &&
        err.name === 'PathTraversalError' &&
        err.reason === 'out-of-base',
    );
  });
});

// ==========================================================================
// Null-byte rejection (ADD). RED: the original guard never inspects for '\0',
// so it throws no PathTraversalError('null-byte') — the reason assertion
// discriminates the fix from the old behavior.
// ==========================================================================

describe('assertPathWithin — null-byte rejection', () => {
  it('rejects a null byte in filePath', () => {
    assert.throws(
      () => assertPathWithin('/base/dir/evil\0.json', '/base/dir'),
      (err: unknown) =>
        err instanceof PathTraversalError && err.reason === 'null-byte',
    );
  });

  it('rejects a null byte in baseDir', () => {
    assert.throws(
      () => assertPathWithin('/base/dir/file.json', '/base/\0dir'),
      (err: unknown) =>
        err instanceof PathTraversalError && err.reason === 'null-byte',
    );
  });
});

// ==========================================================================
// Unicode NFC normalization (ADD). RED: the original byte-wise startsWith
// rejects an NFD-encoded path that names the same NFC directory as baseDir.
// ==========================================================================

describe('assertPathWithin — Unicode NFC normalization', () => {
  it('treats NFD and NFC encodings of the same contained name as contained', () => {
    const tmp = mkTmp();
    const nfc = 'café'; // é precomposed (single code point U+00E9)
    const nfd = 'café'; // e + combining acute accent (two code points)
    assert.notEqual(nfc, nfd); // guard: these MUST be distinct byte sequences
    const base = path.join(tmp, nfc);
    fs.mkdirSync(base);
    const fileNfd = path.join(tmp, nfd, 'x.json');
    assert.doesNotThrow(() => assertPathWithin(fileNfd, base));
  });
});

// ==========================================================================
// Symlink-aware containment (ADD). RED: the original lexical-only guard treats a
// symlink inside baseDir as contained even when it points outside.
// ==========================================================================

describe('assertPathWithin — symlink-aware containment', () => {
  it('rejects a symlink inside baseDir that escapes it', () => {
    const tmp = mkTmp();
    const base = path.join(tmp, 'base');
    const outside = path.join(tmp, 'outside');
    fs.mkdirSync(base);
    fs.mkdirSync(outside);
    fs.writeFileSync(path.join(outside, 'secret'), 'top secret');
    fs.symlinkSync(outside, path.join(base, 'link'));

    const escaping = path.join(base, 'link', 'secret');
    // Lexically contained, but the symlink escapes the base.
    assert.throws(
      () => assertPathWithin(escaping, base),
      (err: unknown) =>
        err instanceof PathTraversalError && err.reason === 'symlink-escape',
    );
  });

  it('allows a symlink that stays within baseDir', () => {
    const tmp = mkTmp();
    const base = path.join(tmp, 'base');
    fs.mkdirSync(base);
    const inside = path.join(base, 'real');
    fs.mkdirSync(inside);
    fs.symlinkSync(inside, path.join(base, 'alias'));
    assert.doesNotThrow(() =>
      assertPathWithin(path.join(base, 'alias', 'f.json'), base),
    );
  });

  it('allows a not-yet-existing write target nested in baseDir', () => {
    const tmp = mkTmp();
    const base = path.join(tmp, 'base');
    fs.mkdirSync(base);
    assert.doesNotThrow(() =>
      assertPathWithin(path.join(base, 'new', 'deep', 'file.json'), base),
    );
  });

  it('fails closed on a symlink cycle (ELOOP) inside baseDir', () => {
    const tmp = mkTmp();
    const base = path.join(tmp, 'base');
    fs.mkdirSync(base);
    fs.symlinkSync(path.join(base, 'loopB'), path.join(base, 'loopA'));
    fs.symlinkSync(path.join(base, 'loopA'), path.join(base, 'loopB'));
    assert.throws(
      () => assertPathWithin(path.join(base, 'loopA', 'x'), base),
      (err: unknown) =>
        err instanceof PathTraversalError && err.reason === 'symlink-escape',
    );
  });
});

// ==========================================================================
// canonicalizeExisting — exhaustive branch coverage via an injected realpath.
// ==========================================================================

describe('canonicalizeExisting', () => {
  it('returns the realpath directly when the path exists (no tail)', () => {
    assert.equal(canonicalizeExisting('/x', () => '/real/x'), '/real/x');
  });

  it('re-appends the non-existing tail after the deepest existing ancestor', () => {
    let calls = 0;
    const realpath = (_p: string): string => {
      calls++;
      if (calls === 1) throw errno('ENOTDIR'); // deepest segment is not a dir
      return '/real'; // ancestor exists
    };
    assert.equal(canonicalizeExisting('/a/b', realpath), path.join('/real', 'b'));
  });

  it('falls back to the lexical path when the whole chain is absent (root reached)', () => {
    // Always ENOENT: the walk climbs to the filesystem root and returns the input.
    assert.equal(
      canonicalizeExisting('/a/b', () => {
        throw errno('ENOENT');
      }),
      '/a/b',
    );
  });

  it('falls back to the lexical path on an unrelated error (e.g. EACCES)', () => {
    assert.equal(
      canonicalizeExisting('/a/b', () => {
        throw errno('EACCES');
      }),
      '/a/b',
    );
  });

  it('throws a symlink-escape PathTraversalError on ELOOP', () => {
    assert.throws(
      () =>
        canonicalizeExisting('/a/b', () => {
          throw errno('ELOOP');
        }),
      (err: unknown) =>
        err instanceof PathTraversalError && err.reason === 'symlink-escape',
    );
  });

  it('defaults to fs.realpathSync and resolves a real directory', () => {
    const tmp = mkTmp();
    const real = fs.realpathSync(tmp);
    assert.equal(canonicalizeExisting(tmp), real.normalize('NFC'));
  });
});
