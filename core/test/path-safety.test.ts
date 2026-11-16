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

  it('fails closed on an unverifiable error (e.g. EACCES) instead of trusting the lexical path', () => {
    // Parity with Claude's realpathDeepestExisting / isRealPathWithinTeamDir, which
    // fail closed on EACCES/EIO: a symlink we could not read may escape the base.
    assert.throws(
      () =>
        canonicalizeExisting('/a/b', () => {
          throw errno('EACCES');
        }),
      (err: unknown) =>
        err instanceof PathTraversalError && err.reason === 'symlink-escape',
    );
  });

  it('includes the errno code in the fail-closed message', () => {
    assert.throws(
      () =>
        canonicalizeExisting('/a/b', () => {
          throw errno('EIO');
        }),
      /cannot verify .*\(EIO\)/,
    );
  });

  it('reports "unknown" when the realpath error carries no errno code', () => {
    assert.throws(
      () =>
        canonicalizeExisting('/a/b', () => {
          throw new Error('mystery');
        }),
      (err: unknown) =>
        err instanceof PathTraversalError &&
        err.reason === 'symlink-escape' &&
        /\(unknown\)/.test(err.message),
    );
  });

  it('walks past an ENAMETOOLONG component to realpath the existing ancestor', () => {
    let calls = 0;
    const realpath = (_p: string): string => {
      calls++;
      if (calls === 1) throw errno('ENAMETOOLONG');
      return '/real';
    };
    assert.equal(canonicalizeExisting('/a/b', realpath), path.join('/real', 'b'));
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

// ==========================================================================
// UNC-path bypass (ADD). realpath'ing a UNC path triggers a network (DNS/SMB)
// lookup; the guard must return the lexical form without touching the fs.
// ==========================================================================

function fakeStat(over: Partial<Record<keyof fs.Stats, boolean>>): fs.Stats {
  const flags = (name: string): boolean => Boolean(over[name as keyof fs.Stats]);
  return {
    isFIFO: () => flags('isFIFO'),
    isSocket: () => flags('isSocket'),
    isCharacterDevice: () => flags('isCharacterDevice'),
    isBlockDevice: () => flags('isBlockDevice'),
    isSymbolicLink: () => flags('isSymbolicLink'),
    isDirectory: () => false,
    isFile: () => false,
  } as unknown as fs.Stats;
}

describe('canonicalizeExisting — UNC-path bypass', () => {
  it('returns a POSIX-style //host/share path without calling realpath', () => {
    let called = false;
    const out = canonicalizeExisting(
      '//server/share/file',
      () => {
        called = true;
        return '/should/not/be/used';
      },
    );
    assert.equal(called, false);
    assert.equal(out, '//server/share/file');
  });

  it('returns a Windows-style \\\\host\\share path without calling realpath', () => {
    let called = false;
    const out = canonicalizeExisting(
      '\\\\server\\share',
      () => {
        called = true;
        return '/nope';
      },
    );
    assert.equal(called, false);
    assert.equal(out, '\\\\server\\share'.normalize('NFC'));
  });
});

// ==========================================================================
// Special-file bypass (ADD). realpathSync can block on a FIFO waiting for a
// writer; the guard must skip canonicalization for FIFO/socket/device nodes.
// ==========================================================================

describe('canonicalizeExisting — special-file bypass', () => {
  for (const kind of ['isFIFO', 'isSocket', 'isCharacterDevice', 'isBlockDevice'] as const) {
    it(`skips realpath for a ${kind} node`, () => {
      let called = false;
      const out = canonicalizeExisting(
        '/dev/special',
        () => {
          called = true;
          return '/resolved';
        },
        () => fakeStat({ [kind]: true }),
      );
      assert.equal(called, false);
      assert.equal(out, '/dev/special');
    });
  }

  it('still canonicalizes a regular file (lstat reports no special type)', () => {
    const out = canonicalizeExisting(
      '/a/file',
      () => '/real/file',
      () => fakeStat({}),
    );
    assert.equal(out, '/real/file');
  });
});

// ==========================================================================
// Dangling-symlink detection (ADD). A final component that lstat's as a symlink
// but realpath's as ENOENT (missing target) is an escape vector — writeFile would
// follow the link and create the target outside the base.
// ==========================================================================

describe('canonicalizeExisting — dangling symlink', () => {
  it('fails closed when the component is a dangling symlink', () => {
    const realpath = (_p: string): string => {
      throw errno('ENOENT');
    };
    const lstat = (_p: string): fs.Stats => fakeStat({ isSymbolicLink: true });
    assert.throws(
      () => canonicalizeExisting('/base/link', realpath, lstat),
      (err: unknown) =>
        err instanceof PathTraversalError && err.reason === 'symlink-escape',
    );
  });

  it('detects a real dangling symlink inside baseDir end-to-end', () => {
    const tmp = mkTmp();
    const base = path.join(tmp, 'base');
    fs.mkdirSync(base);
    // Link points at a target that does not exist — a dangling symlink.
    fs.symlinkSync(path.join(tmp, 'nonexistent-target'), path.join(base, 'dangling'));
    assert.throws(
      () => assertPathWithin(path.join(base, 'dangling'), base),
      (err: unknown) =>
        err instanceof PathTraversalError && err.reason === 'symlink-escape',
    );
  });

  it('walks up normally when realpath ENOENT is a truly-missing component (not a symlink)', () => {
    const realpath = (_p: string): string => {
      throw errno('ENOENT');
    };
    const lstat = (_p: string): fs.Stats => {
      throw errno('ENOENT');
    };
    // No existing ancestor + no symlink → lexical fallback.
    assert.equal(canonicalizeExisting('/a/b', realpath, lstat), '/a/b');
  });
});

// ==========================================================================
// Case-insensitive + /private-alias containment comparison (ADD). RED: a
// byte-exact startsWith lets a case-only mismatch or a macOS /private alias slip
// through; normalizeForComparison closes both.
// ==========================================================================

describe('assertPathWithin — case-insensitive & /private-alias containment', () => {
  it('treats a case-only variant of the base as contained', () => {
    assert.doesNotThrow(() =>
      assertPathWithin('/Base/Dir/File.json', '/base/dir'),
    );
  });

  it('still rejects a case-folded sibling-prefix escape', () => {
    assert.throws(
      () => assertPathWithin('/Base/Dir-Evil/x.json', '/base/dir'),
      (err: unknown) =>
        err instanceof PathTraversalError && err.reason === 'out-of-base',
    );
  });

  it('treats a macOS /private/var alias of the base as contained', () => {
    assert.doesNotThrow(() =>
      assertPathWithin('/private/var/app/sess/f.json', '/var/app/sess'),
    );
  });

  it('treats a macOS /private/tmp alias of the base as contained', () => {
    assert.doesNotThrow(() =>
      assertPathWithin('/private/tmp/app/f.json', '/tmp/app'),
    );
  });
});
