import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import { expandPath, isPathSafe, toRelativePath } from '../src/utils/path';

// ===========================================================================
// expandPath hardening — Claude Code path-utility parity (all additive).
//
// Each block targets one confirmed gap vs. Claude Code's utils/path.ts and is
// RED-GREEN validated: with the corresponding guard reverted the assertion
// fails; with it in place it passes.
// ===========================================================================

// ---------------------------------------------------------------------------
// GAP: NUL-byte rejection.
// RED: remove the `includes('\0')` guard -> expandPath no longer throws and a
//      "/safe\0/../etc" style path slips past the boundary check.
// ---------------------------------------------------------------------------
describe('expandPath — NUL-byte rejection', () => {
  it('throws when the path contains a NUL byte', () => {
    assert.throws(
      () => expandPath('/safe\0/../../etc/passwd', '/home/user/project'),
      /null bytes/,
    );
  });

  it('throws when the cwd contains a NUL byte', () => {
    assert.throws(() => expandPath('file.txt', '/home/user\0/project'), /null bytes/);
  });

  it('isPathSafe refuses a NUL-truncated path instead of judging it lexically safe', () => {
    // Without the guard the "/project\0/../../etc" truncates at the kernel to
    // "/project" and would be judged a safe descendant.
    assert.throws(
      () => isPathSafe('/home/user/project\0/../../../etc/shadow', '/home/user/project'),
      /null bytes/,
    );
  });
});

// ---------------------------------------------------------------------------
// GAP: input type validation with a descriptive TypeError.
// RED: remove the typeof guards -> expandPath throws a different (or no)
//      error whose message does not name the offending argument.
// ---------------------------------------------------------------------------
describe('expandPath — input type validation', () => {
  it('throws a TypeError naming the path argument for a non-string path', () => {
    assert.throws(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      () => expandPath(123 as any, '/tmp'),
      (err: unknown) =>
        err instanceof TypeError && /Path must be a string/.test((err as Error).message),
    );
  });

  it('throws a TypeError naming the cwd argument for a non-string cwd', () => {
    assert.throws(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      () => expandPath('rel', 42 as any),
      (err: unknown) =>
        err instanceof TypeError && /cwd must be a string/.test((err as Error).message),
    );
  });
});

// ---------------------------------------------------------------------------
// GAP: cwd parameter is optional with a process.cwd() default.
// RED: restore the required `cwd` parameter -> the 1-arg call resolves against
//      `undefined` and throws instead of using process.cwd().
// ---------------------------------------------------------------------------
describe('expandPath — optional cwd default', () => {
  it('resolves a relative path against process.cwd() when cwd is omitted', () => {
    assert.equal(expandPath('rel/file.ts'), path.resolve(process.cwd(), 'rel/file.ts').normalize('NFC'));
  });

  it('still honors an explicit cwd', () => {
    assert.equal(expandPath('rel/file.ts', '/opt/app'), path.resolve('/opt/app', 'rel/file.ts'));
  });
});

// ---------------------------------------------------------------------------
// GAP: Unicode NFC normalization of the resolved path and the boundary.
// RED: drop `.normalize('NFC')` -> a decomposed path is returned decomposed and
//      no longer compares equal to its composed form at the boundary.
// ---------------------------------------------------------------------------
describe('expandPath — Unicode NFC normalization', () => {
  const decomposed = 'café'; // "café" with a combining acute accent
  const composed = 'café'; // precomposed "café"

  it('returns the NFC-composed form of a decomposed path', () => {
    const result = expandPath('/tmp/' + decomposed, '/x');
    assert.equal(result, path.normalize('/tmp/' + composed));
    assert.notEqual(result, path.normalize('/tmp/' + decomposed));
  });

  it('makes the containment boundary canonical across equivalent encodings', () => {
    const cwd = '/home/user/' + decomposed;
    const child = '/home/user/' + composed + '/src/app.ts';
    // Visually-identical dir, different byte encoding — must still be "within".
    assert.equal(isPathSafe(child, cwd), true);
    assert.equal(toRelativePath(child, cwd), path.join('src', 'app.ts'));
  });
});

// ---------------------------------------------------------------------------
// GAP: empty / whitespace-only path short-circuits to the normalized cwd.
// RED: remove the trim + empty short-circuit -> a whitespace path resolves to a
//      bogus subdirectory named with the whitespace.
// ---------------------------------------------------------------------------
describe('expandPath — empty / whitespace-only input', () => {
  it('maps an empty path to the normalized cwd', () => {
    assert.equal(expandPath('', '/home/user/project'), path.normalize('/home/user/project'));
  });

  it('maps a whitespace-only path to the normalized cwd (not a whitespace subdir)', () => {
    const result = expandPath('   ', '/home/user/project');
    assert.equal(result, path.normalize('/home/user/project'));
    assert.ok(!result.endsWith('   '));
  });

  it('trims surrounding whitespace before expanding a real path', () => {
    assert.equal(expandPath('  ~/x  ', '/tmp'), path.normalize(path.join(os.homedir(), 'x')));
  });
});
