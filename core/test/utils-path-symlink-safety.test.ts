import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';
import { isPathSafe } from '../src/utils/path';

// ===========================================================================
// GAP (high): symlink-chain resolution when enforcing a path boundary.
//
// isPathSafe previously decided the cwd/home boundary PURELY LEXICALLY (expandPath
// then a string prefix check). A symlink that lives inside cwd but points outside
// it (`<cwd>/link -> /outside`) was therefore judged "safe", defeating the
// traversal guard the function exists to provide. isPathSafe now canonicalizes
// the existing prefix (reusing path-safety.ts's canonicalizeExisting) before the
// boundary check, so the decision is made against the REAL resolved location.
//
// RED: remove the canonicalizeExisting block in isPathSafe (return the lexical
//      result) -> the escaping-symlink assertion returns `true` and fails.
// ===========================================================================
describe('isPathSafe — symlink-chain boundary resolution', () => {
  let tmp: string;
  let cwd: string;
  let outside: string;

  before(() => {
    // Force the fixture under /tmp (not os.tmpdir()/TMPDIR): isPathSafe also
    // allows anything under $HOME, so when TMPDIR lives inside the home tree
    // (e.g. coverage runs that redirect TMPDIR off a quota-exhausted /tmp) an
    // "escape" symlink whose target is still under $HOME would correctly stay
    // safe and the assertion would false-green. /tmp is outside $HOME on CI
    // and local Linux hosts, so the escape is a real boundary crossing.
    // realpathSync still resolves any aliasing (e.g. macOS /var -> /private/var).
    tmp = fs.realpathSync(fs.mkdtempSync(path.join('/tmp', 'ami-pathsafe-')));
    cwd = path.join(tmp, 'workspace');
    outside = path.join(tmp, 'outside');
    fs.mkdirSync(cwd);
    fs.mkdirSync(outside);
    fs.writeFileSync(path.join(outside, 'secret.txt'), 'secret');
    fs.writeFileSync(path.join(cwd, 'real.txt'), 'ok');
  });

  after(() => {
    fs.rmSync(tmp, { recursive: true, force: true });
  });

  it('rejects a file reached through a symlink that lives inside cwd but points outside', () => {
    const link = path.join(cwd, 'escape');
    fs.symlinkSync(outside, link);
    // Lexically `<cwd>/escape/secret.txt` looks contained, but it resolves to
    // `<tmp>/outside/secret.txt`, which is outside cwd and outside home.
    assert.equal(isPathSafe(path.join(link, 'secret.txt'), cwd), false);
  });

  it('rejects a direct dangling/escaping symlink file inside cwd', () => {
    const link = path.join(cwd, 'escape-file');
    fs.symlinkSync(path.join(outside, 'secret.txt'), link);
    assert.equal(isPathSafe(link, cwd), false);
  });

  it('still allows a genuine file inside cwd (no false positive)', () => {
    assert.equal(isPathSafe(path.join(cwd, 'real.txt'), cwd), true);
  });

  it('still allows a symlink inside cwd that points to another in-cwd target', () => {
    const innerTarget = path.join(cwd, 'inner');
    fs.mkdirSync(innerTarget, { recursive: true });
    fs.writeFileSync(path.join(innerTarget, 'x.txt'), 'x');
    const link = path.join(cwd, 'inner-link');
    fs.symlinkSync(innerTarget, link);
    assert.equal(isPathSafe(path.join(link, 'x.txt'), cwd), true);
  });

  it('rejects a path that is lexically outside cwd and home without touching fs', () => {
    // Lexical pre-check short-circuits before any canonicalization.
    assert.equal(isPathSafe('/etc/shadow', '/opt/myapp'), false);
  });
});
