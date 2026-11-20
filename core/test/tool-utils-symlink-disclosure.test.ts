import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { describeSymlinkTarget, _toolUtilsHooks } from '../src/tools/tool-utils';

// ---------------------------------------------------------------------------
// U069 — Symlink-target informed-consent disclosure (tool-utils.ts).
//
// Mirrors the reference's FilePermissionDialog symlink branch
// (FilePermissionDialog.tsx:L75-89,L162-167): for a mutating file op whose target
// is a symlink, compute the resolved real target and produce ONE of two consent
// lines — an explicit "outside working directory via a symlink" warning when the
// target escapes cwd, or a plain "Symlink target:" note for an in-cwd redirect.
// AMI previously disclosed NEITHER at the permission prompt.
// ---------------------------------------------------------------------------

describe('describeSymlinkTarget (symlink informed-consent disclosure)', () => {
  let root: string;
  let outside: string;

  beforeEach(() => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-symlink-cwd-'));
    root = fs.realpathSync(root);
    outside = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-symlink-out-'));
    outside = fs.realpathSync(outside);
  });

  afterEach(() => {
    fs.rmSync(root, { recursive: true, force: true });
    fs.rmSync(outside, { recursive: true, force: true });
  });

  it('returns null for an empty path', () => {
    assert.equal(describeSymlinkTarget('', root), null);
  });

  it('returns null for a non-existent path (new-file write is not a symlink)', () => {
    assert.equal(describeSymlinkTarget(path.join(root, 'does-not-exist.txt'), root), null);
  });

  it('returns null for a regular (non-symlink) file', () => {
    const file = path.join(root, 'plain.txt');
    fs.writeFileSync(file, 'hi');
    assert.equal(describeSymlinkTarget(file, root), null);
  });

  it('discloses an IN-cwd symlink redirect with the plain "Symlink target:" note', () => {
    const real = path.join(root, 'real.txt');
    fs.writeFileSync(real, 'data');
    const link = path.join(root, 'link.txt');
    fs.symlinkSync(real, link);

    const d = describeSymlinkTarget(link, root);
    assert.ok(d, 'expected a disclosure for an in-cwd symlink');
    assert.equal(d!.isOutsideCwd, false);
    assert.equal(d!.resolvedPath, real);
    assert.equal(d!.message, `Symlink target: ${real}`);
    assert.ok(!d!.message.includes('outside working directory'));
  });

  it('warns for a symlink whose target escapes cwd (outside-working-directory branch)', () => {
    const realOutside = path.join(outside, 'secret.txt');
    fs.writeFileSync(realOutside, 'escape');
    const link = path.join(root, 'escape-link.txt');
    fs.symlinkSync(realOutside, link);

    const d = describeSymlinkTarget(link, root);
    assert.ok(d, 'expected a disclosure for an out-of-cwd symlink');
    assert.equal(d!.isOutsideCwd, true);
    assert.equal(d!.resolvedPath, realOutside);
    assert.equal(
      d!.message,
      `This will modify ${realOutside} (outside working directory) via a symlink`,
    );
  });

  it('resolves a dangling symlink a single hop so the target is still disclosed', () => {
    const missingTarget = path.join(outside, 'gone.txt'); // never created
    const link = path.join(root, 'dangling.txt');
    fs.symlinkSync(missingTarget, link);

    const d = describeSymlinkTarget(link, root);
    assert.ok(d, 'expected a disclosure for a dangling symlink');
    assert.equal(d!.resolvedPath, missingTarget);
    assert.equal(d!.isOutsideCwd, true);
    assert.ok(d!.message.includes('outside working directory'));
  });

  it('returns null when BOTH realpath and readlink fail for the symlink', () => {
    const real = path.join(root, 'r.txt');
    fs.writeFileSync(real, 'x');
    const link = path.join(root, 'l.txt');
    fs.symlinkSync(real, link);
    const realOrig = _toolUtilsHooks.realpathSync;
    const readlinkOrig = _toolUtilsHooks.readlinkSync;
    _toolUtilsHooks.realpathSync = () => { throw new Error('EACCES'); };
    _toolUtilsHooks.readlinkSync = () => { throw new Error('EACCES'); };
    try {
      assert.equal(describeSymlinkTarget(link, root), null);
    } finally {
      _toolUtilsHooks.realpathSync = realOrig;
      _toolUtilsHooks.readlinkSync = readlinkOrig;
    }
  });

  it('resolves a relative dangling symlink target against the link directory', () => {
    const link = path.join(root, 'rel-dangling.txt');
    fs.symlinkSync('sibling-missing.txt', link); // relative, never created
    const d = describeSymlinkTarget(link, root);
    assert.ok(d);
    assert.equal(d!.resolvedPath, path.join(root, 'sibling-missing.txt'));
    assert.equal(d!.isOutsideCwd, false);
    assert.equal(d!.message, `Symlink target: ${path.join(root, 'sibling-missing.txt')}`);
  });
});
