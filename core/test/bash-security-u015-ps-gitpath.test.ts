/**
 * U015 — PowerShell/NTFS git-internal path-arg normalization (parity gaps 8-11).
 *
 * Claude Code's PowerShellTool/gitSafety.ts canonicalizes a raw arg before the
 * git-internal prefix match so a hook-injection target cannot dodge the guard via
 * PS/Windows idioms that resolve to the same file: colon-bound params
 * (`/Path:hooks`), the `FileSystem::` provider prefix, drive-relative `C:foo`,
 * backtick escapes, cwd-reentry (`../<cwd-basename>/HEAD`), and absolute cwd
 * re-entry (`C:\<cwd>\HEAD`). AMI only had trailing-dot/space + 8.3 short-name
 * normalization; this suite locks in the added idioms.
 *
 * Each `RED` case is a malicious equivalence form that MUST block; the matching
 * GREEN case is a benign lookalike that MUST stay allowed. Reverting the
 * normalization turns a RED case green-lit (unblocked), proving discrimination.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { detectGitInternalWrite, validateBashSecurity } from '../src/tools/bash-security';

describe('U015 PS git-path normalization: colon-bound param (gap 8/9)', () => {
  it('RED: blocks a colon-bound forward-slash param `/Path:.git/hooks/...` on win32', () => {
    assert.equal(detectGitInternalWrite('cp evil /Path:.git/hooks/pre-commit', 'win32').blocked, true);
  });
  it('RED: blocks an en-dash colon-bound param `\u2013Path:.git/config` on win32', () => {
    assert.equal(detectGitInternalWrite('cp evil \u2013Path:.git/config', 'win32').blocked, true);
  });
  it('GREEN: a colon-bound param to a benign file stays allowed', () => {
    assert.equal(detectGitInternalWrite('cp evil /Path:docs/readme.md', 'win32').blocked, false);
  });
  it('GREEN: an ordinary absolute POSIX path is NOT misread as a colon-bound param', () => {
    assert.equal(detectGitInternalWrite('cp evil /etc/hosts', 'win32').blocked, false);
  });
});

describe('U015 PS git-path normalization: FileSystem:: provider prefix (gap 8/9)', () => {
  it('RED: blocks `FileSystem::.git/config` on win32', () => {
    assert.equal(detectGitInternalWrite('cp evil FileSystem::.git/config', 'win32').blocked, true);
  });
  it('RED: blocks the fully-qualified `Microsoft.PowerShell.Core\\FileSystem::.git/hooks` form', () => {
    assert.equal(detectGitInternalWrite('cp evil Microsoft.PowerShell.Core\\FileSystem::.git/hooks/pre-commit', 'win32').blocked, true);
  });
  it('GREEN: a provider prefix to a benign file stays allowed', () => {
    assert.equal(detectGitInternalWrite('cp evil FileSystem::notes.txt', 'win32').blocked, false);
  });
});

describe('U015 PS git-path normalization: drive-relative + backtick (gap 8/9)', () => {
  it('RED: blocks drive-relative `C:.git/hooks/...` (no separator after colon) on win32', () => {
    assert.equal(detectGitInternalWrite('cp evil C:.git/hooks/pre-commit', 'win32').blocked, true);
  });
  it('RED: blocks a backtick-escaped `.g`+`it/hooks` form on win32', () => {
    assert.equal(detectGitInternalWrite('cp evil .g`it/hooks/pre-commit', 'win32').blocked, true);
  });
  it('GREEN: drive-relative to a benign file stays allowed', () => {
    assert.equal(detectGitInternalWrite('cp evil C:notes.txt', 'win32').blocked, false);
  });
  it('GREEN: none of the PS idioms are applied on linux (no false positive)', () => {
    assert.equal(detectGitInternalWrite('cp evil /Path:.git/hooks/pre-commit', 'linux').blocked, false);
    assert.equal(detectGitInternalWrite('cp evil FileSystem::.git/config', 'linux').blocked, false);
  });
});

describe('U015 cwd-reentry resolution (gap 10)', () => {
  const cwd = 'C:/project';
  it('RED: blocks `../project/HEAD && git ...` that re-enters cwd via the parent on win32', () => {
    assert.equal(detectGitInternalWrite('cp evil ../project/HEAD && git status', 'win32', cwd).blocked, true);
  });
  it('RED: reentry is case-insensitive (`../Project/HEAD`)', () => {
    assert.equal(detectGitInternalWrite('cp evil ../Project/objects/x && git fsck', 'win32', cwd).blocked, true);
  });
  it('GREEN: `../other/HEAD` (genuinely external dir) is NOT blocked by reentry', () => {
    assert.equal(detectGitInternalWrite('cp evil ../other/HEAD && git status', 'win32', cwd).blocked, false);
  });
  it('GREEN: reentry needs a cwd — without one, bare `../project/HEAD` is not resolved', () => {
    assert.equal(detectGitInternalWrite('cp evil ../project/HEAD && git status', 'win32').blocked, false);
  });
});

describe('U015 escaping-path-to-cwd-relative resolution (gap 11)', () => {
  const cwd = 'C:/project';
  it('RED: blocks an absolute `C:/project/HEAD && git ...` bare-repo HEAD plant on win32', () => {
    assert.equal(detectGitInternalWrite('cp evil C:/project/HEAD && git status', 'win32', cwd).blocked, true);
  });
  it('RED: blocks an absolute `C:/project/refs/heads/x` plant', () => {
    assert.equal(detectGitInternalWrite('cp evil C:/project/refs/heads/x && git status', 'win32', cwd).blocked, true);
  });
  it('GREEN: an absolute path OUTSIDE cwd (`C:/other/HEAD`) is not blocked by this guard', () => {
    assert.equal(detectGitInternalWrite('cp evil C:/other/HEAD && git status', 'win32', cwd).blocked, false);
  });
  it('GREEN: a bare `HEAD` write with NO git invocation stays allowed (bare-repo guard requires git)', () => {
    assert.equal(detectGitInternalWrite('echo doc > C:/project/HEAD', 'win32', cwd).blocked, false);
  });
});

describe('U015 wiring: validateBashSecurity threads cwd into the git-internal guard', () => {
  it('RED: validateBashSecurity blocks `../project/HEAD && git ...` on win32 with cwd', () => {
    assert.equal(
      validateBashSecurity('cp evil ../project/HEAD && git status', { platform: 'win32', cwd: 'C:/project' }).safe,
      false,
    );
  });
  it('GREEN: the same command with an external target stays safe', () => {
    assert.equal(
      validateBashSecurity('cp evil ../other/HEAD && git status', { platform: 'win32', cwd: 'C:/project' }).safe,
      true,
    );
  });
});
