import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  patternWithRoot,
  matchRootedFileRulePattern,
  PermissionManager,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 / w113 — gitignore-style per-root resolution for FILE permission rules.
//
// Parity: Claude Code resolves each Edit/Read file rule against a root derived
// from the pattern's leading sigil before matching
// (references/claude/claude-code-main/src/utils/permissions/filesystem.ts
// patternWithRoot L852-917 / matchingRuleForInput L955). AMI previously matched
// file rules as a RAW glob against the literal path string, so a root-relative
// rule (`//abs/**`), a home-relative rule (`~/x`) or a project-relative rule
// (`/src/**`) silently never matched the absolute path they name — AMI even
// GENERATES `//abs/**` suggestions (createReadRuleSuggestion) that could not match.
//
// [FUNCTIONAL CHANGE / ADDITIVE] patternWithRoot + matchRootedFileRulePattern add
// the per-root resolution, wired as a UNION into check()'s allow/ask file-rule
// loop (only when the raw matcher failed), so bare-pattern behavior is unchanged
// and rooted rules now match. RED/GREEN: reverting the
// `|| this.matchFilePathRuleWithRoot(...)` union in permissions.ts makes the
// "rooted allow rule flips ask -> allow" integration test fail.
// ---------------------------------------------------------------------------

const HOME = '/home/tester';

describe('patternWithRoot — per-root resolution of a file rule pattern', () => {
  it('`//x` resolves against the filesystem root, dropping one slash', () => {
    assert.deepEqual(patternWithRoot('//etc/passwd', '/proj', HOME), {
      relativePattern: '/etc/passwd',
      root: '/',
    });
  });

  it('`~/x` resolves against the NFC-normalized home directory', () => {
    assert.deepEqual(patternWithRoot('~/.ssh/**', '/proj', HOME), {
      relativePattern: '/.ssh/**',
      root: HOME,
    });
  });

  it('`/x` resolves against the settings source root', () => {
    assert.deepEqual(patternWithRoot('/src/**', '/proj', HOME), {
      relativePattern: '/src/**',
      root: '/proj',
    });
  });

  it('a bare pattern is root-agnostic (root === null)', () => {
    assert.deepEqual(patternWithRoot('src/**', '/proj', HOME), {
      relativePattern: 'src/**',
      root: null,
    });
  });

  it('a leading `./` is stripped on a root-agnostic pattern', () => {
    assert.deepEqual(patternWithRoot('./.env', '/proj', HOME), {
      relativePattern: '.env',
      root: null,
    });
  });
});

describe('matchRootedFileRulePattern — rooted pattern vs absolute path', () => {
  it('`//abs/**` matches a file inside that absolute dir', () => {
    assert.equal(matchRootedFileRulePattern('//etc/**', '/etc/passwd'), true);
  });

  it('`//abs/**` matches the directory itself (trailing /** = dir AND contents)', () => {
    assert.equal(matchRootedFileRulePattern('//etc/**', '/etc'), true);
  });

  it('`//abs/**` does not match an unrelated absolute path', () => {
    assert.equal(matchRootedFileRulePattern('//etc/**', '/var/log/x'), false);
  });

  it('`/src/**` resolves against the provided sourceRoot', () => {
    assert.equal(
      matchRootedFileRulePattern('/src/**', '/proj/src/a/b.ts', { sourceRoot: '/proj' }),
      true,
    );
    assert.equal(
      matchRootedFileRulePattern('/src/**', '/other/src/a.ts', { sourceRoot: '/proj' }),
      false,
    );
  });

  it('`~/x` resolves against the provided home directory', () => {
    assert.equal(
      matchRootedFileRulePattern('~/.ssh/**', '/home/tester/.ssh/id_rsa', { home: HOME }),
      true,
    );
  });

  it('returns null for a root-agnostic pattern so the caller falls back', () => {
    assert.equal(matchRootedFileRulePattern('src/**', '/proj/src/x.ts'), null);
    assert.equal(matchRootedFileRulePattern('./.env', '/proj/.env'), null);
  });

  it('honors case-insensitive matching when requested', () => {
    assert.equal(
      matchRootedFileRulePattern('//etc/**', '/ETC/passwd', { caseInsensitive: true }),
      true,
    );
    assert.equal(
      matchRootedFileRulePattern('//etc/**', '/ETC/passwd', { caseInsensitive: false }),
      false,
    );
  });
});

describe('PermissionManager.check — rooted file-rule allow (integration)', () => {
  const dirs: string[] = [];
  after(() => {
    for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
  });
  function mkTemp(): string {
    const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'w113-rooted-')));
    dirs.push(dir);
    return dir;
  }

  it('a `//<abs>/**` allow rule flips a file_edit ask -> allow', async () => {
    const dir = mkTemp();
    const file = path.join(dir, 'note.txt');
    const pm = new PermissionManager('ask');
    pm.registerAllowedRoot(dir);

    // No rule yet: an edit requires confirmation.
    assert.equal(await pm.check('file_edit', { file_path: file }), 'ask');

    // Add a ROOT-relative allow rule naming the dir. The raw glob matcher cannot
    // resolve `//<abs>/**` against the absolute path — only the per-root resolution
    // makes it match.
    pm.addRule({ tool: 'file_edit', pattern: `/${dir}/**`, action: 'allow' });
    assert.equal(await pm.check('file_edit', { file_path: file }), 'allow');
  });

  it('a non-matching rooted allow rule leaves the edit at ask', async () => {
    const dir = mkTemp();
    const file = path.join(dir, 'note.txt');
    const pm = new PermissionManager('ask');
    pm.registerAllowedRoot(dir);
    pm.addRule({ tool: 'file_edit', pattern: '//nonexistent-root-xyz/**', action: 'allow' });
    assert.equal(await pm.check('file_edit', { file_path: file }), 'ask');
  });

  it('a `//<abs>/**` deny-less bare pattern still behaves as before (no regression)', async () => {
    const dir = mkTemp();
    const file = path.join(dir, 'note.txt');
    const pm = new PermissionManager('ask');
    pm.registerAllowedRoot(dir);
    // A root-agnostic pattern that does not match the literal path string: unchanged
    // (stays ask) — the per-root union only applies to rooted forms.
    pm.addRule({ tool: 'file_edit', pattern: 'totally/unrelated/**', action: 'allow' });
    assert.equal(await pm.check('file_edit', { file_path: file }), 'ask');
  });
});
