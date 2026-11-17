import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  detectTildeExpansionVariant,
  validateBashSecurity,
  extractCommandPaths,
  CHECK_IDS,
} from '../src/tools/bash-security.ts';

// ---------------------------------------------------------------------------
// U015 (wave 79) parity hardening for core/core/src/tools/bash-security.ts
//
// Gaps covered:
//  1. Tilde-expansion-variant TOCTOU guard at the BASH-tool layer — a path
//     operand that begins with ~user/~+/~-/~N expands (at exec time) to a
//     DIFFERENT directory than static validation sees, so it can never be
//     silently auto-run (utils/permissions/pathValidation.ts L394-L411). The
//     existing permissions.ts `detectPathExpansionHazard` only gates the
//     file_read/file_edit TOOLS; free-form bash commands were ungated.
//  2. sed per-command path extractor (script-vs-file aware, multi -e/-f, glob
//     operands) replacing the naive `!/^s[/|]/` filter (BashTool/pathValidation
//     PATH_EXTRACTORS.sed L379-L427; hasFileArgs L303-L379).
//  3. `tr` SET-skipping extractor (SET1/SET2 are character sets, not paths)
//     (PATH_EXTRACTORS.tr).
//  4. grep/rg recursive-default path target (`.`) so a recursive read's
//     implicit cwd target is still surfaced (PATH_EXTRACTORS.grep/.rg).
// ---------------------------------------------------------------------------

describe('detectTildeExpansionVariant — TOCTOU tilde variants', () => {
  const blocked = [
    'cat ~root/.ssh/id_rsa',
    'cat ~user/file.txt',
    'ls ~+/subdir',
    'ls ~-/subdir',
    'cat ~2/notes.txt',
    'rm -rf ~app/data',
    'FOO=~bar/x true',           // assignment RHS is tilde-expanded by bash
    'echo hi; cat ~root/secret', // later compound segment
  ];
  for (const cmd of blocked) {
    it(`blocks ${cmd}`, () => {
      assert.equal(detectTildeExpansionVariant(cmd).blocked, true);
    });
  }

  const allowed = [
    'cat ~/file.txt',            // bare ~/ resolves to $HOME (expanded)
    'echo ~',                    // bare ~ alone
    'git log HEAD~3',            // git rev, not a path at a word boundary
    'git rebase main~2',         // same
    'echo $((~5))',              // arithmetic bitwise-NOT, no trailing slash
    "cat '~root/secret'",        // single-quoted → literal to bash
    'curl http://example.com/~user/page', // ~user inside a URL path (slash before ~)
    'ls -la',                    // no tilde at all
  ];
  for (const cmd of allowed) {
    it(`allows ${cmd}`, () => {
      assert.equal(detectTildeExpansionVariant(cmd).blocked, false);
    });
  }

  it('returns a TOCTOU-worded reason', () => {
    const r = detectTildeExpansionVariant('cat ~root/.ssh/id_rsa');
    assert.match(r.reason ?? '', /TOCTOU/);
    assert.match(r.reason ?? '', /~user/);
  });
});

describe('validateBashSecurity — tilde-variant wiring (end-to-end entry point)', () => {
  it('fails the real bash validator with TILDE_EXPANSION_VARIANT', () => {
    const r = validateBashSecurity('cat ~root/.ssh/id_rsa');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.TILDE_EXPANSION_VARIANT);
  });

  it('does not block a legitimate ~/ home path', () => {
    // (~/projects/app expands to $HOME/... — the command may still fail other
    // checks, but NOT for the tilde-variant reason)
    const r = validateBashSecurity('cat ~/projects/app/README.md');
    assert.notEqual(r.checkId, CHECK_IDS.TILDE_EXPANSION_VARIANT);
  });

  it('blocks a tilde variant even in a later compound segment', () => {
    const r = validateBashSecurity('ls && cat ~foo/secret.txt');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.TILDE_EXPANSION_VARIANT);
  });
});

describe('extractCommandPaths — sed script-vs-file extractor', () => {
  it('skips the inline script, keeps the file operand', () => {
    const paths = extractCommandPaths("sed -n '/foo/p' file.txt");
    assert.ok(paths.includes('file.txt'));
    assert.ok(!paths.some(p => p.includes('foo')));
  });

  it('keeps file operands when the script is given via -e', () => {
    const paths = extractCommandPaths("sed -e 's/a/b/' f1.txt f2.txt");
    assert.ok(paths.includes('f1.txt'));
    assert.ok(paths.includes('f2.txt'));
  });

  it('surfaces the -f script FILE as a path to validate plus the data file', () => {
    const paths = extractCommandPaths('sed -f /etc/evil.sed data.txt');
    assert.ok(paths.includes('/etc/evil.sed'));
    assert.ok(paths.includes('data.txt'));
  });

  it('treats glob operands after the script as files', () => {
    const paths = extractCommandPaths("sed -i 's/a/b/' *.log");
    assert.ok(paths.includes('*.log'));
  });

  it('still extracts the file for the classic in-place form', () => {
    const paths = extractCommandPaths("sed -i 's/old/new/' file.txt");
    assert.ok(paths.includes('file.txt'));
  });
});

describe('extractCommandPaths — tr SET skipping', () => {
  it('does not mistake SET1/SET2 for paths', () => {
    assert.deepEqual(extractCommandPaths('tr a-z A-Z'), []);
  });

  it('skips a single SET for -d (delete)', () => {
    assert.deepEqual(extractCommandPaths('tr -d abc'), []);
  });

  it('does not treat a slash SET as a filesystem path', () => {
    assert.deepEqual(extractCommandPaths("tr / _"), []);
  });
});

describe('extractCommandPaths — grep/rg recursive default', () => {
  it('defaults a recursive grep with no path to the current dir', () => {
    assert.deepEqual(extractCommandPaths('grep -r needle'), ['.']);
  });

  it('keeps explicit grep paths intact', () => {
    assert.deepEqual(extractCommandPaths('grep needle file.txt'), ['file.txt']);
  });

  it('does not inject a default for non-recursive grep (stdin)', () => {
    assert.deepEqual(extractCommandPaths('grep needle'), []);
  });

  it('defaults a bare rg (recurses cwd by default) to the current dir', () => {
    assert.deepEqual(extractCommandPaths('rg needle'), ['.']);
  });

  it('keeps explicit rg paths intact', () => {
    assert.deepEqual(extractCommandPaths('rg needle src/'), ['src/']);
  });
});
