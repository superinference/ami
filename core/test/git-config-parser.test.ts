import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { parseConfigString, parseGitConfigValue } from '../src/git-config-parser';
import { getRemoteOriginUrl } from '../src/utils/git';

// ---------------------------------------------------------------------------
// parseConfigString — section / subsection matching
// ---------------------------------------------------------------------------

test('parseConfigString: reads a value under a quoted subsection', () => {
  const cfg = '[remote "origin"]\n\turl = https://github.com/acme/widget.git\n';
  assert.equal(parseConfigString(cfg, 'remote', 'origin', 'url'), 'https://github.com/acme/widget.git');
});

test('parseConfigString: section + key matching is case-insensitive', () => {
  const cfg = '[ReMoTe "origin"]\n\tUrL = https://example.com/r.git\n';
  assert.equal(parseConfigString(cfg, 'remote', 'origin', 'url'), 'https://example.com/r.git');
  assert.equal(parseConfigString(cfg, 'REMOTE', 'origin', 'URL'), 'https://example.com/r.git');
});

test('parseConfigString: subsection matching is CASE-SENSITIVE', () => {
  const cfg = '[remote "Origin"]\n\turl = https://example.com/r.git\n';
  assert.equal(parseConfigString(cfg, 'remote', 'origin', 'url'), null);
  assert.equal(parseConfigString(cfg, 'remote', 'Origin', 'url'), 'https://example.com/r.git');
});

test('parseConfigString: simple (no-subsection) section matches only when subsection is null', () => {
  const cfg = '[core]\n\teditor = vim\n';
  assert.equal(parseConfigString(cfg, 'core', null, 'editor'), 'vim');
  // A null-subsection lookup must NOT match a subsectioned header.
  const sub = '[core "sub"]\n\teditor = emacs\n';
  assert.equal(parseConfigString(sub, 'core', null, 'editor'), null);
});

test('parseConfigString: subsection lookup does not match a simple section header', () => {
  const cfg = '[remote]\n\turl = x\n';
  assert.equal(parseConfigString(cfg, 'remote', 'origin', 'url'), null);
});

test('parseConfigString: returns null for an absent key / section', () => {
  const cfg = '[remote "origin"]\n\turl = x\n';
  assert.equal(parseConfigString(cfg, 'remote', 'origin', 'pushurl'), null);
  assert.equal(parseConfigString(cfg, 'branch', 'main', 'remote'), null);
});

test('parseConfigString: first matching key wins', () => {
  const cfg = '[remote "origin"]\n\turl = first\n\turl = second\n';
  assert.equal(parseConfigString(cfg, 'remote', 'origin', 'url'), 'first');
});

// ---------------------------------------------------------------------------
// parseConfigString — empty / comment lines, boolean keys
// ---------------------------------------------------------------------------

test('parseConfigString: skips blank and comment-only lines (# and ;)', () => {
  const cfg = '\n# a comment\n   ; another\n[core]\n\t# inner comment\n\tbare = yes\n';
  assert.equal(parseConfigString(cfg, 'core', null, 'bare'), 'yes');
});

test('parseConfigString: a boolean / valueless key (no =) yields null', () => {
  const cfg = '[core]\n\tbare\n\tfilemode = true\n';
  assert.equal(parseConfigString(cfg, 'core', null, 'bare'), null);
  // The valueless key does not swallow the following real key.
  assert.equal(parseConfigString(cfg, 'core', null, 'filemode'), 'true');
});

test('parseConfigString: a key that is only an = with no name yields null (no key char)', () => {
  const cfg = '[core]\n\t= value\n\teditor = vim\n';
  assert.equal(parseConfigString(cfg, 'core', null, 'editor'), 'vim');
});

// ---------------------------------------------------------------------------
// parseConfigString — value parsing: quotes, escapes, comments, whitespace
// ---------------------------------------------------------------------------

test('parseConfigString: inline comments (# and ;) outside quotes terminate the value', () => {
  assert.equal(parseConfigString('[u]\n\tk = val # trailing', 'u', null, 'k'), 'val');
  assert.equal(parseConfigString('[u]\n\tk = val ; trailing', 'u', null, 'k'), 'val');
});

test('parseConfigString: a # or ; INSIDE quotes is literal, not a comment', () => {
  assert.equal(parseConfigString('[u]\n\tk = "a#b;c"', 'u', null, 'k'), 'a#b;c');
});

test('parseConfigString: recognized in-quote escapes decode (\\n \\t \\b \\" \\\\)', () => {
  assert.equal(parseConfigString('[u]\n\tk = "a\\nb"', 'u', null, 'k'), 'a\nb');
  assert.equal(parseConfigString('[u]\n\tk = "a\\tb"', 'u', null, 'k'), 'a\tb');
  assert.equal(parseConfigString('[u]\n\tk = "a\\bb"', 'u', null, 'k'), 'a\bb');
  assert.equal(parseConfigString('[u]\n\tk = "a\\"b"', 'u', null, 'k'), 'a"b');
  assert.equal(parseConfigString('[u]\n\tk = "a\\\\b"', 'u', null, 'k'), 'a\\b');
});

test('parseConfigString: unknown in-quote escape silently drops the backslash', () => {
  assert.equal(parseConfigString('[u]\n\tk = "a\\xb"', 'u', null, 'k'), 'axb');
});

test('parseConfigString: outside quotes, backslash is literal except \\\\', () => {
  assert.equal(parseConfigString('[u]\n\tk = a\\\\b', 'u', null, 'k'), 'a\\b');
  assert.equal(parseConfigString('[u]\n\tk = a\\b', 'u', null, 'k'), 'a\\b');
});

test('parseConfigString: a trailing backslash at end of line is left literal', () => {
  // i + 1 is out of range, so the escape branch is skipped and \ is appended.
  assert.equal(parseConfigString('[u]\n\tk = ab\\', 'u', null, 'k'), 'ab\\');
});

test('parseConfigString: trailing whitespace is trimmed unless the value ends INSIDE a quote', () => {
  assert.equal(parseConfigString('[u]\n\tk = val   \t ', 'u', null, 'k'), 'val');
  // A closed quote toggles inQuote off before the trim, so trailing spaces are
  // still trimmed (parity with the reference's single-line trim approximation).
  assert.equal(parseConfigString('[u]\n\tk = "val  "', 'u', null, 'k'), 'val');
  // Whitespace INSIDE quotes (not at the line end, which line.trim() strips) is
  // preserved verbatim.
  assert.equal(parseConfigString('[u]\n\tk = "val  "more', 'u', null, 'k'), 'val  more');
});

test('parseConfigString: whitespace around = is skipped on both sides', () => {
  assert.equal(parseConfigString('[u]\n\tkey   =   value', 'u', null, 'key'), 'value');
});

test('parseConfigString: an empty value is an empty string', () => {
  assert.equal(parseConfigString('[u]\n\tk =', 'u', null, 'k'), '');
});

// ---------------------------------------------------------------------------
// parseConfigString — malformed / edge-case section headers (never throws)
// ---------------------------------------------------------------------------

test('parseConfigString: simple header missing the closing ] does not match', () => {
  const cfg = '[core\n\teditor = vim\n';
  assert.equal(parseConfigString(cfg, 'core', null, 'editor'), null);
});

test('parseConfigString: subsection header with no opening quote does not match', () => {
  const cfg = '[remote origin]\n\turl = x\n';
  assert.equal(parseConfigString(cfg, 'remote', 'origin', 'url'), null);
});

test('parseConfigString: unterminated subsection quote does not match (no throw)', () => {
  const cfg = '[remote "origin\n\turl = x\n';
  assert.equal(parseConfigString(cfg, 'remote', 'origin', 'url'), null);
});

test('parseConfigString: subsection quote not followed by ] does not match', () => {
  const cfg = '[remote "origin" extra]\n\turl = x\n';
  assert.equal(parseConfigString(cfg, 'remote', 'origin', 'url'), null);
});

test('parseConfigString: subsection backslash escapes decode (\\\\ \\" and unknown drop)', () => {
  assert.equal(parseConfigString('[r "a\\\\b"]\n\turl = x\n', 'r', 'a\\b', 'url'), 'x');
  assert.equal(parseConfigString('[r "a\\"b"]\n\turl = x\n', 'r', 'a"b', 'url'), 'x');
  // Unknown subsection escape drops the backslash.
  assert.equal(parseConfigString('[r "a\\zb"]\n\turl = x\n', 'r', 'azb', 'url'), 'x');
});

test('parseConfigString: a trailing backslash in a subsection name is handled without throwing', () => {
  // The \ is the last char before the closing quote is absent -> no match.
  const cfg = '[r "ab\\';
  assert.equal(parseConfigString(cfg, 'r', 'ab', 'url'), null);
});

test('parseConfigString: keys before the matching section are ignored (inSection gate)', () => {
  const cfg = 'leadingjunk = ignored\n[core]\n\teditor = vim\n';
  assert.equal(parseConfigString(cfg, 'core', null, 'editor'), 'vim');
});

// ---------------------------------------------------------------------------
// parseGitConfigValue — reads from a gitDir, never throws
// ---------------------------------------------------------------------------

function tmp(): string {
  return fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-gitcfg-')));
}

test('parseGitConfigValue: reads <gitDir>/config off disk', () => {
  const gitDir = tmp();
  try {
    fs.writeFileSync(
      path.join(gitDir, 'config'),
      '[remote "origin"]\n\turl = https://github.com/acme/widget.git\n',
    );
    assert.equal(
      parseGitConfigValue(gitDir, 'remote', 'origin', 'url'),
      'https://github.com/acme/widget.git',
    );
  } finally {
    fs.rmSync(gitDir, { recursive: true, force: true });
  }
});

test('parseGitConfigValue: missing config file -> null (never throws)', () => {
  const gitDir = tmp();
  try {
    assert.equal(parseGitConfigValue(gitDir, 'remote', 'origin', 'url'), null);
  } finally {
    fs.rmSync(gitDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// Integration / red-green: getRemoteOriginUrl reads .git/config WITHOUT git.
//
// A hand-written `.git/config` in a directory that is NOT a complete git repo
// (no HEAD, no objects): `git config --get remote.origin.url` run there exits
// non-zero, so the OLD subprocess-only reader returned null. The filesystem-first
// reader resolves the URL directly. This test FAILS if the fs-first wiring is
// reverted (getRemoteOriginUrl falls back to the subprocess, which yields null).
// ---------------------------------------------------------------------------

test('getRemoteOriginUrl: resolves origin from a bare .git/config with no subprocess (red-green)', () => {
  const repo = tmp();
  try {
    fs.mkdirSync(path.join(repo, '.git'));
    fs.writeFileSync(
      path.join(repo, '.git', 'config'),
      '[core]\n\trepositoryformatversion = 0\n[remote "origin"]\n\turl = https://github.com/acme/widget.git\n',
    );
    // No .git/HEAD -> `git config` refuses (exit 1); only the fs parser succeeds.
    assert.equal(getRemoteOriginUrl(repo), 'https://github.com/acme/widget.git');
  } finally {
    fs.rmSync(repo, { recursive: true, force: true });
  }
});

test('getRemoteOriginUrl: a non-git directory still yields null', () => {
  const plain = tmp();
  try {
    assert.equal(getRemoteOriginUrl(plain), null);
  } finally {
    fs.rmSync(plain, { recursive: true, force: true });
  }
});
