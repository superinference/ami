import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { extractInputRedirectionPaths, extractCommandPaths } from '../src/tools/bash-security';

// ---------------------------------------------------------------------------
// Gap #34, #43 — input redirection path extraction
// ---------------------------------------------------------------------------

describe('extractInputRedirectionPaths', () => {
  // ---- Simple < file redirections ----

  it('extracts path from simple < file', () => {
    const paths = extractInputRedirectionPaths('< /etc/shadow');
    assert.deepStrictEqual(paths, ['/etc/shadow']);
  });

  it('extracts path from command < file', () => {
    const paths = extractInputRedirectionPaths('wc -l < /etc/passwd');
    assert.deepStrictEqual(paths, ['/etc/passwd']);
  });

  it('extracts path from fd redirect: 0< file', () => {
    const paths = extractInputRedirectionPaths('cat 0< /etc/shadow');
    assert.deepStrictEqual(paths, ['/etc/shadow']);
  });

  it('extracts multiple input redirections', () => {
    const paths = extractInputRedirectionPaths('diff <(cat /a) < /b');
    // The < /b is the only plain input redirection
    assert.ok(paths.includes('/b'));
  });

  // ---- Quoted file paths ----

  it('extracts double-quoted path: < "file name"', () => {
    const paths = extractInputRedirectionPaths('cat < "my file.txt"');
    assert.deepStrictEqual(paths, ['my file.txt']);
  });

  it('extracts single-quoted path: < \'file name\'', () => {
    const paths = extractInputRedirectionPaths("cat < '/etc/shadow'");
    assert.deepStrictEqual(paths, ['/etc/shadow']);
  });

  // ---- Heredocs should NOT be flagged ----

  it('does NOT flag heredoc << EOF as file read', () => {
    const paths = extractInputRedirectionPaths('cat << EOF\nhello\nEOF');
    assert.deepStrictEqual(paths, []);
  });

  it('does NOT flag heredoc <<- with indent stripping', () => {
    const paths = extractInputRedirectionPaths('cat <<- MARKER\n\thello\nMARKER');
    assert.deepStrictEqual(paths, []);
  });

  it('does NOT flag quoted heredoc << "EOF"', () => {
    const paths = extractInputRedirectionPaths('cat << "EOF"\nhello\nEOF');
    assert.deepStrictEqual(paths, []);
  });

  // ---- Herestrings should NOT be flagged ----

  it('does NOT flag herestring <<< as file read', () => {
    const paths = extractInputRedirectionPaths('read var <<< "hello world"');
    assert.deepStrictEqual(paths, []);
  });

  // ---- /dev/null is safe ----

  it('skips /dev/null (safe no-op)', () => {
    const paths = extractInputRedirectionPaths('cmd < /dev/null');
    assert.deepStrictEqual(paths, []);
  });

  it('skips /dev/fd/ paths (safe)', () => {
    const paths = extractInputRedirectionPaths('cmd < /dev/fd/3');
    assert.deepStrictEqual(paths, []);
  });

  // ---- Multiple redirections ----

  it('extracts multiple input redirections from compound commands', () => {
    const paths = extractInputRedirectionPaths('cmd1 < /a ; cmd2 < /b');
    assert.deepStrictEqual(paths, ['/a', '/b']);
  });

  // ---- Edge cases ----

  it('returns empty for no redirections', () => {
    const paths = extractInputRedirectionPaths('echo hello world');
    assert.deepStrictEqual(paths, []);
  });

  it('does not false-positive on output redirection >', () => {
    const paths = extractInputRedirectionPaths('echo hello > /tmp/out');
    assert.deepStrictEqual(paths, []);
  });

  it('does not false-positive on append >>', () => {
    const paths = extractInputRedirectionPaths('echo hello >> /tmp/out');
    assert.deepStrictEqual(paths, []);
  });

  it('handles < inside single quotes (not a redirection)', () => {
    const paths = extractInputRedirectionPaths("echo '< /etc/shadow'");
    assert.deepStrictEqual(paths, []);
  });

  it('handles < inside double quotes (not a redirection)', () => {
    const paths = extractInputRedirectionPaths('echo "< /etc/shadow"');
    assert.deepStrictEqual(paths, []);
  });

  it('handles escaped < (not a redirection)', () => {
    const paths = extractInputRedirectionPaths('echo \\< /etc/shadow');
    assert.deepStrictEqual(paths, []);
  });

  it('extracts path with relative path', () => {
    const paths = extractInputRedirectionPaths('cat < ./secret.txt');
    assert.deepStrictEqual(paths, ['./secret.txt']);
  });

  it('extracts path with tilde', () => {
    const paths = extractInputRedirectionPaths('cat < ~/Desktop/secrets.env');
    assert.deepStrictEqual(paths, ['~/Desktop/secrets.env']);
  });

  it('handles pipe after redirection correctly', () => {
    const paths = extractInputRedirectionPaths('cmd < /etc/shadow | grep root');
    assert.deepStrictEqual(paths, ['/etc/shadow']);
  });

  it('handles && after redirection correctly', () => {
    const paths = extractInputRedirectionPaths('cmd < /etc/shadow && echo done');
    assert.deepStrictEqual(paths, ['/etc/shadow']);
  });
});

// ---------------------------------------------------------------------------
// Reader command path extraction (tac, egrep, fgrep, etc.)
// ---------------------------------------------------------------------------

describe('extractCommandPaths — reader commands', () => {
  it('extracts file path from tac', () => {
    const paths = extractCommandPaths('tac /etc/passwd');
    assert.deepStrictEqual(paths, ['/etc/passwd']);
  });

  it('extracts file path from tac with flags', () => {
    const paths = extractCommandPaths('tac -s : /etc/passwd');
    // -s and : are flags, /etc/passwd is the file
    assert.ok(paths.includes('/etc/passwd'));
  });

  it('extracts file path from rev', () => {
    const paths = extractCommandPaths('rev /etc/shadow');
    assert.deepStrictEqual(paths, ['/etc/shadow']);
  });

  it('extracts file path from egrep (pattern then file)', () => {
    const paths = extractCommandPaths('egrep root /etc/passwd');
    // egrep works like grep: first non-flag arg is pattern, rest are files
    assert.deepStrictEqual(paths, ['/etc/passwd']);
  });

  it('extracts file path from fgrep', () => {
    const paths = extractCommandPaths('fgrep secret /etc/shadow');
    assert.deepStrictEqual(paths, ['/etc/shadow']);
  });

  it('extracts file path from nl', () => {
    const paths = extractCommandPaths('nl /etc/passwd');
    assert.deepStrictEqual(paths, ['/etc/passwd']);
  });

  it('extracts file path from less', () => {
    const paths = extractCommandPaths('less /etc/shadow');
    assert.deepStrictEqual(paths, ['/etc/shadow']);
  });

  it('extracts file path from more', () => {
    const paths = extractCommandPaths('more /etc/passwd');
    assert.deepStrictEqual(paths, ['/etc/passwd']);
  });

  it('extracts file path from fmt', () => {
    const paths = extractCommandPaths('fmt /etc/passwd');
    assert.deepStrictEqual(paths, ['/etc/passwd']);
  });

  it('extracts file path from column', () => {
    const paths = extractCommandPaths('column -t /etc/fstab');
    assert.ok(paths.includes('/etc/fstab'));
  });

  it('extracts file path from paste', () => {
    const paths = extractCommandPaths('paste /a /b');
    assert.deepStrictEqual(paths, ['/a', '/b']);
  });

  it('extracts file path from uniq', () => {
    const paths = extractCommandPaths('uniq /tmp/data.txt');
    assert.deepStrictEqual(paths, ['/tmp/data.txt']);
  });

  it('extracts file path from cut', () => {
    const paths = extractCommandPaths('cut -d: -f1 /etc/passwd');
    assert.ok(paths.includes('/etc/passwd'));
  });

  it('extracts file path from comm', () => {
    const paths = extractCommandPaths('comm /a /b');
    assert.deepStrictEqual(paths, ['/a', '/b']);
  });

  it('extracts file path from join', () => {
    const paths = extractCommandPaths('join /a /b');
    assert.deepStrictEqual(paths, ['/a', '/b']);
  });

  it('skips flags for reader commands', () => {
    const paths = extractCommandPaths('tac -r /etc/passwd');
    assert.ok(!paths.includes('-r'));
    assert.ok(paths.includes('/etc/passwd'));
  });

  it('handles egrep with flags', () => {
    const paths = extractCommandPaths('egrep -i pattern /etc/passwd /etc/shadow');
    assert.ok(paths.includes('/etc/passwd'));
    assert.ok(paths.includes('/etc/shadow'));
  });
});
