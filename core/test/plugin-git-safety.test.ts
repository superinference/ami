import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  validateGitSource,
  parseGitRef,
  buildGitCloneArgs,
  assertPathWithinBase,
  resolvePluginDir,
  GIT_NO_PROMPT_ENV,
} from '../src/plugins';
import * as path from 'path';

test('validateGitSource rejects argument-injection leading-dash sources', () => {
  assert.equal(validateGitSource('--upload-pack=touch /tmp/pwn').ok, false);
  assert.equal(validateGitSource('-oProxyCommand=sh').ok, false);
});

test('validateGitSource rejects transport-helper sources (ext::/fd::/file::)', () => {
  assert.equal(validateGitSource('ext::sh -c "touch /tmp/pwn"').ok, false);
  assert.equal(validateGitSource('fd::17/foo').ok, false);
  assert.equal(validateGitSource('file::/etc/passwd').ok, false);
  const r = validateGitSource('ext::sh');
  assert.match(r.reason ?? '', /transport-helper/);
});

test('validateGitSource rejects non-allowlisted URL schemes (ftp)', () => {
  assert.equal(validateGitSource('ftp://host/repo.git').ok, false);
});

test('validateGitSource allows file:// local-clone URL but blocks file:: helper', () => {
  assert.equal(validateGitSource('file:///tmp/repo.git').ok, true);
  assert.equal(validateGitSource('file::/tmp/repo').ok, false);
});

test('validateGitSource accepts https/http/ssh/git URLs, scp-style and shorthand', () => {
  assert.equal(validateGitSource('https://github.com/owner/repo.git').ok, true);
  assert.equal(validateGitSource('http://example.com/r.git').ok, true);
  assert.equal(validateGitSource('ssh://git@host/owner/repo.git').ok, true);
  assert.equal(validateGitSource('git://host/repo.git').ok, true);
  assert.equal(validateGitSource('git@github.com:owner/repo.git').ok, true);
  assert.equal(validateGitSource('owner/repo').ok, true);
});

test('validateGitSource rejects empty and NUL-bearing sources', () => {
  assert.equal(validateGitSource('').ok, false);
  assert.equal(validateGitSource('   ').ok, false);
  assert.equal(validateGitSource('owner/re\0po').ok, false);
});

test('validateGitSource rejects malformed URL with allowed-looking scheme', () => {
  assert.equal(validateGitSource('https://').ok, false);
});

test('parseGitRef splits url#ref and handles bare / empty fragment', () => {
  assert.deepEqual(parseGitRef('owner/repo#v1.2.0'), { url: 'owner/repo', ref: 'v1.2.0' });
  assert.deepEqual(parseGitRef('owner/repo'), { url: 'owner/repo' });
  assert.deepEqual(parseGitRef('owner/repo#'), { url: 'owner/repo', ref: undefined });
  assert.deepEqual(parseGitRef('#ref'), { url: '#ref' });
});

test('buildGitCloneArgs inserts -- before url/dest and pins ref', () => {
  assert.deepEqual(buildGitCloneArgs('https://x/r.git', '/tmp/dest'), [
    'clone', '--depth', '1', '--', 'https://x/r.git', '/tmp/dest',
  ]);
  assert.deepEqual(buildGitCloneArgs('https://x/r.git', '/tmp/dest', 'v1'), [
    'clone', '--depth', '1', '--branch', 'v1', '--', 'https://x/r.git', '/tmp/dest',
  ]);
});

test('GIT_NO_PROMPT_ENV fails closed on prompts', () => {
  assert.equal(GIT_NO_PROMPT_ENV.GIT_TERMINAL_PROMPT, '0');
  assert.equal(GIT_NO_PROMPT_ENV.GIT_ASKPASS, '');
  assert.match(GIT_NO_PROMPT_ENV.GIT_SSH_COMMAND, /BatchMode=yes/);
});

test('assertPathWithinBase blocks traversal, allows contained', () => {
  const base = '/home/u/.ami/plugins';
  assert.equal(assertPathWithinBase(base, 'foo'), path.resolve(base, 'foo'));
  assert.equal(assertPathWithinBase(base, '.'), path.resolve(base));
  assert.throws(() => assertPathWithinBase(base, '../../etc'), /Path traversal/);
  assert.throws(() => assertPathWithinBase(base, '/etc/passwd'), /Path traversal/);
  // sibling-prefix must not pass for the base
  assert.throws(() => assertPathWithinBase('/foo/bar', '../barbaz'), /Path traversal/);
});

test('resolvePluginDir rejects unsafe names and contains safe ones', () => {
  const base = '/home/u/.ami/plugins';
  assert.equal(resolvePluginDir(base, 'myplugin'), path.resolve(base, 'myplugin'));
  assert.throws(() => resolvePluginDir(base, '..'), /Unsafe plugin name/);
  assert.throws(() => resolvePluginDir(base, 'a/b'), /Unsafe plugin name/);
  assert.throws(() => resolvePluginDir(base, '../evil'), /Unsafe plugin name/);
});
