/**
 * U058 — reference-journal user-bin-dir convention + PATH-membership helper:
 *  - getUserBinDir: the conventional per-user executable dir (`~/.local/bin`),
 *    HOME-aware via resolveHome (parity: Claude Code's xdg.ts getUserBinDir).
 *  - isDirOnPath: canonical PATH-membership check (resolve + trim + skip-empty;
 *    case-insensitive on Windows) shared by the native-install PATH diagnostic and
 *    the `~/.local/bin` doctor check so the rule lives in exactly one place.
 *
 * Run: cd core/core && node --import tsx --test test/reference-journal-userbin.test.ts
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import * as path from 'path';
import * as os from 'os';
import { getUserBinDir, isDirOnPath } from '../src/reference-journal';

describe('getUserBinDir (~/.local/bin convention)', () => {
  it('returns <HOME>/.local/bin honoring $HOME', () => {
    const env = { HOME: '/home/alice' } as NodeJS.ProcessEnv;
    assert.equal(getUserBinDir(env), path.join('/home/alice', '.local', 'bin'));
  });

  it('prefers an explicit homedir override over $HOME', () => {
    const env = { HOME: '/home/alice' } as NodeJS.ProcessEnv;
    assert.equal(getUserBinDir(env, '/tmp/inject'), path.join('/tmp/inject', '.local', 'bin'));
  });

  it('falls back to os.homedir() when HOME is unset/blank', () => {
    const env = { HOME: '   ' } as NodeJS.ProcessEnv;
    assert.equal(getUserBinDir(env), path.join(os.homedir(), '.local', 'bin'));
  });
});

describe('isDirOnPath (canonical PATH-membership)', () => {
  it('returns true for a directory present in PATH', () => {
    const dir = '/home/alice/.local/bin';
    const env = { PATH: `/usr/bin${path.delimiter}${dir}${path.delimiter}/bin` } as NodeJS.ProcessEnv;
    assert.equal(isDirOnPath(dir, env, 'linux'), true);
  });

  it('returns false when the directory is absent from PATH', () => {
    const env = { PATH: `/usr/bin${path.delimiter}/bin` } as NodeJS.ProcessEnv;
    assert.equal(isDirOnPath('/home/alice/.local/bin', env, 'linux'), false);
  });

  it('matches regardless of a trailing slash / non-canonical entry (path.resolve)', () => {
    const env = { PATH: `/home/alice/.local/bin/${path.delimiter}/bin` } as NodeJS.ProcessEnv;
    assert.equal(isDirOnPath('/home/alice/.local/bin', env, 'linux'), true);
  });

  it('skips empty/whitespace PATH entries without matching', () => {
    const env = { PATH: `${path.delimiter}   ${path.delimiter}` } as NodeJS.ProcessEnv;
    assert.equal(isDirOnPath('/x', env, 'linux'), false);
  });

  it('treats an unset/non-string PATH as empty', () => {
    assert.equal(isDirOnPath('/x', {} as NodeJS.ProcessEnv, 'linux'), false);
  });

  it('is case-insensitive on win32 only', () => {
    // Use absolute POSIX paths so path.resolve is stable on a POSIX test host;
    // only the final string comparison differs by `platform`.
    const env = { PATH: '/opt/bin' } as NodeJS.ProcessEnv;
    assert.equal(isDirOnPath('/Opt/Bin', env, 'win32'), true);
    assert.equal(isDirOnPath('/Opt/Bin', env, 'linux'), false);
  });
});
