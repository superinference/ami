import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  whichSync,
  findOnPath,
  isBinaryInstalled,
  clearBinaryCache,
  detectPackageManagers,
  detectRuntimes,
  KNOWN_PACKAGE_MANAGERS,
  KNOWN_JS_RUNTIMES,
} from '../src/utils/which';

// ---------------------------------------------------------------------------
// whichSync — cross-platform executable resolution (no-spawn fast path,
// win32 `where.exe`/PATHEXT, POSIX `command -v` fallback). The win32 branches
// are exercised by overriding process.platform (there is no executable bit on
// Windows, so a plain file with a PATHEXT extension counts as runnable).
// ---------------------------------------------------------------------------

let dir: string;
const savedPath = process.env.PATH;
const savedPathExt = process.env.PATHEXT;
const savedTimeout = process.env.AMI_WHICH_TIMEOUT_MS;
const realPlatform = process.platform;

function setPlatform(p: NodeJS.Platform): void {
  Object.defineProperty(process, 'platform', { value: p, configurable: true });
}

beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'which-'));
});

afterEach(() => {
  setPlatform(realPlatform);
  if (savedPath === undefined) delete process.env.PATH;
  else process.env.PATH = savedPath;
  if (savedPathExt === undefined) delete process.env.PATHEXT;
  else process.env.PATHEXT = savedPathExt;
  if (savedTimeout === undefined) delete process.env.AMI_WHICH_TIMEOUT_MS;
  else process.env.AMI_WHICH_TIMEOUT_MS = savedTimeout;
  fs.rmSync(dir, { recursive: true, force: true });
});

function makeExe(name: string, mode = 0o755): string {
  const p = path.join(dir, name);
  fs.writeFileSync(p, '#!/bin/sh\n');
  fs.chmodSync(p, mode);
  return p;
}

describe('whichSync', () => {
  it('returns null for an empty command', () => {
    assert.equal(whichSync(''), null);
  });

  it('resolves an executable found on PATH to its full path (no-spawn fast path)', () => {
    const exe = makeExe('mytool');
    process.env.PATH = dir;
    assert.equal(whichSync('mytool'), exe);
  });

  it('returns null when the command is not on PATH', () => {
    process.env.PATH = dir; // empty temp dir
    assert.equal(whichSync('definitely-not-here-xyz'), null);
  });

  it('ignores a non-executable (no x-bit) file on POSIX', () => {
    makeExe('plainfile', 0o644);
    process.env.PATH = dir;
    assert.equal(whichSync('plainfile'), null);
  });

  it('ignores a directory that shares the command name', () => {
    fs.mkdirSync(path.join(dir, 'notabin'));
    process.env.PATH = dir;
    assert.equal(whichSync('notabin'), null);
  });

  it('resolves a command that already contains a path separator', () => {
    const exe = makeExe('rel');
    assert.equal(whichSync(exe), exe);
  });

  it('returns null for a path-separated command that does not exist', () => {
    assert.equal(whichSync(path.join(dir, 'ghost')), null);
  });

  it('skips empty PATH entries without crashing', () => {
    const exe = makeExe('withgap');
    process.env.PATH = `${path.delimiter}${dir}${path.delimiter}`;
    assert.equal(whichSync('withgap'), exe);
  });

  it('falls back to the POSIX `command -v` subprocess when the fast path misses', () => {
    // A POSIX shell builtin (`:`) is not a file on PATH, so the fast path misses
    // and only the subprocess fallback can resolve it.
    assert.equal(whichSync(':'), ':');
  });

  describe('win32', () => {
    it('matches a file via a PATHEXT extension and treats it as executable', () => {
      fs.writeFileSync(path.join(dir, 'tool.EXE'), 'binary');
      setPlatform('win32');
      process.env.PATHEXT = '.COM;.EXE;.BAT';
      process.env.PATH = dir;
      assert.equal(whichSync('tool'), path.join(dir, 'tool.EXE'));
    });

    it('uses the default PATHEXT list when the env var is unset', () => {
      fs.writeFileSync(path.join(dir, 'x.CMD'), 'binary');
      setPlatform('win32');
      delete process.env.PATHEXT;
      process.env.PATH = dir;
      assert.equal(whichSync('x'), path.join(dir, 'x.CMD'));
    });

    it('resolves a win32 path that already carries a (forward-slash) separator', () => {
      const p = path.join(dir, 'abs.EXE');
      fs.writeFileSync(p, 'binary');
      setPlatform('win32');
      process.env.PATHEXT = '.EXE';
      assert.equal(whichSync(p), p);
    });

    it('treats a backslash-separated command as a path on win32 (misses -> null)', () => {
      setPlatform('win32');
      process.env.PATHEXT = '.EXE';
      // No forward slash: exercises the `isWindows() && includes("\\\\")` arm.
      assert.equal(whichSync('nosuch\\tool'), null);
    });

    it('falls back to where.exe (absent on this host) and returns null', () => {
      setPlatform('win32');
      process.env.PATH = dir; // nothing matches -> fast path misses
      process.env.PATHEXT = '.EXE';
      assert.equal(whichSync('no-such-win-tool'), null);
    });
  });

  it('honors AMI_WHICH_TIMEOUT_MS for the subprocess fallback', () => {
    process.env.AMI_WHICH_TIMEOUT_MS = '500';
    process.env.PATH = dir;
    assert.equal(whichSync('still-not-here'), null);
  });

  it('falls back to the default timeout when the env var is non-positive', () => {
    process.env.AMI_WHICH_TIMEOUT_MS = '0';
    process.env.PATH = dir;
    assert.equal(whichSync('also-not-here'), null);
  });
});

// ---------------------------------------------------------------------------
// whichSync — Bun.which native fast path (no process spawn). Under Bun the
// lookup routes through the in-process `Bun.which` resolver and never scans PATH
// or spawns `where.exe`/`command -v`. Exercised by installing a fake `Bun`
// global (absent under Node). Parity with Claude Code's `bunWhich` switch.
// ---------------------------------------------------------------------------
describe('whichSync — Bun fast path', () => {
  const g = globalThis as { Bun?: unknown };
  const savedBun = Object.prototype.hasOwnProperty.call(g, 'Bun')
    ? { present: true, value: g.Bun }
    : { present: false, value: undefined };

  afterEach(() => {
    if (savedBun.present) g.Bun = savedBun.value;
    else delete g.Bun;
  });

  it('routes through Bun.which when present, without scanning PATH or spawning', () => {
    const calls: string[] = [];
    g.Bun = {
      which: (cmd: string) => {
        calls.push(cmd);
        return `/bun/resolved/${cmd}`;
      },
    };
    // PATH is empty, so a PATH scan / spawn fallback would return null — only the
    // Bun fast path can produce a hit here.
    process.env.PATH = '';
    assert.equal(whichSync('node'), '/bun/resolved/node');
    assert.deepEqual(calls, ['node']);
  });

  it('returns null (no Node fallback) when Bun.which misses', () => {
    // Even though the binary IS on PATH, under Bun the Node PATH-scan fallback is
    // never consulted — Bun.which is authoritative (matches Claude: `bunWhich ?? …`
    // binds the function, so whichNodeSync never runs under Bun).
    const exe = makeExe('ontool');
    process.env.PATH = dir;
    g.Bun = { which: () => null };
    assert.equal(whichSync('ontool'), null);
    // Sanity: without Bun the same lookup resolves via the Node fast path.
    delete g.Bun;
    assert.equal(whichSync('ontool'), exe);
  });

  it('normalizes a Bun.which undefined result to null', () => {
    g.Bun = { which: () => undefined as unknown as string | null };
    assert.equal(whichSync('anything'), null);
  });

  it('falls back to the Node implementation when Bun exists but which is not a function', () => {
    const exe = makeExe('nodepath');
    process.env.PATH = dir;
    g.Bun = { which: 'not-a-function' };
    assert.equal(whichSync('nodepath'), exe);
  });

  it('short-circuits an empty command before consulting Bun.which', () => {
    let called = false;
    g.Bun = {
      which: () => {
        called = true;
        return '/should/not/happen';
      },
    };
    assert.equal(whichSync(''), null);
    assert.equal(called, false);
  });
});

describe('findOnPath — no-spawn lookup with injectable env', () => {
  it('returns null for an empty command', () => {
    assert.equal(findOnPath('', { PATH: dir }), null);
  });

  it('resolves against the PASSED env.PATH, not process.env', () => {
    const exe = makeExe('injtool');
    // process.env.PATH does NOT contain `dir`, but the injected env does.
    process.env.PATH = '';
    assert.equal(findOnPath('injtool', { PATH: dir }), exe);
  });

  it('returns null when the injected PATH has no match', () => {
    makeExe('present');
    assert.equal(findOnPath('absent', { PATH: dir }), null);
  });

  it('returns null for an empty injected PATH', () => {
    assert.equal(findOnPath('anything', { PATH: '' }), null);
  });

  it('never executes the candidate — a non-zero-exit script still resolves', () => {
    const p = path.join(dir, 'failing');
    fs.writeFileSync(p, '#!/bin/sh\nexit 3\n');
    fs.chmodSync(p, 0o755);
    assert.equal(findOnPath('failing', { PATH: dir }), p);
  });

  it('resolves a path-separated command without consulting PATH', () => {
    const exe = makeExe('sep');
    assert.equal(findOnPath(exe, { PATH: '' }), exe);
  });

  it('defaults to process.env when no env is supplied', () => {
    const exe = makeExe('deftool');
    process.env.PATH = dir;
    assert.equal(findOnPath('deftool'), exe);
  });
});

// ---------------------------------------------------------------------------
// isBinaryInstalled — cached, cross-platform binary-presence check built on
// whichSync. Parity with Claude Code's binaryCheck.ts.
// ---------------------------------------------------------------------------
describe('isBinaryInstalled', () => {
  beforeEach(() => clearBinaryCache());
  afterEach(() => clearBinaryCache());

  it('returns true for a binary resolvable on PATH, false for a missing one', () => {
    makeExe('tool-present');
    process.env.PATH = dir;
    assert.equal(isBinaryInstalled('tool-present'), true);
    assert.equal(isBinaryInstalled('tool-absent-xyz'), false);
  });

  it('returns false for an empty / whitespace / nullish command without probing', () => {
    // PATH points at an empty dir; the guard must short-circuit before any lookup.
    process.env.PATH = dir;
    assert.equal(isBinaryInstalled(''), false);
    assert.equal(isBinaryInstalled('   '), false);
    assert.equal(isBinaryInstalled(undefined as unknown as string), false);
  });

  it('caches BOTH positive and negative results (and clearBinaryCache re-probes)', () => {
    // Negative is cached: a binary that appears AFTER the first (missing) probe
    // is NOT seen until the cache is cleared.
    process.env.PATH = dir; // empty → miss
    assert.equal(isBinaryInstalled('late-tool'), false);
    makeExe('late-tool'); // now really present on PATH
    assert.equal(isBinaryInstalled('late-tool'), false, 'stale negative served from cache');
    clearBinaryCache();
    assert.equal(isBinaryInstalled('late-tool'), true, 're-probed after cache clear');

    // Positive is cached: removing the binary does not change the cached answer.
    fs.rmSync(path.join(dir, 'late-tool'));
    assert.equal(isBinaryInstalled('late-tool'), true, 'stale positive served from cache');
  });
});

// ---------------------------------------------------------------------------
// detectPackageManagers / detectRuntimes — system-wide, ordered, no-exec PATH
// probes (parity with Claude Code's detectPackageManagers/detectRuntimes).
// ---------------------------------------------------------------------------
describe('detectPackageManagers / detectRuntimes', () => {
  it('exposes the canonical ordered probe lists', () => {
    assert.deepEqual([...KNOWN_PACKAGE_MANAGERS], ['npm', 'yarn', 'pnpm']);
    assert.deepEqual([...KNOWN_JS_RUNTIMES], ['bun', 'deno', 'node']);
  });

  it('returns installed package managers in preference order (npm→yarn→pnpm)', () => {
    // Only pnpm and npm present; order must follow the canonical list, not the
    // order they were "discovered".
    const present = new Set(['pnpm', 'npm']);
    assert.deepEqual(
      detectPackageManagers((cmd) => present.has(cmd)),
      ['npm', 'pnpm'],
    );
  });

  it('returns [] when no package manager is installed', () => {
    assert.deepEqual(detectPackageManagers(() => false), []);
  });

  it('returns installed runtimes in preference order (bun→deno→node)', () => {
    const present = new Set(['node', 'bun']);
    assert.deepEqual(
      detectRuntimes((cmd) => present.has(cmd)),
      ['bun', 'node'],
    );
  });

  it('defaults to the real PATH probe (no-exec): seeded binaries are detected', () => {
    clearBinaryCache();
    makeExe('npm');
    makeExe('deno');
    process.env.PATH = dir;
    assert.deepEqual(detectPackageManagers(), ['npm']);
    assert.deepEqual(detectRuntimes(), ['deno']);
    clearBinaryCache();
  });
});
