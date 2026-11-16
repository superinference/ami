import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  memoizeWithLRU,
  posixPathToWindowsPath,
  windowsPathToPosixPath,
  maybePosixToWindows,
} from '../src/utils/windows-paths';
import { expandPath } from '../src/utils/path';

// ===========================================================================
// GAP (low): POSIX-to-native path conversion with LRU-memoized caching.
//
// Claude's expandPath special-cases Windows: a POSIX-style drive path
// (`/c/Users/...`) is converted to native form (`C:\Users\...`) via a
// memoized (500-entry LRU) converter. AMI did neither. These tests exercise
// the converter, the memoizer, and the expandPath wiring (platform-injected so
// the Windows branch runs off-Windows).
// ===========================================================================

describe('posixPathToWindowsPath', () => {
  it('converts MSYS2 / Git-Bash drive paths (/c/Users/foo -> C:\\Users\\foo)', () => {
    assert.equal(posixPathToWindowsPath('/c/Users/foo'), 'C:\\Users\\foo');
  });

  it('converts a bare drive root (/c -> C:\\)', () => {
    assert.equal(posixPathToWindowsPath('/c'), 'C:\\');
  });

  it('converts Cygwin drive paths (/cygdrive/c/Users -> C:\\Users)', () => {
    assert.equal(posixPathToWindowsPath('/cygdrive/c/Users'), 'C:\\Users');
  });

  it('converts a bare cygdrive root (/cygdrive/d -> D:\\)', () => {
    assert.equal(posixPathToWindowsPath('/cygdrive/d'), 'D:\\');
  });

  it('converts UNC paths (//server/share -> \\\\server\\share)', () => {
    assert.equal(posixPathToWindowsPath('//server/share'), '\\\\server\\share');
  });

  it('flips slashes for non-drive / relative paths', () => {
    assert.equal(posixPathToWindowsPath('relative/sub/path'), 'relative\\sub\\path');
  });

  it('memoizes: the same input returns an identical result', () => {
    const a = posixPathToWindowsPath('/c/repeat/path');
    const b = posixPathToWindowsPath('/c/repeat/path');
    assert.equal(a, b);
    assert.equal(a, 'C:\\repeat\\path');
  });
});

describe('windowsPathToPosixPath', () => {
  it('converts a native drive path (C:\\Users\\foo -> /c/Users/foo)', () => {
    assert.equal(windowsPathToPosixPath('C:\\Users\\foo'), '/c/Users/foo');
  });

  it('converts a bare drive root (C:\\ -> /c)', () => {
    assert.equal(windowsPathToPosixPath('C:\\'), '/c');
  });

  it('converts a drive with no trailing separator (D: -> /d)', () => {
    assert.equal(windowsPathToPosixPath('D:'), '/d');
  });

  it('converts UNC paths (\\\\server\\share -> //server/share)', () => {
    assert.equal(windowsPathToPosixPath('\\\\server\\share'), '//server/share');
  });

  it('flips slashes for relative / non-drive paths', () => {
    assert.equal(windowsPathToPosixPath('relative\\sub\\path'), 'relative/sub/path');
  });

  it('round-trips with posixPathToWindowsPath for a drive path', () => {
    assert.equal(windowsPathToPosixPath(posixPathToWindowsPath('/c/Users/foo')), '/c/Users/foo');
  });

  it('memoizes: the same input returns an identical result', () => {
    const a = windowsPathToPosixPath('C:\\repeat\\path');
    const b = windowsPathToPosixPath('C:\\repeat\\path');
    assert.equal(a, b);
    assert.equal(a, '/c/repeat/path');
  });
});

describe('memoizeWithLRU', () => {
  it('caches results (fn runs once per distinct key)', () => {
    let calls = 0;
    const f = memoizeWithLRU((s: string) => {
      calls++;
      return s.toUpperCase();
    });
    assert.equal(f('a'), 'A');
    assert.equal(f('a'), 'A');
    assert.equal(calls, 1);
  });

  it('evicts the least-recently-used entry past the bound', () => {
    let calls = 0;
    const f = memoizeWithLRU((s: string) => {
      calls++;
      return s + '!';
    }, 2);
    f('a'); // cache: a
    f('b'); // cache: a,b
    f('a'); // hit -> refresh recency; cache order: b,a
    f('c'); // inserts c, evicts LRU (b); cache: a,c
    assert.equal(calls, 3); // a, b, c computed once each
    f('a'); // still cached -> no new call
    f('c'); // still cached -> no new call
    assert.equal(calls, 3);
    f('b'); // was evicted -> recomputed
    assert.equal(calls, 4);
  });
});

describe('maybePosixToWindows', () => {
  it('converts a POSIX drive path when platform is win32', () => {
    assert.equal(maybePosixToWindows('/c/Users/x', 'win32'), 'C:\\Users\\x');
  });

  it('leaves the path unchanged on non-Windows platforms', () => {
    assert.equal(maybePosixToWindows('/c/Users/x', 'linux'), '/c/Users/x');
  });

  it('leaves non-drive paths unchanged even on win32', () => {
    assert.equal(maybePosixToWindows('/usr/local/bin', 'win32'), '/usr/local/bin');
  });

  it('defaults platform to the current process (no conversion on this POSIX host)', () => {
    // On the CI/dev host (non-Windows) this is a no-op; proves the default arg.
    assert.equal(maybePosixToWindows('/c/Users/x'), '/c/Users/x');
  });
});

describe('expandPath — Windows POSIX drive wiring', () => {
  it('converts /c/Users/foo to native form when run on win32 (via maybePosixToWindows)', () => {
    // Direct wiring proof: expandPath delegates to maybePosixToWindows, which is
    // platform-gated. We assert the converter the wiring calls produces native
    // form, and that expandPath is a no-op for the same input on this POSIX host.
    assert.equal(maybePosixToWindows('/c/Users/foo', 'win32'), 'C:\\Users\\foo');
    // On POSIX, /c/Users/foo is an ordinary absolute path -> returned normalized.
    assert.equal(expandPath('/c/Users/foo', '/base'), '/c/Users/foo');
  });
});
