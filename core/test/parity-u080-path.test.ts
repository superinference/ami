import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  isPathSafe,
  isPathWithin,
  toRelativePath,
  validatePathExpansionSafety,
  _pathHooks,
} from '../src/utils/path';
import { validateFilePath } from '../src/tools/tool-utils';
import { fileReadTool } from '../src/tools/file-read';
import type { ToolContext } from '../src/types';

const realPlatform = _pathHooks.platform;
afterEach(() => {
  _pathHooks.platform = realPlatform;
});

// ---------------------------------------------------------------------------
// GAPS 8 + 10 — case-insensitive path containment on case-insensitive
// filesystems (Windows drive letter + macOS case fold).
// RED: isDescendant compares case-sensitively on all platforms, so a
// differently-cased-but-genuinely-contained path is judged OUTSIDE the boundary.
// ---------------------------------------------------------------------------

describe('case-insensitive containment (gaps 8, 10)', () => {
  it('win32: differently-cased drive letter is still inside the boundary', () => {
    _pathHooks.platform = 'win32';
    // Forward slashes keep the test host-independent (the POSIX `path` module on
    // a Linux CI host does not treat `\\` as a separator); the case-fold seam is
    // what is under test. Lower-case drive letter on the candidate vs upper-case
    // on the boundary still matches.
    assert.equal(isPathWithin('c:/Users/Foo/proj/src/a.ts', 'C:/Users/Foo/proj'), true);
    // General casing differences fold too.
    assert.equal(isPathWithin('C:/USERS/FOO/PROJ/x.ts', 'C:/Users/Foo/proj'), true);
  });

  it('darwin: casing differences in the path fold to match', () => {
    _pathHooks.platform = 'darwin';
    assert.equal(isPathWithin('/Users/Foo/project/src/a.ts', '/users/foo/project'), true);
  });

  it('linux: comparison stays case-sensitive (no spurious match)', () => {
    _pathHooks.platform = 'linux';
    assert.equal(isPathWithin('/home/Foo/project/a.ts', '/home/foo/project'), false);
    assert.equal(isPathWithin('/home/foo/project/a.ts', '/home/foo/project'), true);
  });

  it('case fold does not let a sibling-prefix dir false-match on win32', () => {
    _pathHooks.platform = 'win32';
    // "c:/app2" must NOT be considered inside "C:/app" even after folding.
    assert.equal(isPathWithin('c:/app2/x.ts', 'C:/app'), false);
  });

  it('isPathSafe honours the case-insensitive boundary on darwin', () => {
    _pathHooks.platform = 'darwin';
    // A child whose casing differs from cwd is safe on a case-insensitive FS.
    assert.ok(isPathSafe('/Users/Foo/Project/src/a.ts', '/users/foo/project'));
  });
});

// ---------------------------------------------------------------------------
// GAPS 3, 6, 7 — reject unresolved tilde-expansion variants.
// GAPS 4, 7 — reject shell-expansion syntax ($, %, leading =).
// RED: validatePathExpansionSafety does not exist / validateFilePath only
// checks NUL bytes, so a ~root / $HOME / %TEMP% / =rg path is accepted.
// ---------------------------------------------------------------------------

describe('validatePathExpansionSafety (gaps 3, 4, 6, 7)', () => {
  it('rejects tilde variants (~user, ~+, ~-, ~N)', () => {
    for (const p of ['~root/.ssh/id_rsa', '~+/x', '~-/x', '~1/x', '~user']) {
      const r = validatePathExpansionSafety(p);
      assert.ok(r && /tilde/i.test(r), `expected ${p} rejected, got ${r}`);
    }
  });

  it('allows the bare ~, ~/ and ~\\ forms expandPath resolves', () => {
    assert.equal(validatePathExpansionSafety('~'), null);
    assert.equal(validatePathExpansionSafety('~/notes.md'), null);
    assert.equal(validatePathExpansionSafety('~\\Documents'), null);
  });

  it('rejects $VAR / ${VAR} / $(cmd) shell expansion', () => {
    for (const p of ['$HOME/x', '${HOME}/x', '$(pwd)/x', 'a/$VAR/b']) {
      const r = validatePathExpansionSafety(p);
      assert.ok(r && /shell expansion/i.test(r), `expected ${p} rejected, got ${r}`);
    }
  });

  it('rejects %VAR% Windows expansion and leading = zsh expansion', () => {
    assert.ok(validatePathExpansionSafety('%TEMP%/x'));
    assert.ok(validatePathExpansionSafety('=rg'));
  });

  it('allows ordinary absolute and relative paths', () => {
    assert.equal(validatePathExpansionSafety('/home/user/project/file.txt'), null);
    assert.equal(validatePathExpansionSafety('src/app.ts'), null);
    assert.equal(validatePathExpansionSafety('./a/b.ts'), null);
  });

  it('is a no-op for non-string input (handled by other guards)', () => {
    // @ts-expect-error — exercising the runtime guard
    assert.equal(validatePathExpansionSafety(undefined), null);
  });
});

// ---------------------------------------------------------------------------
// Functional: the shared file-tool guard now rejects these inputs.
// ---------------------------------------------------------------------------

describe('validateFilePath wires in expansion-safety (gaps 3, 4, 6, 7)', () => {
  it('rejects a tilde-variant path', () => {
    const r = validateFilePath('~root/.ssh/id_rsa');
    assert.ok(r?.isError && /tilde/i.test(r.output));
  });
  it('rejects a $VAR path', () => {
    const r = validateFilePath('$HOME/secret');
    assert.ok(r?.isError && /shell expansion/i.test(r.output));
  });
  it('still rejects NUL bytes (unchanged)', () => {
    const r = validateFilePath('/x\x00/y');
    assert.ok(r?.isError && /null byte/i.test(r.output));
  });
  it('accepts a plain path', () => {
    assert.equal(validateFilePath('/home/user/ok.txt'), null);
  });
});

// ---------------------------------------------------------------------------
// E2E through the real file_read tool: a shell-expansion path is blocked
// before any filesystem access.
// ---------------------------------------------------------------------------

describe('file_read rejects shell-expansion path (E2E, gaps 4/7)', () => {
  let tmpDir: string;
  const ctx = (): ToolContext => ({ cwd: tmpDir, abortSignal: new AbortController().signal });

  it('returns an error for a $-containing file_path', async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u080-'));
    try {
      const result = await fileReadTool.execute({ file_path: '$HOME/.ssh/id_rsa' }, ctx());
      assert.ok(result.isError, 'expected error result');
      assert.match(result.output, /shell expansion/i);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('returns an error for a ~root tilde-variant file_path', async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u080-'));
    try {
      const result = await fileReadTool.execute({ file_path: '~root/.ssh/id_rsa' }, ctx());
      assert.ok(result.isError, 'expected error result');
      assert.match(result.output, /tilde/i);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// GAP 15 — toRelativePath cwd is optional (defaults to process.cwd()).
// RED: cwd was a required positional parameter.
// ---------------------------------------------------------------------------

describe('toRelativePath optional cwd (gap 15)', () => {
  it('defaults cwd to process.cwd()', () => {
    const abs = path.join(process.cwd(), 'sub', 'file.ts');
    assert.equal(toRelativePath(abs), path.join('sub', 'file.ts'));
  });
  it('still accepts an explicit cwd', () => {
    assert.equal(toRelativePath('/a/b/c.ts', '/a/b'), 'c.ts');
  });
});
