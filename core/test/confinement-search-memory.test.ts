/**
 * WORKSPACE CONFINEMENT — search tools (glob/grep) and instruction @-imports.
 *
 * Regression guard for the escape vectors beyond bash/file-tools:
 *   - glob pattern "../../**" walking above the search root,
 *   - grep `include` filter "../**" widening the search out,
 *   - resolveSearchPath symlink escapes (string-prefix bypass),
 *   - MemoryManager @-imports inlining arbitrary files (@~/.ssh/id_rsa, @/etc/passwd,
 *     @../../other) into the system prompt.
 * Crucially also asserts the NON-escaping cases still work (grep's regex may contain ".."
 * like "a..b"; normal globs like "**\/*.ts" are fine; in-cwd @-imports still inline).
 *
 * Run: cd core/core && node --import tsx --test test/confinement-search-memory.test.ts
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { rejectEscapingPattern, resolveSearchPath } from '../src/tools/tool-utils';
import { globTool } from '../src/tools/glob';
import { grepTool } from '../src/tools/grep';
import { MemoryManager } from '../src/memory';

let CWD = '';
let PARENT = '';
before(() => {
  PARENT = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'conf-sm-')));
  CWD = fs.realpathSync(fs.mkdtempSync(path.join(PARENT, 'ws-')));
});
after(() => { try { fs.rmSync(PARENT, { recursive: true, force: true }); } catch { /* ignore */ } });

const ctx = () => ({ cwd: CWD, abortSignal: new AbortController().signal });

// ---------------------------------------------------------------------------
describe('rejectEscapingPattern — ".." path-segment detection', () => {
  const rejected = ['../x', '../../**/*.ts', 'a/../b', '..', '../', 'src/../../etc/*', '..\\win', 'a/b/../../../c'];
  for (const p of rejected) {
    it(`rejects escaping pattern ${JSON.stringify(p)}`, () => {
      assert.ok(rejectEscapingPattern(p), `${p} must be rejected`);
    });
  }
  // ".." must be a whole path SEGMENT — substrings like "a..b" or "..bar" are fine, so a
  // grep regex such as "a..b" is never mis-flagged.
  const allowed = ['**/*.ts', 'src/**', '*.json', 'a..b', 'foo..bar/*', 'file..name.txt', '.config/**', 'dir/..suffix', 'x/y/z.ts'];
  for (const p of allowed) {
    it(`allows non-escaping pattern ${JSON.stringify(p)}`, () => {
      assert.equal(rejectEscapingPattern(p), null, `${p} must be allowed`);
    });
  }
});

// ---------------------------------------------------------------------------
describe('resolveSearchPath — root confinement + symlink resolution', () => {
  it('allows a relative subdir and the cwd itself', () => {
    assert.equal(resolveSearchPath('src', CWD).error, undefined);
    assert.equal(resolveSearchPath(undefined, CWD).error, undefined);
    assert.equal(resolveSearchPath('.', CWD).error, undefined);
  });
  it('rejects a relative path that escapes cwd', () => {
    assert.ok(resolveSearchPath('../elsewhere', CWD).error);
  });
  it('rejects an absolute path outside cwd', () => {
    assert.ok(resolveSearchPath(path.join(PARENT, 'nope'), CWD).error);
  });
  it('rejects a symlink inside cwd that points OUTSIDE (realpath, not string prefix)', () => {
    const link = path.join(CWD, 'sl-out');
    try { fs.symlinkSync(PARENT, link); } catch { return; }
    assert.ok(resolveSearchPath('sl-out', CWD).error, 'symlink escaping cwd must be rejected');
  });
});

// ---------------------------------------------------------------------------
describe('globTool — pattern must not escape the workspace', () => {
  it('rejects a glob pattern that traverses above the root', async () => {
    const r = await globTool.execute({ pattern: '../../**/*.ts' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /must not contain/i);
  });
  it('rejects an explicit search path outside cwd', async () => {
    const r = await globTool.execute({ pattern: '**/*.ts', path: path.join(PARENT, 'x') }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /outside the workspace/i);
  });
  it('accepts a normal in-workspace pattern (matches a seeded file)', async () => {
    fs.writeFileSync(path.join(CWD, 'hello.ts'), 'x');
    const r = await globTool.execute({ pattern: '**/*.ts' }, ctx());
    assert.ok(!r.isError, r.output);
    assert.match(r.output, /hello\.ts/);
  });
});

// ---------------------------------------------------------------------------
describe('grepTool — include filter confined; regex pattern NOT path-checked', () => {
  it('rejects an `include` glob that escapes the workspace', async () => {
    const r = await grepTool.execute({ pattern: 'anything', include: '../../**' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /must not contain/i);
  });
  it('rejects a search path outside cwd', async () => {
    const r = await grepTool.execute({ pattern: 'anything', path: path.join(PARENT, 'x') }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /outside the workspace/i);
  });
  it('does NOT reject a regex pattern that merely contains ".." (e.g. "a..b")', async () => {
    fs.writeFileSync(path.join(CWD, 'data.txt'), 'axb aYb a.b\n');
    const r = await grepTool.execute({ pattern: 'a..b', path: '.' }, ctx());
    // Whatever it finds, it must NOT be the pattern-escape error — the regex is not a path.
    assert.doesNotMatch(r.output ?? '', /must not contain/i);
  });
});

// ---------------------------------------------------------------------------
describe('MemoryManager @-imports — confined to the workspace', () => {
  const mm = () => new MemoryManager(CWD);
  const base = () => path.join(CWD, 'CLAUDE.md');

  it('blocks @~/… home imports (leaves a visible marker, no file content)', () => {
    const out = mm().resolveAtImports('refs @~/.ssh/id_rsa here', base());
    assert.match(out, /import blocked — outside workspace/i);
  });
  it('blocks @/etc/passwd absolute imports', () => {
    const out = mm().resolveAtImports('see @/etc/passwd', base());
    assert.match(out, /import blocked — outside workspace/i);
  });
  it('blocks @../../other relative escapes', () => {
    fs.writeFileSync(path.join(PARENT, 'secret.md'), 'TOPSECRET');
    const out = mm().resolveAtImports('load @../secret.md', base());
    assert.doesNotMatch(out, /TOPSECRET/, 'outside file content must NOT be inlined');
    assert.match(out, /import blocked — outside workspace/i);
  });
  it('STILL inlines an in-workspace @-import (normal behavior preserved)', () => {
    fs.writeFileSync(path.join(CWD, 'inside.md'), 'INSIDE_CONTENT');
    const out = mm().resolveAtImports('include @./inside.md', base());
    assert.match(out, /INSIDE_CONTENT/, 'in-workspace import must still inline');
  });
  it('a nested subdir @-import inside the workspace inlines', () => {
    fs.mkdirSync(path.join(CWD, 'sub'), { recursive: true });
    fs.writeFileSync(path.join(CWD, 'sub', 'n.md'), 'NESTED_OK');
    const out = mm().resolveAtImports('@./sub/n.md', base());
    assert.match(out, /NESTED_OK/);
  });
  it('an @-token inside a code fence is left untouched (not imported)', () => {
    const src = '```\n@~/.ssh/id_rsa\n```';
    const out = mm().resolveAtImports(src, base());
    assert.match(out, /@~\/\.ssh\/id_rsa/, 'fenced token is literal, not resolved');
    assert.doesNotMatch(out, /import blocked/i);
  });
});
