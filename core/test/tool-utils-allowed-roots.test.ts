/**
 * CORE UNIT — allowed-read-roots plumbing in the tool path guards.
 *
 * Regression guard for the reported bug: a skill loaded from a folder OUTSIDE the launch
 * cwd (e.g. ~/.ami/plugins/<name>/skills/<name>) could not read its own bundled files —
 * the core path guards (resolveFilePath / resolveSearchPath / isWithinWorkspaceRoots)
 * hard-confined to cwd with NO escape hatch, so even after the permission layer granted the
 * dir, glob/read/list_dir still returned "outside the workspace directory".
 *
 * These tests isolate the guard functions (no executor, no permission manager) and pin the
 * three behaviors the fix must preserve forever:
 *   - DENY  : an out-of-cwd path with NO granted roots stays blocked (default confinement),
 *   - ALLOW : the SAME path becomes readable once its dir is passed as an allowed root,
 *   - corner: sensitive paths, symlink escapes, and ungranted siblings stay blocked even
 *             with roots present; nested subdirs of a granted root are allowed.
 *
 * Run: cd core/core && node --import tsx --test test/tool-utils-allowed-roots.test.ts
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  isWithinWorkspaceRoots,
  resolveFilePath,
  resolveSearchPath,
  safeReadFile,
  safeLstat,
} from '../src/tools/tool-utils';

let BASE = '';
let CWD = '';
let SKILL = '';      // an external "plugin/skill" dir OUTSIDE cwd (a sibling)
let SIBLING = '';    // a different external dir, never granted
before(() => {
  BASE = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'tuar-')));
  CWD = fs.realpathSync(fs.mkdtempSync(path.join(BASE, 'cwd-')));
  SKILL = fs.realpathSync(fs.mkdtempSync(path.join(BASE, 'skill-')));
  SIBLING = fs.realpathSync(fs.mkdtempSync(path.join(BASE, 'sib-')));
  fs.writeFileSync(path.join(CWD, 'inside.txt'), 'inside');
  fs.mkdirSync(path.join(SKILL, 'skills'), { recursive: true });
  fs.writeFileSync(path.join(SKILL, 'skills', 'SKILL.md'), '# skill guidance');
  fs.writeFileSync(path.join(SIBLING, 'secret.txt'), 'nope');
});
after(() => { try { fs.rmSync(BASE, { recursive: true, force: true }); } catch { /* ignore */ } });

describe('isWithinWorkspaceRoots — logical containment with granted roots', () => {
  it('cwd itself and paths under it are in-bounds', () => {
    assert.equal(isWithinWorkspaceRoots(CWD, CWD), true);
    assert.equal(isWithinWorkspaceRoots(path.join(CWD, 'a/b.txt'), CWD), true);
  });
  it('an out-of-cwd path is OUT of bounds with no roots (DENY)', () => {
    assert.equal(isWithinWorkspaceRoots(path.join(SKILL, 'skills/SKILL.md'), CWD), false);
  });
  it('the SAME path is in-bounds once its root (or an ancestor) is granted (ALLOW)', () => {
    assert.equal(isWithinWorkspaceRoots(path.join(SKILL, 'skills/SKILL.md'), CWD, [SKILL]), true);
    // a nested subdir of the grant is also allowed
    assert.equal(isWithinWorkspaceRoots(path.join(SKILL, 'skills'), CWD, [SKILL]), true);
  });
  it('an ungranted sibling stays blocked even when another root is granted (corner)', () => {
    assert.equal(isWithinWorkspaceRoots(path.join(SIBLING, 'secret.txt'), CWD, [SKILL]), false);
  });
  it('a prefix-sibling is NOT matched by string prefix (corner: /skill vs /skill-2)', () => {
    // Granting "<BASE>/skill-x" must not accidentally allow "<BASE>/skill-xtra".
    assert.equal(isWithinWorkspaceRoots(`${SKILL}tra/f.txt`, CWD, [SKILL]), false);
  });
});

describe('resolveFilePath — file read guard honors granted roots', () => {
  it('DENY: out-of-cwd file is blocked with no roots', () => {
    const r = resolveFilePath(path.join(SKILL, 'skills/SKILL.md'), CWD);
    assert.ok(r.error, 'must be blocked without a grant');
    assert.match(r.error!.output, /outside the workspace/i);
  });
  it('ALLOW: out-of-cwd file is permitted once its root is granted', () => {
    const r = resolveFilePath(path.join(SKILL, 'skills/SKILL.md'), CWD, [SKILL]);
    assert.equal(r.error, undefined, 'granted root makes the skill file readable');
  });
  it('corner: a sensitive system path is blocked even if its dir were granted', () => {
    const r = resolveFilePath('/etc/shadow', CWD, ['/etc']);
    assert.ok(r.error);
    assert.match(r.error!.output, /sensitive system path/i);
  });
  it('corner: an ungranted sibling stays blocked while the skill root is granted', () => {
    const r = resolveFilePath(path.join(SIBLING, 'secret.txt'), CWD, [SKILL]);
    assert.ok(r.error);
  });
  it('safeReadFile returns the skill file content once the root is granted', () => {
    const r = safeReadFile(path.join(SKILL, 'skills/SKILL.md'), CWD, [SKILL]);
    assert.equal(r.error, undefined);
    assert.match(r.content!, /skill guidance/);
  });
});

describe('resolveSearchPath / safeLstat — glob & stat guards honor granted roots', () => {
  it('DENY then ALLOW: out-of-cwd search root flips with the grant', () => {
    assert.ok(resolveSearchPath(SKILL, CWD).error, 'blocked without grant');
    assert.equal(resolveSearchPath(SKILL, CWD, [SKILL]).error, undefined, 'allowed with grant');
  });
  it('safeLstat: out-of-cwd stat is blocked without, allowed with, the grant', () => {
    assert.ok(safeLstat(path.join(SKILL, 'skills'), CWD).error);
    assert.equal(safeLstat(path.join(SKILL, 'skills'), CWD, [SKILL]).error, undefined);
  });
});

describe('symlink-escape stays blocked even with grants (corner, defense-in-depth)', () => {
  it('a symlink inside cwd that resolves to an UNGRANTED dir is rejected via symlink', () => {
    const link = path.join(CWD, 'escape-link');
    try { fs.unlinkSync(link); } catch { /* fresh */ }
    fs.symlinkSync(SIBLING, link); // cwd/escape-link -> <ungranted sibling>
    // Logical path is inside cwd, so it passes the first check; the realpath step must catch it.
    const r = resolveFilePath(path.join('escape-link', 'secret.txt'), CWD, [SKILL]);
    assert.ok(r.error, 'symlink to an ungranted external dir must be blocked');
    assert.match(r.error!.output, /symlink/i);
  });
  it('a symlink inside cwd that resolves INTO a granted root is allowed', () => {
    const link = path.join(CWD, 'skill-link');
    try { fs.unlinkSync(link); } catch { /* fresh */ }
    fs.symlinkSync(SKILL, link); // cwd/skill-link -> <granted skill dir>
    const r = resolveFilePath(path.join('skill-link', 'skills/SKILL.md'), CWD, [SKILL]);
    assert.equal(r.error, undefined, 'symlink into a granted root resolves in-bounds');
  });
});
