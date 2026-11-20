import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  PermissionManager,
  GLOB_PATTERN_REGEX,
  containsGlobPattern,
  containsPathTraversal,
  isUnvalidatableReadGlob,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U030 — glob boundary handling for READ paths. Parity: the reference's
// pathValidation glob handling (pathValidation.ts L1161-L1241 / "finding #15").
// A plain (non-traversal) read glob cannot be statically validated — the pre-glob
// base is the only segment realpath'd, so a symlink matched INSIDE the expansion
// (e.g. `/project/<star>/passwd` with `/project/link -> /etc`) is never examined
// while the literal glob still prefix-matches the workspace. Such a glob must be
// forced to a manual-approval prompt (not silently auto-allowed). A traversal glob
// falls through to normal resolve-and-revalidate.
// ---------------------------------------------------------------------------

let CWD = '';
before(() => {
  CWD = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'glob-guard-')));
});
after(() => { try { fs.rmSync(CWD, { recursive: true, force: true }); } catch { /* ignore */ } });

describe('glob metacharacter + traversal predicates', () => {
  it('GLOB_PATTERN_REGEX matches * ? [ ] and nothing inert', () => {
    for (const p of ['a*.txt', 'file?.log', '[abc].ts', 'x].ts']) {
      assert.equal(GLOB_PATTERN_REGEX.test(p), true, `${p} is a glob`);
      assert.equal(containsGlobPattern(p), true);
    }
    for (const p of ['plain.txt', 'src/index.ts', 'a-b_c.ts']) {
      assert.equal(containsGlobPattern(p), false, `${p} is inert`);
    }
  });

  it('containsPathTraversal detects .. segments only', () => {
    assert.equal(containsPathTraversal('a/../b'), true);
    assert.equal(containsPathTraversal('../b'), true);
    assert.equal(containsPathTraversal('a/..'), true);
    assert.equal(containsPathTraversal('a..b/c'), false); // ".." inside a name is not traversal
    assert.equal(containsPathTraversal('a/b/c'), false);
  });

  it('isUnvalidatableReadGlob is true for a plain glob, false for a traversal glob or an inert path', () => {
    assert.equal(isUnvalidatableReadGlob('subdir/*.txt'), true);
    assert.equal(isUnvalidatableReadGlob('*/passwd'), true);
    // A traversal glob is excluded — it is resolved-and-revalidated instead.
    assert.equal(isUnvalidatableReadGlob('*/../../etc/shadow'), false);
    // Inert paths are not globs.
    assert.equal(isUnvalidatableReadGlob('src/index.ts'), false);
  });
});

describe('classifyReadAccess / isPathAllowed — glob read paths', () => {
  const pm = () => new PermissionManager('ask');

  it('forces a prompt for a plain read glob under the workspace (symlink-in-expansion hole)', () => {
    const glob = path.join(CWD, 'projects', '*', 'passwd');
    // Without the guard this literal prefix-matches CWD and would be auto-allowed.
    assert.equal(pm().classifyReadAccess(glob, CWD), 'prompt');
    assert.equal(pm().isPathAllowed(glob, CWD), false);
  });

  it('forces a prompt for a bare cwd-relative glob', () => {
    assert.equal(pm().classifyReadAccess('logs/*.log', CWD), 'prompt');
    assert.equal(pm().isPathAllowed('logs/*.log', CWD), false);
    assert.equal(pm().classifyReadAccess('report-?.txt', CWD), 'prompt');
    assert.equal(pm().classifyReadAccess('[abc].ts', CWD), 'prompt');
  });

  it('surfaces a glob-specific manual-approval reason (not mislabelled "outside the workspace")', () => {
    const reason = pm().pathExpansionHazardReason(path.join(CWD, 'x', '*.ts'));
    assert.ok(reason, 'a glob read path has an explanatory reason');
    assert.match(reason!, /glob/i);
    assert.match(reason!, /symlink/i);
  });

  it('still ALLOWS a concrete (non-glob) path inside the workspace', () => {
    const concrete = path.join(CWD, 'src', 'index.ts');
    assert.equal(pm().classifyReadAccess(concrete, CWD), 'allow');
    assert.equal(pm().isPathAllowed(concrete, CWD), true);
    assert.equal(pm().pathExpansionHazardReason(concrete), null);
  });

  it('a traversal glob escaping to a forbidden target is DENIED (resolved + revalidated)', () => {
    // `.../*/../../../etc/shadow` collapses to /etc/shadow -> forbidden system path.
    const escape = path.join(CWD, 'a', '*', '..', '..', '..', '..', 'etc', 'shadow');
    assert.equal(pm().classifyReadAccess(escape, CWD), 'deny');
    assert.equal(pm().isPathAllowed(escape, CWD), false);
  });

  it('a forbidden glob base (e.g. /etc/*) is DENIED, not merely prompted', () => {
    assert.equal(pm().classifyReadAccess('/etc/*', CWD), 'deny');
    assert.equal(pm().isPathAllowed('/etc/*', CWD), false);
  });
});
