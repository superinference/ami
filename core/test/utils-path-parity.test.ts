import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import {
  expandPath,
  isPathSafe,
  toRelativePath,
  isPathWithin,
  abbreviateHome,
  displayPath,
  stripTrailingSep,
} from '../src/utils/path';

// ===========================================================================
// Core-parity additions for utils/path.ts.
//
// Each block below targets a specific functional gap vs. Claude Code's path
// utilities and is RED-GREEN validated: with the corresponding implementation
// reverted/stubbed the assertion fails; with it in place it passes.
// ===========================================================================

// ---------------------------------------------------------------------------
// GAP 1: containment must tolerate a trailing separator on the boundary.
// RED: revert isDescendant to compare against `parent` directly (no
//      stripTrailingSep) -> these assertions fail.
// ---------------------------------------------------------------------------
describe('containment — trailing-separator hardening', () => {
  it('isPathSafe treats a cwd with a trailing slash the same as without', () => {
    // Before the fix, "/home/user/project" is NOT a descendant of
    // "/home/user/project/" and (if outside home) would be flagged unsafe.
    assert.equal(isPathSafe('/opt/app', '/opt/app/'), true);
    assert.equal(isPathSafe('/opt/app', '/opt/app'), true);
  });

  it('toRelativePath resolves to empty for cwd itself even with trailing slash', () => {
    assert.equal(toRelativePath('/opt/app', '/opt/app/'), '');
  });

  it('a true sibling is still rejected (no prefix false-positive)', () => {
    const home = os.homedir();
    if (!'/opt/app2'.startsWith(home)) {
      assert.equal(isPathSafe('/opt/app2', '/opt/app'), false);
    }
  });
});

// ---------------------------------------------------------------------------
// GAP 2: stripTrailingSep helper.
// RED: remove/stub stripTrailingSep -> fails to import or returns wrong value.
// ---------------------------------------------------------------------------
describe('stripTrailingSep', () => {
  it('removes a single trailing separator', () => {
    assert.equal(stripTrailingSep('/a/b/'), '/a/b');
  });

  it('leaves a path without a trailing separator unchanged', () => {
    assert.equal(stripTrailingSep('/a/b'), '/a/b');
  });

  it('never strips the root separator', () => {
    assert.equal(stripTrailingSep('/'), '/');
  });

  it('handles empty string', () => {
    assert.equal(stripTrailingSep(''), '');
  });
});

// ---------------------------------------------------------------------------
// GAP 3: isPathWithin — public containment primitive accepting raw inputs.
// RED: remove isPathWithin -> import fails; stub returning true -> negative
//      cases fail.
// ---------------------------------------------------------------------------
describe('isPathWithin', () => {
  it('reports a nested path as within', () => {
    assert.equal(isPathWithin('/opt/app/src/x.ts', '/opt/app'), true);
  });

  it('reports the parent itself as within', () => {
    assert.equal(isPathWithin('/opt/app', '/opt/app'), true);
  });

  it('rejects a sibling that merely shares a prefix', () => {
    assert.equal(isPathWithin('/opt/app2', '/opt/app'), false);
  });

  it('expands tilde and relative inputs against cwd before checking', () => {
    const home = os.homedir();
    assert.equal(isPathWithin('~/sub/file', '~', '/tmp'), true);
    assert.equal(isPathWithin('sub/file', '.', path.join(home, 'proj')), true);
  });

  it('tolerates a trailing separator on the parent', () => {
    assert.equal(isPathWithin('/opt/app/x', '/opt/app/', '/tmp'), true);
  });
});

// ---------------------------------------------------------------------------
// GAP 4: abbreviateHome — tilde-display abbreviation.
// RED: remove abbreviateHome -> import fails; stub returning input unchanged
//      -> the "~/..." assertions fail.
// ---------------------------------------------------------------------------
describe('abbreviateHome', () => {
  it('abbreviates a path under home to tilde form', () => {
    const home = os.homedir();
    const full = path.join(home, 'docs', 'a.txt');
    assert.equal(abbreviateHome(full), '~' + path.sep + path.join('docs', 'a.txt'));
  });

  it('abbreviates the home directory itself to ~', () => {
    assert.equal(abbreviateHome(os.homedir()), '~');
  });

  it('leaves a path outside home unchanged', () => {
    const home = os.homedir();
    if (!'/opt/app'.startsWith(home)) {
      assert.equal(abbreviateHome('/opt/app'), '/opt/app');
    }
  });

  it('does not abbreviate a sibling of home that shares a prefix', () => {
    // e.g. home "/home/user" must not swallow "/home/user2".
    const fakeHome = '/home/user';
    assert.equal(abbreviateHome('/home/user2/file', fakeHome), '/home/user2/file');
  });

  it('accepts an explicit home argument', () => {
    assert.equal(abbreviateHome('/srv/me/x', '/srv/me'), '~' + path.sep + 'x');
  });
});

// ---------------------------------------------------------------------------
// GAP 5: displayPath — workspace-relative display.
// RED: remove displayPath -> import fails; stub returning input -> assertions
//      fail.
// ---------------------------------------------------------------------------
describe('displayPath', () => {
  it('renders a path inside cwd as a relative path', () => {
    assert.equal(
      displayPath('/opt/app/src/app.ts', '/opt/app'),
      path.join('src', 'app.ts'),
    );
  });

  it('renders the cwd itself as "."', () => {
    assert.equal(displayPath('/opt/app', '/opt/app'), '.');
  });

  it('renders a path outside cwd but inside home with a tilde', () => {
    const home = os.homedir();
    const cwd = path.join(home, 'proj');
    const target = path.join(home, 'other', 'f.ts');
    assert.equal(displayPath(target, cwd), '~' + path.sep + path.join('other', 'f.ts'));
  });

  it('renders a path outside both cwd and home as absolute', () => {
    const home = os.homedir();
    if (!'/var/data/x'.startsWith(home) && !'/var/data/x'.startsWith('/opt/app')) {
      const result = displayPath('/var/data/x', '/opt/app', home);
      assert.ok(path.isAbsolute(result));
      assert.equal(result, path.normalize('/var/data/x'));
    }
  });

  it('expands tilde/relative inputs before display', () => {
    const home = os.homedir();
    assert.equal(displayPath('~/proj/a.ts', path.join(home, 'proj')), 'a.ts');
  });
});

// ---------------------------------------------------------------------------
// Regression: existing behavior preserved (expandPath unchanged).
// ---------------------------------------------------------------------------
describe('expandPath — preserved behavior', () => {
  it('still expands a leading tilde', () => {
    assert.equal(expandPath('~/x', '/tmp'), path.normalize(path.join(os.homedir(), 'x')));
  });

  it('still resolves relative paths against cwd', () => {
    assert.equal(expandPath('a/b', '/home/u'), path.resolve('/home/u', 'a/b'));
  });
});
