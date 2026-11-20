import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import { shortestDisplayPath } from '../src/utils/path';

// ===========================================================================
// shortestDisplayPath — parity with the reference's getRelativeMemoryPath
// (src/components/memory/MemoryUpdateNotification.tsx L7-L20).
//
// Claude computes BOTH a home-relative form ('~' + slice) and a cwd-relative
// form ('./' + relative), then returns whichever STRING IS SHORTER (home wins
// ties), falling back to whichever single form applies, else the absolute path.
//
// This is a distinct display POLICY from displayPath() (which always prefers the
// cwd-relative form and renders cwd as '.'): shortestDisplayPath picks the more
// compact of the two representations. RED: with shortestDisplayPath absent the
// import fails; GREEN: the assertions below hold.
// ===========================================================================

describe('shortestDisplayPath — shorter of ~home vs ./cwd', () => {
  it('returns the cwd form when it is shorter than the home form', () => {
    const home = '/home/user';
    const cwd = '/home/user/deeply/nested/project';
    // resolved lives inside cwd; cwd form './a.ts' is shorter than
    // home form '~/deeply/nested/project/a.ts'.
    assert.equal(shortestDisplayPath(`${cwd}/a.ts`, cwd, home), './a.ts');
  });

  it('returns the home form when it is shorter than the cwd form', () => {
    const home = '/home/user';
    const cwd = '/home/user/a/b/c/d/e';
    // A file under home but outside cwd => only the home form applies.
    const target = '/home/user/notes.md';
    assert.equal(shortestDisplayPath(target, cwd, home), `~${path.sep}notes.md`);
  });

  it('breaks ties in favor of the home form (<=)', () => {
    const home = '/home/user';
    // Construct a case where both forms are equal length.
    // home form:  '~/x/y'        (len 5)
    // cwd form:   './x/y'  with cwd = home  => also '~/x/y' vs './x/y' both len 5
    const cwd = home;
    const target = '/home/user/x/y';
    // home => '~' + '/x/y' = '~/x/y' (len 5); cwd => './' + 'x/y' = './x/y' (len 5)
    assert.equal(shortestDisplayPath(target, cwd, home), `~${path.sep}x/y`);
  });

  it('renders the home directory itself as "~"', () => {
    const home = '/home/user';
    const cwd = '/tmp/elsewhere';
    assert.equal(shortestDisplayPath(home, cwd, home), '~');
  });

  it('renders the cwd itself as "./"', () => {
    const home = '/opt';
    const cwd = '/srv/app';
    assert.equal(shortestDisplayPath(cwd, cwd, home), './');
  });

  it('falls back to the absolute path when neither home nor cwd applies', () => {
    const home = '/home/user';
    const cwd = '/home/user/project';
    const target = '/var/log/system.log';
    assert.equal(shortestDisplayPath(target, cwd, home), target);
  });

  it('does not treat a sibling prefix dir as inside the boundary', () => {
    // '/home/user2' must NOT be considered inside '/home/user' (raw startsWith
    // would wrongly match). Superset improvement over the reference's raw prefix check.
    const home = '/home/user';
    const cwd = '/home/user/project';
    assert.equal(shortestDisplayPath('/home/user2/x', cwd, home), '/home/user2/x');
  });

  it('expands ~ and relative inputs before choosing a form', () => {
    const home = os.homedir();
    const cwd = path.join(home, 'proj');
    // '~/proj/file.ts' -> inside cwd => cwd form './file.ts' (shorter).
    assert.equal(shortestDisplayPath('~/proj/file.ts', cwd, home), './file.ts');
  });
});
