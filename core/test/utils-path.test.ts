import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import { expandPath, isPathSafe, toRelativePath } from '../src/utils/path';

// ---------------------------------------------------------------------------
// expandPath — additional edge cases
// ---------------------------------------------------------------------------

describe('expandPath – additional edge cases', () => {
  it('handles ~/ with trailing slash', () => {
    const result = expandPath('~/', '/tmp');
    assert.equal(result, os.homedir() + path.sep);
  });

  it('handles path starting with ~\\ on posix', () => {
    const result = expandPath('~\\Documents', '/tmp');
    assert.ok(result.includes(os.homedir()));
  });

  it('handles empty relative path', () => {
    const result = expandPath('', '/home/user/project');
    assert.equal(result, path.resolve('/home/user/project', ''));
  });

  it('handles deeply nested relative path', () => {
    const result = expandPath('a/b/c/d/e/f.txt', '/home/user');
    assert.equal(result, path.resolve('/home/user', 'a/b/c/d/e/f.txt'));
  });

  it('handles .. traversal', () => {
    const result = expandPath('../sibling/file.txt', '/home/user/project');
    assert.equal(result, path.resolve('/home/user/sibling/file.txt'));
  });

  it('normalizes double separators', () => {
    const result = expandPath('/home//user///project', '/tmp');
    assert.equal(result, path.normalize('/home/user/project'));
  });
});

// ---------------------------------------------------------------------------
// isPathSafe — additional edge cases
// ---------------------------------------------------------------------------

describe('isPathSafe – additional edge cases', () => {
  it('path equal to cwd is safe', () => {
    assert.ok(isPathSafe('/home/user/project', '/home/user/project'));
  });

  it('child of cwd is safe', () => {
    assert.ok(isPathSafe('/home/user/project/src/file.ts', '/home/user/project'));
  });

  it('sibling of cwd (outside cwd but inside home) is safe', () => {
    const home = os.homedir();
    const cwd = path.join(home, 'project1');
    const sibling = path.join(home, 'project2', 'file.txt');
    assert.ok(isPathSafe(sibling, cwd));
  });

  it('system path outside cwd and home is unsafe', () => {
    const home = os.homedir();
    if (!'/etc'.startsWith(home)) {
      assert.equal(isPathSafe('/etc/shadow', '/opt/myapp'), false);
    }
  });
});

// ---------------------------------------------------------------------------
// toRelativePath — additional edge cases
// ---------------------------------------------------------------------------

describe('toRelativePath – additional edge cases', () => {
  it('path inside cwd returns relative', () => {
    const result = toRelativePath('/home/user/project/src/app.ts', '/home/user/project');
    assert.equal(result, path.join('src', 'app.ts'));
  });

  it('path that is cwd returns empty string', () => {
    const result = toRelativePath('/home/user/project', '/home/user/project');
    assert.equal(result, '');
  });

  it('path outside cwd returns absolute', () => {
    const home = os.homedir();
    if (!'/var/log'.startsWith(home)) {
      const result = toRelativePath('/var/log/syslog', '/opt/project');
      assert.ok(path.isAbsolute(result));
    }
  });
});
