import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';

import { expandPath, isPathSafe, toRelativePath } from '../src/utils/path';
import { estimateTokens, truncateToTokenLimit } from '../src/utils/tokens';

// ---------------------------------------------------------------------------
// expandPath
// ---------------------------------------------------------------------------
describe('expandPath', () => {
  const cwd = '/home/user/project';

  it('expands ~ to home directory', () => {
    const result = expandPath('~/Documents/file.txt', cwd);
    assert.equal(result, path.join(os.homedir(), 'Documents/file.txt'));
  });

  it('expands bare ~ to home directory', () => {
    const result = expandPath('~', cwd);
    assert.equal(result, os.homedir());
  });

  it('resolves relative paths against cwd', () => {
    const result = expandPath('src/main.ts', cwd);
    assert.equal(result, path.resolve(cwd, 'src/main.ts'));
  });

  it('leaves absolute paths unchanged (normalized)', () => {
    const result = expandPath('/usr/bin/node', cwd);
    assert.equal(result, '/usr/bin/node');
  });

  it('normalizes paths with ..', () => {
    const result = expandPath('src/../lib/util.ts', cwd);
    assert.equal(result, path.resolve(cwd, 'lib/util.ts'));
  });

  it('normalizes paths with redundant separators', () => {
    const result = expandPath('/usr//bin///node', cwd);
    assert.equal(result, '/usr/bin/node');
  });

  it('handles . as relative path', () => {
    const result = expandPath('.', cwd);
    assert.equal(result, path.normalize(cwd));
  });

  it('handles ./file relative paths', () => {
    const result = expandPath('./file.txt', cwd);
    assert.equal(result, path.resolve(cwd, 'file.txt'));
  });
});

// ---------------------------------------------------------------------------
// isPathSafe
// ---------------------------------------------------------------------------
describe('isPathSafe', () => {
  const cwd = '/home/user/project';

  it('returns true for paths inside cwd', () => {
    assert.ok(isPathSafe('src/main.ts', cwd));
    assert.ok(isPathSafe('/home/user/project/src/main.ts', cwd));
  });

  it('returns true for the cwd itself', () => {
    assert.ok(isPathSafe(cwd, cwd));
  });

  it('returns true for paths inside home directory', () => {
    assert.ok(isPathSafe('~/.config/app.json', cwd));
    assert.ok(isPathSafe(path.join(os.homedir(), '.bashrc'), cwd));
  });

  it('returns false for paths outside cwd and home', () => {
    // Use a cwd that is not under home directory to test this properly
    const restrictedCwd = '/opt/restricted/project';
    // This path is outside both /opt/restricted/project and the home dir
    const outsidePath = '/etc/passwd';
    // Only fails if /etc/passwd is not a descendant of homedir or cwd
    const homedir = os.homedir();
    if (!outsidePath.startsWith(homedir)) {
      assert.equal(isPathSafe(outsidePath, restrictedCwd), false);
    }
  });

  it('prevents path traversal attacks', () => {
    const restrictedCwd = '/opt/restricted/project';
    const homedir = os.homedir();
    const traversal = '/opt/restricted/project/../../etc/passwd';
    const resolved = path.resolve(traversal);
    // Only test if resolved is truly outside both boundaries
    if (!resolved.startsWith(restrictedCwd) && !resolved.startsWith(homedir)) {
      assert.equal(isPathSafe(traversal, restrictedCwd), false);
    }
  });

  it('does not confuse similar directory prefixes', () => {
    // /home/user2 should NOT be treated as inside /home/user
    const homedir = os.homedir();
    const similarPath = homedir + '2/secret.txt';
    // This path must not be inside homedir
    const restrictedCwd = '/nonexistent/cwd';
    assert.equal(isPathSafe(similarPath, restrictedCwd), false);
  });
});

// ---------------------------------------------------------------------------
// toRelativePath
// ---------------------------------------------------------------------------
describe('toRelativePath', () => {
  const cwd = '/home/user/project';

  it('converts absolute path inside cwd to relative', () => {
    const result = toRelativePath('/home/user/project/src/main.ts', cwd);
    assert.equal(result, path.join('src', 'main.ts'));
  });

  it('returns cwd as empty string', () => {
    const result = toRelativePath('/home/user/project', cwd);
    assert.equal(result, '');
  });

  it('returns absolute path unchanged when outside cwd', () => {
    const result = toRelativePath('/etc/config.json', cwd);
    // Should be the full absolute path since it's outside cwd
    assert.ok(path.isAbsolute(result));
    assert.ok(result.includes('etc'));
  });

  it('handles relative input paths (resolves against cwd first)', () => {
    const result = toRelativePath('src/main.ts', cwd);
    assert.equal(result, path.join('src', 'main.ts'));
  });

  it('handles ~ paths', () => {
    const result = toRelativePath('~/.config/app.json', cwd);
    // If home dir is inside cwd, it should be relative; otherwise absolute
    const resolved = expandPath('~/.config/app.json', cwd);
    if (resolved.startsWith(path.normalize(cwd))) {
      assert.ok(!path.isAbsolute(result));
    } else {
      assert.ok(path.isAbsolute(result));
    }
  });
});

// ---------------------------------------------------------------------------
// estimateTokens
// ---------------------------------------------------------------------------
describe('estimateTokens', () => {
  it('returns 0 for empty string', () => {
    assert.equal(estimateTokens(''), 0);
  });

  it('estimates tokens based on ~4 chars per token', () => {
    // 20 characters should be ~5 tokens
    assert.equal(estimateTokens('12345678901234567890'), 5);
  });

  it('rounds up for partial tokens', () => {
    // 1 character should be 1 token (ceil(1/4))
    assert.equal(estimateTokens('a'), 1);
    // 5 characters should be 2 tokens (ceil(5/4))
    assert.equal(estimateTokens('abcde'), 2);
  });

  it('handles longer text', () => {
    const text = 'a'.repeat(400); // 400 chars = 100 tokens
    assert.equal(estimateTokens(text), 100);
  });
});

// ---------------------------------------------------------------------------
// truncateToTokenLimit
// ---------------------------------------------------------------------------
describe('truncateToTokenLimit', () => {
  it('returns text unchanged when within limit', () => {
    const text = 'Hello world'; // ~3 tokens
    assert.equal(truncateToTokenLimit(text, 100), text);
  });

  it('truncates text that exceeds the token limit', () => {
    const text = 'a'.repeat(1000); // 250 tokens
    const result = truncateToTokenLimit(text, 10); // max 10 tokens = ~40 chars
    assert.ok(result.length < text.length);
    assert.ok(result.includes('[truncated]'));
  });

  it('returns [truncated] for maxTokens <= 0', () => {
    assert.equal(truncateToTokenLimit('anything', 0), '[truncated]');
    assert.equal(truncateToTokenLimit('anything', -1), '[truncated]');
  });

  it('preserves beginning of text when truncating', () => {
    const text = 'START_MARKER' + 'x'.repeat(1000);
    const result = truncateToTokenLimit(text, 20);
    assert.ok(result.startsWith('START_MARKER'));
    assert.ok(result.includes('[truncated]'));
  });

  it('does not truncate text exactly at the limit', () => {
    // 40 chars = 10 tokens exactly
    const text = 'a'.repeat(40);
    assert.equal(truncateToTokenLimit(text, 10), text);
  });

  it('handles empty text', () => {
    assert.equal(truncateToTokenLimit('', 100), '');
  });
});
