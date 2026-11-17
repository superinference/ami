import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { listDirTool } from '../src/tools/list-dir';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-list-dir-'));
  fs.writeFileSync(path.join(tmpDir, 'file1.txt'), '');
  fs.writeFileSync(path.join(tmpDir, 'file2.ts'), '');
  fs.mkdirSync(path.join(tmpDir, 'subdir'));
  fs.mkdirSync(path.join(tmpDir, 'another'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('listDirTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(listDirTool.name, 'list_dir');
  });

  it('is read-only and concurrency-safe', () => {
    assert.equal(listDirTool.isReadOnly, true);
    assert.equal(listDirTool.isConcurrencySafe, true);
  });

  it('schema requires path', () => {
    assert.ok(listDirTool.inputSchema.required?.includes('path'));
  });

  it('description gives when-to-use guidance and notes the entry cap', () => {
    const d = listDirTool.description.toLowerCase();
    // When-to-use affordance: steer the model toward glob/grep for the other jobs.
    assert.ok(d.includes('glob'), 'should mention glob as an alternative');
    assert.ok(d.includes('grep'), 'should mention grep as an alternative');
    // Truncation note: the 1000-entry cap must be disclosed in the description.
    assert.ok(d.includes('1000'), 'should note the 1000-entry truncation cap');
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe('listDirTool – validation', () => {
  it('rejects empty path', async () => {
    const result = await listDirTool.execute({ path: '' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must not be empty'));
  });

  it('rejects whitespace-only path', async () => {
    const result = await listDirTool.execute({ path: '   ' }, ctx());
    assert.equal(result.isError, true);
  });
});

// ---------------------------------------------------------------------------
// Listing
// ---------------------------------------------------------------------------

describe('listDirTool – listing', () => {
  it('lists directories and files', async () => {
    const result = await listDirTool.execute({ path: tmpDir }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('[dir]'));
    assert.ok(result.output.includes('[file]'));
    assert.ok(result.output.includes('subdir/'));
    assert.ok(result.output.includes('another/'));
    assert.ok(result.output.includes('file1.txt'));
    assert.ok(result.output.includes('file2.ts'));
  });

  it('shows directories before files', async () => {
    const result = await listDirTool.execute({ path: tmpDir }, ctx());
    const lines = result.output.split('\n').filter(l => l.includes('['));
    const firstFileIdx = lines.findIndex(l => l.includes('[file]'));
    const lastDirIdx = lines.length - 1 - [...lines].reverse().findIndex(l => l.includes('[dir]'));
    assert.ok(lastDirIdx < firstFileIdx, 'Directories should appear before files');
  });

  it('sorts entries alphabetically within groups', async () => {
    const result = await listDirTool.execute({ path: tmpDir }, ctx());
    const lines = result.output.split('\n').filter(l => l.includes('[dir]'));
    // another/ should come before subdir/
    const anotherIdx = lines.findIndex(l => l.includes('another'));
    const subdirIdx = lines.findIndex(l => l.includes('subdir'));
    assert.ok(anotherIdx < subdirIdx, 'another/ should be before subdir/');
  });

  it('shows entry count', async () => {
    const result = await listDirTool.execute({ path: tmpDir }, ctx());
    assert.ok(result.output.includes('4 entries'));
  });

  it('handles empty directories', async () => {
    const emptyDir = path.join(tmpDir, 'empty');
    fs.mkdirSync(emptyDir);
    const result = await listDirTool.execute({ path: emptyDir }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('0 entries'));
  });
});

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

describe('listDirTool – error handling', () => {
  it('returns error for non-existent directory', async () => {
    const result = await listDirTool.execute({ path: path.join(tmpDir, 'nonexistent-subdir') }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
  });

  it('returns error when path is a file', async () => {
    const file = path.join(tmpDir, 'file1.txt');
    const result = await listDirTool.execute({ path: file }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not a directory'));
  });
});

// ---------------------------------------------------------------------------
// Cancellation / abort support
// ---------------------------------------------------------------------------

describe('listDirTool – cancellation', () => {
  it('returns a cancelled error when the abort signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    const result = await listDirTool.execute(
      { path: tmpDir },
      ctx({ abortSignal: controller.signal }),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.toLowerCase().includes('cancel'));
  });

  it('short-circuits before touching the filesystem when aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    // Point at a non-existent dir: if the abort guard did not fire first we would
    // instead get the "not found" error, so this discriminates the fix.
    const result = await listDirTool.execute(
      { path: path.join(tmpDir, 'does-not-exist') },
      ctx({ abortSignal: controller.signal }),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.toLowerCase().includes('cancel'));
    assert.ok(!result.output.includes('not found'));
  });

  it('lists normally when the signal is not aborted', async () => {
    const controller = new AbortController();
    const result = await listDirTool.execute(
      { path: tmpDir },
      ctx({ abortSignal: controller.signal }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('4 entries'));
  });
});

// ---------------------------------------------------------------------------
// UNC path guard (NTLM credential-leak hardening)
// ---------------------------------------------------------------------------

describe('listDirTool – UNC path guard', () => {
  it('rejects backslash UNC paths before any filesystem access', async () => {
    const result = await listDirTool.execute({ path: '\\\\attacker\\share' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('UNC'));
    assert.ok(result.output.includes('NTLM'));
  });

  it('rejects double-slash UNC paths', async () => {
    const result = await listDirTool.execute({ path: '//attacker/share' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('UNC'));
  });
});

// ---------------------------------------------------------------------------
// Read-deny filtering (do not leak the names of permission-denied files)
// ---------------------------------------------------------------------------

describe('listDirTool – read-deny filtering', () => {
  it('omits entries whose basename matches a read-deny glob', async () => {
    fs.writeFileSync(path.join(tmpDir, '.env'), 'SECRET=1');
    const result = await listDirTool.execute(
      { path: tmpDir },
      ctx({ readIgnorePatterns: ['.env'] }),
    );
    assert.ok(!result.isError);
    assert.ok(!result.output.includes('.env'), 'denied file name must not leak');
    // The non-denied entries are still present.
    assert.ok(result.output.includes('file1.txt'));
    // The reported count reflects only the surfaced (non-denied) entries.
    assert.ok(result.output.includes('4 entries'));
  });

  it('omits directory entries matching an any-depth deny glob', async () => {
    fs.mkdirSync(path.join(tmpDir, 'secrets'));
    const result = await listDirTool.execute(
      { path: tmpDir },
      ctx({ readIgnorePatterns: ['**/secrets'] }),
    );
    assert.ok(!result.isError);
    assert.ok(!result.output.includes('secrets/'), 'denied dir name must not leak');
    assert.ok(result.output.includes('subdir/'));
  });

  it('matches deny globs anchored at the cwd-relative path', async () => {
    const nested = path.join(tmpDir, 'subdir');
    fs.writeFileSync(path.join(nested, 'token.key'), 'x');
    const result = await listDirTool.execute(
      { path: nested },
      // Pattern anchored by relative path from cwd (subdir/token.key).
      ctx({ readIgnorePatterns: ['subdir/*.key'] }),
    );
    assert.ok(!result.isError);
    assert.ok(!result.output.includes('token.key'), 'relative-anchored deny must apply');
  });

  it('lists everything when no deny patterns are configured', async () => {
    fs.writeFileSync(path.join(tmpDir, '.env'), 'SECRET=1');
    const result = await listDirTool.execute({ path: tmpDir }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('.env'), 'no deny rules → nothing filtered');
  });
});

// ---------------------------------------------------------------------------
// Relative paths
// ---------------------------------------------------------------------------

describe('listDirTool – relative paths', () => {
  it('resolves relative paths against cwd', async () => {
    const result = await listDirTool.execute({ path: 'subdir' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('0 entries'));
  });
});

// ---------------------------------------------------------------------------
// Workspace boundary enforcement
// ---------------------------------------------------------------------------

describe('listDirTool – workspace boundary', () => {
  it('rejects path outside workspace', async () => {
    const result = await listDirTool.execute({ path: '/tmp' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('outside'));
  });

  it('rejects symlink escaping workspace', async () => {
    const outsideDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-outside-'));
    try {
      const symPath = path.join(tmpDir, 'escape-link');
      try {
        fs.symlinkSync(outsideDir, symPath);
      } catch {
        return;
      }
      const result = await listDirTool.execute({ path: symPath }, ctx());
      assert.equal(result.isError, true);
      assert.ok(result.output.includes('symlink') || result.output.includes('outside'));
    } finally {
      fs.rmSync(outsideDir, { recursive: true, force: true });
    }
  });
});
