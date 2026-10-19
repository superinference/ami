/**
 * Tests for the device/special-file guard in file-edit (Gap 11).
 *
 * Verifies that file_edit blocks writes to /dev/*, /proc/self/*, and that
 * the shared isBlockedPath helper (moved to tool-utils) works correctly.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import { isBlockedPath, BLOCKED_PATHS } from '../src/tools/tool-utils';
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-edit-blocked-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// isBlockedPath (unit — from tool-utils)
// ---------------------------------------------------------------------------

describe('isBlockedPath', () => {
  it('blocks /dev/null', () => {
    assert.equal(isBlockedPath('/dev/null'), true);
  });

  it('blocks /dev/zero', () => {
    assert.equal(isBlockedPath('/dev/zero'), true);
  });

  it('blocks /dev/random', () => {
    assert.equal(isBlockedPath('/dev/random'), true);
  });

  it('blocks /dev/urandom', () => {
    assert.equal(isBlockedPath('/dev/urandom'), true);
  });

  it('blocks /dev/stdin', () => {
    assert.equal(isBlockedPath('/dev/stdin'), true);
  });

  it('blocks /dev/stdout', () => {
    assert.equal(isBlockedPath('/dev/stdout'), true);
  });

  it('blocks /dev/stderr', () => {
    assert.equal(isBlockedPath('/dev/stderr'), true);
  });

  it('blocks /dev/fd/3 (prefix match)', () => {
    assert.equal(isBlockedPath('/dev/fd/3'), true);
  });

  it('blocks /dev/tcp/127.0.0.1/80 (prefix match)', () => {
    assert.equal(isBlockedPath('/dev/tcp/127.0.0.1/80'), true);
  });

  it('blocks /dev/udp/127.0.0.1/53 (prefix match)', () => {
    assert.equal(isBlockedPath('/dev/udp/127.0.0.1/53'), true);
  });

  it('blocks /proc/self/fd/0 (prefix match)', () => {
    assert.equal(isBlockedPath('/proc/self/fd/0'), true);
  });

  it('blocks /proc/self/mem', () => {
    assert.equal(isBlockedPath('/proc/self/mem'), true);
  });

  it('does NOT block a normal file path', () => {
    assert.equal(isBlockedPath('/home/user/project/file.ts'), false);
  });

  it('does NOT block /tmp/testfile', () => {
    assert.equal(isBlockedPath('/tmp/testfile'), false);
  });

  it('normalizes Windows backslashes before checking', () => {
    assert.equal(isBlockedPath('\\dev\\null'), true);
  });

  it('BLOCKED_PATHS array is exported and non-empty', () => {
    assert.ok(Array.isArray(BLOCKED_PATHS));
    assert.ok(BLOCKED_PATHS.length > 0);
    assert.ok(BLOCKED_PATHS.includes('/dev/null'));
  });
});

// ---------------------------------------------------------------------------
// file_edit execute() — blocked paths
// ---------------------------------------------------------------------------

describe('fileEditTool – blocked device/special file paths', () => {
  it('returns isError for /dev/null (write mode)', async () => {
    const result = await fileEditTool.execute(
      { file_path: '/dev/null', new_string: 'test content' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('Cannot edit device or special file'),
      `Expected blocked-path error but got: ${result.output}`,
    );
    assert.ok(result.output.includes('/dev/null'));
  });

  it('returns isError for /proc/self/mem (write mode)', async () => {
    const result = await fileEditTool.execute(
      { file_path: '/proc/self/mem', new_string: 'test content' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('Cannot edit device or special file'),
      `Expected blocked-path error but got: ${result.output}`,
    );
    assert.ok(result.output.includes('/proc/self/mem'));
  });

  it('returns isError for /dev/fd/3 (write mode)', async () => {
    const result = await fileEditTool.execute(
      { file_path: '/dev/fd/3', new_string: 'test content' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('Cannot edit device or special file'),
      `Expected blocked-path error but got: ${result.output}`,
    );
  });

  it('returns isError for /dev/zero (edit mode with old_string)', async () => {
    const result = await fileEditTool.execute(
      { file_path: '/dev/zero', old_string: 'old', new_string: 'new' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('Cannot edit device or special file'),
      `Expected blocked-path error but got: ${result.output}`,
    );
  });

  it('returns isError for /dev/tcp/127.0.0.1/80', async () => {
    const result = await fileEditTool.execute(
      { file_path: '/dev/tcp/127.0.0.1/80', new_string: 'test' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('Cannot edit device or special file'),
      `Expected blocked-path error but got: ${result.output}`,
    );
  });

  it('returns isError for /dev/random (sequential edits mode)', async () => {
    const result = await fileEditTool.execute(
      { file_path: '/dev/random', edits: [{ old_string: 'a', new_string: 'b' }] },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('Cannot edit device or special file'),
      `Expected blocked-path error but got: ${result.output}`,
    );
  });

  it('does NOT block a normal file in the workspace', async () => {
    const filePath = path.join(tmpDir, 'normal.txt');
    const result = await fileEditTool.execute(
      { file_path: filePath, new_string: 'hello world' },
      ctx(),
    );
    // Should succeed (not blocked by device guard)
    assert.notEqual(result.output.includes('Cannot edit device or special file'), true);
  });
});

// ---------------------------------------------------------------------------
// UNC path blocking (NTLM credential leak prevention)
// ---------------------------------------------------------------------------

describe('file-edit UNC path blocking', () => {
  it('blocks backslash UNC paths', async () => {
    const result = await fileEditTool.execute!(
      { file_path: '\\\\evil-server\\share\\file.txt', new_string: 'pwned' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('UNC'));
    assert.ok(result.output.includes('NTLM'));
  });

  it('blocks forward-slash UNC paths', async () => {
    const result = await fileEditTool.execute!(
      { file_path: '//evil-server/share/file.txt', new_string: 'pwned' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('UNC'));
  });

  it('allows normal absolute paths', async () => {
    const filePath = path.join(tmpDir, 'normal.txt');
    fs.writeFileSync(filePath, 'hello\n');
    const result = await fileEditTool.execute!(
      { file_path: filePath, old_string: 'hello', new_string: 'world' },
      ctx(),
    );
    assert.equal(result.isError, undefined);
  });
});

// ---------------------------------------------------------------------------
// Symlink guard — blocks writes to symlinks (redirect attack prevention)
// ---------------------------------------------------------------------------

describe('file-edit symlink guard', () => {
  it('blocks editing a symlink target (write mode)', async () => {
    const target = path.join(tmpDir, 'target.txt');
    const link = path.join(tmpDir, 'link.txt');
    fs.writeFileSync(target, 'original content');
    fs.symlinkSync(target, link);
    const result = await fileEditTool.execute!(
      { file_path: link, new_string: 'overwritten' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('symlink'), `Expected symlink error but got: ${result.output}`);
    // Verify original file was NOT overwritten
    assert.equal(fs.readFileSync(target, 'utf-8'), 'original content');
  });

  it('blocks editing a symlink target (edit mode with old_string)', async () => {
    const target = path.join(tmpDir, 'target2.txt');
    const link = path.join(tmpDir, 'link2.txt');
    fs.writeFileSync(target, 'hello world');
    fs.symlinkSync(target, link);
    const result = await fileEditTool.execute!(
      { file_path: link, old_string: 'hello', new_string: 'goodbye' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('symlink'), `Expected symlink error but got: ${result.output}`);
    // Verify original file was NOT modified
    assert.equal(fs.readFileSync(target, 'utf-8'), 'hello world');
  });

  it('blocks editing a symlink target (sequential edits mode)', async () => {
    const target = path.join(tmpDir, 'target3.txt');
    const link = path.join(tmpDir, 'link3.txt');
    fs.writeFileSync(target, 'line one\nline two\n');
    fs.symlinkSync(target, link);
    const result = await fileEditTool.execute!(
      { file_path: link, edits: [{ old_string: 'one', new_string: 'ONE' }] },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('symlink'), `Expected symlink error but got: ${result.output}`);
    // Verify original file was NOT modified
    assert.equal(fs.readFileSync(target, 'utf-8'), 'line one\nline two\n');
  });

  it('allows writing to a regular (non-symlink) file', async () => {
    const filePath = path.join(tmpDir, 'regular.txt');
    fs.writeFileSync(filePath, 'regular content');
    const result = await fileEditTool.execute!(
      { file_path: filePath, old_string: 'regular', new_string: 'modified' },
      ctx(),
    );
    assert.notEqual(result.isError, true);
  });

  it('allows creating a new file (no symlink to check)', async () => {
    const filePath = path.join(tmpDir, 'brand-new.txt');
    const result = await fileEditTool.execute!(
      { file_path: filePath, new_string: 'new file content' },
      ctx(),
    );
    assert.notEqual(result.isError, true);
    assert.ok(fs.existsSync(filePath));
  });
});
