import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { isGrepNoMatchExit } from '../src/tools/grep';
import { grepTool } from '../src/tools/grep';

describe('isGrepNoMatchExit', () => {
  it('returns true for exit code 1 (no matches)', () => {
    assert.equal(isGrepNoMatchExit(1), true);
  });

  it('returns false for exit code 0 (matches found)', () => {
    assert.equal(isGrepNoMatchExit(0), false);
  });

  it('returns false for exit code 2 (permission/regex error)', () => {
    assert.equal(isGrepNoMatchExit(2), false);
  });

  it('returns false for exit code 127 (command not found)', () => {
    assert.equal(isGrepNoMatchExit(127), false);
  });

  it('returns false for exit code 128 (signal base)', () => {
    assert.equal(isGrepNoMatchExit(128), false);
  });

  it('returns false for exit code 137 (killed by SIGKILL)', () => {
    assert.equal(isGrepNoMatchExit(137), false);
  });

  it('returns false for negative exit codes', () => {
    assert.equal(isGrepNoMatchExit(-1), false);
  });
});

describe('grep tool error reporting', () => {
  let tmpDir: string;
  const ctx = () => ({ cwd: tmpDir, abortSignal: new AbortController().signal });

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-err-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns no matches for a pattern that does not appear', async () => {
    fs.writeFileSync(path.join(tmpDir, 'test.txt'), 'hello world\n');
    const result = await grepTool.execute(
      { pattern: 'zzz_nonexistent_zzz', path: tmpDir },
      ctx(),
    );
    assert.ok(result.output.includes('No matches found'));
    assert.equal(result.isError, undefined);
  });

  it('returns matches for a pattern that does appear', async () => {
    fs.writeFileSync(path.join(tmpDir, 'test.txt'), 'hello world\nfoo bar\n');
    const result = await grepTool.execute(
      { pattern: 'hello', path: tmpDir },
      ctx(),
    );
    assert.ok(result.output.includes('hello'));
    assert.equal(result.isError, undefined);
  });

  it('reports error for invalid regex pattern (if rg/grep both fail)', async () => {
    fs.writeFileSync(path.join(tmpDir, 'test.txt'), 'hello\n');
    const result = await grepTool.execute(
      { pattern: '(unclosed', path: tmpDir },
      ctx(),
    );
    assert.ok(
      result.isError === true || result.output.includes('Error'),
      `Expected error for invalid regex, got: ${result.output.slice(0, 200)}`,
    );
  });

  it('returns error when pattern is empty', async () => {
    const result = await grepTool.execute(
      { pattern: '', path: tmpDir },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must not be empty') || result.output.includes('pattern is required'));
  });
});
