import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

function makeContext(cwd: string): ToolContext {
  return {
    cwd,
    abortSignal: new AbortController().signal,
    readFileTimestamps: new Map(),
    filesRead: new Set(),
    setToolResult: () => {},
    tokenTracker: { addTokens: () => {}, getTokens: () => ({ input: 0, output: 0, cacheCreation: 0, cacheRead: 0 }) } as any,
  };
}

describe('file_edit — directory target guard', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dir-guard-'));
    fs.mkdirSync(path.join(tmpDir, 'subdir'));
    fs.writeFileSync(path.join(tmpDir, 'existing.txt'), 'hello world');
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns error when Write target is an existing directory', async () => {
    const ctx = makeContext(tmpDir);
    const result = await fileEditTool.execute!(
      { file_path: path.join(tmpDir, 'subdir'), new_string: 'hello' },
      ctx,
    );
    assert.ok(result.isError, `Expected error but got: ${result.output}`);
    assert.ok(result.output.includes('is a directory'), `Expected "is a directory" but got: ${result.output}`);
    assert.ok(result.output.includes('not a file'));
  });

  it('returns error when Edit target is an existing directory', async () => {
    const ctx = makeContext(tmpDir);
    const result = await fileEditTool.execute!(
      { file_path: path.join(tmpDir, 'subdir'), old_string: 'foo', new_string: 'bar' },
      ctx,
    );
    assert.ok(result.isError, `Expected error but got: ${result.output}`);
    assert.ok(result.output.includes('is a directory'));
  });

  it('returns error when sequential edits target is an existing directory', async () => {
    const ctx = makeContext(tmpDir);
    const result = await fileEditTool.execute!(
      { file_path: path.join(tmpDir, 'subdir'), edits: [{ old_string: 'a', new_string: 'b' }] },
      ctx,
    );
    assert.ok(result.isError, `Expected error but got: ${result.output}`);
    assert.ok(result.output.includes('is a directory'));
  });

  it('allows writing to a new file path', async () => {
    const ctx = makeContext(tmpDir);
    const target = path.join(tmpDir, 'newfile.txt');
    const result = await fileEditTool.execute!(
      { file_path: target, new_string: 'hello world' },
      ctx,
    );
    assert.ok(!result.isError, `Expected no error but got: ${result.output}`);
    assert.equal(fs.readFileSync(target, 'utf-8'), 'hello world');
  });

  it('allows editing an existing regular file', async () => {
    const existing = path.join(tmpDir, 'existing.txt');
    const ctx = makeContext(tmpDir);
    ctx.filesRead!.add(existing);
    const result = await fileEditTool.execute!(
      { file_path: existing, old_string: 'hello world', new_string: 'goodbye world' },
      ctx,
    );
    assert.ok(!result.isError, `Expected no error but got: ${result.output}`);
    assert.equal(fs.readFileSync(existing, 'utf-8'), 'goodbye world');
  });
});
