import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { globTool } from '../src/tools/glob';
import type { ToolContext } from '../src/types';

describe('globTool', () => {
  let tmpDir: string;
  let ctx: ToolContext;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'glob-test-'));
    ctx = { cwd: tmpDir } as ToolContext;
    fs.writeFileSync(path.join(tmpDir, 'file1.ts'), 'export const a = 1;');
    fs.writeFileSync(path.join(tmpDir, 'file2.ts'), 'export const b = 2;');
    fs.writeFileSync(path.join(tmpDir, 'readme.md'), '# Readme');
    fs.mkdirSync(path.join(tmpDir, 'sub'));
    fs.writeFileSync(path.join(tmpDir, 'sub', 'nested.ts'), 'export const c = 3;');
  });

  after(() => { fs.rmSync(tmpDir, { recursive: true }); });

  it('has correct name and schema', () => {
    assert.equal(globTool.name, 'glob');
    assert.ok(globTool.isReadOnly);
    assert.ok(globTool.isConcurrencySafe);
  });

  it('finds .ts files with glob pattern', async () => {
    const result = await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    assert.ok(result.output.includes('file1.ts'));
    assert.ok(result.output.includes('file2.ts'));
    assert.ok(result.output.includes('nested.ts'));
  });

  it('returns no matches for unmatched pattern', async () => {
    const result = await globTool.execute!({ pattern: '**/*.py' }, ctx);
    assert.ok(result.output.includes('No files found'));
  });

  it('finds specific file', async () => {
    const result = await globTool.execute!({ pattern: 'readme.md' }, ctx);
    assert.ok(result.output.includes('readme.md'));
    assert.ok(result.output.includes('1 file'));
  });

  it('respects path parameter', async () => {
    const result = await globTool.execute!({ pattern: '*.ts', path: path.join(tmpDir, 'sub') }, ctx);
    assert.ok(result.output.includes('nested.ts'));
    assert.ok(!result.output.includes('file1.ts'));
  });

  it('returns error for invalid path', async () => {
    const result = await globTool.execute!({ pattern: '*.ts', path: '/nonexistent/dir/xyz' }, ctx);
    assert.ok(result.output.includes('No files found') || result.isError);
  });
});
