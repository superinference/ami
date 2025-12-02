import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { multiEditTool } from '../src/tools/multi-edit';

describe('multi_edit tool', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-multi-edit-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  const ctx = () => ({ cwd: tmpDir, abortSignal: new AbortController().signal });

  it('applies multiple edits to a file', async () => {
    const file = path.join(tmpDir, 'test.ts');
    fs.writeFileSync(file, 'const a = 1;\nconst b = 2;\nconst c = 3;\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [
        { old_string: 'const a = 1;', new_string: 'const a = 10;' },
        { old_string: 'const c = 3;', new_string: 'const c = 30;' },
      ],
    }, ctx());

    assert.ok(!result.isError);
    assert.ok(result.output.includes('2/2 edits applied'));
    const content = fs.readFileSync(file, 'utf-8');
    assert.ok(content.includes('const a = 10;'));
    assert.ok(content.includes('const b = 2;'));
    assert.ok(content.includes('const c = 30;'));
  });

  it('fails when old_string not found', async () => {
    const file = path.join(tmpDir, 'test.ts');
    fs.writeFileSync(file, 'hello world\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [
        { old_string: 'nonexistent', new_string: 'replacement' },
      ],
    }, ctx());

    assert.ok(result.isError);
    assert.ok(result.output.includes('not found'));
  });

  it('fails when old_string matches multiple locations', async () => {
    const file = path.join(tmpDir, 'test.ts');
    fs.writeFileSync(file, 'foo\nbar\nfoo\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [
        { old_string: 'foo', new_string: 'baz' },
      ],
    }, ctx());

    assert.ok(result.isError);
    assert.ok(result.output.includes('found') && result.output.includes('times'), `Expected error about multiple matches, got: ${result.output.slice(0, 200)}`);
  });

  it('reports partial success', async () => {
    const file = path.join(tmpDir, 'test.ts');
    fs.writeFileSync(file, 'const a = 1;\nconst b = 2;\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [
        { old_string: 'const a = 1;', new_string: 'const a = 10;' },
        { old_string: 'nonexistent', new_string: 'nope' },
      ],
    }, ctx());

    assert.ok(!result.isError);
    assert.ok(result.output.includes('1/2 edits applied'));
    assert.ok(result.output.includes('Failed edits'));
  });

  it('rejects empty edits array', async () => {
    const file = path.join(tmpDir, 'test.ts');
    fs.writeFileSync(file, 'content\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [],
    }, ctx());

    assert.ok(result.isError);
  });

  it('rejects identical old_string and new_string', async () => {
    const file = path.join(tmpDir, 'test.ts');
    fs.writeFileSync(file, 'content\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [{ old_string: 'content', new_string: 'content' }],
    }, ctx());

    assert.ok(result.isError);
  });

  it('applies edits sequentially (later sees earlier results)', async () => {
    const file = path.join(tmpDir, 'test.ts');
    fs.writeFileSync(file, 'AAA\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [
        { old_string: 'AAA', new_string: 'BBB' },
        { old_string: 'BBB', new_string: 'CCC' },
      ],
    }, ctx());

    assert.ok(!result.isError);
    assert.ok(result.output.includes('2/2'));
    assert.equal(fs.readFileSync(file, 'utf-8'), 'CCC\n');
  });
});
