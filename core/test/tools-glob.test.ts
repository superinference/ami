import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { globTool } from '../src/tools/glob';
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-glob-'));
  fs.writeFileSync(path.join(tmpDir, 'a.ts'), '');
  fs.writeFileSync(path.join(tmpDir, 'b.ts'), '');
  fs.writeFileSync(path.join(tmpDir, 'c.json'), '');
  fs.mkdirSync(path.join(tmpDir, 'src'));
  fs.writeFileSync(path.join(tmpDir, 'src', 'd.ts'), '');
  fs.writeFileSync(path.join(tmpDir, 'src', 'e.json'), '');
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('globTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(globTool.name, 'glob');
  });

  it('is read-only and concurrency-safe', () => {
    assert.equal(globTool.isReadOnly, true);
    assert.equal(globTool.isConcurrencySafe, true);
  });

  it('schema requires pattern', () => {
    assert.ok(globTool.inputSchema.required?.includes('pattern'));
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe('globTool – validation', () => {
  it('rejects empty pattern', async () => {
    const result = await globTool.execute({ pattern: '' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must not be empty'));
  });
});

// ---------------------------------------------------------------------------
// Search results
// ---------------------------------------------------------------------------

describe('globTool – search', () => {
  it('finds files matching a glob pattern', async () => {
    const result = await globTool.execute({ pattern: '**/*.ts' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('a.ts'));
    assert.ok(result.output.includes('b.ts'));
    assert.ok(result.output.includes('d.ts'));
    // Should not include .json files
    assert.ok(!result.output.includes('.json'));
  });

  it('finds only json files', async () => {
    const result = await globTool.execute({ pattern: '**/*.json' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('c.json'));
    assert.ok(result.output.includes('e.json'));
    assert.ok(!result.output.includes('.ts'));
  });

  it('returns no files found message when pattern matches nothing', async () => {
    const result = await globTool.execute({ pattern: '**/*.xyz' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('No files found'));
  });

  it('results are sorted alphabetically', async () => {
    const result = await globTool.execute({ pattern: '*.ts' }, ctx());
    assert.ok(!result.isError);
    const lines = result.output.trim().split('\n');
    for (let i = 1; i < lines.length; i++) {
      assert.ok(lines[i - 1].localeCompare(lines[i]) <= 0,
        `Expected ${lines[i - 1]} <= ${lines[i]}`);
    }
  });

  it('respects the path option', async () => {
    const result = await globTool.execute(
      { pattern: '*.ts', path: path.join(tmpDir, 'src') },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('d.ts'));
    // Should not include files from root dir
    assert.ok(!result.output.includes('a.ts'));
    assert.ok(!result.output.includes('b.ts'));
  });

  it('resolves relative base path against cwd', async () => {
    const result = await globTool.execute(
      { pattern: '*.ts', path: 'src' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('d.ts'));
  });
});
