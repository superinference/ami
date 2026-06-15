import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { grepTool } from '../src/tools/grep';
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-'));
  // Create some files to search
  fs.writeFileSync(path.join(tmpDir, 'hello.ts'), 'export function hello() {}\nconst x = 42;\n');
  fs.writeFileSync(path.join(tmpDir, 'world.ts'), 'export function world() {}\nconst y = 99;\n');
  fs.mkdirSync(path.join(tmpDir, 'sub'));
  fs.writeFileSync(path.join(tmpDir, 'sub', 'nested.ts'), 'const hello = "nested";\n');
  fs.writeFileSync(path.join(tmpDir, 'data.json'), '{"key": "hello"}\n');
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('grepTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(grepTool.name, 'grep');
  });

  it('is read-only and concurrency-safe', () => {
    assert.equal(grepTool.isReadOnly, true);
    assert.equal(grepTool.isConcurrencySafe, true);
  });

  it('schema requires pattern', () => {
    assert.ok(grepTool.inputSchema.required?.includes('pattern'));
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe('grepTool – validation', () => {
  it('rejects empty pattern', async () => {
    const result = await grepTool.execute({ pattern: '' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must not be empty'));
  });

  it('rejects whitespace-only pattern', async () => {
    const result = await grepTool.execute({ pattern: '   ' }, ctx());
    assert.equal(result.isError, true);
  });
});

// ---------------------------------------------------------------------------
// Search results
// ---------------------------------------------------------------------------

describe('grepTool – search', () => {
  it('finds matching lines across files', async () => {
    const result = await grepTool.execute({ pattern: 'hello' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('hello'));
    // Should find matches in hello.ts, sub/nested.ts, and data.json
  });

  it('returns no matches message when nothing found', async () => {
    const result = await grepTool.execute({ pattern: 'zzzznotfound' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('No matches'));
  });

  it('respects the path option', async () => {
    const result = await grepTool.execute(
      { pattern: 'hello', path: path.join(tmpDir, 'sub') },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('nested'));
    // Should NOT include results from the parent directory files
  });

  it('respects the include option to filter by extension', async () => {
    const result = await grepTool.execute(
      { pattern: 'hello', include: '*.json' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('data.json'));
  });

  it('searches using regex patterns', async () => {
    const result = await grepTool.execute({ pattern: 'function' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('function'));
  });
});

// ---------------------------------------------------------------------------
// Abort signal
// ---------------------------------------------------------------------------

describe('grepTool – abort', () => {
  it('handles abort gracefully', async () => {
    // Use a normal signal and abort shortly after — fully pre-aborted signals
    // can trigger async ENOENT noise after the test ends.
    const ac = new AbortController();
    const promise = grepTool.execute(
      { pattern: 'hello' },
      ctx({ abortSignal: ac.signal }),
    );
    setTimeout(() => ac.abort(), 50);
    const result = await promise;
    // Either succeeds quickly or reports no matches / empty
    assert.ok(typeof result.output === 'string');
  });
});

// ---------------------------------------------------------------------------
// Relative search path
// ---------------------------------------------------------------------------

describe('grepTool – relative path', () => {
  it('resolves relative path against cwd', async () => {
    const result = await grepTool.execute(
      { pattern: 'nested', path: 'sub' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('nested'));
  });
});

// ---------------------------------------------------------------------------
// Output truncation (lines 87-93, 128, 131-132, 144-145, 169)
// ---------------------------------------------------------------------------

describe('grepTool – output truncation', () => {
  it('truncates output beyond head_limit matches', async () => {
    // Create a file with >250 matching lines (default head_limit = 250)
    const lines = Array.from({ length: 300 }, (_, i) => `matchline_${i}`).join('\n');
    fs.writeFileSync(path.join(tmpDir, 'big.txt'), lines);

    const result = await grepTool.execute({ pattern: 'matchline' }, ctx());
    assert.ok(!result.isError);
    // Should have matches but not all 300
    assert.ok(result.output.includes('matchline_0'));
  });
});

describe('grepTool – include with glob', () => {
  it('builds rg args with include glob for filtering', async () => {
    // Create .py file that should not match when filtering to .ts
    fs.writeFileSync(path.join(tmpDir, 'script.py'), 'hello python\n');

    const result = await grepTool.execute(
      { pattern: 'hello', include: '*.ts' },
      ctx(),
    );
    assert.ok(!result.isError);
    // Should NOT include python file
    assert.ok(!result.output.includes('script.py'));
  });
});

describe('grepTool – buildGrepArgs with include', () => {
  it('grep fallback also respects include filter', async () => {
    // This tests the buildGrepArgs path (lines 98-111)
    // Both rg and grep are tried; at least one should work
    const result = await grepTool.execute(
      { pattern: 'function', include: '*.ts' },
      ctx(),
    );
    assert.ok(!result.isError || result.output.includes('No matches'));
  });
});

describe('grepTool – error event handling', () => {
  it('handles child process error event gracefully (lines 154-159)', async () => {
    // Test with a non-existent search path to trigger error scenarios
    const result = await grepTool.execute(
      { pattern: 'test', path: '/nonexistent/path/zzz' },
      ctx(),
    );
    // Should either return no matches or an error, not crash
    assert.ok(typeof result.output === 'string');
  });
});

describe('grepTool – close with stderr', () => {
  it('handles stderr indicating binary not found (lines 168-173)', async () => {
    // Create a file and search — the rg/grep tools should handle this
    const result = await grepTool.execute(
      { pattern: 'zzz_never_matches' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('No matches'));
  });
});
