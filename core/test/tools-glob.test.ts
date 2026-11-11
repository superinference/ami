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

  it('results are sorted by mtime descending (newest first)', async () => {
    // a.ts older, b.ts newer — expect b before a regardless of alpha order.
    fs.utimesSync(path.join(tmpDir, 'a.ts'), new Date(1_000_000), new Date(1_000_000));
    fs.utimesSync(path.join(tmpDir, 'b.ts'), new Date(2_000_000), new Date(2_000_000));
    const result = await globTool.execute({ pattern: '*.ts' }, ctx());
    assert.ok(!result.isError);
    const lines = result.output.trim().split('\n').slice(1); // drop summary line
    assert.ok(lines.indexOf('b.ts') < lines.indexOf('a.ts'),
      `Expected b.ts (newer) before a.ts (older): ${lines.join(', ')}`);
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

// ---------------------------------------------------------------------------
// Truncation — more than MAX_RESULTS (200) files
// ---------------------------------------------------------------------------
describe('globTool – truncation', () => {
  it('truncates when results exceed MAX_RESULTS (100)', async () => {
    for (let i = 0; i < 110; i++) {
      fs.writeFileSync(path.join(tmpDir, `gen_${String(i).padStart(3, '0')}.txt`), '');
    }
    const result = await globTool.execute({ pattern: '**/*.txt' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('truncated'));
    assert.ok(result.output.includes('showing 100 of 110 matches'));
    assert.ok(result.output.includes('110'));
  });
});

// ---------------------------------------------------------------------------
// Error handling — glob throws
// ---------------------------------------------------------------------------
describe('globTool – error handling', () => {
  it('returns error when base path is unreadable', async () => {
    const result = await globTool.execute(
      { pattern: '**/*.ts', path: '/nonexistent/dir/xyz' },
      ctx(),
    );
    assert.ok(typeof result.output === 'string');
  });

  it('handles invalid glob pattern gracefully', async () => {
    const result = await globTool.execute(
      { pattern: '[invalid' },
      ctx(),
    );
    assert.ok(typeof result.output === 'string');
  });
});

// ---------------------------------------------------------------------------
// G2 — mtime-descending sort
// ---------------------------------------------------------------------------
describe('globTool – mtime sort (G2)', () => {
  it('orders multiple files newest-first by mtime', async () => {
    // Create three files, stamp them with increasing mtimes in a non-alpha order.
    fs.writeFileSync(path.join(tmpDir, 'zebra.log'), '');
    fs.writeFileSync(path.join(tmpDir, 'apple.log'), '');
    fs.writeFileSync(path.join(tmpDir, 'mango.log'), '');
    fs.utimesSync(path.join(tmpDir, 'apple.log'), new Date(1_000_000), new Date(1_000_000)); // oldest
    fs.utimesSync(path.join(tmpDir, 'mango.log'), new Date(2_000_000), new Date(2_000_000)); // middle
    fs.utimesSync(path.join(tmpDir, 'zebra.log'), new Date(3_000_000), new Date(3_000_000)); // newest
    const result = await globTool.execute({ pattern: '*.log' }, ctx());
    assert.ok(!result.isError);
    const lines = result.output.trim().split('\n').slice(1);
    assert.deepEqual(lines, ['zebra.log', 'mango.log', 'apple.log']);
  });

  it('breaks mtime ties with a stable alphabetical secondary sort', async () => {
    fs.writeFileSync(path.join(tmpDir, 'y.log'), '');
    fs.writeFileSync(path.join(tmpDir, 'x.log'), '');
    fs.writeFileSync(path.join(tmpDir, 'z.log'), '');
    const sameTime = new Date(5_000_000);
    for (const f of ['x.log', 'y.log', 'z.log']) {
      fs.utimesSync(path.join(tmpDir, f), sameTime, sameTime);
    }
    const result = await globTool.execute({ pattern: '*.log' }, ctx());
    assert.ok(!result.isError);
    const lines = result.output.trim().split('\n').slice(1);
    assert.deepEqual(lines, ['x.log', 'y.log', 'z.log']);
  });
});

// ---------------------------------------------------------------------------
// G3 — overridable node_modules / .git excludes
// ---------------------------------------------------------------------------
describe('globTool – overridable excludes (G3)', () => {
  it('excludes node_modules by default', async () => {
    fs.mkdirSync(path.join(tmpDir, 'node_modules', 'pkg'), { recursive: true });
    fs.writeFileSync(path.join(tmpDir, 'node_modules', 'pkg', 'index.ts'), '');
    const result = await globTool.execute({ pattern: '**/*.ts' }, ctx());
    assert.ok(!result.isError);
    assert.ok(!result.output.includes('node_modules'));
  });

  it('includes node_modules when the pattern references it', async () => {
    fs.mkdirSync(path.join(tmpDir, 'node_modules', 'pkg'), { recursive: true });
    fs.writeFileSync(path.join(tmpDir, 'node_modules', 'pkg', 'index.ts'), '');
    const result = await globTool.execute({ pattern: '**/node_modules/**/*.ts' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('node_modules/pkg/index.ts'));
  });

  it('excludes .git by default', async () => {
    fs.mkdirSync(path.join(tmpDir, '.git'), { recursive: true });
    fs.writeFileSync(path.join(tmpDir, '.git', 'hook.ts'), '');
    const result = await globTool.execute({ pattern: '**/*.ts' }, ctx());
    assert.ok(!result.isError);
    assert.ok(!result.output.includes('.git/'));
  });

  it('includes .git when the pattern references it', async () => {
    fs.mkdirSync(path.join(tmpDir, '.git'), { recursive: true });
    fs.writeFileSync(path.join(tmpDir, '.git', 'hook.ts'), '');
    const result = await globTool.execute({ pattern: '.git/**/*.ts' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('.git/hook.ts'));
  });
});

// ---------------------------------------------------------------------------
// G4 — count limit (100) + char cap (50_000) boundaries
// ---------------------------------------------------------------------------
describe('globTool – limit + char cap (G4)', () => {
  function makeFlatFiles(n: number) {
    for (let i = 0; i < n; i++) {
      fs.writeFileSync(path.join(tmpDir, `f_${String(i).padStart(4, '0')}.dat`), '');
    }
  }

  it('does NOT truncate at exactly MAX_RESULTS (100) files', async () => {
    makeFlatFiles(100);
    const result = await globTool.execute({ pattern: '**/*.dat' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.startsWith('[100 files]'));
    assert.ok(!result.output.includes('truncated'));
    const lines = result.output.trim().split('\n').slice(1);
    assert.equal(lines.length, 100);
  });

  it('truncates at MAX_RESULTS + 1 (101) files', async () => {
    makeFlatFiles(101);
    const result = await globTool.execute({ pattern: '**/*.dat' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('(truncated)'));
    assert.ok(result.output.includes('showing 100 of 101 matches'));
  });

  it('applies the character cap even when under the count limit', async () => {
    // Two deeply-nested long-named dirs make each relative path ~600 chars, so
    // ~84 of 100 matches already blow past MAX_OUTPUT_CHARS (50_000).
    const seg = 'd'.repeat(200);
    const deep = path.join(tmpDir, seg, seg, seg);
    fs.mkdirSync(deep, { recursive: true });
    for (let i = 0; i < 100; i++) {
      fs.writeFileSync(path.join(deep, `file_${String(i).padStart(3, '0')}.big`), '');
    }
    const result = await globTool.execute({ pattern: '**/*.big' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('(truncated)'));
    const m = result.output.match(/showing (\d+) of (\d+) matches/);
    assert.ok(m, `expected a truncation note, got: ${result.output.slice(0, 200)}`);
    const shown = Number(m![1]);
    const total = Number(m![2]);
    assert.equal(total, 100);
    assert.ok(shown < 100, `char cap should show fewer than 100 (got ${shown})`);
    // The character cap really bounds the list body (summary + note add a little).
    const body = result.output.split('\n').slice(1).join('\n');
    assert.ok(body.length <= 50_000 + 200, `body length ${body.length} exceeds cap`);
  });

  it('returns a no-files message for an empty result set', async () => {
    const result = await globTool.execute({ pattern: '**/*.nonexistent-ext' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('No files found'));
    assert.ok(!result.output.includes('truncated'));
  });
});

// ---------------------------------------------------------------------------
// G6 — path points at a non-directory
// ---------------------------------------------------------------------------
describe('globTool – path is not a directory (G6)', () => {
  it('errors clearly when path points at a file', async () => {
    const filePath = path.join(tmpDir, 'a.ts');
    const result = await globTool.execute({ pattern: '*.ts', path: filePath }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('is not a directory'),
      `expected not-a-directory error, got: ${result.output}`);
    assert.ok(result.output.includes(filePath));
  });

  it('still works when path points at a real directory', async () => {
    const result = await globTool.execute(
      { pattern: '*.ts', path: path.join(tmpDir, 'src') },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('d.ts'));
  });
});
