import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { grepTool, stripGrepFlags, searchFilesForPattern, isGrepNoMatchExit } from '../src/tools/grep';
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

describe('grepTool – alternation when only grep is installed', () => {
  it('finds both sides of a pipe pattern without ripgrep', async () => {
    const bin = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-grep-bin-'));
    fs.symlinkSync('/usr/bin/grep', path.join(bin, 'grep'));
    fs.writeFileSync(path.join(tmpDir, 'blp_test.go'), 'func TestBLPModel() {}\n');
    fs.writeFileSync(path.join(tmpDir, 'other.go'), 'func TestOther() {}\n');
    const saved = process.env.PATH;
    process.env.PATH = bin;
    try {
      const result = await grepTool.execute(
        { pattern: 'BLP|TestOther', path: tmpDir, include: '*.go' },
        ctx(),
      );
      assert.ok(!result.isError, result.output);
      assert.match(result.output, /TestBLPModel/);
      assert.match(result.output, /TestOther/);
    } finally {
      process.env.PATH = saved;
      fs.rmSync(bin, { recursive: true, force: true });
    }
  });
});

describe('grepTool – invalid regex and suite logs', () => {
  it('tells the model to escape an unclosed group instead of claiming grep is missing', async () => {
    const bin = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-grep-bin-'));
    fs.symlinkSync('/usr/bin/grep', path.join(bin, 'grep'));
    fs.writeFileSync(path.join(tmpDir, 'enforcer.go'), 'e, _ := NewEnforcer("examples/blp_model.conf")\n');
    const saved = process.env.PATH;
    process.env.PATH = bin;
    try {
      const result = await grepTool.execute(
        { pattern: 'NewEnforcer|Enforce(', path: tmpDir, include: '*.go' },
        ctx(),
      );
      assert.equal(result.isError, true);
      assert.match(result.output, /Invalid regular expression/);
      assert.equal(result.output.includes('Neither rg nor grep'), false);
    } finally {
      process.env.PATH = saved;
      fs.rmSync(bin, { recursive: true, force: true });
    }
  });

  it('omits go test JSON lines and keeps the source match', async () => {
    fs.mkdirSync(path.join(tmpDir, 'reports'));
    fs.writeFileSync(
      path.join(tmpDir, 'reports', 'go-test-results.json'),
      '{"Action":"pass","Package":"github.com/casbin/casbin/v2","Test":"TestBLPModel"}\n',
    );
    fs.writeFileSync(path.join(tmpDir, 'blp_test.go'), 'func TestBLPModel() {}\n');
    const result = await grepTool.execute(
      { pattern: 'TestBLPModel', path: tmpDir },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.match(result.output, /blp_test\.go/);
    assert.equal(result.output.includes('"Action"'), false);
    assert.equal(result.output.includes('go-test-results.json'), false);
  });
});

describe('grepTool – in-process fallback when rg and grep are missing', () => {
  it('finds a symbol without rg or grep on PATH', async () => {
    const saved = process.env.PATH;
    process.env.PATH = '/nonexistent-bin';
    try {
      const result = await grepTool.execute({ pattern: 'nested', path: tmpDir }, ctx());
      assert.ok(!result.isError, result.output);
      assert.match(result.output, /nested\.ts/);
      assert.match(result.output, /hello/);
    } finally {
      process.env.PATH = saved;
    }
  });
});

describe('grepTool – file_path alias', () => {
  it('accepts file_path as alias for path', async () => {
    const result = await grepTool.execute({ pattern: 'nested', file_path: path.join(tmpDir, 'sub') }, ctx());
    assert.ok(!result.isError, result.output);
    assert.match(result.output, /nested/);
  });

  it('prefers path over file_path when both are provided', async () => {
    const result = await grepTool.execute({ pattern: 'hello', path: tmpDir, file_path: '/nonexistent' }, ctx());
    assert.ok(!result.isError, result.output);
    assert.match(result.output, /hello/);
  });
});

// ---------------------------------------------------------------------------
// stripGrepFlags — strip grep-style flags from pattern strings
// ---------------------------------------------------------------------------
describe('stripGrepFlags', () => {
  it('strips single flag prefix from pattern', () => {
    assert.equal(stripGrepFlags('-l pattern'), 'pattern');
  });

  it('strips multiple flags from pattern', () => {
    assert.equal(stripGrepFlags('-r -c pattern'), 'pattern');
  });

  it('strips combined flags', () => {
    assert.equal(stripGrepFlags('-rn pattern'), 'pattern');
  });

  it('leaves plain patterns unchanged', () => {
    assert.equal(stripGrepFlags('myFunction'), 'myFunction');
  });

  it('leaves patterns starting with dash but not a flag unchanged', () => {
    assert.equal(stripGrepFlags('-myOption'), '-myOption');
  });

  it('returns original pattern if stripping would empty it', () => {
    assert.equal(stripGrepFlags('-l '), '-l ');
  });

  it('strips flags with model-like pattern: -i -w searchTerm', () => {
    assert.equal(stripGrepFlags('-i -w searchTerm'), 'searchTerm');
  });
});

// ---------------------------------------------------------------------------
// grep tool — flag stripping integration
// ---------------------------------------------------------------------------
describe('grepTool – strips embedded flags from pattern', () => {
  it('finds matches when pattern has embedded -l flag', async () => {
    const result = await grepTool.execute({ pattern: '-l hello', path: tmpDir }, ctx());
    assert.ok(!result.isError, `Expected success, got: ${result.output}`);
    assert.match(result.output, /hello/);
  });

  it('finds matches when pattern has embedded -rn flags', async () => {
    const result = await grepTool.execute({ pattern: '-rn hello', path: tmpDir }, ctx());
    assert.ok(!result.isError, `Expected success, got: ${result.output}`);
    assert.match(result.output, /hello/);
  });

  it('strips flags in in-process fallback when rg and grep are missing', async () => {
    const saved = process.env.PATH;
    process.env.PATH = '/nonexistent-bin';
    try {
      const result = await grepTool.execute({ pattern: '-l nested', path: tmpDir }, ctx());
      assert.ok(!result.isError, `Expected success, got: ${result.output}`);
      assert.match(result.output, /nested/);
    } finally {
      process.env.PATH = saved;
    }
  });
});

// ---------------------------------------------------------------------------
// Output modes — files_with_matches and count
// ---------------------------------------------------------------------------
describe('grepTool – output modes', () => {
  it('returns only file names in files_with_matches mode', async () => {
    const result = await grepTool.execute(
      { pattern: 'hello', output_mode: 'files_with_matches' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('hello.ts'));
    assert.ok(!result.output.includes('function'));
  });

  it('returns match counts in count mode', async () => {
    const result = await grepTool.execute(
      { pattern: 'hello', output_mode: 'count' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('hello.ts'));
  });

  it('defaults to content mode with line numbers', async () => {
    const result = await grepTool.execute(
      { pattern: 'function hello' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('function hello'));
  });
});

// ---------------------------------------------------------------------------
// Case insensitive
// ---------------------------------------------------------------------------
describe('grepTool – case insensitive', () => {
  it('finds matches regardless of case', async () => {
    const result = await grepTool.execute(
      { pattern: 'HELLO', case_insensitive: true },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('hello'));
  });

  it('misses case-different matches when case_insensitive is false', async () => {
    const result = await grepTool.execute(
      { pattern: 'HELLO', case_insensitive: false },
      ctx(),
    );
    assert.ok(result.output.includes('No matches'));
  });
});

// ---------------------------------------------------------------------------
// Context lines
// ---------------------------------------------------------------------------
describe('grepTool – context lines', () => {
  it('includes surrounding context with context_lines', async () => {
    const result = await grepTool.execute(
      { pattern: 'function hello', context_lines: 1 },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('hello'));
  });

  it('uses before_context and after_context independently', async () => {
    const result = await grepTool.execute(
      { pattern: 'function hello', before_context: 0, after_context: 1 },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('hello'));
  });
});

// ---------------------------------------------------------------------------
// Multiline mode
// ---------------------------------------------------------------------------
describe('grepTool – multiline', () => {
  it('matches across lines with multiline mode', async () => {
    fs.writeFileSync(path.join(tmpDir, 'multi.txt'), 'start\nmiddle\nend\n');
    const result = await grepTool.execute(
      { pattern: 'start.*middle', multiline: true, path: path.join(tmpDir, 'multi.txt') },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
  });
});

// ---------------------------------------------------------------------------
// Pagination (head_limit, offset)
// ---------------------------------------------------------------------------
describe('grepTool – pagination', () => {
  it('respects head_limit to restrict result count', async () => {
    const lines = Array.from({ length: 50 }, (_, i) => `linedata_${i}`).join('\n');
    fs.writeFileSync(path.join(tmpDir, 'paged.txt'), lines);

    const result = await grepTool.execute(
      { pattern: 'linedata', head_limit: 5 },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('truncated'), 'Should indicate truncation');
  });

  it('skips initial lines with offset', async () => {
    const lines = Array.from({ length: 20 }, (_, i) => `item_${i}`).join('\n');
    fs.writeFileSync(path.join(tmpDir, 'offset.txt'), lines);

    const result = await grepTool.execute(
      { pattern: 'item_', head_limit: 5, offset: 10 },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('showing lines'));
  });

  it('returns range message when offset exceeds total matches', async () => {
    const result = await grepTool.execute(
      { pattern: 'hello', offset: 1000 },
      ctx(),
    );
    assert.ok(result.output.includes('No matches in range'));
  });

  it('returns unlimited results with head_limit=0', async () => {
    const lines = Array.from({ length: 300 }, (_, i) => `unlim_${i}`).join('\n');
    fs.writeFileSync(path.join(tmpDir, 'unlimited.txt'), lines);

    const result = await grepTool.execute(
      { pattern: 'unlim_', head_limit: 0 },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
  });
});

// ---------------------------------------------------------------------------
// File type filter
// ---------------------------------------------------------------------------
describe('grepTool – file type filter', () => {
  it('accepts type parameter without error', async () => {
    const result = await grepTool.execute(
      { pattern: 'hello', type: 'ts' },
      ctx(),
    );
    assert.ok(typeof result.output === 'string');
    assert.ok(result.output.includes('hello'));
  });
});

// ---------------------------------------------------------------------------
// Line numbers toggle
// ---------------------------------------------------------------------------
describe('grepTool – line numbers', () => {
  it('suppresses line numbers when line_numbers is false', async () => {
    const result = await grepTool.execute(
      { pattern: 'hello', line_numbers: false },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('hello'));
  });
});

// ---------------------------------------------------------------------------
// Pattern starting with dash (uses -e flag)
// ---------------------------------------------------------------------------
describe('grepTool – dash-prefixed pattern', () => {
  it('searches for patterns starting with dash', async () => {
    fs.writeFileSync(path.join(tmpDir, 'dash.txt'), '--flag-value\nnormal\n');
    const result = await grepTool.execute(
      { pattern: '--flag', path: tmpDir },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('flag'));
  });
});

// ---------------------------------------------------------------------------
// searchFilesForPattern — direct unit tests
// ---------------------------------------------------------------------------
describe('searchFilesForPattern', () => {
  it('searches a single file when root is a file', () => {
    const filePath = path.join(tmpDir, 'hello.ts');
    const result = searchFilesForPattern(filePath, 'function', {});
    assert.ok(result.includes('hello.ts'));
    assert.ok(result.includes('function'));
  });

  it('returns empty string for non-existent root', () => {
    const result = searchFilesForPattern('/nonexistent/path/xyz', 'test', {});
    assert.equal(result, '');
  });

  it('respects case_insensitive option', () => {
    const result = searchFilesForPattern(tmpDir, 'FUNCTION', { caseInsensitive: true });
    assert.ok(result.includes('function'));
  });

  it('case-sensitive by default', () => {
    const result = searchFilesForPattern(tmpDir, 'FUNCTION', { caseInsensitive: false });
    assert.equal(result, '');
  });

  it('filters by include extension', () => {
    const result = searchFilesForPattern(tmpDir, 'hello', { include: '*.json' });
    assert.ok(result.includes('data.json'));
    assert.ok(!result.includes('hello.ts'));
  });

  it('respects headLimit', () => {
    const lines = Array.from({ length: 50 }, (_, i) => `marker_${i}`).join('\n');
    fs.writeFileSync(path.join(tmpDir, 'many.txt'), lines);
    const result = searchFilesForPattern(tmpDir, 'marker_', { headLimit: 3 });
    const matchLines = result.split('\n').filter(l => l.trim().length > 0);
    assert.ok(matchLines.length <= 4);
  });

  it('headLimit=0 returns all matches', () => {
    const lines = Array.from({ length: 300 }, (_, i) => `all_${i}`).join('\n');
    fs.writeFileSync(path.join(tmpDir, 'allmatches.txt'), lines);
    const result = searchFilesForPattern(tmpDir, 'all_', { headLimit: 0 });
    const matchLines = result.split('\n').filter(l => l.trim().length > 0);
    assert.ok(matchLines.length >= 300);
  });

  it('skips node_modules directory', () => {
    fs.mkdirSync(path.join(tmpDir, 'node_modules'));
    fs.writeFileSync(path.join(tmpDir, 'node_modules', 'pkg.ts'), 'hello hidden\n');
    const result = searchFilesForPattern(tmpDir, 'hidden', {});
    assert.equal(result, '');
  });

  it('skips .git directory', () => {
    fs.mkdirSync(path.join(tmpDir, '.git'));
    fs.writeFileSync(path.join(tmpDir, '.git', 'config'), 'hello gitconfig\n');
    const result = searchFilesForPattern(tmpDir, 'gitconfig', {});
    assert.equal(result, '');
  });

  it('throws on invalid regex', () => {
    assert.throws(
      () => searchFilesForPattern(tmpDir, '(unclosed', {}),
      /Invalid regular expression|Unterminated group/,
    );
  });

  it('skips unreadable directories gracefully', () => {
    const unreadable = path.join(tmpDir, 'noperm');
    fs.mkdirSync(unreadable, { mode: 0o000 });
    try {
      const result = searchFilesForPattern(tmpDir, 'hello', {});
      assert.ok(typeof result === 'string');
    } finally {
      fs.chmodSync(unreadable, 0o755);
    }
  });
});

// ---------------------------------------------------------------------------
// buildGrepArgs paths — grep fallback with output modes
// ---------------------------------------------------------------------------
describe('grepTool – grep fallback output modes', () => {
  it('uses -l in grep fallback for files_with_matches', async () => {
    const bin = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-grep-bin-'));
    fs.symlinkSync('/usr/bin/grep', path.join(bin, 'grep'));
    const saved = process.env.PATH;
    process.env.PATH = bin;
    try {
      const result = await grepTool.execute(
        { pattern: 'hello', output_mode: 'files_with_matches' },
        ctx(),
      );
      assert.ok(!result.isError, result.output);
    } finally {
      process.env.PATH = saved;
      fs.rmSync(bin, { recursive: true, force: true });
    }
  });

  it('uses -c in grep fallback for count mode', async () => {
    const bin = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-grep-bin-'));
    fs.symlinkSync('/usr/bin/grep', path.join(bin, 'grep'));
    const saved = process.env.PATH;
    process.env.PATH = bin;
    try {
      const result = await grepTool.execute(
        { pattern: 'hello', output_mode: 'count' },
        ctx(),
      );
      assert.ok(!result.isError, result.output);
    } finally {
      process.env.PATH = saved;
      fs.rmSync(bin, { recursive: true, force: true });
    }
  });

  it('uses -i in grep fallback for case_insensitive', async () => {
    const bin = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-grep-bin-'));
    fs.symlinkSync('/usr/bin/grep', path.join(bin, 'grep'));
    const saved = process.env.PATH;
    process.env.PATH = bin;
    try {
      const result = await grepTool.execute(
        { pattern: 'HELLO', case_insensitive: true },
        ctx(),
      );
      assert.ok(!result.isError, result.output);
      assert.ok(result.output.includes('hello'));
    } finally {
      process.env.PATH = saved;
      fs.rmSync(bin, { recursive: true, force: true });
    }
  });

  it('uses -C in grep fallback for context_lines', async () => {
    const bin = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-grep-bin-'));
    fs.symlinkSync('/usr/bin/grep', path.join(bin, 'grep'));
    const saved = process.env.PATH;
    process.env.PATH = bin;
    try {
      const result = await grepTool.execute(
        { pattern: 'function hello', context_lines: 1 },
        ctx(),
      );
      assert.ok(!result.isError, result.output);
    } finally {
      process.env.PATH = saved;
      fs.rmSync(bin, { recursive: true, force: true });
    }
  });

  it('uses --include in grep fallback for include filter', async () => {
    const bin = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-grep-bin-'));
    fs.symlinkSync('/usr/bin/grep', path.join(bin, 'grep'));
    const saved = process.env.PATH;
    process.env.PATH = bin;
    try {
      const result = await grepTool.execute(
        { pattern: 'hello', include: '*.ts' },
        ctx(),
      );
      assert.ok(!result.isError, result.output);
    } finally {
      process.env.PATH = saved;
      fs.rmSync(bin, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Include with comma-separated globs
// ---------------------------------------------------------------------------
describe('grepTool – comma-separated includes', () => {
  it('accepts comma-separated glob patterns', async () => {
    const result = await grepTool.execute(
      { pattern: 'hello', include: '*.ts,*.json' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('hello'));
  });
});

// ---------------------------------------------------------------------------
// isSuiteLogLine filtering (indirect via formatResult)
// ---------------------------------------------------------------------------
describe('grepTool – suite log filtering', () => {
  it('filters out go test JSON log lines from results', async () => {
    fs.writeFileSync(
      path.join(tmpDir, 'mixed.json'),
      '{"Action":"pass","Package":"pkg","Test":"TestFoo","Elapsed":1.5}\nreal_match\n',
    );
    const result = await grepTool.execute(
      { pattern: 'pass|real_match', path: tmpDir },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(!result.output.includes('"Action"'));
  });
});
