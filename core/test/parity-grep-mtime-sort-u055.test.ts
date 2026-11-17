import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'node:events';
import { grepTool, grepHooks, sortFilesWithMatchesByMtime } from '../src/tools/grep';
import type { ToolContext } from '../src/types';

/**
 * U055 parity: files_with_matches is recency-ordered in JS (stat + mtime) and
 * ripgrep runs WITHOUT `--sortr` so it stays multi-threaded. Content/count output
 * keeps ripgrep's natural order. Mirrors Claude Code's GrepTool mtime sort.
 */

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

/** Capture the argv passed to the mocked rg spawn; the fake child emits the given
 *  stdout then closes with exit 0 (matches found) so execute() stays on the rg path. */
function mockRg(stdout: string): { args: string[] } {
  const captured: { args: string[] } = { args: [] };
  mock.method(grepHooks, 'spawn', (_cmd: string, args: string[]) => {
    captured.args = args;
    const child: any = new EventEmitter();
    child.stdout = new EventEmitter();
    child.stderr = new EventEmitter();
    child.kill = () => true;
    setImmediate(() => {
      if (stdout) child.stdout.emit('data', Buffer.from(stdout));
      child.emit('close', stdout ? 0 : 1, null);
    });
    return child;
  });
  return captured;
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-mtime-'));
});

afterEach(() => {
  mock.restoreAll();
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// sortFilesWithMatchesByMtime — direct unit coverage of both branches
// ---------------------------------------------------------------------------
describe('sortFilesWithMatchesByMtime (parity)', () => {
  it('orders files newest-first by mtime (non-test mode)', () => {
    const older = path.join(tmpDir, 'older.ts');
    const newer = path.join(tmpDir, 'newer.ts');
    fs.writeFileSync(older, 'x');
    fs.writeFileSync(newer, 'x');
    // Force a clear mtime gap regardless of filesystem timestamp granularity.
    fs.utimesSync(older, new Date(1_000_000), new Date(1_000_000));
    fs.utimesSync(newer, new Date(2_000_000), new Date(2_000_000));

    const out = sortFilesWithMatchesByMtime(`${older}\n${newer}\n`, false);
    assert.deepEqual(out.split('\n'), [newer, older], 'newest-modified file must come first');
  });

  it('breaks mtime ties deterministically by filename (non-test mode)', () => {
    const a = path.join(tmpDir, 'a.ts');
    const b = path.join(tmpDir, 'b.ts');
    fs.writeFileSync(a, 'x');
    fs.writeFileSync(b, 'x');
    const same = new Date(1_500_000);
    fs.utimesSync(a, same, same);
    fs.utimesSync(b, same, same);
    // Feed in reverse so only the tiebreaker can produce a,b order.
    const out = sortFilesWithMatchesByMtime(`${b}\n${a}`, false);
    assert.deepEqual(out.split('\n'), [a, b]);
  });

  it('sorts filename-only in test mode (deterministic fixtures), ignoring mtime', () => {
    const a = path.join(tmpDir, 'a.ts'); // give a.ts the NEWER mtime
    const b = path.join(tmpDir, 'b.ts');
    fs.writeFileSync(a, 'x');
    fs.writeFileSync(b, 'x');
    fs.utimesSync(a, new Date(9_000_000), new Date(9_000_000));
    fs.utimesSync(b, new Date(1_000_000), new Date(1_000_000));
    // In mtime mode a.ts (newer) would come first; test mode forces filename order (still a,b here),
    // so assert via a case that distinguishes: reverse-named inputs.
    const z = path.join(tmpDir, 'z.ts');
    fs.writeFileSync(z, 'x');
    fs.utimesSync(z, new Date(9_999_999), new Date(9_999_999)); // newest by far
    const out = sortFilesWithMatchesByMtime(`${z}\n${b}\n${a}`, true);
    // Filename order ignores z's newest mtime.
    assert.deepEqual(out.split('\n'), [a, b, z]);
  });

  it('sorts a failed stat (missing path) last (mtime 0)', () => {
    const real = path.join(tmpDir, 'real.ts');
    fs.writeFileSync(real, 'x');
    fs.utimesSync(real, new Date(1_000), new Date(1_000)); // old, but still beats a missing file
    const missing = path.join(tmpDir, 'does-not-exist.ts');
    const out = sortFilesWithMatchesByMtime(`${missing}\n${real}`, false);
    assert.deepEqual(out.split('\n'), [real, missing], 'statless/missing file sorts last');
  });

  it('returns a single-path (or empty) output unchanged', () => {
    assert.equal(sortFilesWithMatchesByMtime('only.ts', false), 'only.ts');
    assert.equal(sortFilesWithMatchesByMtime('', false), '');
  });
});

// ---------------------------------------------------------------------------
// buildRgArgs: ripgrep runs WITHOUT --sortr (stays multi-threaded)
// ---------------------------------------------------------------------------
describe('grep – ripgrep is NOT asked to sort (keeps parallelism) (parity)', () => {
  it('does not pass --sortr/modified for files_with_matches', async () => {
    const cap = mockRg('');
    await grepTool.execute!({ pattern: 'x', output_mode: 'files_with_matches' }, ctx());
    assert.ok(!cap.args.includes('--sortr'), 'rg must not be asked to sort (forces single-thread)');
    assert.ok(!cap.args.includes('modified'), 'no `modified` sort key may be passed');
  });

  it('does not pass --sortr/modified for content mode (natural rg order)', async () => {
    const cap = mockRg('');
    await grepTool.execute!({ pattern: 'x', output_mode: 'content' }, ctx());
    assert.ok(!cap.args.includes('--sortr'));
    assert.ok(!cap.args.includes('modified'));
  });
});

// ---------------------------------------------------------------------------
// End-to-end through grepTool: rg file list gets JS mtime ordering
// ---------------------------------------------------------------------------
describe('grep – files_with_matches is recency-ordered end-to-end (parity)', () => {
  it('orders the rg file list newest-first via the JS stat sort', async () => {
    const older = path.join(tmpDir, 'older.ts');
    const newer = path.join(tmpDir, 'newer.ts');
    fs.writeFileSync(older, 'needle');
    fs.writeFileSync(newer, 'needle');
    fs.utimesSync(older, new Date(1_000_000), new Date(1_000_000));
    fs.utimesSync(newer, new Date(2_000_000), new Date(2_000_000));
    // rg prints absolute paths (prefixed with the resolved search root), in
    // arbitrary (unsorted, parallel) order — here deliberately oldest-first.
    mockRg(`${older}\n${newer}\n`);

    // Force the production (mtime) ordering regardless of ambient NODE_ENV.
    const prev = process.env.NODE_ENV;
    delete process.env.NODE_ENV;
    try {
      const res = await grepTool.execute!(
        { pattern: 'needle', output_mode: 'files_with_matches' },
        ctx(),
      );
      const out = res.output;
      // Resolved root prefix is stripped by execute(), leaving bare file names.
      assert.ok(out.includes('newer.ts') && out.includes('older.ts'));
      assert.ok(
        out.indexOf('newer.ts') < out.indexOf('older.ts'),
        'newest-modified file must appear before the older one',
      );
    } finally {
      if (prev === undefined) delete process.env.NODE_ENV;
      else process.env.NODE_ENV = prev;
    }
  });
});
