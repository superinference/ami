import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'node:events';
import { grepTool, grepHooks, searchFilesForPattern } from '../src/tools/grep';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

/**
 * Capture the args passed to the (mocked) rg spawn. The fake child emits a
 * clean "no matches" close (exit 1) so grepTool.execute resolves through the
 * ripgrep branch without falling back to grep.
 */
function captureRgArgs(): { args: string[] } {
  const captured: { args: string[] } = { args: [] };
  mock.method(grepHooks, 'spawn', (_cmd: string, args: string[]) => {
    captured.args = args;
    const child: any = new EventEmitter();
    child.stdout = new EventEmitter();
    child.stderr = new EventEmitter();
    child.kill = () => true;
    setImmediate(() => child.emit('close', 1));
    return child;
  });
  return captured;
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-vcs-'));
});

afterEach(() => {
  mock.restoreAll();
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Sapling (.sl) VCS metadata exclusion — ripgrep glob + in-process fallback
// ---------------------------------------------------------------------------
describe('grep – VCS exclusion set includes Sapling (.sl)', () => {
  it('passes --glob !.sl to ripgrep alongside the other VCS globs', async () => {
    const cap = captureRgArgs();
    await grepTool.execute!({ pattern: 'needle' }, ctx());
    // The glob is pushed as two argv entries: '--glob' then '!.sl'.
    assert.ok(cap.args.includes('!.sl'), 'rg must exclude the Sapling .sl store');
    // Sanity: the pre-existing VCS globs are still present.
    for (const g of ['!.git', '!.svn', '!.hg', '!.bzr', '!.jj']) {
      assert.ok(cap.args.includes(g), `rg must still exclude ${g}`);
    }
  });

  it('the in-process fallback skips .sl (and .hg/.jj) metadata directories', () => {
    // A match lives both inside a Sapling store and in a normal tracked file.
    fs.mkdirSync(path.join(tmpDir, '.sl'), { recursive: true });
    fs.writeFileSync(path.join(tmpDir, '.sl', 'store.ts'), 'const SECRET = "needle";\n');
    fs.mkdirSync(path.join(tmpDir, '.hg'), { recursive: true });
    fs.writeFileSync(path.join(tmpDir, '.hg', 'dirstate.ts'), 'const HG = "needle";\n');
    fs.mkdirSync(path.join(tmpDir, '.jj'), { recursive: true });
    fs.writeFileSync(path.join(tmpDir, '.jj', 'op.ts'), 'const JJ = "needle";\n');
    fs.writeFileSync(path.join(tmpDir, 'keep.ts'), 'const KEEP = "needle";\n');

    const out = searchFilesForPattern(tmpDir, 'needle', {});
    assert.ok(out.includes('keep.ts'), 'tracked file must be searched');
    assert.ok(!out.includes('store.ts'), '.sl store must be skipped');
    assert.ok(!out.includes('dirstate.ts'), '.hg store must be skipped');
    assert.ok(!out.includes('op.ts'), '.jj store must be skipped');
  });
});

// ---------------------------------------------------------------------------
// context_lines (-C) takes precedence over before_context / after_context
// ---------------------------------------------------------------------------
describe('grep – context_lines precedence over before/after', () => {
  it('emits -C and drops -B/-A when context_lines is set', async () => {
    const cap = captureRgArgs();
    await grepTool.execute!(
      { pattern: 'x', output_mode: 'content', context_lines: 2, before_context: 5, after_context: 7 },
      ctx(),
    );
    assert.ok(cap.args.includes('-C'), 'symmetric context flag must be present');
    assert.ok(cap.args.includes('2'), 'context value must be forwarded');
    assert.ok(!cap.args.includes('-B'), 'before_context must be ignored when context_lines is set');
    assert.ok(!cap.args.includes('-A'), 'after_context must be ignored when context_lines is set');
  });

  it('still applies before/after independently when context_lines is not set', async () => {
    const cap = captureRgArgs();
    await grepTool.execute!(
      { pattern: 'x', output_mode: 'content', before_context: 3, after_context: 4 },
      ctx(),
    );
    assert.ok(cap.args.includes('-B'), 'before_context must be applied');
    assert.ok(cap.args.includes('3'));
    assert.ok(cap.args.includes('-A'), 'after_context must be applied');
    assert.ok(cap.args.includes('4'));
    assert.ok(!cap.args.includes('-C'), 'no symmetric context when context_lines is unset');
  });
});
