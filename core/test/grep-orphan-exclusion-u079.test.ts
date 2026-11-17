import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'node:events';
import { grepTool, grepHooks, searchFilesForPattern } from '../src/tools/grep';
import { ORPHANED_PLUGIN_HOLD_DIRNAME } from '../src/tools/tool-utils';
import type { ToolContext } from '../src/types';

/**
 * U079 — grep must never return matches from superseded plugin versions that the
 * installer relocated into the hidden `.orphaned/` hold dir (common's
 * plugin-orphan-cache). Parity: Claude's getGlobExclusionsForPluginCache feeds
 * `--glob '!<orphaned>/**'` into Grep/Glob; AMI excludes its hold dir by name on
 * BOTH the ripgrep path and the in-process fallback walker.
 */
let tmpDir: string;
function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal, ...overrides };
}
beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'grep-orphan-')); });
afterEach(() => { mock.restoreAll(); fs.rmSync(tmpDir, { recursive: true, force: true }); });

describe('grep — orphaned plugin hold dir excluded (ripgrep route)', () => {
  it('passes --glob !.orphaned to the ripgrep invocation', async () => {
    let captured: string[] = [];
    mock.method(grepHooks, 'spawn', (_cmd: string, args: string[]) => {
      captured = args;
      const child: EventEmitter & { stdout: EventEmitter; stderr: EventEmitter; kill: () => boolean } =
        Object.assign(new EventEmitter(), {
          stdout: new EventEmitter(),
          stderr: new EventEmitter(),
          kill: () => true,
        });
      setImmediate(() => child.emit('close', 1, null)); // exit 1 = no matches
      return child as never;
    });
    await grepTool.execute!({ pattern: 'TOKEN' }, ctx());
    const globValues = captured.map((a, i) => (a === '--glob' ? captured[i + 1] : null)).filter(Boolean);
    assert.ok(
      globValues.includes(`!${ORPHANED_PLUGIN_HOLD_DIRNAME}`),
      `expected !${ORPHANED_PLUGIN_HOLD_DIRNAME} in ${JSON.stringify(globValues)}`,
    );
  });
});

describe('grep — orphaned plugin hold dir skipped (in-process fallback)', () => {
  it('does not match files inside .orphaned/, keeps live files', () => {
    fs.writeFileSync(path.join(tmpDir, 'live.ts'), 'const NEEDLE = 1;\n');
    const stale = path.join(tmpDir, ORPHANED_PLUGIN_HOLD_DIRNAME, 'plugin-1-abc');
    fs.mkdirSync(stale, { recursive: true });
    fs.writeFileSync(path.join(stale, 'stale.ts'), 'const NEEDLE = 2;\n');

    const out = searchFilesForPattern(tmpDir, 'NEEDLE', {});
    assert.match(out, /live\.ts/, 'live file must still match');
    assert.doesNotMatch(out, /stale\.ts/, 'orphaned version file must be skipped');
    assert.doesNotMatch(out, new RegExp(ORPHANED_PLUGIN_HOLD_DIRNAME.replace('.', '\\.')));
  });

  it('RED-guard: an ordinarily-named dir with the same file IS matched', () => {
    // Proves the exclusion discriminates on the hold-dir name, not on all nesting.
    const normal = path.join(tmpDir, 'kept');
    fs.mkdirSync(normal, { recursive: true });
    fs.writeFileSync(path.join(normal, 'inner.ts'), 'const NEEDLE = 3;\n');
    const out = searchFilesForPattern(tmpDir, 'NEEDLE', {});
    assert.match(out, /inner\.ts/, 'control: a normal nested dir is searched');
  });
});
