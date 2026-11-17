import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { markTeammateIdle } from '../src/tools/task';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U031 / w79: markTeammateIdle wires the process-manager idle flag (parity:
// Claude Code's InProcessTeammateTask isIdle) so a leader awaiting an idle
// transition via waitForIdle is woken event-driven. Red-green: reverting the
// `context.processManager?.markIdle?.(taskId, true)` line fails this test.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function makeTmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-markidle-'));
  tmpDirs.push(d);
  return d;
}
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best-effort */ } }
});

describe('markTeammateIdle → ProcessManager.markIdle', () => {
  it('marks a running agent idle and wakes a waitForIdle waiter', async () => {
    const pm = new ProcessManager(makeTmpDir());
    pm.registerAgent('a1', {
      command: '[agent] a1',
      description: 'a1',
      outputPath: path.join(makeTmpDir(), 'a1.out'),
      abortFn: () => { /* no-op */ },
    });
    const ctx = { cwd: '/tmp', processManager: pm } as unknown as ToolContext;
    let woke = false;
    const waiter = pm.waitForIdle('a1').then(() => { woke = true; });
    assert.equal(pm.isIdle('a1'), false);
    markTeammateIdle(ctx, 'a1');
    assert.equal(pm.isIdle('a1'), true);
    await waiter;
    assert.equal(woke, true);
  });

  it('is a safe no-op when the manager has no markIdle (minimal stub)', () => {
    const ctx = { cwd: '/tmp', processManager: { } } as unknown as ToolContext;
    assert.doesNotThrow(() => markTeammateIdle(ctx, 'whatever'));
  });
});
