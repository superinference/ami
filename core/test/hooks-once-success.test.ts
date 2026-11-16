import { describe, it, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { HookManager } from '../src/hooks';
import { fsHooks } from '../src/logger';

// ---------------------------------------------------------------------------
// once:true hooks — removed ONLY after a SUCCESSFUL execution (Claude Code's
// onHookSuccess semantics). A once hook that errors / times out / is blocked by
// an error stays registered to retry; a clean run self-removes. Covers both the
// synchronous and the fire-and-forget async paths, plus the observable
// "Removing one-shot hook for event <name>" debug line.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function writeHooks(entries: unknown[]): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-once-'));
  tmpDirs.push(tmpDir);
  const dir = path.join(tmpDir, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return tmpDir;
}

function makeManager(entries: unknown[]): HookManager {
  const hm = new HookManager();
  hm.setWorkspaceTrusted(true);
  hm.loadFromFile(writeHooks(entries));
  return hm;
}

after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch {} }
});

describe('hooks — once:true removed only after successful execution', () => {
  it('SYNC: a once hook that exits 0 (success) is removed', async () => {
    const hm = makeManager([
      { event: 'preToolUse', hook: { type: 'command', command: 'exit 0', once: true } },
    ]);
    assert.equal(hm.getSummary().preToolUse, 1, 'registered');
    await hm.executePreToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {} } as any);
    assert.equal(hm.getSummary().preToolUse, 0, 'removed after a successful run');
  });

  it('SYNC: a once hook that BLOCKS via exit 2 (error) stays registered to retry', async () => {
    const hm = makeManager([
      { event: 'preToolUse', hook: { type: 'command', command: 'exit 2', once: true } },
    ]);
    assert.equal(hm.getSummary().preToolUse, 1, 'registered');
    const r1 = await hm.executePreToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {} } as any);
    // Exit 2 is a blocking error, so the tool is denied...
    assert.equal(r1.action, 'deny', 'exit 2 denies the tool');
    // ...and the failed one-shot hook is NOT consumed — it fires again next time.
    assert.equal(hm.getSummary().preToolUse, 1, 'failed once hook retained');
    const r2 = await hm.executePreToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {} } as any);
    assert.equal(r2.action, 'deny', 'still denies on the retry');
    assert.equal(hm.getSummary().preToolUse, 1, 'still retained after a second failure');
  });

  it('SYNC: a once hook that exits non-zero non-2 (error) stays registered', async () => {
    const hm = makeManager([
      { event: 'postToolUse', hook: { type: 'command', command: 'exit 3', once: true } },
    ]);
    await hm.executePostToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {}, toolOutput: 'o', isError: false } as any);
    assert.equal(hm.getSummary().postToolUse, 1, 'non-zero-exit once hook retained');
  });

  it('ASYNC: a once hook that succeeds in the background is removed after it settles', async () => {
    const hm = makeManager([
      { event: 'preToolUse', hook: { type: 'command', command: 'exit 0', once: true, async: true } },
    ]);
    assert.equal(hm.getSummary().preToolUse, 1, 'registered');
    await hm.executePreToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {} } as any);
    // Async hooks do not block the turn, so the removal happens once the
    // background run resolves — await the pending-async barrier first.
    await hm.finalizePendingAsyncHooks();
    assert.equal(hm.getSummary().preToolUse, 0, 'successful async once hook removed');
  });

  it('ASYNC: a once hook that fails in the background stays registered', async () => {
    const hm = makeManager([
      { event: 'preToolUse', hook: { type: 'command', command: 'exit 2', once: true, async: true } },
    ]);
    await hm.executePreToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {} } as any);
    await hm.finalizePendingAsyncHooks();
    assert.equal(hm.getSummary().preToolUse, 1, 'failed async once hook retained');
  });

  it('emits an observable "Removing one-shot hook for event <name>" debug line', async () => {
    const lines: string[] = [];
    const spy = mock.method(fsHooks, 'appendFileSync', (_f: unknown, line: unknown) => {
      lines.push(String(line));
    });
    try {
      const hm = makeManager([
        { event: 'preToolUse', hook: { type: 'command', command: 'exit 0', once: true } },
      ]);
      await hm.executePreToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {} } as any);
    } finally {
      spy.mock.restore();
    }
    assert.ok(
      lines.some(l => l.includes('Removing one-shot hook for event preToolUse')),
      'lifecycle removal is logged with the event name',
    );
  });
});
