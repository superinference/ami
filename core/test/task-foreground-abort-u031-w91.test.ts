// Integration / E2E red-green reproducer for U031 / w91: a RUNNING foreground
// subagent can be aborted individually via the ProcessManager foreground-abort
// registry, instead of only by Ctrl+C aborting the whole parent turn.
//
// This drives the real taskTool foreground path with a real ProcessManager and a
// mock sub-engine that blocks until its own abort signal fires. While the subagent
// runs the task tool registers it as individually abortable; calling
// pm.abortForeground(taskId) fires the sub-engine's AbortController, ending the run.
//
// RED on revert: without task.ts's registerForegroundAbort call the subagent is
// never registered, so `hasForegroundAbort` stays false and the abort is never
// delivered — the subagent would run forever (the test times out) and the
// hasForegroundAbort assertion fails.

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { taskTool, resetAgentRegistry } from '../src/tools/task';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

let tmpDir: string;
let pm: ProcessManager | null = null;

beforeEach(() => {
  resetAgentRegistry();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fg-abort-'));
});

afterEach(() => {
  pm?.cleanup();
  pm = null;
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function waitUntil(pred: () => boolean, timeoutMs = 4000): Promise<void> {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const tick = (): void => {
      if (pred()) return resolve();
      if (Date.now() - started > timeoutMs) return reject(new Error('timeout waiting for condition'));
      setTimeout(tick, 10);
    };
    tick();
  });
}

describe('taskTool — per-foreground-subagent abort (U031/w91)', () => {
  it('registers a running foreground subagent as individually abortable and abortForeground stops it', async () => {
    pm = new ProcessManager(tmpDir);
    const agentDef = { name: 'fgagent', systemPrompt: 'be a fg agent', tools: ['file_read'] };

    let capturedAbort: AbortController | undefined;
    let startedTaskId: string | undefined;

    // Mock sub-engine: emit one event, then BLOCK until its own abort signal fires
    // (parity with a real long-running subagent that keeps working until cancelled).
    const engineFactory = (cfg: any) => {
      capturedAbort = cfg.abortController as AbortController;
      return {
        submit: async function* (_p: string) {
          yield { type: 'text_delta', text: 'working…' };
          const sig = cfg.abortController.signal as AbortSignal;
          await new Promise<void>((resolve) => {
            if (sig.aborted) return resolve();
            sig.addEventListener('abort', () => resolve(), { once: true });
          });
          // After abort, end the stream (the task tool's loop treats this as a stop).
        },
      };
    };

    const ctx = {
      cwd: tmpDir,
      abortSignal: new AbortController().signal,
      processManager: pm,
      _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
      _skillManager: { getAgent: () => agentDef, listAgents: () => [agentDef] },
      _engineFactory: engineFactory,
      _emitChild: (ev: any) => {
        if (ev.type === 'subagent_start') startedTaskId = ev.nodeId;
      },
    } as unknown as ToolContext;

    // Start the foreground delegation WITHOUT awaiting — it blocks until aborted.
    const runPromise = taskTool.execute({ prompt: 'do the work', subagent_type: 'fgagent' }, ctx);

    // The subagent must become individually abortable while it runs.
    await waitUntil(() => !!startedTaskId && pm!.hasForegroundAbort(startedTaskId!));
    assert.ok(startedTaskId, 'subagent_start should have emitted a taskId');
    assert.equal(pm!.hasForegroundAbort(startedTaskId!), true,
      'a running foreground subagent must be registered as individually abortable');

    // Abort THIS one subagent (not the whole turn).
    assert.equal(pm!.abortForeground(startedTaskId!), true);
    assert.equal(capturedAbort!.signal.aborted, true,
      'abortForeground must fire the subagent sub-engine AbortController');

    // The delegation resolves (the sub-engine stream ended on abort).
    await runPromise;

    // One-shot + teardown: the registration is gone once the run ends.
    assert.equal(pm!.hasForegroundAbort(startedTaskId!), false);
  });
});
