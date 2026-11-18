// U011 / w99 — per-spawn `share_abort_controller` knob (parity: Claude Code's
// SubagentContextOverrides.shareAbortController). AMI's default (true) cascades a
// parent-turn abort to the child subagent — the safe default that never orphans a
// detached agent. An explicit `false` opts the child OUT of the cascade so a
// long-lived delegation survives the spawning turn's interrupt and is stopped
// explicitly via task_kill / abortForeground.
//
// This drives the real taskTool foreground path with a real ProcessManager and a
// mock sub-engine that blocks until its own abort signal fires, so we can observe
// whether aborting the PARENT turn cascades into the child's sub-engine controller.
//
// RED on revert: remove the `share_abort_controller ? … : () => {}` branch (always
// linking) and the opt-out test fails — the child's controller fires on the parent
// abort even though the caller asked for an independent controller.

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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-share-abort-'));
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

interface Harness {
  ctx: ToolContext;
  parent: AbortController;
  getChildAbort: () => AbortController | undefined;
  getStartedTaskId: () => string | undefined;
}

function makeHarness(): Harness {
  const agentDef = { name: 'sa_agent', systemPrompt: 'be an agent', tools: ['file_read'] };
  let capturedAbort: AbortController | undefined;
  let startedTaskId: string | undefined;
  const parent = new AbortController();

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
      },
    };
  };

  const ctx = {
    cwd: tmpDir,
    abortSignal: parent.signal,
    processManager: pm!,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
    _skillManager: { getAgent: () => agentDef, listAgents: () => [agentDef] },
    _engineFactory: engineFactory,
    _emitChild: (ev: any) => {
      if (ev.type === 'subagent_start') startedTaskId = ev.nodeId;
    },
  } as unknown as ToolContext;

  return { ctx, parent, getChildAbort: () => capturedAbort, getStartedTaskId: () => startedTaskId };
}

describe('taskTool — share_abort_controller (U011/w99)', () => {
  it('advertises the share_abort_controller boolean parameter', () => {
    const props = taskTool.inputSchema.properties as Record<string, { type?: string }>;
    assert.ok(props.share_abort_controller, 'share_abort_controller property present');
    assert.equal(props.share_abort_controller.type, 'boolean');
  });

  it('DEFAULT: a parent-turn abort cascades into the child sub-engine controller', async () => {
    pm = new ProcessManager(tmpDir);
    const h = makeHarness();

    const runPromise = taskTool.execute({ prompt: 'do the work', subagent_type: 'sa_agent' }, h.ctx);
    await waitUntil(() => !!h.getChildAbort() && !!h.getStartedTaskId());

    assert.equal(h.getChildAbort()!.signal.aborted, false, 'child not aborted before parent abort');
    h.parent.abort();
    assert.equal(h.getChildAbort()!.signal.aborted, true,
      'default (shared) must cascade the parent abort into the child controller');

    await runPromise;
  });

  it('OPT-OUT (false): the child controller is INDEPENDENT — a parent abort does NOT cascade', async () => {
    pm = new ProcessManager(tmpDir);
    const h = makeHarness();

    const runPromise = taskTool.execute(
      { prompt: 'do the work', subagent_type: 'sa_agent', share_abort_controller: false },
      h.ctx,
    );
    const taskId = await (async () => {
      await waitUntil(() => !!h.getChildAbort() && !!h.getStartedTaskId() && pm!.hasForegroundAbort(h.getStartedTaskId()!));
      return h.getStartedTaskId()!;
    })();

    // Aborting the parent turn must NOT reach the opted-out child.
    h.parent.abort();
    // Give any (buggy) cascade a tick to fire.
    await new Promise((r) => setTimeout(r, 30));
    assert.equal(h.getChildAbort()!.signal.aborted, false,
      'an independent child controller must survive the parent-turn abort');

    // It is still stoppable explicitly (task_kill / abortForeground path).
    assert.equal(pm!.abortForeground(taskId), true);
    assert.equal(h.getChildAbort()!.signal.aborted, true,
      'explicit abortForeground still stops the independent child');

    await runPromise;
  });

  it('rejects a non-boolean share_abort_controller', async () => {
    pm = new ProcessManager(tmpDir);
    const h = makeHarness();
    const result = await taskTool.execute(
      { prompt: 'x', subagent_type: 'sa_agent', share_abort_controller: 'yes' },
      h.ctx,
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('share_abort_controller must be a boolean'),
      'malformed value is rejected with a clear message');
  });
});
