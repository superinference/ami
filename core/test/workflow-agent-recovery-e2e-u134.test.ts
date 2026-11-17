import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { workflowTool } from '../src/tools/workflow';
import type { ToolContext } from '../src/types';

// U134 E2E — per-agent recovery wired through the REAL workflow tool.
//
// Proves the full path: workflowTool → executeWorkflow(onRuntime) → the per-agent
// recovery callbacks (retryFn / skipFn) registered on the process-manager entry →
// WorkflowRuntime.skipAgent / retryAgent. A single wedged workflow agent is
// recovered without aborting the whole run (parity: onSkipAgent / onRetryAgent).

const dirs: string[] = [];
afterEach(() => { for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true }); });
function mkcwd(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-recover-'));
  dirs.push(d);
  return d;
}

interface Reg { id: string; opts: { command: string; description: string; outputPath: string; abortFn: () => void; retryFn?: () => void; skipFn?: () => void } }

/** Minimal fake ProcessManager that captures the FULL registerAgent opts
 *  (including the new retryFn / skipFn recovery callbacks). */
function fakePm() {
  const registered: Reg[] = [];
  return {
    registered,
    flowManager: undefined as unknown,
    registerAgent(id: string, opts: Reg['opts']) { registered.push({ id, opts }); },
    updateEntry() { /* ignore */ },
  };
}

function ctxWith(cwd: string, submitGen: (config: any, call: number) => AsyncIterable<any>, pm: ReturnType<typeof fakePm>): ToolContext {
  let call = 0;
  const ctx: any = {
    cwd,
    abortSignal: new AbortController().signal,
    processManager: pm,
    _providerConfig: { provider: 'test', model: 'base-model', baseUrl: 'u', apiKey: 'k' },
    _engineFactory: (config: any) => {
      const thisCall = call++;
      return { submit: () => submitGen(config, thisCall), shutdown() {} };
    },
  };
  return ctx as ToolContext;
}

/** A submit stream that signals it is live (via `onLive`) then ends once its
 *  per-agent abort fires — modelling a wedged agent that only stops when a
 *  per-agent recovery action tears it down. */
function hangUntilAbort(onLive: () => void) {
  return async function* (config: any): AsyncIterable<any> {
    onLive();
    await new Promise<void>((resolve) => {
      if (config.abortController?.signal.aborted) return resolve();
      config.abortController?.signal.addEventListener('abort', () => resolve(), { once: true });
    });
  };
}

describe('U134 E2E — per-agent skip through the workflow tool', () => {
  it('skipFn settles ONE wedged agent as null and the workflow completes', async () => {
    const cwd = mkcwd();
    const pm = fakePm();
    const ctx = ctxWith(cwd, hangUntilAbort(() => {
      // The first (only) agent is live and registered — skip it like a user would.
      const reg = pm.registered[0];
      assert.ok(typeof reg.opts.skipFn === 'function', 'skipFn must be wired onto the pm entry');
      reg.opts.skipFn!();
    }), pm);
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do')";
    const r = await workflowTool.execute!({ script }, ctx);
    assert.equal(r.isError, false);
    assert.match(r.output, /completed/);
    assert.match(r.output, /Agents spawned: 1/);
  });
});

describe('U134 E2E — per-agent retry through the workflow tool', () => {
  it('retryFn re-runs the wedged agent; the second attempt succeeds', async () => {
    const cwd = mkcwd();
    const pm = fakePm();
    const ctx = ctxWith(cwd, (config, call) => {
      if (call === 0) {
        // First attempt wedges, then is retried via the registered callback.
        return hangUntilAbort(() => {
          const reg = pm.registered[0];
          assert.ok(typeof reg.opts.retryFn === 'function', 'retryFn must be wired onto the pm entry');
          reg.opts.retryFn!();
        })(config);
      }
      // Second attempt (same agent, re-run) produces output.
      return (async function* () { yield { type: 'text_delta', text: 'recovered' }; })();
    }, pm);
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do')";
    const r = await workflowTool.execute!({ script }, ctx);
    assert.equal(r.isError, false);
    assert.match(r.output, /recovered/);
    // The retried agent is the SAME agent — the run still reports one agent.
    assert.match(r.output, /Agents spawned: 1/);
  });
});
