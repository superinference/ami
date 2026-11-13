import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { workflowTool } from '../src/tools/workflow';
import { SUBAGENT_RETURN_DIRECTIVE } from '../src/tools/task';
import { projectStateDir } from '../src/reference-journal';

// Targets the agentHandler path in workflow.ts that only runs when BOTH an
// engine factory AND a processManager are present: taskId/registration (src
// line ~89), the SUBAGENT_RETURN_DIRECTIVE framing (src line ~109), the
// completed/failed finish() writes, and the registered abortFn.

/** Minimal in-process ProcessManager + FlowManager stand-in capturing calls. */
function makeProcessManager() {
  const calls = {
    flows: [] as string[],
    agentsAddedTo: [] as Array<{ flowId: string; taskId: string }>,
    registered: [] as Array<{ taskId: string; opts: any }>,
    updates: [] as Array<{ taskId: string; updates: any }>,
    abortInvoked: 0,
  };
  let flowCounter = 0;
  const pm: any = {
    flowManager: {
      createFlow(name: string) {
        const id = `flow-${++flowCounter}-${name}`;
        calls.flows.push(id);
        return id;
      },
      addAgent(flowId: string, taskId: string) {
        calls.agentsAddedTo.push({ flowId, taskId });
      },
    },
    registerAgent(taskId: string, opts: any) {
      calls.registered.push({ taskId, opts });
      // Exercise the registered abort callback (the ()=>{} abortFn).
      if (typeof opts.abortFn === 'function') {
        opts.abortFn();
        calls.abortInvoked++;
      }
    },
    updateEntry(taskId: string, updates: any) {
      calls.updates.push({ taskId, updates });
    },
  };
  return { pm, calls };
}

describe('workflowTool agentHandler with processManager + engine factory', () => {
  const dirs: string[] = [];
  afterEach(() => { for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true }); });
  const mkcwd = () => { const d = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-ah-')); dirs.push(d); return d; };

  it('registers the agent, frames the prompt, streams text, and finishes completed', async () => {
    const cwd = mkcwd();
    const { pm, calls } = makeProcessManager();

    let submittedPrompt: string | undefined;
    const engineFactory = (_config: any) => ({
      async *submit(prompt: string) {
        submittedPrompt = prompt;
        yield { type: 'text_delta', text: 'Hello ' } as any;
        yield { type: 'text_delta', text: 'world' } as any;
        // a non-text event must be ignored by the accumulator
        yield { type: 'tool_use_start', id: 'x' } as any;
      },
    });

    const script = `export const meta = { name: 'wf-ok', description: 'd' }\nreturn await agent('do the thing', { label: 'MyStep' })`;
    const ctx: any = {
      cwd,
      abortSignal: new AbortController().signal,
      processManager: pm,
      _providerConfig: { provider: 'test' },
      _engineFactory: engineFactory,
    };

    const r = await workflowTool.execute!({ script }, ctx);

    assert.equal(r.isError, false);
    // The streamed deltas became the agent result and the workflow's return value.
    assert.match(r.output, /Hello world/);
    assert.match(r.output, /Workflow 'wf-ok' completed/);

    // A flow was created under the workflow's meta.name and the agent joined it.
    assert.equal(calls.flows.length, 1);
    assert.match(calls.flows[0], /wf-ok$/);
    assert.equal(calls.agentsAddedTo.length, 1);
    assert.equal(calls.agentsAddedTo[0].flowId, calls.flows[0]);

    // The agent was registered with a hex taskId and the label-derived command.
    assert.equal(calls.registered.length, 1);
    const reg = calls.registered[0];
    assert.match(reg.taskId, /^agent-[0-9a-f]{8}$/);
    assert.match(reg.opts.command, /\[agent\] MyStep/);
    assert.match(reg.opts.description, /Workflow agent: MyStep/);
    assert.equal(calls.agentsAddedTo[0].taskId, reg.taskId);

    // The registered abortFn is a real (no-op) function and is invokable.
    assert.equal(calls.abortInvoked, 1);

    // The prompt sent to the sub-engine is the return-and-stop framed prompt.
    assert.ok(submittedPrompt);
    assert.ok(submittedPrompt!.startsWith(SUBAGENT_RETURN_DIRECTIVE));
    assert.match(submittedPrompt!, /do the thing$/);

    // finish('completed', ...) updated the entry and wrote the output file.
    assert.equal(calls.updates.length, 1);
    assert.deepEqual(calls.updates[0].updates, { status: 'completed', exitCode: 0 });
    const outFile = path.join(projectStateDir(cwd, 'tasks'), `${reg.taskId}.out`);
    assert.equal(fs.readFileSync(outFile, 'utf-8'), 'Hello world');
  });

  it('on sub-engine failure finishes failed (exitCode 1) and agent() yields a null result', async () => {
    const cwd = mkcwd();
    const { pm, calls } = makeProcessManager();

    let shutdownCalls = 0;
    const engineFactory = (_config: any) => ({
      // eslint-disable-next-line require-yield
      async *submit(_prompt: string) {
        throw new Error('stream exploded');
      },
      shutdown() { shutdownCalls++; },
    });

    const script = `export const meta = { name: 'wf-fail', description: 'd' }\nreturn await agent('will fail', {})`;
    const ctx: any = {
      cwd,
      abortSignal: new AbortController().signal,
      processManager: pm,
      _providerConfig: { provider: 'test' },
      _engineFactory: engineFactory,
    };

    const r = await workflowTool.execute!({ script }, ctx);

    // agent() swallows the thrown error and resolves null -> workflow completes
    // with no result line.
    assert.equal(r.isError, false);
    assert.doesNotMatch(r.output, /\nResult:/);

    // finish('failed', ...) marked the entry failed with exitCode 1.
    assert.equal(calls.updates.length, 1);
    assert.deepEqual(calls.updates[0].updates, { status: 'failed', exitCode: 1 });

    // The failed agent still wrote its (empty) output as '(no output)'.
    const reg = calls.registered[0];
    const outFile = path.join(projectStateDir(cwd, 'tasks'), `${reg.taskId}.out`);
    assert.equal(fs.readFileSync(outFile, 'utf-8'), '(no output)');

    // shutdown() ran on the failure path too.
    assert.equal(shutdownCalls, 1);
  });
});
