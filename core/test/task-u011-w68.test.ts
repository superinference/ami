import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { taskTool, resetAgentRegistry, getAutoBackgroundMs } from '../src/tools/task';
import { projectStateDir } from '../src/reference-journal';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext, EngineEvent } from '../src/types';

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

// ===========================================================================
// U011-w68 — Auto-background of long-running foreground subagents.
//
// Parity with the reference's AgentTool getAutoBackgroundMs()/autoBackgroundMs
// (AgentTool.tsx:72-77, 808-833, ~1195): a foreground agent that keeps running
// past a configured threshold is automatically promoted to a background task so
// the parent turn is not blocked indefinitely. AMI previously declared the
// AUTO_BG_MS = 120_000 constant but left it DEAD (eslint-disabled, unreferenced).
// ===========================================================================

describe('U011-w68 — getAutoBackgroundMs env knob', () => {
  const saved = { ...process.env };
  afterEach(() => {
    delete process.env.AMI_AUTO_BACKGROUND_TASKS;
    delete process.env.AMI_AUTO_BACKGROUND_MS;
    Object.assign(process.env, saved);
  });

  it('is disabled (0) by default — no auto-background when the knob is unset', () => {
    assert.equal(getAutoBackgroundMs({}), 0);
  });

  it('returns the 120s default when AMI_AUTO_BACKGROUND_TASKS is truthy', () => {
    assert.equal(getAutoBackgroundMs({ AMI_AUTO_BACKGROUND_TASKS: '1' }), 120_000);
    assert.equal(getAutoBackgroundMs({ AMI_AUTO_BACKGROUND_TASKS: 'true' }), 120_000);
  });

  it('honors an explicit AMI_AUTO_BACKGROUND_MS override (positive-int only)', () => {
    assert.equal(getAutoBackgroundMs({ AMI_AUTO_BACKGROUND_TASKS: '1', AMI_AUTO_BACKGROUND_MS: '5000' }), 5000);
    // a non-positive / garbage override falls back to the 120s default (parsePositiveIntEnv)
    assert.equal(getAutoBackgroundMs({ AMI_AUTO_BACKGROUND_TASKS: '1', AMI_AUTO_BACKGROUND_MS: '-5' }), 120_000);
    assert.equal(getAutoBackgroundMs({ AMI_AUTO_BACKGROUND_TASKS: '1', AMI_AUTO_BACKGROUND_MS: 'x' }), 120_000);
  });

  it('stays disabled when the override is set but the master knob is off', () => {
    assert.equal(getAutoBackgroundMs({ AMI_AUTO_BACKGROUND_MS: '5000' }), 0);
  });

  it('defaults to reading process.env when called with no argument', () => {
    delete process.env.AMI_AUTO_BACKGROUND_TASKS;
    assert.equal(getAutoBackgroundMs(), 0);
  });
});

describe('U011-w68 — auto-background promotion of a long-running foreground subagent', () => {
  let tmp: string;
  beforeEach(() => {
    resetAgentRegistry();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u011w68-'));
    process.env.AMI_AUTO_BACKGROUND_TASKS = '1';
    process.env.AMI_AUTO_BACKGROUND_MS = '30';
  });
  afterEach(() => {
    delete process.env.AMI_AUTO_BACKGROUND_TASKS;
    delete process.env.AMI_AUTO_BACKGROUND_MS;
    fs.rmSync(tmp, { recursive: true, force: true });
  });

  it('promotes a still-running foreground subagent to the background and returns a task_id', async () => {
    const pm = new ProcessManager(tmp);
    // A subagent that emits once, then keeps the turn alive well past the 30ms
    // auto-background threshold before finishing.
    const factory = () => ({
      submit: async function* (): AsyncGenerator<EngineEvent> {
        yield { type: 'text_delta', text: 'partial ' } as EngineEvent;
        await sleep(150);
        yield { type: 'text_delta', text: 'done-text' } as EngineEvent;
        yield { type: 'done', exitReason: 'completed' } as unknown as EngineEvent;
      },
      shutdown() {},
    });

    const r = await taskTool.execute(
      { prompt: 'long task', description: 'long one' },
      ctx({
        cwd: tmp,
        allowedReadRoots: [tmp],
        processManager: pm as unknown as ToolContext['processManager'],
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: factory,
      } as unknown as Partial<ToolContext>),
    );

    assert.ok(!r.isError, `promotion must not be an error: ${r.output}`);
    assert.match(r.output, /Background agent started/i, r.output);
    assert.match(r.output, /auto-backgrounded/i, r.output);
    const m = r.output.match(/agent-[0-9a-f]{16}/);
    assert.ok(m, `must surface a task_id, got: ${r.output}`);
    const taskId = m![0];

    // The promoted agent is registered with the parent's process manager so it is
    // visible to task_list / task_output / task_kill (introspection surface).
    assert.ok(pm.get(taskId), 'promoted agent must be registered with the process manager');

    // The drain keeps running in the background and writes the remaining output to
    // the task output file; wait for it to finish.
    const outputPath = path.join(projectStateDir(tmp, 'tasks'), `${taskId}.output`);
    let out = '';
    for (let i = 0; i < 50; i++) {
      await sleep(30);
      out = fs.existsSync(outputPath) ? fs.readFileSync(outputPath, 'utf-8') : '';
      if (/done-text/.test(out)) break;
    }
    assert.match(out, /done-text/, `background drain must stream the rest to the output file, got: ${JSON.stringify(out)}`);
    // The output accumulated before promotion is preserved, not lost.
    assert.match(out, /partial/, 'pre-promotion output must be preserved in the task file');
    assert.equal(pm.getStatus(taskId), 'completed');
  });

  it('does NOT promote a fast foreground subagent (completes before threshold)', async () => {
    const pm = new ProcessManager(tmp);
    const factory = () => ({
      submit: async function* (): AsyncGenerator<EngineEvent> {
        yield { type: 'text_delta', text: 'quick answer' } as EngineEvent;
        yield { type: 'done', exitReason: 'completed' } as unknown as EngineEvent;
      },
      shutdown() {},
    });
    const r = await taskTool.execute(
      { prompt: 'fast task', description: 'fast one' },
      ctx({
        cwd: tmp,
        allowedReadRoots: [tmp],
        processManager: pm as unknown as ToolContext['processManager'],
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: factory,
      } as unknown as Partial<ToolContext>),
    );
    assert.ok(!r.isError, r.output);
    assert.doesNotMatch(r.output, /Background agent started/i, 'a fast subagent must stay foreground');
    assert.match(r.output, /quick answer/, r.output);
  });

  it('never promotes when the knob is disabled, even on a long run', async () => {
    delete process.env.AMI_AUTO_BACKGROUND_TASKS;
    delete process.env.AMI_AUTO_BACKGROUND_MS;
    const pm = new ProcessManager(tmp);
    const factory = () => ({
      submit: async function* (): AsyncGenerator<EngineEvent> {
        yield { type: 'text_delta', text: 'slow ' } as EngineEvent;
        await sleep(80);
        yield { type: 'text_delta', text: 'but foreground' } as EngineEvent;
        yield { type: 'done', exitReason: 'completed' } as unknown as EngineEvent;
      },
      shutdown() {},
    });
    const r = await taskTool.execute(
      { prompt: 'slow task', description: 'slow one' },
      ctx({
        cwd: tmp,
        allowedReadRoots: [tmp],
        processManager: pm as unknown as ToolContext['processManager'],
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: factory,
      } as unknown as Partial<ToolContext>),
    );
    assert.ok(!r.isError, r.output);
    assert.doesNotMatch(r.output, /Background agent started/i, 'disabled knob must keep the agent foreground');
    assert.match(r.output, /but foreground/, r.output);
  });
});
