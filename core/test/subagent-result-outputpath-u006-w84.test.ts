import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { taskTool, resetAgentRegistry } from '../src/tools/task';
import { projectStateDir } from '../src/reference-journal';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext, EngineEvent } from '../src/types';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ===========================================================================
// U006-w84 — subagent_result carries `outputPath` for the spilled agent output.
//
// Parity with Claude Code's task terminal notification `output_file`: a stream
// consumer needs to locate the file holding the (auto-backgrounded) agent's
// output without a separate task_output call. AMI background agents already write
// to `<project>/tasks/<taskId>.output`, but the subagent_result event exposed no
// path to it. The auto-background promotion now stamps `outputPath` on the event.
// ===========================================================================

describe('U006-w84 — subagent_result.outputPath on auto-background promotion', () => {
  let tmp: string;
  beforeEach(() => {
    resetAgentRegistry();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u006w84-'));
    process.env.AMI_AUTO_BACKGROUND_TASKS = '1';
    process.env.AMI_AUTO_BACKGROUND_MS = '30';
  });
  afterEach(() => {
    delete process.env.AMI_AUTO_BACKGROUND_TASKS;
    delete process.env.AMI_AUTO_BACKGROUND_MS;
    fs.rmSync(tmp, { recursive: true, force: true });
  });

  it('stamps the task output-file path on the promoted subagent_result event', async () => {
    const pm = new ProcessManager(tmp);
    const childEvents: Array<Record<string, unknown>> = [];
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
      {
        cwd: tmp,
        abortSignal: new AbortController().signal,
        allowedReadRoots: [tmp],
        processManager: pm as unknown as ToolContext['processManager'],
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: factory,
        _emitChild: (e: Record<string, unknown>) => { childEvents.push(e); },
      } as unknown as ToolContext,
    );

    assert.ok(!r.isError, r.output);
    assert.match(r.output, /auto-backgrounded/i, r.output);
    const taskId = r.output.match(/agent-[0-9a-f]{16}/)![0];

    const result = childEvents.find((e) => e['type'] === 'subagent_result');
    assert.ok(result, 'a subagent_result event must be emitted on promotion');
    const expected = path.join(projectStateDir(tmp, 'tasks'), `${taskId}.output`);
    assert.equal(result!['outputPath'], expected);

    // Let the background drain finish so no timers dangle past the test.
    for (let i = 0; i < 50; i++) {
      await sleep(30);
      if (pm.getStatus(taskId) === 'completed') break;
    }
  });
});
