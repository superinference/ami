import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { taskTool } from '../src/tools/task';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

// ---------------------------------------------------------------------------
// Background agent roster: tokenCount column
//
// repl-components.ts renders a tokenCount column for each roster agent, but the
// background-agent path in task.ts previously never recorded token usage on the
// ProcessManager entry, leaving that render branch permanently dead. The agent
// loop sees usage_update events; it must surface the cumulative totalTokens on
// the entry so the roster column is populated.
// ---------------------------------------------------------------------------

describe('taskTool – background agent tokenCount roster column', () => {
  it('records cumulative totalTokens on the ProcessManager entry', async () => {
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'usage_update' as const, stats: { totalTokens: 1234 } };
      yield { type: 'text_delta' as const, text: 'done' };
    }

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-tok-'));
    const pm = new ProcessManager(tmpDir);
    try {
      const result = await taskTool.execute(
        { prompt: 'bg work with usage', run_in_background: true },
        ctx({
          cwd: tmpDir,
          _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
          _engineFactory: (_cfg: any) => ({ submit: fakeSubmit }),
          processManager: pm,
        }),
      );

      const taskId = result.output.match(/agent-[a-f0-9]+/)?.[0];
      assert.ok(taskId, 'should return an agent task id');

      // Let the detached background loop drain the usage_update + text_delta.
      await new Promise(r => setTimeout(r, 120));

      const entry = pm.list().find(e => e.taskId === taskId);
      assert.ok(entry, 'process entry should exist');
      assert.equal(
        (entry as any).tokenCount,
        1234,
        'background agent must surface cumulative token usage on the roster entry',
      );
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
