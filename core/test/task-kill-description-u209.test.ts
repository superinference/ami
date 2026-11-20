import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskKillTool } from '../src/tools/task-kill';

/**
 * U209 — PARITY (additive): task_kill's description carries explicit
 * when-to-use + parameter/result-contract guidance, mirroring the reference's
 * TaskStop prompt.ts (prompt.ts:3-9):
 *   - "Use this tool when you need to terminate a long-running task"
 *   - "Takes a task_id parameter identifying the task to stop"
 *   - "Returns a success or failure status"
 *
 * The terse two-sentence AMI wording omitted the when-to-use affordance, which
 * helps the model reach for the tool at the right moment.
 *
 * Red-green: without the added clauses these assertions fail; adding the
 * when-to-use + contract sentences turns them green.
 */
describe('U209 — task_kill description usage guidance (parity)', () => {
  it('states when to use the tool (terminate a long-running background task)', () => {
    assert.match(
      taskKillTool.description,
      /use this tool when you need to terminate a long-running/i,
    );
  });

  it('names the task_id parameter contract', () => {
    assert.match(taskKillTool.description, /task_id parameter/i);
  });

  it('states it returns a success or failure status', () => {
    assert.match(taskKillTool.description, /returns a success or failure status/i);
  });
});
