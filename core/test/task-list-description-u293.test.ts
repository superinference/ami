import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { taskListTool } from '../src/tools/task-list';
import { taskOutputTool } from '../src/tools/task-output';
import { taskKillTool } from '../src/tools/task-kill';

// --------------------------------------------------------------------------
// U293 gap (TaskListTool/prompt.ts:28-48): Claude's TaskList description has a
// "When to Use This Tool" section AND closes the loop by naming the follow-up
// tool a listed id feeds into ("Use TaskGet with a specific task ID…"). AMI's
// task_list was a single terse sentence with no when-to-use guidance and no
// cross-reference to the sibling tools (task_output / task_kill) its returned
// taskId column unlocks. These tests lock the additive teaching in.
// --------------------------------------------------------------------------
describe('task_list description — teaches when-to-use + cross-references follow-up tools (U293)', () => {
  const desc = taskListTool.description;

  it('still states what the tool does (parity floor preserved)', () => {
    assert.match(desc, /background task/i);
    assert.match(desc, /status/i);
  });

  it('teaches WHEN to call it — long-running processes started in the background', () => {
    assert.match(desc, /when to use/i);
    assert.match(desc, /run_in_background/);
  });

  it('cross-references task_output as the read/wait follow-up by taskId', () => {
    assert.match(desc, /task_output/);
    assert.ok(
      taskOutputTool.name === 'task_output',
      'cross-reference must name a real sibling tool',
    );
  });

  it('cross-references task_kill as the stop follow-up by taskId', () => {
    assert.match(desc, /task_kill/);
    assert.ok(
      taskKillTool.name === 'task_kill',
      'cross-reference must name a real sibling tool',
    );
  });

  it('closes the list→inspect/act loop by naming the taskId handoff', () => {
    // The description must tell the model the id it gets back is what feeds the
    // follow-up tools (Claude's "Use TaskGet with a specific task ID").
    assert.match(desc, /taskId/i);
  });
});
