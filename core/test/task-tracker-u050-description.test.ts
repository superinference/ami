import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { taskTrackerTool } from '../src/tools/task-tracker';

// ---------------------------------------------------------------------------
// U050 parity: the richer per-action prompt/affordance guidance Claude Code
// ships on its dedicated TaskCreate/TaskGet/TaskList tool prompts must be
// surfaced on AMI's consolidated task_tracker tool description — the string the
// model always sees — not relegated to a disable-model-invocation skill. These
// assert the exported runtime description (what the engine advertises), not
// source text.
// ---------------------------------------------------------------------------
describe('task_tracker description — U050 affordance/prompt parity', () => {
  const d = taskTrackerTool.description;

  it('frames the user-facing rationale (thoroughness + progress visibility)', () => {
    // Claude TaskCreate prompt L16-17: "demonstrate thoroughness to the user"
    // and "helps the user understand the progress ... of their requests".
    assert.match(d, /thoroughness to the user/i);
    assert.match(d, /progress of their requests/i);
  });

  it('documents teammate authoring: self-contained description + owner assignment', () => {
    // Claude TaskCreate teammate tips: enough detail for another agent; tasks
    // start pending/no owner, assigned via the owner parameter.
    assert.match(d, /enough detail in the description for another agent/i);
    assert.match(d, /no owner/i);
    assert.match(d, /owner parameter/i);
  });

  it('documents the get output fields + verify-blockers-before-start tip', () => {
    // Claude TaskGet prompt: output fields subject/description/status/blocks/
    // blockedBy and "verify its blockedBy list is empty before beginning work".
    assert.match(d, /blocks/);
    assert.match(d, /blockedBy/);
    assert.match(d, /verify blockedBy is empty/i);
    assert.match(d, /summary form/i);
  });

  it('documents when to list + the blocked-cannot-be-claimed rule', () => {
    // Claude TaskList prompt L30-47: when to list + blockedBy tasks cannot be
    // claimed until dependencies resolve.
    assert.match(d, /available work/i);
    assert.match(d, /newly unblocked work/i);
    assert.match(d, /cannot be claimed until those dependencies complete/i);
  });
});
