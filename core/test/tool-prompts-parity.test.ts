import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { taskTool } from '../src/tools/task';
import { sendMessageTool } from '../src/tools/send-message';
import { teamCreateTool } from '../src/tools/team';

describe('task tool description sections (gap: usage-guidance sections)', () => {
  const d = taskTool.description;
  it('has a "Writing the prompt" guidance section', () => {
    assert.match(d, /Writing the prompt/);
    assert.match(d, /smart colleague/);
    assert.match(d, /delegate understanding/i);
  });
  it('has a "When NOT to use" section', () => {
    assert.match(d, /When NOT to use/);
    assert.match(d, /file_read/);
  });
  it('has usage notes covering description, concurrency, background, worktree', () => {
    assert.match(d, /3-5 words/);
    assert.match(d, /concurrently/);
    assert.match(d, /run_in_background/);
    assert.match(d, /worktree/);
  });
});

describe('send_message description affordances (gap: messaging affordance hints)', () => {
  const d = sendMessageTool.description;
  it('tells the model plain text is not visible and it MUST call the tool', () => {
    assert.match(d, /not visible/i);
    assert.match(d, /MUST call this tool/);
  });
  it('explains automatic delivery and bare-name addressing', () => {
    assert.match(d, /delivered to you automatically/);
    assert.match(d, /bare name/);
  });
  it('documents the protocol response handshake', () => {
    assert.match(d, /shutdown_request/);
    assert.match(d, /plan_approval_request/);
  });
});

describe('team_create operating guide (gap: rich model-facing team guide)', () => {
  const d = teamCreateTool.description;
  it('covers when-to-use, agent-type selection, and workflow', () => {
    assert.match(d, /When to use/);
    assert.match(d, /read-only agents/i);
    assert.match(d, /Workflow/);
    assert.match(d, /shutdown_request/);
  });
  it('explains automatic message delivery and idle semantics', () => {
    assert.match(d, /automatically/);
    assert.match(d, /idle/);
  });
});
