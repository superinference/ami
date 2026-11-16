/**
 * U022 (structured-protocol extension) — task_assignment, idle_notification,
 * mode_set_request message schemas; plan_approval_response.permissionMode
 * inheritance; and default guidance on a plan rejection with empty feedback.
 *
 * Covers parse (schema validation), summarize (concise relay), and execute
 * (routing + lead-only authorization) for each new capability.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  getMailbox,
  resetAllMailboxes,
  parseStructuredMessage,
  summarizeStructuredMessage,
  isStructuredProtocolMessage,
  TEAM_LEAD_NAME,
} from '../src/tools/send-message';
import { resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';

function ctx(agentId?: string): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, agentId } as ToolContext;
}

beforeEach(() => { resetAllMailboxes(); resetAgentRegistry(); });
afterEach(() => { resetAllMailboxes(); resetAgentRegistry(); });

// ---------------------------------------------------------------------------
// task_assignment schema (gaps #55/#70/#71)
// ---------------------------------------------------------------------------
describe('task_assignment message', () => {
  it('parses a well-formed task_assignment', () => {
    const m = parseStructuredMessage(JSON.stringify({
      type: 'task_assignment', taskId: '42', subject: 'Fix the parser', description: 'details here',
    }));
    assert.ok(m && !('error' in m));
    assert.equal(m!.type, 'task_assignment');
    assert.equal((m as any).taskId, '42');
    assert.equal((m as any).subject, 'Fix the parser');
  });

  it('rejects a task_assignment missing taskId or subject', () => {
    const noId = parseStructuredMessage(JSON.stringify({ type: 'task_assignment', subject: 's' }));
    assert.ok(noId && 'error' in noId);
    const noSubj = parseStructuredMessage(JSON.stringify({ type: 'task_assignment', taskId: '1' }));
    assert.ok(noSubj && 'error' in noSubj);
  });

  it('summarizes with the canonical [Task Assigned] #<id> - <subject> form', () => {
    const s = summarizeStructuredMessage(JSON.stringify({
      type: 'task_assignment', taskId: '7', subject: 'Write tests',
    }));
    assert.equal(s, '[Task Assigned] #7 - Write tests');
  });

  it('routes to the named teammate and stamps assignedBy from the trusted sender', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'task_assignment', taskId: '9', subject: 'Deploy', assignedBy: 'SPOOF' }) },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Task #9 "Deploy" assigned to worker/);
    const box = getMailbox('worker');
    assert.equal(box.length, 1);
    const wire = JSON.parse(box[0].content);
    // assignedBy is stamped from the trusted sender (main), not the model-supplied SPOOF.
    assert.equal(wire.assignedBy, TEAM_LEAD_NAME);
    assert.equal(wire.description, '');
  });

  it('is recognized as a structured protocol message', () => {
    assert.equal(isStructuredProtocolMessage(JSON.stringify({ type: 'task_assignment', taskId: '1', subject: 's' })), true);
  });
});

// ---------------------------------------------------------------------------
// idle_notification schema (gaps #52/#59/#61)
// ---------------------------------------------------------------------------
describe('idle_notification message', () => {
  it('parses an empty idle_notification (all fields optional)', () => {
    const m = parseStructuredMessage(JSON.stringify({ type: 'idle_notification' }));
    assert.ok(m && !('error' in m));
    assert.equal(m!.type, 'idle_notification');
  });

  it('parses full fields', () => {
    const m = parseStructuredMessage(JSON.stringify({
      type: 'idle_notification', idleReason: 'available', completedTaskId: '3',
      completedStatus: 'resolved', summary: 'done it',
    }));
    assert.ok(m && !('error' in m));
    assert.equal((m as any).idleReason, 'available');
    assert.equal((m as any).completedStatus, 'resolved');
  });

  it('rejects an invalid idleReason / completedStatus enum', () => {
    const badReason = parseStructuredMessage(JSON.stringify({ type: 'idle_notification', idleReason: 'napping' }));
    assert.ok(badReason && 'error' in badReason);
    const badStatus = parseStructuredMessage(JSON.stringify({ type: 'idle_notification', completedStatus: 'maybe' }));
    assert.ok(badStatus && 'error' in badStatus);
  });

  it('summarizes with reason + completed + summary joined by " · "', () => {
    const s = summarizeStructuredMessage(
      JSON.stringify({ type: 'idle_notification', idleReason: 'failed', completedTaskId: '5', completedStatus: 'blocked', failureReason: 'oom', summary: 'ran out' }),
      'worker',
    );
    assert.equal(s, '[Teammate Idle worker] (failed) — #5 blocked · failed: oom · ran out');
  });

  it('completedStatus defaults to "completed" in the summary when omitted', () => {
    const s = summarizeStructuredMessage(JSON.stringify({ type: 'idle_notification', completedTaskId: '8' }), 'w');
    assert.equal(s, '[Teammate Idle w] — #8 completed');
  });

  it('routes to the lead and rejects a non-lead target', async () => {
    const ok = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: JSON.stringify({ type: 'idle_notification', idleReason: 'available' }) },
      ctx('agent-1'),
    );
    assert.equal(ok.isError, undefined);
    assert.match(ok.output, /Idle notification sent to main \(available\)/);
    assert.equal(getMailbox(TEAM_LEAD_NAME).length, 1);

    const bad = await sendMessageTool.execute(
      { to: 'peer', content: JSON.stringify({ type: 'idle_notification' }) },
      ctx('agent-1'),
    );
    assert.equal(bad.isError, true);
    assert.match(bad.output, /must be sent to "main"/);
  });
});

// ---------------------------------------------------------------------------
// mode_set_request schema (gap #57)
// ---------------------------------------------------------------------------
describe('mode_set_request message', () => {
  it('parses + normalizes a valid mode (claude "default" alias -> ask)', () => {
    const m = parseStructuredMessage(JSON.stringify({ type: 'mode_set_request', mode: 'default' }));
    assert.ok(m && !('error' in m));
    assert.equal((m as any).mode, 'ask');
  });

  it('rejects an unknown mode / missing mode', () => {
    const bad = parseStructuredMessage(JSON.stringify({ type: 'mode_set_request', mode: 'wideopen' }));
    assert.ok(bad && 'error' in bad);
    const missing = parseStructuredMessage(JSON.stringify({ type: 'mode_set_request' }));
    assert.ok(missing && 'error' in missing);
  });

  it('summarizes', () => {
    const s = summarizeStructuredMessage(JSON.stringify({ type: 'mode_set_request', mode: 'plan' }), 'main');
    assert.equal(s, '[Mode Set Request] from main → plan');
  });

  it('lead may route to a teammate; a non-lead is refused', async () => {
    const ok = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'mode_set_request', mode: 'auto-allow' }) },
      ctx(),
    );
    assert.equal(ok.isError, undefined);
    assert.match(ok.output, /Permission mode "auto-allow" requested for worker/);
    assert.equal(getMailbox('worker').length, 1);

    const refused = await sendMessageTool.execute(
      { to: 'other', content: JSON.stringify({ type: 'mode_set_request', mode: 'plan' }) },
      ctx('agent-9'),
    );
    assert.equal(refused.isError, true);
    assert.match(refused.output, /only the team lead/);
  });
});

// ---------------------------------------------------------------------------
// plan_approval_response.permissionMode inheritance (gaps #29/#98)
// ---------------------------------------------------------------------------
describe('plan_approval_response permissionMode', () => {
  it('parses and normalizes an inherited permissionMode', () => {
    const m = parseStructuredMessage(JSON.stringify({
      type: 'plan_approval_response', requestId: 'plan-1@w', approve: true, permissionMode: 'acceptedits',
    }));
    assert.ok(m && !('error' in m));
    assert.equal((m as any).permissionMode, 'acceptEdits');
  });

  it('rejects a non-string / unknown permissionMode', () => {
    const bad = parseStructuredMessage(JSON.stringify({
      type: 'plan_approval_response', requestId: 'plan-1@w', approve: true, permissionMode: 'turbo',
    }));
    assert.ok(bad && 'error' in bad);
    const notStr = parseStructuredMessage(JSON.stringify({
      type: 'plan_approval_response', requestId: 'plan-1@w', approve: true, permissionMode: 5,
    }));
    assert.ok(notStr && 'error' in notStr);
  });

  it('surfaces the granted mode in the approval output + summary', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'plan_approval_response', requestId: 'plan-2@worker', approve: true, permissionMode: 'plan' }) },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Plan approved for worker .*\(mode: plan\)/);
    const s = summarizeStructuredMessage(JSON.stringify({ type: 'plan_approval_response', requestId: 'x', approve: true, permissionMode: 'plan' }));
    assert.match(s!, /\[Plan Approved\].*\(mode: plan\)/);
  });
});

// ---------------------------------------------------------------------------
// Default guidance on plan rejection with empty feedback (gaps #66/#67)
// ---------------------------------------------------------------------------
describe('plan rejection default guidance', () => {
  it('summary falls back to "Please revise your plan" when feedback is empty', () => {
    const s = summarizeStructuredMessage(JSON.stringify({ type: 'plan_approval_response', requestId: 'x', approve: false }));
    assert.equal(s, '[Plan Rejected] Please revise your plan — revise and call ExitPlanMode again');
  });

  it('summary uses explicit feedback when given', () => {
    const s = summarizeStructuredMessage(JSON.stringify({ type: 'plan_approval_response', requestId: 'x', approve: false, feedback: 'add tests' }));
    assert.equal(s, '[Plan Rejected] add tests — revise and call ExitPlanMode again');
  });

  it('execute output carries the default guidance + ExitPlanMode affordance (no feedback required)', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', content: JSON.stringify({ type: 'plan_approval_response', requestId: 'plan-3@worker', approve: false }) },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Plan rejected for worker/);
    assert.match(r.output, /Please revise your plan; revise and call ExitPlanMode again/);
  });
});
