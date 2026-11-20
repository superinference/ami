import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  resetTeam,
  getCurrentTeam,
  getSelfRole,
  getSelfIdentity,
  canSelfApprovePermission,
  setDynamicTeamContext,
  clearDynamicTeamContext,
} from '../src/tools/team';
import {
  sendMessageTool,
  getMailbox,
  resetAllMailboxes,
  TEAM_LEAD_NAME,
} from '../src/tools/send-message';
import { resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U038 (w83) — worker permission-escalation gate.
// Parity: the reference's WorkerPendingPermission (components/permissions/
// WorkerPendingPermission.tsx L12-15) embodies a privilege-separation rule: a
// WORKER cannot self-approve a permission request; its request is held pending
// and routed UP to the human-supervised team LEADER, who decides. AMI surfaces
// the functional essence as:
//   - canSelfApprovePermission() — the self-identity decision (worker => false),
//     folded onto the getSelfIdentity() introspection aggregate; and
//   - a worker-ONLY guard on send_message's permission_request branch (the lead,
//     being the approver, cannot forward a permission prompt to itself).
// ---------------------------------------------------------------------------

let tmp: string;

function teamCtx(sessionId = 'sess-w83'): ToolContext {
  return { cwd: tmp, sessionId, abortSignal: new AbortController().signal } as ToolContext;
}

describe('U038 w83 — canSelfApprovePermission (worker self-approval gate)', () => {
  beforeEach(() => {
    resetTeam();
    clearDynamicTeamContext();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w83-'));
  });
  afterEach(() => {
    resetTeam();
    clearDynamicTeamContext();
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('a standalone (non-team) session may self-approve', () => {
    assert.equal(getSelfRole(), 'none');
    assert.equal(canSelfApprovePermission(), true);
    assert.equal(getSelfIdentity().canSelfApprovePermission, true);
  });

  it('the team LEAD may self-approve (it IS the approver)', async () => {
    const res = await teamCreateTool.execute({ team_name: 'sq' }, teamCtx());
    assert.equal(res.isError, undefined);
    assert.ok(getCurrentTeam());
    assert.equal(getSelfRole(), 'lead');
    assert.equal(canSelfApprovePermission(), true);
    assert.equal(getSelfIdentity().canSelfApprovePermission, true);
  });

  it('a non-lead WORKER may NOT self-approve — it must escalate to the lead', () => {
    setDynamicTeamContext({
      agentId: 'worker@sq',
      agentName: 'worker',
      teamName: 'sq',
      planModeRequired: false,
    });
    assert.equal(getSelfRole(), 'teammate');
    assert.equal(canSelfApprovePermission(), false);
    assert.equal(getSelfIdentity().canSelfApprovePermission, false);
  });

  it('the gate follows the role: clearing the teammate context restores self-approval', () => {
    setDynamicTeamContext({ agentId: 'worker@sq', agentName: 'worker', teamName: 'sq', planModeRequired: false });
    assert.equal(canSelfApprovePermission(), false);
    clearDynamicTeamContext();
    assert.equal(getSelfRole(), 'none');
    assert.equal(canSelfApprovePermission(), true);
  });
});

describe('U038 w83 — send_message permission_request is worker-only (lead cannot self-escalate)', () => {
  beforeEach(() => { resetAllMailboxes(); resetAgentRegistry(); });
  afterEach(() => { resetAllMailboxes(); resetAgentRegistry(); });

  function ctx(agentId?: string): ToolContext {
    return { cwd: process.cwd(), abortSignal: new AbortController().signal, agentId } as ToolContext;
  }

  it('a WORKER may forward a permission_request up to the lead', async () => {
    const r = await sendMessageTool.execute(
      { to: TEAM_LEAD_NAME, content: '{"type":"permission_request","requestId":"x1","tool":"bash","description":"rm -rf"}' },
      ctx('agent-7'),
    );
    assert.equal(r.isError, undefined);
    assert.ok(r.output.includes('forwarded'));
    assert.equal(getMailbox(TEAM_LEAD_NAME).length, 1);
  });

  it('the LEAD cannot originate a permission_request (it is the approver, nobody to escalate to)', async () => {
    // Addressed to a worker so the self-send guard (lead -> main) does not pre-empt
    // the worker-only gate; a permission_request always routes UP to the lead, so a
    // lead originating one has no one to escalate to.
    const r = await sendMessageTool.execute(
      { to: 'worker', content: '{"type":"permission_request","requestId":"x2","tool":"bash","description":"rm -rf"}' },
      // No agentId => the sender resolves as the lead.
      ctx(),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /lead cannot forward a permission request/);
    // Nothing was queued — the lead's bogus self-escalation never reaches a mailbox.
    assert.equal(getMailbox(TEAM_LEAD_NAME).length, 0);
  });
});
