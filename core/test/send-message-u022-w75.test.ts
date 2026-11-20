/**
 * U022 wave-75 additions to the teammate send_message subsystem. Each block maps to
 * a confirmed the reference parity gap that was NOT yet implemented on the file:
 *
 *  - errorCode-carrying input-shape validation — tools/SendMessageTool/
 *    SendMessageTool.ts validateInput returns `errorCode: 9` for every input-shape
 *    rejection (empty `to`, empty scheme-address target, an `@` in a bare `to`, a
 *    missing message). AMI validated inline but returned a bare {isError} with no
 *    machine-readable code; this attaches `metadata.errorCode` (the same structured
 *    channel cron-delete uses) so a consumer can distinguish a malformed-input
 *    rejection from a downstream routing error.
 *  - Richer forwarded-permission schema (PermissionRequest.toolUseId/input,
 *    PermissionResponse.resolvedBy) — utils/swarm/permissionSync.ts
 *    SwarmPermissionRequest carries the pending tool_use id + the tool INPUT the
 *    lead is asked to approve, and the resolution records who resolved it. AMI's
 *    forwarded permission messages carried only tool/description; these add the
 *    args + correlation id + resolvedBy, surfaced through the summary relay.
 *  - Proactive teammate_terminated push on abnormal exit (notifyLeadTeammateTerminated,
 *    wired via markTeammateIdle terminalStatus) — components/teams/TeamsDialog.tsx
 *    killTeammate injects a teammate_terminated system message into the lead's inbox
 *    with always-run cleanup. AMI pushed an idle/available notice on EVERY finish;
 *    this routes a killed/failed exit to a termination notice instead (an orphaned
 *    peer is not "available"), surfaced silently by the receive-side relay.
 *
 * Red-green: the imported symbols do not exist before the fix; the markTeammateIdle
 * terminalStatus wiring pushes teammate_terminated only after the change.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  sendMessageTool,
  getMailbox,
  resetAllMailboxes,
  parseStructuredMessage,
  summarizeStructuredMessage,
  relayIncomingMessage,
  notifyLeadTeammateTerminated,
  compactInput,
  SEND_MESSAGE_VALIDATION_ERROR_CODE,
  MAX_INPUT_PREVIEW_CHARS,
  TEAM_LEAD_NAME,
  type PermissionRequest,
  type PermissionResponse,
  type TeammateTerminated,
} from '../src/tools/send-message';
import { markTeammateIdle } from '../src/tools/task';
import { resetAgentRegistry } from '../src/tools/task';
import { teamCreateTool, getCurrentTeam, resetTeam, registerTeamMember } from '../src/tools/team';
import type { ToolContext } from '../src/types';

function ctx(agentId?: string): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, agentId } as ToolContext;
}

beforeEach(() => { resetAllMailboxes(); resetAgentRegistry(); });
afterEach(() => { resetAllMailboxes(); resetAgentRegistry(); });

// ---------------------------------------------------------------------------
// errorCode-carrying input-shape validation
// ---------------------------------------------------------------------------
describe('U022-w75 errorCode on input-shape validation', () => {
  it('exposes the reference-parity errorCode constant (9)', () => {
    assert.equal(SEND_MESSAGE_VALIDATION_ERROR_CODE, 9);
  });

  it('attaches metadata.errorCode to an empty "to"', async () => {
    const r = await sendMessageTool.execute({ to: '   ', content: 'hi' }, ctx());
    assert.equal(r.isError, true);
    assert.equal(r.metadata?.errorCode, 9);
  });

  it('attaches metadata.errorCode to empty content', async () => {
    const r = await sendMessageTool.execute({ to: 'worker', content: '  ' }, ctx());
    assert.equal(r.isError, true);
    assert.equal(r.metadata?.errorCode, 9);
  });

  it('attaches metadata.errorCode to an "@"-bearing bare recipient', async () => {
    const r = await sendMessageTool.execute({ to: 'worker@host', content: 'hi' }, ctx());
    assert.equal(r.isError, true);
    assert.equal(r.metadata?.errorCode, 9);
    assert.match(r.output, /@/);
  });

  it('attaches metadata.errorCode to an empty scheme-address target', async () => {
    const r = await sendMessageTool.execute({ to: 'uds:', content: 'hi' }, ctx());
    assert.equal(r.isError, true);
    assert.equal(r.metadata?.errorCode, 9);
  });

  it('does NOT attach errorCode to a successful send', async () => {
    const r = await sendMessageTool.execute({ to: 'worker', content: 'hello there', summary: 's' }, ctx());
    assert.equal(r.isError, undefined);
    assert.equal(r.metadata?.errorCode, undefined);
  });
});

// ---------------------------------------------------------------------------
// Richer forwarded-permission schema
// ---------------------------------------------------------------------------
describe('U022-w75 PermissionRequest toolUseId + input', () => {
  it('parses toolUseId and a plain-object input', () => {
    const m = parseStructuredMessage(JSON.stringify({
      type: 'permission_request', tool: 'bash', description: 'run a build',
      toolUseId: 'tu-7', input: { command: 'npm run build' },
    })) as PermissionRequest;
    assert.equal(m.type, 'permission_request');
    assert.equal(m.toolUseId, 'tu-7');
    assert.deepEqual(m.input, { command: 'npm run build' });
  });

  it('drops a non-object input (string/array) rather than forwarding it', () => {
    const a = parseStructuredMessage('{"type":"permission_request","tool":"bash","input":"rm -rf /"}') as PermissionRequest;
    assert.equal(a.input, undefined);
    const b = parseStructuredMessage('{"type":"permission_request","tool":"bash","input":[1,2]}') as PermissionRequest;
    assert.equal(b.input, undefined);
  });

  it('still parses a minimal request with neither field', () => {
    const m = parseStructuredMessage('{"type":"permission_request","tool":"bash"}') as PermissionRequest;
    assert.equal(m.tool, 'bash');
    assert.equal(m.toolUseId, undefined);
    assert.equal(m.input, undefined);
  });

  it('surfaces a compact args projection in the relayed summary', () => {
    const summary = summarizeStructuredMessage(JSON.stringify({
      type: 'permission_request', tool: 'bash', description: 'build', input: { command: 'make' },
    }));
    assert.match(summary!, /tool "bash"/);
    assert.match(summary!, /args: \{"command":"make"\}/);
  });

  it('forwards toolUseId/input end-to-end through the lead mailbox (auto request id)', async () => {
    await sendMessageTool.execute({
      to: TEAM_LEAD_NAME,
      content: JSON.stringify({ type: 'permission_request', tool: 'bash', description: 'd', toolUseId: 'tu-9', input: { command: 'ls' } }),
    }, ctx('w-1'));
    const box = getMailbox(TEAM_LEAD_NAME);
    assert.equal(box.length, 1);
    const parsed = parseStructuredMessage(box[0].content) as PermissionRequest;
    assert.equal(parsed.toolUseId, 'tu-9');
    assert.deepEqual(parsed.input, { command: 'ls' });
    assert.ok(parsed.requestId, 'an id is auto-generated when omitted');
  });
});

describe('U022-w75 PermissionResponse resolvedBy', () => {
  it('parses the resolvedBy enum', () => {
    const leader = parseStructuredMessage('{"type":"permission_response","requestId":"r1","approve":true,"resolvedBy":"leader"}') as PermissionResponse;
    assert.equal(leader.resolvedBy, 'leader');
    const worker = parseStructuredMessage('{"type":"permission_response","requestId":"r1","approve":false,"resolvedBy":"worker","feedback":"self-denied"}') as PermissionResponse;
    assert.equal(worker.resolvedBy, 'worker');
  });

  it('drops an invalid resolvedBy value', () => {
    const m = parseStructuredMessage('{"type":"permission_response","requestId":"r1","approve":true,"resolvedBy":"nobody"}') as PermissionResponse;
    assert.equal(m.resolvedBy, undefined);
  });

  it('surfaces "(resolved by worker)" in the summary when self-resolved', () => {
    const granted = summarizeStructuredMessage('{"type":"permission_response","requestId":"r1","approve":true,"resolvedBy":"worker"}');
    assert.match(granted!, /\(resolved by worker\)/);
    const denied = summarizeStructuredMessage('{"type":"permission_response","requestId":"r1","approve":false,"resolvedBy":"worker"}');
    assert.match(denied!, /\(resolved by worker\)/);
    // A leader (default) resolution carries no "resolved by" tag.
    const leader = summarizeStructuredMessage('{"type":"permission_response","requestId":"r1","approve":true}');
    assert.doesNotMatch(leader!, /resolved by/);
  });
});

describe('U022-w75 compactInput', () => {
  it('collapses whitespace in the JSON projection', () => {
    assert.equal(compactInput({ a: 1, b: 'x' }), '{"a":1,"b":"x"}');
  });

  it('caps an oversized projection with an ellipsis', () => {
    const big = { command: 'x'.repeat(MAX_INPUT_PREVIEW_CHARS * 2) };
    const s = compactInput(big);
    assert.ok(s.length <= MAX_INPUT_PREVIEW_CHARS, `must be bounded, got ${s.length}`);
    assert.ok(s.endsWith('…'));
  });
});

// ---------------------------------------------------------------------------
// Proactive teammate_terminated push on abnormal exit
// ---------------------------------------------------------------------------
describe('U022-w75 notifyLeadTeammateTerminated', () => {
  it('queues a teammate_terminated notice into the lead mailbox', () => {
    const queued = notifyLeadTeammateTerminated('worker', 'agent killed');
    assert.equal(queued, true);
    const box = getMailbox(TEAM_LEAD_NAME);
    assert.equal(box.length, 1);
    const parsed = parseStructuredMessage(box[0].content) as TeammateTerminated;
    assert.equal(parsed.type, 'teammate_terminated');
    assert.equal(parsed.message, 'agent killed');
    assert.equal(box[0].from, 'worker');
  });

  it('is a no-op for the lead itself and for a blank name', () => {
    assert.equal(notifyLeadTeammateTerminated(TEAM_LEAD_NAME), false);
    assert.equal(notifyLeadTeammateTerminated('   '), false);
    assert.equal(getMailbox(TEAM_LEAD_NAME).length, 0);
  });

  it('is dropped (silent lifecycle signal) by the receive-side relay', () => {
    notifyLeadTeammateTerminated('worker');
    const [msg] = getMailbox(TEAM_LEAD_NAME);
    assert.equal(relayIncomingMessage(msg.content, msg.from).drop, true);
  });
});

describe('U022-w75 markTeammateIdle terminalStatus → termination vs idle push', () => {
  let tmp: string;
  beforeEach(() => { resetTeam(); resetAllMailboxes(); tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-term-push-')); });
  afterEach(() => { resetTeam(); resetAllMailboxes(); try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } });

  function tctx(): ToolContext {
    return { cwd: tmp, sessionId: 'sess-1', abortSignal: new AbortController().signal } as unknown as ToolContext;
  }

  it('pushes teammate_terminated when a teammate exits killed', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, tctx());
    registerTeamMember({ name: 'worker', agentId: 'a-1' });
    markTeammateIdle(tctx(), 'a-1', 'killed');
    const box = getMailbox(TEAM_LEAD_NAME);
    assert.equal(box.length, 1);
    const parsed = parseStructuredMessage(box[0].content) as TeammateTerminated;
    assert.equal(parsed.type, 'teammate_terminated');
    assert.equal(box[0].from, 'worker');
  });

  it('pushes teammate_terminated when a teammate exits failed', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, tctx());
    registerTeamMember({ name: 'worker', agentId: 'a-1' });
    markTeammateIdle(tctx(), 'a-1', 'failed');
    const parsed = parseStructuredMessage(getMailbox(TEAM_LEAD_NAME)[0].content) as TeammateTerminated;
    assert.equal(parsed.type, 'teammate_terminated');
  });

  it('still pushes idle_notification on a clean completion (regression lock)', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, tctx());
    registerTeamMember({ name: 'worker', agentId: 'a-1' });
    markTeammateIdle(tctx(), 'a-1', 'completed');
    const parsed = parseStructuredMessage(getMailbox(TEAM_LEAD_NAME)[0].content)!;
    assert.equal((parsed as { type: string }).type, 'idle_notification');
    // Member is marked inactive regardless of exit kind.
    assert.equal(getCurrentTeam()!.members.find(m => m.agentId === 'a-1')!.isActive, false);
  });

  it('defaults to idle when no terminalStatus is given (back-compat of the 2-arg call)', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, tctx());
    registerTeamMember({ name: 'worker', agentId: 'a-1' });
    markTeammateIdle(tctx(), 'a-1');
    const parsed = parseStructuredMessage(getMailbox(TEAM_LEAD_NAME)[0].content)!;
    assert.equal((parsed as { type: string }).type, 'idle_notification');
  });
});
