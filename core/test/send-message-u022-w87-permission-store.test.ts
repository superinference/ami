/**
 * U022 (w87) — concurrency-safe, crash-safe SHARED PERMISSION-REQUEST store.
 *
 * Gap (permissionSync.ts:L215-250): Claude's worker→leader permission delegation
 * guards every mutation of the shared pending/ queue with a directory `.lock` +
 * atomic write + guaranteed release, so two agent PROCESSES writing a request and
 * resolving it cannot interleave or corrupt the files.
 *
 * AMI does NOT add a second, parallel permission store (that would be semantic
 * duplication, AGENTS.md No-Duplication Rule). The permission handshake is a pair of
 * ordinary control messages — a `permission_request` routed up to the lead and a
 * `permission_response` routed back to the worker — and when the cross-process
 * transport is enabled BOTH legs ride the ONE canonical lock-guarded, atomic,
 * crash-safe inbox (`core/src/tools/mailbox-fs.ts`, built on the shared
 * `withFileLockSync` primitive: retry/backoff + PID stale-lock steal + re-read under
 * lock + temp-file + atomic rename + guaranteed `finally` release).
 *
 * These tests assert the EXACT guarantees the gap names, for the permission payloads
 * specifically (the generic mailbox-fs suite covers only plain chat messages): the
 * request and the resolving response written by two separate writers to the same
 * team queue neither interleave nor clobber, a crash-torn trailing line is tolerated,
 * and no `.lock`/`.tmp` artifact leaks after the queue drains.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  appendToInbox,
  takeFsInbox,
  inboxFilePath,
  inboxDir,
  type FsMailboxLocation,
} from '../src/tools/mailbox-fs';
import { type AgentMessage } from '../src/tools/send-message';

let tmp: string;
let loc: FsMailboxLocation;

const TEAM_LEAD = 'main';

/** A worker's permission_request control message, as send_message serializes it. */
function permissionRequest(requestId: string, tool: string): AgentMessage {
  return {
    id: `pr-${requestId}`,
    from: 'worker',
    to: TEAM_LEAD,
    content: JSON.stringify({ type: 'permission_request', requestId, tool, toolUseId: `tu-${requestId}`, input: { cmd: 'rm -rf build' } }),
    timestamp: Date.now(),
  };
}

/** The lead's permission_response (the "resolve" leg), routed back to the worker. */
function permissionResponse(requestId: string, approve: boolean): AgentMessage {
  return {
    id: `resp-${requestId}`,
    from: TEAM_LEAD,
    to: 'worker',
    content: JSON.stringify({ type: 'permission_response', requestId, approve }),
    timestamp: Date.now(),
  };
}

describe('U022 w87 — crash-safe shared permission-request store (via the canonical lock-guarded inbox)', () => {
  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-permstore-'));
    loc = { teamsRoot: path.join(tmp, '.ami', 'teams'), teamName: 'alpha' };
  });
  afterEach(() => {
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* best-effort */ }
  });

  it('a permission_request persists to and drains from the lead inbox intact', () => {
    appendToInbox(loc, TEAM_LEAD, permissionRequest('permission-1@worker', 'bash'));
    const pending = takeFsInbox(loc, TEAM_LEAD, new Set());
    assert.equal(pending.length, 1);
    const parsed = JSON.parse(pending[0].content);
    assert.equal(parsed.type, 'permission_request');
    assert.equal(parsed.requestId, 'permission-1@worker');
    assert.equal(parsed.tool, 'bash');
    assert.deepEqual(parsed.input, { cmd: 'rm -rf build' });
  });

  it('two writers to the SAME lead queue (two workers escalating) do not clobber each other', () => {
    // A write-only (non re-reading) store would lose the first request.
    appendToInbox(loc, TEAM_LEAD, permissionRequest('permission-1@worker-a', 'bash'));
    appendToInbox(loc, TEAM_LEAD, permissionRequest('permission-2@worker-b', 'web_fetch'));
    const pending = takeFsInbox(loc, TEAM_LEAD, new Set());
    const ids = pending.map((m) => JSON.parse(m.content).requestId).sort();
    assert.deepEqual(ids, ['permission-1@worker-a', 'permission-2@worker-b']);
  });

  it('the resolve leg (permission_response) rides the same lock-guarded transport back to the worker', () => {
    appendToInbox(loc, 'worker', permissionResponse('permission-1@worker', true));
    const resolved = takeFsInbox(loc, 'worker', new Set());
    assert.equal(resolved.length, 1);
    const parsed = JSON.parse(resolved[0].content);
    assert.equal(parsed.type, 'permission_response');
    assert.equal(parsed.approve, true);
    assert.equal(parsed.requestId, 'permission-1@worker');
  });

  it('a retried request is idempotent by id — the lead never sees a duplicate pending entry', () => {
    const req = permissionRequest('permission-1@worker', 'bash');
    appendToInbox(loc, TEAM_LEAD, req);
    appendToInbox(loc, TEAM_LEAD, req); // crash-retry of the same escalation
    const pending = takeFsInbox(loc, TEAM_LEAD, new Set());
    assert.equal(pending.length, 1);
  });

  it('tolerates a crash-torn trailing line, keeping the intact permission records', () => {
    appendToInbox(loc, TEAM_LEAD, permissionRequest('permission-1@worker', 'bash'));
    // Simulate a process crash mid-append: a half-written JSON line appended by hand.
    const file = inboxFilePath(loc, TEAM_LEAD);
    fs.appendFileSync(file, '{"type":"permission_request","requestId":"torn');
    const pending = takeFsInbox(loc, TEAM_LEAD, new Set());
    assert.equal(pending.length, 1, 'the one intact request survives; the torn line is skipped');
    assert.equal(JSON.parse(pending[0].content).requestId, 'permission-1@worker');
  });

  it('leaves no .lock/.tmp artifacts after the queue is written and drained (atomic + guaranteed release)', () => {
    appendToInbox(loc, TEAM_LEAD, permissionRequest('permission-1@worker', 'bash'));
    takeFsInbox(loc, TEAM_LEAD, new Set());
    const residue = fs.readdirSync(inboxDir(loc)).filter((e) => e.includes('.tmp-') || e.endsWith('.jsonl.lock'));
    assert.deepEqual(residue, [], `no lock/tmp residue, found: ${residue.join(', ')}`);
  });
});
