import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  enqueueWorkerSandboxRequest,
  getWorkerSandboxRequestQueue,
  getSelectedWorkerSandboxRequest,
  getSelectedSandboxRequestIndex,
  moveSelectedSandboxRequest,
  resolveWorkerSandboxRequest,
  setPendingSandboxRequest,
  getPendingSandboxRequest,
  clearPendingSandboxRequest,
  processSandboxPermissionResponse,
  clearSandboxPermissionGrants,
  isSandboxHostGranted,
} from '../src/tools/sandbox-permission-registry';

// ---------------------------------------------------------------------------
// U117 — per-host network-egress APPROVAL FLOW for sandboxed child processes
// (parity: references/claude/claude-code-main/src/state/AppStateStore.ts
//  L362-384 — leader-side `workerSandboxPermissions.queue` of in-flight requests +
//  worker-side `pendingSandboxRequest`). The grant STORE records outcomes; this is
//  the IN-FLIGHT request queue awaiting a leader decision, which was missing.
// ---------------------------------------------------------------------------

beforeEach(() => {
  // Each test starts from a clean registry (also clears the new queue state).
  clearSandboxPermissionGrants();
});

const req = (id: string, host: string, over: Partial<{ workerId: string; workerName: string; workerColor: string; createdAt: number }> = {}) => ({
  requestId: id,
  workerId: over.workerId ?? 'w1',
  workerName: over.workerName ?? 'alpha',
  workerColor: over.workerColor,
  host,
  createdAt: over.createdAt,
});

describe('leader-side worker sandbox request queue', () => {
  it('enqueues, snapshots, and normalizes the host; dedupes by requestId', () => {
    assert.equal(enqueueWorkerSandboxRequest(req('r1', 'API.Example.COM ', { createdAt: 10 })), true);
    assert.equal(enqueueWorkerSandboxRequest(req('r2', 'b.example.com', { createdAt: 20 })), true);
    // Duplicate requestId is ignored (re-send never double-queues).
    assert.equal(enqueueWorkerSandboxRequest(req('r1', 'other.com')), false);
    const q = getWorkerSandboxRequestQueue();
    assert.equal(q.length, 2);
    assert.equal(q[0]!.host, 'api.example.com'); // trimmed + lowercased
    assert.equal(q[0]!.createdAt, 10);
    // Snapshot is a copy — mutating it must not corrupt the registry.
    q[0]!.host = 'tampered';
    assert.equal(getWorkerSandboxRequestQueue()[0]!.host, 'api.example.com');
  });

  it('defaults createdAt to now when omitted', () => {
    const before = Date.now();
    enqueueWorkerSandboxRequest(req('r1', 'h.com'));
    const entry = getWorkerSandboxRequestQueue()[0]!;
    assert.ok(entry.createdAt >= before);
  });

  it('rejects an empty requestId or host (fail-closed)', () => {
    assert.equal(enqueueWorkerSandboxRequest(req('  ', 'h.com')), false);
    assert.equal(enqueueWorkerSandboxRequest(req('r1', '   ')), false);
    assert.equal(getWorkerSandboxRequestQueue().length, 0);
  });

  it('tracks a selected index and navigates with wraparound', () => {
    assert.equal(getSelectedWorkerSandboxRequest(), null); // empty queue
    assert.equal(getSelectedSandboxRequestIndex(), 0);
    assert.equal(moveSelectedSandboxRequest(1), null); // no-op on empty

    enqueueWorkerSandboxRequest(req('r1', 'a.com'));
    enqueueWorkerSandboxRequest(req('r2', 'b.com'));
    enqueueWorkerSandboxRequest(req('r3', 'c.com'));

    assert.equal(getSelectedWorkerSandboxRequest()!.requestId, 'r1');
    assert.equal(moveSelectedSandboxRequest(1)!.requestId, 'r2');
    assert.equal(moveSelectedSandboxRequest(1)!.requestId, 'r3');
    assert.equal(moveSelectedSandboxRequest(1)!.requestId, 'r1'); // wrap forward
    assert.equal(moveSelectedSandboxRequest(-1)!.requestId, 'r3'); // wrap backward
    assert.equal(getSelectedSandboxRequestIndex(), 2);
  });

  it('resolves (dequeues) a request and keeps the selected index valid', () => {
    enqueueWorkerSandboxRequest(req('r1', 'a.com'));
    enqueueWorkerSandboxRequest(req('r2', 'b.com'));
    enqueueWorkerSandboxRequest(req('r3', 'c.com'));
    moveSelectedSandboxRequest(2); // select r3 (index 2)
    // Removing an entry before the cursor shifts the cursor left.
    const removed = resolveWorkerSandboxRequest('r1');
    assert.equal(removed!.requestId, 'r1');
    assert.equal(getSelectedSandboxRequestIndex(), 1);
    assert.equal(getSelectedWorkerSandboxRequest()!.requestId, 'r3');
    // Unknown id → undefined, no change.
    assert.equal(resolveWorkerSandboxRequest('nope'), undefined);
    assert.equal(getWorkerSandboxRequestQueue().length, 2);
  });
});

describe('worker-side pending sandbox request', () => {
  it('sets, reads (as a copy), and clears the pending marker', () => {
    assert.equal(getPendingSandboxRequest(), null);
    setPendingSandboxRequest('r1', 'Host.COM');
    const p = getPendingSandboxRequest()!;
    assert.deepEqual(p, { requestId: 'r1', host: 'host.com' });
    p.host = 'x'; // mutating the copy must not affect state
    assert.equal(getPendingSandboxRequest()!.host, 'host.com');
    clearPendingSandboxRequest();
    assert.equal(getPendingSandboxRequest(), null);
  });

  it('an empty requestId/host clears the pending marker (fail-closed)', () => {
    setPendingSandboxRequest('r1', 'h.com');
    setPendingSandboxRequest('', 'h.com');
    assert.equal(getPendingSandboxRequest(), null);
    setPendingSandboxRequest('r2', 'h.com');
    setPendingSandboxRequest('r2', '   ');
    assert.equal(getPendingSandboxRequest(), null);
  });
});

describe('processSandboxPermissionResponse resolves the in-flight request', () => {
  it('allow: grants the host, clears the matching worker marker, dequeues the leader entry', () => {
    enqueueWorkerSandboxRequest(req('r1', 'grant.com'));
    setPendingSandboxRequest('r1', 'grant.com');
    const ok = processSandboxPermissionResponse({ requestId: 'r1', host: 'grant.com', allow: true });
    assert.equal(ok, true);
    assert.equal(isSandboxHostGranted('grant.com'), true);
    assert.equal(getPendingSandboxRequest(), null);
    assert.equal(getWorkerSandboxRequestQueue().length, 0);
  });

  it('deny: grants nothing but still clears the in-flight request', () => {
    enqueueWorkerSandboxRequest(req('r2', 'deny.com'));
    setPendingSandboxRequest('r2', 'deny.com');
    const ok = processSandboxPermissionResponse({ requestId: 'r2', host: 'deny.com', allow: false });
    assert.equal(ok, false);
    assert.equal(isSandboxHostGranted('deny.com'), false);
    assert.equal(getPendingSandboxRequest(), null);
    assert.equal(getWorkerSandboxRequestQueue().length, 0);
  });

  it('a non-matching worker marker is left intact', () => {
    setPendingSandboxRequest('other', 'keep.com');
    processSandboxPermissionResponse({ requestId: 'r9', host: 'h.com', allow: true });
    assert.deepEqual(getPendingSandboxRequest(), { requestId: 'other', host: 'keep.com' });
  });
});

describe('/clear drops all in-flight egress-approval state', () => {
  it('clears the queue, selection, and worker pending marker', () => {
    enqueueWorkerSandboxRequest(req('r1', 'a.com'));
    enqueueWorkerSandboxRequest(req('r2', 'b.com'));
    moveSelectedSandboxRequest(1);
    setPendingSandboxRequest('r1', 'a.com');
    clearSandboxPermissionGrants();
    assert.equal(getWorkerSandboxRequestQueue().length, 0);
    assert.equal(getSelectedSandboxRequestIndex(), 0);
    assert.equal(getPendingSandboxRequest(), null);
  });
});
