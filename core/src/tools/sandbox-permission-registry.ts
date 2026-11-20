/**
 * Sandbox (network-host) permission-delegation registry.
 *
 * PARITY (the reference's `useSwarmPermissionPoller` sandbox registry —
 * `pendingSandboxCallbacks` / `processSandboxPermissionResponse`, and the combined
 * `clearAllPendingCallbacks` on `/clear`): a SECOND, distinct delegation registry
 * (separate from the tool-permission flow) that handles per-host sandbox EGRESS
 * approval. When a sandboxed WORKER hits a network host that is NOT on its static
 * egress allowlist, it escalates a per-host request to the team leader and the
 * leader's allow/deny decision is applied here.
 *
 * ARCHITECTURE NOTE (intentional divergence from the reference's promise-callback shape):
 * the reference resolves the worker's blocked call synchronously via an INDEPENDENT
 * permission poller (`useSwarmPermissionPoller`) that runs regardless of the main
 * agent loop. AMI has no such independent poller — its mailbox is drained between
 * turns by the engine loop itself — so a worker that blocked inside a tool call
 * could never observe its own response (deadlock). AMI therefore models the
 * delegation as a TURN-BASED grant store: the leader's `sandbox_permission_response`
 * is applied by the worker's next mailbox poll (see `relayIncomingMessage`), which
 * records the granted host here; the worker's NEXT sandboxed command then reaches
 * the approved host (the container backend stops blackholing a granted deny-host —
 * see `wrapWithDocker`). This is the same allow/deny delegation, wired the way AMI's
 * turn-based engine can actually honor it (no dead promise that can never resolve).
 *
 * The transport (the `sandbox_permission_request` / `sandbox_permission_response`
 * control messages) lives in `send-message.ts`; this module owns only the
 * session-scoped grant set and its mutators.
 */

import * as fs from 'fs';
import * as path from 'path';
import type { PermissionPromptHandler } from '../permissions';
import { resolveConfigDir } from '../reference-journal';
import { atomicWriteFileSync } from '../utils/fs-atomic';

/**
 * Session-scoped set of network hosts approved for sandbox egress THIS run only
 * (an `allow_once` / `allow_session` verdict, or a leader delegation). Dropped on
 * `/clear` and when the process exits.
 */
const sessionGrantedHosts = new Set<string>();

/**
 * Durably-REMEMBERED egress hosts, lazily hydrated from {@link sandboxHostsPath}
 * on first membership check. These come from the "allow and don't ask again"
 * verdict ({@link persistSandboxHostGrant}) and survive `/clear` and restarts —
 * the parity analog of the reference's `persistToSettings:true` network-egress response.
 * `undefined` until loaded so a config-dir change (test isolation) re-reads.
 */
let persistedGrantedHosts: Set<string> | undefined;

/** Normalize a host for stable membership: trim + lowercase (DNS is case-insensitive). */
function normalizeHost(host: string): string {
  return host.trim().toLowerCase();
}

/**
 * The durable store for "remembered" sandbox-egress hosts, co-located with the rest
 * of AMI's persistent state under the resolved data dir (`~/.ami`, or the
 * `SUPERINFERENCE_DATA_DIR`/`CLAUDE_CONFIG_DIR` override — so tests isolate it).
 */
function sandboxHostsPath(): string {
  return path.join(resolveConfigDir(), 'sandbox-hosts.json');
}

/**
 * Hydrate (once) the remembered-hosts set from disk. A missing/unreadable/corrupt
 * file, or a non-string/empty entry, is treated as "nothing remembered" (fail-soft:
 * a broken store never blocks egress decisions, it just forgets). Each entry is
 * re-normalized so the on-disk form can never smuggle a case/whitespace variant past
 * a later membership check.
 */
function ensurePersistedLoaded(): Set<string> {
  if (persistedGrantedHosts) return persistedGrantedHosts;
  const loaded = new Set<string>();
  try {
    const raw = fs.readFileSync(sandboxHostsPath(), 'utf-8');
    const parsed = JSON.parse(raw) as unknown;
    const hosts = (parsed as { hosts?: unknown })?.hosts;
    if (Array.isArray(hosts)) {
      for (const entry of hosts) {
        if (typeof entry === 'string') {
          const h = normalizeHost(entry);
          if (h.length > 0) loaded.add(h);
        }
      }
    }
  } catch {
    /* no store yet / unreadable / corrupt → nothing remembered */
  }
  persistedGrantedHosts = loaded;
  return loaded;
}

/**
 * Record a leader-approved egress host so the worker's subsequent sandboxed
 * commands can reach it — for THIS session only. Idempotent; ignores empty input.
 */
export function recordSandboxHostGrant(host: string): void {
  const h = normalizeHost(host);
  if (h.length > 0) sessionGrantedHosts.add(h);
}

/**
 * Remember an egress host DURABLY ("allow and don't ask again"): record the session
 * grant AND append it to the on-disk {@link sandboxHostsPath} store so it is never
 * re-prompted in future sessions. This is the parity analog of the reference's
 * `persistToSettings:true` on a per-host network-egress response (allow-and-remember,
 * as opposed to allow-once). Idempotent; ignores empty input. The disk write is
 * crash-safe (atomic rename, owner-only) and fail-soft — a write error still leaves
 * the session grant in place, so the host is reachable this run even if persistence
 * fails.
 */
export function persistSandboxHostGrant(host: string): void {
  const h = normalizeHost(host);
  if (h.length === 0) return;
  sessionGrantedHosts.add(h);
  const persisted = ensurePersistedLoaded();
  if (persisted.has(h)) return; // already remembered — no rewrite
  persisted.add(h);
  try {
    atomicWriteFileSync(
      sandboxHostsPath(),
      JSON.stringify({ hosts: [...persisted].sort() }, null, 2) + '\n',
    );
  } catch {
    /* fail-soft: the session grant above still makes the host reachable this run */
  }
}

/**
 * True if `host` has been granted sandbox egress — either for this session or as a
 * durably-remembered host ({@link persistSandboxHostGrant}).
 */
export function isSandboxHostGranted(host: string): boolean {
  const h = normalizeHost(host);
  return sessionGrantedHosts.has(h) || ensurePersistedLoaded().has(h);
}

/**
 * The granted egress hosts (sorted, de-duplicated) — the union of this session's
 * grants and the durably-remembered hosts. Folded into the effective sandbox
 * outbound allowlist by both the container ({@link wrapWithDocker}) and host
 * (`wrapWithUnshare`) backends, so a remembered host is reachable across restarts.
 */
export function getGrantedSandboxHosts(): string[] {
  return [...new Set([...sessionGrantedHosts, ...ensurePersistedLoaded()])].sort();
}

/**
 * Apply a leader's `sandbox_permission_response` from a mailbox message. On `allow`
 * the host is recorded as granted (the worker's next sandboxed command may reach
 * it); on deny nothing is granted (fail-closed — a denied/absent decision never
 * opens egress). Returns `true` when a grant was recorded, `false` otherwise.
 * Called by the worker's mailbox poller (`relayIncomingMessage`). Mirrors the reference's
 * `processSandboxPermissionResponse`.
 */
export function processSandboxPermissionResponse(params: {
  requestId: string;
  host: string;
  allow: boolean;
}): boolean {
  // The in-flight request for this host is now resolved (either way): drop the
  // worker-side pending marker if it matches, and splice any leader-side queue
  // entry so a stale request never lingers after a decision.
  if (pendingSandboxRequest && pendingSandboxRequest.requestId === params.requestId.trim()) {
    pendingSandboxRequest = null;
  }
  resolveWorkerSandboxRequest(params.requestId);
  if (!params.allow) return false;
  recordSandboxHostGrant(params.host);
  return true;
}

// ---------------------------------------------------------------------------
// In-flight per-host egress approval requests — the leader-side reviewable QUEUE
// and the worker-side pending marker. PARITY: the reference's AppState
// `workerSandboxPermissions.queue` + `pendingSandboxRequest`
// (state/AppStateStore.ts L362-384). The grant store above records the OUTCOME of
// an approval; this models the REQUESTS still awaiting a leader decision — the
// piece the grant store alone lacks. A sandboxed WORKER that hits a
// non-allowlisted host escalates a request (`pendingSandboxRequest` on its side)
// to the leader, whose queue holds every such request with a selected index for
// review/navigation; the leader's allow/deny dequeues it and the worker's grant is
// applied by {@link processSandboxPermissionResponse}.
// ---------------------------------------------------------------------------

/**
 * One in-flight per-host egress approval request, as held in the leader-side
 * queue. Mirrors the reference's `workerSandboxPermissions.queue` entry shape.
 */
export interface WorkerSandboxRequest {
  requestId: string;
  workerId: string;
  workerName: string;
  workerColor?: string;
  /** The network host the worker needs egress to (DNS, case-insensitive). */
  host: string;
  /** Epoch ms the request entered the queue (ordering / staleness display). */
  createdAt: number;
}

/** Leader-side queue of worker egress requests awaiting a decision. */
const workerSandboxRequestQueue: WorkerSandboxRequest[] = [];
/** The currently-selected queue entry index (for leader review UI navigation). */
let selectedSandboxRequestIndex = 0;
/** Worker-side pending request shown while waiting for the leader's decision. */
let pendingSandboxRequest: { requestId: string; host: string } | null = null;

/** The selected index, made safe for the current queue length (0 when empty). */
function safeSelectedSandboxIndex(): number {
  const len = workerSandboxRequestQueue.length;
  return len === 0 ? 0 : Math.min(Math.max(selectedSandboxRequestIndex, 0), len - 1);
}

/**
 * LEADER SIDE: enqueue a worker's per-host egress request. De-duplicated by
 * `requestId` (a re-sent request never double-queues). An empty `requestId` or
 * `host` is rejected (fail-closed — an unidentifiable request can never be
 * approved). `createdAt` defaults to now when omitted. Returns `true` when a new
 * entry was queued, `false` when rejected or a duplicate. Mirrors the reference pushing
 * onto `workerSandboxPermissions.queue`.
 */
export function enqueueWorkerSandboxRequest(
  request: Omit<WorkerSandboxRequest, 'createdAt'> & { createdAt?: number },
): boolean {
  const requestId = request.requestId?.trim() ?? '';
  const host = normalizeHost(request.host ?? '');
  if (requestId.length === 0 || host.length === 0) return false;
  if (workerSandboxRequestQueue.some((e) => e.requestId === requestId)) return false;
  workerSandboxRequestQueue.push({
    requestId,
    workerId: request.workerId,
    workerName: request.workerName,
    workerColor: request.workerColor,
    host,
    createdAt: request.createdAt ?? Date.now(),
  });
  return true;
}

/** LEADER SIDE: a snapshot (copy) of the pending-request queue, in arrival order. */
export function getWorkerSandboxRequestQueue(): WorkerSandboxRequest[] {
  return workerSandboxRequestQueue.map((e) => ({ ...e }));
}

/** The currently-selected queue entry (for leader review), or `null` when empty. */
export function getSelectedWorkerSandboxRequest(): WorkerSandboxRequest | null {
  if (workerSandboxRequestQueue.length === 0) return null;
  return { ...workerSandboxRequestQueue[safeSelectedSandboxIndex()]! };
}

/** The selected index, clamped into range (0 when the queue is empty). */
export function getSelectedSandboxRequestIndex(): number {
  return safeSelectedSandboxIndex();
}

/**
 * Move the leader's review cursor by `delta` (e.g. +1 / -1 for next/prev),
 * wrapping around the queue so navigation never gets stuck at an end. No-op on an
 * empty queue. Returns the newly-selected entry (or `null`).
 */
export function moveSelectedSandboxRequest(delta: number): WorkerSandboxRequest | null {
  const len = workerSandboxRequestQueue.length;
  if (len === 0) {
    selectedSandboxRequestIndex = 0;
    return null;
  }
  // Euclidean modulo so a negative delta wraps correctly; start from the safe index.
  selectedSandboxRequestIndex = ((safeSelectedSandboxIndex() + delta) % len + len) % len;
  return { ...workerSandboxRequestQueue[selectedSandboxRequestIndex]! };
}

/**
 * LEADER SIDE: remove (resolve) the queued request `requestId` once a decision has
 * been made, keeping the selected index valid (shifted left if the removed entry
 * was at/before the cursor). Returns the removed entry, or `undefined` if no such
 * request was queued. Mirrors the reference splicing the entry out of
 * `workerSandboxPermissions.queue` after an allow/deny.
 */
export function resolveWorkerSandboxRequest(requestId: string): WorkerSandboxRequest | undefined {
  const id = requestId?.trim() ?? '';
  const idx = workerSandboxRequestQueue.findIndex((e) => e.requestId === id);
  if (idx === -1) return undefined;
  const [removed] = workerSandboxRequestQueue.splice(idx, 1);
  // Shift the cursor left when the removed entry was at/before it, then re-clamp to
  // the shrunken queue so the selection always points at a live entry.
  if (idx <= selectedSandboxRequestIndex && selectedSandboxRequestIndex > 0) {
    selectedSandboxRequestIndex--;
  }
  selectedSandboxRequestIndex = safeSelectedSandboxIndex();
  return removed;
}

/**
 * WORKER SIDE: record that this worker is blocked waiting on the leader to approve
 * egress to `host` (request `requestId`). Shown as the worker's "pending sandbox
 * request" affordance while it waits. An empty requestId/host clears any pending
 * marker (fail-closed). Mirrors the reference setting `pendingSandboxRequest`.
 */
export function setPendingSandboxRequest(requestId: string, host: string): void {
  const id = requestId?.trim() ?? '';
  const h = normalizeHost(host ?? '');
  pendingSandboxRequest = id.length > 0 && h.length > 0 ? { requestId: id, host: h } : null;
}

/** WORKER SIDE: the pending egress request awaiting leader approval, or `null`. */
export function getPendingSandboxRequest(): { requestId: string; host: string } | null {
  return pendingSandboxRequest ? { ...pendingSandboxRequest } : null;
}

/** WORKER SIDE: clear the pending egress marker (decision arrived / abandoned). */
export function clearPendingSandboxRequest(): void {
  pendingSandboxRequest = null;
}

/**
 * PARITY (the reference's `createSandboxAskCallback` prompt wording — structuredIO.ts
 * L745 `Allow network connection to ${host}?`): the EXACT user-facing message an
 * interactive per-host sandbox egress approval shows. Centralized so the host
 * backend, the container backend, and the web_fetch egress gate all ask with
 * identical wording (no per-site string drift). Trims the host for display.
 */
export function sandboxNetworkAskMessage(host: string): string {
  return `Allow network connection to ${host.trim()}?`;
}

/**
 * PARITY (interactive per-host sandbox egress gate — the reference's `SandboxAskCallback`):
 * the env gate `AMI_SANDBOX_NETWORK_ASK` that turns the LOCAL deny-by-default
 * outbound posture on. When enabled, a network egress to a host that is neither
 * pre-allowlisted nor already granted is surfaced for an interactive allow/deny
 * decision ({@link requestSandboxHostAccess}) instead of being silently allowed or
 * hard-failed. Default OFF → the pre-declared static allowlist behavior is
 * unchanged. Reads an injectable env for testability (mirrors the other sandbox env
 * gates). Truthy = `1`/`true`/`yes`/`on`.
 */
export const SANDBOX_NETWORK_ASK_ENV = 'AMI_SANDBOX_NETWORK_ASK';
export function isSandboxNetworkAskEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const v = (env[SANDBOX_NETWORK_ASK_ENV] ?? '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes' || v === 'on';
}

/**
 * The runtime per-host network-egress request shape, parity with the reference's
 * `NetworkHostPattern` handed to a `SandboxAskCallback`. `port` is optional and
 * carried for parity; the grant is keyed on `host` (DNS, case-insensitive).
 */
export interface NetworkHostPattern {
  host: string;
  port?: number;
}

/**
 * PARITY (the reference's `SandboxAskCallback`): a callback invoked when a sandboxed/
 * network egress is attempted to a host that is not already permitted. Resolves
 * `true` to allow the connection, `false` to deny it (fail-closed on error).
 */
export type SandboxAskCallback = (hostPattern: NetworkHostPattern) => Promise<boolean>;

/**
 * PARITY (interactive per-host sandbox network approval — the reference's
 * `createSandboxAskCallback` + REPL `sandboxAskCallback`): decide whether an
 * outbound connection to `host` may proceed, prompting the user ON DEMAND when it
 * is not already permitted. Semantics:
 *   - an empty host → `false` (fail-closed);
 *   - a host already granted this session ({@link isSandboxHostGranted}) → `true`
 *     with no re-prompt (idempotent, mirrors the reference not re-asking a granted host);
 *   - otherwise, when a {@link PermissionPromptHandler} is available, prompt with
 *     the exact {@link sandboxNetworkAskMessage} wording. An allow* verdict RECORDS
 *     the grant ({@link recordSandboxHostGrant}) — so the SAME host is subsequently
 *     reachable from both the bash sandbox allowlist (host + container backends fold
 *     {@link getGrantedSandboxHosts}) and further web fetches — and returns `true`;
 *     a deny/cancel returns `false`;
 *   - no handler available → `false` (fail-closed, mirroring the reference's `catch → deny`).
 * A handler that throws is caught and treated as a deny (fail-closed). This is the
 * single canonical approval entry point reused by every egress site (no parallel
 * approval path).
 *
 * ALLOW-ONCE vs ALLOW-AND-REMEMBER (parity with the reference's `{ allow, persistToSettings }`
 * network-egress response): the handler's verdict distinguishes a one-shot allow from
 * a "don't ask again" grant. An `allow_pattern` verdict (the prompt's "always allow
 * this host" option) is DURABLY remembered ({@link persistSandboxHostGrant}) so the
 * host is never re-prompted in future sessions; a plain `allow_once` / `allow_session`
 * verdict grants only for this run ({@link recordSandboxHostGrant}). Both reach the
 * host now; only the former survives a restart.
 */
export async function requestSandboxHostAccess(
  host: string,
  handler: PermissionPromptHandler | undefined,
  abortSignal?: AbortSignal,
): Promise<boolean> {
  const h = normalizeHost(host);
  if (h.length === 0) return false;
  if (isSandboxHostGranted(h)) return true;
  if (!handler) return false;
  let verdict;
  try {
    verdict = await handler.showPrompt('sandbox', sandboxNetworkAskMessage(h), undefined, abortSignal);
  } catch {
    return false;
  }
  if (verdict.action === 'allow_pattern') {
    // "Always allow this host" → remember across sessions (persistToSettings:true).
    persistSandboxHostGrant(h);
    return true;
  }
  if (verdict.action.startsWith('allow')) {
    // "Allow once" / session grant (persistToSettings:false).
    recordSandboxHostGrant(h);
    return true;
  }
  return false;
}

/**
 * PARITY (the reference's `createSandboxAskCallback`): build a {@link SandboxAskCallback}
 * bound to a permission handler + abort signal, delegating each decision to the
 * canonical {@link requestSandboxHostAccess}. Lets a consumer hand a plain
 * `(hostPattern) => Promise<boolean>` to a sandbox/egress layer exactly as the reference's
 * callback is shaped, without re-implementing the approval logic.
 */
export function createSandboxAskCallback(
  handler: PermissionPromptHandler | undefined,
  abortSignal?: AbortSignal,
): SandboxAskCallback {
  return (hostPattern) => requestSandboxHostAccess(hostPattern.host, handler, abortSignal);
}

/**
 * Clear this session's sandbox-host grants. Called from the `/clear` session reset
 * alongside the rest of the session-cache eviction so stale cross-agent approval
 * state never leaks into a fresh conversation (parity with the reference's
 * `clearAllPendingCallbacks`, which clears the sandbox registry together with the
 * tool-permission registry on `/clear`).
 *
 * DURABLY-REMEMBERED hosts ({@link persistSandboxHostGrant}, "don't ask again") are
 * deliberately KEPT — they are the user's persistent allowlist, equivalent to the reference's
 * `persistToSettings:true` hosts, which `/clear` must not forget. The lazy-load flag
 * is reset so the next membership check re-reads the on-disk store (which also lets a
 * test that points {@link sandboxHostsPath} at a fresh dir pick up the new store).
 */
export function clearSandboxPermissionGrants(): void {
  sessionGrantedHosts.clear();
  persistedGrantedHosts = undefined;
  // Drop every in-flight approval request too — a half-reviewed leader queue or a
  // worker's pending marker from the prior conversation must not leak into a fresh
  // one (parity: `clearAllPendingCallbacks` clears the sandbox registry on /clear).
  workerSandboxRequestQueue.length = 0;
  selectedSandboxRequestIndex = 0;
  pendingSandboxRequest = null;
}
