/**
 * Sandbox (network-host) permission-delegation registry.
 *
 * PARITY (Claude's `useSwarmPermissionPoller` sandbox registry —
 * `pendingSandboxCallbacks` / `processSandboxPermissionResponse`, and the combined
 * `clearAllPendingCallbacks` on `/clear`): a SECOND, distinct delegation registry
 * (separate from the tool-permission flow) that handles per-host sandbox EGRESS
 * approval. When a sandboxed WORKER hits a network host that is NOT on its static
 * egress allowlist, it escalates a per-host request to the team leader and the
 * leader's allow/deny decision is applied here.
 *
 * ARCHITECTURE NOTE (intentional divergence from Claude's promise-callback shape):
 * Claude resolves the worker's blocked call synchronously via an INDEPENDENT
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

/** Session-scoped set of network hosts the leader has approved for sandbox egress. */
const grantedSandboxHosts = new Set<string>();

/** Normalize a host for stable membership: trim + lowercase (DNS is case-insensitive). */
function normalizeHost(host: string): string {
  return host.trim().toLowerCase();
}

/**
 * Record a leader-approved egress host so the worker's subsequent sandboxed
 * commands can reach it. Idempotent; ignores empty input.
 */
export function recordSandboxHostGrant(host: string): void {
  const h = normalizeHost(host);
  if (h.length > 0) grantedSandboxHosts.add(h);
}

/** True if `host` has been granted sandbox egress for this session. */
export function isSandboxHostGranted(host: string): boolean {
  return grantedSandboxHosts.has(normalizeHost(host));
}

/** The granted egress hosts (sorted copy) — for the `/sandbox` surface and tests. */
export function getGrantedSandboxHosts(): string[] {
  return [...grantedSandboxHosts].sort();
}

/**
 * Apply a leader's `sandbox_permission_response` from a mailbox message. On `allow`
 * the host is recorded as granted (the worker's next sandboxed command may reach
 * it); on deny nothing is granted (fail-closed — a denied/absent decision never
 * opens egress). Returns `true` when a grant was recorded, `false` otherwise.
 * Called by the worker's mailbox poller (`relayIncomingMessage`). Mirrors Claude's
 * `processSandboxPermissionResponse`.
 */
export function processSandboxPermissionResponse(params: {
  requestId: string;
  host: string;
  allow: boolean;
}): boolean {
  if (!params.allow) return false;
  recordSandboxHostGrant(params.host);
  return true;
}

/**
 * Clear ALL sandbox-host grants. Called from the `/clear` session reset alongside
 * the rest of the session-cache eviction so stale cross-agent approval state never
 * leaks into a fresh conversation (parity with Claude's `clearAllPendingCallbacks`,
 * which clears the sandbox registry together with the tool-permission registry on
 * `/clear`).
 */
export function clearSandboxPermissionGrants(): void {
  grantedSandboxHosts.clear();
}
