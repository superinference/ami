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

import type { PermissionPromptHandler } from '../permissions';

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
 * PARITY (Claude's `createSandboxAskCallback` prompt wording — structuredIO.ts
 * L745 `Allow network connection to ${host}?`): the EXACT user-facing message an
 * interactive per-host sandbox egress approval shows. Centralized so the host
 * backend, the container backend, and the web_fetch egress gate all ask with
 * identical wording (no per-site string drift). Trims the host for display.
 */
export function sandboxNetworkAskMessage(host: string): string {
  return `Allow network connection to ${host.trim()}?`;
}

/**
 * PARITY (interactive per-host sandbox egress gate — Claude's `SandboxAskCallback`):
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
 * The runtime per-host network-egress request shape, parity with Claude's
 * `NetworkHostPattern` handed to a `SandboxAskCallback`. `port` is optional and
 * carried for parity; the grant is keyed on `host` (DNS, case-insensitive).
 */
export interface NetworkHostPattern {
  host: string;
  port?: number;
}

/**
 * PARITY (Claude's `SandboxAskCallback`): a callback invoked when a sandboxed/
 * network egress is attempted to a host that is not already permitted. Resolves
 * `true` to allow the connection, `false` to deny it (fail-closed on error).
 */
export type SandboxAskCallback = (hostPattern: NetworkHostPattern) => Promise<boolean>;

/**
 * PARITY (interactive per-host sandbox network approval — Claude's
 * `createSandboxAskCallback` + REPL `sandboxAskCallback`): decide whether an
 * outbound connection to `host` may proceed, prompting the user ON DEMAND when it
 * is not already permitted. Semantics:
 *   - an empty host → `false` (fail-closed);
 *   - a host already granted this session ({@link isSandboxHostGranted}) → `true`
 *     with no re-prompt (idempotent, mirrors Claude not re-asking a granted host);
 *   - otherwise, when a {@link PermissionPromptHandler} is available, prompt with
 *     the exact {@link sandboxNetworkAskMessage} wording. An allow* verdict RECORDS
 *     the grant ({@link recordSandboxHostGrant}) — so the SAME host is subsequently
 *     reachable from both the bash sandbox allowlist (host + container backends fold
 *     {@link getGrantedSandboxHosts}) and further web fetches — and returns `true`;
 *     a deny/cancel returns `false`;
 *   - no handler available → `false` (fail-closed, mirroring Claude's `catch → deny`).
 * A handler that throws is caught and treated as a deny (fail-closed). This is the
 * single canonical approval entry point reused by every egress site (no parallel
 * approval path).
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
  if (verdict.action.startsWith('allow')) {
    recordSandboxHostGrant(h);
    return true;
  }
  return false;
}

/**
 * PARITY (Claude's `createSandboxAskCallback`): build a {@link SandboxAskCallback}
 * bound to a permission handler + abort signal, delegating each decision to the
 * canonical {@link requestSandboxHostAccess}. Lets a consumer hand a plain
 * `(hostPattern) => Promise<boolean>` to a sandbox/egress layer exactly as Claude's
 * callback is shaped, without re-implementing the approval logic.
 */
export function createSandboxAskCallback(
  handler: PermissionPromptHandler | undefined,
  abortSignal?: AbortSignal,
): SandboxAskCallback {
  return (hostPattern) => requestSandboxHostAccess(hostPattern.host, handler, abortSignal);
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
