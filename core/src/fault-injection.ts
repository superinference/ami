// ===========================================================================
// fault-injection.ts — runtime fault-injection seam for the LIVE recovery path
//
// Parity: Claude Code's bridge/bridgeDebug.ts wrapApiForFaultInjection. Claude
// wraps the real BridgeApiClient so a queued one-shot fault is thrown on the
// next matching call and then delegates through, letting an operator drive the
// LIVE recovery machinery (retry/backoff/teardown) — not a mock — and watch it
// react.
//
// AMI already has the recovery LOGIC (error-classifier's retryable /
// shouldFallback / abort, provider.ts's abortable backoff retry loop,
// mcp/client.ts's bounded reconnect) but could previously only SIMULATE faults
// inside unit tests via mock SSE servers / DI seams. This module adds the
// missing runtime seam: an env-gated, one-shot classified fault that a live
// request path consumes and THROWS so the real retry/backoff/teardown code
// fires in-process.
//
// A `fatal` fault throws a NON-retryable terminal error (classifyError →
// auth_error, retryable:false → the engine surfaces it and stops = teardown).
// A `transient` fault throws a RETRYABLE error (classifyError → server_error,
// retryable:true → the provider retry loop backs off and retries). The two are
// distinguished exactly the way a real wire error is: by the HTTP status the
// thrown error carries (RETRYABLE_STATUS_CODES), so the REAL classifier decides
// — matching Claude's fatal-vs-transient split.
//
// Driven entirely by the AMI_FAULT_INJECTION env var (a test/debug knob,
// modelled on AMI_MOCK_RATE_LIMIT): zero extra overhead when unset — a single
// lookup against an empty, lazily-parsed queue per request. Surfaced via
// `ami --introspect` and a `fault_injected` diagnostic when a fault fires.
// ===========================================================================

import { recordDiagnostic } from './logger';

/** Env knob holding the fault spec. Value format below (see {@link parseFaultInjection}). */
export const FAULT_INJECTION_ENV = 'AMI_FAULT_INJECTION';

/** The two recovery regimes a fault exercises. */
export type FaultKind = 'fatal' | 'transient';

/**
 * Default HTTP status per kind. A `transient` fault defaults to 503 (in
 * RETRYABLE_STATUS_CODES → the provider retry loop retries it); a `fatal` fault
 * defaults to 403 (non-retryable → classifyError auth_error → terminal/teardown).
 */
export const FAULT_DEFAULT_STATUS: Readonly<Record<FaultKind, number>> = {
  transient: 503,
  fatal: 403,
};

/** One-shot fault consumed on the next matching call to {@link maybeThrowFault}. */
export interface InjectedFault {
  /** The live client/path the fault targets (e.g. 'provider'). */
  target: string;
  kind: FaultKind;
  /** HTTP status embedded in the thrown error's message so the real classifier sees it. */
  status: number;
  /** Remaining injections; decremented on consume, removed at 0. */
  count: number;
  /** Optional provider error-type label folded into the message (diagnostic only). */
  errorType?: string;
}

/**
 * Parse the `AMI_FAULT_INJECTION` spec into a fault queue. Format: a
 * comma-separated list of `target:kind[:status[:count]]` entries, e.g.
 *   `provider:transient`                      → one transient 503 on the next provider call
 *   `provider:fatal:403:2`                    → two fatal 403s
 *   `provider:transient:503:3,provider:fatal` → three transient then one fatal
 *
 * Fail-safe: an unset/blank value or any malformed entry (missing target,
 * unknown kind, non-positive count) is skipped so a typo never injects a
 * surprise fault. Returns a fresh array (the mutable live queue).
 */
export function parseFaultInjection(raw: string | undefined): InjectedFault[] {
  if (!raw) return [];
  const faults: InjectedFault[] = [];
  for (const entry of raw.split(',')) {
    const token = entry.trim();
    if (!token) continue;
    const parts = token.split(':').map((p) => p.trim());
    const target = parts[0];
    const kind = parts[1];
    if (!target || (kind !== 'fatal' && kind !== 'transient')) continue;
    let status = FAULT_DEFAULT_STATUS[kind];
    if (parts[2] !== undefined && parts[2] !== '') {
      const parsed = Number(parts[2]);
      if (!Number.isInteger(parsed) || parsed <= 0) continue;
      status = parsed;
    }
    let count = 1;
    if (parts[3] !== undefined && parts[3] !== '') {
      const parsed = Number(parts[3]);
      if (!Number.isInteger(parsed) || parsed <= 0) continue;
      count = parsed;
    }
    faults.push({ target, kind, status, count });
  }
  return faults;
}

// Lazily-parsed live queue. Parsed once from the env on first use (the env is
// fixed at launch in production), then mutated as faults are consumed. Tests and
// the programmatic injector reset / push into it.
let queue: InjectedFault[] | null = null;

function registry(): InjectedFault[] {
  if (queue === null) queue = parseFaultInjection(process.env[FAULT_INJECTION_ENV]);
  return queue;
}

/** Re-read the env and clear the queue. For tests after mutating process.env. */
export function resetFaultInjection(): void {
  queue = null;
}

/** True when at least one fault is queued (env-configured or programmatic). */
export function faultInjectionActive(): boolean {
  return registry().length > 0;
}

/** Queue a fault programmatically (e.g. from a debug slash command). */
export function injectFault(fault: InjectedFault): void {
  registry().push({ ...fault });
}

/**
 * Consume the next queued fault for `target`, decrementing its count and
 * removing it at zero. Returns null when none match (the hot path when
 * fault injection is unconfigured).
 */
export function consumeFault(target: string): InjectedFault | null {
  const q = registry();
  const idx = q.findIndex((f) => f.target === target);
  if (idx === -1) return null;
  const fault = q[idx]!;
  fault.count--;
  if (fault.count <= 0) q.splice(idx, 1);
  return fault;
}

/**
 * Build the error a fault throws. The HTTP status is embedded in the message so
 * the REAL classifier (classifyError / the provider retry loop's
 * extractStatusCode) decides retryable-vs-terminal exactly as it would for a
 * genuine wire error — a transient 503 retries, a fatal 403 tears down.
 */
export function makeInjectedError(fault: InjectedFault): Error {
  const label = fault.errorType ? ` ${fault.errorType}` : '';
  return new Error(`HTTP ${fault.status}: [injected ${fault.kind} fault]${label} ${fault.target}`);
}

/**
 * The one-shot seam a live request path calls before issuing its real request.
 * If a fault is queued for `target`, record a `fault_injected` diagnostic and
 * THROW the classified error so the surrounding recovery machinery reacts.
 * No-op (and near-zero cost) when nothing is queued.
 */
export function maybeThrowFault(target: string): void {
  const fault = consumeFault(target);
  if (!fault) return;
  recordDiagnostic('warn', 'fault_injected', {
    target: fault.target,
    kind: fault.kind,
    status: fault.status,
    ...(fault.errorType !== undefined ? { errorType: fault.errorType } : {}),
  });
  throw makeInjectedError(fault);
}
