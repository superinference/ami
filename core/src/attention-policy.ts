/**
 * Pure attention + liveness policy for the fleet model (core-safe: never imports common/ or
 * cli/). Three independent decisions kept deliberately separate, as the honest-attention model
 * requires:
 *   - LIVENESS ({@link resolveAgentLiveness}): is an agent `live` / `unverifiable` / `settled`,
 *     derived from evidence age + whether a live session exists — never a lying spinner, never a
 *     false 'done'. Parity: Orca `agent-status/presentation/agent-row-decay-state.ts`.
 *   - ADMISSION ({@link resolveAgentAttention}): is a turn-settle real / live / not superseded,
 *     and should it mark unread. Parity: Orca `attention/agent-attention-policy.ts`.
 *   - the COORDINATOR-ONLY-NEWS roll-up ({@link isCoordinatorOnlyNews}): a dispatched worker
 *     reports to its parent, so only the top-level agent the human converses with raises attention.
 *
 * Every dropped finish carries a typed {@link AgentFinishDropCause} so "why didn't agent X ping me"
 * is always answerable (a stateful breadcrumb log + replay guard live in common/status-reconcile.ts).
 */

import type { AgentStatus } from './agent-status';

/**
 * The three honest liveness values. The middle value — `unverifiable` — is the point of the
 * whole model: a live session with stale reporting proves only MISSING evidence, not idle,
 * work, or completion, so it must never be shown as a confident spinner or a false 'done'.
 */
export type AgentLiveness = 'live' | 'unverifiable' | 'settled';

/** Iteration order (loudest/most-certain-live first). */
export const AGENT_LIVENESS_STATES: readonly AgentLiveness[] = ['live', 'unverifiable', 'settled'];

/** Default silence window after which a claimed live state is no longer trusted as fresh. */
export const DEFAULT_AGENT_STALE_AFTER_MS = 60_000;

/** States that assert the agent is actively doing (or waiting on) something RIGHT NOW. */
const LIVE_CLAIM_STATES: ReadonlySet<AgentStatus> = new Set<AgentStatus>([
  'working',
  'monitoring',
  'waiting',
  'blocked',
  'permission',
]);

/** Terminal outcomes — already settled; evidence age is irrelevant. */
const SETTLED_OUTCOME_STATES: ReadonlySet<AgentStatus> = new Set<AgentStatus>([
  'done',
  'failed',
  'interrupted',
]);

/** Evidence about one agent used to settle its liveness. */
export interface AgentLivenessEvidence {
  /** The last reported (claimed) status. */
  state: AgentStatus;
  /** Epoch ms of the most recent PROOF of this state (not merely a bookkeeping updatedAt). */
  evidenceObservedAt: number;
  /** Whether a live session/PTY for this agent is currently present. */
  hasLiveSession: boolean;
  /** A seed restored from a prior process: it predates this run and has no silence to report. */
  restoredUnconfirmed?: boolean;
}

/** The settled verdict for one agent: its honest liveness plus the status to actually display. */
export interface AgentLivenessVerdict {
  liveness: AgentLiveness;
  /** The status a surface shows — a decayed live-claim becomes `unverifiable` or `idle`. */
  status: AgentStatus;
}

/** Whether a claimed-live status's evidence is still within the trust window. Pure. */
export function isLivenessFresh(
  evidenceObservedAt: number,
  now: number,
  staleAfterMs: number = DEFAULT_AGENT_STALE_AFTER_MS,
): boolean {
  return now - evidenceObservedAt < staleAfterMs;
}

/**
 * Decay a STALE live-claim: a live session proves only missing evidence (-> `unverifiable`),
 * an absent session with nothing live left is simply `idle`. A restored seed predates this
 * process, so it has no silence duration to report and settles to `idle`. Parity: Orca
 * `resolveDecayedAgentRowState`. Pure.
 */
export function resolveDecayedLiveness(
  evidence: Pick<AgentLivenessEvidence, 'state' | 'restoredUnconfirmed'>,
  hasLiveSession: boolean,
): AgentLivenessVerdict {
  const unverifiable =
    hasLiveSession && evidence.state !== 'done' && evidence.restoredUnconfirmed !== true;
  return unverifiable
    ? { liveness: 'unverifiable', status: 'unverifiable' }
    : { liveness: 'settled', status: 'idle' };
}

/**
 * The honest liveness verdict for one agent from its evidence + the clock:
 *   - a terminal outcome (done/failed/interrupted) is `settled` as-is;
 *   - a fresh live-claim (working/monitoring/waiting/blocked/permission) is `live` as-is;
 *   - a STALE live-claim decays via {@link resolveDecayedLiveness} (never a lying spinner);
 *   - an already-ambiguous status (unconfirmed/unverifiable) stays `unverifiable`;
 *   - anything else (idle) is `settled`.
 * Parity: Orca `resolveAgentRowState`. Pure — `now` is caller-supplied (no local clock).
 */
export function resolveAgentLiveness(
  evidence: AgentLivenessEvidence,
  now: number,
  staleAfterMs: number = DEFAULT_AGENT_STALE_AFTER_MS,
): AgentLivenessVerdict {
  const { state } = evidence;
  if (SETTLED_OUTCOME_STATES.has(state)) {
    return { liveness: 'settled', status: state };
  }
  if (LIVE_CLAIM_STATES.has(state)) {
    return isLivenessFresh(evidence.evidenceObservedAt, now, staleAfterMs)
      ? { liveness: 'live', status: state }
      : resolveDecayedLiveness(evidence, evidence.hasLiveSession);
  }
  if (state === 'unconfirmed' || state === 'unverifiable') {
    return { liveness: 'unverifiable', status: state };
  }
  return { liveness: 'settled', status: 'idle' };
}

/**
 * Every path that can drop a finish WITHOUT marking it unread, each typed so a missing ping is
 * always traceable (parity: Orca `AgentFinishDropCause` — "every path that drops a finish was
 * silent, so a missing unread could not be traced"). Fed to the breadcrumb log and
 * `--introspect` / `/attention-log`.
 */
export type AgentFinishDropCause =
  | 'fresh-active-hook-status'
  | 'superseded-snapshot'
  | 'muted-workspace'
  | 'coordinator-only-news'
  | 'no-live-session'
  | 'unknown-surface'
  | 'superseded-surface'
  | 'viewed'
  | 'pane-pruned-before-quiet-window'
  | 'held-revision-expired'
  | 'pane-not-live'
  | 'hook-lane-disabled'
  | 'turn-end-not-counted'
  | 'restored-unconfirmed'
  | 'host-replay-skipped'
  // AMI-specific causes (the generation guard + replay guard surfaces).
  | 'replayed-turn'
  | 'superseded-generation'
  | 'muted-delivery';

/** The full catalog of drop causes (for `--introspect` discovery). */
export const AGENT_FINISH_DROP_CAUSES: readonly AgentFinishDropCause[] = [
  'fresh-active-hook-status',
  'superseded-snapshot',
  'muted-workspace',
  'coordinator-only-news',
  'no-live-session',
  'unknown-surface',
  'superseded-surface',
  'viewed',
  'pane-pruned-before-quiet-window',
  'held-revision-expired',
  'pane-not-live',
  'hook-lane-disabled',
  'turn-end-not-counted',
  'restored-unconfirmed',
  'host-replay-skipped',
  'replayed-turn',
  'superseded-generation',
  'muted-delivery',
];

/** An unread marker to write — carried in the decision so a suppressed banner still leaves it. */
export interface AgentAttentionUnread {
  workspaceId: string;
  subjectKey: string | null;
  reason: string;
}

/** A delivery request (bell / flash / OSC) — the cli owns how it is delivered. */
export interface AgentAttentionDelivery {
  workspaceId: string;
  subjectKey: string | null;
}

/** The admission facts for a settling (or signalling) agent surface. */
export interface AgentAttentionRequest {
  workspaceId: string;
  subjectKey: string | null;
  /** A settled turn decides unread; a bare bell signal only needs liveness proof. */
  settlesTurn: boolean;
  /** Out-of-band proof the subject just produced work (used when no live session is visible). */
  hasFreshActivityEvidence: boolean;
  /** Whether a live session/PTY for the subject is currently present. */
  hasLiveSession: boolean;
  /** A muted workspace gets no unread, banner, sound or push. */
  isMuted: boolean;
  /** The surface is already on-screen: it may deliver but earns no unread marker. */
  isViewed: boolean;
  /** This is a dispatched worker: its coordinator gets the result, so the person gets nothing. */
  coordinatorOnlyNews?: boolean;
  /** Why the attention was raised (carried onto the unread marker). */
  reason: string;
}

/** The outcome of {@link resolveAgentAttention}: either a traced drop or an admitted delivery. */
export type AgentAttentionDecision =
  | { admitted: false; cause: AgentFinishDropCause }
  | { admitted: true; unread: AgentAttentionUnread | null; delivery: AgentAttentionDelivery };

/**
 * Decide whether an agent event earns attention, separating ADMISSION from UNREAD-MARKING from
 * DELIVERY. A muted workspace, a coordinator-only worker, and an agent with no live session and
 * no fresh evidence are all dropped with a typed cause. An admitted, turn-settling, UNVIEWED
 * surface earns an unread marker; a viewed one may still deliver but writes none. Parity: Orca
 * `resolveAgentAttention`. Pure.
 */
export function resolveAgentAttention(req: AgentAttentionRequest): AgentAttentionDecision {
  if (req.isMuted) {
    return { admitted: false, cause: 'muted-workspace' };
  }
  if (req.coordinatorOnlyNews) {
    return { admitted: false, cause: 'coordinator-only-news' };
  }
  if (!req.hasLiveSession && !req.hasFreshActivityEvidence) {
    return { admitted: false, cause: 'no-live-session' };
  }
  const delivery: AgentAttentionDelivery = {
    workspaceId: req.workspaceId,
    subjectKey: req.subjectKey,
  };
  // A bare bell signal (not a turn-settle) delivers but never marks unread.
  if (!req.settlesTurn) {
    return { admitted: true, unread: null, delivery };
  }
  // Unread is written before delivery so a suppressed banner still leaves the marker behind;
  // a viewed surface earns none.
  const unread: AgentAttentionUnread | null = req.isViewed
    ? null
    : { workspaceId: req.workspaceId, subjectKey: req.subjectKey, reason: req.reason };
  return { admitted: true, unread, delivery };
}

/**
 * The coordinator-only-news rule: a dispatched worker (dispatch depth > 0) finishing is NOT
 * the operator's news — its coordinator receives the result, and only the top-level agent the
 * human converses with raises attention. This is what stops N subagents from flooding the
 * operator. Parity: Orca's `isCoordinatorOnlyChatNews` / `coordinatorOnlyNews`. Pure.
 */
export function isCoordinatorOnlyNews(dispatchDepth: number): boolean {
  return dispatchDepth > 0;
}

/**
 * The replay-guard predicate: a resumed-then-redone turn must not double-notify. A turn is
 * admitted only when its boundary marker (a stable per-turn id) has not already been seen.
 * Pure — the caller owns the `seen` set (the stateful guard wrapping this lives in common).
 */
export function shouldAdmitTurnBoundary(marker: string, seen: ReadonlySet<string>): boolean {
  return marker.length > 0 && !seen.has(marker);
}
