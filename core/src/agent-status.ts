/**
 * Canonical attention-state taxonomy for the fleet model — the SINGLE source of truth
 * every fleet surface reads so chrome and detail never disagree. One {@link AgentStatus}
 * enum with an exhaustive TOTAL-ORDER rank ({@link AGENT_STATUS_RANK}) collapsing into the
 * Needs-You / Working / Idle / Failed buckets ({@link agentStatusBucket}), plus one
 * state -> glyph + tone map ({@link AGENT_STATUS_GLYPH}).
 *
 * Parity: Orca's `agent-status/presentation/agent-list-status.ts` (AGENT_LIST_STATUS_RANK)
 * and `orchestration-crew/CrewAgentIcons.tsx` (crewStatusGroup). Replaces AMI's cli-local
 * `SubagentStatus` (cli/src/subagent-roster.ts) which has no attention/permission states and
 * no total order; {@link subagentStatusToAgentStatus} lifts that lifecycle vocabulary into
 * this canonical taxonomy without the cli layer having to.
 *
 * Pure, dependency-free, core-safe (never imports common/ or cli/).
 */

/**
 * Every distinct attention state an agent row can be in. A new member MUST also be
 * given a unique rank in {@link AGENT_STATUS_RANK} (the `Record<AgentStatus, number>`
 * makes that a COMPILE error — the object is non-exhaustive without it) and a glyph in
 * {@link AGENT_STATUS_GLYPH}; {@link assertUniqueAgentStatusRanks} additionally fails at
 * module load if two states share a rank, so recency can never reorder headers.
 */
export type AgentStatus =
  | 'waiting'
  | 'blocked'
  | 'permission'
  | 'failed'
  | 'unconfirmed'
  | 'working'
  | 'monitoring'
  | 'interrupted'
  | 'unverifiable'
  | 'done'
  | 'idle';

/**
 * Exhaustive, unique total-order rank (lower = louder / sorts first). A row with a lower
 * rank demands attention sooner; equal ranks are forbidden ({@link assertUniqueAgentStatusRanks})
 * so the header order is a genuine total order and never depends on recency. Parity:
 * Orca's AGENT_LIST_STATUS_RANK (same numbering).
 */
export const AGENT_STATUS_RANK: Record<AgentStatus, number> = {
  waiting: 0,
  blocked: 1,
  permission: 2,
  failed: 3,
  unconfirmed: 4,
  working: 5,
  monitoring: 6,
  // A user's Stop is not news, so it follows live work, but it is not a finish either.
  interrupted: 7,
  unverifiable: 8,
  done: 9,
  idle: 10,
};

/** Insertion/rank order of every status — the canonical iteration order for a roster header. */
export const AGENT_STATUS_ORDER: readonly AgentStatus[] = (
  Object.keys(AGENT_STATUS_RANK) as AgentStatus[]
).sort((a, b) => AGENT_STATUS_RANK[a] - AGENT_STATUS_RANK[b]);

/**
 * Fail at module load if two statuses were ever given the same rank (a copy-paste bug a
 * `Record` cannot catch). Keeping this a runtime assertion — rather than only a type — means
 * the test suite and a production boot both reject an ambiguous total order immediately.
 * Returns the ranks when valid (so a caller/test can assert on them). Pure.
 */
export function assertUniqueAgentStatusRanks(): Record<AgentStatus, number> {
  const seen = new Map<number, AgentStatus>();
  for (const status of Object.keys(AGENT_STATUS_RANK) as AgentStatus[]) {
    const rank = AGENT_STATUS_RANK[status];
    const prior = seen.get(rank);
    if (prior !== undefined) {
      throw new Error(
        `AGENT_STATUS_RANK: duplicate rank ${rank} shared by '${prior}' and '${status}'`,
      );
    }
    seen.set(rank, status);
  }
  return AGENT_STATUS_RANK;
}

// Enforce the total order the moment this module is imported.
assertUniqueAgentStatusRanks();

/**
 * Total-order comparator over {@link AgentStatus} by {@link AGENT_STATUS_RANK}: negative
 * when `a` is louder (sorts first), positive when quieter, 0 only for the same status.
 * Pure — the single shared sort key every fleet surface uses so two surfaces never disagree
 * on which agent's row comes first.
 */
export function compareAgentStatus(a: AgentStatus, b: AgentStatus): number {
  return AGENT_STATUS_RANK[a] - AGENT_STATUS_RANK[b];
}

/** The four coarse buckets a roster groups its rows into. Parity: Orca's CrewStatusGroup. */
export type AgentAttentionBucket = 'needsYou' | 'working' | 'idle' | 'failed';

/** Bucket iteration order, loudest first — the order a grouped roster renders its sections. */
export const AGENT_ATTENTION_BUCKETS: readonly AgentAttentionBucket[] = [
  'needsYou',
  'working',
  'idle',
  'failed',
];

/**
 * Collapse a fine-grained {@link AgentStatus} into its coarse attention bucket. A `Record`
 * (not a `switch`) so adding an AgentStatus without bucketing it is a COMPILE error. Parity:
 * Orca's crewStatusGroup (permission/blocked/waiting -> needsYou; working/monitoring -> working;
 * failed -> failed; everything settled/ambiguous -> idle).
 */
export const AGENT_STATUS_BUCKET: Record<AgentStatus, AgentAttentionBucket> = {
  waiting: 'needsYou',
  blocked: 'needsYou',
  permission: 'needsYou',
  failed: 'failed',
  // A dropped/missing-evidence or settled turn is quiet, never a Needs-You.
  unconfirmed: 'idle',
  working: 'working',
  monitoring: 'working',
  interrupted: 'idle',
  unverifiable: 'idle',
  done: 'idle',
  idle: 'idle',
};

/** The coarse attention bucket for a status. Pure. */
export function agentStatusBucket(status: AgentStatus): AgentAttentionBucket {
  return AGENT_STATUS_BUCKET[status];
}

/** Whether a status is in the loudest bucket (the operator must act). Pure. */
export function agentStatusNeedsYou(status: AgentStatus): boolean {
  return agentStatusBucket(status) === 'needsYou';
}

/**
 * Visual tone for a status row — the semantic color role a frontend maps to its theme
 * (never a raw color, so core stays display-agnostic). `attention` = act now, `active` =
 * live work, `error` = failed, `warning` = ambiguous/missing-evidence, `muted` = quiet.
 */
export type AgentStatusTone = 'attention' | 'active' | 'error' | 'warning' | 'muted';

/** One status's glyph + tone — the single display descriptor every surface reuses. */
export interface AgentStatusGlyph {
  /** A terminal-safe single-width marker. */
  glyph: string;
  tone: AgentStatusTone;
}

/**
 * State -> glyph + tone map. A `Record` so a new AgentStatus without a glyph is a COMPILE
 * error. The two missing-evidence states (`unconfirmed`, `unverifiable`) deliberately share
 * the question-glyph + warning tone — they prove MISSING evidence, never completion, so they
 * must NEVER borrow the done check-mark (parity: Orca's "use the missing-evidence glyph, never
 * completion" rule).
 */
export const AGENT_STATUS_GLYPH: Record<AgentStatus, AgentStatusGlyph> = {
  waiting: { glyph: '◆', tone: 'attention' },
  blocked: { glyph: '⊘', tone: 'attention' },
  permission: { glyph: '?', tone: 'attention' },
  failed: { glyph: '✗', tone: 'error' },
  unconfirmed: { glyph: '⁇', tone: 'warning' },
  working: { glyph: '●', tone: 'active' },
  monitoring: { glyph: '◉', tone: 'active' },
  interrupted: { glyph: '■', tone: 'muted' },
  unverifiable: { glyph: '⁇', tone: 'warning' },
  done: { glyph: '✓', tone: 'muted' },
  idle: { glyph: '○', tone: 'muted' },
};

/** The glyph descriptor for a status. Pure. */
export function agentStatusGlyph(status: AgentStatus): AgentStatusGlyph {
  return AGENT_STATUS_GLYPH[status];
}

/**
 * The cli roster's lifecycle vocabulary (cli/src/subagent-roster.ts `SubagentStatus`):
 * a registered-but-unstarted `pending`, a live `running`, and the three terminal outcomes.
 * Re-declared here (not imported — core must never import cli) so {@link subagentStatusToAgentStatus}
 * can lift it into the canonical taxonomy.
 */
export type SubagentLifecycleStatus = 'pending' | 'running' | 'done' | 'failed' | 'killed';

/**
 * Orthogonal transient flags that layer on top of a live subagent's base status, mirroring
 * the cli roster node's flags. They refine a `running`/`pending` node into a finer canonical
 * attention state (an `awaitingApproval` running node is `permission`, i.e. Needs-You).
 */
export interface SubagentStatusFlags {
  isIdle?: boolean;
  awaitingApproval?: boolean;
  awaitingPlanApproval?: boolean;
  shutdownRequested?: boolean;
  hasError?: boolean;
}

/**
 * Lift a cli roster lifecycle status (+ its transient flags) into the canonical
 * {@link AgentStatus} taxonomy, with a fixed precedence so chrome and detail agree:
 *   error > terminal outcome > permission wait > shutdown > idle > live work.
 * This is the one mapper event-processor / the agent board derive from, so the cli roster
 * never has to know the canonical vocabulary. Pure.
 */
export function subagentStatusToAgentStatus(
  status: SubagentLifecycleStatus,
  flags?: SubagentStatusFlags,
): AgentStatus {
  // A hard error outranks even a nominally-successful terminal status.
  if (flags?.hasError) return 'failed';
  if (status === 'failed') return 'failed';
  // A user Stop is an interruption, not a finish.
  if (status === 'killed') return 'interrupted';
  if (status === 'done') return 'done';
  // Live (pending/running): a permission/plan wait is the loudest live state.
  if (flags?.awaitingApproval || flags?.awaitingPlanApproval) return 'permission';
  if (flags?.shutdownRequested) return 'monitoring';
  if (flags?.isIdle) return 'idle';
  // A registered-but-unstarted node is queued work; a running node is working.
  return 'working';
}
