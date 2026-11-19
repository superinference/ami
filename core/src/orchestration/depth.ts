// ===========================================================================
// Agent lineage graph + dispatch-depth cap.
//
// Faithful AMI instance of Orca's runtime/orchestration/db/dispatch-depth.ts.
// Orca persists a creator identity on every Dispatch row and derives a child's
// nesting depth from the live Dispatch its creator is itself working on, so a
// worker that spins up its own Run can't reset the fence (the "run-create
// bypass"). AMI has no SQLite orchestration DB, so the same mechanism lives as
// an in-process lineage graph: every spawned agent/task registers its creator,
// depth is derived from the parent, cycles are rejected, and a child's "done"
// is authorized only when the sender matches the recorded assignee.
//
// This is a PURE core building block — no I/O, no imports from common/ or cli/.
// common/ (background-runner, engine) stamps lineage into it and asks
// `canSpawnMore`; system-prompt.ts tells the worker the answer.
// ===========================================================================

import type { SpawnLineage, SpawnCreator } from '../types';

/** The dispatch-depth cap applied when a caller does not supply one. Matches a
 *  shallow default delegation tree (coordinator → worker → sub-worker). */
export const DEFAULT_MAX_SPAWN_DEPTH = 3;

/**
 * `true` when an agent at `depth` is still permitted to spawn a child (the
 * fork-bomb / token-burn rail). A child would land at `depth + 1`, so spawning
 * is allowed while `depth < maxDepth`. Pure; the single source both the system
 * prompt (to TELL the worker) and the runtime fence (to ENFORCE it) consult, so
 * what the model is told can never drift from what is enforced.
 */
export function canSpawnMore(depth: number, maxDepth: number): boolean {
  return depth < maxDepth;
}

/** Raised when registering a spawn under a parent that isn't in the graph. */
export class LineageUnknownParentError extends Error {
  readonly code = 'lineage_unknown_parent';
  constructor(
    readonly id: string,
    readonly parentId: string,
  ) {
    super(`Cannot register ${id}: its creator ${parentId} is not a known agent.`);
    this.name = 'LineageUnknownParentError';
  }
}

/** Raised when a registration would make an agent its own ancestor. */
export class LineageCycleError extends Error {
  readonly code = 'lineage_cycle';
  constructor(
    readonly id: string,
    readonly parentId: string,
  ) {
    super(`Registering ${id} under ${parentId} would create a lineage cycle.`);
    this.name = 'LineageCycleError';
  }
}

/** Raised when a spawn would exceed the dispatch-depth cap. Mirrors Orca's
 *  depth-fence error wording so the operator sees why a delegation was refused. */
export class SpawnDepthExceededError extends Error {
  readonly code = 'spawn_depth_exceeded';
  constructor(
    readonly depth: number,
    readonly maxDepth: number,
  ) {
    super(
      `Delegation may nest at most ${maxDepth} level${maxDepth === 1 ? '' : 's'} deep, ` +
        `so this worker can't start another (depth ${depth}).`,
    );
    this.name = 'SpawnDepthExceededError';
  }
}

/** Parameters to register one spawned agent/task into the lineage graph. */
export interface RegisterSpawnParams {
  readonly id: string;
  /** Per-incarnation id; defaults to `id` when a caller has no separate handle. */
  readonly instanceId?: string;
  readonly creator: SpawnCreator;
  readonly runId?: string | null;
  readonly taskId?: string | null;
  /**
   * When set, registration throws {@link SpawnDepthExceededError} if the derived
   * depth would meet/exceed the cap (the enforcement counterpart to
   * {@link canSpawnMore}). Omit to register without a fence (depth is still
   * recorded for the prompt + dashboard).
   */
  readonly maxDepth?: number;
}

/**
 * In-process parent/child lineage for everything AMI spawns. Keyed by BOTH `id`
 * and `instanceId` (Orca's dual key) so a retry/restart with a fresh incarnation
 * is tracked without losing the logical identity. Derives depth from the creator,
 * rejects cycles, enforces the optional depth cap, and gates settle-authority.
 */
export class LineageGraph {
  private readonly byId = new Map<string, SpawnLineage>();
  private readonly byInstanceId = new Map<string, SpawnLineage>();

  /** Number of distinct logical agents currently tracked. */
  get size(): number {
    return this.byId.size;
  }

  /**
   * Record a spawn and return its lineage. Depth is 0 for a `system` creator and
   * `parent.depth + 1` for an `agent` creator (whose parent MUST already be
   * registered). Throws {@link LineageUnknownParentError} for an unknown parent,
   * {@link LineageCycleError} if the edge would form a cycle, and
   * {@link SpawnDepthExceededError} when `maxDepth` is supplied and the child
   * would breach it.
   */
  registerSpawn(params: RegisterSpawnParams): SpawnLineage {
    const { id, creator, maxDepth } = params;
    const instanceId = params.instanceId ?? id;
    let parentId: string | null = null;
    let depth = 0;
    if (creator.kind === 'agent') {
      parentId = creator.id;
      const parent = this.byId.get(parentId);
      if (!parent) {
        throw new LineageUnknownParentError(id, parentId);
      }
      if (parentId === id || this.isAncestor(id, parentId)) {
        throw new LineageCycleError(id, parentId);
      }
      depth = parent.depth + 1;
    }
    if (maxDepth !== undefined && depth > maxDepth) {
      throw new SpawnDepthExceededError(depth, maxDepth);
    }
    const lineage: SpawnLineage = {
      id,
      instanceId,
      parentId,
      creator,
      runId: params.runId ?? null,
      taskId: params.taskId ?? null,
      depth,
    };
    this.byId.set(id, lineage);
    this.byInstanceId.set(instanceId, lineage);
    return lineage;
  }

  /** Lineage for a logical id (latest incarnation), if known. */
  get(id: string): SpawnLineage | undefined {
    return this.byId.get(id);
  }

  /** Lineage for a specific incarnation, if known. */
  getByInstanceId(instanceId: string): SpawnLineage | undefined {
    return this.byInstanceId.get(instanceId);
  }

  /** Depth of a known agent, or 0 for an unknown/root agent. */
  depthOf(id: string): number {
    return this.byId.get(id)?.depth ?? 0;
  }

  /**
   * Whether the agent `id` may spawn a child under `maxDepth`. An unknown id is
   * treated as a root (depth 0), so an un-stamped top-level agent can always
   * delegate. Pure read — the runtime fence counterpart is {@link registerSpawn}
   * with a `maxDepth`.
   */
  canSpawnMore(id: string, maxDepth: number): boolean {
    return canSpawnMore(this.depthOf(id), maxDepth);
  }

  /**
   * Lineage-authority gate: a child's settle ("done"/"failed") is accepted only
   * when the sender's identity matches the recorded assignee — the child's own
   * `id` or `instanceId`. A message whose sender is anyone else (a sibling, a
   * spoof, an unknown id) is rejected, so one agent cannot settle another's work.
   * Mirrors Orca's `hasLifecycleAuthority` assignee check.
   */
  isAuthorizedToSettle(childId: string, senderId: string): boolean {
    const node = this.byId.get(childId) ?? this.byInstanceId.get(childId);
    if (!node) return false;
    return senderId === node.id || senderId === node.instanceId;
  }

  /** Direct children of an agent (one lineage hop below). */
  children(id: string): SpawnLineage[] {
    const out: SpawnLineage[] = [];
    for (const node of this.byId.values()) {
      if (node.parentId === id) out.push(node);
    }
    return out;
  }

  /** Ancestor chain from the immediate parent up to the root (nearest first). */
  ancestors(id: string): SpawnLineage[] {
    const out: SpawnLineage[] = [];
    const seen = new Set<string>([id]);
    let cursor = this.byId.get(id)?.parentId ?? null;
    while (cursor && !seen.has(cursor)) {
      const node = this.byId.get(cursor);
      if (!node) break;
      out.push(node);
      seen.add(cursor);
      cursor = node.parentId;
    }
    return out;
  }

  /** Whether `candidateAncestor` is an ancestor of `id` in the current graph. */
  private isAncestor(candidateAncestor: string, id: string): boolean {
    const seen = new Set<string>();
    let cursor: string | null = id;
    while (cursor && !seen.has(cursor)) {
      if (cursor === candidateAncestor) return true;
      seen.add(cursor);
      cursor = this.byId.get(cursor)?.parentId ?? null;
    }
    return false;
  }

  /** All tracked lineage, latest incarnation per logical id (for a fold tree). */
  list(): SpawnLineage[] {
    return [...this.byId.values()];
  }
}
