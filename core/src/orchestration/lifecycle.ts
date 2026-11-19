// ===========================================================================
// Formal agent/task lifecycle state machine.
//
// Faithful AMI instance of Orca's runtime/orchestration/db/lifecycle-transition.ts
// (LEGAL_TRANSITIONS + transitionLifecycleWithDb). One declarative transition
// graph (see LEGAL_TRANSITIONS in ../types) drives a guarded transition that
// asserts the row's `from` set — optimistic concurrency — BEFORE checking a
// legal successor. Orca binds this to a SQLite row with an `UPDATE ... WHERE
// state IN (from)` CAS; AMI has no orchestration DB, so the same guard runs
// over an in-process Map (the `from`-set assertion IS the compare-and-swap:
// once one writer settles the agent, the stored state no longer matches a
// second writer's `from` set, so the race is rejected as a lifecycle_conflict
// instead of silently clobbering).
//
// Pure core building block — no imports from common/ or cli/.
// ===========================================================================

import { LEGAL_TRANSITIONS, type AgentLifecycleState } from '../types';

/** Thrown when a transition targets an id the machine has never seen. */
export class LifecycleNotFoundError extends Error {
  readonly code = 'lifecycle_not_found';
  constructor(readonly id: string) {
    super(`Lifecycle entity ${id} was not found.`);
    this.name = 'LifecycleNotFoundError';
  }
}

/**
 * Thrown when a transition is rejected: either the stored state is not in the
 * caller's `from` set (lost the optimistic-concurrency CAS) or `to` is not a
 * legal successor of the stored state. `state` is the observed current state.
 */
export class LifecycleConflictError extends Error {
  readonly code = 'lifecycle_conflict';
  constructor(
    message: string,
    readonly id: string,
    readonly state: AgentLifecycleState,
    readonly to?: AgentLifecycleState,
  ) {
    super(message);
    this.name = 'LifecycleConflictError';
  }
}

/** Whether `state` is terminal (no outgoing edges in the legal graph). */
export function isTerminalState(state: AgentLifecycleState): boolean {
  return LEGAL_TRANSITIONS[state].length === 0;
}

/** Whether `to` is a legal successor of `from` per the declarative graph. */
export function canTransition(from: AgentLifecycleState, to: AgentLifecycleState): boolean {
  return LEGAL_TRANSITIONS[from].includes(to);
}

/**
 * Pure guarded transition over a known current state (DB-free analogue of Orca's
 * `transitionLifecycleWithDb`): assert `current` ∈ `from` (optimistic CAS), then
 * assert `to` is a legal successor, and return the new state. Throws
 * {@link LifecycleConflictError} on either failure. Reused by {@link LifecycleMachine}
 * and callable directly by a writer that holds the current state another way.
 */
export function applyTransition(
  id: string,
  current: AgentLifecycleState,
  from: AgentLifecycleState | readonly AgentLifecycleState[],
  to: AgentLifecycleState,
): AgentLifecycleState {
  const allowed = Array.isArray(from) ? from : [from as AgentLifecycleState];
  if (!allowed.includes(current)) {
    throw new LifecycleConflictError(
      `${id} is ${current}; expected ${allowed.join(' or ')}.`,
      id,
      current,
      to,
    );
  }
  if (!LEGAL_TRANSITIONS[current].includes(to)) {
    throw new LifecycleConflictError(
      `${id} cannot transition from ${current} to ${to}.`,
      id,
      current,
      to,
    );
  }
  return to;
}

/** Parameters for a guarded {@link LifecycleMachine.transition}. */
export interface LifecycleTransitionParams {
  readonly id: string;
  /** The state(s) the caller believes the entity is in (optimistic CAS set). */
  readonly from: AgentLifecycleState | readonly AgentLifecycleState[];
  readonly to: AgentLifecycleState;
}

/**
 * In-process lifecycle registry replacing AMI's ad-hoc, scattered agent/task
 * state tracking with one declarative state machine. Every entity has exactly
 * one current state; {@link transition} is the only mutator and runs the guarded
 * CAS, so two writers settling the same subagent are serialized — the first wins,
 * the second is rejected with {@link LifecycleConflictError}.
 */
export class LifecycleMachine {
  private readonly states = new Map<string, AgentLifecycleState>();

  /** Number of tracked entities. */
  get size(): number {
    return this.states.size;
  }

  /**
   * Register a new entity at `initial` (default `starting`). Returns the state.
   * Re-registering an existing id is a no-op that returns its CURRENT state (so a
   * duplicate register never silently resets a live/terminal entity).
   */
  register(id: string, initial: AgentLifecycleState = 'starting'): AgentLifecycleState {
    const existing = this.states.get(id);
    if (existing !== undefined) return existing;
    this.states.set(id, initial);
    return initial;
  }

  /** Current state of an entity, or undefined if never registered. */
  get(id: string): AgentLifecycleState | undefined {
    return this.states.get(id);
  }

  /** Whether an entity is registered. */
  has(id: string): boolean {
    return this.states.has(id);
  }

  /**
   * Guarded transition with optimistic from-set CAS. Throws
   * {@link LifecycleNotFoundError} for an unknown id and
   * {@link LifecycleConflictError} when the stored state is not in `from` or `to`
   * is an illegal successor. On success the stored state is updated and
   * `{ changed: true }` is returned.
   */
  transition(params: LifecycleTransitionParams): { changed: boolean } {
    const current = this.states.get(params.id);
    if (current === undefined) {
      throw new LifecycleNotFoundError(params.id);
    }
    const next = applyTransition(params.id, current, params.from, params.to);
    this.states.set(params.id, next);
    return { changed: true };
  }

  /** Whether an entity has settled into a terminal state. */
  isSettled(id: string): boolean {
    const state = this.states.get(id);
    return state !== undefined && isTerminalState(state);
  }

  /** Snapshot of every tracked entity (for a fleet / dashboard view). */
  list(): { id: string; state: AgentLifecycleState }[] {
    return [...this.states.entries()].map(([id, state]) => ({ id, state }));
  }
}
