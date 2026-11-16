import { AsyncLocalStorage } from 'async_hooks';
import type { AgentColorName } from '../skills';

/**
 * AsyncLocalStorage-isolated runtime context for an IN-PROCESS teammate (parity:
 * Claude Code's `utils/teammateContext.ts`). When several teammates run inside the
 * SAME Node process (the `in-process` {@link BackendType}), a module-global
 * identity singleton would have them clobber each other's `agentId`/`agentName`.
 * Storing each teammate's identity in an {@link AsyncLocalStorage} store keyed to
 * the async call-tree it was spawned under keeps every concurrent teammate's
 * identity isolated — the top of the identity-resolution precedence chain in
 * team.ts (ALS > dynamic team context > env).
 */
export interface TeammateContext {
  /** Full agent id, e.g. "researcher@my-team". */
  agentId: string;
  /** Display name, e.g. "researcher". */
  agentName: string;
  /** Team name this teammate belongs to. */
  teamName: string;
  /** Display color assigned to this teammate (from the shared palette). */
  color?: AgentColorName;
  /** Whether this teammate must enter plan mode before implementing. */
  planModeRequired: boolean;
  /** The lead's session id (for transcript correlation). */
  parentSessionId: string;
  /** Discriminator — always true for an in-process teammate. */
  isInProcess: true;
  /** Abort controller linked to the parent, for lifecycle cancellation. */
  abortController: AbortController;
}

const teammateContextStorage = new AsyncLocalStorage<TeammateContext>();

/**
 * The current in-process teammate context, or undefined when the running code is
 * not executing inside a {@link runWithTeammateContext} scope (parity:
 * getTeammateContext). This is the first layer consulted by team.ts's identity
 * accessors.
 */
export function getTeammateContext(): TeammateContext | undefined {
  return teammateContextStorage.getStore();
}

/**
 * Run `fn` with `context` installed as the ambient in-process teammate context
 * (parity: runWithTeammateContext). Every async operation launched inside `fn`
 * sees `context` via {@link getTeammateContext} without it being passed down
 * explicitly, and two concurrent `run` scopes do not see each other's context.
 */
export function runWithTeammateContext<T>(context: TeammateContext, fn: () => T): T {
  return teammateContextStorage.run(context, fn);
}

/**
 * Whether the running code is inside an in-process teammate scope (parity:
 * isInProcessTeammate) — a cheaper check than comparing {@link getTeammateContext}
 * to undefined when only the boolean is needed.
 */
export function isInProcessTeammate(): boolean {
  return teammateContextStorage.getStore() !== undefined;
}

/**
 * Build a {@link TeammateContext} from spawn configuration (parity:
 * createTeammateContext). Normalizes the discriminator (`isInProcess: true`) so a
 * caller only supplies the identity fields and lifecycle controller.
 */
export function createTeammateContext(cfg: {
  agentId: string;
  agentName: string;
  teamName: string;
  color?: AgentColorName;
  planModeRequired?: boolean;
  parentSessionId: string;
  abortController?: AbortController;
}): TeammateContext {
  return {
    agentId: cfg.agentId,
    agentName: cfg.agentName,
    teamName: cfg.teamName,
    color: cfg.color,
    planModeRequired: cfg.planModeRequired ?? false,
    parentSessionId: cfg.parentSessionId,
    isInProcess: true,
    abortController: cfg.abortController ?? new AbortController(),
  };
}
