// Split from types.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { relative, isAbsolute } from 'node:path';
import { isValidPort } from './types-validate-engine-config';
import type { EngineConfig, SandboxPolicy } from './types';

export function sanitizeEngineConfig(config: Partial<EngineConfig>): Partial<EngineConfig> {
  const out = { ...config };
  if (out.maxTurns !== undefined) out.maxTurns = Math.max(1, Math.min(10000, Math.round(out.maxTurns)));
  if (out.maxSessionMs !== undefined) out.maxSessionMs = Math.max(1, Math.min(86_400_000, out.maxSessionMs));
  if (out.maxBudgetUsd !== undefined) out.maxBudgetUsd = Math.max(0.01, Math.min(1000, out.maxBudgetUsd));
  if (out.maxAgentDepth !== undefined) out.maxAgentDepth = Math.max(1, Math.min(20, Math.round(out.maxAgentDepth)));
  if (out.maxUsageLimitWaitMs !== undefined) out.maxUsageLimitWaitMs = Math.max(0, out.maxUsageLimitWaitMs);
  if (out.requestTimeoutMs !== undefined) out.requestTimeoutMs = Math.max(1000, Math.min(600_000, out.requestTimeoutMs));
  if (out.tokenBudget !== undefined) out.tokenBudget = Math.max(1, out.tokenBudget);
  if (out.maxWebSearchesPerSession !== undefined) out.maxWebSearchesPerSession = Math.max(1, Math.round(out.maxWebSearchesPerSession));
  if (out.bashOutputMaxChars !== undefined) out.bashOutputMaxChars = Math.max(1, Math.round(out.bashOutputMaxChars));
  if (out.taskOutputMaxChars !== undefined) out.taskOutputMaxChars = Math.max(1, Math.round(out.taskOutputMaxChars));
  if (out.lspRecommendationIgnoredCount !== undefined) out.lspRecommendationIgnoredCount = Math.max(0, Math.round(out.lspRecommendationIgnoredCount));
  if (out.maxSkillProgressMessages !== undefined) out.maxSkillProgressMessages = Math.max(0, Math.round(out.maxSkillProgressMessages));
  if (out.sandbox !== undefined) out.sandbox = sanitizeSandboxPolicy(out.sandbox);
  return out;
}

/** Drop invalid proxy ports and normalize host/path lists (trim, drop empties,
 *  dedupe). Returns a new policy; never mutates the input. */
export function sanitizeSandboxPolicy(sandbox: SandboxPolicy): SandboxPolicy {
  const cleanList = (list?: string[]): string[] | undefined => {
    if (!list) return undefined;
    const cleaned = [...new Set(list.map((s) => (typeof s === 'string' ? s.trim() : '')).filter((s) => s !== ''))];
    return cleaned.length ? cleaned : undefined;
  };
  const out: SandboxPolicy = { ...sandbox };
  if (sandbox.network) {
    const net = { ...sandbox.network };
    if (net.httpProxyPort !== undefined && !isValidPort(net.httpProxyPort)) delete net.httpProxyPort;
    if (net.socksProxyPort !== undefined && !isValidPort(net.socksProxyPort)) delete net.socksProxyPort;
    net.allowedHosts = cleanList(net.allowedHosts);
    net.deniedHosts = cleanList(net.deniedHosts);
    net.allowUnixSockets = cleanList(net.allowUnixSockets);
    net.managedAllowedHosts = cleanList(net.managedAllowedHosts);
    out.network = net;
  }
  if (sandbox.filesystem) {
    const fsc = { ...sandbox.filesystem };
    fsc.allowWrite = cleanList(fsc.allowWrite);
    fsc.denyWrite = cleanList(fsc.denyWrite);
    fsc.denyRead = cleanList(fsc.denyRead);
    fsc.allowRead = cleanList(fsc.allowRead);
    fsc.managedAllowRead = cleanList(fsc.managedAllowRead);
    out.filesystem = fsc;
  }
  if (sandbox.seccomp) {
    const profilePath = typeof sandbox.seccomp.profilePath === 'string' ? sandbox.seccomp.profilePath.trim() : '';
    out.seccomp = profilePath ? { profilePath } : {};
  }
  if (sandbox.enabledPlatforms) out.enabledPlatforms = cleanList(sandbox.enabledPlatforms);
  if (sandbox.ripgrep) {
    const command = typeof sandbox.ripgrep.command === 'string' ? sandbox.ripgrep.command.trim() : '';
    if (command) {
      const args = cleanList(sandbox.ripgrep.args);
      out.ripgrep = args ? { command, args } : { command };
    } else {
      delete out.ripgrep;
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// w96 parity primitives — pure, dependency-light building blocks. Each mirrors a
// specific the reference behavior; display/rendering stays in the cli layer, these
// only derive/validate data.
// ---------------------------------------------------------------------------

/** Resolved inputs for {@link isInferenceConfigImmediate}. Keeps the decision a
 *  pure, side-effect-free function — the caller resolves the privileged-user flag
 *  and any experiment override from the environment. */
export interface InferenceConfigImmediacy {
  /** Persisted/config toggle ({@link EngineConfig.immediateInferenceConfig}). */
  configEnabled?: boolean;
  /** Privileged internal user. Parity: `process.env.USER_TYPE === 'ant'`. */
  privilegedUser?: boolean;
  /** Experiment / feature-flag override. Parity:
   *  `getFeatureValue_CACHED_MAY_BE_STALE('tengu_immediate_model_command')`. */
  experimentEnabled?: boolean;
}

/**
 * Whether inference-config commands (`/model`, `/thinking`, `/fast`) should apply
 * immediately to an in-flight turn rather than deferring to turn end. Matches
 * the reference's `shouldInferenceConfigCommandBeImmediate()`: a privileged user OR an
 * enabled experiment forces it on; otherwise the persisted config toggle decides
 * (default OFF). Pure.
 */
export function isInferenceConfigImmediate(r: InferenceConfigImmediacy = {}): boolean {
  return r.privilegedUser === true || r.experimentEnabled === true || r.configEnabled === true;
}

// --- Diff-preview width (parity: NotebookEditPermissionRequest `verbose ? 120 : 80`) ---

/** Diff-preview column width used in verbose mode. Parity: the reference's `120`. */
export const VERBOSE_DIFF_PREVIEW_WIDTH = 120;

/** Diff-preview column width used in compact (non-verbose) mode. Parity: the reference's `80`. */
export const COMPACT_DIFF_PREVIEW_WIDTH = 80;

/**
 * Column width for the inline diff preview in a file-edit permission prompt. An
 * explicit {@link EngineConfig.diffPreviewWidth} override wins; otherwise the width
 * follows the verbose flag ({@link VERBOSE_DIFF_PREVIEW_WIDTH} vs
 * {@link COMPACT_DIFF_PREVIEW_WIDTH}). Parity: the reference's `verbose ? 120 : 80`. Pure.
 */
export function resolveDiffPreviewWidth(
  verbose: boolean,
  config?: Pick<EngineConfig, 'diffPreviewWidth'>,
): number {
  const override = config?.diffPreviewWidth;
  if (typeof override === 'number' && Number.isFinite(override) && override > 0) return override;
  return verbose ? VERBOSE_DIFF_PREVIEW_WIDTH : COMPACT_DIFF_PREVIEW_WIDTH;
}

// --- Symlink disclosure in the edit-approval prompt (parity: ShowInIDEPrompt.tsx) ---

/** Informed-consent disclosure describing that a file-edit target is a symlink
 *  pointing somewhere other than the literal edit path. Parity: the reference's
 *  `ShowInIDEPrompt` `symlinkTarget` note. */
export interface SymlinkEditDisclosure {
  /** Realpath the edited symlink resolves to. */
  target: string;
  /** True when the resolved target is OUTSIDE the working directory. */
  outsideWorkspace: boolean;
  /** Pre-rendered warning note for the permission prompt. */
  note: string;
}

/**
 * Build the symlink-target disclosure for a file-edit permission prompt. Returns
 * `undefined` when there is no symlink target or it resolves to the same path being
 * edited (not a redirecting symlink). Otherwise mirrors the reference's wording: a target
 * that escapes the working directory ⇒
 * `"This will modify <target> (outside working directory) via a symlink"`, an
 * in-workspace target ⇒ `"Symlink target: <target>"`. Pure.
 */
export function describeSymlinkEdit(
  editPath: string,
  symlinkTarget: string | null | undefined,
  cwd: string,
): SymlinkEditDisclosure | undefined {
  if (!symlinkTarget || symlinkTarget === editPath) return undefined;
  const rel = relative(cwd, symlinkTarget);
  const outsideWorkspace = rel === '' ? false : rel.startsWith('..') || isAbsolute(rel);
  const note = outsideWorkspace
    ? `This will modify ${symlinkTarget} (outside working directory) via a symlink`
    : `Symlink target: ${symlinkTarget}`;
  return { target: symlinkTarget, outsideWorkspace, note };
}

// --- Worker/subagent attribution badge (parity: WorkerBadge.tsx) ---

/** Identity of a non-primary swarm worker/subagent requesting a permission, so a
 *  prompt can render a colored `● @name` badge. Parity: the reference's `WorkerBadge`
 *  props ({ name, color }). */
export interface WorkerAttribution {
  /** Display name of the requesting worker (rendered as `@name`). */
  name: string;
  /** Theme/ANSI color for the badge dot + name. */
  color?: string;
}

/**
 * Whether a permission prompt should show a worker-attribution badge. The badge is
 * shown ONLY when the request originates from a named, non-primary subagent — the
 * primary session never badges itself. Parity: the reference renders `WorkerBadge` only for
 * swarm workers. Pure.
 */
export function shouldShowWorkerBadge(
  requestingAgent: WorkerAttribution | undefined,
  isPrimary: boolean,
): requestingAgent is WorkerAttribution {
  return !isPrimary && !!requestingAgent && requestingAgent.name.trim() !== '';
}

// --- In-REPL startup notification queue (parity: context/notifications.tsx) ---

/** Default auto-dismiss timeout for a {@link ReplNotification}. Parity: the reference's
 *  `DEFAULT_TIMEOUT_MS = 8000`. */
export const DEFAULT_NOTIFICATION_TIMEOUT_MS = 8000;

/** Priority tier of a {@link ReplNotification}. Parity: the reference's notification
 *  `Priority`. */
export const NOTIFICATION_PRIORITYS = ['low', 'medium', 'high', 'immediate'] as const;

export type NotificationPriority = (typeof NOTIFICATION_PRIORITYS)[number];

/** A transient in-REPL startup notification. Parity: the reference's `Notification`
 *  (context/notifications.tsx) minus the React-specific JSX variant. */
export interface ReplNotification {
  /** Stable identity; a later notification with the same key folds over the earlier. */
  key: string;
  /** Message text to display. */
  text: string;
  /** Display priority; `'immediate'` bypasses the queue to the front. */
  priority: NotificationPriority;
  /** Auto-dismiss timeout (ms); defaults to {@link DEFAULT_NOTIFICATION_TIMEOUT_MS}. */
  timeoutMs?: number;
  /** Keys of queued/displayed notifications this one invalidates (removes). */
  invalidates?: string[];
}

/** Effective auto-dismiss timeout for a notification. Parity: `timeoutMs ?? DEFAULT_TIMEOUT_MS`. */
export function resolveNotificationTimeout(n: Pick<ReplNotification, 'timeoutMs'>): number {
  return n.timeoutMs ?? DEFAULT_NOTIFICATION_TIMEOUT_MS;
}

/**
 * Compute the next notification queue after an incoming notification arrives,
 * mirroring the reference's queue semantics: the incoming notification's `invalidates`
 * keys are dropped from the queue; an existing entry with the SAME key is folded
 * (replaced) by the incoming one; an `'immediate'` notification jumps to the front,
 * any other is appended. Pure — returns a new array, never mutates `existing`.
 */
export function planNotificationQueue(
  existing: readonly ReplNotification[],
  incoming: ReplNotification,
): ReplNotification[] {
  const invalid = new Set(incoming.invalidates ?? []);
  const kept = existing.filter((n) => n.key !== incoming.key && !invalid.has(n.key));
  return incoming.priority === 'immediate' ? [incoming, ...kept] : [...kept, incoming];
}

// --- Slash-command lifecycle beacon (parity: utils/commandLifecycle.ts) ---

/**
 * Lifecycle phase of a single slash-command invocation. Parity: the reference's
 * `commandLifecycle.ts` `CommandLifecycleState` (`'started' | 'completed'`), the
 * lightweight signal a UI/observer consumes to track in-flight command
 * invocations (by {@link SlashCommandLifecycleEvent.uuid}) and render affordances
 * while a command runs.
 */
export const SLASH_COMMAND_LIFECYCLE_STATES = ['started', 'completed'] as const;

export type SlashCommandLifecycleState = (typeof SLASH_COMMAND_LIFECYCLE_STATES)[number];

/** The canonical analytics-event TYPE AMI records each slash-command lifecycle
 *  transition under, so observers (the usage view, remote-control, a `/` spinner)
 *  query ONE event name via `getEventsByType`. Parity: the reference's per-command
 *  started/completed notification, routed through AMI's analytics surface. */
export const SLASH_COMMAND_LIFECYCLE_EVENT = 'slash_command_lifecycle';

/**
 * Structured payload for one slash-command lifecycle transition. `uuid` pairs the
 * `'started'` and `'completed'` records of a single invocation so an observer can
 * measure in-flight commands and their duration; `command` is the
 * display-redacted command line (never the raw argument text). Parity: the reference's
 * `notifyCommandLifecycle(uuid, state)`.
 */
export interface SlashCommandLifecycleEvent {
  uuid: string;
  command: string;
  state: SlashCommandLifecycleState;
}

/**
 * Build the structured payload for a slash-command lifecycle transition. Pure —
 * the single constructor both the `'started'` and `'completed'` emissions use so
 * the two records of one invocation are shaped identically (only `state` differs)
 * and can never drift. Returned as a fresh plain object safe to hand to
 * `analytics.log`.
 */
export function buildSlashCommandLifecycleEvent(
  uuid: string,
  command: string,
  state: SlashCommandLifecycleState,
): SlashCommandLifecycleEvent {
  return { uuid, command, state };
}

// ===========================================================================
// Agent spawn lineage + dispatch-depth cap (Orca parity:
// runtime/orchestration/db/dispatch-depth.ts). First-class parent/child
// lineage for everything AMI spawns, so a child's depth is derived from its
// creator (a fork-bomb / token-burn rail) and a child's "done" is accepted
// only when the sender's identity matches the recorded assignee. The graph
// logic lives in core/core/src/orchestration/depth.ts; these are its types.
// ===========================================================================

/**
 * Identity of whatever created a spawned agent/task, recorded on its lineage so
 * depth can later tell real delegation from host bookkeeping (parity: Orca's
 * `DispatchCreator`). `system` is host-local coordinator code — it never counts
 * toward depth and can't be selected by a caller to bypass the fence. `agent` is
 * another spawned agent (its logical `id`, plus an optional human-readable
 * `handle` for display / audit).
 */
export type SpawnCreator =
  | { readonly kind: 'system' }
  | { readonly kind: 'agent'; readonly id: string; readonly handle?: string | null };

/**
 * First-class parent/child lineage record for one spawned agent/task. Keyed by
 * BOTH `id` (stable logical identity) AND `instanceId` (per-incarnation — a
 * retry/restart of the same logical id gets a fresh `instanceId`), matching
 * Orca's dual-key lineage so a remint can be told from the original. `depth` is
 * 0 for a root/system-spawned agent and `parent.depth + 1` otherwise; it drives
 * the `canSpawnMore = depth < maxDepth` fence the worker is told about in its
 * system prompt.
 */
export interface SpawnLineage {
  readonly id: string;
  readonly instanceId: string;
  /** Logical id of the creating agent, or null for a root/system spawn. */
  readonly parentId: string | null;
  readonly creator: SpawnCreator;
  /** Origin associations carried for the cross-agent dashboard / fleet nav. */
  readonly runId?: string | null;
  readonly taskId?: string | null;
  /** 0 for a root agent; `parent.depth + 1` otherwise. */
  readonly depth: number;
}

// ===========================================================================
// Formal agent/task lifecycle state machine (Orca parity:
// runtime/orchestration/db/lifecycle-transition.ts LEGAL_TRANSITIONS). One
// declarative transition graph driving a guarded transition with optimistic
// from-set CAS. The guard + machine live in core/core/src/orchestration/
// lifecycle.ts; the state vocabulary + legal-edge table live here.
// ===========================================================================

/**
 * The lifecycle states a spawned agent/task moves through. `starting` and
 * `running` are live; `done`, `failed`, and `circuit_broken` are TERMINAL (no
 * outgoing edges — see {@link LEGAL_TRANSITIONS}). Replaces AMI's ad-hoc state
 * tracking spread across engine/detached-runner/task with one shared vocabulary.
 */
export const AGENT_LIFECYCLE_STATES = ['starting', 'running', 'done', 'failed', 'circuit_broken'] as const;

export type AgentLifecycleState = (typeof AGENT_LIFECYCLE_STATES)[number];

/**
 * Declarative legal-transition graph (parity: Orca's `LEGAL_TRANSITIONS`).
 * Non-terminal states include a self-edge so an idempotent re-assert of the same
 * state is legal; the three terminal states have NO outgoing edges, so once an
 * agent settles it can never leave that state. The guarded transition in
 * lifecycle.ts asserts the caller-supplied `from` set (optimistic concurrency)
 * BEFORE checking that `to` is a legal successor, so two writers racing to settle
 * the same agent are rejected (`lifecycle_conflict`) rather than clobbering.
 */
export const LEGAL_TRANSITIONS: Readonly<
  Record<AgentLifecycleState, readonly AgentLifecycleState[]>
> = {
  starting: ['starting', 'running', 'done', 'failed', 'circuit_broken'],
  running: ['running', 'done', 'failed', 'circuit_broken'],
  done: [],
  failed: [],
  circuit_broken: [],
};

/**
 * Why a message to a recipient is NOT delivered the instant it is sent, read from
 * the SAME gates that actually hold it — so a send receipt states only what is true
 * at send time, never a dishonest "delivered". A reader returning `null` (rather than
 * a {@link DeliveryHold}) means delivery goes ahead now. Mirrors Orca's
 * `MailboxDeliveryHold` (runtime/orchestration/mailbox/delivery-hold/
 * mailbox-delivery-hold.ts `describeMailboxDeliveryHold`), narrowed to the states AMI
 * can honestly observe about an inter-agent/subagent recipient:
 *  - `busy`        — the recipient is running but has not settled at its prompt / is
 *                    not actively listening, so the message waits in its mailbox until
 *                    its next tool round.
 *  - `blocked`     — the recipient's OWN state says it is blocked (awaiting a plan
 *                    approval, mid-shutdown handshake, shutting down); `reason` names it.
 *  - `detached`    — the recipient is a known/declared teammate that is not yet
 *                    attached/online; the message waits until it comes up.
 *  - `failed-turn` — the recipient's task ended in failure; the message waits for a
 *                    retry / its next turn rather than being dropped.
 *  - `unreachable` — no live/known agent answers to this name (a likely typo / stale
 *                    name, or a killed/finished agent); `recipient` echoes the name.
 *
 * ADDITIVE/append-only: new members may be appended as AMI learns to observe more
 * recipient states; existing members never change shape so consumers stay stable.
 */
export type DeliveryHold =
  | { kind: 'busy' }
  | { kind: 'blocked'; reason: string }
  | { kind: 'detached' }
  | { kind: 'failed-turn' }
  | { kind: 'unreachable'; recipient: string };
