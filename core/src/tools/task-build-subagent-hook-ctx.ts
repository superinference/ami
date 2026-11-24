// Split from task.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { ToolContext } from '../types';
import { coerceBool } from '../utils/coerce';
import { isEnvTruthy } from '../analytics';
import { permissionRuleValueFromString, type PermissionRule } from '../permissions';
import type { SubagentContext } from '../hooks';
import { EXPLORE_AGENT_MIN_QUERIES, collapseAgentTypeForAnalytics } from './task-link-child-abort-to-parent';

/**
 * Convert a per-spawn `auto_approve_tools` spec list into `allow` {@link PermissionRule}s
 * appended to the spawned subagent's permission rules — the concrete wiring of the reference
 * Code's auto-approve `allowedTools` allowlist. Each spec is parsed with the one
 * canonical {@link permissionRuleValueFromString} (so a bare name → a tool-wide allow,
 * and a patterned `Bash(git:*)` → an allow scoped to that arg glob), yielding a rule
 * the engine's permission layer honors under its rule-evaluated modes (ask/acceptEdits):
 * the listed tool/pattern is auto-approved while every unlisted tool still prompts or
 * denies per the mode. A matching user DENY rule still wins (deny is unconditional), and
 * the standard destructive-/code-exec downgrades still force a prompt for a dangerous
 * command even under one of these allows — so an auto-approval can never silently run an
 * irreversible or arbitrary-code operation. Returns `[]` for an empty/undefined list.
 */
export function buildAutoApproveRules(specs: readonly string[] | undefined): PermissionRule[] {
  if (!specs || specs.length === 0) return [];
  return specs.map(spec => {
    const { toolName, ruleContent } = permissionRuleValueFromString(spec);
    const rule: PermissionRule = { tool: toolName, action: 'allow' };
    if (ruleContent) rule.pattern = ruleContent;
    return rule;
  });
}

/**
 * Resolve the per-spawn `allow_permission_prompts` knob into a permission-mode
 * contribution. Mirrors the reference's `allowPermissionPrompts` spawn flag
 * (`swarm/backends/types.ts`, default false): a spawned agent cannot show an
 * interactive permission prompt for a tool outside its granted scope — those calls
 * are auto-DENIED (`deny-all`) rather than blocking a detached/teammate run on a
 * prompt no one will answer. Passing `true` opts the subagent back into the
 * interactive `ask` handler. Returns `undefined` when the flag is absent, so the
 * existing fallback chain (explicit permission_mode → agent-def → explore → `ask`)
 * is untouched — this is an ADDITIVE opt-in that never changes AMI's default. The
 * companion `permissions` grant from the reference maps onto AMI's existing `allowed_tools`
 * inline tool scope (no separate field — see {@link parseAllowedTools}).
 */
export function resolvePromptGatePermissionMode(raw: unknown): 'ask' | 'deny-all' | undefined {
  if (raw === undefined || raw === null) return undefined;
  return coerceBool(raw) ? 'ask' : 'deny-all';
}

/**
 * Env knob that globally disables background (`run_in_background`) subagents —
 * mirrors the reference's `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS`. When truthy the
 * `run_in_background` field is OMITTED from the advertised input schema (so the
 * model is never offered the flag) AND a late attempt to set it (e.g. a stale /
 * replayed call) is rejected at runtime. Operators/CI use it to turn detached
 * agent spawning off in environments where unsupervised background work is
 * disallowed.
 */
export const DISABLE_BACKGROUND_TASKS_ENV = 'CLAUDE_CODE_DISABLE_BACKGROUND_TASKS';

/** True when background subagents are disabled via {@link DISABLE_BACKGROUND_TASKS_ENV}. */
export function isBackgroundTasksDisabled(): boolean {
  return isEnvTruthy(process.env[DISABLE_BACKGROUND_TASKS_ENV]);
}

/**
 * Env knob that forces EVERY spawned subagent into read-only PLAN mode by default —
 * the agent explores and must return an implementation plan, it may NOT mutate the
 * workspace (edit/write/bash-write are denied by the `plan` permission mode) until a
 * human/parent has reviewed that plan. Mirrors the reference's `planModeRequired`
 * teammate knob and its `CLAUDE_CODE_PLAN_MODE_REQUIRED` env default (the reference name
 * is honored too, matching how AMI accepts the reference spelling for other knobs). A
 * per-spawn `plan_mode_required` input overrides this default either way.
 */
export const SUBAGENT_PLAN_REQUIRED_ENV = 'AMI_SUBAGENT_PLAN_REQUIRED';

/** the reference cross-framework alias for {@link SUBAGENT_PLAN_REQUIRED_ENV}. */
export const CLAUDE_PLAN_MODE_REQUIRED_ENV = 'CLAUDE_CODE_PLAN_MODE_REQUIRED';

/** True when spawned subagents default to plan-required via either env spelling. */
export function isSubagentPlanRequiredByEnv(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[SUBAGENT_PLAN_REQUIRED_ENV]) || isEnvTruthy(env[CLAUDE_PLAN_MODE_REQUIRED_ENV]);
}

/**
 * Resolve whether THIS spawn must run plan-required (read-only until a plan is
 * reviewed). Three-tier, matching the reference's `isPlanModeRequired()` precedence:
 * an explicit per-spawn `plan_mode_required` input wins; absent → the env default
 * ({@link isSubagentPlanRequiredByEnv}). A missing/garbage input contributes nothing
 * so the env default decides — purely additive (default off → no change).
 */
export function resolvePlanRequired(raw: unknown, env: NodeJS.ProcessEnv = process.env): boolean {
  if (raw !== undefined && raw !== null) return coerceBool(raw);
  return isSubagentPlanRequiredByEnv(env);
}

/**
 * Clamp a resolved permission mode to an "at least read-only plan" floor when the
 * spawn is plan-required. The `plan` mode allows read-only tools and DENIES every
 * mutation, so forcing it is always a DE-escalation. `deny-all` is stricter still
 * (denies reads too) and is never loosened; everything more permissive than `plan`
 * (ask/acceptEdits/auto-allow/bypassPermissions) is locked down to `plan`. A no-op
 * when `planRequired` is false, so the existing resolution is unchanged by default.
 */
export function applyPlanRequirement(mode: string, planRequired: boolean): string {
  if (!planRequired) return mode;
  return mode === 'deny-all' ? mode : 'plan';
}

/**
 * Notice prepended to a plan-required subagent's first task turn so it knows the
 * read-only boundary is intentional: produce a concrete implementation plan and
 * return it, do NOT attempt to write code (edits/writes/mutating bash are denied
 * and will fail). Mirrors the reference's plan-mode spawn directive.
 */
export function buildPlanRequiredNotice(): string {
  return 'PLAN MODE REQUIRED: You are running read-only. Investigate, then return a concrete, step-by-step implementation plan for this task. Do NOT edit files, write files, or run mutating shell commands — those tools are denied in this mode and any such attempt will fail. Hand the plan back for review; implementation happens only after it is approved.';
}

/**
 * System-prompt addendum appended to a subagent that is spawned as a named member
 * of an active team (mirrors the reference's teammatePromptAddendum). AMI already
 * has all the machinery — `send_message` (with `to: "*"` broadcast), team
 * mailboxes with eager delivery, the shared team task list — but nothing TOLD a
 * teammate that (a) it is one agent in a team, (b) it reaches teammates via
 * `send_message({ to: "<name>" })`, (c) a plain text reply is invisible to
 * teammates so `send_message` is REQUIRED, and (d) the user talks mainly to the
 * lead and work is coordinated through the task system + messaging.
 */
export const TEAMMATE_PROMPT_ADDENDUM =
  `You are running as an agent in a team of agents collaborating on this work.
- Reach a teammate with the send_message tool: send_message({ to: "<name>", message: ... }); broadcast to everyone with to: "*".
- CRITICAL: just writing a response in text is NOT visible to others on your team — you MUST use the send_message tool to communicate with them.
- The user interacts primarily with the team lead. Your work is coordinated through the shared task system and teammate messaging, not by replying in plain text.`;

/**
 * Build the caller-requested thoroughness directive for an `explore`-mode
 * sub-agent (mirrors the reference's exploreAgent thoroughness parameter). The
 * explore prompt tells the agent to "adapt your search breadth to the
 * thoroughness the caller requests"; this turns an abstract instruction into a
 * concrete, caller-chosen depth dial so a "very thorough" request produces a
 * comprehensive multi-location/naming-convention sweep and a "quick" request a
 * single-pass lookup.
 */
export function buildThoroughnessDirective(level: string): string {
  switch (level) {
    case 'quick':
      return '[Thoroughness: quick] Do a fast, single-pass lookup — the smallest search that answers the question, then stop.';
    case 'very thorough':
      return `[Thoroughness: very thorough] Do a comprehensive sweep: search across multiple locations and alternate naming conventions, run well beyond the ${EXPLORE_AGENT_MIN_QUERIES}-query floor, and cross-check before concluding.`;
    case 'medium':
    default:
      return '[Thoroughness: medium] Do moderate exploration — check the obvious locations and a couple of alternate strategies before reporting.';
  }
}

/** Valid values for the explore `thoroughness` dial. */
export const EXPLORE_THOROUGHNESS_LEVELS = ['quick', 'medium', 'very thorough'] as const;

/**
 * One curated, selectable sub-agent model option with a per-tier affordance
 * description (parity: the reference's curated agent model options with per-tier
 * guidance + an explicit "Inherit from parent" choice, `utils/model/agent.ts`).
 * `value` is what the caller passes as the `model` arg — the empty string denotes
 * the default of OMITTING the field (inherit the parent conversation's model).
 */
export interface SubagentModelOption {
  /** The `model` arg to pass; '' means omit the field (inherit the parent). */
  value: string;
  /** Short human label. */
  label: string;
  /** When to pick this tier. */
  description: string;
}

/**
 * Path of the clean FINAL-ANSWER sidecar for a background agent, derived from its
 * `<taskId>.output` transcript path (`<taskId>.result`). Parity with the reference's
 * TaskOutputTool preferring the agent's in-memory final answer over the raw on-disk
 * transcript: the `.output` file streams the whole noisy run (thinking, tool calls,
 * tool results), whereas `.result` holds ONLY the agent's last text-bearing turn —
 * the deliverable the parent actually wants. A non-agent background task (a bash
 * job) never writes this sidecar, so its presence also discriminates an agent task.
 */
export function agentResultPath(outputPath: string): string {
  return outputPath.replace(/\.output$/, '.result');
}

/**
 * Build the {@link SubagentContext} payload fired on the SubagentStart / SubagentStop
 * lifecycle hooks for a spawned sub-agent. Shared by the FOREGROUND and BACKGROUND
 * spawn paths so both emit an identical attribution/lineage payload — including the
 * discriminated-union `agentContextKind` (a named spawn joined to a team is a
 * `'teammate'`, every other spawn a plain `'subagent'`; parity: the reference's
 * subagent-vs-teammate agent-context discriminator). Before this was factored out the
 * background path fired NO lifecycle hook at all, so a SubagentStart/Stop hook never
 * saw async/background agents and their `agentContextKind` was invisible to hook
 * consumers — only foreground spawns emitted it.
 */
export function buildSubagentHookCtx(p: {
  context: ToolContext;
  taskId: string;
  subSessionId?: string;
  prompt: string;
  /** The EFFECTIVE run mode (fork/explore may reassign it to 'general'); surfaced as
   *  the ctx `mode` field. */
  mode: string;
  /** The RAW requested mode used ONLY for the privacy-safe analytics label, so the
   *  analytics bucket mirrors what the caller asked for (parity with the foreground
   *  path, which collapsed the raw `mode`, not the reassigned effective one). */
  rawMode: string;
  resumed: boolean;
  isBuiltIn: boolean;
  subagentType: string | undefined;
  chainId: string;
  depth: number;
  isTeammate: boolean;
}): SubagentContext {
  return {
    parentSessionId: p.context.sessionId || p.taskId,
    subagentSessionId: p.subSessionId || p.taskId,
    prompt: p.prompt,
    mode: p.mode,
    invocationKind: p.resumed ? 'resume' : 'spawn',
    isBuiltIn: p.isBuiltIn,
    analyticsAgentType: collapseAgentTypeForAnalytics(p.subagentType || p.rawMode, p.isBuiltIn),
    invokingRequestId: p.context.agentId || p.context.sessionId || p.taskId,
    chainId: p.chainId,
    depth: p.depth,
    agentContextKind: p.isTeammate ? 'teammate' : 'subagent',
  };
}
