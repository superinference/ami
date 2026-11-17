import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { expandPath, realpathNFC } from './utils/path';
import { memoizeWithLRU } from './utils/memoize';
import { atomicWriteFileSync, OWNER_ONLY_FILE_MODE } from './utils/fs-atomic';
import { extractCommandPaths, commandTargetsCredentialPath, commandTargetsSystemDir, isCredentialPath, detectUnsafeHelpCommand, detectArchiveExtractionWithGit, detectSymlinkCreateCompound, detectCdSensitiveDesync, WRAPPER_VALUE_FLAGS } from './tools/bash-security';
import { isPowerShellNeverSuggest, isReadOnlyPowerShellCommand } from './tools/powershell-security';
import { deobfuscateEnvExpansion } from './tools/command-normalize';
import { isDangerousCodeExecPrefix } from './dangerous-code-exec';
import { getManagedConfigPath } from './config';
import { classifyAutoModeUnavailableReason, type AutoModeUnavailableReason } from './auto-mode';
import { evaluateToolCeiling, type ToolCapabilityCeiling } from './tools/scoped-permissions';
import { isReadOnlyBashCommand } from './tools/bash-cache';
import type { SandboxPolicy } from './types';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// Each mode is a DISTINCT permission semantic (names mix AMI-native kebab and
// Claude-Code wire-names — that is intentional, we match CC semantics, not a naming
// convention). `dontAsk` was removed: it was a semantic DUPLICATE of `deny-all` (both
// deny every tool call); normalizePermissionMode maps the legacy spelling to deny-all.
export const PERMISSION_MODES = ['ask', 'auto-allow', 'deny-all', 'acceptEdits', 'bypassPermissions', 'plan'] as const;
export type PermissionMode = (typeof PERMISSION_MODES)[number];

/**
 * Non-canonical permission-mode spellings → the canonical {@link PermissionMode}
 * they denote. This is purely ADDITIVE acceptance: it widens the set of strings
 * {@link normalizePermissionMode} will ACCEPT, it never changes what a canonical
 * mode means and never maps anything to a MORE-permissive default (unknown stays
 * `null`). Keys are matched exactly first, then case-insensitively.
 *
 * Parity note: Claude Code's interactive default mode is named `default`; AMI
 * names that same semantic `ask`. A Claude settings.json (`"defaultMode":
 * "default"`) or `--permission-mode default` must therefore resolve to `ask`
 * rather than be rejected. The kebab/camel/lowercase variants let a rule file
 * authored against either naming convention load without a spelling mismatch
 * (the mode names deliberately mix AMI-kebab and CC-camel, see above).
 */
export const PERMISSION_MODE_ALIASES: Record<string, PermissionMode> = {
  // Legacy removed duplicate (both denied every tool call) — kept for back-compat.
  dontAsk: 'deny-all',
  // Claude Code's canonical name for the interactive default.
  default: 'ask',
  normal: 'ask',
  interactive: 'ask',
  // Case/kebab-insensitive spellings of the mixed-naming canonical modes.
  acceptedits: 'acceptEdits',
  'accept-edits': 'acceptEdits',
  bypasspermissions: 'bypassPermissions',
  'bypass-permissions': 'bypassPermissions',
  bypass: 'bypassPermissions',
  denyall: 'deny-all',
  autoallow: 'auto-allow',
  'plan-mode': 'plan',
};

/**
 * The single acceptance boundary for a permission-mode string (discovery-driven: the
 * valid set IS {@link PERMISSION_MODES}, so a new mode is accepted everywhere the moment
 * it is added to the const — no hardcoded subset can drift, cf. ui#19). Returns the
 * canonical mode, resolves a non-canonical spelling via {@link PERMISSION_MODE_ALIASES}
 * (exact match first, then case-insensitively — e.g. `default` → `ask`, the Claude Code
 * wire name), or null if unknown. Every consumer (CLI `--permission-mode`, project
 * config, `/permissions <mode>`) MUST validate through this rather than an inline list.
 */
export function normalizePermissionMode(raw: string): PermissionMode | null {
  const m = raw.trim();
  // Canonical modes win first — an alias can never shadow or re-map a real mode.
  if ((PERMISSION_MODES as readonly string[]).includes(m)) return m as PermissionMode;
  // Exact alias (preserves camel spellings like `dontAsk`), then case-insensitive.
  return PERMISSION_MODE_ALIASES[m] ?? PERMISSION_MODE_ALIASES[m.toLowerCase()] ?? null;
}

/**
 * The ordered set of permission modes a user cycles through interactively (the
 * Claude Code shift-tab cycle: default → accept-edits → plan → default). The
 * blanket modes (`deny-all`, `auto-allow`, `bypassPermissions`) are deliberately
 * EXCLUDED so an accidental keypress can never silently drop into a mode that
 * allows or denies everything — those are reachable only via an explicit flag.
 */
export const PERMISSION_MODE_CYCLE: readonly PermissionMode[] = ['ask', 'acceptEdits', 'plan'];

/**
 * Advance the permission mode one step around {@link PERMISSION_MODE_CYCLE}
 * (parity with Claude Code's shift-tab mode toggle). `forward` mirrors shift-tab,
 * `backward` the reverse. If `current` is a flag-only blanket mode outside the
 * cycle, re-enter at the safe interactive default (`forward`) or the last cycle
 * entry (`backward`) — a user-initiated cycle never auto-widens a blanket deny
 * into a permissive blanket mode; it lands on a rule-evaluated mode.
 */
export function cyclePermissionMode(
  current: PermissionMode,
  direction: 'forward' | 'backward' = 'forward',
): PermissionMode {
  const n = PERMISSION_MODE_CYCLE.length;
  const idx = PERMISSION_MODE_CYCLE.indexOf(current);
  if (idx === -1) {
    return direction === 'forward' ? PERMISSION_MODE_CYCLE[0]! : PERMISSION_MODE_CYCLE[n - 1]!;
  }
  const next = direction === 'forward' ? (idx + 1) % n : (idx - 1 + n) % n;
  return PERMISSION_MODE_CYCLE[next]!;
}

/**
 * Where a permission rule/update is persisted, in precedence order (later wins at
 * runtime; earlier is broader scope). Mirrors Claude's PermissionUpdateDestination:
 * - `userSettings`   — global (~/.ami), applies everywhere;
 * - `projectSettings`— shared per-project, committed (cwd/.ami);
 * - `localSettings`  — per-project but gitignored (cwd/.ami, *.local.json);
 * - `session`        — in-memory only, this run;
 * - `cliArg`         — supplied via a command-line flag.
 */
export const PERMISSION_UPDATE_DESTINATIONS = [
  'userSettings', 'projectSettings', 'localSettings', 'session', 'cliArg',
] as const;
export type PermissionUpdateDestination = (typeof PERMISSION_UPDATE_DESTINATIONS)[number];

/**
 * The full set of tiers a permission rule can ORIGINATE from — a superset of the
 * writable {@link PERMISSION_UPDATE_DESTINATIONS}. It adds three read-only sources
 * that a rule can be loaded/evaluated from but can NEVER be persisted to:
 *  - `policySettings` — managed/enterprise rules (admin-owned, highest precedence);
 *  - `flagSettings`   — rules injected from a settings file named by a launch flag;
 *  - `command`        — rules contributed at runtime by a slash command / tool.
 * Mirrors Claude's `PermissionRuleSource` (types/permissions.ts L54-63), which is
 * deliberately wider than its writable `PermissionUpdateDestination` set.
 */
export const PERMISSION_RULE_SOURCES = [
  'userSettings', 'projectSettings', 'localSettings', 'session', 'cliArg',
  'policySettings', 'flagSettings', 'command',
] as const;
export type PermissionRuleSource = (typeof PERMISSION_RULE_SOURCES)[number];

/**
 * Validate an arbitrary value against the known {@link PERMISSION_RULE_SOURCES},
 * returning the typed source or `undefined`. Lets a persisted rule carry its own
 * `source` tag (e.g. a managed/flag file that bundles rules from several origins)
 * while a bogus value falls back to the loader's default tier.
 */
export function coercePermissionRuleSource(raw: unknown): PermissionRuleSource | undefined {
  return typeof raw === 'string' && (PERMISSION_RULE_SOURCES as readonly string[]).includes(raw)
    ? (raw as PermissionRuleSource)
    : undefined;
}

/** A rule source is managed/enterprise-owned iff it is `policySettings`. Under the
 *  {@link PermissionManager}'s managed-rules-only lockdown, ONLY these are honored. */
export function isManagedPermissionSource(source: PermissionRuleSource | undefined): boolean {
  return source === 'policySettings';
}

/**
 * Is a rule's {@link PermissionRuleSource} SHARED (visible to / imposed on other
 * users) rather than PERSONAL to this machine/run? Shared tiers are committed or
 * pushed to the whole team: `projectSettings` (committed to git), `policySettings`
 * (enterprise-managed), and `command` (slash-command frontmatter). Personal tiers
 * (`userSettings`, `localSettings`, `cliArg`, `session`, `flagSettings`) only
 * affect the current user. An undefined source is treated as personal (the safe
 * default — it is never attributed to the team). Mirrors Claude's
 * `isSharedSettingSource` (utils/permissions/shadowedRuleDetection.ts L61-67); it
 * gates whether the sandbox-auto-allow shadow carve-out applies (a personal ask
 * rule a user's own sandbox would auto-allow is NOT reported as shadowing, but a
 * shared one still warns because teammates may lack the sandbox).
 */
export function isSharedSettingSource(source: PermissionRuleSource | undefined): boolean {
  return source === 'projectSettings' || source === 'policySettings' || source === 'command';
}

/** Human-readable prose for each {@link PermissionRuleSource}, for naming WHERE a
 *  rule came from in `/permissions` shadow warnings, `/doctor` findings, and fix
 *  suggestions (an opaque token like `localSettings` is unreadable). Mirrors
 *  Claude's `permissionRuleSourceDisplayString`. */
const PERMISSION_RULE_SOURCE_DISPLAY: Record<PermissionRuleSource, string> = {
  userSettings: 'user settings',
  projectSettings: 'project settings',
  localSettings: 'local settings',
  session: 'session rules',
  cliArg: 'command-line arguments',
  policySettings: 'enterprise managed settings',
  flagSettings: 'flag settings',
  command: 'slash command',
};

/** Prose label for a rule source ('project settings', …); 'an unknown source'
 *  when the rule carries no source tag. Used in shadow-detection reasons/fixes. */
export function permissionRuleSourceDisplayString(source: PermissionRuleSource | undefined): string {
  return source ? PERMISSION_RULE_SOURCE_DISPLAY[source] : 'an unknown source';
}

export interface PermissionRule {
  tool: string;       // tool name or '*'
  pattern?: string;   // glob pattern for args (e.g., "git *")
  /** Glob match on named input params, e.g. { model: 'opus*' } for Tool(param:value). */
  paramPattern?: Record<string, string>;
  action: 'allow' | 'deny' | 'ask';
  reason?: string;    // optional reason for deny rules
  /** Tier this rule originated from (set when loaded from a source file). */
  source?: PermissionRuleSource;
}

/**
 * Plain-language, one-line preview of what a {@link PermissionRule} does, for
 * showing the user — under a pattern input while they type, and in the "Rule
 * added" confirmation — what they are about to grant/deny BEFORE it takes effect.
 * Parity: Claude's `PermissionRuleDescription`. Reads behavior + tool + scope +
 * (optionally) origin tier, e.g.:
 *   `Allow bash commands matching "git status"`
 *   `Deny edits to files matching "/etc/**" (from enterprise managed settings)`
 *   `Ask before any web_fetch call`
 */
export function describeRule(rule: PermissionRule): string {
  const verb = rule.action === 'allow' ? 'Allow' : rule.action === 'deny' ? 'Deny' : 'Ask before';
  const tool = rule.tool === '*' ? 'any tool' : rule.tool;
  let scope: string;
  if (rule.pattern && rule.pattern !== '*') {
    scope = ` matching "${rule.pattern}"`;
  } else if (rule.paramPattern && Object.keys(rule.paramPattern).length > 0) {
    scope = ` with ${Object.entries(rule.paramPattern).map(([k, v]) => `${k}="${v}"`).join(', ')}`;
  } else {
    scope = ' calls';
  }
  const origin = rule.source ? ` (from ${permissionRuleSourceDisplayString(rule.source)})` : '';
  return `${verb} ${tool}${scope}${origin}`;
}

/**
 * How a higher-priority rule makes a lower one unreachable:
 *  - `deny` — the allow is COMPLETELY blocked (a tool-wide deny always wins); the
 *    more severe case.
 *  - `ask`  — the allow is never reached because a preceding tool-wide ask always
 *    prompts first (it still "fires", just never silently allows).
 * Mirrors Claude's `ShadowType` (utils/permissions/shadowedRuleDetection.ts).
 */
export type ShadowType = 'deny' | 'ask';

/**
 * A permission rule that can never fire because a higher-priority rule shadows it,
 * with a human-readable {@link reason} naming the shadowing rule + its source and a
 * concrete {@link fix} telling the user which rule to remove to make it reachable.
 * Returned by {@link PermissionManager.detectShadowedRules}. Mirrors Claude's
 * `UnreachableRule`.
 */
export interface UnreachableRule {
  /** The shadowed (unreachable) allow rule. */
  rule: PermissionRule;
  /** The higher-priority rule that shadows it. */
  shadowedBy: PermissionRule;
  /** Whether the shadow is a hard block (`deny`) or an always-prompt (`ask`). */
  shadowType: ShadowType;
  /** Human sentence: `Blocked by "bash" deny rule (from project settings)`, etc. */
  reason: string;
  /** Actionable remediation naming the exact rules + sources to change. */
  fix: string;
}

/**
 * Options for {@link PermissionManager.detectShadowedRules}. When
 * `sandboxAutoAllowEnabled` is true, a tool-wide Bash *ask* rule from PERSONAL
 * settings is NOT reported as shadowing a specific Bash allow — sandboxed commands
 * are auto-allowed so the ask never actually gates them for this user (shared-source
 * ask rules still warn, since teammates may lack the sandbox). Mirrors Claude's
 * `DetectUnreachableRulesOptions`. Omitted → derived from the manager's own sandbox
 * policy.
 */
export interface DetectUnreachableRulesOptions {
  sandboxAutoAllowEnabled?: boolean;
}

/**
 * Resolve the on-disk file a permission destination maps to, or null for the
 * in-memory tiers (`session`, `cliArg`). `userSettings` is global (~/.ami);
 * `projectSettings`/`localSettings` are per-project under `cwd/.ami` (local is
 * the gitignored `.local.json` variant). Mirrors Claude's per-destination
 * settings-file resolution.
 */
export function permissionDestinationPath(
  cwd: string,
  destination: PermissionUpdateDestination,
): string | null {
  switch (destination) {
    case 'userSettings':
      return path.join(os.homedir(), '.ami', 'permissions.json');
    case 'projectSettings':
      return path.join(cwd, '.ami', 'permissions.json');
    case 'localSettings':
      return path.join(cwd, '.ami', 'permissions.local.json');
    case 'session':
    case 'cliArg':
      return null; // in-memory only — never persisted
  }
}

/**
 * The managed/enterprise permission-rules file (`policySettings` source). It lives
 * beside the managed config (`SUPERINFERENCE_MANAGED_CONFIG` env override, else
 * `/etc/superinference/config.json`) as `permissions.json`, so one admin-owned
 * location holds both managed config and managed permission policy. Admin-owned and
 * read-only to the agent: these rules can be loaded/evaluated but never persisted to.
 * Mirrors Claude's `policySettings` managed-settings tier.
 */
export function managedPermissionsPath(): string {
  return path.join(path.dirname(getManagedConfigPath()), 'permissions.json');
}

export const BASH_CLASSIFICATIONS = ['safe', 'unsafe', 'destructive'] as const;
export type BashClassification = (typeof BASH_CLASSIFICATIONS)[number];

/** Scope for an `allow_session` grant on the agent's own config folder. */
export const SESSION_GRANT_SCOPES = ['config-folder', 'global-config-folder'] as const;
export type SessionGrantScope = (typeof SESSION_GRANT_SCOPES)[number];

export interface PermissionPromptResult {
  action: 'allow_once' | 'allow_pattern' | 'allow_session' | 'deny' | 'cancelled';
  pattern?: string;
  /** Session-grant scope (for `allow_session` self-config edits). */
  scope?: SessionGrantScope;
  /**
   * Free-text the user attached when DENYING ("tell the model what to do
   * differently") — routed back to the model so it can steer the next turn.
   */
  feedback?: string;
  /**
   * Free-text the user attached when APPROVING ("tell the model what to do
   * next") — extra guidance delivered alongside the allowed result.
   */
  acceptFeedback?: string;
  /**
   * Tool input the user EDITED at the prompt (or an IDE "apply changes" flow
   * rewrote) before approving — applied in place of the model's original input
   * when the decision is an allow. Honored only on `allow_once` / `allow_pattern`;
   * ignored on deny/cancelled. The executor validates it against the tool schema
   * (dropping undeclared keys) before substituting it, so the UI cannot smuggle
   * unknown parameters. Mirrors Claude's `onAllow(updatedInput)` /
   * `ideDiffSupport.applyChanges` (the permission dialog returns the user's
   * modified edits instead of a plain allow).
   */
  updatedInput?: Record<string, unknown>;
}

export interface PermissionPromptHandler {
  showPrompt(
    toolName: string,
    inputSummary: string,
    classification?: BashClassification,
    /**
     * Optional request abort signal. When it fires while the prompt is pending
     * the handler should resolve `{ action: 'cancelled' }` so a cancelled turn
     * doesn't leave a permission dialog hanging.
     */
    abortSignal?: AbortSignal,
  ): Promise<PermissionPromptResult>;
}

/**
 * Structured reason a permission decision needs confirmation/was denied. The
 * `type` discriminator lets the UI explain WHY approval is required instead of a
 * bare "needs approval". Mirrors Claude's permissionResult.decisionReason union.
 */
export type PermissionDecisionReason =
  | { type: 'hardline'; description: string }
  | {
      type: 'rule';
      action: 'allow' | 'deny' | 'ask';
      ruleValue?: string;
      /** Free-text attribution shown inline ("from <source>") — the rule's own
       *  reason/label, NOT the tier it came from. */
      source?: string;
      /**
       * The originating settings TIER of the matched rule ({@link PermissionRuleSource}).
       * Distinct from the free-text {@link source}: this is the machine-readable
       * provenance used to decide whether a remediation hint applies — a rule from the
       * enterprise-managed `policySettings` tier is admin-owned and cannot be changed by
       * the user, so the "update your rules" hint is suppressed for it (parity with
       * Claude's `decisionReason.rule.source === 'policySettings'` branch in
       * PermissionRuleExplanation.tsx).
       */
      ruleSource?: PermissionRuleSource;
    }
  | { type: 'mode'; mode: PermissionMode }
  | { type: 'safetyCheck'; check: SafetyCheckKind; message: string }
  | { type: 'classifier'; name?: string; reason?: string }
  /**
   * The action targets a path OUTSIDE the agent's workspace (launch cwd + granted
   * read roots), so it needs explicit consent. Mirrors Claude's `workingDir`
   * decisionReason. `path` is the offending target; `reason` is the human sentence.
   */
  | { type: 'workingDir'; path: string; reason: string }
  /**
   * A hook (PreToolUse / PermissionRequest) asked for or denied confirmation.
   * Mirrors Claude's `hook` decisionReason (hookName + optional reason + source tag).
   * `behavior` is the decision the hook actually returned (allow/deny/ask) when it
   * made one, so the message can read "<hook> allowed/denied/asked for confirmation
   * for this action" (parity with Claude's toolHooks `getRuleBehaviorDescription`
   * phrasing) instead of the generic "requires approval".
   */
  | { type: 'hook'; hookName: string; reason?: string; hookSource?: string; behavior?: 'allow' | 'deny' | 'ask' }
  /**
   * The call opted out of the sandbox (`dangerouslyDisableSandbox`), so it must be
   * explicitly authorized to run unsandboxed. Mirrors Claude's `sandboxOverride`
   * decisionReason ("Requires permission to bypass sandbox").
   */
  | { type: 'sandboxOverride' }
  | { type: 'other'; message?: string };

export const SAFETY_CHECK_KINDS = [
  'credential', 'systemDir', 'suspiciousPath', 'agentConfig', 'buildConfig', 'destructive', 'codeExec', 'toctou',
] as const;
export type SafetyCheckKind = (typeof SAFETY_CHECK_KINDS)[number];

/**
 * Map a permission BEHAVIOR enum (`allow`/`deny`/`ask`) to its grammatical past-tense
 * prose, so a decision can be narrated in a sentence — "<hook> allowed this action",
 * "<rule> denied this action", "<hook> asked for confirmation for this action".
 * Mirrors Claude's `getRuleBehaviorDescription` (utils/permissions/PermissionResult.ts),
 * where `ask` (the default) reads "asked for confirmation for". A single canonical
 * mapping so callers never hand-roll the allow→"allowed" / deny→"denied" conversion.
 */
export function getRuleBehaviorDescription(behavior: 'allow' | 'deny' | 'ask'): string {
  switch (behavior) {
    case 'allow':
      return 'allowed';
    case 'deny':
      return 'denied';
    default:
      return 'asked for confirmation for';
  }
}

/** Build a human-readable explanation from a structured decision reason. */
export function createPermissionRequestMessage(reason: PermissionDecisionReason): string {
  switch (reason.type) {
    case 'hardline':
      return `Blocked: ${reason.description}`;
    case 'rule': {
      const from = reason.source ? ` from ${reason.source}` : '';
      const which = reason.ruleValue ? ` "${reason.ruleValue}"` : '';
      return `Permission rule${which}${from} requires ${reason.action === 'deny' ? 'denial' : 'approval'} for this command`;
    }
    case 'mode':
      return `Current permission mode (${reason.mode}) requires approval for this action`;
    case 'safetyCheck':
      return reason.message;
    case 'classifier':
      return `The safety classifier${reason.name ? ` (${reason.name})` : ''} flagged this action${reason.reason ? `: ${reason.reason}` : ''}`;
    case 'workingDir':
      return reason.reason;
    case 'hook': {
      const verb = reason.behavior ? getRuleBehaviorDescription(reason.behavior) : 'requires approval for';
      return `Hook "${reason.hookName}" ${verb} this action${reason.reason ? `: ${reason.reason}` : ''}`;
    }
    case 'sandboxOverride':
      return 'This action requires permission to bypass the sandbox';
    case 'other':
      return reason.message ?? 'This action requires approval';
  }
}

// ---------------------------------------------------------------------------
// Deterministic permission risk explainer (RiskLevel / PermissionExplanation)
// ---------------------------------------------------------------------------

/** Risk tier of a pending tool call, ordered LOW < MEDIUM < HIGH. */
export const RISK_LEVELS = ['LOW', 'MEDIUM', 'HIGH'] as const;
export type RiskLevel = (typeof RISK_LEVELS)[number];

/** Numeric severity of a {@link RiskLevel} (LOW=1, MEDIUM=2, HIGH=3), for ordering
 *  and analytics bucketing. Parity: Claude's `RISK_LEVEL_NUMERIC` map. */
export function riskLevelRank(level: RiskLevel): number {
  return RISK_LEVELS.indexOf(level) + 1;
}

/**
 * A plain-language risk assessment of a pending tool call, surfaced on the
 * permission prompt so the user can make an informed allow/deny decision and
 * attached to denial records for later review. Parity: Claude's
 * `PermissionExplanation` (utils/permissions/permissionExplainer.ts). Claude
 * generates this with an LLM side-query; AMI computes it DETERMINISTICALLY from the
 * same command-classification primitives the permission engine already uses
 * ({@link PermissionManager.classifyBashCommand}, {@link detectHardlineCommand}),
 * so it needs no extra model round-trip and is stable and unit-testable.
 */
export interface PermissionExplanation {
  /** Overall risk: LOW (safe dev workflow), MEDIUM (recoverable change), HIGH (dangerous/irreversible). */
  riskLevel: RiskLevel;
  /** What the tool call does (1-2 sentences). */
  explanation: string;
  /** Why it is being run, model-voiced (starts with "I"). */
  reasoning: string;
  /** What could go wrong, concisely. */
  risk: string;
}

/** Tool names (canonical) that only READ state and are therefore always low risk. */
const READ_ONLY_RISK_TOOLS = new Set([
  'file_read', 'read', 'grep', 'glob', 'ls', 'list', 'web_search', 'notebook_read',
]);
/** Tool names (canonical) that mutate the local workspace in a recoverable way. */
const MUTATING_RISK_TOOLS = new Set([
  'file_edit', 'file_write', 'notebook_edit', 'write', 'edit',
]);

/** Map a {@link BashClassification} to its {@link RiskLevel}. */
const BASH_CLASSIFICATION_RISK: Record<BashClassification, RiskLevel> = {
  safe: 'LOW',
  unsafe: 'MEDIUM',
  destructive: 'HIGH',
};

/** Lazily-created manager reused only for its static bash classifier, so
 *  {@link classifyPermissionRisk} reuses the canonical classification (no duplicate
 *  destructive/safe tables) without allocating per call. */
let _riskClassifierManager: PermissionManager | undefined;
function classifyBashForRisk(command: string): BashClassification {
  _riskClassifierManager ??= new PermissionManager();
  return _riskClassifierManager.classifyBashCommand(command);
}

function extractBashCommandText(input: unknown): string {
  if (typeof input === 'string') return input;
  if (input && typeof input === 'object' && 'command' in input) {
    const c = (input as { command?: unknown }).command;
    if (typeof c === 'string') return c;
  }
  return '';
}

/**
 * Deterministically assess the risk of a pending tool call and produce a
 * {@link PermissionExplanation}. Pure and side-effect free: it reuses the
 * permission engine's own classifiers (bash → {@link PermissionManager.classifyBashCommand},
 * which folds in hardline/destructive/credential detection) and a small tool-name
 * risk map for non-bash tools. Unknown tools default to MEDIUM (recoverable) —
 * never silently LOW. This is the data the CLI's permission-prompt explainer and
 * the denial log render; AMI computes it locally instead of via an LLM side-query.
 */
export function classifyPermissionRisk(toolName: string, input: unknown): PermissionExplanation {
  const tool = normalizeToolName(toolName);

  if (tool === 'bash') {
    const command = extractBashCommandText(input).trim();
    if (!command) {
      return {
        riskLevel: 'LOW',
        explanation: 'Runs an empty shell command.',
        reasoning: 'I am about to run a shell command.',
        risk: 'No effect.',
      };
    }
    const riskLevel = BASH_CLASSIFICATION_RISK[classifyBashForRisk(command)];
    const explanation = `Runs the shell command \`${command}\`.`;
    switch (riskLevel) {
      case 'HIGH':
        return {
          riskLevel,
          explanation,
          reasoning: 'I need to run this command to make the requested change.',
          risk: 'This command is destructive or irreversible and may cause data loss.',
        };
      case 'MEDIUM':
        return {
          riskLevel,
          explanation,
          reasoning: 'I need to run this command to make progress on the task.',
          risk: 'This command changes local state; effects are usually recoverable.',
        };
      default:
        return {
          riskLevel,
          explanation,
          reasoning: 'I need to inspect the project to continue.',
          risk: 'Read-only; no state is modified.',
        };
    }
  }

  if (READ_ONLY_RISK_TOOLS.has(tool)) {
    return {
      riskLevel: 'LOW',
      explanation: `Uses the ${tool} tool, which only reads or searches.`,
      reasoning: 'I need to inspect the project to continue.',
      risk: 'Read-only; no state is modified.',
    };
  }

  if (MUTATING_RISK_TOOLS.has(tool)) {
    return {
      riskLevel: 'MEDIUM',
      explanation: `Uses the ${tool} tool to modify a file in the workspace.`,
      reasoning: 'I need to apply this change to complete the task.',
      risk: 'Overwrites file content; effects are usually recoverable from version control.',
    };
  }

  if (tool === 'web_fetch') {
    return {
      riskLevel: 'MEDIUM',
      explanation: 'Fetches content from a remote URL.',
      reasoning: 'I need information from this URL to continue.',
      risk: 'Sends a network request to an external host.',
    };
  }

  return {
    riskLevel: 'MEDIUM',
    explanation: `Uses the ${tool} tool.`,
    reasoning: 'I need to use this tool to continue the task.',
    risk: 'Effects depend on the tool; review before approving.',
  };
}

export interface DenyCheckResult {
  denied: boolean;
  reason?: string;
}

/** A recency-ordered record of a denied action, for user review (`/permissions`). */
export interface RecentDenial {
  /** Human-readable command / summary of the denied action. */
  display: string;
  /** What produced the denial. */
  source: 'rule' | 'hardline' | 'classifier' | 'mode';
  /** Optional explanation. */
  reason?: string;
  /** Deterministic risk tier of the denied action, so a reviewer can triage the
   *  recent-denials list by severity. Computed from {@link classifyPermissionRisk}
   *  when `recordDenial` is given a display string. */
  riskLevel?: RiskLevel;
  timestamp: number;
}

// ---------------------------------------------------------------------------
// Permission verdict hold-over
// ---------------------------------------------------------------------------

export interface PermissionVerdict {
  decision: 'allow' | 'deny' | 'ask';
  rule?: string;
  source?: string;
  reason?: string;
}

export function verdictRank(decision: string): number {
  switch (decision) {
    case 'deny': return 3;
    case 'modify': return 2;
    case 'ask': return 2;
    case 'allow': return 1;
    default: return 0;
  }
}

export function holdsOver(held: PermissionVerdict, answer: PermissionVerdict): boolean {
  const heldRank = verdictRank(held.decision);
  const answerRank = verdictRank(answer.decision);
  if (answerRank > heldRank) return false;
  if (heldRank > answerRank) return true;
  const heldHasAttribution = held.rule !== undefined || held.source !== undefined;
  const answerHasAttribution = answer.rule !== undefined || answer.source !== undefined;
  return heldHasAttribution && !answerHasAttribution;
}

export function mostRestrictiveVerdict(verdicts: PermissionVerdict[]): PermissionVerdict {
  if (verdicts.length === 0) return { decision: 'allow' };
  let best = verdicts[0];
  for (let i = 1; i < verdicts.length; i++) {
    if (holdsOver(verdicts[i], best)) {
      best = verdicts[i];
    }
  }
  return best;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/**
 * Commands that are inherently read-only or side-effect-free.
 * These are auto-allowed in 'ask' mode.
 *
 * NOTE: `env` and `printenv` are intentionally excluded — they expose
 * sensitive environment variables (API keys, tokens, etc.).
 */
const SAFE_COMMANDS = new Set([
  'ls', 'cat', 'head', 'tail', 'wc', 'grep', 'rg', 'echo', 'pwd',
  'which', 'whoami', 'date', 'uname', 'hostname',
  'cd', // directory change — no side effects on the filesystem
  'id', 'df', 'du', 'file', 'stat', 'readlink', 'basename', 'dirname',
  'sort', 'uniq', 'diff', 'tr', 'cut', 'less', 'more',
  'test', 'true', 'false', 'type', 'man', 'help',
  // JSON/YAML processors (flag-validated)
  'jq', 'yq',
  // Command builder (flag-validated, target-restricted)
  'xargs',
  // Network/process inspection (flag-validated)
  'netstat', 'lsof', 'pgrep', 'ss', 'ps',
  // Terminal capabilities (flag-validated)
  'tput',
  // File finders (flag-validated)
  'fd', 'fdfind',
  // Directory tree (flag-validated)
  'tree',
  // Computational / text read-only utilities (side-effect-free)
  'seq', 'expr', 'printf', 'comm', 'cmp', 'numfmt', 'getconf', 'tsort', 'pr', 'fmt',
  // Encoding / checksum read-only utilities (flag-validated). base64 only decodes/
  // encodes to stdout; the *sum commands only read files to compute/verify a hash.
  // Mirrors Claude's base64 / sha256sum / sha1sum / md5sum read-only configs.
  'base64', 'sha256sum', 'sha1sum', 'md5sum',
  // Read-only type checker (flag-validated, --watch blocked). Mirrors Claude's
  // PYRIGHT_READ_ONLY_COMMANDS. `dotnet` is handled specially below (only its
  // read-only top-level flags like --version/--info are auto-allowed).
  'pyright', 'dotnet',
]);

/**
 * Per-flag argument-type classification (mirrors Claude Code's `FlagArgType`,
 * readOnlyCommandValidation.ts). It tells the flag validator exactly how many
 * tokens a flag consumes and how to validate the value, closing the parser
 * differential where a flag's VALUE (or a bundled arg-taking flag's next token)
 * is mis-read as a positional/target.
 */
export type FlagArgType =
  | 'none'   // takes no argument (e.g. --hidden, -H)
  | 'number' // integer argument (e.g. --max-depth 3)
  | 'string' // any string argument (e.g. --type file)
  | 'char'   // a single character (e.g. a delimiter)
  | '{}'     // the literal "{}" only
  | 'EOF';   // the literal "EOF" only

/** Flag-shaped token detector, matching Claude's FLAG_PATTERN. */
const FLAG_PATTERN = /^-[a-zA-Z0-9_-]/;

/**
 * Validate a flag's argument value against its declared {@link FlagArgType}.
 * Mirrors Claude Code's `validateFlagArgument`. 'none' always returns false —
 * it must never be called for a flag that takes no argument.
 */
export function validateFlagArgument(value: string, argType: FlagArgType): boolean {
  switch (argType) {
    case 'none': return false;
    case 'number': return /^\d+$/.test(value);
    case 'string': return true;
    case 'char': return value.length === 1;
    case '{}': return value === '{}';
    case 'EOF': return value === 'EOF';
    default: return false;
  }
}

/**
 * Arg-type map for `fd`/`fdfind` (shared). Depth flags are numeric, type/
 * extension/exclude/glob/search-path flags are strings, the rest are no-arg.
 * Modeled on Claude's FD_SAFE_FLAGS so the validator consumes the right number
 * of tokens per flag and validates each value.
 */
const FD_ARG_TYPES: Record<string, FlagArgType> = {
  '-H': 'none', '--hidden': 'none', '-I': 'none', '--no-ignore': 'none',
  '-d': 'number', '--max-depth': 'number',
  '-t': 'string', '--type': 'string',
  '-e': 'string', '--extension': 'string',
  '-E': 'string', '--exclude': 'string',
  '-g': 'string', '-p': 'string',
};

/**
 * Arg-type map for `base64`. Decode/mode flags take no argument; the line-length
 * flags are numeric; `-i`/`--input` names a file to READ (never a write). Mirrors
 * Claude's base64 safeFlags (readOnlyValidation.ts). macOS base64 ignores POSIX
 * `--`, hence respectsDoubleDash:false on the entry below.
 */
const BASE64_ARG_TYPES: Record<string, FlagArgType> = {
  '-d': 'none', '-D': 'none', '--decode': 'none',
  '-b': 'number', '--break': 'number', '-w': 'number', '--wrap': 'number',
  '-i': 'string', '--input': 'string',
  '--ignore-garbage': 'none',
  '-h': 'none', '--help': 'none', '--version': 'none',
};

/**
 * Arg-type map shared by the checksum commands (`sha256sum`/`sha1sum`/`md5sum`).
 * Every flag only selects an input mode, a verification mode, or an output
 * format — none write anything. Mirrors Claude's checksum safeFlags.
 */
const CHECKSUM_ARG_TYPES: Record<string, FlagArgType> = {
  '-b': 'none', '--binary': 'none', '-t': 'none', '--text': 'none',
  '-c': 'none', '--check': 'none', '--ignore-missing': 'none',
  '--quiet': 'none', '--status': 'none', '--strict': 'none',
  '-w': 'none', '--warn': 'none',
  '--tag': 'none', '-z': 'none', '--zero': 'none',
  '--help': 'none', '--version': 'none',
};

/**
 * Arg-type map for `grep`. Pattern/file flags take a string value, the rest are
 * no-arg mode selectors. Strict: any flag outside this set (notably the
 * preprocessor/exec-adjacent flags blocked below) falls through to a prompt.
 * Mirrors Claude's grep safeFlags.
 */
const GREP_ARG_TYPES: Record<string, FlagArgType> = {
  '-e': 'string', '--regexp': 'string', '-f': 'string', '--file': 'string',
  '-F': 'none', '--fixed-strings': 'none', '-G': 'none', '--basic-regexp': 'none',
  '-E': 'none', '--extended-regexp': 'none', '-P': 'none', '--perl-regexp': 'none',
  '-i': 'none', '--ignore-case': 'none', '-v': 'none', '--invert-match': 'none',
  '-n': 'none', '--line-number': 'none', '-c': 'none', '--count': 'none',
  '-l': 'none', '--files-with-matches': 'none', '-L': 'none', '--files-without-match': 'none',
  '-o': 'none', '--only-matching': 'none', '-w': 'none', '--word-regexp': 'none',
  '-x': 'none', '--line-regexp': 'none', '-r': 'none', '-R': 'none', '--recursive': 'none',
  '-h': 'none', '--no-filename': 'none', '-H': 'none', '--with-filename': 'none',
  '-A': 'number', '--after-context': 'number', '-B': 'number', '--before-context': 'number',
  '-C': 'number', '--context': 'number', '-m': 'number', '--max-count': 'number',
  '-s': 'none', '--no-messages': 'none', '-a': 'none', '--text': 'none',
  '-q': 'none', '--quiet': 'none', '--color': 'string', '--colour': 'string',
  '--include': 'string', '--exclude': 'string', '--exclude-dir': 'string',
  '-V': 'none', '--version': 'none',
};

/**
 * Arg-type map for `pyright`. Project/version/level flags take a string value;
 * stats/verbosity/version flags are no-arg. Strict: unknown flags prompt. The
 * long-running `--watch`/`-w` is blocked via dangerousArgs below. pyright treats
 * `--` as a FILE path (not end-of-options), so respectsDoubleDash:false keeps the
 * validator scanning past it. Mirrors Claude's PYRIGHT_READ_ONLY_COMMANDS.
 */
const PYRIGHT_ARG_TYPES: Record<string, FlagArgType> = {
  '--outputjson': 'none', '--project': 'string', '-p': 'string',
  '--pythonversion': 'string', '--pythonplatform': 'string',
  '--typeshedpath': 'string', '--venvpath': 'string', '--level': 'string',
  '--stats': 'none', '--verbose': 'none', '--version': 'none',
  '--dependencies': 'none', '--warnings': 'none',
};

/**
 * `dotnet` top-level flags that are purely read-only SDK queries. Any other
 * invocation (build/run/test/…) must prompt. Mirrors Claude's
 * DOTNET_READ_ONLY_FLAGS / isDotnetSafe.
 */
const DOTNET_READ_ONLY_FLAGS = new Set([
  '--version', '--info', '--list-runtimes', '--list-sdks',
]);

/**
 * Whether a `dotnet` invocation is a read-only SDK query: it has at least one
 * argument AND every argument is a recognised read-only top-level flag. A bare
 * `dotnet` (prints help but is treated as non-read by Claude) and any subcommand
 * (build/run/…) return false. Mirrors Claude's isDotnetSafe.
 */
function isDotnetSafe(args: string[]): boolean {
  if (args.length === 0) return false;
  return args.every(a => DOTNET_READ_ONLY_FLAGS.has(a.toLowerCase()));
}

const SAFE_COMMAND_FLAGS: Record<string, { allowed: Set<string>; blocked: Set<string>; strict?: boolean; argTypes?: Record<string, FlagArgType>; dangerousArgs?: (args: string[]) => boolean; respectsDoubleDash?: boolean }> = {
  jq: {
    allowed: new Set(['-r', '-e', '-c', '-S', '-s', '--raw-output', '--exit-status', '--compact-output', '--sort-keys', '--arg', '--argjson', '--null-input', '-n']),
    blocked: new Set(['-f', '--from-file', '--rawfile', '--slurpfile', '-L', '--library-path']),
  },
  yq: {
    allowed: new Set(['-r', '-e', '-c', '-S', '-s', '--raw-output', '--exit-status', '--compact-output', '--sort-keys', '--arg', '--argjson', '--null-input', '-n']),
    blocked: new Set(['-f', '--from-file', '--rawfile', '--slurpfile', '-L', '--library-path']),
  },
  // `man`: a pager/renderer launcher. `-P <cmd>`/`--pager=<cmd>` runs an arbitrary
  // program as the pager and `-H[browser]`/`--html[=browser]` launches a browser —
  // both are arbitrary-command-execution vectors, so they can never be auto-safe.
  man: { allowed: new Set([]), blocked: new Set(['-P', '--pager', '-H', '--html']) },
  // `help` is frequently aliased to `man` (e.g. oh-my-zsh's common-aliases
  // plugin), whose `-P`/`-l`/`-H` flags run an arbitrary pager/browser or read a
  // local file. Restrict `help` to the bash-builtin help flags only (strict), so
  // an aliased `help -P <cmd>` cannot be auto-approved. Mirrors Claude's `help`
  // safeFlags allowlist (readOnlyValidation.ts L326-332).
  help: { allowed: new Set(['-d', '-m', '-s']), blocked: new Set([]), strict: true },
  // `file`: identification is read-only, but `-C`/`--compile` WRITES a compiled
  // magic database to disk and `-m`/`--magic-file` loads an attacker-controlled
  // magic file; neither is auto-safe (Claude's `file` safeFlags restriction).
  file: { allowed: new Set([]), blocked: new Set(['-C', '--compile', '-m', '--magic-file']) },
  xargs: { allowed: new Set(['-n', '-P', '-I', '-0', '-d', '-t', '-p', '-r', '-L', '--max-procs', '--null']), blocked: new Set(['-i', '-e']) },
  // `ps`: UNIX `-e` (select all) is read-only and SAFE. The dangerous form is the
  // BSD-style environment modifier `e` — a NON-dash letter-only token containing
  // `e` (`ps axe`, `ps auxe`) that prints every process's environment (secrets).
  // The old `blocked:['e']` both over-blocked UNIX `-e` (flag scan maps `-e`→'e')
  // AND missed the dash-less BSD token (never entered the flag branch). A
  // dangerousArgs callback flags only the BSD letter-only `e` token, mirroring
  // Claude's ps additionalCommandIsDangerousCallback (readOnlyValidation.ts L420-429).
  ps: {
    allowed: new Set(['-e', '-f', '-a', '-u', '-x', '--sort', '-o', '--no-headers', '-p']),
    blocked: new Set([]),
    dangerousArgs: (args) => args.some(a => !a.startsWith('-') && /^[a-zA-Z]*e[a-zA-Z]*$/.test(a)),
  },
  tree: { allowed: new Set(['-L', '-d', '-f', '-a', '--dirsfirst', '-I', '--prune', '-P', '--charset', '-i']), blocked: new Set(['-R']) },
  // `-l`/`--list-details` is excluded: fd internally execs `ls` as a subprocess
  // (same pathway as --exec-batch — a PATH-hijack / arbitrary-exec risk), so it is
  // blocked alongside the exec flags. Mirrors Claude's fd -l exclusion.
  fd: { allowed: new Set(['-t', '-e', '-E', '--type', '--extension', '--exclude', '-H', '--hidden', '-I', '--no-ignore', '-g', '-p', '--max-depth', '-d']), blocked: new Set(['-x', '--exec', '-X', '--exec-batch', '-l', '--list-details']), argTypes: FD_ARG_TYPES },
  fdfind: { allowed: new Set(['-t', '-e', '-E', '--type', '--extension', '--exclude', '-H', '--hidden', '-I', '--no-ignore', '-g', '-p', '--max-depth', '-d']), blocked: new Set(['-x', '--exec', '-X', '--exec-batch', '-l', '--list-details']), argTypes: FD_ARG_TYPES },
  // `find` does NOT honor POSIX `--` for its expression primaries — a `--` only
  // ends the list of starting-point paths, so `find . -- -delete` STILL deletes.
  // Keep scanning past `--` (respectsDoubleDash:false) so a write/exec primary
  // cannot be smuggled behind it. Mirrors Claude's per-command respectsDoubleDash.
  find: { allowed: new Set(['-name', '-type', '-maxdepth', '-mindepth', '-path', '-iname', '-size', '-newer', '-print']), blocked: new Set(['-delete', '-exec', '-execdir', '-ok', '-okdir']), respectsDoubleDash: false },
  date: { allowed: new Set(['-u', '--utc', '-d', '--date', '-R', '--rfc-email', '-I', '--iso-8601']), blocked: new Set(['-s', '--set']) },
  netstat: { allowed: new Set(['-t', '-u', '-l', '-n', '-p', '-a', '--tcp', '--udp', '--listening', '--numeric']), blocked: new Set([]) },
  lsof: { allowed: new Set(['-i', '-n', '-P', '-p']), blocked: new Set([]) },
  pgrep: { allowed: new Set(['-l', '-a', '-f', '-x']), blocked: new Set([]) },
  // ripgrep: read-only search. Block preprocessor/exec-adjacent flags that can
  // run an arbitrary command or escape the search sandbox (S7).
  rg: { allowed: new Set(['-i', '-n', '-N', '-w', '-l', '-c', '-v', '-e', '-F', '-o', '-A', '-B', '-C', '-g', '-t', '-T', '-S', '-s', '-U', '-m', '-r', '--json', '--count', '--files', '--files-with-matches', '--no-heading', '--color', '--max-count', '--glob', '--type', '--hidden', '--no-ignore', '--context', '--ignore-case', '--line-number', '--fixed-strings', '--word-regexp']), blocked: new Set(['--pre', '--pre-glob', '--hostname-bin', '--search-zip', '-z']) },
  // which: display/lookup only. Any flag outside this set falls through to a
  // prompt instead of auto-safe (S8).
  which: { allowed: new Set(['-a', '--all', '-s', '--read-alias', '--read-functions', '--skip-alias', '--skip-dot', '--skip-tilde', '-i', '--tty-only', '--version', '-v', '-V']), blocked: new Set([]), strict: true },
  tput: { allowed: new Set(['cols', 'lines', 'colors', 'setaf', 'sgr0']), blocked: new Set([]) },
  ss: { allowed: new Set(['-t', '-u', '-l', '-n', '-p', '-a']), blocked: new Set([]) },
  // sort is read-only EXCEPT `-o/--output FILE`, which writes the sorted result to
  // a file. Block the output flags; other sort flags stay safe (S: strict off).
  sort: { allowed: new Set([]), blocked: new Set(['-o', '--output']) },
  // base64: encode/decode to stdout only. Typed flags (numeric wrap, string input
  // file) + respectsDoubleDash:false because macOS base64 ignores POSIX `--`.
  base64: { allowed: new Set(Object.keys(BASE64_ARG_TYPES)), blocked: new Set([]), strict: true, argTypes: BASE64_ARG_TYPES, respectsDoubleDash: false },
  // Checksum commands: read files + compute/verify a hash. No flag writes.
  sha256sum: { allowed: new Set(Object.keys(CHECKSUM_ARG_TYPES)), blocked: new Set([]), strict: true, argTypes: CHECKSUM_ARG_TYPES },
  sha1sum: { allowed: new Set(Object.keys(CHECKSUM_ARG_TYPES)), blocked: new Set([]), strict: true, argTypes: CHECKSUM_ARG_TYPES },
  md5sum: { allowed: new Set(Object.keys(CHECKSUM_ARG_TYPES)), blocked: new Set([]), strict: true, argTypes: CHECKSUM_ARG_TYPES },
  // grep: read-only search. Strict typed allowlist so any flag outside the known
  // read-only set (e.g. an unrecognised/exec-adjacent flag) falls through to a
  // prompt instead of being silently auto-approved. Mirrors Claude's grep safeFlags.
  grep: { allowed: new Set(Object.keys(GREP_ARG_TYPES)), blocked: new Set([]), strict: true, argTypes: GREP_ARG_TYPES },
  // pyright: read-only type checker. Strict typed allowlist; `--watch`/`-w` is a
  // long-running process (blocked via dangerousArgs). pyright treats `--` as a
  // file path, so keep scanning past it (respectsDoubleDash:false).
  pyright: { allowed: new Set(Object.keys(PYRIGHT_ARG_TYPES)), blocked: new Set([]), strict: true, argTypes: PYRIGHT_ARG_TYPES, respectsDoubleDash: false, dangerousArgs: (args) => args.some(a => a === '--watch' || a === '-w') },
};

/**
 * Sensitive config/credential path fragments that must still prompt even under
 * the blanket-allow modes (yolo/auto-allow and bypassPermissions). A write or
 * edit to any of these is a self-reconfig / credential-exposure vector.
 */
const SENSITIVE_SAFETY_PATHS = [
  '.git/', '.ami/', '.bashrc', '.zshrc', '.profile', '.gitconfig',
  '.vscode/', '.ssh/', '.gnupg/', '.aws/', '.config/',
];

/**
 * Case-fold a path for a security boundary comparison, but ONLY on case-insensitive
 * filesystems (macOS/Windows), where `/Users/x/.SSH/id_rsa` and `/Users/x/.ssh/id_rsa`
 * name the SAME file yet differ byte-for-byte. On a case-sensitive FS (Linux) the
 * casings are genuinely distinct files, so this is a no-op to avoid wrongly merging
 * them. Mirrors Claude's normalizeCaseForComparison (filesystem.ts L81-92), closing
 * the fail-open where a differently-cased sensitive path slips a containment/safety
 * guard on macOS/Windows.
 */
export function normalizeCaseForComparison(p: string): string {
  return (process.platform === 'darwin' || process.platform === 'win32')
    ? p.toLowerCase()
    : p;
}

/**
 * A path token that survives permission validation as a LITERAL string but is
 * expanded differently by the shell at execution time — a TOCTOU gap where the
 * boundary check and the actual filesystem target disagree:
 *   - `shell-expansion`: `$VAR` / `${VAR}` / `$(cmd)` (Unix), `%VAR%` (Windows),
 *     or a leading `=` (zsh equals-expansion, e.g. `=rg` → `/usr/bin/rg`).
 *   - `tilde-variant`:   an unsupported `~` form (`~user`, `~+`, `~-`, `~N`) that
 *     `expandPath` leaves for the shell (it resolves only bare `~`, `~/`, `~\`).
 */
export type PathExpansionHazard = 'shell-expansion' | 'tilde-variant';

/**
 * Classify a raw (un-expanded) path token for shell-expansion / tilde-variant
 * TOCTOU hazards. Mirrors Claude's pathValidation blanket-reject (pathValidation.ts
 * L393-L436): such a path is never silently auto-allowed — it is kept literal here
 * but the shell rewrites it at run time, so the permission decision and the real
 * target can diverge. Returns the hazard kind, or `null` when the path is inert.
 *
 * Tilde variants are detected on the RAW token because AMI's {@link expandPath}
 * resolves an unsupported `~user`/`~+` against cwd (→ `/cwd/~user`), so the `~`
 * no longer leads the expanded string.
 */
export function detectPathExpansionHazard(rawPath: string): PathExpansionHazard | null {
  const trimmed = rawPath.trim();
  if (trimmed.startsWith('~') && trimmed !== '~'
      && !trimmed.startsWith('~/') && !trimmed.startsWith('~\\')) {
    return 'tilde-variant';
  }
  if (trimmed.includes('$') || trimmed.includes('%') || trimmed.startsWith('=')) {
    return 'shell-expansion';
  }
  return null;
}

/**
 * Glob metacharacters (`*`, `?`, `[`, `]`) in a filesystem-path token. Parity:
 * Claude Code's GLOB_PATTERN_REGEX (pathValidation.ts L50). A path token carrying
 * one of these is a glob pattern whose runtime filesystem expansion cannot be
 * statically validated.
 */
export const GLOB_PATTERN_REGEX = /[*?[\]]/;

/** True if `p` contains a glob metacharacter (`* ? [ ]`). */
export function containsGlobPattern(p: string): boolean {
  return GLOB_PATTERN_REGEX.test(p);
}

/**
 * True if `p` contains a `..` path-traversal segment. Parity: Claude Code's
 * containsPathTraversal (utils/path.ts L133). A traversal glob such as
 * `/project/[star]/../../etc/shadow` is resolved-and-revalidated rather than
 * blindly gated, because collapsing the `..` segments pins down a concrete target
 * the forbidden/workspace guards can then inspect.
 */
export function containsPathTraversal(p: string): boolean {
  return /(?:^|[\\/])\.\.(?:[\\/]|$)/.test(p);
}

/**
 * Whether a read-path token is a glob that cannot be statically validated and must
 * therefore be forced to manual approval. Parity: Claude Code's pathValidation glob
 * handling (pathValidation.ts L1199-L1241, "finding #15"): a plain read glob's
 * pre-glob base is the only segment that gets realpath'd, so a symlink MATCHED
 * inside the expansion (e.g. `/project/[star]/passwd` with `/project/link → /etc`)
 * is never examined while the literal glob string still prefix-matches the
 * workspace. A traversal glob is excluded here (returns false) so it falls through
 * to normal resolve-and-revalidate, where its `..`-collapsed target is checked.
 */
export function isUnvalidatableReadGlob(rawPath: string): boolean {
  return containsGlobPattern(rawPath) && !containsPathTraversal(rawPath);
}

/**
 * Human-readable, user-facing reason for why an expansion-hazard path requires
 * manual approval, worded to match Claude's distinct messages so the user
 * understands why an otherwise-valid-looking path was gated.
 */
export function pathExpansionHazardReason(hazard: PathExpansionHazard): string {
  return hazard === 'tilde-variant'
    ? 'Tilde expansion variants (~user, ~+, ~-) in paths require manual approval'
    : 'Shell expansion syntax in paths requires manual approval';
}

/** Safe target commands for xargs — only these can follow xargs. */
const XARGS_SAFE_TARGETS = new Set([
  'echo', 'cat', 'ls', 'grep', 'wc', 'head', 'tail', 'basename', 'dirname',
]);

export function validateCommandFlags(cmd: string, args: string[]): boolean {
  const flagDef = SAFE_COMMAND_FLAGS[cmd];
  if (!flagDef) return true;
  // Custom per-command danger predicate (e.g. ps's BSD `e` environment modifier,
  // a dash-less token the flag scan below never inspects). Runs first so it
  // catches shapes the generic allow/block scan structurally cannot.
  if (flagDef.dangerousArgs && flagDef.dangerousArgs(args)) return false;
  // Commands with a per-flag arg-type map get getopt-accurate token consumption
  // so each flag's VALUE is validated and not mis-read as a positional/target.
  if (flagDef.argTypes) return validateTypedCommandFlags(flagDef, args);
  for (const arg of args) {
    // POSIX `--` end-of-options: every token after it is a positional, not a flag,
    // so stop scanning (unless the command ignores `--`, e.g. find — gap U003).
    if (arg === '--' && flagDef.respectsDoubleDash !== false) break;
    if (arg.startsWith('-')) {
      const flag = arg.replace(/^--?/, '').split('=')[0];
      const dashedFlag = arg.split('=')[0];
      if (flagDef.blocked.has(arg) || flagDef.blocked.has(flag) || flagDef.blocked.has(dashedFlag)) return false;
      // strict allowlist: any flag not explicitly allowed is treated as unsafe
      // (falls through to a prompt) rather than silently auto-safe.
      if (flagDef.strict && !flagDef.allowed.has(dashedFlag)) return false;
    }
  }
  return true;
}

/**
 * Getopt-accurate flag walk for a command whose flags carry {@link FlagArgType}
 * metadata. Mirrors Claude's `validateFlags`:
 *  - `--flag=value` validates the inline value (and rejects a value on a 'none' flag);
 *  - an arg-taking flag consumes the NEXT token as its value (rejecting a missing
 *    arg, a dash-led 'string' value — type confusion — and an invalid typed value);
 *  - a bundled short cluster (e.g. `-He`) is only safe when EVERY member is a
 *    known no-arg flag — a bundled arg-taking flag would consume the next token in
 *    GNU getopt, a differential the validator cannot model, so it is rejected.
 * Blocked flags always win. Unknown flags fall back to the allowed/strict rules.
 */
function validateTypedCommandFlags(
  flagDef: { allowed: Set<string>; blocked: Set<string>; strict?: boolean; argTypes?: Record<string, FlagArgType>; respectsDoubleDash?: boolean },
  args: string[],
): boolean {
  const argTypes = flagDef.argTypes!;
  let i = 0;
  while (i < args.length) {
    const token = args[i]!;
    // POSIX `--` end-of-options: stop treating later tokens as flags (unless the
    // command ignores `--`). Keeps the typed walker aligned with the real binary.
    if (token === '--' && flagDef.respectsDoubleDash !== false) break;
    if (!FLAG_PATTERN.test(token)) { i++; continue; } // positional
    const hasEquals = token.includes('=');
    const flag = token.split('=')[0]!;
    const inlineValue = token.slice(flag.length + 1);
    const bare = flag.replace(/^--?/, '');
    if (flagDef.blocked.has(token) || flagDef.blocked.has(flag) || flagDef.blocked.has(bare)) return false;

    const argType = argTypes[flag];
    if (argType === undefined) {
      // A short cluster like `-He` — safe only if every member is a known no-arg
      // flag. Any arg-taking or unknown member means we cannot safely model the
      // token consumption, so reject (conservative, matches Claude).
      if (/^-[a-zA-Z0-9]{2,}$/.test(flag)) {
        for (const ch of flag.slice(1)) {
          if (argTypes['-' + ch] !== 'none') return false;
        }
        i++; continue;
      }
      // Unknown single flag: honor the strict allowlist like the untyped path.
      if (flagDef.strict && !flagDef.allowed.has(flag)) return false;
      i++; continue;
    }

    if (argType === 'none') {
      if (hasEquals) return false; // a no-arg flag given `=value`
      i++; continue;
    }

    // Arg-taking flag: resolve its value (inline after `=` or the next token).
    let value: string;
    if (hasEquals) {
      value = inlineValue;
      i++;
    } else {
      const next = args[i + 1];
      if (next === undefined || (next.length > 1 && FLAG_PATTERN.test(next))) return false; // missing arg
      value = next;
      i += 2;
    }
    if (argType === 'string' && value.startsWith('-')) return false; // type confusion
    if (!validateFlagArgument(value, argType)) return false;
  }
  return true;
}

/**
 * Check whether a jq/yq expression contains dangerous constructs.
 * Blocks `-f`, `--rawfile`, `--slurpfile`, `-L` flags and `system()` calls.
 */
function isJqDangerous(args: string[]): boolean {
  const dangerousFlags = ['-f', '--rawfile', '--slurpfile', '-L', '--library-path', '--from-file'];
  for (const arg of args) {
    if (dangerousFlags.includes(arg)) return true;
  }
  // Check for system() call in jq expression
  const expr = args.find(a => !a.startsWith('-'));
  if (expr && /\bsystem\b/.test(expr)) return true;
  return false;
}

/** xargs short flags that CONSUME an argument (their value is either inline —
 *  `-n1` — or the next token — `-I {}`). Getopt-accurate modeling of these is
 *  what prevents a validator-vs-xargs parser differential: a naive scanner that
 *  skips `-I` but then reads `-I`'s VALUE as the target command would validate the
 *  wrong word and wave an unsafe real target (`xargs -I echo rm` → RCE) through. */
const XARGS_ARG_TAKING = new Set(['I', 'i', 'd', 'E', 'e', 'L', 'l', 'n', 'P', 's', 'a']);

/**
 * Check whether xargs is being used with a safe (read-only) target command.
 *
 * Getopt-accurate: walks xargs' own options, correctly consuming each arg-taking
 * flag's value (inline like `-n1`, bundled like `-rI {}`, or the next token like
 * `-I {}`) and honoring a `--` end-of-options marker, so the REAL target command
 * is identified — not a flag's value. Mirrors Claude Code's xargs read-only
 * validation / bundled arg-taking-flag handling (readOnlyCommandValidation.ts).
 */
function isXargsSafe(command: string): boolean {
  const m = command.match(/\bxargs\b(.*)$/s);
  if (!m) return false;
  const tokens = m[1]!.trim().split(/\s+/).filter(Boolean);
  let k = 0;
  while (k < tokens.length) {
    const t = tokens[k]!;
    if (t === '--') { k++; break; } // end of options — next token is the target
    if (t.startsWith('--')) { k++; continue; } // long opts (value is `=`-attached)
    if (t.startsWith('-')) {
      // Short-flag cluster: scan chars; an arg-taking flag consumes the rest of
      // the token as an inline value, or (if it is the last char) the NEXT token.
      const body = t.slice(1);
      let consumedNext = false;
      for (let c = 0; c < body.length; c++) {
        const ch = body[c]!;
        if (XARGS_ARG_TAKING.has(ch)) {
          if (c === body.length - 1) consumedNext = true; // value is the next token
          break; // rest of the cluster is this flag's inline value
        }
      }
      k += consumedNext ? 2 : 1;
      continue;
    }
    break; // first non-flag token is the target command
  }
  const target = tokens[k];
  if (!target) return false;
  return XARGS_SAFE_TARGETS.has(target);
}

/**
 * Check whether a python3 -e/-c one-liner is read-only.
 * Blocks imports of os, subprocess, shutil which can mutate state.
 */
function isScriptOneLinerSafe(lang: string, code: string): boolean {
  if (lang === 'python3' || lang === 'python') {
    return !/\bimport\s+(os|subprocess|shutil)\b/.test(code) &&
           !/\bfrom\s+(os|subprocess|shutil)\b/.test(code);
  }
  if (lang === 'node') {
    return !/\brequire\s*\(\s*['"](?:child_process|fs)['"]\s*\)/.test(code) &&
           !/\bimport\b.*\bfrom\s+['"](?:child_process|fs)['"]/.test(code);
  }
  if (lang === 'ruby') {
    return !/\bsystem\b/.test(code) && !/\bexec\b/.test(code) &&
           !/`/.test(code) && !/\bIO\.popen\b/.test(code);
  }
  if (lang === 'perl') {
    return !/\bsystem\b/.test(code) && !/\bexec\b/.test(code) &&
           !/`/.test(code) && !/\bqx\b/.test(code);
  }
  return false;
}

/**
 * Strip quoted sections from a command to analyze unquoted content.
 */
function stripQuotedSections(command: string): string {
  // Remove double-quoted sections
  let result = command.replace(/"[^"]*"/g, '');
  // Remove single-quoted sections
  result = result.replace(/'[^']*'/g, '');
  return result;
}

/**
 * Check whether a command contains unquoted variable expansions or glob
 * patterns that could be exploited for injection.
 *
 * Uses a single-pass quote-state walker (NOT a regex over quote-stripped text) so
 * single-quote escape semantics are correct — in bash `\` is a literal no-op inside
 * single quotes, so a stray `'\'` cannot desync the tracker and drop a later
 * expansion. It also covers EVERY `$`-expansion shape: a following variable-name
 * char, a `${...}`/`$(...)` substitution start, OR a special parameter
 * (`$@ $* $# $? $! $$ $- $0-$9`) that the old `\$[A-Za-z_{(]` regex missed — plus
 * the unquoted glob metacharacters `* ? [` (filename expansion / injection vectors).
 * Mirrors Claude's containsUnquotedExpansion char-walk (readOnlyValidation.ts).
 */
export function containsUnquotedExpansion(command: string): boolean {
  let inSingle = false;
  let inDouble = false;
  let escaped = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i]!;
    if (escaped) { escaped = false; continue; }
    if (c === '\\' && !inSingle) { escaped = true; continue; }
    if (c === "'" && !inDouble) { inSingle = !inSingle; continue; }
    if (c === '"' && !inSingle) { inDouble = !inDouble; continue; }
    if (inSingle || inDouble) continue; // quoted content is inert
    if (c === '$') {
      const next = command[i + 1];
      // variable-name char, `${`/`$(` substitution, or a special parameter.
      if (next && /[A-Za-z_@*#?!$0-9({-]/.test(next)) return true;
    }
    // Unquoted glob metacharacters (`*`, `?`, bracket expression `[`).
    if (c === '*' || c === '?' || c === '[') return true;
  }
  return false;
}

/**
 * Check whether a command contains unquoted shell substitutions ($VAR,
 * ${VAR}, $(cmd)) but tolerate glob patterns (* ? [...]).
 * Used for safe read-only commands where globs are harmless filename expansion.
 */
export function containsShellSubstitution(command: string): boolean {
  // A quote-tracking scan (not a regex over quote-stripped text) so single-quote
  // escape semantics are correct AND every `$`-expansion shape is covered. In
  // bash a `\` is a NO-OP inside single quotes (literal), so escapes are only
  // honored OUTSIDE single quotes — otherwise `'\'` desyncs the tracker and the
  // scanner stays "in a quote" for the rest of the command, missing later
  // expansions (Claude's containsUnquotedExpansion single-quote guard).
  let inSingle = false;
  let inDouble = false;
  let escaped = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i]!;
    if (escaped) { escaped = false; continue; }
    if (c === '\\' && !inSingle) { escaped = true; continue; }
    if (c === "'" && !inDouble) { inSingle = !inSingle; continue; }
    if (c === '"' && !inSingle) { inDouble = !inDouble; continue; }
    if (inSingle || inDouble) continue; // quoted — AMI tolerates `$VAR`/`"$f"` here
    // UNQUOTED `$`: match a following variable-name char, a special parameter
    // ($@ $* $# $? $! $$ $0-9 $-), or the start of a ${...} / $(...) substitution.
    // AMI deliberately tolerates `$` inside quotes (single AND double) so the
    // for-loop-with-glob perk (`for f in *.ts; do cat "$f"; done`) stays auto-safe
    // — an intentional superset divergence from Claude; this adds the previously
    // missed UNQUOTED special-parameter shapes ($@/$*/$?/…) to the parser-
    // differential defense without regressing that perk.
    if (c === '$') {
      const next = command[i + 1];
      if (next && /[A-Za-z_@*#?!$0-9({-]/.test(next)) return true;
    }
  }
  return false;
}

/** Safe read-only docker subcommands. */
const DOCKER_SAFE_SUBCOMMANDS = new Set([
  'ps', 'images', 'logs', 'inspect', 'stats', 'top', 'port', 'version', 'info',
]);

/** Compound safe prefixes: "command subcommand" pairs that are safe. */
const SAFE_PREFIXES = new Set([
  'git status', 'git log', 'git diff', 'git branch', 'git show',
  'git remote', 'git tag', 'git stash list', 'git rev-parse',
  // `git ls-remote` lists refs on a remote — read-only, EXCEPT the --server-option
  // exfil vector gated in isGitInvocationReadOnly. Mirrors Claude's ls-remote config.
  'git ls-remote',
  'npm list', 'npm ls', 'npm view',
  'tsc', 'tsc --noEmit',
  'cargo build', 'cargo check', 'cargo test', 'cargo clippy',
  'go build', 'go test', 'go vet', 'go fmt',
  'make build', 'make test', 'make check', 'make lint',
]);

/**
 * Commands that modify state and should prompt the user.
 */
const UNSAFE_COMMANDS = new Set([
  'git push', 'git commit', 'git merge', 'git rebase', 'git cherry-pick',
  'npm install', 'npm ci', 'npm update', 'npm uninstall', 'npm publish',
  'pip install', 'pip uninstall',
  'docker-compose',
  'curl', 'wget',
  'ssh', 'scp', 'rsync',
  'chmod', 'chown', 'chgrp',
  'sudo', 'su', 'doas',
  'apt', 'apt-get', 'dnf', 'yum', 'pacman', 'brew',
  'mv', 'cp', 'ln', 'mkdir', 'touch',
  'node', 'python', 'python3', 'ruby', 'perl',
]);

/**
 * Commands that are destructive and always require explicit permission
 * with an extra warning.
 */
const DESTRUCTIVE_COMMANDS = new Set([
  'rm', 'rmdir',
  'git reset --hard', 'git clean',
  'kill', 'pkill', 'killall',
  'format', 'mkfs',
  'dd',
  'truncate', 'shred',
  'redirect-to-system-path',
]);

/** SQL statements that are destructive. */
const DESTRUCTIVE_SQL_PATTERNS = [
  /\bDROP\s+TABLE\b/i,
  /\bDROP\s+DATABASE\b/i,
  // `DROP SCHEMA public CASCADE` destroys an entire schema and everything in it;
  // Claude's SQL pattern covers DROP of TABLE/DATABASE/SCHEMA alike
  // (destructiveCommandWarning.ts L71-74). Without this arm a schema drop is not
  // classified destructive by the SQL branch.
  /\bDROP\s+SCHEMA\b/i,
  /\bDELETE\s+FROM\b/i,
  /\bTRUNCATE\s+TABLE\b/i,
  /\bTRUNCATE\b/i,
];

/**
 * Env vars that are safe to pass through / strip before allow-rule matching. The
 * allowlist deliberately EXCLUDES PATH, LD_*, DYLD_*, PYTHONPATH, NODE_OPTIONS,
 * HOME, BASH_ENV and endpoint/daemon vars (DOCKER_HOST/KUBECONFIG) — those are an
 * escalation vector. Exported so the trust/safe-mode config scanner reuses the
 * SAME allowlist (one canonical source, no duplication).
 */
export const SAFE_ENV_VARS = new Set([
  'NODE_ENV', 'LANG', 'LC_ALL', 'LC_CTYPE', 'TERM', 'COLORTERM',
  'NO_COLOR', 'FORCE_COLOR', 'RUST_LOG', 'RUST_BACKTRACE',
  'GOOS', 'GOARCH', 'GOPATH', 'GOBIN', 'GOROOT',
  'PYTHONDONTWRITEBYTECODE', 'PYTHONUNBUFFERED', 'VIRTUAL_ENV',
  'EDITOR', 'VISUAL', 'PAGER', 'LESS', 'TZ',
  'CI', 'GITHUB_ACTIONS', 'GITLAB_CI', 'JENKINS_URL',
  'npm_config_loglevel', 'npm_config_registry',
  'CARGO_HOME', 'RUSTUP_HOME',
]);

export function stripSafeEnvVars(command: string): string {
  // eslint-disable-next-line security/detect-unsafe-regex
  return command.replace(/^(\s*(?:\w+=\S+\s+)*)/g, (match) => {
    const assignments = match.trim().split(/\s+/);
    const unsafe = assignments.filter(a => {
      const varName = a.split('=')[0];
      return !SAFE_ENV_VARS.has(varName);
    });
    return unsafe.length > 0 ? unsafe.join(' ') + ' ' : '';
  });
}

/**
 * Hardline patterns — blocked unconditionally, even in auto-allow mode.
 * These prevent catastrophic system damage regardless of settings.
 */
const HARDLINE_PATTERNS: Array<{ pattern: RegExp; description: string }> = [
  // Match `rm … /` targeting the filesystem root, including a trailing token/
  // redirect (`rm -rf / 2>/dev/null`) and the catastrophic glob (`rm -rf /*`).
  // The arg run is `[^;&|]*` so it cannot span a command separator and
  // false-positive on a later bare `/` (e.g. `rm x.txt && ls /`).
  { pattern: /\brm\s+(-[a-zA-Z]*r[a-zA-Z]*\s+|[^;&|]*\s+)\/(?=\s|$|[;&|><*])/,  description: 'rm -r on filesystem root' },
  { pattern: /\brm\s+(-[a-zA-Z]*r[a-zA-Z]*\s+|[^;&|]*\s+)\/(home|etc|usr|var|bin|sbin|boot|lib|lib64|Volumes|mnt|media)\b/, description: 'rm -r on system directory' },
  { pattern: /\brm\s+(-[a-zA-Z]*r[a-zA-Z]*\s+|[^;&|]*\s+)\/private\/(etc|var|tmp|home)\b/, description: 'rm -r on macOS system directory (/private/*)' },
  { pattern: /\bmkfs\b/, description: 'filesystem format' },
  { pattern: /\bdd\b.*\bof\s*=\s*\/dev\/[sh]d/, description: 'dd to block device' },
  { pattern: /:\(\)\s*\{\s*:\s*\|\s*:\s*&\s*\}\s*;\s*:/, description: 'fork bomb' },
  { pattern: /\b(shutdown|reboot|halt|poweroff)\b/, description: 'system power control' },
  { pattern: /\binit\s+[06]\b/, description: 'init level change' },
  { pattern: /\bsystemctl\s+(poweroff|reboot|halt)\b/, description: 'systemctl power control' },
  { pattern: />\s*\/dev\/[sh]d/, description: 'redirect to block device' },
  { pattern: /\bchmod\b.{0,20}777\s+\//, description: 'chmod 777 on system path' },
  { pattern: /\bcurl\b.*\|\s*(ba|z|da|fi|k|c|tc)?sh\b/, description: 'pipe curl to shell' },
  { pattern: /\bwget\b.*\|\s*(ba|z|da|fi|k|c|tc)?sh\b/, description: 'pipe wget to shell' },
  { pattern: /\bpkill\b.*\bnode\b/, description: 'pkill node would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\bkillall\b.*\bnode\b/, description: 'killall node would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\bpkill\b.*\btsx\b/, description: 'pkill tsx would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\bkillall\b.*\btsx\b/, description: 'killall tsx would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\bpkill\b.*\bsuperinference\b/i, description: 'would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\b(curl|wget|fetch|nc|ncat|http|https)\b[^\n]*(?:\b(?:169\.254\.169\.254|metadata\.google\.internal|metadata\.google\.com|100\.100\.100\.200|instance-data)\b|fd00:ec2::254)/i, description: 'cloud instance-metadata credential fetch (SSRF/credential-exfil vector)' },
  // PowerShell power-control / disk-format cmdlets — the Windows equivalents of
  // the POSIX shutdown/reboot/mkfs hardlines above (AMI runs PowerShell on Windows).
  { pattern: /\b(Stop-Computer|Restart-Computer)\b/i, description: 'PowerShell power control' },
  { pattern: /\bClear-RecycleBin\b/i, description: 'PowerShell empties the recycle bin (irreversible)' },
  { pattern: /\b(Format-Volume|Clear-Disk)\b/i, description: 'PowerShell disk format / wipe' },
];

/** Serializable snapshot of the built-in (default) permission patterns. */
export interface DefaultPermissionPatterns {
  /** Bash commands auto-classified SAFE (read-only) — never prompt. */
  autoAllowBashCommands: string[];
  /** Unconditionally BLOCKED command patterns (hardline deny — interrupts). */
  hardlineDenied: Array<{ pattern: string; description: string }>;
  /** Commands classified DESTRUCTIVE (irreversible) — require confirmation. */
  destructiveCommands: string[];
  /** SQL statement patterns classified DESTRUCTIVE. */
  destructiveSqlPatterns: string[];
  /** Bash base-commands `acceptEdits` mode auto-approves without a prompt. */
  acceptEditsAutoAllowedCommands: string[];
}

/**
 * Return the built-in (default) permission classifier patterns as a serializable
 * structure — the auto-allow safe-command list, the unconditional hardline-deny
 * patterns, and the destructive-command/SQL sets. Surfaced by `/permissions
 * defaults` (and merged with the live session rules by `/permissions effective`)
 * so a user can see the floor AMI classifies against before any of their own
 * rules apply. Parity: Claude Code's `auto-mode defaults` / `getDefaultExternalAutoModeRules`
 * dump. Pure + side-effect free; the source sets are the SAME canonical ones the
 * runtime classifier uses (no duplicate table).
 */
export function getDefaultPermissionPatterns(): DefaultPermissionPatterns {
  return {
    autoAllowBashCommands: [...SAFE_COMMANDS].sort(),
    hardlineDenied: HARDLINE_PATTERNS.map(h => ({ pattern: h.pattern.source, description: h.description })),
    destructiveCommands: [...DESTRUCTIVE_COMMANDS].sort(),
    destructiveSqlPatterns: DESTRUCTIVE_SQL_PATTERNS.map(p => p.source),
    acceptEditsAutoAllowedCommands: [...getAutoAllowedCommands('acceptEdits')],
  };
}

/**
 * Expand two documented evasions of the unconditional hardline guard so the
 * patterns still fire: (a) a catastrophic base built via a bare assignment and
 * referenced by variable (`c=rm; $c -rf /`), and (b) a quoted payload inside
 * `sh -c '...'` / `bash -c "..."` / `eval '...'` where the trailing quote
 * defeats the end-anchored patterns. Returns the original command augmented with
 * the normalized/unwrapped forms (newline-joined) so pattern matching sees both.
 */
const HARDLINE_BASES = 'rm|dd|mkfs|shutdown|reboot|halt|poweroff|chmod|curl|wget|pkill|killall';

function normalizeForHardline(command: string): string {
  let out = command;

  // (a) Substitute variable references with their assigned values. Covers both a
  // bare dangerous base (`name=rm`) and a full quoted command (`c='rm -rf /'`),
  // so `eval "$c"` is normalized to the real command.
  // Optional assignment-prefix keywords (export/declare/typeset/readonly/local)
  // must not let `export c=rm; $c -rf /` bypass the variable-indirection guard.
  const ASSIGN_PREFIX = `(?:(?:export|declare|typeset|readonly|local)\\s+)?`;
  const assigns = new Map<string, string>();
  const baseAssignRe = new RegExp(
    `(?:^|;|&&|\\|\\||\\n)\\s*${ASSIGN_PREFIX}([A-Za-z_][A-Za-z0-9_]*)=(?:\\/[^\\s"';|&]*\\/)?(${HARDLINE_BASES})(?=\\s|;|&|\\||$)`,
    'g',
  );
  let a: RegExpExecArray | null;
  while ((a = baseAssignRe.exec(command)) !== null) assigns.set(a[1], a[2]);
  const quotedAssignRe = new RegExp(
    `(?:^|;|&&|\\|\\||\\n)\\s*${ASSIGN_PREFIX}([A-Za-z_][A-Za-z0-9_]*)=(['"])([\\s\\S]*?)\\2`,
    'g',
  );
  let q: RegExpExecArray | null;
  while ((q = quotedAssignRe.exec(command)) !== null) assigns.set(q[1], q[3]);
  // Multiple space-separated assignments on one statement, e.g.
  // `export a=1 c=rm` — scan every name=BASE pair within each statement
  // (prefixed by export/declare/… or a bare assignment run). Split first to
  // keep the per-pair regex simple and ReDoS-safe.
  const pairRe = new RegExp(`\\b([A-Za-z_][A-Za-z0-9_]*)=(?:\\/[^\\s"';|&]*\\/)?(${HARDLINE_BASES})(?=\\s|$|[|&])`, 'g');
  for (const rawSeg of command.split(/[;\n]|&&|\|\|/)) {
    const seg = rawSeg.replace(/^\s*(?:export|declare|typeset|readonly|local)\s+/, '');
    let pr: RegExpExecArray | null;
    pairRe.lastIndex = 0;
    while ((pr = pairRe.exec(seg)) !== null) assigns.set(pr[1], pr[2]);
  }
  if (assigns.size) {
    out = out.replace(/\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?/g, (full, name) =>
      assigns.has(name) ? assigns.get(name)! : full);
  }

  // (b) Expose command-substitution-built bases: $(echo rm)/`which rm` -> rm.
  out = out.replace(/\$\(\s*(?:echo|which|command\s+-v)\s+([^)]*)\)/g, '$1');
  out = out.replace(/`\s*(?:echo|which|command\s+-v)\s+([^`]*)`/g, '$1');

  // (c) Recursively unwrap sh/bash/eval -c quoted payloads (handles nesting),
  // appending each inner script as its own line so end-anchored patterns match.
  for (let depth = 0; depth < 5; depth++) {
    const wrapRe = /\b(?:sh|bash|zsh|dash|ksh|eval)\b[^'"]*(['"])([\s\S]*?)\1/g;
    const inners: string[] = [];
    let w: RegExpExecArray | null;
    while ((w = wrapRe.exec(out)) !== null) {
      if (w[2] && !out.includes('\n' + w[2])) inners.push(w[2]);
    }
    if (!inners.length) break;
    out += '\n' + inners.join('\n');
  }
  return out;
}

export function detectHardlineCommand(command: string): { blocked: boolean; description: string | null } {
  // Collapse quote/expansion/backslash obfuscation (r""m, r$()m, r\m → rm) so a
  // split command word cannot evade the hardline patterns.
  const deobfuscated = deobfuscateEnvExpansion(command);
  const normalized = normalizeForHardline(deobfuscated);
  // Test the original, the deobfuscated form, the whole normalized string, AND
  // each normalized line, so an end-anchored pattern (e.g. `rm -rf /$`) still
  // matches a catastrophic statement that is not the last token of a
  // multi-statement command.
  const candidates = [command, deobfuscated, normalized, ...normalized.split('\n')];
  for (const { pattern, description } of HARDLINE_PATTERNS) {
    for (const candidate of candidates) {
      if (pattern.test(candidate)) return { blocked: true, description };
    }
  }
  return { blocked: false, description: null };
}

/**
 * A single destructive-command signature: a {@link RegExp} and the
 * human-readable {@link warning} shown for it. Mirrors Claude's
 * `DestructivePattern` (utils/.../destructiveCommandWarning.ts, bash variant).
 */
interface DestructivePattern {
  pattern: RegExp;
  warning: string;
}

/**
 * Ordered bash destructive-command signatures, MOST-SPECIFIC-FIRST so the first
 * match wins (a recursive+force `rm` is reported ahead of a plain recursive one).
 * Purely informational — these drive the human-readable note surfaced in a
 * permission prompt ({@link getDestructiveCommandWarning}); they do NOT change
 * any allow/deny decision (that is owned by {@link PermissionManager.check} /
 * {@link detectHardlineCommand}). Parity with Claude's bash DESTRUCTIVE_PATTERNS.
 */
const DESTRUCTIVE_WARNING_PATTERNS: DestructivePattern[] = [
  // Git — data loss / hard to reverse.
  { pattern: /\bgit\s+reset\s+--hard\b/, warning: 'Note: may discard uncommitted changes' },
  { pattern: /\bgit\s+push\b[^;&|\n]*[ \t](--force|--force-with-lease|-f)\b/, warning: 'Note: may overwrite remote history' },
  { pattern: /\bgit\s+clean\b(?![^;&|\n]*(?:-[a-zA-Z]*n|--dry-run))[^;&|\n]*-[a-zA-Z]*f/, warning: 'Note: may permanently delete untracked files' },
  { pattern: /\bgit\s+checkout\s+(?:--[ \t])?\.(?=$|[ \t;&|\n])/, warning: 'Note: may discard all working tree changes' },
  { pattern: /\bgit\s+restore\s+(?:--[ \t])?\.(?=$|[ \t;&|\n])/, warning: 'Note: may discard all working tree changes' },
  { pattern: /\bgit\s+stash[ \t]+(drop|clear)\b/, warning: 'Note: may permanently remove stashed changes' },
  { pattern: /\bgit\s+branch\s+(-D[ \t]|--delete\s+--force|--force\s+--delete)\b/, warning: 'Note: may force-delete a branch' },
  // Git — safety bypass.
  { pattern: /\bgit\s+(commit|push|merge)\b[^;&|\n]*--no-verify\b/, warning: 'Note: may skip safety hooks' },
  { pattern: /\bgit\s+commit\b[^;&|\n]*--amend\b/, warning: 'Note: may rewrite the last commit' },
  // File deletion (dangerous target paths are handled separately by the hardline
  // / dangerous-removal guards; this is the informational note).
  { pattern: /(^|[;&|\n]\s*)rm\s+-[a-zA-Z]*[rR][a-zA-Z]*f|(^|[;&|\n]\s*)rm\s+-[a-zA-Z]*f[a-zA-Z]*[rR]/, warning: 'Note: may recursively force-remove files' },
  { pattern: /(^|[;&|\n]\s*)rm\s+-[a-zA-Z]*[rR]/, warning: 'Note: may recursively remove files' },
  { pattern: /(^|[;&|\n]\s*)rm\s+-[a-zA-Z]*f/, warning: 'Note: may force-remove files' },
  { pattern: /(^|[;&|\n]\s*)rmdir\b/, warning: 'Note: may remove directories' },
  // Secure-deletion / truncation. `shred` is DETERMINISTIC (it always overwrites
  // then removes), so its note uses "will"; `truncate` is conditional ("may",
  // e.g. only shrinks when `-s` reduces the size) — mirrors Claude's will/may
  // certainty distinction in destructiveCommandWarning.ts.
  { pattern: /(^|[;&|\n]\s*)shred\b/, warning: 'Note: will permanently overwrite and delete the target files' },
  { pattern: /(^|[;&|\n]\s*)truncate\b/, warning: 'Note: may shrink or empty the target file' },
  // Process control.
  { pattern: /(^|[;&|\n]\s*)(pkill|killall)\b/, warning: 'Note: may terminate matching processes by name' },
  { pattern: /(^|[;&|\n]\s*)kill\s+-[a-zA-Z]*9\b/, warning: 'Note: may forcibly terminate a process' },
  { pattern: /(^|[;&|\n]\s*)kill\b/, warning: 'Note: may terminate a running process' },
  // Database.
  { pattern: /\b(DROP|TRUNCATE)\s+(TABLE|DATABASE|SCHEMA)\b/i, warning: 'Note: may drop or truncate database objects' },
  { pattern: /\bDELETE\s+FROM\s+\w+[ \t]*(;|"|'|\n|$)/i, warning: 'Note: may delete all rows from a database table' },
  // Infrastructure.
  { pattern: /\bkubectl\s+delete\b/, warning: 'Note: may delete Kubernetes resources' },
  { pattern: /\bterraform\s+destroy\b/, warning: 'Note: may destroy Terraform infrastructure' },
];

/**
 * Return a specific, human-readable warning for a potentially destructive bash
 * command (most-specific signature wins), or `null` when none matches. This is
 * purely INFORMATIONAL: it explains WHY a command is destructive in a permission
 * prompt and never gates auto-approval. Mirrors Claude's `getDestructiveCommandWarning`
 * (bash). The allow/deny logic stays in {@link PermissionManager.check}.
 */
export function getDestructiveCommandWarning(command: string): string | null {
  for (const { pattern, warning } of DESTRUCTIVE_WARNING_PATTERNS) {
    if (pattern.test(command)) return warning;
  }
  return null;
}

export function detectCommandChaining(command: string): { chained: boolean; operators: string[]; count: number } {
  const stripped = command.replace(/"[^"]*"|'[^']*'/g, '');
  const operators: string[] = [];
  if (/&&/.test(stripped)) operators.push('&&');
  if (/\|\|/.test(stripped)) operators.push('||');
  if (/;/.test(stripped)) operators.push(';');

  const segments = stripped.split(/\s*(?:&&|\|\||;)\s*/).filter(s => s.trim());
  return { chained: operators.length > 0, operators, count: segments.length };
}

// ---------------------------------------------------------------------------
// Build-configuration file guard (supply-chain attack surface)
// ---------------------------------------------------------------------------

/**
 * Basenames of build/CI/package-manager config files that can be weaponized
 * for supply-chain attacks. These require explicit approval even in
 * `acceptEdits` mode.
 */
const BUILD_CONFIG_BASENAMES = new Set([
  // Make
  'makefile', 'gnumakefile',
  // Docker
  'dockerfile', 'docker-compose.yml', 'docker-compose.yaml', '.dockerignore',
  // CI
  '.gitlab-ci.yml', '.gitlab-ci.yaml', 'jenkinsfile',
  // Python packaging
  'pyproject.toml', 'setup.py', 'setup.cfg',
  // Rust
  'cargo.toml',
  // Go
  'go.mod', 'go.sum',
  // Node/npm/yarn — package.json scripts run arbitrary code on npm install
  'package.json', 'package-lock.json',
  '.npmrc', '.yarnrc', '.yarnrc.yml',
  'pnpm-lock.yaml', 'yarn.lock',
  // Git/tool config files that redirect fetches or run arbitrary commands:
  // `.gitmodules` points submodules at attacker URLs, `.ripgreprc` can set a
  // `--pre` preprocessor command (RCE), `.mailmap` rewrites identities. Mirrors
  // Claude's DANGEROUS_FILES list.
  '.gitmodules', '.ripgreprc', '.mailmap',
  // Env files
  '.env', '.env.local', '.env.production',
  // Shell startup / login files
  '.bashrc', '.bash_profile', '.bash_login', '.bash_logout',
  '.zshrc', '.zshenv', '.zprofile', '.zlogin', '.zlogout',
  '.profile', '.login',
  // Git config
  '.gitconfig',
]);

/**
 * Path patterns (checked against the full normalized path) for build config
 * files whose identity depends on directory context, not just the basename.
 */
const BUILD_CONFIG_PATH_PATTERNS: RegExp[] = [
  // GitHub Actions workflows
  /\/\.github\/workflows\/[^/]+\.ya?ml$/,
  // CircleCI
  /\/\.circleci\/config\.ya?ml$/,
  // Husky git hooks
  /\/\.husky\//,
  // Git hooks directory — writing here runs code on git operations
  /\/\.git\/hooks\//,
  // SSH directory — authorized_keys, config, known_hosts, private keys
  /\/\.ssh\//,
  // GPG keys and config
  /\/\.gnupg\//,
  // AWS credentials and config
  /\/\.aws\//,
  // Kubernetes configs
  /\/\.kube\//,
  // Docker credentials and config
  /\/\.docker\//,
];

/**
 * Return true if `filePath` names a build-configuration file that should
 * require explicit approval even in `acceptEdits` mode.
 */
export function isBuildConfigFile(filePath: string): boolean {
  const normalized = filePath.replace(/\\/g, '/');
  const basename = path.basename(normalized).toLowerCase();

  if (BUILD_CONFIG_BASENAMES.has(basename)) return true;

  for (const pattern of BUILD_CONFIG_PATH_PATTERNS) {
    if (pattern.test(normalized)) return true;
  }

  return false;
}

// ---------------------------------------------------------------------------
// Agent-own-config guard — edits to the agent's own settings/config dirs need
// explicit approval even in acceptEdits (prompt-injection / self-reconfig vector)
// ---------------------------------------------------------------------------

/**
 * Config/IDE directory segments that must never be silently auto-edited: the
 * agent's own settings (`.ami`, `.ami-plugin`, `.claude`), IDE config (`.vscode`,
 * `.idea`) and the git metadata dir (`.git`). Writing into these can reconfigure
 * the agent, add hooks/commands/skills, or weaponize IDE tasks. Mirrors Claude's
 * DANGEROUS_DIRECTORIES + isClaudeConfigFilePath guards.
 */
const AGENT_CONFIG_DIR_SEGMENTS = ['.ami', '.ami-plugin', '.claude', '.vscode', '.idea', '.git'];

/** Settings/config basenames that are sensitive wherever they live. */
const AGENT_CONFIG_BASENAMES = new Set([
  'settings.json', 'settings.local.json', 'permissions.json', 'hooks.json',
  'plugin.json', 'config.json', 'mcp.json', '.mcp.json',
]);

/**
 * Return true if `filePath` targets the agent's own config/settings/IDE/git
 * directories or a sensitive settings file — such edits require explicit approval
 * even in acceptEdits mode, mirroring Claude's isInClaudeFolder / isAgentSettingsFile.
 */
export function isAgentConfigPath(filePath: string): boolean {
  const normalized = filePath.replace(/\\/g, '/');
  const segments = normalized.split('/');
  for (const seg of segments) {
    if (AGENT_CONFIG_DIR_SEGMENTS.includes(seg)) return true;
  }
  if (AGENT_CONFIG_BASENAMES.has(path.basename(normalized))) return true;
  return false;
}

// ---------------------------------------------------------------------------
// Suspicious path-pattern detection (canonicalization-bypass vectors)
// ---------------------------------------------------------------------------

/** UNC path (`\\server\share` or `//server/share`) — remote-resource / creds vector. */
function isUncPath(p: string): boolean {
  if (/^\\\\[^\\]/.test(p)) return true;      // \\server
  if (/^\/\/[^/]/.test(p)) return true;       // //server
  return false;
}

/**
 * Detect path-canonicalization bypass vectors that string-based blocklists miss,
 * applied on ALL platforms (NTFS can be mounted on Linux/macOS via ntfs-3g):
 * - NTFS Alternate Data Streams (`file.txt::$DATA`, `.bashrc:$DATA`);
 * - 8.3 short names (`GIT~1`, `SETTIN~1.JSON`);
 * - long-path prefixes (`\\?\`, `\\.\`, `//?/`, `//./`);
 * - trailing dots/spaces Windows strips (`.git.`, `settings.json `);
 * - DOS device-name suffixes (`.git.CON`, `settings.json.PRN`);
 * - triple-dot path components (`/.../`);
 * - UNC paths.
 *
 * The ADS colon check is restricted to the explicit `:$`/`::` stream markers so
 * legitimate colons in POSIX filenames are not false-positived (a deliberate
 * tightening of Claude's platform-gated "any colon after the drive letter" rule).
 * Mirrors Claude's hasSuspiciousWindowsPathPattern.
 */
export function hasSuspiciousPathPattern(p: string): boolean {
  if (!p) return false;
  // NTFS ADS stream markers (explicit, to stay cross-platform safe).
  if (/::|:\$/.test(p)) return true;
  // 8.3 short names.
  if (/~\d/.test(p)) return true;
  // Long-path prefixes.
  if (p.startsWith('\\\\?\\') || p.startsWith('\\\\.\\') || p.startsWith('//?/') || p.startsWith('//./')) {
    return true;
  }
  // Trailing dots / spaces.
  if (/[.\s]+$/.test(p)) return true;
  // DOS device-name suffixes.
  if (/\.(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])$/i.test(p)) return true;
  // Triple-dot path component.
  if (/(^|[/\\])\.{3,}([/\\]|$)/.test(p)) return true;
  // UNC path.
  if (isUncPath(p)) return true;
  return false;
}

/**
 * Walk the FULL symlink chain for `filePath` hop-by-hop and return the original
 * path, every intermediate symlink target, AND the final resolved path — so a
 * deny/boundary check matches at ANY hop (e.g. `test.txt → /etc/passwd` yields
 * both, closing the symlink-escape vector where only the final realpath is
 * checked). Bounded by `maxDepth` (SYMLOOP_MAX=40) with a visited-set loop guard.
 * Mirrors Claude's getPathsForPermissionCheck. Non-existent / unreadable hops stop
 * the walk gracefully and still return what was collected.
 */
export function getPathsForPermissionCheck(filePath: string, maxDepth = 40): string[] {
  const results: string[] = [];
  const visited = new Set<string>();
  let current = filePath;
  for (let depth = 0; depth < maxDepth; depth++) {
    const abs = path.isAbsolute(current) ? current : path.resolve(current);
    const norm = path.normalize(abs);
    if (visited.has(norm)) break; // circular symlink loop
    visited.add(norm);
    if (!results.includes(norm)) results.push(norm);
    let target: string;
    try {
      const stat = fs.lstatSync(norm);
      if (!stat.isSymbolicLink()) break;
      target = fs.readlinkSync(norm);
    } catch {
      break; // non-existent or unreadable — stop, keep what we have
    }
    // Resolve a relative link target against the link's own directory.
    current = path.isAbsolute(target) ? target : path.resolve(path.dirname(norm), target);
  }
  return results;
}

/** System directories that should never be written to. */
const SYSTEM_DIRECTORIES = [
  '/etc', '/usr', '/bin', '/sbin', '/var',
  '/sys', '/proc', '/dev', '/boot', '/lib', '/lib64',
  '/opt', '/Volumes', '/mnt', '/media', '/run/media',
];

const WINDOWS_DRIVE_ROOT_REGEX = /^[A-Za-z]:\/?$/;
const WINDOWS_DRIVE_CHILD_REGEX = /^[A-Za-z]:\/[^/]+$/;

/**
 * Is an ALREADY-RESOLVED path a catastrophic target for a removal (`rm`/`rmdir`)?
 * Generalizes the enumerated system-directory HARDLINE list to EVERY direct child
 * of the filesystem root (so `/srv`, `/root`, `/run` — not just the hardcoded
 * `/etc`/`/usr`/… — are caught) plus the whole-directory wildcard, the root
 * itself, the home directory, and Windows drive roots / their direct children.
 * Pure string predicate (no fs access) so it can run on an unvalidated path.
 *
 * Dangerous when the resolved path is:
 * - the bare wildcard `*` or any path ending in `/*` (wipes a whole directory);
 * - the filesystem root `/`;
 * - a Windows drive root (`C:\`, `D:/`);
 * - the user's home directory;
 * - a direct child of root (`/usr`, `/tmp`, `/opt`, `/srv`, …) — but NOT a
 *   grandchild (`/usr/local` is left to the normal flow);
 * - a direct child of a Windows drive root (`C:\Windows`, `C:\Users`).
 *
 * Mirrors Claude's `isDangerousRemovalPath` (utils/permissions/pathValidation.ts).
 */
export function isDangerousRemovalPath(resolvedPath: string): boolean {
  // Collapse mixed/duplicated separators so `C:\\Windows` (valid in PowerShell)
  // and `//srv` don't slip past the drive-child / root-child checks.
  const forwardSlashed = resolvedPath.replace(/[\\/]+/g, '/');

  if (forwardSlashed === '*' || forwardSlashed.endsWith('/*')) return true;

  const normalizedPath =
    forwardSlashed === '/' ? forwardSlashed : forwardSlashed.replace(/\/$/, '');

  if (normalizedPath === '/') return true;
  if (WINDOWS_DRIVE_ROOT_REGEX.test(normalizedPath)) return true;

  const normalizedHome = os.homedir().replace(/[\\/]+/g, '/');
  if (normalizedPath === normalizedHome) return true;

  // Direct children of root: /usr, /tmp, /etc (but not /usr/local).
  if (path.posix.dirname(normalizedPath) === '/') return true;
  if (WINDOWS_DRIVE_CHILD_REGEX.test(normalizedPath)) return true;

  return false;
}

/**
 * True when `command` contains an `rm`/`rmdir` invocation whose target resolves
 * to a catastrophic path ({@link isDangerousRemovalPath}). Such a removal must
 * ALWAYS confirm explicitly — it can never be silently auto-allowed by a wildcard
 * allow rule or by a blanket auto-allow mode. Compound-command aware (each
 * `&&`/`||`/`;`/`|` segment is inspected independently) and tilde-aware (`~`
 * expands to the home directory; relative operands resolve against `cwd`), so
 * `rm -rf ~`, `rm -rf /srv`, `rm -rf *` and `rm -rf C:\` are all caught even
 * where the enumerated system-directory hardline list is not. Mirrors Claude's
 * BashTool `checkDangerousRemovalPaths` (tools/BashTool/pathValidation.ts).
 */
export function commandHasDangerousRemoval(command: string, cwd: string = process.cwd()): boolean {
  for (const segment of splitCompoundCommand(command)) {
    const trimmed = stripEnvVarPrefix(segment, /* safeEnvOnly */ false).trim();
    const base = trimmed.split(/\s+/)[0];
    if (base !== 'rm' && base !== 'rmdir') continue;
    for (const rawPath of extractCommandPaths(trimmed)) {
      // Strip surrounding quotes, then expand a leading `~` / resolve relatives —
      // mirroring Claude's expandTilde + resolve(cwd, …) before the predicate.
      const cleaned = rawPath.replace(/^['"]|['"]$/g, '');
      let resolved: string;
      if (cleaned === '~' || cleaned.startsWith('~/')) {
        resolved = path.join(os.homedir(), cleaned.slice(1));
      } else if (path.isAbsolute(cleaned) || WINDOWS_DRIVE_ROOT_REGEX.test(cleaned.replace(/\\/g, '/'))) {
        resolved = cleaned;
      } else {
        resolved = path.resolve(cwd, cleaned);
      }
      if (isDangerousRemovalPath(resolved)) return true;
    }
  }
  return false;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Shell wrappers that execute their arguments as commands.
 * If we see these, we must also classify the inner command.
 */
const SHELL_WRAPPERS = new Set([
  'bash', 'sh', 'zsh', 'dash', 'fish', 'ksh', 'csh', 'tcsh',
  'env', 'sudo', 'su', 'doas', 'nohup', 'nice', 'timeout',
  'xargs', 'exec', 'eval',
  // `time <cmd>` measures the inner command's runtime — the SAFETY of the line is
  // the safety of the inner command, so unwrap it (Claude's `time` command spec).
  'time',
  // `stdbuf [opts] <cmd>` only tweaks the inner command's stdio buffering — the
  // safety of the line is the inner command's. bash-security's SAFE_WRAPPERS
  // already unwraps `stdbuf`; add it here too so the two layers agree and the
  // existing stdbuf operand-skip below becomes reachable.
  'stdbuf',
  // `srun [opts] <cmd>` is a SLURM launcher whose trailing operand is itself a
  // command run on cluster nodes (Claude's srun spec marks that arg isCommand).
  // Unwrap it so `srun rm -rf /` / `srun mkfs …` classify on the INNER command
  // rather than the opaque `srun` token.
  'srun',
  'watch', 'ionice', 'setarch', 'taskset', 'chrt', 'strace', 'ltrace',
]);

/**
 * Unwrap shell control structures (for/while/until/if/case) so that the
 * body commands are what gets classified, not the keywords themselves.
 *
 * for f in *.ts; do cat "$f"; done  →  cat "$f"
 * while read l; do echo "$l"; done  →  echo "$l"
 * if test -f x; then cat x; else echo no; fi  →  cat x\necho no
 *
 * Operates recursively — nested loops are unwrapped.
 */
export function unwrapShellControlStructures(command: string): string {
  let result = command;
  // For/while/until loops: extract body between do...done
  // Handles: for ...; do BODY; done | while ...; do BODY; done
  const loopRe = /\b(?:for|while|until)\b[^;]*;\s*do\b([\s\S]*?)\bdone\b/g;
  let prev = '';
  while (prev !== result) {
    prev = result;
    result = result.replace(loopRe, (_match, body: string) => body.trim());
  }
  // If/then/else/elif/fi: extract bodies from then/else/elif branches
  const ifRe = /\bif\b[^;]*;\s*then\b([\s\S]*?)\bfi\b/g;
  prev = '';
  while (prev !== result) {
    prev = result;
    result = result.replace(ifRe, (_match, body: string) => {
      // Split on else/elif to get all branch bodies
      return body.replace(/\b(?:elif\b[^;]*;\s*then|else)\b/g, ';').trim();
    });
  }
  return result;
}

/**
 * Extract all command names from a shell string.
 * Handles: pipes, semicolons, &&/||, command substitution $(...),
 * subshells (...), shell wrappers (env, sudo, bash -c),
 * shell control structures (for/while/if), and
 * environment variable assignments as prefixes.
 */
function findMatchingParen(s: string, start: number): number {
  let depth = 1;
  for (let i = start; i < s.length; i++) {
    if (s[i] === '(') depth++;
    if (s[i] === ')') { depth--; if (depth === 0) return i; }
  }
  return -1;
}

/**
 * Hard cap on how deep the recursive command decomposition
 * ({@link extractBaseCommands} into `$(...)`/backticks/subshells/`-c` payloads and
 * {@link extractCommandSubstitutions}'s nested-substitution walk) may descend
 * before it stops. A maliciously deep nesting chain
 * (`$( $( $( … rm -rf / … ) ) )`) would otherwise recurse unbounded and blow the
 * call stack (RangeError) or hang permission evaluation — a denial-of-service on
 * the safety layer itself. Mirrors Claude Code's `recursionDepth > 10` boundary in
 * `utils/bash/prefix.ts`. Beyond the cap the decomposition returns what it has
 * collected so far; the width guard ({@link MAX_SUBCOMMANDS_FOR_SECURITY_CHECK})
 * independently fails compound commands closed, so a command too deep to fully
 * analyze still can never be *proven* safe.
 */
export const MAX_COMMAND_PARSE_DEPTH = 10;

function extractBaseCommands(command: string, depth = 0): string[] {
  const commands: string[] = [];

  // Bounded recursion: a pathologically deep wrapper/substitution chain must not
  // blow the stack or hang the permission check (DoS). Mirrors Claude's
  // recursionDepth>10 guard — stop descending past the cap.
  if (depth > MAX_COMMAND_PARSE_DEPTH) return commands;

  // 0. Unwrap shell control structures so for/while/if bodies are classified
  command = unwrapShellControlStructures(command);

  function extractDelimited(
    cmd: string,
    match: (c: string, idx: number, src: string) => { innerStart: number; end: number } | null,
  ): void {
    let i = 0;
    while (i < cmd.length) {
      const m = match(cmd[i], i, cmd);
      if (m) {
        commands.push(...extractBaseCommands(cmd.slice(m.innerStart, m.end), depth + 1));
        i = m.end + 1;
      } else {
        i++;
      }
    }
  }

  // 1. Extract commands from $(...) and `...` substitutions
  extractDelimited(command, (ch, i, src) => {
    if (ch === '$' && i + 1 < src.length && src[i + 1] === '(') {
      const end = findMatchingParen(src, i + 2);
      return end !== -1 ? { innerStart: i + 2, end } : null;
    }
    if (ch === '`') {
      const end = src.indexOf('`', i + 1);
      return end !== -1 ? { innerStart: i + 1, end } : null;
    }
    return null;
  });

  // 2. Extract commands from subshells (...)
  extractDelimited(command, (ch, i, src) => {
    if (ch === '(' && (i === 0 || src[i - 1] !== '$')) {
      const end = findMatchingParen(src, i + 1);
      return end !== -1 ? { innerStart: i + 1, end } : null;
    }
    return null;
  });

  // 3. Split on pipes, semicolons, newlines, && and ||
  const segments = command.split(/\s*(?:\|\||&&|[|;\n])\s*/);

  for (const segment of segments) {
    const trimmed = segment.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;

    const tokens = trimmed.split(/\s+/);
    let i = 0;

    // Skip environment variable assignments
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) {
      i++;
    }

    if (i >= tokens.length) continue;

    let cmd = tokens[i]!;

    // Unwrap shell wrappers: env cmd, sudo cmd, bash -c "cmd"
    while (SHELL_WRAPPERS.has(cmd) && i + 1 < tokens.length) {
      const wrapper = cmd;
      commands.push(cmd);
      i++;
      // Skip flags (e.g., sudo -u root, bash -c)
      while (i < tokens.length && tokens[i]!.startsWith('-')) {
        const flag = tokens[i]!;
        i++;
        // -c takes the next arg as a command string — parse it
        if ((flag === '-c') && i < tokens.length) {
          const innerCmd = tokens.slice(i).join(' ').replace(/^["']|["']$/g, '');
          commands.push(...extractBaseCommands(innerCmd, depth + 1));
          i = tokens.length;
          break;
        }
        // `srun` has value-taking options that consume the NEXT token as a count
        // (`-n 4`, `--ntasks 4`, `-N 2`, `--nodes 2`). The generic flag skip above
        // only drops the `-flag` token itself, so without this the count operand
        // would be mis-read as the wrapped command. Mirrors Claude's srun spec
        // option model so `srun -n 4 rm -rf /` reaches the inner `rm`.
        if (wrapper === 'srun' && /^(-n|--ntasks|-N|--nodes)$/.test(flag)
            && i < tokens.length && /^\d/.test(tokens[i]!)) {
          i++;
        }
        // Value-taking wrapper flags (`timeout -s KILL`, `timeout -k 3`, `stdbuf
        // -o L`, `nice -n 5`) consume the NEXT token as their operand. The generic
        // flag skip above drops only the `-flag` token, so without this the operand
        // (`KILL`/`L`/`5`) is mis-read as the wrapped command — e.g. `timeout -s
        // KILL 5 rm -rf /` would classify as `KILL` instead of reaching `rm`.
        // Shared WRAPPER_VALUE_FLAGS table keeps this operand knowledge in one place.
        const wrapperValueFlag = WRAPPER_VALUE_FLAGS[wrapper];
        if (wrapperValueFlag && wrapperValueFlag.test(flag) && i < tokens.length
            && !tokens[i]!.startsWith('-')) {
          i++;
        }
      }
      // Skip a leading duration/priority operand so the WRAPPED command is
      // reached: `timeout 30 ls` -> ls, `nice 10 ls` -> ls (mirrors
      // stripSafeWrappers in bash-security). Without this the base command is
      // mis-detected as the numeric token and classified unsafe (S7).
      if ((wrapper === 'timeout' || wrapper === 'stdbuf') && i < tokens.length && /^[\d.]/.test(tokens[i]!)) {
        i++;
      } else if (wrapper === 'nice' && i < tokens.length && /^\d+$/.test(tokens[i]!)) {
        i++;
      }
      // After unwrapping a shell wrapper (especially env), skip env var
      // assignments (e.g., env PATH=/usr/bin ls -> skip PATH=/usr/bin)
      while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) {
        i++;
      }
      if (i < tokens.length) {
        cmd = tokens[i]!;
      }
    }

    if (i < tokens.length) {
      commands.push(cmd);
      if (i + 1 < tokens.length) {
        commands.push(cmd + ' ' + tokens[i + 1]!);
        if (i + 2 < tokens.length) {
          commands.push(cmd + ' ' + tokens[i + 1]! + ' ' + tokens[i + 2]!);
        }
      }
      // Spec-driven subcommand prefix: for a CLI with a known option schema, ALSO
      // emit the `command subcommand` form reached after skipping leading global
      // flags and the values they consume, so `git -C /repo status` yields the
      // `git status` base (not just `git -C /repo`) and matches SAFE_PREFIXES.
      // Added alongside the raw adjacent-token bases above — never replaces them —
      // so existing matches are preserved. Mirrors Claude's findFirstSubcommand
      // spec-driven flag-value skipping (utils/shell/specPrefix.ts).
      const spec = BASH_COMMAND_SPECS[cmd];
      if (spec) {
        const sub = skipGlobalFlags(tokens, i + 1, spec);
        if (sub !== i + 1 && sub < tokens.length && !tokens[sub]!.startsWith('-')) {
          commands.push(cmd + ' ' + tokens[sub]!);
          if (sub + 1 < tokens.length && !tokens[sub + 1]!.startsWith('-')) {
            commands.push(cmd + ' ' + tokens[sub]! + ' ' + tokens[sub + 1]!);
          }
        }
      }
    }
  }

  // 4. Detect dangerous redirects to system paths
  if (/>\s*\/etc\/|>\s*\/usr\/|>\s*\/bin\//i.test(command)) {
    commands.push('redirect-to-system-path');
  }

  return commands;
}

function allSegmentsKnown(command: string): boolean {
  const unwrapped = unwrapShellControlStructures(command);
  const segments = unwrapped.split(/\s*(?:\|\||&&|[|;])\s*/).filter(s => s.trim());
  if (segments.length <= 1) return true;
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    let i = 0;
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
    if (i >= tokens.length) continue;
    const cmdTokens = tokens.slice(i);
    let found = false;
    for (let j = 1; j <= Math.min(cmdTokens.length, 3); j++) {
      const prefix = cmdTokens.slice(0, j).join(' ');
      if (SAFE_COMMANDS.has(prefix) || SAFE_PREFIXES.has(prefix)) { found = true; break; }
    }
    // Spec-driven retry: if the raw first-N tokens didn't match, skip leading
    // global flags (and their values) for a known-schema CLI and test the
    // flag-skipped `command subcommand` prefix, so `git -C /repo status` matches
    // `git status`. Mirrors Claude's spec-driven prefix extraction.
    if (!found && cmdTokens.length > 0) {
      const spec = BASH_COMMAND_SPECS[cmdTokens[0]!];
      if (spec) {
        const sub = skipGlobalFlags(cmdTokens, 1, spec);
        for (let j = 2; j <= 3 && sub + (j - 2) < cmdTokens.length; j++) {
          const prefix = cmdTokens[0]! + ' ' + cmdTokens.slice(sub, sub + (j - 1)).join(' ');
          if (SAFE_PREFIXES.has(prefix)) { found = true; break; }
        }
      }
    }
    if (!found) return false;
  }
  return true;
}

/**
 * Benign edit-adjacent filesystem-mutation commands that `acceptEdits` mode may
 * auto-allow, mirroring Claude's ACCEPT_EDITS_ALLOWED_COMMANDS
 * ([mkdir, touch, rm, rmdir, mv, cp, sed]). AMI DELIBERATELY excludes `rm`/`rmdir`:
 * its destructive-command safety invariant keeps every deletion gated behind an
 * explicit prompt even in acceptEdits (an intentional superset divergence —
 * never regress a delete guard). `sed` is included because `sed -i` is an edit.
 */
const ACCEPT_EDITS_FS_COMMANDS = new Set(['mkdir', 'touch', 'mv', 'cp', 'sed']);

/**
 * True when EVERY sub-command of `command` is a benign filesystem-mutation base
 * command ({@link ACCEPT_EDITS_FS_COMMANDS}). Used by `acceptEdits` mode to
 * auto-allow file-edit-adjacent shell work (`mkdir`, `touch`, `mv`, `cp`,
 * `sed -i`) the way Claude does — without broadening to arbitrary commands.
 * A command substitution (`$(...)`, backticks, `$VAR`) is rejected outright so it
 * cannot smuggle a non-fs command into an otherwise-benign invocation.
 */
export function isAcceptEditsFilesystemMutation(command: string): boolean {
  const trimmed = command.trim();
  if (!trimmed) return false;
  if (containsShellSubstitution(trimmed) || trimmed.includes('`')) return false;
  const subs = splitCompoundCommand(trimmed);
  if (subs.length === 0) return false;
  for (const sub of subs) {
    const base = stripEnvVarPrefix(sub, /* safeEnvOnly */ false).trim().split(/\s+/)[0];
    if (!base || !ACCEPT_EDITS_FS_COMMANDS.has(base)) return false;
  }
  return true;
}

/**
 * Introspection accessor: the bash base-commands a given permission {@link
 * PermissionMode} auto-approves without a prompt, SORTED for stable display.
 * Only `acceptEdits` auto-allows a fixed edit-adjacent set (the SAME canonical
 * {@link ACCEPT_EDITS_FS_COMMANDS} the runtime {@link isAcceptEditsFilesystemMutation}
 * gate uses — no duplicate table); every other mode returns an empty list (their
 * auto-approval is classification-driven, not a fixed command set). Mirrors
 * Claude's `getAutoAllowedCommands(mode)` (BashTool/modeValidation.ts), adapted to
 * AMI's superset divergence that excludes `rm`/`rmdir` so deletions stay gated.
 * Surfaced through {@link getDefaultPermissionPatterns} (→ `/permissions defaults`)
 * so a user can see exactly which commands acceptEdits waves through.
 */
export function getAutoAllowedCommands(mode: PermissionMode): readonly string[] {
  return mode === 'acceptEdits' ? [...ACCEPT_EDITS_FS_COMMANDS].sort() : [];
}

/**
 * Git read-only write-guard — shared by the bash classifier AND the bash-result
 * cache (`isReadOnlyBashCommand`) so there is ONE place that knows which git
 * forms actually mutate state. A git subcommand can appear in a "read-only"
 * allowlist yet still WRITE: creating a ref (`git branch <name>`, `git tag
 * <name>`), rewriting remotes (`git remote add/remove/set-url/rename`), writing
 * a file via `--output`/`-o`, or setting config (`git config key value`).
 * Claude Code's readOnlyCommandValidation.ts rejects each of these before
 * auto-approval; this mirrors that.
 *
 * `tokens` is a single git invocation beginning at the `git` token. Returns
 * true when the invocation is a genuine read-only form, false when it writes.
 */
export function isGitInvocationReadOnly(tokens: string[]): boolean {
  if (tokens[0] !== 'git') return true; // not a git invocation — not our concern

  // Skip git global options (and the value they consume) to reach the subcommand.
  const valueGlobals = new Set(['-C', '-c', '--git-dir', '--work-tree', '--namespace']);
  // Globals that change git's EXECUTION CONTEXT — a different repo/worktree
  // (`-C`, `--git-dir`, `--work-tree`, `--namespace`) whose on-disk config can run
  // arbitrary code during a "read" (e.g. a malicious `core.fsmonitor`), or an
  // inline config injection (`-c core.pager=<cmd>` / `--config-env`) — are NEVER
  // auto-allowed as read-only, mirroring AMI's existing cd+git RCE guard
  // (isGitReadOnlyCommandSafe) so a global flag before the subcommand cannot
  // smuggle an exec vector past the classifier. A prompt is still shown.
  const execContextGlobals = new Set([
    '-C', '-c', '--git-dir', '--work-tree', '--namespace', '--config-env',
  ]);
  let i = 1;
  while (i < tokens.length) {
    const t = tokens[i]!;
    if (!t.startsWith('-')) break;
    const bare = t.includes('=') ? t.slice(0, t.indexOf('=')) : t;
    if (execContextGlobals.has(bare)) return false; // exec-context change / config injection
    if (valueGlobals.has(t)) { i += 2; continue; } // `--opt value`
    i += 1;                                         // flag or `--opt=value`
  }
  const sub = tokens[i];
  if (!sub) return true; // bare `git` prints help — harmless
  const rest = tokens.slice(i + 1);

  // A file-writing output flag turns ANY read subcommand into a write.
  for (const a of rest) {
    if (a === '-o' || a === '--output' || a.startsWith('--output=')) return false;
  }

  // `git ls-remote` is read-only EXCEPT `--server-option`/`-o`, which transmit an
  // arbitrary attacker-controlled string to the remote in the protocol-v2
  // capability advertisement — a network WRITE / exfil primitive on a nominally
  // read-only command. `-o` is already caught by the output-flag scan above; block
  // the long form here too. Mirrors Claude's ls-remote --server-option exclusion.
  if (sub === 'ls-remote') {
    for (const a of rest) {
      if (a === '--server-option' || a.startsWith('--server-option=')) return false;
    }
  }

  if (sub === 'branch' || sub === 'tag') {
    const writeFlags = sub === 'branch'
      ? new Set(['-d', '-D', '--delete', '-m', '-M', '--move', '-c', '-C', '--copy',
          '-f', '--force', '--set-upstream-to', '-u', '--unset-upstream', '--edit-description'])
      : new Set(['-d', '--delete', '-a', '--annotate', '-s', '--sign', '-m', '--message',
          '-F', '--file', '-f', '--force']);
    // Read flags whose FOLLOWING token is a value (not a ref name / positional).
    const valueFlags = new Set(['--contains', '--no-contains', '--merged', '--no-merged',
      '--points-at', '--sort', '--format', '--color', '--column']);
    let hasList = false;
    let hasPositional = false;
    let endOfOptions = false; // after a bare `--`, everything is a positional
    for (let k = 0; k < rest.length; k++) {
      const a = rest[k]!;
      if (endOfOptions) { hasPositional = true; continue; } // post-`--` tokens are ref names
      if (a === '--') { endOfOptions = true; continue; } // `git branch -- -l` → `-l` is a ref name
      if (a === '-l' || a === '--list') { hasList = true; continue; }
      if (writeFlags.has(a)) return false;
      if (a.startsWith('-')) {
        if (!a.includes('=') && valueFlags.has(a)) k++; // skip the consumed value
        continue;
      }
      hasPositional = true; // a bare ref name
    }
    // A bare positional without --list is ref CREATION (a write).
    return !(hasPositional && !hasList);
  }

  if (sub === 'remote') {
    const first = rest.find(a => !a.startsWith('-'));
    if (!first) return true; // bare `git remote` / `git remote -v`
    return first === 'show' || first === 'get-url';
  }

  if (sub === 'config') {
    const writeSubops = ['--add', '--unset', '--unset-all', '--replace-all',
      '--remove-section', '--rename-section', '--edit', '-e'];
    if (rest.some(a => writeSubops.includes(a))) return false;
    const readSubops = ['--get', '--get-all', '--get-regexp', '--get-urlmatch', '--list', '-l'];
    if (rest.some(a => readSubops.includes(a))) return true;
    // `git config key value` writes; `git config key` reads a single value.
    const positionals = rest.filter(a => !a.startsWith('-'));
    return positionals.length < 2;
  }

  return true; // other read subcommands (status/log/diff/show/...) stay read-only
}

/**
 * Whole-command git read-only guard for the bash classifier. Splits a (possibly
 * compound) command into segments, validates every git invocation via
 * isGitInvocationReadOnly, and refuses a compound that changes directory before
 * running git (`cd /attacker && git status`) — a sandbox escape where git runs
 * attacker-planted .git/hooks from the new cwd (Claude's cd+git guard).
 */
export function isGitReadOnlyCommandSafe(command: string): boolean {
  const unwrapped = unwrapShellControlStructures(command);
  const segments = unwrapped.split(/\s*(?:\|\||&&|[|;])\s*/).filter(s => s.trim());
  let hasCd = false;
  let hasGit = false;
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    let i = 0;
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
    const cmd = tokens[i];
    if (cmd === 'cd' || cmd === 'pushd' || cmd === 'popd' || cmd === 'chdir') hasCd = true;
    if (cmd === 'git') {
      hasGit = true;
      if (!isGitInvocationReadOnly(tokens.slice(i))) return false;
    }
  }
  if (hasCd && hasGit) return false;
  return true;
}

/**
 * Does `command` invoke `git` as a base command (compound-aware)? Env-var prefixes
 * (`GIT_DIR=… git …`) and leading `sudo`/`env`-style wrappers are peeled by the
 * shared {@link extractBaseCommands} decomposition, so `git`, `GIT_DIR=x git log`,
 * and `cd x && git status` all report true. Used to gate git invocations behind the
 * disguised-bare-repo boundary check in {@link PermissionManager.check}.
 */
export function commandInvokesGit(command: string): boolean {
  return extractBaseCommands(command).some(b => b === 'git' || b.startsWith('git '));
}

/**
 * Detect that `cwd` is (or has been manipulated to look like) a BARE git repo, so
 * that running any git command from it would make git treat `cwd` as the git dir
 * and execute attacker-planted hooks (`hooks/pre-commit`, …) — a sandbox-escape
 * vector. Git's `is_git_directory()` (setup.c) treats a directory as a git dir when
 * it holds a valid `HEAD` ref plus `objects/` and `refs/`. The attack: plant
 * `HEAD` + `objects/` + `refs/` + `hooks/pre-commit` in `cwd` and remove/corrupt
 * `.git/HEAD` so git falls back to cwd discovery and runs the hook.
 *
 * Returns true when `cwd` has bare-repo indicators WITHOUT a valid `.git/HEAD`:
 * - `.git` is a regular FILE → worktree/submodule gitdir reference → SAFE (false).
 * - `.git` is a directory with a regular-file `HEAD` → normal repo → SAFE (false).
 * - otherwise, flag if any of `HEAD` (file), `objects/` (dir), `refs/` (dir) exist
 *   directly in `cwd`.
 *
 * Each indicator is stat'd under its own try/catch so one stat error can't mask
 * another. Pure + synchronous (a permission-eval boundary check). Mirrors Claude's
 * `isCurrentDirectoryBareGitRepo` (utils/git.ts). `cwd` defaults to the process cwd.
 */
export function isDisguisedBareGitRepo(cwd: string = process.cwd()): boolean {
  const gitPath = path.join(cwd, '.git');
  try {
    const stats = fs.statSync(gitPath);
    if (stats.isFile()) return false; // worktree/submodule gitdir reference
    if (stats.isDirectory()) {
      try {
        // An attacker creating .git/HEAD as a DIRECTORY passes a bare statSync but
        // git rejects a non-file HEAD and falls back to cwd discovery — so only a
        // regular-file HEAD proves a normal repo.
        if (fs.statSync(path.join(gitPath, 'HEAD')).isFile()) return false;
      } catch { /* .git exists but no HEAD — fall through to bare-repo check */ }
    }
  } catch { /* no .git — fall through to bare-repo indicator check */ }

  // No valid .git/HEAD — flag if cwd itself carries bare-repo indicators.
  try { if (fs.statSync(path.join(cwd, 'HEAD')).isFile()) return true; } catch { /* none */ }
  try { if (fs.statSync(path.join(cwd, 'objects')).isDirectory()) return true; } catch { /* none */ }
  try { if (fs.statSync(path.join(cwd, 'refs')).isDirectory()) return true; } catch { /* none */ }
  return false;
}

/**
 * Detect a compound command that changes directory (`cd`/`pushd`/`chdir`/
 * PowerShell `Set-Location`) AND then performs a file-writing output redirection
 * (`>`/`>>` to a file, not an fd-dup like `2>&1`). The validator classifies the
 * redirect target against the launch cwd, but at runtime it resolves against the
 * NEW cwd — a path-resolution/TOCTOU desync that can write outside the workspace
 * (`cd /outside && echo payload > config`). Claude requires manual approval for
 * this shape (pathValidation.ts cd+write guard, PowerShell Set-Location desync).
 */
export function commandHasCdWriteDesync(command: string): boolean {
  const unwrapped = unwrapShellControlStructures(command);
  const segments = unwrapped.split(/\s*(?:\|\||&&|[|;])\s*/).filter(s => s.trim());
  let hasCd = false;
  let hasRedirect = false;
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    let i = 0;
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
    const cmd = tokens[i];
    if (cmd === 'cd' || cmd === 'pushd' || cmd === 'chdir' ||
        (cmd && cmd.toLowerCase() === 'set-location')) hasCd = true;
    // A file redirect: optional leading fd, `>`/`>>`, then a target that is not
    // an fd-dup (`&1`). Quoted sections are stripped so `echo "a > b"` is ignored.
    const unquoted = stripQuotedSections(seg);
    if (/\d*>>?\s*(?![&\s])\S/.test(unquoted)) hasRedirect = true;
  }
  return hasCd && hasRedirect;
}

/**
 * Count the directory-change segments (`cd`/`pushd`/`chdir`/PowerShell
 * `Set-Location`) in a (possibly compound) command. More than one directory
 * change in a single command is an unusual navigation pattern that defeats
 * cwd-relative path validation and must require approval (Claude's multi-cd
 * guard, bashCommandHelpers.ts).
 */
export function countDirectoryChanges(command: string): number {
  const unwrapped = unwrapShellControlStructures(command);
  const segments = unwrapped.split(/\s*(?:\|\||&&|[|;])\s*/).filter(s => s.trim());
  let count = 0;
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    let i = 0;
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
    const cmd = tokens[i];
    if (cmd === 'cd' || cmd === 'pushd' || cmd === 'chdir' ||
        (cmd && cmd.toLowerCase() === 'set-location')) count++;
  }
  return count;
}

/**
 * Match a simple glob-like pattern against a string.
 * Supports '*' as a wildcard for any sequence of characters.
 */
/**
 * Compile+cache a glob pattern's regex, bounded via the shared LRU memoizer so a
 * long-lived process matching many distinct rules can never grow this cache
 * without bound (dotAll `s`: `*` also spans embedded newlines, so a rule like
 * `*rm -rf*` still matches a multi-line command and a deny cannot be bypassed by
 * splitting the command across lines — Claude's dotAll wildcard matching).
 */
const _compilePattern = memoizeWithLRU(
  (pattern: string): RegExp => new RegExp(`^${globToRegexSource(pattern)}$`, 's'),
  (pattern: string) => pattern,
  1000,
);
/**
 * Match a glob-like pattern (only `*` is special, expanding to any run of
 * characters; every other metachar is escaped) against a value. This is the
 * canonical wildcard matcher reused by MCP-server allowlists, permission rules
 * and the HTTP-hook URL allowlist — do not re-implement it per site.
 */
export function wildcardMatch(pattern: string, value: string): boolean {
  return matchPattern(pattern, value);
}

/**
 * Compile a glob-like pattern to a regex source, honouring backslash escapes so
 * a pattern can match a LITERAL asterisk or backslash (Claude's matchWildcardPattern
 * escape handling, shellRuleMatching.ts): an UNescaped `*` is the wildcard
 * (`.*`), `\*` is a literal `*`, and `\\` is a literal `\`. A lone backslash
 * before any other character stays a literal backslash (back-compat). Every
 * other regex metacharacter is escaped.
 */
export function globToRegexSource(pattern: string): string {
  let out = '';
  let unescapedStars = 0;
  for (let i = 0; i < pattern.length; i++) {
    const ch = pattern[i]!;
    if (ch === '\\') {
      const next = pattern[i + 1];
      if (next === '*') { out += '\\*'; i++; continue; }   // escaped → literal asterisk
      if (next === '\\') { out += '\\\\'; i++; continue; } // escaped → literal backslash
      out += '\\\\'; continue;                             // lone backslash → literal
    }
    if (ch === '*') { out += '.*'; unescapedStars++; continue; } // wildcard
    if (/[.+^${}()|[\]?]/.test(ch)) { out += '\\' + ch; continue; } // escape other metachars
    out += ch;
  }
  // Trailing ` *` prefix semantics: when the ONLY unescaped wildcard is a sole
  // trailing ` *` (space + wildcard), treat it like a legacy `:*` prefix rule —
  // `git *` matches the bare `git` command AND `git add`, aligning wildcard rules
  // with prefix (`git:*`) semantics — by making the trailing segment optional.
  // Multi-wildcard patterns (e.g. `* run *`) are deliberately excluded so they
  // keep their strict meaning. Mirrors Claude's shellRuleMatching trailing-
  // wildcard-optional rewrite.
  if (unescapedStars === 1 && pattern.endsWith(' *') && out.endsWith(' .*')) {
    out = out.slice(0, -3) + '( .*)?';
  }
  return out;
}

function matchPattern(pattern: string, value: string): boolean {
  if (pattern.includes('\x00') || value.includes('\x00')) return false;
  if (pattern === value) return true;
  if (pattern === '*') return true;

  return _compilePattern(pattern).test(value);
}

/**
 * Normalize line continuations (backslash + newline) in a bash command.
 * Bash treats `\<newline>` as a line join — stripping it reveals the
 * actual command that will execute, preventing pattern-bypass via
 * multi-line splitting.
 */
export function normalizeLineContinuations(cmd: string): string {
  // A `\<newline>` is a true line-continuation ONLY when the backslash run
  // immediately before the newline is ODD (the last backslash escapes the
  // newline). With an EVEN run the backslashes pair up as literal escapes and
  // the newline is a REAL command separator that must be KEPT — otherwise
  // `echo \\<newline>rm -rf /` would be mis-joined into one command and the
  // second statement could slip past an end-anchored pattern. Mirrors Claude's
  // odd/even backslash-count discrimination (utils/bash/commands.ts L96-139).
  let out = '';
  for (let i = 0; i < cmd.length; i++) {
    if (cmd[i] === '\n') {
      let bs = 0;
      let j = out.length - 1;
      while (j >= 0 && out[j] === '\\') { bs++; j--; }
      if (bs % 2 === 1) {
        out = out.slice(0, out.length - 1); // drop the escaping backslash, join lines
        continue; // and drop the newline
      }
      out += '\n'; // even run (or none) — real separator, keep it
      continue;
    }
    out += cmd[i];
  }
  return out;
}

const COMPOUND_SPLIT_RE = /\s*(?:\|\||&&|[;|])\s*/;

/**
 * Split a compound bash command into individual sub-commands.
 * First unwraps shell control structures (for/while/if), then splits
 * on &&, ||, ;, and | operators.
 */
function splitCompoundCommand(cmd: string): string[] {
  const unwrapped = unwrapShellControlStructures(cmd);
  return unwrapped.split(COMPOUND_SPLIT_RE).map(s => s.trim()).filter(s => s.length > 0);
}

/**
 * Cap on how many sub-commands a single compound command may fan out into before
 * the per-subcommand safety analysis (classification + rule matching) is run.
 * Above the cap we FAIL CLOSED — the command is treated as unsafe and an allow
 * rule cannot auto-approve it, so it falls back to a prompt. A pathologically
 * wide `a && b && c && …` chain otherwise runs the full validator set per segment
 * and can starve the event loop (Claude's MAX_SUBCOMMANDS_FOR_SECURITY_CHECK,
 * CC-643). Fifty is generous — legitimate user commands do not split that wide.
 */
export const MAX_SUBCOMMANDS_FOR_SECURITY_CHECK = 50;

/**
 * Extract the inner command string(s) from `$(...)` command substitutions and
 * `` `...` `` backticks, recursively (nested substitutions included).
 *
 * Used ONLY for deny/ask matching (M1): a DENIED command must not be able to hide
 * inside a substitution (e.g. `echo $(rm -rf x)` or `` echo `rm -rf x` ``). Allow
 * matching never consults this — decomposition can only ADD denies, never broaden
 * an allow.
 */
function extractCommandSubstitutions(command: string): string[] {
  const results: string[] = [];
  const walk = (s: string, depth: number): void => {
    // Bounded recursion: a deeply-nested substitution chain must not blow the
    // stack or hang deny-matching (DoS). Mirrors Claude's recursionDepth>10 guard.
    if (depth > MAX_COMMAND_PARSE_DEPTH) return;
    for (let i = 0; i < s.length; i++) {
      if (s[i] === '$' && i + 1 < s.length && s[i + 1] === '(') {
        const end = findMatchingParen(s, i + 2);
        if (end !== -1) {
          const inner = s.slice(i + 2, end);
          results.push(inner);
          walk(inner, depth + 1); // nested $(...) / backticks
          i = end;
        }
      } else if (s[i] === '`') {
        const end = s.indexOf('`', i + 1);
        if (end !== -1) {
          const inner = s.slice(i + 1, end);
          results.push(inner);
          walk(inner, depth + 1);
          i = end;
        }
      }
    }
  };
  walk(command, 0);
  return results;
}

/**
 * Match a bash command against a permission rule pattern, handling:
 * 1. Line continuation normalization (\<newline> → join)
 * 2. Compound command decomposition:
 *    - allow rules: ALL sub-commands must match (prevents "git status && rm -rf /")
 *    - deny rules: ANY sub-command matching triggers deny
 */
export function stripEnvVarPrefix(cmd: string, safeEnvOnly = false): string {
  // Allow-rule matching passes safeEnvOnly=true: only provably-harmless env vars
  // (SAFE_ENV_VARS) are stripped, so an escalation prefix like `PATH=/evil` or
  // `LD_PRELOAD=x` STAYS in the command and breaks the match — forcing a prompt
  // instead of silently auto-approving. Deny/ask matching strips ALL assignments
  // aggressively so a denied command cannot hide behind any env-var prefix.
  // Mirrors Claude's split (allow = safe-list strip, deny = strip-all).
  if (safeEnvOnly) {
    // Drop leading assignments one at a time, but STOP at the first non-safe var
    // (it stays, breaking any allow match). The command tail is preserved exactly.
    // Allow matching is single-pass and never strips wrappers — auto-approving a
    // wrapped/escalated form (`sudo git status`) must NOT match a `git status`
    // allow rule — so de-obfuscation can only ever force a prompt, never broaden.
    let s = cmd;
    for (;;) {
      const m = s.match(/^\s*([A-Za-z_]\w*)=(?:"[^"]*"|'[^']*'|\S+)\s+/);
      if (!m || !SAFE_ENV_VARS.has(m[1]!)) break;
      s = s.slice(m[0].length);
    }
    return s;
  }
  // Deny/ask matching: strip ALL leading env assignments AND privilege/shell
  // wrappers (sudo, env, bash -c, eval, timeout, …), iterating both to a FIXED
  // POINT so interleaved layers fully reduce. A single pass stops at the first
  // non-wrapper token, so `timeout 5 FOO=bar curl` would keep the `FOO=bar`
  // prefix and miss a `curl` deny rule; looping reduces it to `curl` (strip
  // `timeout 5` → strip `FOO=bar` → `curl`). Mirrors Claude's fixed-point
  // composition of stripSafeWrappers + stripAllLeadingEnvVars for deny rules.
  let stripped = cmd;
  for (;;) {
    const before = stripped;
    // eslint-disable-next-line security/detect-unsafe-regex -- anchored, disjoint alternation
    stripped = stripped.replace(/^(\s*[A-Za-z_]\w*=(?:"[^"]*"|'[^']*'|\S+)\s+)+/, '');
    const tokens = stripped.split(/\s+/);
    let i = 0;
    while (i < tokens.length && SHELL_WRAPPERS.has(tokens[i]!)) {
      i++;
      while (i < tokens.length && tokens[i]!.startsWith('-')) i++;
      while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
      // A numeric operand belongs to the wrapper, not the wrapped command:
      // `timeout 5 CMD` / `timeout 1m CMD` (duration), `nice 10 CMD` /
      // `ionice 2 CMD` (priority). Skip it so the deny/ask match reaches the real
      // command (`timeout 5 rm -rf /` must still hit a `rm` deny rule).
      while (i < tokens.length && /^\d+[smhd]?$/.test(tokens[i]!)) i++;
    }
    if (i > 0 && i < tokens.length) stripped = tokens.slice(i).join(' ');
    if (stripped === before) break;
  }
  return stripped;
}

// ---------------------------------------------------------------------------
// Scoped command-prefix derivation for "allow always" permission rules
// ---------------------------------------------------------------------------

const ENV_VAR_ASSIGN_RE = /^[A-Za-z_][A-Za-z0-9_]*=/;
// Subcommand shape: lowercase alnum, hyphen-separated (e.g. `commit`, `rev-parse`).
// The `[a-z0-9]*` run and the `-[a-z0-9]+` groups start with disjoint chars
// (alnum vs `-`), so there is no ambiguous overlap / ReDoS backtracking.
// eslint-disable-next-line security/detect-unsafe-regex
const SUBCOMMAND_SHAPE_RE = /^[a-z][a-z0-9]*(-[a-z0-9]+)*$/;

/**
 * Bare single-word prefixes that must NEVER be offered as an "allow always"
 * scope: shells and arg-executing wrappers (a `bash:*` / `env:*` / `sudo:*` rule
 * auto-approves arbitrary code via `-c` or by wrapping any command) plus
 * cross-platform code interpreters (a `node:*` / `python:*` rule would
 * auto-allow arbitrary code execution). Mirrors Claude's BARE_SHELL_PREFIXES +
 * NEVER_SUGGEST / CROSS_PLATFORM_CODE_EXEC denylists.
 */
export const BARE_SHELL_PREFIXES = new Set([
  'sh', 'bash', 'zsh', 'fish', 'csh', 'tcsh', 'ksh', 'dash', 'cmd', 'powershell', 'pwsh',
  // wrappers that exec their args as a command
  'env', 'xargs', 'nice', 'stdbuf', 'nohup', 'timeout', 'time',
  // privilege escalation
  'sudo', 'doas', 'pkexec',
  // cross-platform code-exec interpreters
  'node', 'deno', 'bun', 'python', 'python3', 'ruby', 'perl', 'php', 'osascript', 'iex',
]);

/**
 * Commands that take subcommands or depth-scoped flags: a bare single-word
 * `git:*` / `docker:*` rule would auto-allow dangerous subcommands forever
 * (`git push --force`, `docker run`). For these we refuse a bare-root suggestion
 * and require the two-word `command subcommand` form (or fall back to the exact
 * command). Mirrors Claude's DEPTH_RULES + "fig-spec declares subcommands"
 * bare-root guard (staticPrefix.ts L142-156).
 */
export const SUBCOMMAND_AWARE_ROOTS = new Set([
  'git', 'gh', 'docker', 'kubectl', 'az', 'aws', 'gcloud', 'npm', 'pnpm', 'yarn',
  'cargo', 'go', 'pip', 'pip3', 'brew', 'apt', 'apt-get', 'dnf', 'yum', 'pacman',
  'systemctl', 'terraform', 'helm', 'make',
]);

/**
 * Minimal command-spec model for prefix extraction. For a subcommand-aware CLI
 * it declares the global option flags that CONSUME the following token as their
 * value (so `git -C /repo status` / `kubectl -n kube-system get pods` skip the
 * flag+value to reach the real subcommand) and an optional `depth` — how many
 * words the scoped prefix should extend to (default 2). Mirrors Claude's fig-spec
 * `options[].args` "does this flag take a value" (utils/shell/specPrefix.ts) and
 * its `DEPTH_RULES` per-command default depth table (utils/powershell/staticPrefix.ts).
 *
 * This is a static, parser-free analogue of Claude's dynamically-imported fig
 * specs — the specs those builds cannot load at runtime are exactly the ones
 * Claude hardcodes in DEPTH_RULES, so AMI hardcodes the same shallow table here.
 */
export interface BashCommandSpec {
  /** Global flags (long or short) that consume the next token as their value. */
  valueFlags: ReadonlySet<string>;
  /** Scoped-prefix depth in words (command + subcommands). Defaults to 2. */
  depth?: number;
}

export const BASH_COMMAND_SPECS: Record<string, BashCommandSpec> = {
  git: { valueFlags: new Set(['-C', '-c', '--git-dir', '--work-tree', '--exec-path', '--namespace']) },
  docker: { valueFlags: new Set(['-H', '--host', '--context', '--config', '-l', '--log-level']), depth: 3 },
  kubectl: {
    valueFlags: new Set(['-n', '--namespace', '--context', '--kubeconfig', '--cluster', '--user', '--server', '--as']),
    depth: 3,
  },
  helm: { valueFlags: new Set(['-n', '--namespace', '--kube-context', '--kubeconfig']), depth: 3 },
  aws: { valueFlags: new Set(['--region', '--profile', '--endpoint-url', '--output', '--cli-read-timeout']), depth: 4 },
  az: { valueFlags: new Set(['--subscription', '--output', '-o']), depth: 4 },
  gcloud: { valueFlags: new Set(['--project', '--account', '--configuration', '--format', '--verbosity']), depth: 4 },
  npm: { valueFlags: new Set(['--prefix', '-w', '--workspace', '--registry']) },
  pnpm: { valueFlags: new Set(['--prefix', '-w', '--workspace', '--filter', '-C', '--dir']) },
  yarn: { valueFlags: new Set(['--cwd']) },
  cargo: { valueFlags: new Set(['-Z', '--color', '--config']) },
};

/** Does `flag` consume the following token as its value, per `spec`? */
function flagTakesValue(flag: string, spec: BashCommandSpec | undefined): boolean {
  // Strip an inline `--flag=value` form — that carries its own value.
  if (flag.includes('=')) return false;
  return spec?.valueFlags.has(flag) ?? false;
}

/**
 * Advance past leading global flags (and the values `spec` says they consume)
 * starting at `tokens[start]`, returning the index of the first non-flag token —
 * the real subcommand. `git -C /repo status` → the index of `status`;
 * `docker -D ps` → the index of `ps`. This is the canonical spec-driven
 * flag-value skip shared by prefix suggestion (getSimpleCommandPrefix), base
 * extraction (extractBaseCommands) and classification (allSegmentsKnown + the
 * docker read-only check), so a global flag before the subcommand can no longer
 * defeat SAFE_PREFIXES / DOCKER_SAFE_SUBCOMMANDS matching. Mirrors Claude's
 * findFirstSubcommand + flagTakesArg (utils/shell/specPrefix.ts L40-137).
 */
function skipGlobalFlags(tokens: string[], start: number, spec: BashCommandSpec | undefined): number {
  let i = start;
  while (i < tokens.length && tokens[i]!.startsWith('-')) {
    if (flagTakesValue(tokens[i]!, spec)) i++;
    i++;
  }
  return i;
}

/**
 * Scoped subcommand prefix ("command subcommand", e.g. `git commit`, `npm run`,
 * `kubectl get pods`). Skips leading SAFE_ENV_VARS assignments (returns null on
 * an unsafe env var so matching falls back to the exact command), then — spec
 * driven — skips leading global flags AND the values they consume (`git -C /repo
 * status` → `git status`) before resolving the first real subcommand. Extends to
 * the command's spec `depth` (default 2) so deep CLIs like `docker`/`kubectl`/
 * `aws` yield a usefully-scoped prefix instead of a bare root. Returns null when
 * no subcommand-shaped token follows the command (a flag, path, number or
 * filename), so the caller falls back to the first-word / exact-command rule.
 * Mirrors Claude's getSimpleCommandPrefix + spec-driven flag-value skipping +
 * DEPTH_RULES.
 */
export function getSimpleCommandPrefix(command: string): string | null {
  const tokens = command.trim().split(/\s+/).filter(Boolean);
  if (tokens.length === 0) return null;
  let i = 0;
  while (i < tokens.length && ENV_VAR_ASSIGN_RE.test(tokens[i]!)) {
    if (!SAFE_ENV_VARS.has(tokens[i]!.split('=')[0]!)) return null;
    i++;
  }
  const cmd = tokens[i];
  if (!cmd) return null;
  const spec = BASH_COMMAND_SPECS[cmd];
  const depth = spec?.depth ?? 2;
  const parts = [cmd];
  // Skip leading global flags (and any value they consume) to reach the subcommand.
  i = skipGlobalFlags(tokens, i + 1, spec);
  // Collect subcommand-shaped words up to the spec depth; stop at the first flag
  // or non-subcommand-shaped token once collection has begun.
  while (parts.length < depth && i < tokens.length) {
    const tok = tokens[i]!;
    if (tok.startsWith('-') || !SUBCOMMAND_SHAPE_RE.test(tok)) break;
    parts.push(tok);
    i++;
  }
  if (parts.length < 2) return null;
  return parts.join(' ');
}

/**
 * First-word fallback prefix (e.g. `npm`, `cargo`). Skips safe env vars, rejects
 * paths/flags/numbers/filenames and anything in BARE_SHELL_PREFIXES. Mirrors
 * Claude's getFirstWordPrefix.
 */
export function getFirstWordPrefix(command: string): string | null {
  const tokens = command.trim().split(/\s+/).filter(Boolean);
  let i = 0;
  while (i < tokens.length && ENV_VAR_ASSIGN_RE.test(tokens[i]!)) {
    if (!SAFE_ENV_VARS.has(tokens[i]!.split('=')[0]!)) return null;
    i++;
  }
  const cmd = tokens[i];
  if (!cmd) return null;
  if (!SUBCOMMAND_SHAPE_RE.test(cmd)) return null;
  if (BARE_SHELL_PREFIXES.has(cmd)) return null;
  // Dangerous PowerShell cmdlets/aliases (download/exfil, code-exec, process
  // spawn, scriptblock-arg gate, …): a `invoke-webrequest*` / `foreach-object*`
  // wildcard would defeat the per-command security checks, so never suggest one.
  if (isPowerShellNeverSuggest(cmd)) return null;
  return cmd;
}

/**
 * Strip shell output/input redirection clauses (`> out.txt`, `>>log`, `2>&1`,
 * `&> both`, `< in`, `<<<here`) from a command so a redirect *target filename*
 * never leaks into a derived permission-rule prefix (turning `npm test > out.txt`
 * into a bogus `Bash(out.txt)` rule). Quote-aware: a `>` inside single/double
 * quotes is preserved. Mirrors Claude's `commandTransform` hook on
 * `generateShellSuggestionsLabel` (shellPermissionHelpers: "Bash strips output
 * redirections so filenames don't show as commands"). Best-effort for display /
 * rule derivation only — it never participates in a security decision.
 */
export function stripOutputRedirections(command: string): string {
  let out = '';
  let i = 0;
  let quote: string | null = null;
  const n = command.length;
  while (i < n) {
    const ch = command[i]!;
    if (quote) {
      out += ch;
      if (ch === quote && command[i - 1] !== '\\') quote = null;
      i++;
      continue;
    }
    if (ch === '"' || ch === "'") { quote = ch; out += ch; i++; continue; }
    // Optional leading file-descriptor digits (`2>`, `1>>`), then an operator.
    let j = i;
    while (j < n && command[j]! >= '0' && command[j]! <= '9') j++;
    // `>&`/`<&` and `&>` must be tried before the plain `>>?`/`<<?<?` so
    // `2>&1` is consumed as one operator (not `>` + leftover `&1`).
    const op = /^(?:>&|<&|&>>?|>>?|<<?<?)/.exec(command.slice(j));
    // Require a real redirection: either fd digits were consumed (`2>`), or the
    // char itself is a redirection lead (`<`/`>`/`&>`). A lone `&`/`&&` (control
    // operator) and digits that are just an argument are left untouched.
    if (op && (j > i || ch === '<' || ch === '>' || (ch === '&' && op[0].startsWith('&')))) {
      i = j + op[0].length;
      while (i < n && /\s/.test(command[i]!)) i++;          // skip whitespace
      while (i < n && !/[\s|;&<>()]/.test(command[i]!)) i++; // skip target token
      if (out && !out.endsWith(' ')) out += ' ';
      continue;
    }
    out += ch;
    i++;
  }
  return out.replace(/\s+/g, ' ').trim();
}

/**
 * Derive a narrow, scoped "allow always" prefix for a bash command, or null if
 * none is safe to suggest (the caller then falls back to the exact command).
 * Prefers the two-word `command subcommand` form; falls back to the first word
 * ONLY when that root is neither subcommand-aware nor a shell/interpreter/
 * wrapper. This is the single canonical prefix-derivation helper — it replaces
 * ad-hoc "first N words" heuristics and enforces the bare-root + NEVER_SUGGEST
 * guards in one place. Output redirections are stripped first so a redirect
 * target filename never becomes the suggested rule prefix.
 */
/**
 * Is `command` a plain `--help` invocation (`python --help`, `git commit --help`)
 * that is read-only and safe to allow AS-IS? Such a command should be allowed by
 * its FULL text rather than collapsed to a broad prefix (`python:*`), which would
 * grant arbitrary code execution through the interpreter. True only when:
 * - the command ends with `--help`;
 * - it carries no quotes (a quote could smuggle an argument past the check);
 * - `--help` is the ONLY flag; and
 * - every non-flag token is a bare alphanumeric identifier (no paths, operators,
 *   substitutions or redirections).
 *
 * `--help` forms that route through an unsafe help backend (`man -l`,
 * `git help --web`, …) are excluded — those are not simple `--help` at all and
 * are caught here via {@link detectUnsafeHelpCommand}. Mirrors Claude's
 * `isHelpCommand` (utils/bash/commands.ts), which bypasses prefix extraction so a
 * help command is allowed in full instead of as a too-broad prefix.
 */
export function isHelpCommand(command: string): boolean {
  const trimmed = command.trim();
  if (!trimmed.endsWith('--help')) return false;
  // Quotes could hide an injected argument — reject outright.
  if (trimmed.includes('"') || trimmed.includes("'")) return false;
  // A help form that reaches an unsafe backend is never a "simple" help command.
  if (detectUnsafeHelpCommand(trimmed)) return false;
  const words = trimmed.split(/\s+/).filter(Boolean);
  let foundHelp = false;
  const alphanumeric = /^[a-zA-Z0-9]+$/;
  for (const word of words) {
    if (word.startsWith('-')) {
      if (word === '--help') foundHelp = true;
      else return false; // any other flag disqualifies it
    } else if (!alphanumeric.test(word)) {
      return false; // paths, operators, substitutions, etc.
    }
  }
  return foundHelp;
}

export function suggestAllowPrefix(command: string): string | null {
  const cleaned = stripOutputRedirections(command);
  // A simple `--help` command is read-only and safe: offer the FULL command as
  // the allow rule rather than a broad `cmd:*` prefix that would grant arbitrary
  // execution through the same interpreter. Mirrors Claude's isHelpCommand
  // bypass of prefix extraction (utils/bash/commands.ts).
  if (isHelpCommand(cleaned)) return cleaned.trim();
  const two = getSimpleCommandPrefix(cleaned);
  // Guard the two-word path too: `start-process notepad*` would match
  // `start-process notepad.exe & evil`, so a dangerous PowerShell spawner as
  // the ROOT word is never offered even with a scoped subcommand.
  if (two) return isPowerShellNeverSuggest(two.split(' ')[0]!) ? null : two;
  const one = getFirstWordPrefix(cleaned);
  if (one && !SUBCOMMAND_AWARE_ROOTS.has(one)) return one;
  return null;
}

/**
 * Word-aligned longest common prefix of a group of command prefixes.
 * e.g. ["git fetch", "git worktree add"] → "git";
 *      ["npm run test", "npm run lint"]  → "npm run".
 * Returns at least the first word so a group never collapses to "". Mirrors
 * Claude's `longestCommonPrefix` (utils/bash/prefix.ts).
 */
function wordAlignedCommonPrefix(prefixes: string[]): string {
  if (prefixes.length === 0) return '';
  if (prefixes.length === 1) return prefixes[0]!;
  const words = prefixes[0]!.split(' ');
  let common = words.length;
  for (let i = 1; i < prefixes.length; i++) {
    const other = prefixes[i]!.split(' ');
    let shared = 0;
    while (shared < common && shared < other.length && words[shared] === other[shared]) shared++;
    common = shared;
  }
  return words.slice(0, Math.max(1, common)).join(' ');
}

/**
 * Per-subcommand scoped "allow always" prefixes for a (possibly compound)
 * command. A single command returns `[prefix]` (or `[]` when no safe prefix can
 * be derived). A compound command (`cd src && git status && npm test`) is split
 * into subcommands; each subcommand the caller marks via `excludeSubcommand`
 * (typically read-only / already-auto-allowed commands such as `cd`/`git
 * status`) is DROPPED, and a scoped prefix is derived for each remaining one.
 * Prefixes sharing a root word are collapsed via word-aligned LCP so a chain of
 * `git fetch && git worktree add` yields a single `git` rule rather than two.
 *
 * This is the compound-aware counterpart to {@link suggestAllowPrefix}: callers
 * seeding "allow always" rules for a compound command MUST use this so a rule is
 * scoped to each mutating subcommand instead of the misleading two-word prefix of
 * the whole chain (e.g. `cd src`). Mirrors Claude's
 * `getCompoundCommandPrefixesStatic` (utils/bash/prefix.ts) + per-subcommand
 * rule derivation (BashPermissionRequest `subcommandResults`).
 */
export function getCompoundCommandPrefixes(
  command: string,
  excludeSubcommand?: (subcommand: string) => boolean,
): string[] {
  const subcommands = splitCompoundCommand(command);
  if (subcommands.length <= 1) {
    const only = suggestAllowPrefix(command.trim());
    return only ? [only] : [];
  }
  const prefixes: string[] = [];
  for (const sub of subcommands) {
    const trimmed = sub.trim();
    if (!trimmed) continue;
    if (excludeSubcommand?.(trimmed)) continue;
    const prefix = suggestAllowPrefix(trimmed);
    if (prefix) prefixes.push(prefix);
  }
  if (prefixes.length === 0) return [];
  const groups = new Map<string, string[]>();
  for (const prefix of prefixes) {
    const root = prefix.split(' ')[0]!;
    const group = groups.get(root);
    if (group) group.push(prefix);
    else groups.set(root, [prefix]);
  }
  const collapsed: string[] = [];
  for (const group of groups.values()) collapsed.push(wordAlignedCommonPrefix(group));
  return collapsed;
}

// ---------------------------------------------------------------------------
// Permission-rule string format — `Tool` / `Tool(specifier)`
// ---------------------------------------------------------------------------

/** Parsed form of a canonical permission-rule string. */
export interface PermissionRuleValue {
  toolName: string;
  ruleContent?: string;
}

/**
 * Canonical Claude-Code wire name for the subagent/delegation tool (its current
 * tool name is `Agent`). AMI's own tool is `task`; a permission rule, hook, or
 * imported Claude settings file that names `Agent(...)` must therefore resolve to
 * AMI's `task` tool. Mirrors Claude Code's `AGENT_TOOL_NAME` (AgentTool/constants.ts).
 */
export const AGENT_TOOL_NAME = 'Agent';
/**
 * Legacy Claude-Code wire name for the same tool, kept so a rule written against
 * the OLD spelling still matches (parity: Claude's `LEGACY_AGENT_TOOL_NAME = 'Task'`,
 * retained "for backward compat (permission rules, hooks, resumed sessions)").
 */
export const LEGACY_AGENT_TOOL_NAME = 'Task';

/**
 * Legacy / cross-framework tool-name aliases → AMI canonical tool names. A
 * permission rule written with a Claude-Code capitalized name (`Bash`, `Edit`)
 * or an AMI legacy name resolves to the same canonical AMI tool, so rules keep
 * matching after a rename or when imported from a Claude settings file.
 *
 * Both the CURRENT Claude agent-tool name ({@link AGENT_TOOL_NAME} = `Agent`) and
 * its LEGACY spelling ({@link LEGACY_AGENT_TOOL_NAME} = `Task`) map to AMI's `task`
 * tool, so a rule like `Agent(researcher)` imported from a Claude settings file
 * gates AMI's subagent spawns exactly as `task(researcher)` does.
 */
export const PERMISSION_TOOL_ALIASES: Record<string, string> = {
  Bash: 'bash', Read: 'file_read', Edit: 'file_edit', Write: 'file_edit',
  MultiEdit: 'file_edit', Glob: 'glob', Grep: 'grep', LS: 'list_dir',
  WebFetch: 'web_fetch', WebSearch: 'web_search', NotebookEdit: 'notebook_edit',
  [AGENT_TOOL_NAME]: 'task', [LEGACY_AGENT_TOOL_NAME]: 'task', TodoWrite: 'todo_write', Skill: 'skill',
  // AMI legacy aliases
  KillShell: 'task_stop', BashOutputTool: 'task_output', AgentOutputTool: 'task_output',
};

/** Canonicalize a tool name used in a permission rule (legacy/alias → current). */
export function normalizeToolName(name: string): string {
  return PERMISSION_TOOL_ALIASES[name] ?? name;
}

/** True if `str[index]` is escaped (preceded by an odd number of backslashes). */
function isEscapedAt(str: string, index: number): boolean {
  let backslashCount = 0;
  let j = index - 1;
  while (j >= 0 && str[j] === '\\') { backslashCount++; j--; }
  return backslashCount % 2 !== 0;
}

function findFirstUnescapedChar(str: string, char: string): number {
  for (let i = 0; i < str.length; i++) {
    if (str[i] === char && !isEscapedAt(str, i)) return i;
  }
  return -1;
}

function findLastUnescapedChar(str: string, char: string): number {
  for (let i = str.length - 1; i >= 0; i--) {
    if (str[i] === char && !isEscapedAt(str, i)) return i;
  }
  return -1;
}

/** Escape backslashes then parentheses so rule content survives round-tripping. */
export function escapeRuleContent(content: string): string {
  return content.replace(/\\/g, '\\\\').replace(/\(/g, '\\(').replace(/\)/g, '\\)');
}

/** Inverse of {@link escapeRuleContent} (parens first, then backslashes). */
export function unescapeRuleContent(content: string): string {
  return content.replace(/\\\(/g, '(').replace(/\\\)/g, ')').replace(/\\\\/g, '\\');
}

/**
 * Parse a canonical permission-rule string into `{ toolName, ruleContent? }`,
 * honouring escaped parentheses and canonicalizing the tool name. `Tool()` and
 * `Tool(*)` collapse to a tool-wide rule (no ruleContent). Mirrors Claude's
 * permissionRuleValueFromString.
 */
export function permissionRuleValueFromString(ruleString: string): PermissionRuleValue {
  const openParenIndex = findFirstUnescapedChar(ruleString, '(');
  if (openParenIndex === -1) return { toolName: normalizeToolName(ruleString) };
  const closeParenIndex = findLastUnescapedChar(ruleString, ')');
  if (closeParenIndex === -1 || closeParenIndex <= openParenIndex) {
    return { toolName: normalizeToolName(ruleString) };
  }
  if (closeParenIndex !== ruleString.length - 1) {
    return { toolName: normalizeToolName(ruleString) };
  }
  const toolName = ruleString.substring(0, openParenIndex);
  const rawContent = ruleString.substring(openParenIndex + 1, closeParenIndex);
  if (!toolName) return { toolName: normalizeToolName(ruleString) };
  if (rawContent === '' || rawContent === '*') return { toolName: normalizeToolName(toolName) };
  return { toolName: normalizeToolName(toolName), ruleContent: unescapeRuleContent(rawContent) };
}

/** Serialize a {@link PermissionRuleValue} back to its canonical string form. */
export function permissionRuleValueToString(ruleValue: PermissionRuleValue): string {
  if (!ruleValue.ruleContent) return ruleValue.toolName;
  return `${ruleValue.toolName}(${escapeRuleContent(ruleValue.ruleContent)})`;
}

/** The three behaviors a permission rule can carry. Mirrors Claude's
 *  `permissionBehavior` enum; identical to {@link PermissionRule.action}. */
export type PermissionBehavior = 'allow' | 'deny' | 'ask';

/**
 * A structured, declarative mutation of a {@link PermissionManager}'s live rule
 * set / mode / workspace boundary — the canonical representation of "what to
 * change" that a permission prompt, slash command, or hook can hand back as a
 * one-click SUGGESTION (or a batch of them). Mirrors Claude Code's
 * `PermissionUpdate` discriminated union (utils/permissions/PermissionUpdate.ts
 * + PermissionUpdateSchema.ts):
 *  - `addRules`        — append allow/deny/ask rules to a destination tier;
 *  - `replaceRules`    — atomically swap ALL rules of a (behavior, destination);
 *  - `removeRules`     — drop rules (normalized parse→serialize match, so a
 *                         `Bash(*)` suggestion removes a stored tool-wide `Bash`);
 *  - `setMode`         — change the permission mode;
 *  - `addDirectories`/`removeDirectories` — widen/shrink the read-access boundary.
 *
 * `destination` tags which trust tier the change belongs to (the rule `source`,
 * or the directory grant's origin). It is a parity-faithful subset of
 * {@link PERMISSION_UPDATE_DESTINATIONS}. Applying an update is purely in-memory
 * (matching Claude's context transform); persistence is a separate step
 * ({@link PermissionManager.saveRulesToDestination}).
 */
export type PermissionUpdate =
  | { type: 'addRules'; rules: PermissionRuleValue[]; behavior: PermissionBehavior; destination?: PermissionUpdateDestination }
  | { type: 'replaceRules'; rules: PermissionRuleValue[]; behavior: PermissionBehavior; destination?: PermissionUpdateDestination }
  | { type: 'removeRules'; rules: PermissionRuleValue[]; behavior: PermissionBehavior; destination?: PermissionUpdateDestination }
  | { type: 'setMode'; mode: PermissionMode; destination?: PermissionUpdateDestination }
  | { type: 'addDirectories'; directories: string[]; destination?: PermissionUpdateDestination }
  | { type: 'removeDirectories'; directories: string[]; destination?: PermissionUpdateDestination };

/**
 * The kind of filesystem access a tool call represents — mirrors Claude's
 * `FileOperationType` (components/permissions/FilePermissionDialog/permissionOptions.tsx):
 * a read-only access, an edit of pre-existing content (`write`), or the creation of
 * a brand-new file (`create`). It drives OPERATION-AWARE permission affordances: a
 * `read` widens the read boundary by directory, a `write`/`create` offers a path
 * allow rule, and only a non-`read` op targeting the agent's own config folder gets
 * the "allow editing its own settings this session" grant.
 */
export type FileOperationType = 'read' | 'write' | 'create';

/** Read-shaped canonical tools: they access content without mutating the tree. */
const READ_SHAPED_FILE_TOOLS = new Set(['file_read', 'glob', 'grep', 'list_dir']);

/**
 * Classify a tool call's filesystem operation type (parity with Claude's
 * `operationType`), or `undefined` for a tool that targets no file path. Read-shaped
 * tools are `'read'`; a mutating file tool is `'create'` when its target does not yet
 * exist on disk and `'write'` when it does — so a prompt can distinguish editing
 * existing content from bringing a new file into being. The on-disk probe is a
 * best-effort `existsSync`; an erroring path falls back to `'write'`.
 */
export function resolveFileOperationType(
  toolName: string,
  input: Record<string, unknown>,
): FileOperationType | undefined {
  const canonical = normalizeToolName(toolName);
  if (READ_SHAPED_FILE_TOOLS.has(canonical)) return 'read';
  const filePath = (input.file_path ?? input.path) as unknown;
  if (typeof filePath !== 'string' || filePath.length === 0) return undefined;
  try {
    return fs.existsSync(expandPath(filePath)) ? 'write' : 'create';
  } catch {
    return 'write';
  }
}

/**
 * Which of the agent's own config folders a path lives in: the PROJECT config
 * folder (`<cwd>/.ami` | `.ami-plugin` | `.claude`) or the GLOBAL one
 * (`~/.ami` | `~/.ami-plugin` | `~/.claude`). Mirrors Claude's `isInClaudeFolder` /
 * `isInGlobalClaudeFolder` scope distinction.
 */
export type ConfigFolderScope = 'project-config-folder' | 'global-config-folder';

/** Directory names that are a genuine AMI *settings root* the agent may self-grant
 *  (NOT the IDE/git dirs in {@link AGENT_CONFIG_DIR_SEGMENTS}, which are not AMI's
 *  own config to offer a self-edit grant for). */
const CONFIG_FOLDER_NAMES = ['.ami', '.ami-plugin', '.claude'] as const;

/** True when the already-absolute `child` is `root` itself or nested within it,
 *  case-folded on win32 (case-insensitive FS) like Claude's normalizeCaseForComparison. */
function pathIsWithinDir(child: string, root: string): boolean {
  const fold = (p: string) => (process.platform === 'win32' ? p.toLowerCase() : p);
  const c = fold(path.normalize(child));
  const r = fold(path.normalize(root));
  return c === r || c.startsWith(r.endsWith(path.sep) ? r : r + path.sep);
}

/**
 * Resolve the config-folder {@link ConfigFolderScope} + matched root dir for
 * `filePath`, or `null` when it is outside every config folder. The PROJECT folder
 * wins when the two overlap (cwd under $HOME). Shared matcher behind
 * {@link resolveConfigFolderScope} and the config-folder permission suggestion — one
 * matching implementation, no per-caller duplication.
 */
function matchConfigFolderRoot(
  filePath: string,
  cwd: string,
): { scope: ConfigFolderScope; root: string } | null {
  if (!filePath) return null;
  const abs = path.resolve(cwd, expandPath(filePath));
  for (const name of CONFIG_FOLDER_NAMES) {
    const root = path.resolve(cwd, name);
    if (pathIsWithinDir(abs, root)) return { scope: 'project-config-folder', root };
  }
  const home = os.homedir();
  for (const name of CONFIG_FOLDER_NAMES) {
    const root = path.join(home, name);
    if (pathIsWithinDir(abs, root)) return { scope: 'global-config-folder', root };
  }
  return null;
}

/**
 * Resolve whether `filePath` lives inside the agent's own config folder (project or
 * global), returning the {@link ConfigFolderScope} or `null`. Mirrors Claude's
 * `isInClaudeFolder` / `isInGlobalClaudeFolder`; used to offer a config-folder-scoped
 * "allow editing own settings this session" grant at permission-decision time.
 */
export function resolveConfigFolderScope(
  filePath: string,
  cwd: string = process.cwd(),
): ConfigFolderScope | null {
  return matchConfigFolderRoot(filePath, cwd)?.scope ?? null;
}

/**
 * Compute the pre-baked {@link PermissionUpdate} *suggestions* to attach to an
 * ask/deny permission decision — the "always allow X" candidates a prompt can
 * offer for one-click acceptance (Claude's `permissionResult.suggestions`, parsed
 * by `extractRules`/`extractDirectories`). For a bash call it derives a scoped
 * allow rule per (mutating) subcommand via {@link getCompoundCommandPrefixes}
 * (sharing the single canonical prefix helper — no duplicate heuristic); for a
 * read-shaped file tool it suggests widening the read boundary to the file's
 * directory; for a mutating file tool it suggests an allow rule for that path.
 * Returns `[]` when nothing safe can be suggested (the caller then offers only
 * the plain one-off allow). All suggestions default to the ephemeral `session`
 * destination — persistence is a separate, explicit choice.
 */
/**
 * Derive the DOMAIN a `web_fetch` call targets, for a domain-scoped "always
 * allow" rule — the URL's lowercased hostname, or `null` when the input carries
 * no usable URL. Parity with Claude's WebFetch `inputToPermissionRuleContent`
 * (which yields `domain:<hostname>`); AMI's canonical web_fetch rule content is
 * the BARE hostname (`web_fetch(example.com)`) so it matches AMI's existing
 * web_fetch matcher and sandbox-domain extraction — an intentional superset
 * divergence kept consistent with {@link validatePermissionRule}.
 */
export function deriveWebFetchDomainRuleContent(input: Record<string, unknown>): string | null {
  const url = input.url;
  if (typeof url !== 'string' || url.length === 0) return null;
  try {
    const hostname = new URL(url).hostname;
    return hostname ? hostname.toLowerCase() : null;
  } catch {
    return null;
  }
}

export function buildPermissionSuggestions(
  toolName: string,
  input: Record<string, unknown>,
): PermissionUpdate[] {
  const canonical = normalizeToolName(toolName);
  if (canonical === 'bash') {
    const command = typeof input.command === 'string' ? input.command : undefined;
    if (!command) return [];
    const rules: PermissionRuleValue[] = getCompoundCommandPrefixes(command)
      .map(prefix => ({ toolName: 'bash', ruleContent: `${prefix}*` }));
    return rules.length > 0
      ? [{ type: 'addRules', behavior: 'allow', destination: 'session', rules }]
      : [];
  }
  if (canonical === 'web_fetch') {
    // Domain-scoped "always allow" candidate: grant exactly the URL's host, not a
    // tool-wide web_fetch allow (least privilege). Parity with Claude's
    // "Yes, and don't ask again for <hostname>" option.
    const domain = deriveWebFetchDomainRuleContent(input);
    return domain
      ? [{ type: 'addRules', behavior: 'allow', destination: 'session', rules: [{ toolName: 'web_fetch', ruleContent: domain }] }]
      : [];
  }
  const filePath = (input.file_path ?? input.path) as unknown;
  if (typeof filePath === 'string' && filePath.length > 0) {
    // Operation-aware shaping (parity with Claude's operationType): a READ widens
    // the read boundary by directory; a mutating op offers a path allow rule.
    if (resolveFileOperationType(canonical, input) === 'read') {
      return [{ type: 'addDirectories', destination: 'session', directories: [path.dirname(filePath)] }];
    }
    const suggestions: PermissionUpdate[] = [];
    // A write/create inside the agent's OWN config folder also offers a broader,
    // SESSION-scoped grant for that whole config folder — Claude's config-folder
    // "allow editing its own settings for this session" option — so the user is
    // not re-prompted per settings file. Session-only (never persisted): it widens
    // self-modification rights, which must not survive the process.
    const configFolder = matchConfigFolderRoot(filePath, process.cwd());
    if (configFolder) {
      suggestions.push({
        type: 'addRules', behavior: 'allow', destination: 'session',
        rules: [{ toolName: canonical, ruleContent: `${configFolder.root}${path.sep}*` }],
      });
    }
    suggestions.push({
      type: 'addRules', behavior: 'allow', destination: 'session',
      rules: [{ toolName: canonical, ruleContent: filePath }],
    });
    return suggestions;
  }
  return [];
}

/**
 * Build the model-facing "grant-and-retry" message for a set of just-approved,
 * previously-denied commands, so the agent knows it may re-attempt them. Mirrors
 * Claude's `createPermissionRetryMessage` (utils/messages.ts): the content is
 * `Allowed <cmd>, <cmd>, …`. Returns an empty-but-valid notice when the list is
 * empty so callers never have to special-case it.
 */
export function createPermissionRetryMessage(commands: string[]): string {
  if (commands.length === 0) return 'No denied commands to retry';
  return `Allowed ${commands.join(', ')}`;
}

/**
 * Validate a user-supplied permission-rule string, returning an actionable
 * diagnostic. Catches empty rules, mismatched / empty parentheses, empty tool
 * names, and bash/file pattern mistakes (`:*` placement, `:*` in file patterns).
 * Mirrors Claude's validatePermissionRule, minus the Claude-specific
 * uppercase-first convention (AMI canonical tool names are lowercase).
 */
export function validatePermissionRule(rule: string): {
  valid: boolean; error?: string; suggestion?: string; examples?: string[];
} {
  if (!rule || rule.trim() === '') {
    return { valid: false, error: 'Permission rule cannot be empty' };
  }
  let openCount = 0, closeCount = 0;
  for (let i = 0; i < rule.length; i++) {
    if (rule[i] === '(' && !isEscapedAt(rule, i)) openCount++;
    if (rule[i] === ')' && !isEscapedAt(rule, i)) closeCount++;
  }
  if (openCount !== closeCount) {
    return {
      valid: false, error: 'Mismatched parentheses',
      suggestion: 'Ensure all opening parentheses have matching closing parentheses',
    };
  }
  // Empty parentheses (escape-aware).
  for (let i = 0; i < rule.length - 1; i++) {
    if (rule[i] === '(' && rule[i + 1] === ')' && !isEscapedAt(rule, i)) {
      const toolName = rule.substring(0, rule.indexOf('('));
      if (!toolName) {
        return {
          valid: false, error: 'Empty parentheses with no tool name',
          suggestion: 'Specify a tool name before the parentheses',
        };
      }
      return {
        valid: false, error: 'Empty parentheses',
        suggestion: `Either specify a pattern or use just "${toolName}" without parentheses`,
        examples: [`${toolName}`, `${toolName}(some-pattern)`],
      };
    }
  }
  const parsed = permissionRuleValueFromString(rule);
  if (!parsed.toolName || parsed.toolName.length === 0) {
    return { valid: false, error: 'Tool name cannot be empty' };
  }
  // Bash prefix-rule mistakes: `:*` must be at the end and non-empty.
  if ((parsed.toolName === 'bash' || parsed.toolName === 'Bash') && parsed.ruleContent !== undefined) {
    const content = parsed.ruleContent;
    if (content === ':*') {
      return {
        valid: false, error: 'Prefix cannot be empty before :*',
        suggestion: 'Specify a command prefix before :*',
        examples: ['bash(npm:*)', 'bash(git:*)'],
      };
    }
    if (content.includes(':*') && !content.endsWith(':*')) {
      return {
        valid: false, error: 'The :* pattern must be at the end',
        suggestion: 'Move :* to the end for prefix matching, or use * for wildcard matching',
        examples: ['bash(npm run:*) - prefix matching', 'bash(npm run *) - wildcard matching'],
      };
    }
  }
  // Per-tool content validation (Claude's toolValidationConfig customValidation).
  if (parsed.ruleContent !== undefined) {
    // WebSearch: patterns are allowed/blocked search terms — wildcards are
    // meaningless and silently never match. Reject them with guidance.
    if (parsed.toolName === 'web_search') {
      if (parsed.ruleContent.includes('*') || parsed.ruleContent.includes('?')) {
        return {
          valid: false, error: 'WebSearch patterns cannot contain wildcards',
          suggestion: 'Use a plain search term (wildcards are not supported for WebSearch)',
          examples: ['web_search(weather)', 'web_search(stock prices)'],
        };
      }
    }
    // WebFetch: patterns match a domain, not a full URL. Reject a URL-form
    // pattern and point at the bare-domain form (Claude's WebFetch validator).
    if (parsed.toolName === 'web_fetch') {
      if (/^[a-z][a-z0-9+.-]*:\/\//i.test(parsed.ruleContent)) {
        return {
          valid: false, error: 'WebFetch patterns must be a domain, not a URL',
          suggestion: 'Drop the scheme and path — match on the domain only',
          examples: ['web_fetch(example.com)', 'web_fetch(*.github.com)'],
        };
      }
    }
  }
  return { valid: true };
}

/**
 * Fault-tolerant filter over a raw array of permission-rule STRINGS (the
 * `permissions.{allow,deny,ask}` settings.json form). Drops every entry that is
 * not a string or that fails {@link validatePermissionRule}, keeping the rest so
 * one malformed rule cannot poison the whole settings file, and returns a
 * human-readable warning per dropped rule. Fail-open-safe: on any unexpected
 * shape the entry is skipped with a warning rather than throwing. Mirrors
 * Claude's filterInvalidPermissionRules (settings/validation.ts) wording.
 */
export function filterInvalidPermissionRules(
  rules: unknown,
  key = 'permissions',
): { kept: string[]; warnings: string[] } {
  const kept: string[] = [];
  const warnings: string[] = [];
  if (!Array.isArray(rules)) return { kept, warnings };
  for (const entry of rules) {
    if (typeof entry !== 'string') {
      warnings.push(`Non-string value in ${key} array was removed`);
      continue;
    }
    const result = validatePermissionRule(entry);
    if (result.valid) {
      kept.push(entry);
      continue;
    }
    const suffix = result.suggestion ? `. ${result.suggestion}` : '';
    warnings.push(`Invalid permission rule "${entry}" was skipped: ${result.error}${suffix}`);
  }
  return { kept, warnings };
}

/**
 * Upper bound on the length of a model-supplied plan-mode `allowedPrompts` entry.
 * A legitimate scoped action description / command prefix is short; anything far
 * longer is either junk or an attempt to smuggle a huge rule, so it is refused.
 */
export const MAX_ALLOWED_PROMPT_LENGTH = 1000;

/**
 * Validate a single MODEL-supplied plan-mode `allowedPrompts` entry before it is
 * persisted as an `allow` permission rule on plan-mode exit.
 *
 * Claude's ExitPlanMode tool treats `allowedPrompts` as SCOPED action
 * descriptions (`tool: z.enum(['Bash'])`, e.g. "run tests") — never a match-all
 * glob. AMI stores each entry's text directly as a rule pattern
 * (`addRule({ tool, pattern: prompt, action: 'allow' })`), so without this guard
 * a crafted or prompt-injected entry such as `*`, ``, or `**` would become a
 * blanket `bash(*)` allow-EVERYTHING rule — a privilege-escalation vector that
 * silently auto-approves every subsequent command.
 *
 * Returns `{ safe:false, reason }` for a non-string, an empty/whitespace-only
 * value, a pattern that collapses to "match everything" (only glob
 * metacharacters: `*`, `**`, `.*`, `*:*`, a lone `:*`, `?*`, …), or a value
 * longer than {@link MAX_ALLOWED_PROMPT_LENGTH}; otherwise `{ safe:true }`.
 */
export function validateAllowedPromptRule(prompt: unknown): { safe: boolean; reason?: string } {
  if (typeof prompt !== 'string') return { safe: false, reason: 'prompt must be a string' };
  const trimmed = prompt.trim();
  if (!trimmed) return { safe: false, reason: 'prompt is empty' };
  if (trimmed.length > MAX_ALLOWED_PROMPT_LENGTH) {
    return { safe: false, reason: `prompt exceeds ${MAX_ALLOWED_PROMPT_LENGTH} characters` };
  }
  // A pattern made up ONLY of glob/wildcard metacharacters matches every command
  // (the glob matcher treats `*`/`?`/`.*`/`:*` as "anything"), so it is too broad
  // to auto-allow. Whitespace is irrelevant to the match, so collapse it first.
  const stripped = trimmed.replace(/\s+/g, '');
  if (/^[*?.:]+$/.test(stripped)) {
    return { safe: false, reason: 'pattern matches all commands (too broad to auto-allow)' };
  }
  return { safe: true };
}

/**
 * Partition an array of model-supplied plan-mode `allowedPrompts` entries into the
 * ones safe to persist as `allow` rules and the ones refused by
 * {@link validateAllowedPromptRule}, preserving each entry's full shape (so the
 * caller keeps its `tool`). The rejected list carries the refusal reason so the
 * caller can surface WHICH prompts were dropped and WHY (introspection).
 */
export function partitionAllowedPromptRules<T extends { prompt?: unknown }>(
  entries: readonly T[],
): { valid: T[]; rejected: Array<{ entry: T; reason: string }> } {
  const valid: T[] = [];
  const rejected: Array<{ entry: T; reason: string }> = [];
  for (const entry of entries) {
    const result = validateAllowedPromptRule(entry.prompt);
    if (result.safe) valid.push(entry);
    else rejected.push({ entry, reason: result.reason ?? 'invalid prompt' });
  }
  return { valid, rejected };
}

/**
 * Match a single bash sub-command against a rule pattern.
 *
 * For deny/ask rules we ALSO test a de-obfuscated form (r""m, r''m, "rm", r\m →
 * rm) so a command-word split with empty quotes / backslashes / quotes cannot
 * slip a real command past a user deny or ask rule (S3). Allow rules stay
 * STRICT — they only match the literal stripped form — so de-obfuscation can
 * never *broaden* an allow into auto-approving an obfuscated command.
 */
function matchBashSub(pattern: string, sub: string, ruleAction: 'allow' | 'deny' | 'ask'): boolean {
  // Allow rules strip only SAFE_ENV_VARS (an unsafe env prefix must break the
  // match → prompt); deny/ask rules strip ALL env vars so no prefix can hide a
  // denied command.
  const strippedRaw = stripEnvVarPrefix(sub, /* safeEnvOnly */ ruleAction === 'allow');
  if (matchPattern(pattern, strippedRaw)) return true;
  if (ruleAction === 'allow') return false;
  // Strip env prefix FIRST (handles quoted values like FOO="a b"), THEN collapse
  // obfuscation and drop quote/backtick characters from the remaining command.
  const deob = deobfuscateEnvExpansion(strippedRaw).replace(/["'`]/g, '');
  return deob !== strippedRaw && matchPattern(pattern, deob);
}

function matchBashPattern(pattern: string, command: string, ruleAction: 'allow' | 'deny' | 'ask'): boolean {
  const normalized = normalizeLineContinuations(command);
  const subCommands = splitCompoundCommand(normalized);

  // Fail-closed on compound-command fanout above the cap: an allow rule must NOT
  // auto-approve a pathologically wide chain (it falls through to a prompt).
  // Deny/ask matching deliberately keeps scanning so a denied command cannot be
  // hidden by padding a chain past the cap (CC-643).
  if (ruleAction === 'allow' && subCommands.length > MAX_SUBCOMMANDS_FOR_SECURITY_CHECK) {
    return false;
  }

  let baseMatch: boolean;
  if (subCommands.length <= 1) {
    // Use the single sub-command (which has been unwrapped from control
    // structures) rather than the original normalized string, so that
    // e.g. "for f in *.ts; do cat $f; done" matches a "cat *" rule.
    const effective = subCommands.length === 1 ? subCommands[0] : normalized;
    baseMatch = matchBashSub(pattern, effective, ruleAction);
  } else if (ruleAction === 'allow') {
    baseMatch = subCommands.every(sub => matchBashSub(pattern, sub, 'allow'));
  } else {
    // deny / ask: any sub-command match triggers
    baseMatch = subCommands.some(sub => matchBashSub(pattern, sub, ruleAction));
  }
  if (baseMatch) return true;

  // M1: deny/ask rules ALSO decompose `$(...)` / backtick command substitutions
  // (recursively) and match each extracted inner command, so a denied command
  // cannot hide inside a substitution (e.g. `echo $(rm -rf x)`). Allow matching
  // stays STRICT above — decomposition can only ADD denies, never broaden allows.
  if (ruleAction === 'allow') return false;
  for (const inner of extractCommandSubstitutions(normalized)) {
    for (const sub of splitCompoundCommand(inner)) {
      if (matchBashSub(pattern, sub, ruleAction)) return true;
    }
  }
  return false;
}

/**
 * Build a denial key from a tool name and its input for deduplication.
 */
function denialKey(toolName: string, input: Record<string, unknown>): string {
  const inputStr = typeof input.command === 'string'
    ? input.command
    : typeof input.file_path === 'string'
      ? input.file_path
      : JSON.stringify(input);
  return `${toolName}::${inputStr}`;
}

// ---------------------------------------------------------------------------
// PermissionManager
// ---------------------------------------------------------------------------

export class PermissionManager {
  private mode: PermissionMode = 'ask';
  private rules: PermissionRule[] = [];
  private deniedCommands: Set<string> = new Set();
  private _disableBypass = false;
  /** The permission mode active immediately before plan mode was entered, so a
   *  later plan-mode exit can restore it (Claude's ToolPermissionContext.prePlanMode).
   *  Undefined when not in plan mode or when none was captured. */
  private _prePlanMode?: PermissionMode;
  /** Managed-rules-only lockdown (Claude's `allowManagedPermissionRulesOnly`): when
   *  true, ONLY `policySettings`-sourced rules are honored, runtime non-managed grants
   *  are refused, and nothing may be persisted. Set from the managed permissions file
   *  by {@link loadManagedPermissions} / {@link loadTieredRules}. */
  private _managedRulesOnly = false;
  /** Extra absolute paths/dirs the USER has explicitly granted read access to,
   *  OUTSIDE the launch cwd — e.g. via `/add-dir`, `--context-file`, `--image`, or an
   *  approved out-of-workspace read prompt. The launch cwd is always allowed (passed
   *  per-call to the path checks); everything else outside it is confined unless listed
   *  here. This is what makes "point the agent at /some/other/image.png" work while a
   *  silent `cat ../../other-project/secret` stays blocked. */
  private allowedReadRoots: Set<string> = new Set();

  /** Active bash isolation policy (threaded from the engine's EngineConfig.sandbox).
   *  Consulted ONLY to honor {@link SandboxPolicy.autoAllowBashIfSandboxed}: when the
   *  policy runs every bash command in a container, the container IS the safety
   *  boundary, so the opt-in knob lets `check()` skip the prompt. Undefined → the
   *  feature is inert and bash is gated exactly as before. */
  private sandboxPolicy?: SandboxPolicy;
  /** Resolver injected by the engine that reports whether a NON-bash tool call is
   *  irreversible for this input (parity: Claude's generic `Tool.isDestructive(input)`).
   *  Lets {@link check} apply the destructive allow-rule downgrade to ANY tool, not
   *  just bash. Absent → only bash destructiveness is considered (unchanged). */
  private toolDestructiveResolver?: (toolName: string, input: Record<string, unknown>) => boolean;

  /** Least-privilege capability ceiling for a forked/background sub-agent (parity:
   *  Claude's `createAutoMemCanUseTool`). When set, {@link check} enforces it BEFORE
   *  and INDEPENDENT of the ambient mode — a non-read-only bash command or a write
   *  outside the confined roots is denied even under auto-allow/bypass. Undefined →
   *  inert (gated exactly as before). Set per sub-engine from EngineConfig. */
  private capabilityCeiling?: ToolCapabilityCeiling;

  // Denial circuit breaker — abort after too many consecutive or total denials
  private consecutiveDenials = 0;
  private totalDenials = 0;
  private readonly MAX_CONSECUTIVE = 3;
  private readonly MAX_TOTAL = 20;

  constructor(mode?: PermissionMode, rules?: PermissionRule[], allowedRoots?: string[]) {
    if (mode) this.mode = mode;
    if (rules) this.rules = [...rules];
    if (allowedRoots) for (const r of allowedRoots) this.registerAllowedRoot(r);
  }

  /**
   * Grant read access to an absolute path (file or directory) that lies OUTSIDE the
   * launch cwd. Sources: `/add-dir`, `--context-file`/`--image` launch flags, and
   * user-approved out-of-workspace read prompts. Stored resolved + normalized so the
   * containment check is a simple prefix match. Idempotent; ignores empty input.
   */
  registerAllowedRoot(p: string): void {
    if (!p || typeof p !== 'string') return;
    try {
      const abs = path.isAbsolute(p) ? p : path.resolve(p);
      this.allowedReadRoots.add(path.normalize(abs));
    } catch { /* non-fatal: a bad path just isn't granted */ }
  }

  /**
   * Revoke a previously-granted read root (inverse of {@link registerAllowedRoot}).
   * Normalizes `p` the same way so a path granted via `/add-dir` can be dropped by
   * `/remove-dir`, letting the workspace boundary SHRINK, not just widen. Returns
   * true if a matching root was removed. The launch cwd is granted per-call and is
   * never in this set, so it can never be revoked here.
   */
  removeAllowedRoot(p: string): boolean {
    if (!p || typeof p !== 'string') return false;
    try {
      const abs = path.isAbsolute(p) ? p : path.resolve(p);
      return this.allowedReadRoots.delete(path.normalize(abs));
    } catch {
      return false;
    }
  }

  /** The user-granted read roots (absolute, normalized). For inspection/tests. */
  getAllowedReadRoots(): string[] {
    return [...this.allowedReadRoots];
  }

  /**
   * The file-READ deny GLOBS configured in the permission layer, so listing/search
   * tools (grep, glob) can EXCLUDE files the user forbade reading — the denied file's
   * matching lines (and even its path) never surface in results, in every output mode.
   * Collected from every `deny` rule targeting the read tool (`file_read`, its Claude
   * alias `Read`, or a tool-wide `*`) that carries a path glob; param-shaped rules
   * (`paramPattern`) and allow/ask rules are skipped. De-duped, order-preserving.
   * Mirrors Claude Code's `getFileReadIgnorePatterns(toolPermissionContext)`.
   */
  getFileReadIgnorePatterns(): string[] {
    const out: string[] = [];
    const seen = new Set<string>();
    for (const rule of this.rules) {
      if (rule.action !== 'deny' || !rule.pattern || rule.paramPattern) continue;
      if (!this.toolNameMatches(rule.tool, 'file_read')) continue;
      if (seen.has(rule.pattern)) continue;
      seen.add(rule.pattern);
      out.push(rule.pattern);
    }
    return out;
  }

  /**
   * PARITY (Claude's `/add-dir` → `SandboxManager.refreshConfig()` sync — add-dir.tsx
   * L86-94): project the given base sandbox policy with the CURRENT dynamic
   * `allowedReadRoots` folded into `filesystem.allowWrite`, so a directory widened
   * into the workspace at runtime (via `/add-dir`, `--context-file`/`--image`, or an
   * approved out-of-workspace read) is ALSO mounted into the bash container, not just
   * granted in the in-process permission layer.
   *
   * Without this the two guard layers diverge: the permission surface reports the
   * added directory as allowed while the container sandbox still mounts only
   * `projectDir`, so a sandboxed `bash` silently cannot reach a directory the user
   * believes was added (and `/remove-dir` likewise narrows the mount set again, since
   * the projection is recomputed from the live root set each turn).
   *
   * It ALSO folds the active `web_fetch` domain allow/deny rules into
   * `network.allowedHosts`/`deniedHosts`, so the SAME per-domain allowlist that
   * governs the WebFetch tool also governs in-sandbox outbound traffic (parity:
   * Claude's sandbox-adapter, which derives sandbox network domains from WebFetch
   * permission rules). Without this the sandbox network allowlist is never
   * populated from permissions and a sandboxed `curl example.com` is unrestricted
   * even when the user allowed only `web_fetch(github.com)`.
   *
   * Returns `base` unchanged when there is no policy (bash runs on the host, already
   * covered by the permission layer) or when neither extra roots nor web_fetch
   * domain rules are present — the fold only ever ADDS roots/domains. Caller-supplied
   * `filesystem.allowWrite` and `network` entries are preserved and de-duplicated.
   */
  getEffectiveSandboxPolicy(base: SandboxPolicy | undefined): SandboxPolicy | undefined {
    if (!base) return base;
    const roots = [...this.allowedReadRoots];
    const { allow: fetchAllow, deny: fetchDeny } = this.extractWebFetchSandboxDomains();
    if (roots.length === 0 && fetchAllow.length === 0 && fetchDeny.length === 0) return base;
    let out: SandboxPolicy = base;
    if (roots.length > 0) {
      const existing = base.filesystem?.allowWrite ?? [];
      const merged = [...existing];
      const seen = new Set(existing.map((p) => path.normalize(p)));
      for (const r of roots) {
        const norm = path.normalize(r);
        if (seen.has(norm)) continue;
        seen.add(norm);
        merged.push(r);
      }
      out = { ...out, filesystem: { ...out.filesystem, allowWrite: merged } };
    }
    if (fetchAllow.length > 0 || fetchDeny.length > 0) {
      const net = { ...out.network };
      if (fetchAllow.length > 0) net.allowedHosts = [...new Set([...(net.allowedHosts ?? []), ...fetchAllow])];
      if (fetchDeny.length > 0) net.deniedHosts = [...new Set([...(net.deniedHosts ?? []), ...fetchDeny])];
      out = { ...out, network: net };
    }
    return out;
  }

  /**
   * Collect the domains named by the active `web_fetch` allow/deny rules so they
   * can seed the sandbox outbound network allowlist/denylist. AMI's canonical
   * WebFetch rule content IS the bare domain (`web_fetch(example.com)` /
   * `web_fetch(*.github.com)`; see {@link validatePermissionRule}); a tool-wide
   * rule (no pattern, or `*`) names no specific domain and is skipped. Deny rules
   * feed `deniedHosts` (which wins over `allowedHosts` in the sandbox backend).
   */
  private extractWebFetchSandboxDomains(): { allow: string[]; deny: string[] } {
    const allow: string[] = [];
    const deny: string[] = [];
    for (const rule of this.rules) {
      if (normalizeToolName(rule.tool) !== 'web_fetch') continue;
      const domain = rule.pattern?.trim();
      if (!domain || domain === '*') continue;
      if (rule.action === 'allow') allow.push(domain);
      else if (rule.action === 'deny') deny.push(domain);
    }
    return { allow, deny };
  }

  /**
   * Thread the active bash isolation policy into the permission layer so
   * {@link SandboxPolicy.autoAllowBashIfSandboxed} can be honored in {@link check}.
   * The engine calls this once at construction with its `EngineConfig.sandbox`.
   * Passing `undefined` disables the behavior (bash is gated exactly as before).
   */
  setSandboxPolicy(policy: SandboxPolicy | undefined): void {
    this.sandboxPolicy = policy;
  }

  /**
   * Install a least-privilege {@link ToolCapabilityCeiling} for this manager's
   * engine (parity: Claude's forked-agent `canUseTool`). The ceiling is a HARD cap
   * enforced ahead of every mode: it can only deny (never grant), so a forked skill
   * sub-agent can be confined to read-only bash + writes within a directory even when
   * its ambient mode is auto-allow. The engine sets this per sub-engine from
   * `EngineConfig.capabilityCeiling`. Passing `undefined` disables it.
   */
  setCapabilityCeiling(ceiling: ToolCapabilityCeiling | undefined): void {
    this.capabilityCeiling = ceiling;
  }

  /**
   * Register a resolver that reports whether a tool call is DESTRUCTIVE (irreversible:
   * delete / overwrite / send) for the given input, generalizing the bash
   * destructive-command guard to ANY tool (parity: Claude's `Tool.isDestructive(input)`).
   * When it returns true for a call, an over-broad `allow` rule is downgraded to an
   * explicit prompt in `ask` mode — exactly like a destructive bash command — so a
   * wildcard allow can never silently auto-approve an irreversible operation.
   * The engine injects a resolver backed by each tool's `ToolDefinition.isDestructive`.
   * Passing `undefined` disables the generic guard (bash destructiveness still applies).
   */
  setToolDestructiveResolver(
    fn: ((toolName: string, input: Record<string, unknown>) => boolean) | undefined,
  ): void {
    this.toolDestructiveResolver = fn;
  }

  /**
   * Whether this tool call is destructive: a destructive bash command (keyed off the
   * command classifier) OR any tool the injected {@link setToolDestructiveResolver}
   * resolver flags for this input. Single source of truth for the allow→ask downgrade
   * and its reason attribution, so both stay in lockstep.
   */
  private isDestructiveToolUse(toolName: string, input: Record<string, unknown>): boolean {
    if (toolName === 'bash' && typeof input.command === 'string') {
      return this.classifyBashCommand(input.command) === 'destructive';
    }
    return this.toolDestructiveResolver?.(toolName, input) === true;
  }

  /**
   * Whether a bash command would be auto-approved by the
   * `autoAllowBashIfSandboxed` knob: the policy runs every command in a container
   * (`mode === 'container'`) AND the opt-in knob is explicitly enabled. This is an
   * OPT-IN parity knob (Claude's enterprise `sandbox.autoAllowBashIfSandboxed`): it
   * is OFF unless set to exactly `true`, so the default bash prompt is unchanged.
   * The container is the safety boundary (the host FS is not mounted), so a prompt
   * adds no protection — only friction — once the operator opts in.
   */
  private bashAutoAllowedBySandbox(toolName: string, input: Record<string, unknown>): boolean {
    return (
      toolName === 'bash' &&
      typeof input.command === 'string' &&
      this.sandboxPolicy?.mode === 'container' &&
      this.sandboxPolicy.autoAllowBashIfSandboxed === true &&
      // dangerouslyDisableSandbox cannot override enforced container mode, but if the
      // model asked for it we still prompt (handled earlier in check); the escape
      // hatch never silently auto-allows here.
      input.dangerouslyDisableSandbox !== true
    );
  }

  // -------------------------------------------------------------------------
  // Auto-mode / classifier denial store (recency-ordered, bounded, displayable)
  // -------------------------------------------------------------------------

  private _recentDenials: RecentDenial[] = [];
  private readonly MAX_RECENT_DENIALS = 100;

  /** Recency-ordered (newest-first) snapshot of recorded denials. */
  getRecentDenials(): readonly RecentDenial[] {
    return this._recentDenials;
  }

  /** Clear the recorded-denial log (but not the circuit-breaker counters). */
  clearRecentDenials(): void {
    this._recentDenials = [];
  }

  // -------------------------------------------------------------------------
  // Core permission check
  // -------------------------------------------------------------------------

  /**
   * Sensitive-path safety check shared by the blanket-allow modes
   * (`auto-allow`/yolo and `bypassPermissions`). Returns true when the invocation
   * touches a credential path, a system directory, or a sensitive config path and
   * must therefore still prompt rather than be silently auto-allowed. Mirrors
   * Claude's safetyCheck, which fires in every mode.
   */
  private sensitivePathRequiresPrompt(
    toolName: string,
    input: Record<string, unknown>,
  ): boolean {
    if (toolName === 'bash' && typeof input.command === 'string') {
      if (commandTargetsCredentialPath(input.command)) return true;
      if (commandTargetsSystemDir(input.command)) return true;
      // A catastrophic rm/rmdir target (root, home, any direct child of root, a
      // drive root, a `/*` wildcard) must confirm even under a blanket auto-allow.
      if (commandHasDangerousRemoval(input.command)) return true;
    }
    if (toolName === 'bash' || toolName === 'file_edit' || toolName === 'file_write') {
      const pathArg = (input.file_path || input.command || '');
      if (typeof pathArg === 'string') {
        // Case-fold on case-insensitive filesystems so a `.SSH/`, `.Ami/`, or
        // `.AWS/` reference still matches the lowercase safety literals and cannot
        // fail-open past this blanket-allow guard (gap U003).
        const foldedArg = normalizeCaseForComparison(pathArg);
        for (const sp of SENSITIVE_SAFETY_PATHS) {
          if (foldedArg.includes(normalizeCaseForComparison(sp))) return true;
        }
      }
    }
    return false;
  }

  /**
   * Check whether a tool invocation is allowed, denied, or needs user input.
   */
  async check(
    toolName: string,
    input: Record<string, unknown>,
  ): Promise<'allow' | 'deny' | 'ask'> {
    // Hardline safety — unconditional, overrides all modes including auto-allow
    if (toolName === 'bash' && typeof input.command === 'string') {
      const hardline = detectHardlineCommand(input.command);
      if (hardline.blocked) return 'deny';

      // Disguised-bare-repo guard: running git from a cwd that has been made to look
      // like a bare git repo makes git treat the cwd as the git dir and execute
      // attacker-planted hooks (sandbox escape). Force an explicit prompt for any
      // git invocation from such a cwd — even under auto-allow/bypass — so the hook
      // execution can never be silently auto-allowed. deny-all still denies below.
      if (this.mode !== 'deny-all' &&
          commandInvokesGit(input.command) &&
          isDisguisedBareGitRepo()) {
        return 'ask';
      }
    }

    // Least-privilege capability ceiling (parity: Claude's forked-agent canUseTool).
    // A HARD cap on a forked/background sub-agent — enforced ahead of every mode, so a
    // non-read-only bash command or a write outside the confined roots is denied even
    // under auto-allow/bypass. Inert (never set) for the main engine.
    if (this.capabilityCeiling) {
      const denial = evaluateToolCeiling(
        this.capabilityCeiling,
        toolName,
        input,
        isReadOnlyBashCommand,
      );
      if (denial) return 'deny';
    }

    // dangerouslyDisableSandbox always requires explicit confirmation
    if (toolName === 'bash' && input.dangerouslyDisableSandbox === true) {
      if (this.mode === 'deny-all') return 'deny';
      return 'ask';
    }

    // autoAllowBashIfSandboxed (Claude-parity, OPT-IN): when the policy runs every
    // bash command inside a container, the container — not the prompt — is the
    // safety boundary (the host filesystem is not mounted), so an operator who set
    // sandbox.autoAllowBashIfSandboxed=true wants bash to run without a prompt in
    // the normally prompt-driven modes (ask / acceptEdits). Hardline blocks and
    // dangerouslyDisableSandbox (both above) and explicit user deny rules still win;
    // deny-all/plan keep denying and auto-allow/bypass already allow, so this only
    // changes behavior in ask/acceptEdits.
    if ((this.mode === 'ask' || this.mode === 'acceptEdits') &&
        this.bashAutoAllowedBySandbox(toolName, input) &&
        !this.anyDenyMatches(toolName, input)) {
      return 'allow';
    }

    // Fast path: mode-level overrides — but in auto-allow mode, explicit
    // deny rules still fire (Gap #32). M9: a matching deny UNCONDITIONALLY wins
    // over the auto-allow, regardless of rule order (an earlier allow can no
    // longer shadow a later deny). paramPattern rules stay skipped on this
    // fast-path (unchanged behavior); everything else is auto-allowed.
    if (this.mode === 'auto-allow') {
      if (this.anyDenyMatches(toolName, input, /* skipParamPattern */ true)) return 'deny';
      // Sensitive-path safety still fires even under yolo/auto-allow: a bash
      // credential read/exfil or system-dir write, or an edit to a sensitive
      // config path, must still prompt — mirroring Claude's safetyCheck (which
      // is applied in every mode) and the bypassPermissions handling below. This
      // closes the asymmetry where `cat ~/.ssh/id_rsa` was auto-allowed in yolo
      // even though file_read of the same path is forbidden.
      if (this.sensitivePathRequiresPrompt(toolName, input)) return 'ask';
      return 'allow';
    }
    if (this.mode === 'deny-all') return 'deny';

    if (this.mode === 'bypassPermissions') {
      // Explicit user deny rules are BYPASS-IMMUNE: a `{tool, action:'deny'}` rule
      // fires even in bypassPermissions mode, before the blanket auto-allow — so a
      // user's hard stop is never silently overridden (Claude orders deny/safety
      // checks before the bypass allow, permissions.ts L1169-1281).
      if (this.anyDenyMatches(toolName, input)) return 'deny';
      // User-configured content-specific `ask` rules are ALSO bypass-immune: a
      // `{tool, action:'ask'}` rule still prompts, before the blanket allow, so an
      // explicit "always confirm this" is never silently overridden by bypass mode.
      if (this.anyAskMatches(toolName, input)) return 'ask';
      if (this.sensitivePathRequiresPrompt(toolName, input)) return 'ask';
      return 'allow';
    }

    if (this.mode === 'plan') {
      const readOnlyTools = new Set(['file_read', 'grep', 'glob', 'list_dir', 'web_fetch', 'web_search', 'search_symbols', 'tool_search', 'task_tracker', 'task_list', 'task_output', 'todo_read']);
      if (readOnlyTools.has(toolName)) return 'allow';
      return 'deny';
    }

    // acceptEdits: auto-allow file-mutating tools and safe bash
    if (this.mode === 'acceptEdits') {
      const editTools = new Set(['file_edit', 'file_write', 'notebook_edit', 'multi_edit']);
      if (editTools.has(toolName)) {
        // Build-config files can be weaponized for supply-chain attacks —
        // require explicit approval even in acceptEdits mode.
        const filePath = (input.file_path || input.path || '') as string;
        if (filePath && isBuildConfigFile(filePath)) return 'ask';
        // The agent's OWN config/settings/IDE/git dirs are a self-reconfig /
        // prompt-injection vector — never silently auto-edit them.
        if (filePath && isAgentConfigPath(filePath)) return 'ask';
        // A path-canonicalization bypass (ADS, 8.3 short name, trailing dot,
        // UNC, triple-dot, DOS device) could sneak past the basename guards above.
        if (filePath && hasSuspiciousPathPattern(filePath)) return 'ask';
        return 'allow';
      }
      // A compound TOCTOU shape (archive-before-git, symlink, or cwd-desync) must
      // never be silently auto-allowed even when its segments individually classify
      // 'safe' / edit-adjacent — a `cd ~/.ssh && cat id_rsa` reads 'safe' (cat) but
      // resolves the relative operand against the just-entered secret dir. Skip the
      // acceptEdits early-allow for those and fall through to the shared toctou guard
      // below, which forces an explicit ask.
      if (toolName === 'bash' && typeof input.command === 'string' &&
          this.bashToctouEscalation(input.command) === null) {
        const classification = this.classifyBashCommand(input.command);
        if (classification === 'safe') return 'allow';
        // acceptEdits also auto-allows benign edit-adjacent filesystem mutations
        // (mkdir/touch/mv/cp/sed) — Claude's ACCEPT_EDITS_ALLOWED_COMMANDS — but
        // only when the command is not destructive (rm/rmdir stay gated) and does
        // not touch a credential or system path (those keep their explicit prompt).
        if (classification !== 'destructive' &&
            isAcceptEditsFilesystemMutation(input.command) &&
            !commandTargetsCredentialPath(input.command) &&
            !commandTargetsSystemDir(input.command)) {
          return 'allow';
        }
      }
    }

    // Check if this exact invocation was already denied
    const key = denialKey(toolName, input);
    if (this.deniedCommands.has(key)) return 'deny';

    // Determine if this is a destructive tool call (used to guard allow rules
    // from auto-approving irreversible operations via wildcards). Covers a
    // destructive bash command AND any tool whose injected isDestructive resolver
    // flags this input (parity: Claude's generic Tool.isDestructive(input)).
    const isDestructiveToolCall = this.mode === 'ask' && this.isDestructiveToolUse(toolName, input);

    // Code-execution allow-rule downgrade: a matched over-broad interpreter /
    // package-runner / shell allow rule (e.g. Bash(python:*), Bash(npx:*),
    // Bash(bash:*)) grants arbitrary code execution, bypassing the command
    // classifier. Unlike isDestructiveBash (which keys off the COMMAND's
    // classification and fires only in 'ask'), this keys off the matched RULE's
    // shape and fires in BOTH rule-evaluated modes ('ask' and 'acceptEdits'), so
    // such a rule is downgraded to an explicit prompt instead of silently
    // auto-approving arbitrary code. Mirrors Claude's dangerous-permission strip.
    const codeExecDowngradeMode = (this.mode === 'ask' || this.mode === 'acceptEdits') &&
      toolName === 'bash' && typeof input.command === 'string';

    // Credential/secret access always confirms in rule-evaluated modes (ask and
    // acceptEdits), even under a wildcard allow — unless there is an explicit
    // deny rule (which must still win below).
    if ((this.mode === 'ask' || this.mode === 'acceptEdits') &&
        toolName === 'bash' && typeof input.command === 'string' &&
        commandTargetsCredentialPath(input.command) && !this.hasExplicitDeny(toolName, input)) {
      return 'ask';
    }
    // System directory write guard — commands that write to /etc, /usr, /boot,
    // etc. always require explicit approval, even under a wildcard allow rule.
    if ((this.mode === 'ask' || this.mode === 'acceptEdits') &&
        toolName === 'bash' && typeof input.command === 'string' &&
        commandTargetsSystemDir(input.command)) {
      return 'ask';
    }
    // Dangerous-removal guard — an rm/rmdir targeting a catastrophic path (root,
    // home, any direct child of root, a drive root, a `/*` wildcard) ALWAYS
    // confirms explicitly and can never be auto-approved by a wildcard allow rule.
    // An explicit deny rule still wins (resolved below), so it is excluded here.
    // Mirrors Claude's checkDangerousRemovalPaths (tools/BashTool/pathValidation.ts).
    if ((this.mode === 'ask' || this.mode === 'acceptEdits') &&
        toolName === 'bash' && typeof input.command === 'string' &&
        commandHasDangerousRemoval(input.command) && !this.hasExplicitDeny(toolName, input)) {
      return 'ask';
    }

    // Compound-command TOCTOU guard — an archive-extraction-before-git compound
    // or a symlink-creating compound always confirms in rule-evaluated modes
    // (ask/acceptEdits), even under a wildcard allow rule, because the auto-allow
    // paths cannot see the runtime repo/path redirection those shapes introduce
    // (the archive is unpacked, or the link is created, AFTER the permission
    // decision and BEFORE the later command runs). Mirrors Claude's
    // GIT_SAFETY_ARCHIVE_EXTRACTORS + hasSymlinkCreate gates. An explicit deny
    // still wins via the unconditional deny scan below.
    if ((this.mode === 'ask' || this.mode === 'acceptEdits') &&
        toolName === 'bash' && typeof input.command === 'string' &&
        this.bashToctouEscalation(input.command) !== null &&
        !this.hasExplicitDeny(toolName, input)) {
      return 'ask';
    }

    // Policy memo: return cached decision for this exact tool+input if available
    if (this._memo) {
      const inputHash = this._hashInput(input);
      const cached = this._memo.get(toolName, inputHash);
      if (cached !== null) return cached;
    }

    // M9: a matching deny rule UNCONDITIONALLY wins over any allow/ask, regardless
    // of rule order. Scan the whole rule list for a deny first; only if none matches
    // do allow/ask rules get evaluated in order (first match wins) below.
    if (this.anyDenyMatches(toolName, input)) {
      if (this._memo) this._memo.set(toolName, this._hashInput(input), 'deny');
      return 'deny';
    }

    // Evaluate allow/ask rules in order (first match wins). Deny rules were already
    // resolved above and unconditionally win, so they are skipped here.
    for (const rule of this.rules) {
      if (rule.action === 'deny') continue;
      const toolMatches = this.toolNameMatches(rule.tool, toolName);
      if (!toolMatches) continue;
      // Tool(param:value) matching — all named params must match their globs.
      if (rule.paramPattern && !this.paramMatches(rule.paramPattern, input)) continue;

      if (rule.pattern) {
        const inputStr = this.inputToString(toolName, input);
        const matched = toolName === 'bash' && typeof input.command === 'string'
          ? matchBashPattern(rule.pattern, inputStr, rule.action)
          : matchPattern(rule.pattern, inputStr);
        if (matched) {
          const effective = (rule.action === 'allow' &&
            (isDestructiveToolCall || (codeExecDowngradeMode && this.isDangerousCodeExecAllow(rule)))) ? 'ask' : rule.action;
          if (this._memo) this._memo.set(toolName, this._hashInput(input), effective);
          return effective;
        }
      } else {
        // No pattern -- rule applies to the whole tool (param match already gated above).
        // A content-less (tool-wide) rule is NOT a code-exec prefix, so only the
        // destructive-command downgrade applies here.
        const effective = (rule.action === 'allow' && isDestructiveToolCall) ? 'ask' : rule.action;
        if (this._memo) this._memo.set(toolName, this._hashInput(input), effective);
        return effective;
      }
    }

    // Default: 'ask' mode means non-read-only tools require a prompt
    if (this._memo) this._memo.set(toolName, this._hashInput(input), 'ask');
    return 'ask';
  }

  /**
   * Like {@link check} but also returns a STRUCTURED reason explaining the verdict,
   * so a UI can tell the user WHY approval is required (or why it was denied)
   * instead of a bare "needs approval". Reuses check() for the decision (single
   * source of truth — no parallel decision path) and derives the reason from the
   * same guard predicates. `reason`/`message` are undefined for an `allow`.
   */
  async checkWithReason(
    toolName: string,
    input: Record<string, unknown>,
  ): Promise<{
    decision: 'allow' | 'deny' | 'ask';
    reason?: PermissionDecisionReason;
    message?: string;
    /**
     * Pre-baked "always allow X" rule candidates ({@link buildPermissionSuggestions})
     * a prompt can offer for one-click acceptance (Claude's
     * `permissionResult.suggestions`). Omitted when nothing safe can be suggested.
     */
    suggestions?: PermissionUpdate[];
    /**
     * A hard, turn-aborting denial: when set the whole agentic turn should be
     * interrupted, not just the single tool call blocked. Set for `hardline`
     * (blocked-command) denials. Mirrors Claude's deny-branch `interrupt` flag
     * (PermissionResultSchema). Omitted otherwise.
     */
    interrupt?: boolean;
  }> {
    const decision = await this.check(toolName, input);
    if (decision === 'allow') return { decision };
    const reason = this.deriveDecisionReason(toolName, input, decision);
    const suggestions = buildPermissionSuggestions(toolName, input);
    return {
      decision,
      reason,
      message: createPermissionRequestMessage(reason),
      ...(suggestions.length > 0 ? { suggestions } : {}),
      ...(decision === 'deny' && reason.type === 'hardline' ? { interrupt: true } : {}),
    };
  }

  /** Best-effort attribution of a deny/ask verdict to a structured reason. */
  private deriveDecisionReason(
    toolName: string,
    input: Record<string, unknown>,
    decision: 'deny' | 'ask',
  ): PermissionDecisionReason {
    const command = typeof input.command === 'string' ? input.command : undefined;
    if (toolName === 'bash' && command) {
      const hardline = detectHardlineCommand(command);
      if (hardline.blocked) return { type: 'hardline', description: hardline.description ?? 'blocked command' };
    }
    // Sandbox opt-out: a bash call that set `dangerouslyDisableSandbox` is gated
    // because it wants to run UNSANDBOXED, not because of the mode — attribute the
    // ask to `sandboxOverride` so the prompt explains it needs to bypass the sandbox
    // (parity with Claude's sandboxOverride decisionReason). Only on an `ask`; a
    // deny-all deny stays attributed to the mode below.
    if (decision === 'ask' && toolName === 'bash' && input.dangerouslyDisableSandbox === true) {
      return { type: 'sandboxOverride' };
    }
    // An explicit matching rule.
    for (const rule of this.rules) {
      if (!this.toolNameMatches(rule.tool, toolName)) continue;
      if (rule.paramPattern && !this.paramMatches(rule.paramPattern, input)) continue;
      const matches = rule.pattern
        ? (toolName === 'bash' && command
            ? matchBashPattern(rule.pattern, command, rule.action)
            : matchPattern(rule.pattern, this.inputToString(toolName, input)))
        : true;
      if (matches && (rule.action === decision)) {
        return {
          type: 'rule',
          action: rule.action,
          ruleValue: rule.pattern ? `${rule.tool}(${rule.pattern})` : rule.tool,
          source: rule.reason,
          // Machine-readable provenance so a managed (policySettings) rule can
          // suppress the "update your rules" remediation hint downstream.
          ...(rule.source ? { ruleSource: rule.source } : {}),
        };
      }
    }
    // Safety checks (credential / system-dir / suspicious-path / config files).
    if (toolName === 'bash' && command) {
      if (commandTargetsCredentialPath(command)) {
        return { type: 'safetyCheck', check: 'credential', message: 'Command accesses a credential/secret path and requires approval' };
      }
      if (commandTargetsSystemDir(command)) {
        return { type: 'safetyCheck', check: 'systemDir', message: 'Command writes to a system directory and requires approval' };
      }
      if (this.classifyBashCommand(command) === 'destructive') {
        // Surface the SPECIFIC human-readable warning (parity with Claude's
        // getDestructiveCommandWarning) when the command matches a known signature,
        // so the prompt says WHY it is destructive; else a generic line.
        const warning = getDestructiveCommandWarning(command);
        return {
          type: 'safetyCheck', check: 'destructive',
          message: warning ? `${warning} — approval required` : 'Command is destructive and requires approval',
        };
      }
      const toctou = this.bashToctouEscalation(command);
      if (toctou !== null) {
        return { type: 'safetyCheck', check: 'toctou', message: `Command requires approval: ${toctou}` };
      }
      // Code-execution allow-rule downgrade: the ask verdict came from a matched
      // over-broad interpreter / package-runner / shell allow rule, not the mode.
      if (decision === 'ask' && (this.mode === 'ask' || this.mode === 'acceptEdits')) {
        const codeExecRule = this.matchingCodeExecAllowRule(input, command);
        if (codeExecRule) {
          return {
            type: 'safetyCheck', check: 'codeExec',
            message: `Allow rule "${codeExecRule.tool}(${codeExecRule.pattern})" grants arbitrary code execution and requires approval`,
          };
        }
      }
    }
    // Generic destructive-tool downgrade: a non-bash tool whose isDestructive
    // resolver flagged this input had its matched `allow` rule downgraded to an
    // `ask` in {@link check}. Attribute that prompt to the destructive safetyCheck
    // (same reason code as a destructive bash command) rather than the bare mode.
    if (decision === 'ask' && this.mode === 'ask' && toolName !== 'bash' &&
        this.toolDestructiveResolver?.(toolName, input) === true) {
      return { type: 'safetyCheck', check: 'destructive', message: 'Tool call is destructive and requires approval' };
    }
    const filePath = (input.file_path || input.path) as string | undefined;
    if (filePath) {
      if (isBuildConfigFile(filePath)) return { type: 'safetyCheck', check: 'buildConfig', message: 'Edit targets a build/CI config file and requires approval' };
      if (isAgentConfigPath(filePath)) return { type: 'safetyCheck', check: 'agentConfig', message: "Edit targets the agent's own config and requires approval" };
      if (hasSuspiciousPathPattern(filePath)) return { type: 'safetyCheck', check: 'suspiciousPath', message: 'Path uses a canonicalization-bypass pattern and requires approval' };
      // Out-of-workspace target: the path is outside the launch cwd and every
      // user-granted read root, so it needs explicit consent regardless of the
      // mode — attribute the ask to `workingDir` (parity with Claude's workingDir
      // decisionReason) so the prompt can explain WHERE the boundary is and offer
      // /add-dir, instead of a generic "mode requires approval".
      if (decision === 'ask' && this.classifyReadAccess(filePath, process.cwd()) === 'prompt') {
        return {
          type: 'workingDir',
          path: filePath,
          reason: `"${filePath}" is outside the workspace and requires approval to access`,
        };
      }
    }
    // Fall back to the mode.
    return { type: 'mode', mode: this.mode };
  }

  // -------------------------------------------------------------------------
  // Per-tool deny rules
  // -------------------------------------------------------------------------

  /**
   * Check deny rules for a specific tool and input combination.
   * Returns denied: true if any deny rule with a matching pattern fires.
   */
  checkDenyRules(toolName: string, input: string): DenyCheckResult {
    for (const rule of this.rules) {
      // paramPattern rules match structured named params, which this string-only
      // check cannot evaluate — skip them (they are enforced by check()).
      if (rule.paramPattern) continue;
      if (rule.action === 'deny' && this.toolNameMatches(rule.tool, toolName)) {
        if (!rule.pattern) {
          return { denied: true, reason: rule.reason || `Denied by rule for ${rule.tool}` };
        }
        const matched = toolName === 'bash'
          ? matchBashPattern(rule.pattern, input, 'deny')
          : matchPattern(rule.pattern, input);
        if (matched) {
          return { denied: true, reason: rule.reason || `Denied by rule for ${rule.tool}` };
        }
      }
    }
    return { denied: false };
  }

  // -------------------------------------------------------------------------
  // Bash command classification
  // -------------------------------------------------------------------------

  /**
   * Classify a bash command as safe, unsafe, or destructive.
   */
  /**
   * Single source of truth for the compound-command TOCTOU escalation reason:
   * returns a human-readable reason string when a bash command extracts an
   * archive before running git, or creates a filesystem link inside a compound —
   * both of which must require explicit approval even under an allow rule — or
   * `null` when neither shape is present. Used by {@link check} (to force `ask`)
   * and {@link deriveDecisionReason} (to explain WHY), so the predicate lives in
   * exactly one place.
   */
  private bashToctouEscalation(command: string): string | null {
    const archive = detectArchiveExtractionWithGit(command);
    if (archive.escalate) return archive.reason ?? 'archive-extraction TOCTOU';
    const symlink = detectSymlinkCreateCompound(command);
    if (symlink.escalate) return symlink.reason ?? 'symlink-creation TOCTOU';
    const cdDesync = detectCdSensitiveDesync(command);
    if (cdDesync.escalate) return cdDesync.reason ?? 'cwd-desync TOCTOU';
    return null;
  }

  classifyBashCommand(command: string): BashClassification {
    const result = this.classifyBashCommandCore(command);
    // Any command that touches a credential/secret path — even a read like
    // `cat ~/.ssh/id_rsa` — must never be auto-'safe'; elevate to a prompt.
    // Never downgrade a stronger verdict (destructive/unsafe) here.
    if (result === 'safe' && commandTargetsCredentialPath(command.trim())) return 'unsafe';
    return result;
  }

  private classifyBashCommandCore(command: string): BashClassification {
    // Strip shell comment lines — they are no-ops in bash
    const stripped = command.split('\n').filter(l => !l.trim().startsWith('#')).join('\n');
    const trimmed = stripped.trim();
    if (!trimmed) return 'safe';

    // Unwrap shell control structures so classification sees the body
    // commands, not the loop/if keywords. The original trimmed is kept
    // for regex checks that need the full command syntax (like rm -rf).
    const unwrapped = unwrapShellControlStructures(trimmed);

    // Fail-closed on pathologically wide compound commands: above the cap we stop
    // analyzing (each segment would run the full validator set) and treat the
    // command as unsafe so it prompts rather than being proven safe (CC-643).
    if (splitCompoundCommand(trimmed).length > MAX_SUBCOMMANDS_FOR_SECURITY_CHECK) {
      return 'unsafe';
    }

    const bases = extractBaseCommands(trimmed);

    // Check destructive first (highest severity wins)
    for (const cmd of bases) {
      if (DESTRUCTIVE_COMMANDS.has(cmd)) return 'destructive';
    }

    // Check destructive SQL patterns on the entire command string
    for (const pattern of DESTRUCTIVE_SQL_PATTERNS) {
      if (pattern.test(trimmed)) return 'destructive';
    }

    // Check if "rm" with -rf or -r flags
    if (/\brm\s+.*-[^\s]*r/i.test(trimmed) || /\brm\s+.*-[^\s]*f/i.test(trimmed)) {
      return 'destructive';
    }

    // PowerShell destructive cmdlets (AMI runs PowerShell on Windows). Power-control
    // / disk-format variants are blocked unconditionally by HARDLINE above; these
    // are the recursive/forced deletions that need an explicit-approval prompt.
    if (/\bRemove-Item\b[^\n]*-(Recurse|Force)\b/i.test(trimmed)) return 'destructive';
    if (/\bClear-Content\b/i.test(trimmed)) return 'destructive';

    // Destructive git operations — discard local changes irreversibly
    if (/\bgit\s+(checkout|restore)\s+.*--\s*\./.test(trimmed)) return 'destructive';
    if (/\bgit\s+restore\s+\./.test(trimmed)) return 'destructive';
    if (/\bgit\s+stash\s+drop\b/.test(trimmed)) return 'destructive';
    if (/\bgit\s+stash\s+clear\b/.test(trimmed)) return 'destructive';
    if (/\bgit\s+reset\s+--merge\b/.test(trimmed)) return 'destructive';
    if (/\bgit\s+reset\s+--keep\b/.test(trimmed)) return 'destructive';
    // A force push can overwrite remote history on ANY branch (not only protected
    // ones), so it is a distinct destructive boundary above a normal `git push`
    // (Claude escalates --force/--force-with-lease/-f). A bare `git push` stays
    // merely 'unsafe' below via UNSAFE_COMMANDS.
    if (/\bgit\s+push\b[^\n]*?(?:--force\b|--force-with-lease\b|\s-[A-Za-z]*f\b)/.test(trimmed)) return 'destructive';
    // `git branch -D` / `--delete --force` force-deletes a branch even when it is
    // unmerged — irreversible local work loss. A plain `-d` refuses to drop an
    // unmerged branch and stays 'unsafe' (via the git read-only guard).
    if (/\bgit\s+branch\b[^\n]*(?:\s-[A-Za-z]*D\b|--delete\b[^\n]*--force\b|--force\b[^\n]*--delete\b)/.test(trimmed)) return 'destructive';
    // `kubectl delete` removes live cluster resources irreversibly. The `delete`
    // verb may follow global flags (`kubectl -n ns delete …`); a single bounded
    // character-class scan (no nested quantifier) stays within one segment and
    // requires `delete` to be its own whitespace-led token so a flag VALUE like
    // `-l app=delete` does not false-trigger.
    if (/\bkubectl\b[^;|&\n]*\sdelete\b/.test(trimmed)) return 'destructive';

    // Unsafe help/man/git-help/info forms execute arbitrary code: `man -P <cmd>`
    // (pager exec), `man -l`/`--html`/`--pager=`, `git help --web` (browser), and
    // `info --subnodes | …`. These must never be auto-approved even when man/help
    // sit in SAFE_COMMANDS or an allow rule — man and help are classified by their
    // literal command name, and `help` is often aliased to `man`. Wires the
    // bash-security detector into the classifier (Claude's man/help safeFlags).
    if (detectUnsafeHelpCommand(trimmed)) return 'unsafe';

    // A `cd`/`Set-Location` followed by a file-writing redirection resolves the
    // target against the NEW cwd at runtime — a path-desync the validator can't see.
    // Require manual approval (Claude's cd+write guard).
    if (commandHasCdWriteDesync(trimmed)) return 'unsafe';

    // More than one directory change in a single command defeats cwd-relative path
    // validation (the validator can't track which cwd each later statement runs in).
    // Require approval (Claude's multi-cd guard).
    if (countDirectoryChanges(trimmed) > 1) return 'unsafe';

    // xargs anywhere in the command (even behind a pipe like `ls | xargs …`) must
    // target a read-only command. Validate it up-front so a safe leading command
    // can't short-circuit the classifier to 'safe' before the xargs target is
    // checked (parser-differential RCE: `ls | xargs -I echo ./evil.sh`).
    if (bases.includes('xargs') && !isXargsSafe(unwrapped)) return 'unsafe';

    // Flag-aware checks for commands that are safe/destructive depending on flags
    if (/\bsed\s+.*-[^\s]*i/.test(trimmed)) return 'unsafe'; // sed -i modifies files in place
    if (/\bfind\s+.*-delete\b/.test(trimmed)) return 'destructive';
    if (/\bfind\s+.*-(?:exec|ok)(dir)?\s+rm\b/.test(trimmed)) return 'destructive';
    if (/\bxargs\s+.*\brm\b/.test(trimmed)) return 'destructive';
    if (/\bxargs\s+.*\bmv\b/.test(trimmed)) return 'unsafe';
    if (/\bmake\s+(clean|install|distclean|uninstall)\b/.test(trimmed)) return 'unsafe';

    // Script one-liner safety: python3 -c, node -e, ruby -e, perl -e
    // Must come before UNSAFE_COMMANDS so safe one-liners aren't blocked
    const scriptMatch = trimmed.match(/^(python3?|node|ruby|perl)\s+(-[ce])\s+['"]?(.*?)['"]?\s*$/);
    if (scriptMatch) {
      const [, lang, , code] = scriptMatch;
      if (isScriptOneLinerSafe(lang!, code!)) return 'safe';
      return 'unsafe';
    }

    // Check unsafe
    for (const cmd of bases) {
      if (UNSAFE_COMMANDS.has(cmd)) return 'unsafe';
    }

    // Docker: only read-only subcommands are safe. Reject ANY `$`-substitution
    // first — the validator sees the literal `$VAR`/`$(cmd)` text but the shell
    // expands it at runtime, a parser differential that can smuggle a non-read
    // subcommand or secret past this check (Claude's isDockerSafe `$` rejection).
    if (/\bdocker\s+/.test(trimmed)) {
      if (containsShellSubstitution(unwrapped)) return 'unsafe';
      // Skip leading global flags (and the values they consume, e.g. `-H <host>`,
      // `--context <ctx>`) to reach the REAL subcommand, so `docker -D ps` reads
      // `ps` instead of `-D`. Mirrors Claude's spec-driven flag-value skipping.
      const dockerTokens = trimmed.split(/\s+/).filter(Boolean);
      const dIdx = dockerTokens.indexOf('docker');
      const subIdx = dIdx >= 0 ? skipGlobalFlags(dockerTokens, dIdx + 1, BASH_COMMAND_SPECS['docker']) : -1;
      const dockerSub = subIdx >= 0 && subIdx < dockerTokens.length ? dockerTokens[subIdx] : undefined;
      if (dockerSub && DOCKER_SAFE_SUBCOMMANDS.has(dockerSub)) {
        return 'safe';
      }
      return 'unsafe';
    }

    // Check safe prefixes (e.g., "git status", "npm test")
    for (const cmd of bases) {
      if (SAFE_PREFIXES.has(cmd)) {
        // A git read-only prefix can still smuggle a write (ref creation via
        // `git branch/tag <name>`, remote mutation, `--output` file write) or a
        // sandbox escape (`cd /attacker && git ...` runs planted .git/hooks).
        // Validate the full command before auto-allowing.
        if (cmd.startsWith('git ') && !isGitReadOnlyCommandSafe(unwrapped)) return 'unsafe';
        // Reject `$`-substitution in ANY auto-allowed compound/external read
        // command (git, gh, npm, go, cargo, make, …) — the validator sees the
        // literal `$VAR`/`$(cmd)` text but the shell expands it at runtime, a
        // parser differential that can smuggle a non-read subcommand or leak a
        // secret via an error message past this check. Mirrors Claude's blanket
        // `$`-rejection on auto-allowed external commands (isGitSafe/isDockerSafe
        // and the PowerShell readOnlyValidation variable-expansion guard).
        if (containsShellSubstitution(unwrapped)) return 'unsafe';
        if (allSegmentsKnown(unwrapped)) return 'safe';
        break;
      }
    }

    // Check single-word safe commands with flag validation
    for (const cmd of bases) {
      if (SAFE_COMMANDS.has(cmd)) {
        const tokens = unwrapped.split(/\s+/);
        const cmdIdx = tokens.indexOf(cmd);
        const args = cmdIdx >= 0 ? tokens.slice(cmdIdx + 1) : [];

        if (!validateCommandFlags(cmd, args)) return 'unsafe';

        // `date`: a bare positional like `010203042024` SETS the system clock.
        // Only a format string (leading '+') is read-only — any other positional
        // (or -s/--set/-f) must prompt. Mirrors Claude's date dangerous-callback.
        if (cmd === 'date') {
          for (const a of args) {
            if (a.startsWith('-')) {
              if (/^(-s|--set|-f)$/.test(a)) return 'unsafe';
              continue;
            }
            const bare = a.replace(/^["']/, ''); // tolerate a quoted "+..." format
            if (!bare.startsWith('+')) return 'unsafe'; // clock-setting positional
          }
        }

        // jq/yq: additional dangerous-construct check
        if ((cmd === 'jq' || cmd === 'yq') && isJqDangerous(args)) return 'unsafe';

        // `hostname`: a bare positional SETS the system hostname (`hostname newname`).
        // Only flag-driven reads (`hostname -f`, `-i`, …) are safe. Mirrors Claude's
        // hostname positional-argument guard.
        if (cmd === 'hostname') {
          for (const a of args) {
            if (!a.startsWith('-')) return 'unsafe'; // a positional sets the hostname
          }
        }

        // `tput`: capability names are positionals. Only read-only capabilities are
        // safe; the dangerous ones (reset/init/clear/mc5/pfloc/… and the `-S` stdin
        // batch flag) alter terminal state or run arbitrary sequences. Strict: any
        // capability not on the allowlist prompts. Mirrors Claude's tput guard.
        if (cmd === 'tput') {
          const allowedCaps = SAFE_COMMAND_FLAGS['tput']!.allowed;
          for (const a of args) {
            if (a === '-S') return 'unsafe'; // batch-from-stdin: unbounded capabilities
            if (a.startsWith('-')) continue; // `-T<term>` etc. handled by flag validation
            if (!allowedCaps.has(a)) return 'unsafe'; // unlisted capability
          }
        }

        // `lsof`: `+` options write/augment state — notably `+m`/`+m<path>` emits a
        // mount-supplement file. Only `-` read flags are safe. Mirrors Claude's lsof
        // `+`-option rejection.
        if (cmd === 'lsof') {
          for (const a of args) {
            if (a.startsWith('+')) return 'unsafe';
          }
        }

        // xargs: must target a safe command
        if (cmd === 'xargs' && !isXargsSafe(unwrapped)) return 'unsafe';

        // `dotnet`: only its read-only top-level SDK queries (--version/--info/
        // --list-runtimes/--list-sdks) are auto-safe. A bare `dotnet` (help) or any
        // subcommand (build/run/test/…) must prompt. Mirrors Claude's isDotnetSafe.
        if (cmd === 'dotnet' && !isDotnetSafe(args)) return 'unsafe';

        // Block shell substitutions ($VAR, $(cmd)) but allow glob patterns
        if (containsShellSubstitution(unwrapped)) return 'unsafe';

        if (allSegmentsKnown(unwrapped)) return 'safe';
        break;
      }
    }

    // sed without -i is read-only (prints to stdout)
    if (/\bsed\b/.test(trimmed) && !/\bsed\s+.*-[^\s]*i/.test(trimmed)) return 'safe';
    // find is read-only only without any command-running primary
    // (-delete, -exec, -execdir, -ok, -okdir).
    if (/\bfind\b/.test(trimmed) && !/(^|\s)-delete\b/.test(trimmed) && !/(^|\s)-exec(dir)?\b/.test(trimmed) && !/(^|\s)-ok(dir)?\b/.test(trimmed)) return 'safe';

    // Check if extracted paths target system directories
    const cmdPaths = extractCommandPaths(trimmed);
    for (const p of cmdPaths) {
      for (const sysDir of SYSTEM_DIRECTORIES) {
        const resolved = path.resolve(p);
        if (resolved === sysDir || resolved.startsWith(sysDir + path.sep)) {
          return 'destructive';
        }
      }
    }

    // PowerShell read-only cmdlets (Get-ChildItem, Get-Content, Select-String, …)
    // are the Windows equivalents of the POSIX SAFE_COMMANDS reads above. A
    // command composed solely of allowlisted read-only cmdlets with no dangerous
    // construct is auto-allowed, mirroring Claude's CMDLET_ALLOWLIST /
    // isReadOnlyCommand. Placed last so every destructive/unsafe guard above
    // (incl. the system-directory and credential-path nets) wins first.
    if (isReadOnlyPowerShellCommand(trimmed)) return 'safe';

    // Unknown commands default to unsafe
    return 'unsafe';
  }

  // -------------------------------------------------------------------------
  // Path safety
  // -------------------------------------------------------------------------

  /**
   * Check whether a file path is safe to operate on.
   *
   * Allowed:
   *   - Paths within the working directory
   *   - Paths within the user's home directory
   *
   * Denied:
   *   - System directories (/etc, /usr, /bin, /var, /sys, /proc, etc.)
   *   - Paths containing ".." segments that escape cwd
   */
  /**
   * Resolve a path for permission checks: expand ~, make absolute against cwd, and
   * resolve symlinks (so an in-cwd symlink pointing at /etc or a sibling dir cannot
   * smuggle access past the prefix check). A not-yet-existing path (a write target)
   * keeps its normalized form.
   */
  private resolveForCheck(filePath: string, cwd: string): string {
    // Canonicalize to the real location and re-apply NFC normalization: on an
    // NFD-storing filesystem fs.realpathSync returns the decomposed on-disk
    // spelling, which would not byte-match an NFC cwd/rule. A not-yet-existing
    // write target keeps its already-NFC expanded form. Shared helper so the
    // single realpath+NFC rule is not re-implemented per call site.
    return realpathNFC(expandPath(filePath, cwd));
  }

  /**
   * FORBIDDEN paths are never readable by the agent — even inside the workspace, even in
   * a granted root, even with explicit user consent: credential/secret files and system
   * directories. Mirrors the credential write-guard so reads are no narrower than writes
   * (.aws/credentials, .git-credentials, .kube/config, .ssh keys, .env, .pem/.key, /etc…).
   */
  private isForbiddenReadPath(resolved: string): boolean {
    if (isCredentialPath(resolved)) return true;
    const basename = path.basename(resolved).toLowerCase();
    const sensitiveNames = ['.env', 'id_rsa', 'id_ed25519', 'id_ecdsa', 'id_dsa'];
    const sensitiveExts = ['.pem', '.key', '.p12', '.pfx', '.keystore'];
    const sensitivePatterns = [/^\.env$/, /^\.env\.local$/, /^\.env\.production$/,
      /^credentials\.json$/, /^secrets\.ya?ml$/, /^\.netrc$/];
    if (sensitiveNames.some(s => basename === s) ||
        sensitiveExts.some(e => basename.endsWith(e)) ||
        sensitivePatterns.some(p => p.test(basename))) {
      return true;
    }
    // Case-fold the directory-prefix comparison on case-insensitive filesystems so
    // a `/USR/bin` or `/Etc/passwd` reference cannot evade the system-dir guard
    // (gap U003). On Linux this is a no-op.
    const fResolved = normalizeCaseForComparison(resolved);
    for (const sysDir of SYSTEM_DIRECTORIES) {
      const fSysDir = normalizeCaseForComparison(sysDir);
      if (fResolved === fSysDir || fResolved.startsWith(fSysDir + path.sep)) return true;
    }
    return false;
  }

  /**
   * FORBIDDEN at ANY symlink hop: expand `filePath`, walk the FULL symlink chain
   * ({@link getPathsForPermissionCheck}) and return true if the original path, ANY
   * intermediate target, OR the final resolved path is a forbidden read path. This
   * closes the symlink-chain escape where only the final realpath is inspected —
   * e.g. `top → dir/id_rsa → dir/plain.txt`: the final `plain.txt` is innocuous but
   * the intermediate `id_rsa` hop is a credential path the deny must still catch.
   * Mirrors Claude's getPathsForPermissionCheck "match at any hop" semantics.
   */
  private isForbiddenAtAnyHop(filePath: string, cwd: string): boolean {
    const expanded = expandPath(filePath, cwd);
    for (const hop of getPathsForPermissionCheck(expanded)) {
      if (this.isForbiddenReadPath(hop)) return true;
    }
    return false;
  }

  /**
   * WORKSPACE containment: a resolved path is inside the workspace iff it is under the
   * launch cwd OR under a user-granted read root (registerAllowedRoot). This is the
   * boundary that confines the agent to where it was started — sibling, parent, and
   * other-$HOME paths are NOT inside unless the user explicitly granted them.
   */
  private isWithinWorkspace(resolved: string, cwd: string): boolean {
    // Strip trailing separators so a cwd/root passed as "/ws/" doesn't become "/ws//"
    // (path.normalize preserves a trailing slash), which would break the prefix match.
    // Case-fold on case-insensitive filesystems (gap U003): on macOS/Windows a
    // differently-cased-but-identical path (cwd `/Users/foo/Project`, resolved
    // `/users/foo/project/file`) names the same file and must count as contained;
    // a raw case-sensitive prefix test would wrongly treat it as outside. On Linux
    // this is a no-op (distinct casings = distinct files).
    const strip = (p: string) => normalizeCaseForComparison(p.replace(/[/\\]+$/, '') || p);
    const r = strip(resolved);
    const ncwd = strip(path.normalize(cwd));
    if (r === ncwd || r.startsWith(ncwd + path.sep)) return true;
    for (const root of this.allowedReadRoots) {
      const nr = strip(root);
      if (r === nr || r.startsWith(nr + path.sep)) return true;
    }
    return false;
  }

  /**
   * Hard boundary for the tools' path guard: true only when a path is BOTH non-forbidden
   * AND inside the workspace (launch cwd or a user-granted root). Confines the agent to
   * its launch directory — reading elsewhere in $HOME (e.g. ~/dev/other-project), a
   * sibling, or a parent is denied unless the user granted it via /add-dir or an approved
   * out-of-workspace prompt. NOTE: callers that support user consent should prefer
   * classifyReadAccess() so a user-pointed out-of-workspace file can be approved.
   */
  isPathAllowed(filePath: string, cwd: string): boolean {
    const resolved = this.resolveForCheck(filePath, cwd);
    if (this.isForbiddenReadPath(resolved)) return false;
    // Also inspect every symlink hop, not just the final realpath, so a forbidden
    // intermediate target in a symlink chain cannot be laundered behind an
    // innocuous final file.
    if (this.isForbiddenAtAnyHop(filePath, cwd)) return false;
    // A shell-expansion / tilde-variant TOCTOU path is never silently auto-allowed
    // (it is kept literal here but rewritten by the shell at run time) — keep this
    // boundary consistent with classifyReadAccess, which gates it to a prompt.
    if (detectPathExpansionHazard(filePath)) return false;
    // A plain (non-traversal) read glob cannot be statically validated — a symlink
    // matched inside its expansion is never examined — so it is never silently
    // auto-allowed; the consent-aware classifyReadAccess gates it to a prompt.
    if (isUnvalidatableReadGlob(filePath)) return false;
    return this.isWithinWorkspace(resolved, cwd);
  }

  /**
   * Three-way read classification that drives the consent flow:
   *   'deny'   — forbidden (credential/system): never read, even with consent.
   *   'allow'  — inside the workspace (cwd or a granted root): read without prompting.
   *   'prompt' — outside the workspace but NOT forbidden: the user must consent. This is
   *              what lets a file the user explicitly points at (e.g. an image under
   *              /some/other/path) work, while a silent `cat ../../other/secret` is gated.
   *              On approval the caller should registerAllowedRoot(dirname(path)).
   */
  classifyReadAccess(filePath: string, cwd: string): 'allow' | 'deny' | 'prompt' {
    const resolved = this.resolveForCheck(filePath, cwd);
    if (this.isForbiddenReadPath(resolved)) return 'deny';
    // A forbidden target at ANY symlink hop (not only the final realpath) is a
    // hard deny — a chain cannot launder a credential/system path behind an
    // innocuous final file.
    if (this.isForbiddenAtAnyHop(filePath, cwd)) return 'deny';
    // TOCTOU: a path carrying shell-expansion syntax ($/%/leading '=') or an
    // unsupported tilde variant (~user/~+/~-) is validated as a literal here but
    // rewritten by the shell at execution, so it can never be silently auto-allowed
    // even when it resolves inside the workspace — force a manual-approval prompt.
    // Mirrors Claude's pathValidation blanket-reject (pathValidation.ts L393-L436).
    if (detectPathExpansionHazard(filePath)) return 'prompt';
    // Glob read paths cannot be statically validated (finding #15): the pre-glob
    // base is the only realpath'd segment, so a symlink matched inside the
    // expansion (e.g. `/project/[star]/passwd`, `/project/link → /etc`) is never
    // examined while the literal glob still prefix-matches the workspace. Force a
    // manual-approval prompt for a plain read glob; a traversal glob falls through
    // so its `..`-collapsed concrete target is validated against the workspace and
    // forbidden guards above. Parity: Claude's pathValidation.ts L1199-L1241.
    if (isUnvalidatableReadGlob(filePath)) return 'prompt';
    if (this.isWithinWorkspace(resolved, cwd)) return 'allow';
    return 'prompt';
  }

  /**
   * The user-facing reason a path must be manually approved because it carries a
   * shell-expansion / tilde-variant TOCTOU hazard or is an unvalidatable read glob,
   * or `null` when the path is inert. Lets the permission UI explain WHY an
   * otherwise-valid-looking path was gated rather than mislabelling it "outside the
   * workspace".
   */
  pathExpansionHazardReason(filePath: string): string | null {
    const hazard = detectPathExpansionHazard(filePath);
    if (hazard) return pathExpansionHazardReason(hazard);
    if (isUnvalidatableReadGlob(filePath)) {
      return 'Glob patterns in read paths require manual approval — a symlink '
        + 'matched inside the glob expansion cannot be statically validated';
    }
    return null;
  }

  // -------------------------------------------------------------------------
  // Denial tracking & circuit breaker
  // -------------------------------------------------------------------------

  /**
   * Record a denied tool invocation so that repeat requests are auto-denied
   * without re-prompting the user.
   */
  trackDenial(toolName: string, input: Record<string, unknown>): void {
    const key = denialKey(toolName, input);
    this.deniedCommands.add(key);
  }

  /**
   * Check whether a given invocation has already been denied.
   */
  isDenied(toolName: string, input: Record<string, unknown>): boolean {
    const key = denialKey(toolName, input);
    return this.deniedCommands.has(key);
  }

  /**
   * Clear all tracked denials.
   */
  clearDenials(): void {
    this.deniedCommands.clear();
  }

  /**
   * Record a denial. Always advances the circuit-breaker counters; when a
   * `display` string is supplied it is ALSO appended (newest-first, bounded) to
   * the recency-ordered denial log so the user can later review exactly what was
   * blocked (mirrors Claude's getAutoModeDenials / AutoModeDenial store). This is
   * the single canonical denial recorder — do not add a parallel one. `source`
   * distinguishes a model/classifier block from a rule/hardline/mode deny.
   */
  recordDenial(display?: string, source: RecentDenial['source'] = 'rule', reason?: string): void {
    this.consecutiveDenials++;
    this.totalDenials++;
    if (display !== undefined) {
      // A 'mode' denial's display is a prose description, not a command; everything
      // else is command-like, so classify it as a bash call for a best-effort risk
      // tier. Pure + deterministic (reuses the canonical bash classifier).
      const riskLevel =
        source === 'mode'
          ? undefined
          : classifyPermissionRisk('bash', { command: display }).riskLevel;
      this._recentDenials.unshift({ display, source, reason, riskLevel, timestamp: Date.now() });
      if (this._recentDenials.length > this.MAX_RECENT_DENIALS) {
        this._recentDenials.length = this.MAX_RECENT_DENIALS;
      }
    }
  }

  /**
   * Report WHY an elevated "auto"/autonomy permission mode is unavailable right now,
   * or `null` when it may be entered. Consumes this manager's live denial
   * circuit-breaker state ({@link shouldAbort}) and the caller-supplied settings /
   * model capability, delegating the precedence to the pure
   * {@link classifyAutoModeUnavailableReason}. The CLI uses this to show a one-shot
   * reason-tagged notification when the user tries to switch into auto mode.
   */
  autoModeUnavailableReason(
    opts: { disabledBySettings?: boolean; modelSupportsAutoMode?: boolean } = {},
  ): AutoModeUnavailableReason | null {
    return classifyAutoModeUnavailableReason({
      disabledBySettings: opts.disabledBySettings ?? false,
      circuitBroken: this.shouldAbort(),
      modelSupportsAutoMode: opts.modelSupportsAutoMode ?? true,
    });
  }

  /**
   * Record a successful permission grant.
   * Resets the consecutive denial counter.
   */
  recordSuccess(): void {
    this.consecutiveDenials = 0;
  }

  /**
   * Check whether the circuit breaker has tripped.
   * Returns true if too many consecutive or total denials have occurred,
   * indicating the agent should abort rather than keep trying.
   */
  shouldAbort(): boolean {
    return this.consecutiveDenials >= this.MAX_CONSECUTIVE || this.totalDenials >= this.MAX_TOTAL;
  }

  /**
   * Reset the circuit breaker counters.
   */
  resetCircuitBreaker(): void {
    this.consecutiveDenials = 0;
    this.totalDenials = 0;
  }

  // -------------------------------------------------------------------------
  // Rule management
  // -------------------------------------------------------------------------

  /**
   * Get the current permission mode.
   */
  getMode(): PermissionMode {
    return this.mode;
  }

  /**
   * Set the permission mode.
   * Returns true if the mode was set successfully, false if blocked by policy.
   */
  setMode(mode: PermissionMode): boolean {
    if (this._disableBypass && (mode === 'bypassPermissions' || mode === 'auto-allow')) {
      return false; // blocked by managed policy
    }
    // Entering plan mode from a non-plan mode: remember the prior mode so a later
    // exit can restore it. A plan→plan no-op must NOT overwrite the memory, and
    // switching to any explicit non-plan mode clears it (the caller chose the mode).
    if (mode === 'plan') {
      if (this.mode !== 'plan') this._prePlanMode = this.mode;
    } else {
      this._prePlanMode = undefined;
    }
    this.mode = mode;
    return true;
  }

  /**
   * The permission mode captured when plan mode was entered, or undefined if not
   * in plan mode / none captured (Claude's ToolPermissionContext.prePlanMode).
   */
  getPrePlanMode(): PermissionMode | undefined {
    return this._prePlanMode;
  }

  /**
   * Leave plan mode, restoring the permission mode that was active before plan was
   * entered (Claude's ExitPlanMode prePlanMode restore). When not currently in
   * plan mode this is a no-op that returns the current mode. The restore is routed
   * through {@link setMode} so managed policy still applies — if the captured mode
   * was a bypass mode now forbidden by a killswitch, it falls back to 'ask'. When
   * no prior mode was captured it also falls back to 'ask'. Returns the new mode.
   */
  exitPlanMode(): PermissionMode {
    if (this.mode !== 'plan') return this.mode;
    const restore = this._prePlanMode ?? 'ask';
    this._prePlanMode = undefined;
    if (!this.setMode(restore)) {
      this.mode = 'ask';
    }
    return this.mode;
  }

  /**
   * Enable or disable the bypass-permissions guard.
   * When enabled, setMode() rejects 'bypassPermissions' and 'auto-allow'.
   */
  setDisableBypass(disable: boolean): void {
    this._disableBypass = disable;
  }

  /**
   * Check whether bypass is disabled by managed policy.
   */
  isDisableBypass(): boolean {
    return this._disableBypass;
  }

  /**
   * Get the current rule list.
   */
  getRules(): PermissionRule[] {
    return [...this.rules];
  }

  /**
   * Add a rule. It is prepended so that later-added rules take priority.
   * Under the managed-rules-only lockdown, a non-`policySettings` rule is refused
   * (a runtime session/CLI grant cannot bypass managed policy).
   */
  addRule(rule: PermissionRule): void {
    if (this._managedRulesOnly && !isManagedPermissionSource(rule.source)) return;
    this.rules.unshift(rule);
  }

  /**
   * Does an EXPLICIT deny apply to this call — a previously-recorded denial, or
   * a matching rule (first match wins, like check()) whose action is 'deny'?
   *
   * Unlike check(), this deliberately ignores mode-level decisions
   * ('deny-all' / default 'ask'). Read-only tools are safe by
   * design and must keep running under those modes (detached mode defaults to
   * 'deny-all' yet relies on read-only tools), so the read-only path uses this
   * to honor only a rule/denial the user set explicitly — closing the gap where
   * an explicit `{tool:'grep', action:'deny'}` rule was silently bypassed.
   */
  /**
   * Match a rule's tool field against a tool name, supporting glob/prefix
   * wildcards (e.g. `github:*` to scope a rule to one MCP server) in addition to
   * exact names and `*`. Lets allow/deny rules target a namespace without the
   * over-broad `{tool:'*'}` grant.
   */
  private toolNameMatches(ruleTool: string, toolName: string): boolean {
    // Canonicalize legacy/cross-framework names (e.g. a `Bash`/`Edit` rule
    // imported from a Claude settings file matches AMI's `bash`/`file_edit`).
    const rt = normalizeToolName(ruleTool);
    return rt === '*' || rt === toolName
      || (rt.includes('*') && matchPattern(rt, toolName));
  }

  /** Coerce a persisted paramPattern into a validated Record<string,string>,
   *  dropping non-string values (a malformed {"model":123} must not load as a
   *  rule that matches nothing-or-everything). Returns undefined if nothing valid. */
  private sanitizeParamPattern(raw: unknown): Record<string, string> | undefined {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return undefined;
    const entries = Object.entries(raw as Record<string, unknown>);
    if (entries.length === 0) return undefined;
    // Strict: if ANY value is non-string the pattern is malformed. Keeping only
    // the valid subset would silently widen the rule (fewer constraints → more
    // matches), so reject the whole thing (loadRules then skips the rule).
    const out: Record<string, string> = {};
    for (const [k, v] of entries) {
      if (typeof v !== 'string') return undefined;
      out[k] = v;
    }
    return out;
  }

  /**
   * True when a bash `allow` rule's content is an over-broad code-execution
   * prefix (interpreter / package-runner / shell — see dangerous-code-exec.ts).
   * Such a rule grants arbitrary code execution and is downgraded to a prompt in
   * the rule-evaluated modes by check(). A narrow/specific rule returns false.
   */
  private isDangerousCodeExecAllow(rule: PermissionRule): boolean {
    return rule.action === 'allow' && normalizeToolName(rule.tool) === 'bash' &&
      isDangerousCodeExecPrefix(rule.pattern);
  }

  /**
   * The code-execution allow rule (if any) that matched this bash command and
   * caused check()'s allow→ask downgrade — used by deriveDecisionReason to
   * attribute the 'ask' verdict. `isDangerousCodeExecAllow` already constrains the
   * rule to `allow` + bash + a non-empty dangerous prefix, so `rule.pattern` is
   * defined for any candidate.
   */
  private matchingCodeExecAllowRule(
    input: Record<string, unknown>,
    command: string,
  ): PermissionRule | undefined {
    for (const rule of this.rules) {
      if (rule.action !== 'allow' || !this.isDangerousCodeExecAllow(rule)) continue;
      if (rule.paramPattern && !this.paramMatches(rule.paramPattern, input)) continue;
      if (matchBashPattern(rule.pattern!, command, 'allow')) return rule;
    }
    return undefined;
  }

  /**
   * Introspection: the active `allow` rules whose content is an over-broad
   * code-execution prefix and that check() therefore downgrades to a prompt in
   * `ask` / `acceptEdits` modes. Lets a UI / diagnostic surface explain which
   * user rules are being treated as code-execution escape hatches rather than
   * silent auto-approvals. Mirrors Claude's `findDangerousClassifierPermissions`.
   */
  getDangerousCodeExecAllowRules(): PermissionRule[] {
    return this.rules.filter(r => this.isDangerousCodeExecAllow(r));
  }

  /** All named-param globs in a rule's paramPattern must match the tool input. */
  private paramMatches(paramPattern: Record<string, string>, input: Record<string, unknown>): boolean {
    for (const [key, glob] of Object.entries(paramPattern)) {
      const v = input[key];
      if (typeof v !== 'string' || !matchPattern(glob, v)) return false;
    }
    return true;
  }

  /**
   * M9: does ANY deny rule match this call, regardless of rule order? A matching
   * deny UNCONDITIONALLY wins over allow/ask. Unlike hasExplicitDeny (first-match,
   * returns the first matching rule's action), this scans the WHOLE rule list for a
   * deny, so an earlier allow can never shadow a later deny.
   *
   * `skipParamPattern` preserves the auto-allow fast-path's long-standing behavior
   * of ignoring structured paramPattern rules (they are enforced by the ordered
   * rule loop in check()'s rule-evaluated modes, not by the auto-allow fast-path).
   */
  private anyDenyMatches(
    toolName: string,
    input: Record<string, unknown>,
    skipParamPattern = false,
  ): boolean {
    for (const rule of this.rules) {
      if (rule.action !== 'deny') continue;
      if (skipParamPattern && rule.paramPattern) continue;
      if (!this.toolNameMatches(rule.tool, toolName)) continue;
      if (rule.paramPattern && !this.paramMatches(rule.paramPattern, input)) continue;
      if (rule.pattern) {
        const inputStr = this.inputToString(toolName, input);
        const matched = toolName === 'bash' && typeof input.command === 'string'
          ? matchBashPattern(rule.pattern, inputStr, 'deny')
          : matchPattern(rule.pattern, inputStr);
        if (matched) return true;
      } else {
        return true;
      }
    }
    return false;
  }

  /**
   * Does any user-configured `ask` rule match this tool+input? Mirrors
   * {@link anyDenyMatches} but for content-specific `ask` rules. Used to keep
   * explicit `ask` rules BYPASS-IMMUNE: a user's `{tool, action:'ask'}` rule must
   * still prompt in bypassPermissions mode, before the blanket auto-allow (Claude
   * evaluates ask/safety-check rules BEFORE the bypass allow, permissions.ts
   * L1169-1281).
   */
  private anyAskMatches(toolName: string, input: Record<string, unknown>): boolean {
    for (const rule of this.rules) {
      if (rule.action !== 'ask') continue;
      if (!this.toolNameMatches(rule.tool, toolName)) continue;
      if (rule.paramPattern && !this.paramMatches(rule.paramPattern, input)) continue;
      if (rule.pattern) {
        const inputStr = this.inputToString(toolName, input);
        const matched = toolName === 'bash' && typeof input.command === 'string'
          ? matchBashPattern(rule.pattern, inputStr, 'ask')
          : matchPattern(rule.pattern, inputStr);
        if (matched) return true;
      } else {
        return true;
      }
    }
    return false;
  }

  /**
   * Synchronously decide whether a bash COMMAND would be DENIED by the user's
   * configured deny rules (parity with Claude Code's `hasPermissionsToUseTool`
   * deny half, applied to skill/command-embedded `!`cmd` live-command execution —
   * promptShellExecution.ts). It reuses the SAME canonical bash deny-matching that
   * {@link check} uses ({@link anyDenyMatches} → `matchBashPattern`: compound-command
   * fanout, `$(...)`/backtick decomposition, and de-obfuscation), so a deny a user
   * configured for the model's own `bash` calls is ALSO honored when a TRUSTED skill
   * body embeds a live command — closing the gap where a skill injection bypassed
   * the user's permission rules and was gated only by static bash-safety. Pure read
   * of the current rule set; never prompts and never mutates state. A blank/non-string
   * command is not denied (there is nothing to run).
   */
  isBashCommandDenied(command: string): boolean {
    if (typeof command !== 'string' || command.trim().length === 0) return false;
    return this.anyDenyMatches('bash', { command });
  }

  hasExplicitDeny(toolName: string, input: Record<string, unknown>): boolean {
    if (this.deniedCommands.has(denialKey(toolName, input))) return true;
    // First-match semantics (consistent with check()).
    for (const rule of this.rules) {
      if (!this.toolNameMatches(rule.tool, toolName)) continue;
      if (rule.paramPattern && !this.paramMatches(rule.paramPattern, input)) continue;
      if (rule.pattern) {
        if (matchPattern(rule.pattern, this.inputToString(toolName, input))) return rule.action === 'deny';
      } else {
        return rule.action === 'deny';
      }
    }
    return false;
  }

  /**
   * Remove every rule matching the predicate. Returns the number removed.
   * Used to drop scoped rule sets (e.g. a persona's auto-allow rules) when
   * that scope ends, so they do not accumulate or leak across switches.
   *
   * Managed (`policySettings`) rules are IMMUTABLE: they are admin/enterprise-owned
   * and can never be removed by the agent, a persona-scope cleanup, or a
   * `/permissions remove` — even when the predicate matches them. This mirrors
   * Claude's managed-settings tier, where policy rules are read-only to the user
   * ({@link isManagedPermissionSource}). The lockdown in {@link addRule} stops new
   * non-managed rules from being added; this stops existing managed rules from
   * being deleted, so the enterprise policy floor cannot be edited away at runtime.
   */
  removeRule(predicate: (rule: PermissionRule) => boolean): number {
    const before = this.rules.length;
    this.rules = this.rules.filter(r => isManagedPermissionSource(r.source) || !predicate(r));
    return before - this.rules.length;
  }

  /**
   * Apply ONE structured {@link PermissionUpdate} to the live manager. This is the
   * single canonical path for turning a declarative rule/mode/directory mutation
   * (a permission-prompt suggestion, a `/permissions allow|deny|remove` command, a
   * hook-returned update) into state, so callers never hand-roll the addRule /
   * removeRule / setMode / registerAllowedRoot plumbing. Mirrors Claude's
   * `applyPermissionUpdate`; purely in-memory (persistence is a separate step).
   * Returns the number of rules/directories affected. Rule removal matches on the
   * NORMALIZED canonical string (parse→serialize) so a `Bash(*)` suggestion
   * removes a stored tool-wide `bash` rule and vice-versa.
   */
  applyUpdate(update: PermissionUpdate): { applied: number } {
    switch (update.type) {
      case 'setMode':
        return { applied: this.setMode(update.mode) ? 1 : 0 };
      case 'addRules': {
        let applied = 0;
        const source = update.destination as PermissionRuleSource | undefined;
        // Preserve suggestion order: addRule prepends, so insert in reverse.
        for (const rv of [...update.rules].reverse()) {
          const before = this.rules.length;
          this.addRule({ tool: normalizeToolName(rv.toolName), pattern: rv.ruleContent, action: update.behavior, source });
          if (this.rules.length > before) applied++;
        }
        return { applied };
      }
      case 'replaceRules': {
        const source = update.destination as PermissionRuleSource | undefined;
        // Atomically clear every rule of this (behavior, destination) first, then
        // add the replacements — mirrors Claude's per-(behavior,destination) swap.
        this.removeRule(r => r.action === update.behavior && r.source === source);
        return this.applyUpdate({ type: 'addRules', rules: update.rules, behavior: update.behavior, destination: update.destination });
      }
      case 'removeRules': {
        // Canonicalize both sides through parse→serialize so a `Bash(*)` / `Bash()`
        // suggestion matches a stored tool-wide `bash` rule (and vice-versa).
        const canon = (toolName: string, ruleContent?: string): string =>
          permissionRuleValueToString(permissionRuleValueFromString(permissionRuleValueToString({ toolName, ruleContent })));
        const targets = new Set(update.rules.map(rv => canon(normalizeToolName(rv.toolName), rv.ruleContent)));
        const applied = this.removeRule(r =>
          r.action === update.behavior &&
          targets.has(canon(r.tool, r.pattern)));
        return { applied };
      }
      case 'addDirectories': {
        let applied = 0;
        for (const dir of update.directories) {
          const before = this.getAllowedReadRoots().length;
          this.registerAllowedRoot(dir);
          if (this.getAllowedReadRoots().length > before) applied++;
        }
        return { applied };
      }
      case 'removeDirectories': {
        let applied = 0;
        for (const dir of update.directories) {
          const abs = path.isAbsolute(dir) ? dir : path.resolve(dir);
          if (this.removeAllowedRoot(abs)) applied++;
        }
        return { applied };
      }
    }
  }

  /**
   * Apply a BATCH of {@link PermissionUpdate}s in order (the one-click
   * "accept all suggestions" affordance). Mirrors Claude's
   * `applyPermissionUpdates`. Returns the total number of rules/directories
   * affected across every update.
   */
  applyUpdates(updates: PermissionUpdate[]): { applied: number } {
    let applied = 0;
    for (const update of updates) applied += this.applyUpdate(update).applied;
    return { applied };
  }

  /**
   * Grant-and-retry recovery for previously-denied commands: for each selected
   * entry in the recent-denials log (all of them when `selected` is omitted),
   * derive a scoped allow rule ({@link suggestAllowPrefix}) and apply it via
   * {@link applyUpdate}, then drop those entries from the log so they are not
   * offered again. Returns the retried commands and the model-facing
   * {@link createPermissionRetryMessage} so the caller can tell the agent it may
   * re-attempt them. Mirrors Claude's `/permissions` "retry denials" affordance
   * (onRetryDenials → createPermissionRetryMessage), extended to actually seed the
   * allow rule so the retry succeeds instead of prompting again.
   */
  grantRetryForDenials(selected?: string[]): { commands: string[]; message: string } {
    const selectedSet = selected && selected.length > 0 ? new Set(selected) : undefined;
    const chosen = selectedSet
      ? this._recentDenials.filter(d => selectedSet.has(d.display))
      : [...this._recentDenials];
    const commands: string[] = [];
    const rules: PermissionRuleValue[] = [];
    for (const d of chosen) {
      commands.push(d.display);
      const prefix = suggestAllowPrefix(d.display);
      if (prefix) rules.push({ toolName: 'bash', ruleContent: `${prefix}*` });
    }
    if (rules.length > 0) {
      this.applyUpdate({ type: 'addRules', behavior: 'allow', destination: 'session', rules });
    }
    const granted = new Set(commands);
    this._recentDenials = this._recentDenials.filter(d => !granted.has(d.display));
    return { commands, message: createPermissionRetryMessage(commands) };
  }

  /**
   * Detect allow rules that are shadowed by a higher-priority rule and can never
   * take effect, each with a {@link ShadowType}, a human-readable {@link
   * UnreachableRule.reason}, and an actionable {@link UnreachableRule.fix}.
   *
   * Severity ordering (parity with Claude's detectUnreachableRules): deny-shadowing
   * is checked FIRST and, on a hit, the allow is reported ONCE as `deny` and ask
   * shadowing is NOT also reported — each unreachable allow yields exactly one
   * finding, the most severe. A tool-wide deny is order-independent (a matching deny
   * unconditionally wins in {@link check}); a tool-wide ask only shadows when it
   * PRECEDES the allow, because an ask AFTER the allow never fires (AMI evaluates in
   * rule order — this is an intentional superset-accurate divergence from Claude's
   * order-agnostic ask scan, matching AMI's real evaluation).
   *
   * Sandbox carve-out: when sandbox auto-allow is active (from `options` or the
   * manager's own sandbox policy), a tool-wide Bash ask rule from PERSONAL settings
   * is not reported as shadowing — sandboxed bash is auto-allowed for this user — but
   * a shared-source ask rule still warns. Mirrors Claude's
   * `isAllowRuleShadowedByAskRule` exception.
   */
  detectShadowedRules(options: DetectUnreachableRulesOptions = {}): UnreachableRule[] {
    const sandboxAutoAllow = options.sandboxAutoAllowEnabled
      ?? (this.sandboxPolicy?.mode === 'container' && this.sandboxPolicy.autoAllowBashIfSandboxed === true);
    const shadows: UnreachableRule[] = [];
    for (let i = 0; i < this.rules.length; i++) {
      const allow = this.rules[i]!;
      if (allow.action !== 'allow') continue;
      // Scan once for the most-severe shadower: a tool-wide deny (order-independent)
      // beats a preceding tool-wide ask.
      let denyShadow: PermissionRule | undefined;
      let askShadow: PermissionRule | undefined;
      for (let j = 0; j < this.rules.length; j++) {
        if (i === j) continue;
        const shadower = this.rules[j]!;
        if (shadower.tool !== allow.tool) continue;
        // A paramPattern rule is narrow (matches only specific named params),
        // so it never blanket-shadows a later allow.
        if (shadower.paramPattern) continue;
        const blanket = !shadower.pattern || shadower.pattern === '*';
        if (!blanket) continue;
        if (shadower.action === 'deny') denyShadow ??= shadower;
        else if (shadower.action === 'ask' && j < i) askShadow ??= shadower;
      }
      if (denyShadow) {
        shadows.push(this.buildUnreachableRule(allow, denyShadow, 'deny'));
        continue; // most-severe only — do not also report ask-shadowing.
      }
      if (askShadow) {
        // Sandbox carve-out: a personal-source tool-wide Bash ask rule is auto-allowed
        // for this user, so it does not actually shadow the specific allow.
        if (sandboxAutoAllow && normalizeToolName(allow.tool) === 'bash' && !isSharedSettingSource(askShadow.source)) {
          continue;
        }
        shadows.push(this.buildUnreachableRule(allow, askShadow, 'ask'));
      }
    }
    return shadows;
  }

  /** Assemble an {@link UnreachableRule} with a reason + fix naming both rules'
   *  sources. Shared across the deny/ask branches of {@link detectShadowedRules}. */
  private buildUnreachableRule(
    allow: PermissionRule,
    shadower: PermissionRule,
    shadowType: ShadowType,
  ): UnreachableRule {
    const tool = shadower.tool;
    const shadowerSource = permissionRuleSourceDisplayString(shadower.source);
    const allowSource = permissionRuleSourceDisplayString(allow.source);
    const reason = shadowType === 'deny'
      ? `Blocked by "${tool}" deny rule (from ${shadowerSource})`
      : `Shadowed by "${tool}" ask rule (from ${shadowerSource})`;
    const fix = `Remove the "${tool}" ${shadowType} rule from ${shadowerSource}, or remove the specific allow rule from ${allowSource}`;
    return { rule: allow, shadowedBy: shadower, shadowType, reason, fix };
  }

  // -------------------------------------------------------------------------
  // Managed / enterprise policy (policySettings tier)
  // -------------------------------------------------------------------------

  /** Is the managed-rules-only lockdown active (Claude's
   *  `allowManagedPermissionRulesOnly`)? When true, only `policySettings` rules are
   *  honored and nothing may be persisted. Set by {@link loadManagedPermissions}. */
  isManagedRulesOnly(): boolean {
    return this._managedRulesOnly;
  }

  /** Should a permission prompt offer "always allow" / "don't ask again" options?
   *  No, under the managed-rules-only lockdown — new rules can't be persisted, so a
   *  durable-allow affordance would be a dead end. Mirrors Claude's
   *  `shouldShowAlwaysAllowOptions()`. */
  shouldShowAlwaysAllowOptions(): boolean {
    return !this._managedRulesOnly;
  }

  /**
   * Load managed/enterprise permission rules from {@link managedPermissionsPath}
   * (the `policySettings` tier). The file is `{ allowManagedPermissionRulesOnly?:
   * boolean, rules: [...] }`. Every rule is tagged `policySettings` and loaded at
   * HIGHEST precedence (prepended). When `allowManagedPermissionRulesOnly` is true,
   * the lockdown engages: all non-`policySettings` rules already loaded are cleared
   * and no other tier is consulted — mirrors Claude's
   * `loadAllPermissionRulesFromDisk` / `syncPermissionRulesFromDisk` managed path.
   * Returns collected warnings. A missing file is a no-op.
   */
  loadManagedPermissions(): string[] {
    const warnings: string[] = [];
    const filePath = managedPermissionsPath();
    if (!fs.existsSync(filePath)) return warnings;
    let data: Record<string, unknown>;
    try {
      data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
    } catch {
      // A broken enterprise policy must not silently vanish — but it also must not
      // crash the agent; surface a warning and treat as no managed rules.
      warnings.push('policySettings: managed permissions file could not be parsed and was ignored');
      return warnings;
    }
    const lockdown = data.allowManagedPermissionRulesOnly === true;
    const managed: PermissionRule[] = [];
    if (Array.isArray(data.rules)) {
      for (const rule of data.rules) {
        const { rule: coerced, warning } = this._coerceRule(rule, 'policySettings');
        if (coerced) managed.push(coerced);
        else if (warning) warnings.push(`policySettings: ${warning}`);
      }
    }
    if (lockdown) {
      this._managedRulesOnly = true;
      // Clear every non-managed rule — under lockdown only policy rules are honored.
      this.rules = this.rules.filter(r => isManagedPermissionSource(r.source));
    }
    // Prepend managed rules so they win first-match evaluation (highest precedence).
    this.rules.unshift(...managed);
    return warnings;
  }

  // -------------------------------------------------------------------------
  // Persistence
  // -------------------------------------------------------------------------

  /**
   * Load permission rules from `.ami/permissions.json` relative
   * to the given working directory.
   */
  loadRules(cwd: string): string[] {
    const warnings: string[] = [];
    const filePath = path.join(cwd, '.ami', 'permissions.json');
    if (!fs.existsSync(filePath)) return warnings;

    try {
      const raw = fs.readFileSync(filePath, 'utf-8');
      const data = JSON.parse(raw);

      if (data.mode && typeof data.mode === 'string') {
        if ((PERMISSION_MODES as readonly string[]).includes(data.mode)) {
          this.mode = data.mode as PermissionMode;
        } else {
          warnings.push(`Invalid permission mode "${data.mode}" was ignored`);
        }
      }

      if (Array.isArray(data.rules)) {
        for (const rule of data.rules) {
          const { rule: coerced, warning } = this._coerceRule(rule, 'projectSettings');
          if (coerced) this.rules.push(coerced);
          else if (warning) warnings.push(warning);
        }
      }
    } catch {
      warnings.push('permissions.json could not be parsed and was ignored');
    }
    return warnings;
  }

  /**
   * Coerce+validate one raw rule object (shared by loadRules and loadTieredRules
   * — one canonical validator, no per-loader duplication). Returns the typed rule
   * tagged with its `source` tier, or a human-readable warning for a dropped rule.
   */
  private _coerceRule(rule: unknown, source: PermissionRuleSource): { rule?: PermissionRule; warning?: string } {
    const r = rule as Record<string, unknown> | null;
    if (r && typeof r.tool === 'string' && typeof r.action === 'string' &&
        ['allow', 'deny', 'ask'].includes(r.action)) {
      const paramPattern = this.sanitizeParamPattern(r.paramPattern);
      // A rule that specified a paramPattern but whose values were all invalid is
      // malformed — drop it rather than load it as a broad (pattern-less) rule.
      if (r.paramPattern !== undefined && paramPattern === undefined) {
        return { warning: `Invalid permission rule for "${r.tool}" was skipped: malformed paramPattern` };
      }
      const pattern = typeof r.pattern === 'string' ? r.pattern : undefined;
      // Validate the composed canonical rule string so a syntactically bad rule
      // (empty tool name, a misplaced bash `:*` prefix, a WebFetch URL / WebSearch
      // wildcard) is dropped with an actionable warning instead of silently loaded
      // as a rule that never matches. The pattern is paren-escaped first so a
      // legitimate parenthesis in the content is not mistaken for a syntax error.
      const canonical = permissionRuleValueToString({ toolName: r.tool, ruleContent: pattern });
      const verdict = validatePermissionRule(canonical);
      if (!verdict.valid) {
        const suffix = verdict.suggestion ? `. ${verdict.suggestion}` : '';
        return { warning: `Invalid permission rule "${canonical}" was skipped: ${verdict.error}${suffix}` };
      }
      // Honor a per-rule `source` tag when the file bundles rules from several
      // origins (a managed/flag file); a bogus tag falls back to the loader tier.
      return {
        rule: {
          tool: r.tool,
          pattern,
          action: r.action as PermissionRule['action'],
          reason: typeof r.reason === 'string' ? r.reason : undefined,
          paramPattern,
          source: coercePermissionRuleSource(r.source) ?? source,
        },
      };
    }
    // A user who typos a rule gets told WHICH rule was dropped, not a silent no-op.
    return { warning: `Invalid permission rule ${JSON.stringify(rule)} was skipped` };
  }

  /**
   * Load permission rules from ALL persisted tiers and merge them with documented
   * precedence: most-specific first so check()'s first-match allow/ask wins for
   * localSettings over projectSettings over userSettings (deny stays
   * order-independent). Each loaded rule is tagged with its `source` tier. Returns
   * collected warnings. Mirrors Claude's destination-tiered rule merge.
   */
  loadTieredRules(cwd: string): string[] {
    const warnings: string[] = [];
    // Managed/enterprise (policySettings) is the highest-precedence tier and may
    // engage the managed-rules-only lockdown. Load it FIRST; if lockdown engages,
    // no other tier is consulted (only managed rules are honored).
    warnings.push(...this.loadManagedPermissions());
    if (this._managedRulesOnly) return warnings;
    // Precedence (highest first): local → project → user. Push highest first so
    // first-match evaluation prefers the more-specific tier.
    const tiers: PermissionUpdateDestination[] = ['localSettings', 'projectSettings', 'userSettings'];
    for (const tier of tiers) {
      const filePath = permissionDestinationPath(cwd, tier);
      if (!filePath || !fs.existsSync(filePath)) continue;
      try {
        const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
        if (Array.isArray(data.rules)) {
          for (const rule of data.rules) {
            const { rule: coerced, warning } = this._coerceRule(rule, tier);
            if (coerced) this.rules.push(coerced);
            else if (warning) warnings.push(`${tier}: ${warning}`);
          }
        }
      } catch {
        warnings.push(`${tier}: settings file could not be parsed and was ignored`);
      }
    }
    return warnings;
  }

  /**
   * Re-sync the live rule set from disk after an on-disk settings change, mirroring
   * Claude Code's `loadAllPermissionRulesFromDisk` + `syncPermissionRulesFromDisk`
   * (applySettingsChange.ts L37-48): drop every rule that ORIGINATED from a persisted
   * tier (`policySettings`/`localSettings`/`projectSettings`/`userSettings`), reset
   * the managed-rules-only lockdown so a removed policy file no longer locks down,
   * then reload all tiers fresh via {@link loadTieredRules}. In-memory sources
   * (`session`, `cliArg`, `flagSettings`, `command`) — runtime grants a user made
   * this session — are PRESERVED, so a mid-session reload neither duplicates
   * disk rules (repeated syncs are idempotent) nor wipes session grants. The policy
   * memo is cleared so the new allow/deny rules take effect on the very next check
   * instead of serving a stale ≤5s-TTL verdict. Returns collected warnings.
   *
   * This is what makes a mid-session edit to on-disk permission rules (an admin
   * adding a `deny`, a user widening `allow`) apply to the RUNNING session without a
   * restart. Safe to call repeatedly from a settings-change watcher.
   */
  syncRulesFromDisk(cwd: string): string[] {
    const DISK_TIERS: ReadonlySet<PermissionRuleSource> = new Set<PermissionRuleSource>([
      'policySettings', 'localSettings', 'projectSettings', 'userSettings',
    ]);
    this.rules = this.rules.filter(r => r.source === undefined || !DISK_TIERS.has(r.source));
    // A managed lockdown is re-derived from disk on reload; clearing it first lets a
    // now-removed policy file release the lockdown instead of leaving it latched on.
    this._managedRulesOnly = false;
    const warnings = this.loadTieredRules(cwd);
    this._memo?.clear();
    return warnings;
  }

  /**
   * Persist the rules whose `source` matches `destination` to that tier's file
   * (in-memory tiers session/cliArg are a no-op). Returns {ok,error}; a persist
   * failure is surfaced, never silently lost (see {@link saveRules}).
   */
  saveRulesToDestination(cwd: string, destination: PermissionUpdateDestination): { ok: boolean; error?: string } {
    if (this._managedRulesOnly) {
      return { ok: false, error: 'managed-rules-only lockdown: permission rules are controlled by policy and cannot be persisted' };
    }
    const filePath = permissionDestinationPath(cwd, destination);
    if (!filePath) return { ok: true }; // session / cliArg are in-memory only
    try {
      const rules = this.rules.filter(r => r.source === destination);
      // Crash-safe, owner-only persist (temp + fsync + atomic rename, mode 0o600)
      // via the ONE canonical atomic writer — a crash / ENOSPC mid-write can never
      // leave a truncated permissions file that loadRules then silently discards,
      // and the file (which can hold deny patterns describing the project) is not
      // world-readable. Mirrors Claude's writeFileSyncAndFlush_DEPRECATED + 0o600.
      atomicWriteFileSync(filePath, JSON.stringify({ rules }, null, 2) + '\n', { mode: OWNER_ONLY_FILE_MODE });
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e instanceof Error ? e.message : String(e) };
    }
  }

  /**
   * Persist the current rules to `.ami/permissions.json`. Returns a success/error
   * signal so callers can surface a write failure instead of silently losing a
   * "don't ask again" rule (a read-only `.ami/`, a full disk, or a permission
   * error must not look like success). Mirrors Claude's addPermissionRulesToSettings
   * try/catch + boolean-return behavior (permissionsLoader.ts L258-295).
   */
  saveRules(cwd: string): { ok: boolean; error?: string } {
    if (this._managedRulesOnly) {
      return { ok: false, error: 'managed-rules-only lockdown: permission rules are controlled by policy and cannot be persisted' };
    }
    try {
      const filePath = path.join(cwd, '.ami', 'permissions.json');
      const data = { mode: this.mode, rules: this.rules };
      // Crash-safe, owner-only persist (temp + fsync + atomic rename, mode 0o600):
      // a half-written permissions.json would be silently dropped by loadRules'
      // catch, wiping every "don't ask again" rule, and the file is not left
      // world-readable. Uses the ONE canonical atomic writer (utils/fs-atomic),
      // which also creates `.ami/` 0o700 — no separate mkdir needed.
      atomicWriteFileSync(filePath, JSON.stringify(data, null, 2) + '\n', { mode: OWNER_ONLY_FILE_MODE });
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e instanceof Error ? e.message : String(e) };
    }
  }

  // -------------------------------------------------------------------------
  // Policy memo (permission cache)
  // -------------------------------------------------------------------------

  private _memo: PolicyMemo | null = null;

  enableMemo(ttlMs?: number): void {
    this._memo = new PolicyMemo(ttlMs);
  }

  disableMemo(): void {
    this._memo = null;
  }

  getMemo(): PolicyMemo | null {
    return this._memo;
  }

  private _hashInput(input: Record<string, unknown>): string {
    const str = JSON.stringify(input);
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = ((hash << 5) - hash + str.charCodeAt(i)) | 0;
    }
    return hash.toString(36);
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  /**
   * Convert tool input into a flat string for pattern matching.
   */
  private inputToString(toolName: string, input: Record<string, unknown>): string {
    if (typeof input.command === 'string') return input.command;
    if (typeof input.file_path === 'string') return input.file_path;
    return JSON.stringify(input);
  }
}

// ---------------------------------------------------------------------------
// PolicyMemo — memoize permission decisions across bursts of tool calls
// ---------------------------------------------------------------------------

const POLICY_MEMO_MAX_ENTRIES = 1000;

interface MemoEntry {
  decision: 'allow' | 'deny' | 'ask';
  expiresAt: number;
  lastAccess: number;
}

export class PolicyMemo {
  private readonly _ttlMs: number;
  private readonly _cache = new Map<string, MemoEntry>();

  constructor(ttlMs: number = 5000) {
    this._ttlMs = ttlMs;
  }

  get(toolName: string, inputHash: string): 'allow' | 'deny' | 'ask' | null {
    const key = `${toolName}:${inputHash}`;
    const entry = this._cache.get(key);
    if (!entry) return null;
    if (Date.now() > entry.expiresAt) {
      this._cache.delete(key);
      return null;
    }
    entry.lastAccess = Date.now();
    return entry.decision;
  }

  set(toolName: string, inputHash: string, decision: 'allow' | 'deny' | 'ask'): void {
    const key = `${toolName}:${inputHash}`;
    this._cache.set(key, {
      decision,
      expiresAt: Date.now() + this._ttlMs,
      lastAccess: Date.now(),
    });
    if (this._cache.size > POLICY_MEMO_MAX_ENTRIES) {
      this._evictLRU();
    }
  }

  clear(): void {
    this._cache.clear();
  }

  size(): number {
    return this._cache.size;
  }

  prune(): number {
    const now = Date.now();
    let removed = 0;
    for (const [key, entry] of this._cache) {
      if (now > entry.expiresAt) {
        this._cache.delete(key);
        removed++;
      }
    }
    return removed;
  }

  private _evictLRU(): void {
    let oldestKey: string | null = null;
    let oldestAccess = Infinity;
    for (const [key, entry] of this._cache) {
      if (entry.lastAccess < oldestAccess) {
        oldestAccess = entry.lastAccess;
        oldestKey = key;
      }
    }
    if (oldestKey) this._cache.delete(oldestKey);
  }
}
