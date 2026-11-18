import { z } from 'zod';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { expandPath, realpathNFC } from './utils/path';
import { memoizeWithLRU } from './utils/memoize';
import { atomicWriteFileSync, OWNER_ONLY_FILE_MODE } from './utils/fs-atomic';
import { addFileGlobRuleToGitignore } from './utils/git';
import { extractCommandPaths, commandTargetsCredentialPath, commandTargetsSystemDir, isCredentialPath, detectUnsafeHelpCommand, detectArchiveExtractionWithGit, detectSymlinkCreateCompound, detectCdSensitiveDesync, isReadOnlySedCommand, stripHeredocBodies, WRAPPER_VALUE_FLAGS, SRUN_VALUE_FLAGS } from './tools/bash-security';
import { isPowerShellNeverSuggest, isReadOnlyPowerShellCommand, isAcceptEditsPowerShellWrite, POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS, canonicalizePowerShellCommandName } from './tools/powershell-security';
import { deobfuscateEnvExpansion } from './tools/command-normalize';
import { valueFlagsForCommand } from './tools/command-spec-registry';
import { unwrapWrapperChain, isBareSubcommandAwareRoot } from './tools/command-prefix';
import { detectPromptInjection } from './tools/input-sanitizer';
import { isDangerousCodeExecPrefix } from './dangerous-code-exec';
import { getManagedConfigPath } from './config';
import { classifyAutoModeUnavailableReason, type AutoModeUnavailableReason } from './auto-mode';
import { evaluateToolCeiling, type ToolCapabilityCeiling } from './tools/scoped-permissions';
import { isReadOnlyBashCommand } from './tools/bash-cache';
import { TESTING_PERMISSION_TOOL_NAME } from './tools/testing-permission';
import { isCaseInsensitiveFs } from './tools/tool-utils';
import { log as logDebug } from './logger';
import {
  SpeculativeClassifierCache,
  DEFAULT_SPECULATIVE_GRACE_MS,
  PendingPromptLatch,
  type SpeculativeBashClassifier,
} from './speculative-classifier';
import type { SandboxPolicy } from './types';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// Each mode is a DISTINCT permission semantic (names mix AMI-native kebab and
// Claude-Code wire-names — that is intentional, we match CC semantics, not a naming
// convention). `dontAsk` was removed: it was a semantic DUPLICATE of `deny-all` (both
// deny every tool call); normalizePermissionMode maps the legacy spelling to deny-all.
export const PERMISSION_MODES = ['ask', 'auto', 'auto-allow', 'deny-all', 'acceptEdits', 'bypassPermissions', 'plan'] as const;
export type PermissionMode = (typeof PERMISSION_MODES)[number];

/**
 * Zod enum over the canonical {@link PERMISSION_MODES}. The single VALIDATION
 * primitive for a permission-mode string, reusable anywhere a settings/config
 * schema needs to parse one (mirrors Claude Code's `permissionModeSchema`). It
 * is discovery-driven — it derives from the `PERMISSION_MODES` tuple, so a new
 * canonical mode is accepted the moment it is added there (no second list to
 * keep in sync). {@link normalizePermissionMode} parses the canonical arm through
 * this schema, so the schema and the normalizer can never disagree on what counts
 * as a canonical mode. Prefer this over an inline `.includes` check at new call
 * sites so a settings validator gets a typed {@link PermissionMode} back.
 */
export const permissionModeSchema = z.enum(PERMISSION_MODES);

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
  // Parsed through the shared {@link permissionModeSchema} so the normalizer and
  // the settings-validation schema agree byte-for-byte on the canonical set.
  const canonical = permissionModeSchema.safeParse(m);
  if (canonical.success) return canonical.data;
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
 * True when `mode` is the neutral/default permission mode — AMI's default is `ask`
 * — OR is unset (`undefined`). Centralizes the "is this the default?" branch so the
 * undefined-vs-default equivalence lives in ONE place instead of being re-derived as
 * scattered `?? 'ask'` coalescing at each call site (which invites drift when the
 * default changes). Mirrors Claude's `isDefaultMode` (utils/permissions/PermissionMode.ts),
 * which likewise treats both the default string and `undefined` as the default.
 */
export function isDefaultMode(mode: PermissionMode | undefined): boolean {
  return mode === undefined || mode === 'ask';
}

/**
 * The permission modes that RELAX the default prompt-on-everything gate: edits are
 * auto-accepted (`acceptEdits`), or approval is blanket (`auto-allow`, the YOLO
 * alias, and `bypassPermissions`). Centralizes the "is this a more-permissive
 * mode?" predicate so the transition-time rule-cleanup in {@link PermissionManager}
 * and any UI that warns on escalation share ONE definition. The screened-autonomy
 * `auto` mode is NOT included: it still risk-screens every call, so it is not a
 * blanket relaxation. Mirrors Claude's set of modes that trigger
 * `transitionPermissionMode` cleanup (getNextPermissionMode.ts).
 */
export function isPermissivePermissionMode(mode: PermissionMode | undefined): boolean {
  return mode === 'acceptEdits' || mode === 'auto-allow' || mode === 'bypassPermissions';
}

/**
 * The reduced permission-mode vocabulary exposed to EXTERNAL / SDK consumers. A
 * superset consumer should not have to understand AMI's richer internal set
 * (`auto-allow`, `deny-all`); those collapse onto an external equivalent via
 * {@link toExternalPermissionMode}. Mirrors Claude's `EXTERNAL_PERMISSION_MODES`
 * (the internal set minus vendor-only modes), with AMI's neutral mode surfaced as
 * the canonical `default` label.
 */
export const EXTERNAL_PERMISSION_MODES = ['default', 'acceptEdits', 'bypassPermissions', 'plan'] as const;
export type ExternalPermissionMode = (typeof EXTERNAL_PERMISSION_MODES)[number];

/** Zod enum over {@link EXTERNAL_PERMISSION_MODES} — the validation primitive for
 *  the reduced external/SDK mode vocabulary (parity: Claude's
 *  `externalPermissionModeSchema`). Backs {@link isExternalPermissionMode} so the
 *  guard and any external-config schema share one source of truth. */
export const externalPermissionModeSchema = z.enum(EXTERNAL_PERMISSION_MODES);

/**
 * Type guard: is `mode` already a value in the external vocabulary? `acceptEdits`,
 * `bypassPermissions`, and `plan` are shared between the internal and external sets,
 * so they pass through {@link toExternalPermissionMode} unchanged. AMI-internal-only
 * modes (`ask`, `auto-allow`, `deny-all`) are not external and must be mapped.
 */
export function isExternalPermissionMode(mode: string): mode is ExternalPermissionMode {
  return externalPermissionModeSchema.safeParse(mode).success;
}

/**
 * Collapse an INTERNAL {@link PermissionMode} onto the {@link ExternalPermissionMode}
 * surfaced to external/SDK consumers (parity: Claude's `toExternalPermissionMode`).
 * `ask`/`undefined` → `default` (the neutral mode); the three shared modes pass
 * through unchanged; the remaining internal-only modes map to their closest external
 * equivalent — a blanket `auto-allow` reads as `bypassPermissions` (both skip the
 * prompt), and a blanket `deny-all` has no external counterpart so it reads as the
 * conservative `default`.
 */
export function toExternalPermissionMode(mode: PermissionMode | undefined): ExternalPermissionMode {
  // isDefaultMode covers both undefined and the neutral `ask` mode.
  if (mode === undefined || isDefaultMode(mode)) return 'default';
  if (isExternalPermissionMode(mode)) return mode;
  // Remaining internal-only modes after the default + shared-mode checks.
  return mode === 'auto-allow' ? 'bypassPermissions' : 'default';
}

/**
 * The payload delivered to a {@link PermissionModeChangeListener} whenever the
 * permission mode is mutated through {@link PermissionManager}'s single choke point.
 * Carries both the raw internal modes and their externalized forms so a listener can
 * audit the security-critical transition (e.g. into `bypassPermissions`/`auto-allow`)
 * without re-deriving the external label. `reason` names the mutation path that
 * triggered it. Parity: Claude's `notifyPermissionModeChanged` single-choke-point
 * notification (state/onChangeAppState.ts, utils/sessionState.ts) — every path that
 * changes the mode fires this, so no path can silently escalate privileges.
 */
export interface PermissionModeChange {
  /** Mode in effect before this change. */
  previousMode: PermissionMode;
  /** Mode now in effect. */
  mode: PermissionMode;
  /** {@link toExternalPermissionMode} of `previousMode`. */
  previousExternalMode: ExternalPermissionMode;
  /** {@link toExternalPermissionMode} of `mode`. */
  externalMode: ExternalPermissionMode;
  /** The mutation path that triggered the change (`setMode`, `exitPlanMode`,
   *  `deserialize`, `init`), for audit provenance. */
  reason: string;
}

/** A listener invoked on every real permission-mode transition. */
export type PermissionModeChangeListener = (change: PermissionModeChange) => void;

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
  const origin = rule.source ? ` (from ${permissionRuleSourceDisplayString(rule.source)})` : '';
  // Natural-language prompt rules describe an action in prose, not a glob — render
  // them as the description so the preview reads naturally.
  const promptDescription = extractPromptDescription(rule.pattern);
  if (promptDescription) {
    return `${verb} ${tool} commands described as "${promptDescription}"${origin}`;
  }
  let scope: string;
  if (rule.pattern && rule.pattern !== '*') {
    scope = ` matching "${rule.pattern}"`;
  } else if (rule.paramPattern && Object.keys(rule.paramPattern).length > 0) {
    scope = ` with ${Object.entries(rule.paramPattern).map(([k, v]) => `${k}="${v}"`).join(', ')}`;
  } else {
    scope = ' calls';
  }
  return `${verb} ${tool}${scope}${origin}`;
}

/**
 * The SINGLE canonical "is this an overly-broad bash allow?" predicate (shared by
 * core {@link PermissionManager}'s load-time strip and cli config-resolver's
 * warning/opt-in strip — no second definition, cf. No-Duplication Rule). True when
 * a bash (or all-tools `*`) `allow` rule has NO content restriction and therefore
 * auto-approves EVERY bash command — the YOLO / bypass-permissions equivalent:
 * `rule.pattern ? … : true` in {@link PermissionManager.check} matches a
 * pattern-less rule against every command, as does an empty or `*` pattern, and a
 * `*`-tool allow spans bash too. A param-scoped rule is NOT broad (it still
 * constrains the input), nor is any deny / non-bash-and-non-`*` rule. Mirrors
 * Claude's `isOverlyBroadBashAllowRule` (utils/permissions/permissionSetup.ts
 * L349-355 — `Bash`, `Bash(*)`, `Bash()` all parse to a Bash allow with no
 * ruleContent). Interpreter-prefix forms (`python:*`, `node*`) are a SEPARATE,
 * broader hazard class handled by the cli detector on top of this predicate.
 */
export function isOverlyBroadBashAllowRule(rule: PermissionRule): boolean {
  if (rule.action !== 'allow') return false;
  if (rule.paramPattern && Object.keys(rule.paramPattern).length > 0) return false;
  const targetsBash = normalizeToolName(rule.tool) === 'bash' || rule.tool === '*';
  if (!targetsBash) return false;
  const p = rule.pattern;
  return p === undefined || p === '' || p.trim() === '*';
}

/**
 * Structured description of an active permission rule that is effectively
 * equivalent to YOLO / bypass-permissions, for a UI/`/doctor` warning. Mirrors
 * Claude's `DangerousPermissionInfo` (utils/permissions/permissionSetup.ts): a
 * display string for the rule and a human-readable string for where it came from,
 * so the warning can say exactly which rule from which source grants full
 * autonomy. `ruleValue`/`source` are kept for callers that want to remediate
 * (remove/narrow) the rule, not just display it.
 */
export interface DangerousPermissionInfo {
  /** The parsed rule value (`{toolName, ruleContent?}`) for programmatic removal. */
  ruleValue: PermissionRuleValue;
  /** The rule's origin tier (or `'cliArg'` for a `--allow` flag spec). */
  source: PermissionRuleSource;
  /** Display form of the rule, e.g. `bash(*)`. */
  ruleDisplay: string;
  /** Human-readable origin, e.g. `project settings` or `--allow`. */
  sourceDisplay: string;
}

/** True when a PARSED rule value (a `Tool(content)` spec, as from a `--allow`
 *  flag) is an overly-broad bash allow — the value-level twin of
 *  {@link isOverlyBroadBashAllowRule}, which takes a loaded {@link PermissionRule}.
 *  A pattern-less / empty / `*` bash (or all-tools `*`) spec auto-approves every
 *  bash command. */
function isOverlyBroadBashAllowRuleValue(value: PermissionRuleValue): boolean {
  const targetsBash = normalizeToolName(value.toolName) === 'bash' || value.toolName === '*';
  if (!targetsBash) return false;
  const c = value.ruleContent;
  return c === undefined || c === '' || c.trim() === '*';
}

/**
 * Scan loaded `allow` rules AND raw CLI `--allow` tool specs for overly-broad bash
 * grants (the YOLO / bypass-permissions equivalent) and return a structured
 * {@link DangerousPermissionInfo} per hit, so a UI / `/permissions` surface can
 * warn the user that a rule silently grants full shell autonomy. Mirrors Claude's
 * `findOverlyBroadBashPermissions` (utils/permissions/permissionSetup.ts
 * L376-412): it checks both persisted rules and the ephemeral `--allow` flag list,
 * tagging the latter with the `cliArg` source and `--allow` display. Uses the
 * single canonical breadth detector ({@link isOverlyBroadBashAllowRule} /
 * {@link isOverlyBroadBashAllowRuleValue}) — no second classifier.
 */
export function findOverlyBroadBashPermissions(
  rules: readonly PermissionRule[],
  cliAllowedTools: readonly string[] = [],
): DangerousPermissionInfo[] {
  const found: DangerousPermissionInfo[] = [];
  for (const rule of rules) {
    if (isOverlyBroadBashAllowRule(rule)) {
      found.push({
        ruleValue: { toolName: rule.tool, ruleContent: rule.pattern },
        source: rule.source ?? 'session',
        ruleDisplay: 'bash(*)',
        sourceDisplay: permissionRuleSourceDisplayString(rule.source),
      });
    }
  }
  for (const spec of cliAllowedTools) {
    const parsed = permissionRuleValueFromString(spec);
    if (isOverlyBroadBashAllowRuleValue(parsed)) {
      found.push({
        ruleValue: parsed,
        source: 'cliArg',
        ruleDisplay: 'bash(*)',
        sourceDisplay: '--allow',
      });
    }
  }
  return found;
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

/**
 * Context handed to an {@link AutoModeSafetyClassifier} for a tool call the
 * deterministic engine would otherwise auto-approve in an autonomous mode.
 * `staticDecision` is always `'allow'` (the classifier is only consulted on the
 * auto-approve path), carried so a classifier can key off it and so the shape can
 * grow without a breaking change.
 */
export interface AutoModeSafetyContext {
  toolName: string;
  input: Record<string, unknown>;
  /** The autonomous mode that produced the auto-approval. */
  mode: PermissionMode;
  /** The deterministic verdict the classifier is being asked to confirm (always `allow`). */
  staticDecision: 'allow';
}

/**
 * A classifier's verdict on a would-be auto-approval. `allow` (or returning
 * `undefined` from the classifier) leaves the deterministic auto-approval intact;
 * `ask` downgrades it to an interactive prompt and `deny` blocks the call outright,
 * each carrying an optional human-readable `reason` recorded against the denial log.
 */
export interface AutoModeSafetyVerdict {
  /**
   * `allow` confirms the deterministic auto-approval; `ask` downgrades it to a
   * prompt; `deny` blocks it. `passthrough` is an explicit ABSTAIN ("no opinion —
   * defer to the deterministic decision"), the first-class spelling of what a
   * classifier previously could only express by returning `undefined`. It lets a
   * composable/layered classifier decline to weigh in without being read as a
   * positive `allow`. Mirrors Claude Code's `PermissionResult.behavior:
   * 'passthrough'` and consolidates with the {@link PermissionVerdict} /
   * {@link HookVerdict} `passthrough` abstain vocabulary (one canonical word for
   * "defer" across the engine). Treated identically to `allow`/`undefined` by
   * {@link PermissionManager.check}: the deterministic auto-approval stands and no
   * downgrade reason or denial is recorded.
   */
  decision: 'allow' | 'ask' | 'deny' | 'passthrough';
  /** Why the classifier downgraded the approval (recorded for the UI / denial log). */
  reason?: string;
}

/**
 * Optional, injectable safety classifier consulted by {@link PermissionManager.check}
 * on the AUTONOMOUS auto-approve path (`auto-allow` / `bypassPermissions` / inherited
 * plan-bypass / screened `auto`) BEFORE a tool call is silently allowed. It can
 * DOWNGRADE an auto-approval to a prompt (`ask`) or a block (`deny`) with a recorded
 * reason — it can never UPGRADE an `ask`/`deny` to `allow`. Mirrors Claude Code's
 * transcript/"auto-mode" safety classifier (`classifierApprovals.ts` /
 * `useCanUseTool` yolo classifier): a model- or rule-backed check that gates
 * autonomous approvals. The deterministic engine is the floor; this seam only ever
 * TIGHTENS it. Returning `undefined`, resolving to `allow`, or throwing all FAIL OPEN
 * to the deterministic auto-approval (the static classifier remains authoritative),
 * matching Claude's "classifier error → fall back to the static decision" behavior.
 * The common/engine layer injects a concrete classifier via
 * {@link PermissionManager.setAutoModeSafetyClassifier}; unset → the seam is inert and
 * autonomous behavior is byte-for-byte unchanged.
 */
export type AutoModeSafetyClassifier = (
  ctx: AutoModeSafetyContext,
) => AutoModeSafetyVerdict | undefined | Promise<AutoModeSafetyVerdict | undefined>;

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
  'credential', 'systemDir', 'suspiciousPath', 'agentConfig', 'buildConfig', 'destructive', 'codeExec', 'toctou', 'argLeak',
] as const;
export type SafetyCheckKind = (typeof SAFETY_CHECK_KINDS)[number];

/**
 * Map a permission BEHAVIOR enum (`allow`/`deny`/`ask`) to its grammatical past-tense
 * prose, so a decision can be narrated in a sentence — "<hook> allowed this action",
 * "<rule> denied this action", "<hook> asked for confirmation for this action".
 * Mirrors Claude's `getRuleBehaviorDescription` (utils/permissions/PermissionResult.ts),
 * where `ask` (the default) reads "asked for confirmation for". A single canonical
 * mapping so callers never hand-roll the allow→"allowed" / deny→"denied" conversion.
 *
 * The parameter is intentionally widened to `string` so this is a FAIL-SAFE boundary:
 * any value outside `allow`/`deny` — including a future/unknown behavior a newer
 * settings file might carry — degrades to the most conservative "asked for
 * confirmation for" prose rather than throwing or mislabeling it as an allow (parity
 * with Claude's `getRuleBehaviorDescription` `default` arm).
 */
export function getRuleBehaviorDescription(behavior: string): string {
  switch (behavior) {
    case 'allow':
      return 'allowed';
    case 'deny':
      return 'denied';
    default:
      return 'asked for confirmation for';
  }
}

/**
 * The CATEGORY of a tool for permission-prompt phrasing, mirroring Claude's
 * PermissionRuleExplanation `toolType` (`'tool' | 'command' | 'edit' | 'read'`):
 * it shapes the NOUN in a confirmation/denial message ("…for this command" vs
 * "…for this edit"/"…for this read") so the prompt reads naturally for the tool
 * being gated instead of always saying "command"/"action". Distinct from the
 * tool-search {@link getToolCategory} taxonomy, which classifies tools for
 * discovery and takes a full tool object — this one keys off the tool NAME alone
 * so it is usable anywhere a permission decision is narrated.
 */
export type PermissionPromptToolType = 'command' | 'edit' | 'read' | 'tool';

/**
 * Classify a tool by name into its {@link PermissionPromptToolType} for prompt
 * wording. Reuses the permission engine's own read/mutating tool-name sets (no
 * parallel list, cf. No-Duplication Rule): bash → `'command'`, an editing/writing
 * tool → `'edit'`, a read-only tool → `'read'`, anything else → a generic
 * `'tool'`. Mirrors Claude's per-category phrasing in PermissionRuleExplanation.
 */
export function permissionPromptToolType(toolName: string): PermissionPromptToolType {
  const tool = normalizeToolName(toolName);
  if (tool === 'bash') return 'command';
  if (MUTATING_RISK_TOOLS.has(tool)) return 'edit';
  if (READ_ONLY_RISK_TOOLS.has(tool)) return 'read';
  return 'tool';
}

/** The noun a permission message uses for a {@link PermissionPromptToolType}
 *  (`'tool'` → "tool call", every other category verbatim). */
function permissionPromptToolNoun(toolType: PermissionPromptToolType): string {
  return toolType === 'tool' ? 'tool call' : toolType;
}

/**
 * Build a human-readable explanation from a structured decision reason. When a
 * {@link PermissionPromptToolType} is supplied, the generic noun is phrased per
 * tool category ("for this edit" / "for this read" / "for this tool call") —
 * parity with Claude's PermissionRuleExplanation, which interpolates the tool
 * category into every message. Omitting it preserves the default "command"
 * (rule) / "action" (mode/hook/other) wording for callers that have no tool
 * context.
 */
export function createPermissionRequestMessage(
  reason: PermissionDecisionReason,
  toolType?: PermissionPromptToolType,
): string {
  const ruleNoun = toolType ? permissionPromptToolNoun(toolType) : 'command';
  const genericNoun = toolType ? permissionPromptToolNoun(toolType) : 'action';
  switch (reason.type) {
    case 'hardline':
      return `Blocked: ${reason.description}`;
    case 'rule': {
      const from = reason.source ? ` from ${reason.source}` : '';
      const which = reason.ruleValue ? ` "${reason.ruleValue}"` : '';
      return `Permission rule${which}${from} requires ${reason.action === 'deny' ? 'denial' : 'approval'} for this ${ruleNoun}`;
    }
    case 'mode':
      return `Current permission mode (${reason.mode}) requires approval for this ${genericNoun}`;
    case 'safetyCheck':
      return reason.message;
    case 'classifier':
      return `The safety classifier${reason.name ? ` (${reason.name})` : ''} flagged this action${reason.reason ? `: ${reason.reason}` : ''}`;
    case 'workingDir':
      return reason.reason;
    case 'hook': {
      const verb = reason.behavior ? getRuleBehaviorDescription(reason.behavior) : 'requires approval for';
      return `Hook "${reason.hookName}" ${verb} this ${genericNoun}${reason.reason ? `: ${reason.reason}` : ''}`;
    }
    case 'sandboxOverride':
      return 'This action requires permission to bypass the sandbox';
    case 'other':
      return reason.message ?? `This ${genericNoun} requires approval`;
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

// ---------------------------------------------------------------------------
// 'auto' permission mode — screened autonomy (parity: Claude Code's AutoMode)
// ---------------------------------------------------------------------------

/**
 * The verdict of the per-call screen the `auto` permission mode applies to EVERY
 * tool call: `allow` runs it without a prompt, `deny` blocks it (the engine
 * reports the block to the model, which may try a different approach — Claude's
 * "block what is judged risky" fallback). This is a middle ground between `ask`
 * (prompt on everything) and `auto-allow` (run everything except hardline): an
 * autonomous run proceeds without prompts yet is bounded by a per-call risk +
 * prompt-injection screen.
 */
export interface AutoModeScreen {
  decision: 'allow' | 'deny';
  /** Deterministic risk level of the call (reuses {@link classifyPermissionRisk}). */
  riskLevel: RiskLevel;
  /** True when a prompt-injection pattern was found in the tool input. */
  injection: boolean;
  /** Human-readable justification for the decision (surfaced in the denial log). */
  reason: string;
}

/**
 * Swarm/agent-coordination tools that the `auto` permission-mode screen treats as
 * safe-to-run WITHOUT re-screening. These tools do not themselves touch the
 * filesystem/network in a way the per-call risk model should re-judge: they spawn
 * subagents ({@link task}), create/delete teammate inboxes (`team_create` /
 * `team_delete`), route a message to another agent (`send_message`), or launch a
 * workflow — and every action those delegates take is INDEPENDENTLY permission-checked
 * when the delegate runs. Re-screening the coordinator here is both redundant and
 * actively harmful: a `send_message`/`task` payload legitimately carries free-form
 * task text that can trip the prompt-injection heuristic ({@link detectPromptInjection}),
 * which would stall autonomous swarm coordination that is in fact safe. Mirrors
 * Claude Code's `SAFE_YOLO_ALLOWLISTED_TOOLS` (classifierDecision.ts) and its
 * documented rationale that spawned subagents/teammates carry their own per-call
 * permission checks. Names are AMI-canonical (post {@link normalizeToolName}), so a
 * legacy/aliased spelling (`Task` → `task`) is covered too.
 */
export const AUTO_MODE_SAFE_TOOLS: ReadonlySet<string> = new Set([
  'send_message',
  'task',
  'team_create',
  'team_delete',
  'workflow',
]);

/**
 * True when `toolName` is a coordination/orchestration tool the `auto` mode screen
 * may allow without re-judging ({@link AUTO_MODE_SAFE_TOOLS}). The name is
 * canonicalized first so a legacy/cross-framework spelling (e.g. Claude's `Task`)
 * is recognized as AMI's `task`.
 */
export function isAutoModeSafeTool(toolName: string): boolean {
  return AUTO_MODE_SAFE_TOOLS.has(normalizeToolName(toolName));
}

/**
 * Gather the string-valued fields of a tool input into one blob so a
 * prompt-injection scan can inspect everything the model proposed to execute
 * (the bash `command`, an edit's `content`/`new_string`, a URL, etc.) rather than
 * just one well-known field. Non-string values are ignored.
 */
function collectToolInputText(input: unknown): string {
  if (typeof input === 'string') return input;
  if (!input || typeof input !== 'object') return '';
  const parts: string[] = [];
  for (const v of Object.values(input as Record<string, unknown>)) {
    if (typeof v === 'string') parts.push(v);
  }
  return parts.join('\n');
}

/**
 * Screen a single pending tool call for the `auto` permission mode: run it when
 * judged safe, block it when judged risky. A call is blocked when its input
 * carries a prompt-injection pattern ({@link detectPromptInjection}) OR when its
 * deterministic risk is HIGH (destructive/irreversible — {@link classifyPermissionRisk}).
 * LOW (read-only) and MEDIUM (recoverable mutation) calls run without a prompt.
 * Pure and side-effect free; reuses the existing classifiers so the screen can
 * never drift from the permission engine's own risk model (No-Duplication Rule).
 * Parity: Claude Code's auto mode (per-call risk + prompt-injection gate with a
 * block-and-retry fallback, AutoModeOptInDialog.tsx).
 */
export function screenAutoModeToolCall(toolName: string, input: unknown): AutoModeScreen {
  // Coordination/orchestration tools skip the screen: they only delegate work to
  // subagents/teammates/workflows, each of which is INDEPENDENTLY permission-checked
  // when it runs, so re-judging the coordinator is redundant — and the free-form task
  // text these calls carry would otherwise trip the injection/risk heuristics and
  // stall safe autonomous coordination. Parity: Claude's SAFE_YOLO_ALLOWLISTED_TOOLS.
  if (isAutoModeSafeTool(toolName)) {
    return {
      decision: 'allow',
      riskLevel: 'LOW',
      injection: false,
      reason: 'allowed: coordination tool (delegates to self-checking subagents)',
    };
  }
  const injection = detectPromptInjection(collectToolInputText(input));
  if (injection.suspicious) {
    return {
      decision: 'deny',
      riskLevel: 'HIGH',
      injection: true,
      reason: `blocked: tool input matches a prompt-injection pattern (${injection.pattern})`,
    };
  }
  const risk = classifyPermissionRisk(toolName, input);
  if (risk.riskLevel === 'HIGH') {
    return {
      decision: 'deny',
      riskLevel: 'HIGH',
      injection: false,
      reason: `blocked: ${risk.risk}`,
    };
  }
  return {
    decision: 'allow',
    riskLevel: risk.riskLevel,
    injection: false,
    reason: risk.risk,
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
  /**
   * When `source === 'classifier'`, WHICH classifier produced it — `'bash'` for the
   * deterministic bash command classifier, `'auto-mode'` for the injected autonomous
   * safety classifier. Lets `/permissions` show HOW a call was gated (parity with
   * Claude's `ClassifierApproval.classifier` discriminant). Absent for non-classifier
   * sources.
   */
  classifier?: 'bash' | 'auto-mode';
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
  /**
   * The outcome of a single permission evaluator.
   *
   * `passthrough` is a FOURTH, NEUTRAL outcome distinct from a terminal
   * `allow`/`deny`/`ask`: a layered/composable evaluator can observe a tool call
   * and DECLINE to decide ("no opinion — defer to the next evaluator"), without
   * pinning the result to `allow`. It is the identity element of the
   * {@link mostRestrictiveVerdict} lattice — a `passthrough` never overrides a
   * real verdict and is always overridden by one, so a chain of evaluators can
   * abstain until one of them actually decides. Mirrors Claude Code's
   * `PermissionResult.behavior: 'passthrough'` (types/permissions.ts) and
   * consolidates with the {@link HookVerdict} `passthrough` state in
   * `hook-protection.ts` (one canonical abstain vocabulary across the engine).
   */
  decision: 'allow' | 'deny' | 'ask' | 'passthrough';
  rule?: string;
  source?: string;
  reason?: string;
  /**
   * Which classifier produced this verdict, when it came from one (vs a static
   * rule): `'bash'` for the deterministic bash command classifier, `'auto-mode'`
   * for the injected autonomous-safety classifier. Lets a UI explain HOW a tool
   * use was gated/approved — parity with Claude's `ClassifierApproval.classifier`
   * discriminant (utils/classifierApprovals.ts). Absent for static-rule verdicts.
   */
  classifier?: 'bash' | 'auto-mode';
  /**
   * For a `'bash'`-classifier verdict, the specific matched rule/pattern the
   * classifier decided under (parity: `ClassifierApproval.matchedRule`), so a
   * reviewer can answer "which rule approved this command". Absent otherwise.
   */
  matchedRule?: string;
}

export function verdictRank(decision: string): number {
  switch (decision) {
    case 'deny': return 3;
    case 'modify': return 2;
    case 'ask': return 2;
    case 'allow': return 1;
    // `passthrough` (abstain / "no opinion") is the lattice's neutral element:
    // strictly below `allow`, so it never holds over a real verdict and is always
    // overridden by one. An unknown decision shares this floor (fail-neutral).
    case 'passthrough': return 0;
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

/**
 * Combine a chain of layered permission evaluators into one decision under the
 * strictest-wins lattice. `passthrough` (abstain) is the neutral element: it never
 * holds over a real verdict and is always overridden by one, so a chain of
 * evaluators can defer until one actually decides. When EVERY evaluator abstains
 * (all `passthrough`) the result is itself `passthrough` — "no layer had an
 * opinion", which the caller resolves against its own default (parity with
 * Claude's passthrough falling through to the normal permission flow). An empty
 * chain is treated as abstain for the same reason.
 */
export function mostRestrictiveVerdict(verdicts: PermissionVerdict[]): PermissionVerdict {
  if (verdicts.length === 0) return { decision: 'passthrough' };
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

/**
 * Flag policy for a {@link SAFE_COMMANDS} entry (U003 fail-closed model).
 *  - `allowed`/`blocked` — the read-only / never-safe flag sets.
 *  - `strict` — reject any flag outside `allowed` (fall through to a prompt).
 *  - `argTypes` — getopt-accurate per-flag value typing (see {@link validateTypedCommandFlags}).
 *  - `dangerousArgs` — custom per-command danger predicate.
 *  - `respectsDoubleDash` — whether POSIX `--` ends option scanning (default true).
 *  - `allowAllFlags` — the command's ENTIRE flag surface is read-only, so every flag
 *    except those in `blocked` is accepted. This is the EXPLICIT opt-in that Claude's
 *    CommandConfig model requires (readOnlyValidation.ts L39-56): a SAFE command must
 *    now declare its flag policy, and a command with no entry is positional-only
 *    (fail-closed), rather than the old fail-OPEN where a missing entry waved every
 *    flag through. Marks genuinely all-flags-safe read utilities (ls/cat/df/…) so the
 *    inverted default never regresses them.
 */
type SafeCommandFlagDef = {
  allowed: Set<string>;
  blocked: Set<string>;
  strict?: boolean;
  argTypes?: Record<string, FlagArgType>;
  dangerousArgs?: (args: string[]) => boolean;
  respectsDoubleDash?: boolean;
  allowAllFlags?: boolean;
};

const SAFE_COMMAND_FLAGS: Record<string, SafeCommandFlagDef> = {
  // ── All-flags-safe read-only utilities (U003 allowAllFlags opt-in) ────────────
  // Each command's entire flag surface only reads/formats/inspects — no flag writes
  // or executes. An explicit entry is REQUIRED under the fail-closed default below;
  // allowAllFlags makes that entry accept any flag so inverting the default never
  // regresses these. Commands with a WRITE-capable flag surface (hostname below) get
  // a restrictive allowlist instead.
  ls: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  cat: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  head: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  tail: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  wc: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  echo: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  pwd: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  whoami: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  uname: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  cd: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  id: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  df: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  du: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  stat: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  readlink: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  basename: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  dirname: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  uniq: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  diff: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  tr: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  cut: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  less: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  more: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  test: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  true: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  false: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  type: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  seq: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  expr: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  printf: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  comm: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  cmp: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  numfmt: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  getconf: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  tsort: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  pr: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  fmt: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  // `dotnet`: accept its flag SHAPE here (so a read-only query is not rejected by the
  // fail-closed default); the real restriction to read-only SDK queries is enforced
  // by the isDotnetSafe positional/flag check in classifyBashCommand.
  dotnet: { allowed: new Set([]), blocked: new Set([]), allowAllFlags: true },
  // `hostname`: identification flags (-f/-i/-s/-d/…) are read-only, but `-b`/`--boot`
  // SET the hostname from boot config and `-F`/`--file` SET it from a file — writes
  // that must prompt. Strict allowlist (U003): any flag outside the read set (notably
  // the set flags) falls through to a prompt instead of the old fail-OPEN auto-allow.
  hostname: {
    allowed: new Set([
      '-a', '--alias', '-A', '--all-fqdns', '-d', '--domain',
      '-f', '--fqdn', '--long', '-i', '--ip-address', '-I', '--all-ip-addresses',
      '-s', '--short', '-y', '--yp', '--nis', '-h', '--help', '-V', '--version',
    ]),
    blocked: new Set(['-b', '--boot', '-F', '--file']),
    strict: true,
  },
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
 *   - `network-path`:    a UNC / network-share path (`//server`, `\\server`) or a
 *     WebDAV credential-leak marker (`@SSL@`, `DavWWWRoot`). Resolving one can leak
 *     NTLM/host credentials to a remote server, and its real target lives off the
 *     local filesystem where the workspace/forbidden guards cannot reason about it.
 *   - `command-substitution`: a backtick (`` ` ``) escape embedded in the path —
 *     the shell runs the backticked text as a command at execution time, so the
 *     validated literal and the real target diverge (a TOCTOU gap `$(...)` already
 *     covers under `shell-expansion`, but backtick substitution has no `$`).
 */
export type PathExpansionHazard =
  | 'shell-expansion'
  | 'tilde-variant'
  | 'network-path'
  | 'command-substitution';

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
  // UNC / network-share prefix (`//server`, `\\server`) or a WebDAV credential-leak
  // marker (`@SSL@`, `DavWWWRoot`). A normal POSIX path begins with a SINGLE slash,
  // so a leading double slash/backslash is the UNC shape Claude blocks before
  // resolution (pathValidation.ts L1098-L1114) — resolving it can leak NTLM/host
  // credentials to a remote server and its target lives off the local filesystem.
  if (/^(?:\/\/|\\\\)/.test(trimmed) || /@SSL@|DavWWWRoot/i.test(trimmed)) {
    return 'network-path';
  }
  // A backtick escape runs the backticked text as a command at execution time
  // (`/project/`whoami`/x`), so the validated literal and the real target diverge.
  // `$(...)` substitution is already caught below by the `$` test.
  if (trimmed.includes('`')) {
    return 'command-substitution';
  }
  if (trimmed.includes('$') || trimmed.includes('%') || trimmed.startsWith('=')) {
    return 'shell-expansion';
  }
  return null;
}

/**
 * Best-effort strip of the shell-expansion / command-substitution constructs that
 * make a path {@link detectPathExpansionHazard unvalidatable}, yielding a plain
 * LITERAL "guess" of what the path might resolve to. Used ONLY to run a
 * deny-side (forbidden-path) re-check over an obscured path before it is routed to
 * a prompt — never to grant access. Mirrors Claude's `checkDenyRuleForGuessedPath`
 * (PowerShellTool/pathValidation.ts L984-L1008): an obscured path must not be able
 * to DOWNGRADE a hard credential/system deny into a mere "ask" by hiding the
 * forbidden segment behind a backtick / `$(…)` / `${VAR}` / `$VAR` / `%VAR%` /
 * leading-`=` construct — which, in an auto-allow session, an "ask" verdict would
 * then silently grant.
 *
 * Removes (in order): backtick-delimited runs, `$(…)` substitutions, `${…}` and
 * `$VAR` variable refs, `%VAR%` (Windows) refs, and a single leading `=`
 * (zsh equals-expansion). Returns the collapsed remainder. Pure, no I/O. This is a
 * guess heuristic, never a security boundary — the authoritative gate is still the
 * hazard→prompt fall-through for anything the guess does not catch.
 */
export function stripPathObscuring(rawPath: string): string {
  return rawPath
    // Backtick command substitution: `` `whoami` `` → removed entirely.
    .replace(/`[^`]*`/g, '')
    // `$(…)` substitution (single level is enough for a best-effort guess).
    .replace(/\$\([^)]*\)/g, '')
    // `${VAR}` references.
    .replace(/\$\{[^}]*\}/g, '')
    // Bare `$VAR` references.
    .replace(/\$[A-Za-z_][A-Za-z0-9_]*/g, '')
    // Windows `%VAR%` references.
    .replace(/%[^%]*%/g, '')
    // zsh equals-expansion: a single leading `=`.
    .replace(/^=/, '');
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
  switch (hazard) {
    case 'tilde-variant':
      return 'Tilde expansion variants (~user, ~+, ~-) in paths require manual approval';
    case 'network-path':
      return 'UNC / remote network-share paths require manual approval (they can leak '
        + 'credentials to a remote server and resolve outside the local filesystem)';
    case 'command-substitution':
      return 'Backtick command substitution in paths requires manual approval';
    case 'shell-expansion':
    default:
      return 'Shell expansion syntax in paths requires manual approval';
  }
}

/** Safe target commands for xargs — only these can follow xargs. */
const XARGS_SAFE_TARGETS = new Set([
  // `printf`, like `echo`, only writes to stdout (no filesystem mutation), so an
  // `… | xargs printf` pipeline is read-only — parity with Claude's xargs target
  // allowlist (echo/printf/wc/grep/head/tail).
  'echo', 'printf', 'cat', 'ls', 'grep', 'wc', 'head', 'tail', 'basename', 'dirname',
]);

export function validateCommandFlags(cmd: string, args: string[]): boolean {
  const flagDef = SAFE_COMMAND_FLAGS[cmd];
  // U003 fail-closed default: a SAFE command with NO declared flag policy is
  // positional-only — any flag-shaped token falls through to a prompt instead of
  // being silently auto-approved. This inverts the old fail-OPEN (`return true`),
  // where a missing flag table waved every flag through, and mirrors Claude's
  // CommandConfig model (readOnlyValidation.ts L39-56) where a command with empty/
  // missing safeFlags rejects all flags unless it explicitly opts in via
  // `allowAllFlags`. Every current SAFE command declares a policy (an allow/block
  // table, argTypes, or allowAllFlags), so this guards future additions.
  if (!flagDef) return !args.some(a => FLAG_PATTERN.test(a));
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
      // allowAllFlags: the command's whole flag surface is read-only, so any flag
      // that survived the blocked check above is accepted (U003 explicit opt-in).
      if (flagDef.allowAllFlags) continue;
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
  flagDef: SafeCommandFlagDef,
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
  // Windows data-to-code bridge: `xargs` reads its target arguments from STDIN
  // (file contents / piped output), so a UNC path living INSIDE that data —
  // e.g. `cat list.txt | xargs cat` where list.txt holds `\\attacker\share\x` —
  // triggers an SMB/WebDAV fetch that {@link containsVulnerableUncPath} can never
  // see (the hostile path is in the stream, not the command string). Mirrors
  // Claude Code's getCommandAllowlist dropping `xargs` entirely on Windows
  // (tools/BashTool/readOnlyValidation.ts L1203-1209): never auto-allow an xargs
  // pipeline on win32 — it falls back to a prompt.
  if (process.platform === 'win32') return false;
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
 * True when a command cannot be tokenized by a real shell because a quote is left
 * open — an UNBALANCED single or double quote. A single-pass quote-state walker with
 * bash escape semantics: `\` escapes the next char OUTSIDE single quotes (inside
 * `'…'` a backslash is literal and cannot escape the closing quote); double quotes
 * are inert inside single quotes and vice-versa. If the scan ends still inside a
 * quote, the shell's own tokenizer would error — so the naive whitespace/regex
 * splitting AMI's classifier uses would see a DIFFERENT token stream than the shell.
 *
 * The permission classifier calls this to FAIL CLOSED: an unparseable command is
 * classified `'unsafe'` (→ an explicit prompt) rather than being split with naive
 * regexes and possibly auto-allowed. Mirrors Claude Code's "parse first, return
 * passthrough / require-permission when parsing fails" gate (BashTool
 * readOnlyValidation.ts: tryParseShellCommand → requires permission on failure).
 */
export function hasUnbalancedQuotes(command: string): boolean {
  let inSingle = false;
  let inDouble = false;
  let escaped = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i]!;
    if (escaped) { escaped = false; continue; }
    // A backslash escapes the next char everywhere EXCEPT inside single quotes,
    // where it is a literal (bash: single quotes have no escape mechanism).
    if (c === '\\' && !inSingle) { escaped = true; continue; }
    if (c === "'" && !inDouble) { inSingle = !inSingle; continue; }
    if (c === '"' && !inSingle) { inDouble = !inDouble; continue; }
  }
  return inSingle || inDouble;
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

/**
 * Typed safe-flag allowlists for the read-only docker subcommands that still take
 * flags. A subcommand match alone is NOT enough to auto-allow `docker logs` /
 * `docker inspect`: an unrecognized flag can smuggle behavior (and `--format` /
 * `-f` meanings differ per subcommand), so each known flag is pinned to its
 * arity — `'none'` (a boolean switch) or `'string'` (consumes one value). Any
 * flag NOT in the map falls through to a prompt (conservative). Subcommands absent
 * here (`ps`, `images`, `stats`, …) keep their existing name-only auto-allow.
 * Mirrors Claude's `DOCKER_READ_ONLY_COMMANDS` safeFlags
 * (utils/shell/readOnlyCommandValidation.ts).
 */
const DOCKER_SAFE_SUBCOMMAND_FLAGS: Record<string, Record<string, 'none' | 'string'>> = {
  logs: {
    '--follow': 'none', '-f': 'none',
    '--tail': 'string', '-n': 'string',
    '--timestamps': 'none', '-t': 'none',
    '--since': 'string', '--until': 'string',
    '--details': 'none',
  },
  inspect: {
    '--format': 'string', '-f': 'string',
    '--type': 'string',
    '--size': 'none', '-s': 'none',
  },
};

/**
 * True when every flag passed to a read-only docker subcommand is in its typed
 * safe-flag allowlist (positional args — container names/IDs — are always fine).
 * A `--flag=value` form carries its own value; a `'string'` flag in `--flag value`
 * form consumes the following token. Returns true for subcommands with no
 * flag constraints. See {@link DOCKER_SAFE_SUBCOMMAND_FLAGS}.
 */
function dockerReadOnlyFlagsSafe(sub: string, args: string[]): boolean {
  const allow = DOCKER_SAFE_SUBCOMMAND_FLAGS[sub];
  if (!allow) return true;
  for (let i = 0; i < args.length; i++) {
    const tok = args[i]!;
    if (!tok.startsWith('-')) continue; // positional (container/id) — allowed
    const eq = tok.indexOf('=');
    const name = eq >= 0 ? tok.slice(0, eq) : tok;
    const kind = allow[name];
    if (kind === undefined) return false; // unknown flag → not auto-safe
    if (kind === 'string' && eq < 0) i++; // consumes the next token as its value
  }
  return true;
}

/** Compound safe prefixes: "command subcommand" pairs that are safe. */
/**
 * GitHub CLI (`gh`) read-only subcommand groups — the `<group> <subcommand>`
 * pairs (the `gh` prefix stripped) that only display/list data. These feed
 * {@link SAFE_PREFIXES} (as `gh <pair>`) so the bash classifier can auto-approve
 * them, and {@link isGhInvocationReadOnly} re-checks the exact pair plus the
 * HOST/OWNER/REPO exfil guard. Mirrors Claude Code's GH_READ_ONLY_COMMANDS
 * (utils/shell/readOnlyCommandValidation.ts). Write/browser subcommands
 * (`gh pr create`, `gh pr merge`, `gh api`, `… --web`) are deliberately absent so
 * they fall through to a prompt.
 */
const GH_READ_ONLY_SUBCOMMANDS: ReadonlySet<string> = new Set([
  'pr view', 'pr list', 'pr diff', 'pr checks', 'pr status',
  'issue view', 'issue list', 'issue status',
  'repo view',
  'run list', 'run view',
  'auth status',
  'release list', 'release view',
  'workflow list', 'workflow view',
  'label list',
  'search repos', 'search issues', 'search prs', 'search commits', 'search code',
]);

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
  // GitHub CLI (`gh`) read-only subcommands — displays/lists only. Each is still
  // revalidated by {@link isGhReadOnlyCommandSafe} in the classifier before
  // auto-approval so the HOST/OWNER/REPO exfil guard ({@link ghArgsTargetForeignHost})
  // can veto a `--repo host/owner/repo` that would reach a foreign host. Mirrors
  // Claude's GH_READ_ONLY_COMMANDS catalog (readOnlyCommandValidation.ts).
  ...[...GH_READ_ONLY_SUBCOMMANDS].map(s => `gh ${s}`),
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
  // Extended curated allowlist (parity with Claude's SAFE_ENV_VARS, bashPermissions):
  // pytest configuration — disable plugin autoload / enable debug tracing.
  'PYTEST_DISABLE_PLUGIN_AUTOLOAD', 'PYTEST_DEBUG',
  // Extended locale controls beyond the base LANG/LC_ALL/LC_CTYPE set.
  'LANGUAGE', 'LC_TIME', 'CHARSET',
  // Color / display tooling — purely cosmetic, never a code-exec or exfil vector.
  'LS_COLORS', 'LSCOLORS', 'GREP_COLORS', 'GCC_COLORS',
  'TIME_STYLE', 'BLOCK_SIZE', 'BLOCKSIZE',
  // Go build flags — select module/cgo/experiment behavior, not a secrets vector.
  'GO111MODULE', 'CGO_ENABLED', 'GOEXPERIMENT',
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
  // Same-UID heap/credential scraping vectors. A prompt-injected command running
  // as AMI's own UID can attach a debugger/tracer to the agent (or a peer) process
  // and read provider API keys (ANTHROPIC_API_KEY / Bearer tokens) straight out of
  // its heap, or read them via /proc/<pid>/mem. Claude Code closes this by calling
  // prctl(PR_SET_DUMPABLE,0) via Bun FFI (upstreamproxy.ts); AMI runs on the Node
  // runtime (no libc FFI), so it blocks the attach vectors unconditionally at the
  // permission boundary instead — the hardline block overrides every mode.
  { pattern: /\bgdb\b[^\n]*\s--?p(?:id)?\b/, description: 'gdb attach to a live process (in-memory credential scraping vector)' },
  { pattern: /\blldb\b[^\n]*\s(?:-p|--pid|--attach-pid|--attach-name|-n)\b/, description: 'lldb attach to a live process (in-memory credential scraping vector)' },
  { pattern: /\b(?:strace|ltrace)\b[^\n]*\s-p\b/, description: 'strace/ltrace attach to a live process (in-memory credential scraping vector)' },
  { pattern: /\/proc\/(?:\d+|self|\$\{?[\w}$]*)\/mem\b/, description: 'reading /proc/<pid>/mem (process-memory / credential scraping vector)' },
  { pattern: /\bprocess_vm_readv\b/, description: 'process_vm_readv cross-process memory read (credential scraping vector)' },
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
  /** PowerShell write cmdlets `acceptEdits` mode auto-approves without a prompt. */
  acceptEditsAutoAllowedPowerShellCmdlets: string[];
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
    acceptEditsAutoAllowedPowerShellCmdlets: [...getAutoAllowedPowerShellCmdlets('acceptEdits')],
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
 * `git clean` is only destructive when it actually removes files: it requires an
 * `-f`/`--force` and must NOT be a `-n`/`--dry-run` preview (which only lists what
 * WOULD be deleted — read-only). Shared by the informational warning engine AND
 * {@link PermissionManager.classifyBashCommand} so a dry-run is never escalated to a
 * destructive-approval prompt in either path (one canonical regex, no duplication).
 * Mirrors Claude's git-clean negative-lookahead on `-n`/`--dry-run` plus the
 * required `-f` (destructiveCommandWarning.ts L67-71).
 */
const GIT_CLEAN_FORCE_RE = /\bgit\s+clean\b(?![^;&|\n]*(?:-[a-zA-Z]*n|--dry-run))[^;&|\n]*-[a-zA-Z]*f/;

/**
 * Ordered bash destructive-command signatures, MOST-SPECIFIC-FIRST so the first
 * match wins (a recursive+force `rm` is reported ahead of a plain recursive one).
 * Purely informational — these drive the human-readable note surfaced in a
 * permission prompt ({@link getDestructiveCommandWarning}); they do NOT change
 * any allow/deny decision (that is owned by {@link PermissionManager.check} /
 * {@link detectHardlineCommand}). Parity with Claude's bash DESTRUCTIVE_PATTERNS
 * plus the PowerShell DESTRUCTIVE_PATTERNS (destructiveCommandWarning.ts).
 */
const DESTRUCTIVE_WARNING_PATTERNS: DestructivePattern[] = [
  // Git — data loss / hard to reverse.
  { pattern: /\bgit\s+reset\s+--hard\b/, warning: 'Note: may discard uncommitted changes' },
  { pattern: /\bgit\s+push\b[^;&|\n]*[ \t](--force|--force-with-lease|-f)\b/, warning: 'Note: may overwrite remote history' },
  { pattern: GIT_CLEAN_FORCE_RE, warning: 'Note: may permanently delete untracked files' },
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
  // PowerShell deletions: the SPECIFIC, flag-differentiated notes live in the
  // dedicated PowerShell-cmdlet block below (most-specific-first). Earlier this
  // block carried COARSE duplicates (`Remove-Item -(Recurse|Force)` collapsed to
  // one "recursively force-remove" note; a bare `Clear-Content` matched here and
  // shadowed the glob-specific note) — they preceded the fine-grained set and so
  // won the first-match, defeating the very differentiation that set exists for.
  // Removed; the fine-grained block is the single source for PowerShell notes.
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
  // PowerShell cmdlets — Windows counterpart of the POSIX destructive notes
  // (parity with Claude's PowerShellTool/destructiveCommandWarning.ts). Each is
  // anchored to a statement start (`^`, `|`, `;`, `&`, newline, or a block/group
  // opener `{`/`(`) so `git rm` / a `-Force` flag on an unrelated later statement
  // does not false-match; the `[^|;&\n}]*` spans stay inside one statement (the
  // `}` stopper ends a script block). Most-specific (recurse + force) first.
  { pattern: /(?:^|[|;&\n({])\s*(?:Remove-Item|ri|rd|rmdir|del|erase)\b[^|;&\n}]*-Recurse\b[^|;&\n}]*-Force\b/i, warning: 'Note: may recursively force-remove files' },
  { pattern: /(?:^|[|;&\n({])\s*(?:Remove-Item|ri|rd|rmdir|del|erase)\b[^|;&\n}]*-Force\b[^|;&\n}]*-Recurse\b/i, warning: 'Note: may recursively force-remove files' },
  { pattern: /(?:^|[|;&\n({])\s*(?:Remove-Item|ri|rd|rmdir|del|erase)\b[^|;&\n}]*-Recurse\b/i, warning: 'Note: may recursively remove files' },
  { pattern: /(?:^|[|;&\n({])\s*(?:Remove-Item|ri|rd|rmdir|del|erase)\b[^|;&\n}]*-Force\b/i, warning: 'Note: may force-remove files' },
  { pattern: /\bClear-Content\b[^|;&\n]*\*/i, warning: 'Note: may clear content of multiple files' },
  // General Clear-Content (single target or no wildcard): still real data loss —
  // it erases the file's contents in place — so it always carries a note; the
  // glob-specific "multiple files" note above wins first when a wildcard is present.
  { pattern: /\bClear-Content\b/i, warning: 'Note: may erase the contents of the target files' },
  { pattern: /\bFormat-Volume\b/i, warning: 'Note: may format a disk volume' },
  { pattern: /\bClear-Disk\b/i, warning: 'Note: may clear a disk' },
  { pattern: /\bClear-RecycleBin\b/i, warning: 'Note: permanently deletes recycled files' },
  { pattern: /\bStop-Computer\b/i, warning: 'Note: will shut down the computer' },
  { pattern: /\bRestart-Computer\b/i, warning: 'Note: will restart the computer' },
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
  // Windows shells + `.exe` variants. Like the Unix shells above, each execs its
  // `-Command`/`-c` (powershell/pwsh) or `/c`,`/k` (cmd) argument as a command,
  // so it must be unwrapped during decomposition — otherwise a denied inner
  // command hides behind `powershell -Command "<denied>"` / `pwsh -c …` /
  // `cmd /c …` / `bash.exe -c …` and the deny/allow rule never sees it. This is
  // the decomposition half of the protection BARE_SHELL_PREFIXES already gives
  // at the prefix-suggestion boundary (Claude's DANGEROUS_SHELL_PREFIXES).
  'cmd', 'cmd.exe', 'powershell', 'powershell.exe', 'pwsh', 'pwsh.exe', 'bash.exe',
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
      // Skip flags (e.g., sudo -u root, bash -c). Windows shells use `/`-prefixed
      // single-letter switches (`cmd /c`, `cmd /k`, and inert ones like `/q`), so
      // treat an exact `/x` (optionally `/x:value`) token as a flag too — without
      // this the loop would stop at `/c` and mis-read it as the wrapped command.
      // A real path operand (`/usr/bin`) is >1 char after the slash and is left
      // untouched (the switch pattern is anchored to a single letter).
      while (i < tokens.length && (tokens[i]!.startsWith('-') || /^\/[a-zA-Z]$/.test(tokens[i]!))) {
        const flag = tokens[i]!;
        i++;
        // Command-payload flags take the rest of the tokens as a command string —
        // parse it: `-c` (bash/zsh/pwsh), `-Command` (powershell/pwsh), and the
        // cmd-style `/c` / `/k`.
        if ((flag === '-c' || /^-command$/i.test(flag) || /^\/[ck]$/i.test(flag)) && i < tokens.length) {
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
        if (wrapper === 'srun' && SRUN_VALUE_FLAGS.test(flag)
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
      const spec = bashSpecFor(cmd);
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
      const spec = bashSpecFor(cmdTokens[0]!);
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
 * The file-mutating tools that `acceptEdits` mode auto-allows. SINGLE source of
 * truth shared by {@link PermissionManager.check} (the acceptEdits fast-path) and
 * {@link buildPermissionSuggestions} (the "switch to acceptEdits" blocked-edit
 * affordance), so the set a suggestion offers to unlock can never drift from the
 * set the mode actually unlocks.
 */
const ACCEPT_EDITS_EDIT_TOOLS: ReadonlySet<string> = new Set(['file_edit', 'file_write', 'notebook_edit', 'multi_edit']);

/**
 * True when EVERY sub-command of `command` is a benign filesystem-mutation base
 * command ({@link ACCEPT_EDITS_FS_COMMANDS}). Used by `acceptEdits` mode to
 * auto-allow file-edit-adjacent shell work (`mkdir`, `touch`, `mv`, `cp`,
 * `sed -i`) the way Claude does — without broadening to arbitrary commands.
 * A command substitution (`$(...)`, backticks, `$VAR`) is rejected outright so it
 * cannot smuggle a non-fs command into an otherwise-benign invocation.
 *
 * On Windows the bash tool runs PowerShell, so a single benign PowerShell write
 * cmdlet (`Set-Content`/`Add-Content`/`Out-File`/`Tee-Object`/`New-Item` file|dir)
 * is ALSO auto-allowed — the Windows counterpart of the POSIX set — via
 * {@link isAcceptEditsPowerShellWrite}, which fails closed and excludes
 * `Remove-Item` and `New-Item` link creation (parity: Claude's PowerShell
 * modeValidation write-cmdlet allowlist + link-creation detection).
 */
export function isAcceptEditsFilesystemMutation(command: string): boolean {
  const trimmed = command.trim();
  if (!trimmed) return false;
  if (containsShellSubstitution(trimmed) || trimmed.includes('`')) return false;
  const subs = splitCompoundCommand(trimmed);
  if (subs.length === 0) return false;
  const allBashFsMutations = subs.every(sub => {
    const base = stripEnvVarPrefix(sub, /* safeEnvOnly */ false).trim().split(/\s+/)[0];
    return !!base && ACCEPT_EDITS_FS_COMMANDS.has(base);
  });
  if (allBashFsMutations) return true;
  // Not an all-bash-fs compound — accept a lone benign PowerShell write cmdlet.
  return isAcceptEditsPowerShellWrite(trimmed);
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
 * Introspection accessor: the PowerShell edit-adjacent write CMDLETS a given
 * permission {@link PermissionMode} auto-approves without a prompt, SORTED for
 * stable display — the PowerShell counterpart of {@link getAutoAllowedCommands}
 * (which lists only bash base-commands). Only `acceptEdits` auto-allows a fixed
 * set (the SAME canonical {@link POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS} the runtime
 * {@link isAcceptEditsPowerShellWrite} gate uses — no duplicate table); every
 * other mode returns an empty list. Surfaced through
 * {@link getDefaultPermissionPatterns} (→ `/permissions defaults`) so a user can
 * see the PowerShell cmdlets acceptEdits waves through, alongside the bash set.
 */
export function getAutoAllowedPowerShellCmdlets(mode: PermissionMode): readonly string[] {
  return mode === 'acceptEdits' ? [...POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS].sort() : [];
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
 * Canonical, reusable command-identity predicates for the two command roots the
 * compound-command guards recognize specially: a directory change (`cd`) and a
 * `git` invocation. Before this, each guard ({@link isGitReadOnlyCommandSafe},
 * {@link countDirectoryChanges}, {@link commandHasCdWriteDesync},
 * {@link commandInvokesGit}) open-coded its own `cmd === 'cd' || cmd === 'pushd'
 * || …` / `cmd === 'git'` test, and the sets had DRIFTED (one counted `popd` but
 * not `set-location`; the others the reverse) so the same command was recognized
 * as a directory change by one guard and not another. These are the single
 * canonical identity checks every guard now shares, mirroring Claude's injectable
 * `CommandIdentityCheckers` (isNormalizedCdCommand / isNormalizedGitCommand,
 * bashCommandHelpers.ts) so directory-change / git recognition is consistent and
 * independently testable.
 *
 * `isNormalizedCdCommand` recognizes every directory-change verb: the POSIX
 * `cd`/`chdir`/`pushd`/`popd` (lowercase, as a real shell spells them) and the
 * PowerShell `Set-Location` cmdlet (case-insensitive, as PowerShell resolves it).
 */
export function isNormalizedCdCommand(token: string | undefined): boolean {
  if (!token) return false;
  return token === 'cd' || token === 'chdir' || token === 'pushd' || token === 'popd'
    || token.toLowerCase() === 'set-location';
}

/** `isNormalizedGitCommand` recognizes a bare `git` base token (env-var prefixes
 *  and `sudo`/`env` wrappers are peeled by {@link extractBaseCommands} before this
 *  predicate sees the token, so it only needs to match the normalized base word). */
export function isNormalizedGitCommand(token: string | undefined): boolean {
  return token === 'git';
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
    if (isNormalizedCdCommand(cmd)) hasCd = true;
    if (isNormalizedGitCommand(cmd)) {
      hasGit = true;
      if (!isGitInvocationReadOnly(tokens.slice(i))) return false;
    }
  }
  if (hasCd && hasGit) return false;
  return true;
}

/**
 * HOST/OWNER/REPO exfiltration guard for `gh` read-only commands. A normal repo
 * spec is `OWNER/REPO` (exactly one `/`). Any token (or `--flag=value` inline
 * value) that names a FOREIGN host — a URL scheme (`https://`, `git://`, …), an
 * SSH-style `git@host:owner/repo`, or a 3+-segment `host/owner/repo` (2+ slashes)
 * — redirects the read at an attacker-chosen remote and must NOT be auto-approved.
 * Mirrors Claude's `ghIsDangerousCallback` (readOnlyCommandValidation.ts).
 * `args` is the token list AFTER the `gh <group> <subcommand>` pair.
 */
export function ghArgsTargetForeignHost(args: string[]): boolean {
  for (const token of args) {
    if (!token) continue;
    // For a flag token, only its inline `=value` can carry a repo spec; a bare
    // flag (its value is the NEXT token) is inspected on that next iteration.
    let value = token;
    if (token.startsWith('-')) {
      const eqIdx = token.indexOf('=');
      if (eqIdx === -1) continue;
      value = token.slice(eqIdx + 1);
      if (!value) continue;
    }
    if (!value.includes('/') && !value.includes('://') && !value.includes('@')) continue;
    if (value.includes('://')) return true; // explicit URL scheme
    if (value.includes('@')) return true;   // ssh-style git@host:owner/repo
    const slashCount = (value.match(/\//g) || []).length;
    if (slashCount >= 2) return true;        // host/owner/repo (3+ segments)
  }
  return false;
}

/**
 * Is a single `gh` invocation (tokens beginning at the `gh` token) a genuinely
 * read-only form? True only when the leading `<group> <subcommand>` pair is in
 * {@link GH_READ_ONLY_SUBCOMMANDS} AND no argument targets a foreign host
 * ({@link ghArgsTargetForeignHost}). A non-`gh` token list is not our concern
 * (returns true). Everything unlisted (`gh pr create`, `gh api`, bare `gh`) is
 * NOT read-only, so it falls through to a prompt. Mirrors Claude's per-command
 * GH_READ_ONLY_COMMANDS lookup + dangerous-callback.
 */
export function isGhInvocationReadOnly(tokens: string[]): boolean {
  if (tokens[0] !== 'gh') return true; // not a gh invocation — not our concern
  // Collect the leading non-flag words (the command group + subcommand). gh has no
  // value-taking GLOBAL flags before the command group, so a leading flag means an
  // unrecognized form (e.g. `gh --version`) that is not auto-approvable.
  const words: string[] = [];
  let idx = 1;
  for (; idx < tokens.length && words.length < 2; idx++) {
    const t = tokens[idx]!;
    if (t.startsWith('-')) break;
    words.push(t);
  }
  if (words.length < 2) return false; // bare `gh` / `gh <group>` without a subcommand
  const pair = `${words[0]} ${words[1]}`;
  if (!GH_READ_ONLY_SUBCOMMANDS.has(pair)) return false;
  return !ghArgsTargetForeignHost(tokens.slice(1 + words.length));
}

/**
 * Whole-command `gh` read-only guard for the bash classifier — the gh analogue of
 * {@link isGitReadOnlyCommandSafe}. Splits a (possibly compound) command into
 * segments and refuses if ANY `gh` invocation is not a read-only form
 * ({@link isGhInvocationReadOnly}). Non-gh segments are classified by the rest of
 * the classifier (destructive/unsafe guards run before this), so a compound that
 * pairs a safe `gh` read with a write is not whitewashed here.
 */
export function isGhReadOnlyCommandSafe(command: string): boolean {
  const unwrapped = unwrapShellControlStructures(command);
  const segments = unwrapped.split(/\s*(?:\|\||&&|[|;])\s*/).filter(s => s.trim());
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    let i = 0;
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
    if (tokens[i] === 'gh') {
      if (!isGhInvocationReadOnly(tokens.slice(i))) return false;
    }
  }
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
  return extractBaseCommands(command).some(b => isNormalizedGitCommand(b) || b.startsWith('git '));
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
 * Identity ("no-op") `cd` targets that resolve to the CURRENT working directory,
 * so a leading `cd <target>` changes nothing. Models routinely prepend `cd $PWD`
 * (or `cd .`) before the real command; that segment must not be treated as a real
 * directory change by the cwd-desync / multiple-cd guards. Covers the portable
 * identity forms (`.`, `$PWD`, `${PWD}`, quoted variants, `$(pwd)`, `` `pwd` ``)
 * and the Windows `%CD%` spelling.
 */
const NOOP_CWD_TARGET_RE =
  /^(?:\.|\$PWD|\$\{PWD\}|"\$PWD"|'\$PWD'|"\$\{PWD\}"|\$\(pwd\)|`pwd`|%CD%)$/;

/**
 * Strip a SINGLE leading no-op `cd <cwd>` subcommand from a (possibly compound)
 * command, returning the remainder. A model-prepended identity cd (`cd $PWD &&
 * real-cmd`, `cd . ; real-cmd`) is a no-op, so counting it toward the multiple-cd
 * guard or the cd+redirect desync guard would spuriously escalate an otherwise
 * benign command to an explicit prompt. Only an IDENTITY target is peeled — a
 * `cd` into a real directory shifts the resolution base for later relative paths
 * and is never stripped. Mirrors Claude's BashTool prefix normalization, which
 * removes a leading `cd <cwd>` subcommand before counting cd commands / running
 * per-subcommand permission evaluation.
 */
export function stripLeadingNoopCd(command: string): string {
  const m = command.match(/^\s*([^\n;&|]+?)\s*(?:&&|;|\|\||\||&)\s*([\s\S]+)$/);
  if (!m) return command;
  const first = m[1]!.trim();
  const rest = m[2]!;
  const toks = first.split(/\s+/).filter(Boolean);
  const base = (toks[0] ?? '').toLowerCase();
  if (base !== 'cd' && base !== 'chdir' && base !== 'pushd' && base !== 'set-location') {
    return command;
  }
  // The segment must be exactly `cd <single-identity-target>` (no flags, no extra
  // operands) so `cd . file` or `cd $PWD extra` is left intact for scrutiny.
  const operands = toks.slice(1);
  if (operands.length !== 1 || !NOOP_CWD_TARGET_RE.test(operands[0]!)) return command;
  return rest;
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
  // Drop a model-prepended identity `cd <cwd>` (a no-op) so it can't be mistaken
  // for a real directory change before a redirect.
  const unwrapped = unwrapShellControlStructures(stripLeadingNoopCd(command));
  const segments = unwrapped.split(/\s*(?:\|\||&&|[|;])\s*/).filter(s => s.trim());
  let hasCd = false;
  let hasRedirect = false;
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    let i = 0;
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
    const cmd = tokens[i];
    if (isNormalizedCdCommand(cmd)) hasCd = true;
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
  // A model-prepended identity `cd <cwd>` is a no-op and must not count toward the
  // multiple-cd guard (which escalates any command with more than one real cd).
  const unwrapped = unwrapShellControlStructures(stripLeadingNoopCd(command));
  const segments = unwrapped.split(/\s*(?:\|\||&&|[|;])\s*/).filter(s => s.trim());
  let count = 0;
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    let i = 0;
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
    const cmd = tokens[i];
    if (isNormalizedCdCommand(cmd)) count++;
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
  // The cache key is prefixed with `i` (case-insensitive) or `s` (case-sensitive)
  // so the two flavours of the same pattern don't collide in the LRU.
  (key: string): RegExp => new RegExp(`^${globToRegexSource(key.slice(1))}$`, key[0] === 'i' ? 'si' : 's'),
  (key: string) => key,
  1000,
);
/**
 * Match a glob-like pattern (only `*` is special, expanding to any run of
 * characters; every other metachar is escaped) against a value. This is the
 * canonical wildcard matcher reused by MCP-server allowlists, permission rules
 * and the HTTP-hook URL allowlist — do not re-implement it per site.
 *
 * `caseInsensitive` (default false) adds the regex `i` flag, mirroring Claude's
 * `matchWildcardPattern(pattern, command, caseInsensitive)` (shellRuleMatching.ts
 * L90-L94/L150). It is used for case-insensitive filesystems (macOS/Windows) where
 * a path deny/allow rule should match regardless of letter-case.
 */
export function wildcardMatch(pattern: string, value: string, caseInsensitive = false): boolean {
  return matchPattern(pattern, value, caseInsensitive);
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

function matchPattern(pattern: string, value: string, caseInsensitive = false): boolean {
  if (pattern.includes('\x00') || value.includes('\x00')) return false;
  if (pattern === '*') return true;
  if (caseInsensitive
    ? pattern.toLowerCase() === value.toLowerCase()
    : pattern === value) return true;

  return _compilePattern((caseInsensitive ? 'i' : 's') + pattern).test(value);
}

/**
 * Resolve a FILE permission-rule pattern to its `(relativePattern, root)` pair,
 * mirroring Claude Code's `patternWithRoot`
 * (references/claude/claude-code-main/src/utils/permissions/filesystem.ts L852-917).
 *
 * The pattern's leading sigil chooses the directory it is anchored to, so the SAME
 * rule string resolves to the SAME file regardless of which settings tier it came
 * from. AMI previously matched file rules as raw globs against the literal path
 * string, so a root-relative rule (`//etc/**`), a home-relative rule (`~/.ssh/**`)
 * or a project-relative rule (`/src/**`) silently never matched a real absolute
 * path — AMI even generates `//abs/**` suggestions via {@link createReadRuleSuggestion}
 * that could not match. This restores per-root resolution:
 *  - `//x`  → root `/` (filesystem-root-relative); one leading slash is dropped.
 *  - `~/x`  → root = the NFC-normalized home directory.
 *  - `/x`   → root = `sourceRoot` (the directory the rule's settings tier lives in,
 *             i.e. the project root for project/local settings).
 *  - bare `x` / `./x` → root `null` (root-agnostic); a leading `./` is stripped so
 *    `./.env` matches `.env`.
 */
export function patternWithRoot(
  pattern: string,
  sourceRoot: string,
  home: string = os.homedir(),
): { relativePattern: string; root: string | null } {
  if (pattern.startsWith('//')) {
    return { relativePattern: pattern.slice(1), root: '/' };
  }
  if (pattern.startsWith('~/')) {
    return { relativePattern: pattern.slice(1), root: home.normalize('NFC') };
  }
  if (pattern.startsWith('/')) {
    return { relativePattern: pattern, root: sourceRoot };
  }
  const normalized = pattern.startsWith('./') ? pattern.slice(2) : pattern;
  return { relativePattern: normalized, root: null };
}

/**
 * Match a ROOTED file-rule pattern (`//…`, `~/…`, `/…`) against an absolute
 * candidate path after resolving the pattern against its root via
 * {@link patternWithRoot}. Returns `null` for a root-agnostic (bare / `./`) pattern
 * so the caller falls back to AMI's existing raw-glob matcher — this keeps
 * bare-pattern behavior byte-for-byte unchanged and makes the rooted-form resolution
 * purely ADDITIVE (it can only make a rooted rule that previously could not match
 * start matching the file it names).
 *
 * A trailing `/**` is treated gitignore-style: it matches BOTH the directory itself
 * and everything inside it (parity with Claude stripping `/**` before handing the
 * pattern to the `ignore` library, which matches a bare `dir` against `dir` and
 * `dir/anything`).
 */
export function matchRootedFileRulePattern(
  pattern: string,
  absPath: string,
  opts: { sourceRoot?: string; home?: string; caseInsensitive?: boolean } = {},
): boolean | null {
  const { relativePattern, root } = patternWithRoot(
    pattern,
    opts.sourceRoot ?? process.cwd(),
    opts.home ?? os.homedir(),
  );
  if (root === null) return null; // root-agnostic — caller uses the raw matcher
  const ci = opts.caseInsensitive ?? false;
  const absPattern = path.posix.join(root.replace(/\\/g, '/'), relativePattern);
  const candidate = absPath.replace(/\\/g, '/');
  if (absPattern.endsWith('/**')) {
    const base = absPattern.slice(0, -3);
    if (ci ? candidate.toLowerCase() === base.toLowerCase() : candidate === base) return true;
  }
  return matchPattern(absPattern, candidate, ci);
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
      // Skip flags — including Windows cmd-style `/c`, `/k`, `/q` switches (an
      // exact `/x` or `/x:value` token), so `cmd /c <denied>` reduces to the
      // inner command. A path operand (`/usr/bin`) is >1 char and is preserved.
      while (i < tokens.length && (tokens[i]!.startsWith('-') || /^\/[a-zA-Z]$/.test(tokens[i]!))) i++;
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
  // Windows `.exe` shell variants — a cross-platform payload can name the
  // interpreter as `bash.exe` / `powershell.exe` / `cmd.exe`; these must be
  // denied as a grantable prefix exactly like their bare forms (Claude's
  // DANGEROUS_SHELL_PREFIXES lists cmd.exe/powershell.exe/pwsh.exe/bash.exe).
  'cmd.exe', 'powershell.exe', 'pwsh.exe', 'bash.exe', 'sh.exe', 'zsh.exe',
  'fish.exe', 'csh.exe', 'tcsh.exe', 'ksh.exe', 'dash.exe',
  // wrappers that exec their args as a command
  'env', 'xargs', 'nice', 'stdbuf', 'nohup', 'timeout', 'time',
  // privilege escalation
  'sudo', 'doas', 'pkexec',
  // cross-platform code-exec interpreters
  'node', 'deno', 'bun', 'python', 'python3', 'ruby', 'perl', 'php', 'osascript', 'iex',
]);

/**
 * True if `word` names a shell/interpreter/wrapper executable that must never be
 * treated as a permission-granting prefix — matched case-insensitively so
 * Windows-style `Bash.EXE` / `PowerShell.exe` are caught too. Mirrors Claude's
 * DANGEROUS_SHELL_PREFIXES guard (utils/shell/prefix.ts): accepting e.g. `bash`
 * (or a scoped `bash foo`) as a prefix would let any command through the shell
 * and defeat the permission system.
 */
export function isBareShellPrefix(word: string): boolean {
  return BARE_SHELL_PREFIXES.has(word.toLowerCase());
}

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
  /**
   * Per-FIRST-SUBCOMMAND depth overrides, keyed by the lowercased first subcommand
   * word. A deep subcommand tree under one group needs a larger prefix than the
   * command's default `depth` to reach a meaningful leaf (e.g. `gcloud compute
   * instances describe` needs depth 6, while `gcloud config list` is fine at 4).
   * When the first subcommand after the command matches a key here, that value
   * REPLACES `depth` for this invocation. Mirrors Claude's `DEPTH_RULES` COMPOUND
   * keys (`${command} ${firstSubcommand}` → depth), e.g. `gcloud compute`:6,
   * `gcloud beta`:6 (utils/shell/specPrefix.ts DEPTH_RULES / staticPrefix.ts
   * calculateDepth, which consults the `${command} ${sub}` key before the bare
   * command key). Keyed on the first subcommand only (not arbitrary pairs) because
   * that is the sole compound form Claude's table uses.
   */
  subcommandDepth?: Readonly<Record<string, number>>;
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
  gcloud: {
    valueFlags: new Set(['--project', '--account', '--configuration', '--format', '--verbosity']),
    depth: 4,
    // Claude's DEPTH_RULES compound keys: `gcloud compute`/`gcloud beta` nest
    // deeper (service → resource → verb) so a 4-word prefix stops short of the
    // real leaf; 6 reaches it.
    subcommandDepth: { compute: 6, beta: 6 },
  },
  // .NET CLI: a meaningful action lives at the 3rd word (`dotnet tool install`,
  // `dotnet nuget push`, `dotnet workload install`) — parity with Claude's
  // DEPTH_RULES[dotnet]=3. No global value-flags worth skipping before the verb.
  dotnet: { valueFlags: new Set(['-v', '--verbosity']), depth: 3 },
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
 * Resolve the {@link BashCommandSpec} for a command: the hand-maintained
 * {@link BASH_COMMAND_SPECS} table first, then — for a command the table does not
 * cover — a spec derived from the extensible command-spec registry's built-in
 * value-flags ({@link valueFlagsForCommand}). This is the wiring that lets the
 * registry widen flag-value skipping (and thus SAFE_PREFIXES matching) to more
 * CLIs (e.g. `make -C dir test` → `make test`) without a second hardcoded table.
 */
function bashSpecFor(cmd: string): BashCommandSpec | undefined {
  const existing = BASH_COMMAND_SPECS[cmd];
  if (existing) return existing;
  const valueFlags = valueFlagsForCommand(cmd);
  return valueFlags ? { valueFlags } : undefined;
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
  const spec = bashSpecFor(cmd);
  // Skip leading global flags (and any value they consume) to reach the subcommand.
  i = skipGlobalFlags(tokens, i + 1, spec);
  // Resolve the effective depth: a COMPOUND (`command firstSubcommand`) override
  // from the spec's `subcommandDepth` wins over the command's default `depth`, so
  // a deep subcommand tree (`gcloud compute …`) extends to its real leaf while a
  // shallow sibling keeps the default. Mirrors Claude's DEPTH_RULES compound-key
  // lookup (`${command} ${sub}` before the bare `${command}`).
  let depth = spec?.depth ?? 2;
  const firstSub = tokens[i];
  if (firstSub && spec?.subcommandDepth) {
    const override = spec.subcommandDepth[firstSub.toLowerCase()];
    if (override !== undefined) depth = override;
  }
  const parts = [cmd];
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
  if (isBareShellPrefix(cmd)) return null;
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
  // Wrapper-aware derivation: for a wrapped command (`nice -n5 git log`,
  // `sudo apt-get install foo`, `timeout 5 npm test`) the wrapper root is itself a
  // BARE_SHELL_PREFIX (`sudo`/`env`/`nice`/…) so the normal path below would refuse
  // to suggest anything. Instead derive the INNER command's prefix and re-compose
  // it with the (bounded, fail-closed) wrapper chain so the grant stays narrowly
  // scoped to the wrapped form. Mirrors Claude's handleWrapper composed prefix.
  const wrapped = unwrapWrapperChain(cleaned);
  if (wrapped) {
    const innerPrefix = suggestAllowPrefix(wrapped.inner);
    return innerPrefix ? [...wrapped.wrappers, innerPrefix].join(' ') : null;
  }
  const two = getSimpleCommandPrefix(cleaned);
  // Guard the two-word path too: `start-process notepad*` would match
  // `start-process notepad.exe & evil`, so a dangerous PowerShell spawner as
  // the ROOT word is never offered even with a scoped subcommand. Likewise a
  // shell/interpreter root (`bash foo`, `powershell.exe x`, `pwsh something`)
  // must never anchor a scoped prefix — it would auto-allow arbitrary execution
  // through the shell (Claude's DANGEROUS_SHELL_PREFIXES applied to extraction).
  if (two) {
    const root = two.split(' ')[0]!;
    return isBareShellPrefix(root) || isPowerShellNeverSuggest(root) ? null : two;
  }
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
/**
 * Upper bound on how many per-subcommand "allow always" rules a single compound
 * command's permission prompt may suggest. A wide chain (`a && b && c && … && j`)
 * otherwise produces a 10+ rule dump the user has to read through; capping keeps
 * the prompt legible — the user can still allow the rest by re-running or granting
 * a broader rule. Mirrors Claude's `MAX_SUGGESTED_RULES_FOR_COMPOUND` (default 5,
 * bashPermissions.ts). Applied in {@link buildPermissionSuggestions}.
 */
export const MAX_SUGGESTED_RULES_FOR_COMPOUND = 5;

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
  for (const group of groups.values()) {
    const lcp = wordAlignedCommonPrefix(group);
    // Bare-root drop guard: never collapse a group down to a single subcommand
    // aware root (`git fetch`+`git worktree add` → `git`, `git add`+`git commit`
    // → `git`). A bare `git:*` rule would auto-allow `git push --force` forever,
    // so DROP the whole group rather than suggest the too-broad root. A multi-word
    // LCP (`npm run`) is narrowly scoped and kept. Mirrors Claude's
    // getCompoundCommandPrefixesStatic collapse guard (staticPrefix.ts L271-280).
    if (isBareSubcommandAwareRoot(lcp, root => SUBCOMMAND_AWARE_ROOTS.has(root))) continue;
    collapsed.push(lcp);
  }
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

/**
 * Reverse of {@link normalizeToolName}: given a CANONICAL AMI tool name, return
 * every legacy / cross-framework alias that maps to it (the keys of
 * {@link PERMISSION_TOOL_ALIASES} whose value is `canonicalName`), sorted for
 * stable display. This is the inverse boundary Claude Code exposes as
 * `getLegacyToolNames` (utils/permissions/permissionRuleParser.ts L35-41): a rule
 * written against the current name can be expanded to the historical/aliased
 * spellings, and an introspection / diagnostic surface can tell the user "tool X
 * also matches legacy names […]" when displaying a permission rule. Matching
 * already works both ways (both sides go through {@link normalizeToolName}); this
 * is purely for enumeration/display. Returns `[]` for a tool with no aliases.
 */
export function getLegacyToolNames(canonicalName: string): string[] {
  const result: string[] = [];
  for (const [alias, canonical] of Object.entries(PERMISSION_TOOL_ALIASES)) {
    if (canonical === canonicalName && alias !== canonicalName) result.push(alias);
  }
  return result.sort();
}

/** The pattern-syntax class a tool's permission-rule content is written in:
 *  `file` tools take filesystem GLOBS (`*.ts`, `src/**`); `bash` tools take a
 *  command string with free `*` wildcards plus the legacy `command:*` prefix
 *  form. Mirrors Claude Code's toolValidationConfig split (filePatternTools vs
 *  bashPrefixTools) that drives which pattern syntax is legal per tool. */
export const TOOL_PATTERN_CLASSES = ['file', 'bash'] as const;
export type ToolPatternClass = (typeof TOOL_PATTERN_CLASSES)[number];

/** Canonical-tool-name → pattern-class map (single source of truth for
 *  {@link isFilePatternTool}/{@link isBashPrefixTool}). Keyed by AMI canonical
 *  names; callers pass raw rule tool names through {@link normalizeToolName}
 *  first, so a Claude-spelled `Edit`/`Glob` classifies the same as `file_edit`/
 *  `glob`. Tools absent from the map (`web_fetch`, `web_search`, MCP, `grep`,
 *  …) use their own content validators, not glob/bash-wildcard syntax. Exposed
 *  for the permission introspection surface. */
export const TOOL_PATTERN_CLASS: Readonly<Record<string, ToolPatternClass>> = {
  // File-glob tools (parity: Read/Write/Edit/Glob/NotebookRead/NotebookEdit).
  file_read: 'file', file_edit: 'file', glob: 'file', notebook_edit: 'file',
  // Bash wildcard + legacy `:*` prefix tool.
  bash: 'bash',
};

/** The pattern class of a tool's permission-rule content, or `undefined` for a
 *  tool that uses a custom content validator (not file-glob / bash-wildcard). */
export function toolPatternClass(toolName: string): ToolPatternClass | undefined {
  return TOOL_PATTERN_CLASS[normalizeToolName(toolName)];
}

/** True when a tool's permission patterns are filesystem GLOBS (`*.ts`,
 *  `src/**`) — `:*` is Bash-only syntax here and mid-path wildcards are suspect. */
export function isFilePatternTool(toolName: string): boolean {
  return toolPatternClass(toolName) === 'file';
}

/** True when a tool's permission patterns are bash command prefixes/wildcards
 *  (free `*` plus the legacy `command:*` prefix form). */
export function isBashPrefixTool(toolName: string): boolean {
  return toolPatternClass(toolName) === 'bash';
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

/**
 * Parse an `mcp__server[__tool]` permission-rule tool name into its parts, or
 * `null` when the name is not an MCP rule. Mirrors Claude's `mcpInfoFromString`
 * (services/mcp/mcpStringUtils.ts): the first two `__`-delimited segments are
 * the `mcp` marker and the server name; everything after is re-joined so a tool
 * name containing `__` survives (`mcp__srv__a__b` → tool `a__b`). A missing
 * server segment (`mcp__`, `mcp`) is not a valid MCP rule and returns `null`.
 */
export function mcpInfoFromString(
  toolString: string,
): { serverName: string; toolName: string | undefined } | null {
  const parts = toolString.split('__');
  const [mcpPart, serverName, ...toolNameParts] = parts;
  if (mcpPart !== 'mcp' || !serverName) return null;
  return {
    serverName,
    toolName: toolNameParts.length > 0 ? toolNameParts.join('__') : undefined,
  };
}

/** Count unescaped occurrences of `char` in `str` (odd preceding backslash run
 *  = escaped). Shared by the MCP rule validator so `mcp__srv\(x\)` is inspected
 *  as a literal, not a pattern. Mirrors Claude's `countUnescapedChar`. */
function countUnescapedChar(str: string, char: string): number {
  let count = 0;
  for (let i = 0; i < str.length; i++) {
    if (str[i] === char && !isEscapedAt(str, i)) count++;
  }
  return count;
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
 * Validate an UNTRUSTED {@link PermissionUpdate} at a trust boundary before it is
 * applied. Permission updates can arrive from less-trusted surfaces — a delegated
 * permission-prompt tool's JSON reply (an MCP server), a hook process's
 * `updatedPermissions`, or a settings file — where TypeScript's compile-time union
 * does NOT hold at runtime. A malformed mutation (unknown `type`, a missing/invalid
 * `behavior`, a non-array `rules`, a rule whose `ruleContent` is not a string, an
 * empty tool name, a `destination` outside the writable tiers, a bogus `setMode`
 * mode, or a non-string directory) must be REJECTED whole rather than partially
 * applied — a single acceptance boundary mirroring Claude Code's
 * `PermissionUpdateSchema` Zod `discriminatedUnion` with its enum `behavior` /
 * `destination` and typed rule/directory arrays
 * (references/claude/claude-code-main/src/utils/permissions/PermissionUpdateSchema.ts
 * L27-L77).
 *
 * Returns `{ valid: true }` for a well-formed update, else `{ valid: false, error }`
 * naming the first problem. Pure + synchronous; the enforcement point is
 * {@link PermissionManager.applyUpdate}, which drops an invalid update fail-closed
 * (grants nothing) instead of partially applying it or throwing mid-turn.
 */
export function validatePermissionUpdate(update: unknown): { valid: boolean; error?: string } {
  if (update === null || typeof update !== 'object' || Array.isArray(update)) {
    return { valid: false, error: 'permission update must be an object' };
  }
  const u = update as Record<string, unknown>;
  // `destination` is optional, but when present it is a hard boundary: a rule or
  // directory grant may only be routed to a WRITABLE tier (read-only sources like
  // policySettings/flagSettings/command can never be a persistence target).
  const dest = u.destination;
  if (dest !== undefined &&
      !(typeof dest === 'string' && (PERMISSION_UPDATE_DESTINATIONS as readonly string[]).includes(dest))) {
    return { valid: false, error: `invalid destination: ${JSON.stringify(dest)}` };
  }
  switch (u.type) {
    case 'addRules':
    case 'replaceRules':
    case 'removeRules': {
      if (u.behavior !== 'allow' && u.behavior !== 'deny' && u.behavior !== 'ask') {
        return { valid: false, error: `${u.type} requires behavior "allow" | "deny" | "ask"` };
      }
      if (!Array.isArray(u.rules)) {
        return { valid: false, error: `${u.type}.rules must be an array` };
      }
      for (const rv of u.rules) {
        if (rv === null || typeof rv !== 'object' || Array.isArray(rv)) {
          return { valid: false, error: `${u.type}.rules entries must be objects` };
        }
        const r = rv as Record<string, unknown>;
        if (typeof r.toolName !== 'string' || r.toolName === '') {
          return { valid: false, error: `${u.type}.rules entries require a non-empty string toolName` };
        }
        if (r.ruleContent !== undefined && typeof r.ruleContent !== 'string') {
          return { valid: false, error: `${u.type}.rules ruleContent must be a string when present` };
        }
      }
      return { valid: true };
    }
    case 'setMode': {
      if (typeof u.mode !== 'string' || !(PERMISSION_MODES as readonly string[]).includes(u.mode)) {
        return { valid: false, error: `setMode requires a valid mode (${PERMISSION_MODES.join(', ')})` };
      }
      return { valid: true };
    }
    case 'addDirectories':
    case 'removeDirectories': {
      if (!Array.isArray(u.directories)) {
        return { valid: false, error: `${u.type}.directories must be an array` };
      }
      for (const d of u.directories) {
        if (typeof d !== 'string' || d === '') {
          return { valid: false, error: `${u.type}.directories entries must be non-empty strings` };
        }
      }
      return { valid: true };
    }
    default:
      return { valid: false, error: `unknown permission update type: ${JSON.stringify(u.type)}` };
  }
}

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
 * Canonical tools whose deny/allow rule pattern is matched against a FILESYSTEM
 * PATH (not a command string or a free-text pattern like grep's). On a
 * case-insensitive filesystem these are matched case-insensitively so a deny rule
 * written in one casing still blocks the same file reached via another casing
 * (`{@link pathRuleCaseInsensitive}`). `grep` is deliberately excluded — its rule
 * content is a search pattern where case is significant.
 */
const FILE_PATH_RULE_TOOLS = new Set([
  'file_read', 'file_edit', 'file_write', 'notebook_edit', 'notebook_read', 'glob', 'list_dir',
]);

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

/** Config folders that host a `skills/` subdir. `.ami-plugin` is excluded — plugin
 *  bundles are not user-authored skills, so they get no skill-scoped grant. */
const SKILL_CONFIG_FOLDER_NAMES = ['.ami', '.claude'] as const;

/**
 * When a write targets a file inside `<configRoot>/skills/<name>/…` (project or
 * global), return that ONE skill's name plus a NARROW, least-privilege session rule
 * scoped to just that skill dir — instead of the broad `<configRoot>/*` grant that
 * would also hand over settings.json, hooks/, and every OTHER skill. Lets a user
 * iterate on a single skill without granting blanket config-folder access. Mirrors
 * Claude's `getClaudeSkillScope` (utils/permissions/filesystem.ts L101-157), widened
 * to AMI's config folders (`.ami`, `.claude`) and both project + global skill dirs.
 *
 * Returns `null` (caller falls back to the broad config-folder grant) when the path
 * is not inside a skill dir, is a loose file directly under `skills/` (no skill
 * segment), or the skill segment is UNSAFE: empty, `.`, contains `..` (traversal), or
 * carries a glob metacharacter (`* ? [ ]`) that would widen the pattern to ALL skills
 * or enable pattern injection.
 */
export function getSkillScopeRule(
  filePath: string,
  cwd: string = process.cwd(),
): { skillName: string; skillDir: string; ruleContent: string } | null {
  if (!filePath) return null;
  const abs = path.resolve(cwd, expandPath(filePath));
  const absFolded = normalizeCaseForComparison(abs);
  const home = os.homedir();
  const bases: string[] = [];
  for (const name of SKILL_CONFIG_FOLDER_NAMES) {
    bases.push(path.resolve(cwd, name, 'skills'));
    bases.push(path.join(home, name, 'skills'));
  }
  for (const base of bases) {
    const baseFolded = normalizeCaseForComparison(base);
    if (!absFolded.startsWith(baseFolded + path.sep)) continue;
    // Slice the ORIGINAL (un-folded) path so the skill name keeps its real case —
    // downstream path-glob matching is case-sensitive on POSIX.
    const rest = abs.slice(base.length + path.sep.length);
    const sepIdx = rest.indexOf(path.sep);
    // Require a separator: the file must be INSIDE the skill dir, not a loose file
    // directly under skills/ (which gets no skill scope).
    if (sepIdx <= 0) return null;
    const skillName = rest.slice(0, sepIdx);
    // Reject empty/`.`/traversal (`..`) — a traversal segment would escape the skill
    // dir and the broad-grant fallback is safer than a bad narrow rule.
    if (!skillName || skillName === '.' || skillName.includes('..')) return null;
    // Reject glob metacharacters: a skill literally named `*` (valid on POSIX) would
    // produce `…/skills/*/*` and match EVERY skill, defeating the narrowing.
    if (/[*?[\]]/.test(skillName)) return null;
    const skillDir = path.join(base, skillName);
    return { skillName, skillDir, ruleContent: `${skillDir}${path.sep}*` };
  }
  return null;
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

/**
 * Prefix that marks a permission rule's content as a natural-language "prompt
 * rule" — a human-written DESCRIPTION of what to auto-approve (e.g. "running the
 * test suite"), as opposed to a glob prefix (`git:*`). Parity: Claude's
 * `PROMPT_PREFIX` (`utils/permissions/bashClassifier.ts`).
 */
export const PROMPT_RULE_PREFIX = 'prompt:';

/**
 * Encode a free-text description as prompt-rule content (`prompt: <description>`).
 * This is the canonical rule body saved when a user approves a bash command via
 * the "yes, and don't ask again — described as …" classifier path, so the grant
 * is remembered as a reusable natural-language rule rather than a one-off glob.
 * Parity: Claude's `createPromptRuleContent(description)`.
 *
 * DISTINCT from {@link validateAllowedPromptRule}, which guards plan-mode
 * `allowedPrompts` that AMI stores directly AS glob patterns — this codec names
 * the separate `prompt:`-prefixed classifier-rule format and never produces a glob.
 */
export function createPromptRuleContent(description: string): string {
  return `${PROMPT_RULE_PREFIX} ${description.trim()}`;
}

/** Whether a rule's content string is prompt-rule content (carries the
 *  {@link PROMPT_RULE_PREFIX}); tolerant of leading whitespace. */
export function isPromptRuleContent(content: string | undefined): boolean {
  return typeof content === 'string' && content.trimStart().startsWith(PROMPT_RULE_PREFIX);
}

/**
 * Recover the human-readable description from prompt-rule content, or `null` when
 * the content is not a prompt rule or carries no description after the prefix.
 * Round-trips {@link createPromptRuleContent}. Parity: Claude's
 * `extractPromptDescription(ruleContent)`.
 */
export function extractPromptDescription(content: string | undefined): string | null {
  if (typeof content !== 'string') return null;
  const trimmed = content.trimStart();
  if (!trimmed.startsWith(PROMPT_RULE_PREFIX)) return null;
  const description = trimmed.slice(PROMPT_RULE_PREFIX.length).trim();
  return description.length > 0 ? description : null;
}

/**
 * The read-only detail notice shown for a permission rule that originates from
 * managed/enterprise policy ({@link isManagedPermissionSource}) — such a rule
 * cannot be edited or removed by the user. Returns `null` for every
 * user-editable source, so a caller can branch on "is this rule locked". Parity:
 * Claude's PermissionRuleList "Rule details" view for `policySettings` rules.
 */
export function managedRuleReadOnlyNotice(source: PermissionRuleSource | undefined): string | null {
  if (!isManagedPermissionSource(source)) return null;
  return (
    'This rule is configured by managed settings and cannot be modified.\n' +
    'Contact your system administrator for more information.'
  );
}

/**
 * Turn a granted DIRECTORY into a persistable `file_read` allow rule (parity:
 * Claude's `createReadRuleSuggestion`), so "I pointed the agent at this dir" can be
 * saved as a normal, scoped permission rule rather than only a runtime read root.
 * Two algorithmic subtleties are mirrored exactly:
 *  - the filesystem root `/` is rejected as far too broad (returns `undefined`); and
 *  - an ABSOLUTE path gets an extra leading slash to form the root-anchored
 *    `//path/**` pattern, while a relative path becomes `path/**`.
 * The path is POSIX-normalized first so a Windows path produces a forward-slash glob.
 * `destination` defaults to `'session'` (ephemeral) — the caller opts into a
 * persisted tier explicitly.
 */
export function createReadRuleSuggestion(
  dirPath: string,
  destination: PermissionUpdateDestination = 'session',
): PermissionUpdate | undefined {
  // POSIX-normalize for pattern matching (collapse Windows separators).
  const pathForPattern = dirPath.replace(/\\/g, '/');
  // The filesystem root is too broad to be a reasonable permission target.
  if (pathForPattern === '/') return undefined;
  // Absolute paths get an extra leading slash → the `//path/**` root-anchored form.
  const ruleContent = pathForPattern.startsWith('/')
    ? `/${pathForPattern}/**`
    : `${pathForPattern}/**`;
  return {
    type: 'addRules',
    behavior: 'allow',
    destination,
    rules: [{ toolName: 'file_read', ruleContent }],
  };
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
      // Cap the per-subcommand rule dump so a wide compound command does not
      // produce an unreadable 10+ rule prompt (Claude's MAX_SUGGESTED_RULES_FOR_COMPOUND).
      .slice(0, MAX_SUGGESTED_RULES_FOR_COMPOUND)
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
      const dir = path.dirname(filePath);
      // Offer BOTH a runtime read-root widening (addDirectories) AND a persistable,
      // path-scoped `file_read` allow rule (createReadRuleSuggestion), so granting a
      // read can either just widen this session or be saved as a normal rule. The
      // Read rule is omitted when the directory is the un-scopable filesystem root.
      const suggestions: PermissionUpdate[] = [
        { type: 'addDirectories', destination: 'session', directories: [dir] },
      ];
      const readRule = createReadRuleSuggestion(dir);
      if (readRule) suggestions.push(readRule);
      return suggestions;
    }
    const suggestions: PermissionUpdate[] = [];
    // A write under `<configRoot>/skills/<name>/` offers a NARROW, least-privilege
    // "allow edits to this skill only" session grant FIRST — scoped to just that one
    // skill dir — so iterating on a skill does not require the blanket config-folder
    // grant below (which would also expose settings.json, hooks/, and other skills).
    // Mirrors Claude's getClaudeSkillScope skill-scoped addRules suggestion.
    const skillScope = getSkillScopeRule(filePath, process.cwd());
    if (skillScope) {
      suggestions.push({
        type: 'addRules', behavior: 'allow', destination: 'session',
        rules: [{ toolName: canonical, ruleContent: skillScope.ruleContent }],
      });
    }
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
    // A one-tap "switch to acceptEdits" affordance for an edit-capable tool, so the
    // user can unblock ALL future edits for the session rather than granting one
    // path rule at a time (parity: Claude's blocked-write `{ type:'setMode',
    // mode:'acceptEdits' }` suggestion, pathValidation.ts L1792-1798). Offered only
    // for the tools acceptEdits actually auto-allows ({@link ACCEPT_EDITS_EDIT_TOOLS})
    // — it is a no-op for a read or a bash command, which have their own affordances.
    if (ACCEPT_EDITS_EDIT_TOOLS.has(canonical)) {
      suggestions.push({ type: 'setMode', mode: 'acceptEdits', destination: 'session' });
    }
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
 * Does a would-be PERSISTENT bash `allow` pattern contain a shell-DYNAMIC construct
 * that must never be baked into a reusable rule? Returns the offending token kind, or
 * `null` when the pattern is a safe static prefix/glob.
 *
 * Rationale (parity: Claude's `staticPrefix.ts` sanitization, L55-140): a persisted
 * allow rule is matched against FUTURE commands, so a dynamic token makes the rule
 * match MORE — or different — commands than the user actually saw and approved:
 *  - a command/arithmetic substitution (`$(...)`, backtick, `${...}`) or a variable
 *    expansion (`$VAR`, `$@`, …) means the real executed command is computed at run
 *    time, so "allow `deploy $(cat target)`" would silently authorize anything the
 *    substitution expands to;
 *  - a backslash path fragment (`C:\…`, `foo\bar`) is an escape/quoting hazard that
 *    the glob matcher does not interpret the way a shell would, so the rule can
 *    over-match.
 * Glob metacharacters (`*`, `?`, `[`) are intentionally TOLERATED — they are the
 * legitimate way an allow pattern scopes arguments (`git *`). Pure + side-effect
 * free. The enforcement point is {@link PermissionManager.addRule}, which refuses to
 * persist such a rule instead of silently granting an over-broad one.
 */
export function dynamicAllowPatternToken(pattern: string | undefined): 'substitution' | 'variable' | 'backslash' | null {
  if (!pattern) return null;
  for (let i = 0; i < pattern.length; i++) {
    const c = pattern[i]!;
    // A backslash is an escape/quoting hazard in a persisted glob (Claude rejects
    // backslash path fragments). A trailing lone backslash counts too.
    if (c === '\\') return 'backslash';
    // Backtick command substitution.
    if (c === '`') return 'substitution';
    if (c === '$') {
      const next = pattern[i + 1];
      // `$(` command / `$((` arithmetic substitution.
      if (next === '(') return 'substitution';
      // `${...}` parameter expansion, or a `$NAME` / special-parameter expansion
      // ($@ $* $# $? $! $$ $- $0-$9). A bare `$` at end-of-string or before a
      // non-expansion char (e.g. a literal `price$`) is not dynamic.
      if (next === '{') return 'variable';
      if (next && /[A-Za-z_@*#?!$0-9-]/.test(next)) return 'variable';
    }
  }
  return null;
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
  // MCP-rule validation (before the generic tool checks). An MCP rule scopes a
  // whole server (`mcp__server`), all its tools (`mcp__server__*`), or one tool
  // (`mcp__server__tool`) — it never carries a parenthesized pattern. Check BOTH
  // the parsed ruleContent AND the raw '(' count, because the parser normalizes a
  // standalone wildcard (`mcp__server(*)`) to a pattern-less name. Mirrors
  // Claude's permissionValidation MCP branch.
  const mcpInfo = mcpInfoFromString(parsed.toolName);
  if (mcpInfo) {
    if (parsed.ruleContent !== undefined || countUnescapedChar(rule, '(') > 0) {
      return {
        valid: false,
        error: 'MCP rules do not support patterns in parentheses',
        suggestion: `Use "${parsed.toolName}" without parentheses, or use "mcp__${mcpInfo.serverName}__*" for all tools`,
        examples: [
          `mcp__${mcpInfo.serverName}`,
          `mcp__${mcpInfo.serverName}__*`,
          mcpInfo.toolName && mcpInfo.toolName !== '*'
            ? `mcp__${mcpInfo.serverName}__${mcpInfo.toolName}`
            : undefined,
        ].filter(Boolean) as string[],
      };
    }
    return { valid: true };
  }
  // Bash prefix-rule mistakes: `:*` must be at the end and non-empty.
  if (isBashPrefixTool(parsed.toolName) && parsed.ruleContent !== undefined) {
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
  // File-pattern-tool mistakes (file_read / file_edit / glob / notebook_edit):
  // their rule content is a filesystem GLOB, so the bash-only `:*` prefix form is
  // invalid, and a wildcard that is not at a path boundary usually signals a
  // confused pattern. Mirrors Claude's permissionValidation file-tool branch
  // (utils/settings/permissionValidation.ts L199-236).
  if (isFilePatternTool(parsed.toolName) && parsed.ruleContent !== undefined) {
    const content = parsed.ruleContent;
    // `:*` is Bash prefix syntax — point the user at glob patterns instead.
    if (content.includes(':*')) {
      return {
        valid: false, error: 'The ":*" syntax is only for Bash prefix rules',
        suggestion: 'Use glob patterns like "*" or "**" for file matching',
        examples: [
          `${parsed.toolName}(*.ts) - matches .ts files`,
          `${parsed.toolName}(src/**) - matches all files in src`,
          `${parsed.toolName}(**/*.test.ts) - matches test files`,
        ],
      };
    }
    // A `*` that is not at a path boundary (start, end, `**`, `/*`, `*.`, `*)`)
    // is usually a mistake. `**` anywhere is always accepted (recursive glob).
    if (
      content.includes('*') &&
      !/^\*|\*$|\*\*|\/\*|\*\.|\*\)/.test(content) &&
      !content.includes('**')
    ) {
      return {
        valid: false, error: 'Wildcard placement might be incorrect',
        suggestion: 'Wildcards are typically used at path boundaries',
        examples: [
          `${parsed.toolName}(*.js) - all .js files`,
          `${parsed.toolName}(src/*) - all files directly in src`,
          `${parsed.toolName}(src/**) - all files recursively in src`,
        ],
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
    // pattern and point at the bare-domain form (Claude's WebFetch validator,
    // toolValidationConfig.ts L55-66). AMI uses BARE-domain syntax
    // (`web_fetch(example.com)`) — a deliberate divergence from Claude's required
    // `domain:` prefix — so a domain that merely starts with "http" (e.g.
    // `httpbin.org`) stays valid; only a `scheme://` at the start OR a stray
    // `://` ANYWHERE (a malformed URL whose scheme isn't a clean leading token,
    // e.g. `3http://x` or `foo bar://x`) is rejected. Catching `://` anywhere
    // closes the gap where the leading-scheme regex missed a mid-string `://`.
    if (parsed.toolName === 'web_fetch') {
      if (/^[a-z][a-z0-9+.-]*:\/\//i.test(parsed.ruleContent) || parsed.ruleContent.includes('://')) {
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
 * Compose the structured result of {@link validatePermissionRule} into a SINGLE
 * human-readable sentence: `error. suggestion. Examples: ex1, ex2`. This is the
 * canonical rejection-text formatter so every surface that shows a dropped/invalid
 * rule (settings loader warnings, the `/permissions` validate command, a config
 * UI) renders it identically — error, then the suggestion (if any), then the
 * examples (if any). Mirrors Claude's `PermissionRuleSchema` superRefine message
 * composition (utils/settings/permissionValidation.ts L244-262), which previously
 * had no AMI equivalent: AMI composed only `error. suggestion` and silently
 * dropped the examples. Returns the bare error for a result with no suggestion or
 * examples; returns an empty string for a `valid` result (nothing to format).
 */
export function formatPermissionRuleError(result: {
  valid: boolean; error?: string; suggestion?: string; examples?: string[];
}): string {
  if (result.valid || !result.error) return '';
  let message = result.error;
  if (result.suggestion) message += `. ${result.suggestion}`;
  if (result.examples && result.examples.length > 0) {
    message += `. Examples: ${result.examples.join(', ')}`;
  }
  return message;
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
    warnings.push(`Invalid permission rule "${entry}" was skipped: ${formatPermissionRuleError(result)}`);
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
/**
 * Extract the COMMAND PREFIX from a bash rule content written in Claude's `:*`
 * prefix convention (`git commit:*` → `git commit`, `npm:*` → `npm`), or `null`
 * when the content is not a prefix rule (no trailing `:*`, or nothing before it).
 * Mirrors Claude's `permissionRuleExtractPrefix` (shellPermissionHelpers.ts
 * L76-84). AMI's rule validator already BLESSES this form
 * ({@link validatePermissionRule} accepts `bash(git:*)` and even suggests it as
 * the canonical example) and {@link permissionRuleValueFromString} preserves the
 * `:*` content verbatim — but the raw glob matcher treats the `:` literally, so a
 * `git:*` rule never matched `git status`. This extractor lets {@link matchBashSub}
 * honor the convention: a `prefix:*` rule matches a command that IS the prefix or
 * begins with `prefix` followed by its arguments ({@link commandHasRulePrefix}).
 */
export function permissionRuleExtractPrefix(ruleContent: string): string | null {
  if (!ruleContent.endsWith(':*')) return null;
  const prefix = ruleContent.slice(0, -2).trim();
  return prefix.length > 0 ? prefix : null;
}

/**
 * True when `command` is covered by a rule prefix (Claude's `:*` convention): the
 * command equals the prefix exactly, or starts with the prefix followed by a space
 * (its arguments). Anchored on a word boundary so `ls:*` matches `ls -la` and bare
 * `ls` but NOT `lsof` (a different command that merely shares the leading letters).
 */
function commandHasRulePrefix(command: string, prefix: string): boolean {
  const c = command.trim();
  if (!prefix) return false;
  return c === prefix || c.startsWith(prefix + ' ');
}

function matchBashSub(pattern: string, sub: string, ruleAction: 'allow' | 'deny' | 'ask'): boolean {
  // Allow rules strip only SAFE_ENV_VARS (an unsafe env prefix must break the
  // match → prompt); deny/ask rules strip ALL env vars so no prefix can hide a
  // denied command.
  const strippedRaw = stripEnvVarPrefix(sub, /* safeEnvOnly */ ruleAction === 'allow');
  // Claude `cmd:*` prefix convention (validated + stored but previously unmatched):
  // a `prefix:*` rule grants/denies the command prefix plus any arguments. Checked
  // ahead of the glob match because `:` is otherwise a literal glob character.
  const rulePrefix = permissionRuleExtractPrefix(pattern);
  if (rulePrefix !== null && commandHasRulePrefix(strippedRaw, rulePrefix)) return true;
  if (matchPattern(pattern, strippedRaw)) return true;
  // Env-prefix-scoped rule parity: Claude deliberately KEEPS leading `VAR=...`
  // assignments in the matchable command prefix (`GOEXPERIMENT=synctest go test`,
  // `PYTHONPATH=/tmp python3`) so an allow/deny rule can be scoped to a command
  // TOGETHER WITH its specific env prefix (utils/bash/commands.ts L478-484). AMI
  // otherwise strips env vars before matching, which makes a rule that names a
  // SAFE_ENV_VARS prefix (e.g. `Bash(GOEXPERIMENT=synctest go test:*)`) unmatchable
  // — `GOEXPERIMENT` is stripped from the command, so the prefix never lines up.
  // When the RULE pattern ITSELF carries a leading env-var assignment, also match it
  // against the command's RAW (unstripped) form. Additive + safe: only a rule that
  // literally names the env prefix benefits — a bare `go test:*` rule still sees only
  // the stripped command (the raw `GOEXPERIMENT=… go test` would NOT satisfy a bare
  // `go test` prefix), so this never broadens an un-scoped allow.
  if (ENV_VAR_ASSIGN_RE.test(pattern.trimStart())) {
    const rawSub = sub.trim();
    if (rulePrefix !== null && commandHasRulePrefix(rawSub, rulePrefix)) return true;
    if (matchPattern(pattern, rawSub)) return true;
  }
  // PowerShell cross-alias rule matching (Windows only — AMI's bash tool runs
  // PowerShell on Windows): a rule written for one spelling of a cmdlet also
  // matches the command's OTHER spellings of the same canonical cmdlet, so a
  // deny on `Remove-Item` blocks `rm`/`del`/`ri` and a rule on `rm` blocks
  // `Remove-Item`. Deny/ask strip a module prefix (fail-safe over-match); allow
  // keeps it (allow must never over-match a differently-scoped cmdlet). The
  // compare is case-insensitive, mirroring Windows + Claude's PowerShell
  // filterRulesByContentsMatchingInput. Gated off POSIX so a Linux `rm`/`echo`
  // is never canonicalized into a PowerShell cmdlet (which would fail OPEN on an
  // allow rule or wrongly fire a PowerShell-named deny).
  if (process.platform === 'win32') {
    const canonCmd = canonicalizePowerShellCommandName(strippedRaw, /* stripModule */ true);
    const canonPat = canonicalizePowerShellCommandName(pattern, /* stripModule */ ruleAction !== 'allow');
    if ((canonCmd !== strippedRaw || canonPat !== pattern) &&
        matchPattern(canonPat, canonCmd, /* caseInsensitive */ true)) {
      return true;
    }
  }
  if (ruleAction === 'allow') return false;
  // Strip env prefix FIRST (handles quoted values like FOO="a b"), THEN collapse
  // obfuscation and drop quote/backtick characters from the remaining command.
  const deob = deobfuscateEnvExpansion(strippedRaw).replace(/["'`]/g, '');
  if (deob === strippedRaw) return false;
  // A deny/ask `prefix:*` rule must also catch the de-obfuscated form so a denied
  // prefix cannot be hidden behind quote/expansion obfuscation.
  if (rulePrefix !== null && commandHasRulePrefix(deob, rulePrefix)) return true;
  return matchPattern(pattern, deob);
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

/** Strip single-quoted spans — in bash their contents are fully literal (no
 *  `$`/backtick/`$(…)` expansion), so a `$SECRET` inside `'…'` is inert. Double
 *  quotes are kept because they DO expand. Single quotes cannot be escaped in
 *  bash, so no backslash handling is needed. */
function stripSingleQuotedSpans(s: string): string {
  let out = '';
  let inSingle = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i]!;
    if (c === "'") { inSingle = !inSingle; continue; }
    if (!inSingle) out += c;
  }
  return out;
}

/** True when a single argument token carries a VARIABLE expansion whose value
 *  an allow rule never vetted: a reference `$NAME` / `${NAME}` / `$env:NAME`
 *  whose name is not a vetted {@link SAFE_ENV_VARS} entry. Arithmetic `$((…))`
 *  and special/positional params (`$1`, `$@`, `$?`, `$$`) are not value leaks.
 *  Command substitutions (`$(…)` / backtick) are intentionally NOT handled here
 *  — AMI already decomposes substitutions in the deny/ask matcher (and elevates
 *  a destructive inner to a prompt), so folding them in would both regress the
 *  "benign `$(date)` stays allowed" behavior and duplicate that path. */
function argTokenLeaksValue(token: string, vetted?: ReadonlySet<string>): boolean {
  // Named variable expansion: `$NAME`, `${NAME}`, or PowerShell-style `$env:NAME`.
  const re = /\$\{?(?:env:)?([A-Za-z_][A-Za-z0-9_]*)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(token)) !== null) {
    const name = m[1]!;
    // A vetted SAFE_ENV_VARS name OR a loop variable bound to a non-leaking
    // word-list (see {@link collectSafeLoopVars}) is not a value leak.
    if (!SAFE_ENV_VARS.has(name) && !(vetted && vetted.has(name))) return true;
  }
  return false;
}

/**
 * Variable names bound by a `for NAME in LIST` whose word-LIST introduces no
 * unvetted value expansion (a glob/literal list such as `*.ts` or `a b c`). Such
 * a loop variable's runtime values come from that already-vetted list — NOT from
 * an unvetted env/secret — so expanding it in a loop-body argument is not a value
 * leak and must not withhold allow-rule auto-approval (the for-loop-with-glob
 * perk: `for f in *.ts; do cat "$f"; done` stays auto-safe, matching the
 * {@link containsShellSubstitution} tolerance of a quoted loop-body `"$f"`).
 *
 * SECURITY: a list that itself leaks (`for f in $SECRET; do echo "$f"; done`)
 * does NOT vet its variable — `$f` would then iterate the secret's words — so the
 * body's `"$f"` still forces an explicit prompt. Bindings are collected in source
 * order so a later loop's list can safely reference an earlier vetted loop var.
 */
function collectSafeLoopVars(command: string): Set<string> {
  const vetted = new Set<string>();
  // `for NAME in LIST` — LIST runs to the next `;`/newline (where `do` follows).
  const forRe = /\bfor\s+([A-Za-z_][A-Za-z0-9_]*)\s+in\s+([^\n;]*)/g;
  let m: RegExpExecArray | null;
  while ((m = forRe.exec(command)) !== null) {
    const name = m[1]!;
    const listTokens = stripSingleQuotedSpans(m[2]!).trim().split(/\s+/).filter(Boolean);
    if (!listTokens.some(t => argTokenLeaksValue(t, vetted))) vetted.add(name);
  }
  return vetted;
}

/**
 * True when ANY argument of ANY sub-command expands a non-vetted variable — i.e.
 * a user `allow` rule on the command would NOT cover what the expansion actually
 * injects. A user who writes `Bash(echo:*)` asserts that running `echo` is safe,
 * not that `echo $ANTHROPIC_API_KEY` (secret exfiltration) or
 * `echo $env:ANTHROPIC_API_KEY` is safe. The permission engine uses this to
 * withhold allow-rule auto-approval for such a command and force an explicit
 * prompt.
 *
 * Mirrors Claude Code's `argLeaksValue` (PowerShell `readOnlyValidation.ts`),
 * ported to bash variable-expansion syntax: command-word position is exempt (it
 * is what the allow rule names); only ARGUMENTS are inspected. A reference to a
 * vetted {@link SAFE_ENV_VARS} name (`$NODE_ENV`, `$LANG`) is NOT a leak — those
 * are the same names stripped from an allow-rule env prefix, so the two paths
 * agree on what counts as a safe environment passthrough. Command substitutions
 * are handled separately by the deny/ask substitution-decomposition path.
 */
export function bashCommandArgLeaksValue(command: string): boolean {
  const normalized = normalizeLineContinuations(command);
  const vettedLoopVars = collectSafeLoopVars(normalized);
  for (const sub of splitCompoundCommand(normalized)) {
    const stripped = stripSingleQuotedSpans(sub).trim();
    if (!stripped) continue;
    const tokens = stripped.split(/\s+/);
    for (let i = 1; i < tokens.length; i++) {
      if (argTokenLeaksValue(tokens[i]!, vettedLoopVars)) return true;
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
  /** Single observer notified by {@link applyMode} on every real mode transition,
   *  regardless of which mutation path (setMode / exitPlanMode / deserialize)
   *  triggered it. Parity: Claude's single `notifyPermissionModeChanged` choke point. */
  private _modeChangeListener?: PermissionModeChangeListener;
  private rules: PermissionRule[] = [];
  /**
   * Dangerous code-exec `allow` rules that were REMOVED because they came from an
   * untrusted (shared/committable) settings tier — a project/`command` rule is
   * attacker-PR-able, so a broad interpreter grant it carries (`Bash(python:*)`)
   * must not silently widen auto-approval. Retained for reporting (diagnostics /
   * introspection) via {@link getStrippedDangerousRules}. Parity: Claude's
   * `ToolPermissionContext.strippedDangerousRules`.
   */
  private strippedDangerousRules: PermissionRule[] = [];
  /**
   * Overly-broad bash `allow` rules ({@link isOverlyBroadBashAllowRule}: pattern-less
   * / `*` bash or all-tools allow) that were stripped from an untrusted shared tier
   * at load / settings-sync time. Retained for reporting (diagnostics / introspection)
   * via {@link getStrippedOverlyBroadBashRules}. Parity: the rules dropped by Claude's
   * `removeDangerousPermissions(findOverlyBroadBashPermissions(...))`.
   */
  private strippedOverlyBroadBashRules: PermissionRule[] = [];
  private deniedCommands: Set<string> = new Set();
  private _disableBypass = false;
  /** Human-readable explanation for WHY bypass/auto-allow is currently blocked
   *  (set alongside {@link _disableBypass}). Carried so `/permissions` and the
   *  Shift+Tab "auto mode unavailable" message can tell the operator what to do
   *  — e.g. "blocked by managed policy (disableBypassPermissionsMode)" — rather
   *  than surfacing a bare boolean. Mirrors Claude's reason-differentiated
   *  getAutoModeUnavailableReason (settings vs circuit-breaker vs org-allowlist).
   *  Cleared when bypass is re-enabled. */
  private _disableBypassReason?: string;
  /** Whether bypass-permissions mode was AVAILABLE when this session started
   *  (Claude's ToolPermissionContext.isBypassPermissionsModeAvailable). It is the
   *  POSITIVE counterpart to {@link _disableBypass}: a session launched directly in
   *  `bypassPermissions` has bypass available, and a later switch to `plan` inherits
   *  bypass semantics (see {@link check}). Defaults from the constructor's initial
   *  mode; the CLI can also set it from a launch flag via {@link setBypassModeAvailable}. */
  private _bypassModeAvailable = false;
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
   *  silent `cat ../../other-project/secret` stays blocked.
   *
   *  Source-tracked (parity: Claude's `additionalWorkingDirectories` Map keyed by
   *  path with a `{path, source}` value): the value records WHICH tier granted the
   *  root so (a) a grant can be attributed/removed by provenance and (b) only the
   *  PERSISTABLE tiers (not ephemeral `session`/`cliArg`) are written back to
   *  `permissions.additionalDirectories` by {@link saveRules}. An in-session
   *  `/add-dir` grant (`session`) therefore stays in-memory, while a committed
   *  project `additionalDirectories` entry is reloaded on the next launch. */
  private allowedReadRoots: Map<string, PermissionRuleSource> = new Map();

  /** Active bash isolation policy (threaded from the engine's EngineConfig.sandbox).
   *  Consulted ONLY to honor {@link SandboxPolicy.autoAllowBashIfSandboxed}: when the
   *  policy runs every bash command in a container, the container IS the safety
   *  boundary, so the opt-in knob lets `check()` skip the prompt. Undefined → the
   *  feature is inert and bash is gated exactly as before. */
  private sandboxPolicy?: SandboxPolicy;
  /** Resolver injected by the engine that reports whether a bash `command` WILL be
   *  wrapped by the HOST (unshare) auto-sandbox (core's `commandWillBeHostSandboxed`).
   *  Backs the HOST-mode arm of `autoAllowBashIfSandboxed`: when the opt-in knob is on
   *  in a non-container policy, exactly the commands that WILL be sandboxed are
   *  auto-allowed and sandbox-incompatible ones fall through to a regular permission
   *  prompt (parity: Claude Code's "auto-allow" sandbox mode — try sandboxed, else
   *  regular permissions). Undefined → host-mode auto-allow is inert (container-mode
   *  behavior unchanged). */
  private sandboxEligibilityResolver?: (command: string) => boolean;
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

  /** Optional async safety classifier injected by the engine/common layer, consulted by
   *  {@link check} on the autonomous auto-approve path to DOWNGRADE (never upgrade) a
   *  would-be auto-approval. Undefined → inert (autonomous behavior unchanged). See
   *  {@link AutoModeSafetyClassifier} and {@link setAutoModeSafetyClassifier}. */
  private autoModeSafetyClassifier?: AutoModeSafetyClassifier;
  /** Optional listener notified when the {@link autoModeSafetyClassifier} STARTS
   *  (`checking:true`) and FINISHES (`checking:false`) evaluating a tool call, so the
   *  engine can surface an in-flight "safety check in progress" signal while the async
   *  classifier query runs. Fired ONLY around an actual classifier consult (not on the
   *  fast-path allow). Parity: Claude's `setClassifierChecking`/`clearClassifierChecking`
   *  subscribable signal (`classifierApprovals.ts`). Undefined → no signal emitted. */
  private classifierCheckingListener?: (checking: boolean, toolName: string) => void;
  /** Optional speculative bash-classifier cache injected by the engine/common layer.
   *  Consulted (via {@link primeSpeculativeBashCheck} / {@link resolveSpeculativeBashApproval})
   *  on the INTERACTIVE ask path only: a high-confidence match that resolves inside the
   *  grace window turns a would-be prompt into an allow. Undefined → inert (the prompt
   *  path is unchanged). See {@link SpeculativeClassifierCache}. */
  private speculativeClassifierCache?: SpeculativeClassifierCache;
  /** Transient carry-slot for the reason attached to the MOST RECENT classifier-driven
   *  allow→ask/deny downgrade in {@link check}, consumed (and cleared) by
   *  {@link deriveDecisionReason} so {@link checkWithReason} can attribute the verdict to
   *  the classifier. Scoped to a single check()→deriveDecisionReason() hop (deriveDecisionReason
   *  runs before any re-entrant per-segment check()), so it never leaks across calls. */
  private _pendingClassifierReason?: string;

  /** Unrecognized top-level keys read from `.ami/permissions.json` by {@link loadRules}
   *  (anything other than `mode`/`rules` — e.g. `hooks`, `env`, or a key a future AMI
   *  version adds). {@link saveRules} spreads these back so a round-trip through this
   *  manager does not DESTROY settings it doesn't model. Mirrors Claude's
   *  `...settingsData` preservation spread (permissionsLoader.ts L193-L216/L277-L295). */
  private _extraPersistedKeys: Record<string, unknown> = {};

  // Denial circuit breaker — abort after too many consecutive or total denials
  private consecutiveDenials = 0;
  private totalDenials = 0;
  private readonly MAX_CONSECUTIVE = 3;
  private readonly MAX_TOTAL = 20;

  constructor(mode?: PermissionMode, rules?: PermissionRule[], allowedRoots?: string[]) {
    if (mode) this.applyMode(mode, 'init');
    if (rules) this.rules = [...rules];
    if (allowedRoots) for (const r of allowedRoots) this.registerAllowedRoot(r);
    // A session launched directly in bypassPermissions has bypass available, so a
    // later cycle into plan mode inherits it (parity: isBypassPermissionsModeAvailable).
    if (mode === 'bypassPermissions') this._bypassModeAvailable = true;
  }

  /**
   * Emit a human-readable debug trace of a permission-state mutation. This is the
   * introspection surface for the permission layer — EVERY rule/mode/directory
   * change (add, remove, mode switch, persist) routes one descriptive line through
   * the core logger, so an operator can see exactly what the permission layer
   * changed and why. Routed through {@link logDebug} so it honors the debug-mode
   * gate, credential redaction, and the on-disk/stderr routing the logger already
   * owns (no separate sink). Mirrors Claude Code's `logForDebugging` calls in
   * applyPermissionUpdate / persistPermissionUpdate (utils/permissions/PermissionUpdate.ts).
   */
  private traceMutation(message: string, data?: Record<string, unknown>): void {
    logDebug('permissions', message, data);
  }

  /**
   * Grant read access to an absolute path (file or directory) that lies OUTSIDE the
   * launch cwd. Sources: `/add-dir`, `--context-file`/`--image` launch flags, and
   * user-approved out-of-workspace read prompts. Stored resolved + normalized so the
   * containment check is a simple prefix match. Idempotent; ignores empty input.
   *
   * `source` records the granting tier (parity: Claude's per-directory `{path,
   * source}`). It defaults to the ephemeral `session` tier so legacy callers
   * (`/add-dir`, `--context-file`, approved prompts) grant an in-memory root that is
   * NOT persisted. Pass a persistable tier (`projectSettings`/`userSettings`/
   * `localSettings`) to make the grant survive across sessions via
   * {@link saveRules} → `permissions.additionalDirectories`. A re-grant with a
   * persistable source UPGRADES an existing session root so it becomes persisted.
   */
  registerAllowedRoot(p: string, source: PermissionRuleSource = 'session'): void {
    if (!p || typeof p !== 'string') return;
    try {
      const abs = path.isAbsolute(p) ? p : path.resolve(p);
      // NFC-canonicalize the key so a root granted in a decomposed (NFD) Unicode
      // spelling byte-matches the NFC-normalized candidate path from
      // resolveForCheck (realpathNFC). Mirrors Claude's state.ts setProjectRoot /
      // setCwdState `.normalize('NFC')` on every stored root path.
      const norm = path.normalize(abs).normalize('NFC');
      const prior = this.allowedReadRoots.get(norm);
      this.allowedReadRoots.set(norm, source);
      // Only trace a real widening of the read boundary (idempotent re-grants of the
      // SAME source are silent), mirroring Claude's "Adding N director(y/ies)" trace.
      // A provenance change (e.g. session → projectSettings) is a meaningful upgrade
      // worth tracing because it flips the dir from in-memory to persisted.
      if (prior === undefined) this.traceMutation(`Adding read directory: ${norm}`);
      else if (prior !== source) this.traceMutation(`Re-scoping read directory ${norm}: ${prior} → ${source}`);
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
      // Match registerAllowedRoot's NFC canonicalization so a root granted in one
      // Unicode form can be revoked by the other (NFD vs NFC spelling).
      const norm = path.normalize(abs).normalize('NFC');
      const removed = this.allowedReadRoots.delete(norm);
      // (parity: Claude's removeDirectories set-filter — a removed dir is dropped
      //  from the persisted `additionalDirectories` on the next saveRules too.)
      // Trace only an actual boundary shrink, mirroring Claude's "Removing N
      // director(y/ies)" trace.
      if (removed) this.traceMutation(`Removing read directory: ${norm}`);
      return removed;
    } catch {
      return false;
    }
  }

  /** The user-granted read roots (absolute, normalized). For inspection/tests. */
  getAllowedReadRoots(): string[] {
    return [...this.allowedReadRoots.keys()];
  }

  /**
   * The user-granted read roots WITH their granting tier (parity: Claude's
   * source-tracked `additionalWorkingDirectories` Map value `{path, source}`). Lets
   * a diagnostic / `/permissions` view attribute each extra directory to its origin
   * (a committed project setting vs an in-session `/add-dir`) and lets a caller
   * selectively remove or re-persist by provenance. Order-stable (insertion order).
   */
  getAllowedReadRootEntries(): { path: string; source: PermissionRuleSource }[] {
    return [...this.allowedReadRoots].map(([p, source]) => ({ path: p, source }));
  }

  /**
   * The subset of granted read roots that are SAFE TO PERSIST to
   * `permissions.additionalDirectories` — everything except the ephemeral
   * `session` and `cliArg` tiers, which are deliberately in-memory only (a
   * per-launch `/add-dir`/`--context-file` grant must not silently leak into a
   * committed settings file). De-duped (a `Map` key is already unique) and
   * order-stable. Mirrors Claude's `persistPermissionUpdate` dedupe-on-add.
   */
  private persistableReadRoots(): string[] {
    const out: string[] = [];
    for (const [p, source] of this.allowedReadRoots) {
      if (source !== 'session' && source !== 'cliArg') out.push(p);
    }
    return out;
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
    const roots = [...this.allowedReadRoots.keys()];
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
   * Inject the host-sandbox eligibility resolver (core's `commandWillBeHostSandboxed`,
   * pre-bound by the engine with the active exemption/platform options). Enables the
   * HOST-mode arm of {@link SandboxPolicy.autoAllowBashIfSandboxed}: a command that
   * WILL be host-sandboxed is auto-allowed, while a sandbox-incompatible command falls
   * through to a regular permission prompt. Passing `undefined` makes host-mode
   * auto-allow inert (container-mode behavior is unchanged).
   */
  setSandboxEligibilityResolver(resolver: ((command: string) => boolean) | undefined): void {
    this.sandboxEligibilityResolver = resolver;
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
   * Register (or clear, with `undefined`) the autonomous-mode safety classifier
   * ({@link AutoModeSafetyClassifier}). When set, {@link check} consults it on the
   * auto-approve path of the autonomous modes and lets it DOWNGRADE a would-be
   * auto-approval to `ask`/`deny` with a recorded reason. The deterministic engine is
   * the floor — the classifier can only tighten it, never loosen it — and any
   * `undefined`/`allow`/throw FAILS OPEN to the deterministic decision. The engine
   * injects a concrete (e.g. model-backed) classifier here; unset leaves autonomous
   * behavior byte-for-byte unchanged. Parity: Claude's transcript/yolo classifier gate.
   */
  setAutoModeSafetyClassifier(fn: AutoModeSafetyClassifier | undefined): void {
    this.autoModeSafetyClassifier = fn;
  }

  /** Whether an autonomous-mode safety classifier is currently installed. */
  hasAutoModeSafetyClassifier(): boolean {
    return this.autoModeSafetyClassifier !== undefined;
  }

  /**
   * Register (or clear, with `undefined`) a listener fired when the installed
   * {@link setAutoModeSafetyClassifier} STARTS and FINISHES evaluating a tool call.
   * It is invoked with `(true, toolName)` immediately before the async classifier
   * query and `(false, toolName)` once it settles (success, `undefined`, or error) —
   * only when a classifier actually runs for the call. The engine wires this to emit a
   * `classifier_checking` {@link import('./types').EngineEvent} and update the
   * {@link import('./tool-confirmation').ToolConfirmationService} in-flight set. Parity:
   * Claude's `setClassifierChecking`/`clearClassifierChecking` subscribable signal.
   */
  setClassifierCheckingListener(
    fn: ((checking: boolean, toolName: string) => void) | undefined,
  ): void {
    this.classifierCheckingListener = fn;
  }

  /**
   * Register (or clear, with `undefined`) the SPECULATIVE bash-command classifier
   * ({@link SpeculativeBashClassifier}) — the interactive ASK-path grace-race
   * optimization. When set, the engine primes a check for a bash command via
   * {@link primeSpeculativeBashCheck} the moment it needs a decision, then — if the
   * deterministic check resolves to `ask` — calls {@link resolveSpeculativeBashApproval}
   * right before prompting: a HIGH-confidence match that settles inside the grace
   * window auto-approves the command without a prompt. It can only ever turn a
   * would-be prompt into an allow (never deny, never loosen a hardline/deny, which
   * resolve before the ask path). Unset leaves the prompt path byte-for-byte
   * unchanged. Parity: Claude's speculative bash classifier
   * (`bashPermissions.ts` + the race in `useCanUseTool.tsx`). `ttlMs` bounds how
   * long a primed-but-unconsumed check stays live.
   */
  setSpeculativeBashClassifier(
    fn: SpeculativeBashClassifier | undefined,
    options: { ttlMs?: number } = {},
  ): void {
    this.speculativeClassifierCache = fn
      ? new SpeculativeClassifierCache({ classifier: fn, ttlMs: options.ttlMs })
      : undefined;
  }

  /** Whether a speculative bash classifier is currently installed. */
  hasSpeculativeBashClassifier(): boolean {
    return this.speculativeClassifierCache !== undefined;
  }

  /**
   * Prime (start, in the background) a speculative classifier check for a bash
   * `command` so its verdict may be ready by the time a prompt would be shown.
   * No-op when no classifier is installed, and idempotent per command (one in-flight
   * check at a time). Called by the engine as soon as a bash command needs a
   * decision.
   */
  primeSpeculativeBashCheck(command: string): void {
    this.speculativeClassifierCache?.prime(command);
  }

  /**
   * Resolve a primed speculative check for `command`, racing it against `graceMs`.
   * Returns the matched rule description (auto-approve — the engine skips the prompt
   * and allows the command) ONLY when a high-confidence match settles inside the
   * grace window; otherwise `null` (the engine proceeds with the interactive prompt).
   * No-op (`null`) when no classifier is installed or the command was never primed.
   * Called by the engine after {@link check} returns `ask` for a bash command.
   */
  async resolveSpeculativeBashApproval(
    command: string,
    graceMs: number = DEFAULT_SPECULATIVE_GRACE_MS,
  ): Promise<string | null> {
    if (!this.speculativeClassifierCache) return null;
    return this.speculativeClassifierCache.resolveApproval(command, graceMs);
  }

  /**
   * The WHILE-OPEN counterpart of {@link resolveSpeculativeBashApproval}: once the
   * grace race has missed and the interactive prompt is ALREADY on screen, the primed
   * check keeps running; this awaits it to completion and auto-approves the open prompt
   * iff it is a high-confidence match AND the caller's {@link PendingPromptLatch} still
   * permits it (the user has not interacted and nothing else resolved the dialog).
   * Returns the matched description (auto-approve — the UI tears down the dialog and
   * allows the command) or `null` (leave the prompt for the user). No-op (`null`) when
   * no classifier is installed or the command was never primed. The frontend feeds the
   * latch user interactions ({@link PendingPromptLatch.onUserInteraction}) and terminal
   * answers ({@link PendingPromptLatch.claimManualResolve}) so a pressed key can never
   * race-lose to an in-flight auto-approval. Mirrors Claude's background
   * `executeAsyncClassifierCheck` on an open permission dialog (`interactiveHandler.ts`).
   */
  async resolveSpeculativeBashApprovalWhileOpen(
    command: string,
    latch: PendingPromptLatch,
  ): Promise<string | null> {
    if (!this.speculativeClassifierCache) return null;
    return this.speculativeClassifierCache.resolveWhileOpen(command, latch);
  }

  /** Drop every cached speculative check (e.g. on session reset). No-op when no
   *  classifier is installed. */
  clearSpeculativeBashChecks(): void {
    this.speculativeClassifierCache?.clear();
  }

  /**
   * Consult the injected {@link AutoModeSafetyClassifier} for a tool call the
   * deterministic engine is about to auto-approve in an autonomous mode. Returns the
   * (possibly downgraded) verdict. No classifier, an allowlisted-safe tool
   * ({@link isAutoModeSafeTool} — skip the check, mirroring Claude's
   * SAFE_YOLO_ALLOWLISTED_TOOLS fast-path), an `undefined`/`allow` verdict, or a
   * thrown error all FAIL OPEN to `allow`. A downgrade to `ask`/`deny` records the
   * denial (source `'classifier'`) and stashes its reason for
   * {@link deriveDecisionReason}.
   */
  private async applyAutoModeSafetyClassifier(
    toolName: string,
    input: Record<string, unknown>,
  ): Promise<'allow' | 'ask' | 'deny'> {
    const classifier = this.autoModeSafetyClassifier;
    // Fast path: no classifier, or a tool the auto-mode allowlist already deems safe
    // (read-only / coordination) — never spend a classifier call on it.
    if (!classifier || isAutoModeSafeTool(toolName)) return 'allow';
    let verdict: AutoModeSafetyVerdict | undefined;
    // Bracket the async consult with the in-flight CHECKING signal so a frontend can
    // show "safety check in progress" while the classifier query runs. Fired ONLY here
    // (an actual consult), never on the fast-path allow above. The `false` transition
    // runs in `finally` so a thrown/rejected classifier still clears the signal.
    this.classifierCheckingListener?.(true, toolName);
    try {
      verdict = await classifier({ toolName, input, mode: this.mode, staticDecision: 'allow' });
    } catch {
      // Classifier failure must never BLOCK an otherwise-allowed autonomous call:
      // fall back to the deterministic auto-approval (parity: classifier error →
      // static decision). The static engine already screened for hardlines/denies.
      return 'allow';
    } finally {
      this.classifierCheckingListener?.(false, toolName);
    }
    // `allow`, an explicit `passthrough` abstain, or no verdict all FAIL OPEN to
    // the deterministic auto-approval — the classifier declined to tighten it, so
    // no downgrade reason or denial is recorded. `passthrough` is the first-class
    // "no opinion" spelling of returning `undefined` (parity: Claude's passthrough
    // falling through to the normal flow).
    if (!verdict || verdict.decision === 'allow' || verdict.decision === 'passthrough') return 'allow';
    // Downgrade. Carry the reason to deriveDecisionReason so the prompt / denial
    // message can explain WHY the classifier gated this autonomous call.
    this._pendingClassifierReason = verdict.reason ?? '';
    // A `deny` is a genuine block → record it (source 'classifier') so it surfaces in
    // the recent-denials log and feeds the circuit breaker. An `ask` only turns the
    // silent auto-approval into an interactive prompt (not a denial of intent), so it
    // is NOT recorded — matching how a mode-driven `ask` does not trip the breaker.
    if (verdict.decision === 'deny') {
      const display = typeof input.command === 'string' ? input.command : toolName;
      this.recordDenial(display, 'classifier', verdict.reason, 'auto-mode');
    }
    return verdict.decision;
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
   * Whether a bash command would be auto-approved by the `autoAllowBashIfSandboxed`
   * knob (OPT-IN; OFF unless set to exactly `true`, so the default bash prompt is
   * unchanged). Two arms, both requiring the sandbox to be the safety boundary:
   *   - CONTAINER mode: EVERY command runs in a container (host FS not mounted), so
   *     every command is auto-allowed.
   *   - HOST mode (non-container): auto-allow ONLY the commands that WILL actually be
   *     wrapped by the host (unshare) auto-sandbox — reported by the injected
   *     {@link sandboxEligibilityResolver}. A sandbox-incompatible command returns
   *     false here and falls through to a regular permission prompt (parity: Claude
   *     Code's "auto-allow" sandbox mode — try sandboxed, else regular permissions).
   * Claude's enterprise `sandbox.autoAllowBashIfSandboxed`.
   */
  private bashAutoAllowedBySandbox(toolName: string, input: Record<string, unknown>): boolean {
    if (toolName !== 'bash' || typeof input.command !== 'string') return false;
    if (this.sandboxPolicy?.autoAllowBashIfSandboxed !== true) return false;
    // dangerouslyDisableSandbox cannot override the sandbox, but if the model asked for
    // it we still prompt (handled earlier in check); never silently auto-allow here.
    if (input.dangerouslyDisableSandbox === true) return false;
    // Container mode: the container is the boundary → every command is auto-allowed.
    if (this.sandboxPolicy.mode === 'container') return true;
    // Host mode: auto-allow only the commands that WILL be host-sandboxed; others
    // fall through to the normal prompt.
    return this.sandboxEligibilityResolver?.(input.command) === true;
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
    // Clear any stale classifier-downgrade reason from a previous call so a verdict
    // this run that is NOT classifier-driven is never mis-attributed (the slot is set
    // only by applyAutoModeSafetyClassifier and consumed by deriveDecisionReason).
    this._pendingClassifierReason = undefined;

    // PARITY (additive): the env-gated synthetic permission-flow test fixture
    // (testing_permission) UNCONDITIONALLY triggers the permission prompt in every
    // mode — mirroring Claude Code's TestingPermissionTool.checkPermissions(), which
    // always returns {behavior:'ask'}. It is a side-effect-free, test-only tool whose
    // sole purpose is to drive the ask → allow/deny path deterministically. Hard stops
    // still win: deny-all (there is no interactive prompt to answer) denies, and an
    // explicit user deny rule denies; otherwise every mode asks (no standing allow rule
    // can silently auto-approve it). Placed first so no mode fast-path can bypass it.
    if (toolName === TESTING_PERMISSION_TOOL_NAME) {
      if (this.mode === 'deny-all') return 'deny';
      if (this.anyDenyMatches(toolName, input)) return 'deny';
      return 'ask';
    }

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
    // the normally prompt-driven modes (ask / acceptEdits). In HOST mode only the
    // commands that WILL be host-sandboxed take this path; sandbox-incompatible ones
    // fall through to the normal prompt. Hardline blocks and dangerouslyDisableSandbox
    // (both above) and explicit user deny rules still win; deny-all/plan keep denying
    // and auto-allow/bypass already allow, so this only changes behavior in
    // ask/acceptEdits.
    if ((this.mode === 'ask' || this.mode === 'acceptEdits') &&
        this.bashAutoAllowedBySandbox(toolName, input) &&
        !this.anyDenyMatches(toolName, input) &&
        // HOST mode still prompts for sensitive-path reads/writes: the host filesystem
        // IS reachable under the unshare sandbox (unlike container mode where it is not
        // mounted), so a credential read / system-dir write must not silently bypass the
        // prompt. Container mode keeps the full no-prompt behavior (FS not mounted).
        (this.sandboxPolicy?.mode === 'container' || !this.sensitivePathRequiresPrompt(toolName, input))) {
      return 'allow';
    }

    // Auto-allow plain `--help` invocations (parity: Claude's `isHelpCommand`
    // prefix-extraction bypass). A bare help request (`git commit --help`,
    // `python --help`) is side-effect-free, so it never needs a prompt in the
    // rule-evaluated modes — the user is not asked to grant a rule just to read a
    // tool's help text. Hardline blocks (handled above) and explicit user deny
    // rules still win; deny-all/plan keep denying; help forms routed through an
    // unsafe backend (`man -l`, `git help --web`) are excluded by isHelpCommand.
    if ((this.mode === 'ask' || this.mode === 'acceptEdits') &&
        toolName === 'bash' && typeof input.command === 'string' &&
        isHelpCommand(input.command) &&
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
      return this.applyAutoModeSafetyClassifier(toolName, input);
    }
    // Screened autonomy: `auto` runs tool calls without a prompt (like auto-allow)
    // but screens EACH call for risk + prompt injection first, blocking the risky
    // ones so an autonomous run is bounded without interrupting the user. Ordering
    // mirrors the other autonomous branches: explicit user deny/ask rules win, then
    // sensitive-path safety prompts, then the per-call screen decides allow vs block
    // ({@link screenAutoModeToolCall}). A blocked call returns `deny`, which the
    // engine reports back so the model can try a different approach (Claude's auto
    // mode block-and-retry fallback).
    if (this.mode === 'auto') {
      if (this.anyDenyMatches(toolName, input, /* skipParamPattern */ true)) return 'deny';
      if (this.anyAskMatches(toolName, input)) return 'ask';
      if (this.sensitivePathRequiresPrompt(toolName, input)) return 'ask';
      // Static screen first; only when IT allows does the injected classifier get a
      // chance to tighten the verdict (a screen-blocked call is already `deny`/`ask`).
      const screened = screenAutoModeToolCall(toolName, input).decision;
      if (screened !== 'allow') return screened;
      return this.applyAutoModeSafetyClassifier(toolName, input);
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
      return this.applyAutoModeSafetyClassifier(toolName, input);
    }

    if (this.mode === 'plan') {
      // A plan run that was STARTED while bypass was available inherits bypass
      // semantics instead of the read-only allowlist (parity: Claude's
      // shouldBypassPermissions = mode==='plan' && isBypassPermissionsModeAvailable).
      // Explicit user deny/ask rules and sensitive-path safety still win first —
      // identical ordering to the bypassPermissions branch above — so an inherited
      // bypass never silently overrides a hard stop.
      if (this._bypassModeAvailable) {
        if (this.anyDenyMatches(toolName, input)) return 'deny';
        if (this.anyAskMatches(toolName, input)) return 'ask';
        if (this.sensitivePathRequiresPrompt(toolName, input)) return 'ask';
        return this.applyAutoModeSafetyClassifier(toolName, input);
      }
      const readOnlyTools = new Set(['file_read', 'grep', 'glob', 'list_dir', 'web_fetch', 'web_search', 'search_symbols', 'tool_search', 'task_tracker', 'task_list', 'task_output', 'todo_read']);
      if (readOnlyTools.has(toolName)) return 'allow';
      return 'deny';
    }

    // acceptEdits: auto-allow file-mutating tools and safe bash
    if (this.mode === 'acceptEdits') {
      if (ACCEPT_EDITS_EDIT_TOOLS.has(toolName)) {
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
        // (mkdir/touch/mv/cp/sed) — Claude's ACCEPT_EDITS_ALLOWED_COMMANDS — and
        // their PowerShell counterpart (Set-Content/Add-Content/Clear-Content and
        // aliases — Claude's ACCEPT_EDITS_ALLOWED_CMDLETS, minus remove-item which
        // AMI keeps gated) — but only when the command is not destructive
        // (rm/rmdir/Remove-Item stay gated) and does not touch a credential or
        // system path (those keep their explicit prompt).
        if (classification !== 'destructive' &&
            (isAcceptEditsFilesystemMutation(input.command) ||
             isAcceptEditsPowerShellWrite(input.command)) &&
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

    // argLeaksValue guard — an `allow` rule must NOT auto-approve a bash command
    // whose ARGUMENTS expand a non-vetted variable (secret exfil, e.g.
    // `echo $ANTHROPIC_API_KEY`) or a command substitution (arbitrary code, e.g.
    // `cp $(curl evil) .`): the rule asserts the COMMAND is safe, not that
    // arbitrary expansion through it is. Fires in the rule-evaluated modes (deny
    // already resolved above and still wins), forcing an explicit prompt. Mirrors
    // Claude's argLeaksValue allow-path gate. A vetted SAFE_ENV_VARS reference
    // (`$NODE_ENV`) is not a leak and stays auto-approvable.
    if ((this.mode === 'ask' || this.mode === 'acceptEdits') &&
        toolName === 'bash' && typeof input.command === 'string' &&
        bashCommandArgLeaksValue(input.command)) {
      if (this._memo) this._memo.set(toolName, this._hashInput(input), 'ask');
      return 'ask';
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
        const matched = (toolName === 'bash' && typeof input.command === 'string'
          ? matchBashPattern(rule.pattern, inputStr, rule.action)
          : matchPattern(rule.pattern, inputStr))
          // ADDITIVE: a rooted file-rule pattern (`//abs/**`, `~/x`, `/src/**`) that
          // the raw matcher cannot resolve still matches its named file per-root.
          || this.matchFilePathRuleWithRoot(rule, toolName, input);
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
    /**
     * For a COMPOUND bash command (`a && b | c`) that did not auto-allow, the
     * per-segment verdict map: each pipe/`&&`/`;` segment evaluated INDEPENDENTLY
     * through {@link check} so a prompt/executor can show WHICH specific segment
     * forced the ask/deny instead of collapsing the whole chain to one verdict.
     * The aggregate `decision` above stays authoritative (deny-wins); this map is
     * purely additive diagnostics. Omitted for single (non-compound) commands,
     * non-bash tools, and the auto-allow fast path. Mirrors Claude's
     * BashPermissionRequest `subcommandResults: Map<string, PermissionResult>`
     * (tools/BashTool/bashCommandHelpers.ts L84-156).
     */
    subcommandResults?: Record<string, 'allow' | 'deny' | 'ask'>;
    /**
     * The EXACT path that tripped the gate, when the ask/deny was triggered by a
     * specific filesystem target (an out-of-workspace path, or a file-tool's
     * `file_path`/`path`). Lets a prompt/denial name the offending path ("… for
     * /etc/hosts") instead of only the whole tool call. Mirrors Claude's
     * `PermissionAskDecision.blockedPath` / `PermissionResult.blockedPath`
     * (types/permissions.ts L196-266). Omitted when no single path is at fault
     * (e.g. a mode-wide prompt, a bash-rule deny) and always omitted on `allow`.
     */
    blockedPath?: string;
  }> {
    const decision = await this.check(toolName, input);
    if (decision === 'allow') return { decision };
    const reason = this.deriveDecisionReason(toolName, input, decision);
    const suggestions = buildPermissionSuggestions(toolName, input);
    const subcommandResults = await this.evaluateCompoundSubcommands(toolName, input);
    const blockedPath = this.deriveBlockedPath(toolName, input, reason);
    return {
      decision,
      reason,
      message: createPermissionRequestMessage(reason, permissionPromptToolType(toolName)),
      ...(suggestions.length > 0 ? { suggestions } : {}),
      ...(decision === 'deny' && reason.type === 'hardline' ? { interrupt: true } : {}),
      ...(subcommandResults ? { subcommandResults } : {}),
      ...(blockedPath ? { blockedPath } : {}),
    };
  }

  /**
   * The specific filesystem path a non-allow verdict is about, or `undefined`
   * when the verdict is not path-specific. Prefers the structured `workingDir`
   * reason's offending path; otherwise falls back to a file tool's own
   * `file_path`/`path` input. Pure read — never mutates. Parity with Claude's
   * `blockedPath`, surfaced so a prompt/denial can name the exact path.
   */
  private deriveBlockedPath(
    toolName: string,
    input: Record<string, unknown>,
    reason: PermissionDecisionReason,
  ): string | undefined {
    if (reason.type === 'workingDir') return reason.path;
    const filePath = input.file_path ?? input.path;
    return typeof filePath === 'string' && filePath.length > 0 ? filePath : undefined;
  }

  /**
   * Structured, machine-readable reason WHY a tool call was AUTO-ALLOWED — the
   * introspection counterpart to {@link deriveDecisionReason} (which only explains
   * ask/deny). Call AFTER {@link check}/{@link checkWithReason} returned `allow`.
   * Attribution mirrors {@link check}'s own precedence — the only two ways AMI ever
   * auto-approves a call:
   *  1. a standing `allow` RULE matched → `{ type:'rule', action:'allow', ruleValue }`
   *     (reuses {@link matchedAllowRule}, the single rule-provenance source);
   *  2. otherwise the active permission MODE granted it (bypassPermissions / auto-allow /
   *     acceptEdits / plan read-only) → `{ type:'mode', mode }` — parity with Claude's
   *     auto-allow `decisionReason: { type:'mode', mode:'acceptEdits' }` (PowerShellTool
   *     modeValidation.ts L396-403). (The default `ask` mode never auto-allows a rule-less
   *     call, so there is no third, classifier-only arm to attribute.)
   * So an audit/introspection layer can record the BASIS of an auto-approval
   * (explicit rule vs permission mode), which a bare `'allow'` loses.
   */
  explainAllow(toolName: string, input: Record<string, unknown>): PermissionDecisionReason {
    const ruleLabel = this.matchedAllowRule(toolName, input);
    if (ruleLabel) return { type: 'rule', action: 'allow', ruleValue: ruleLabel };
    return { type: 'mode', mode: this.mode };
  }

  /**
   * Evaluate EACH segment of a compound bash command independently through
   * {@link check}, returning a `{ segment: verdict }` map — the data a UI/executor
   * needs to attribute a compound command's ask/deny to the exact offending
   * segment (parity: Claude's per-segment `subcommandResults`, deny-wins
   * aggregation). Returns `undefined` for a non-bash tool, a non-string command,
   * or a single (non-compound) command — cases with nothing to disambiguate.
   * Segments are de-duplicated (a repeated segment is evaluated once).
   */
  private async evaluateCompoundSubcommands(
    toolName: string,
    input: Record<string, unknown>,
  ): Promise<Record<string, 'allow' | 'deny' | 'ask'> | undefined> {
    if (toolName !== 'bash' || typeof input.command !== 'string') return undefined;
    const segments = splitCompoundCommand(input.command);
    if (segments.length <= 1) return undefined;
    const results: Record<string, 'allow' | 'deny' | 'ask'> = {};
    for (const segment of segments) {
      if (segment in results) continue;
      results[segment] = await this.check('bash', { ...input, command: segment });
    }
    return results;
  }

  /** Best-effort attribution of a deny/ask verdict to a structured reason. */
  private deriveDecisionReason(
    toolName: string,
    input: Record<string, unknown>,
    decision: 'deny' | 'ask',
  ): PermissionDecisionReason {
    // The verdict came from the injected auto-mode safety classifier (check() set the
    // carry-slot on its downgrade): attribute it to the classifier so the prompt /
    // denial message explains WHY, and consume the slot so it never leaks to a later
    // non-classifier verdict. Only set on a classifier-driven downgrade, so its mere
    // presence is authoritative here.
    const classifierReason = this._pendingClassifierReason;
    this._pendingClassifierReason = undefined;
    if (classifierReason !== undefined) {
      return { type: 'classifier', ...(classifierReason ? { reason: classifierReason } : {}) };
    }
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
        ? ((toolName === 'bash' && command
            ? matchBashPattern(rule.pattern, command, rule.action)
            : matchPattern(rule.pattern, this.inputToString(toolName, input)))
            // Attribute a verdict produced by the rooted file-rule resolution (above)
            // to the rule that matched, so the reason is not left unattributed.
            || this.matchFilePathRuleWithRoot(rule, toolName, input))
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
      // argLeaksValue: the ask verdict came from an allow rule being withheld
      // because an argument expands a non-vetted variable / command substitution.
      if (decision === 'ask' && (this.mode === 'ask' || this.mode === 'acceptEdits') &&
          bashCommandArgLeaksValue(command)) {
        return {
          type: 'safetyCheck', check: 'argLeak',
          message: 'Command expands a variable or command substitution its allow rule did not vet and requires approval',
        };
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

    // Fail-closed on an UNPARSEABLE command: if a quote is left open a real shell's
    // tokenizer would error, so the naive whitespace/regex splitting below would see
    // a different token stream than the shell actually runs — a parser differential
    // that could hide a dangerous command (e.g. `echo "ls` auto-allowed as a bare
    // `echo`). Classify it `'unsafe'` so it prompts rather than being proven safe.
    // Heredoc bodies are stripped first so a legitimate `'`/`"` inside a heredoc body
    // (not shell syntax) never trips the balance check. Mirrors Claude Code's
    // tryParseShellCommand → require-permission-on-parse-failure gate.
    if (hasUnbalancedQuotes(stripHeredocBodies(trimmed))) return 'unsafe';

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
      // `git clean` is flag-dependent: a `-n`/`--dry-run` preview only LISTS what
      // would be removed (read-only) and must NOT be escalated; only an actual
      // clean (`-f`/`--force`) deletes untracked files. Shares GIT_CLEAN_FORCE_RE
      // with the warning engine so both paths agree (parity: Claude excludes the
      // dry-run from the destructive boundary).
      if (cmd === 'git clean') {
        if (GIT_CLEAN_FORCE_RE.test(trimmed)) return 'destructive';
        continue;
      }
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
        // A safe subcommand is only auto-allowed when its FLAGS are also known-safe
        // (`docker logs`/`docker inspect` carry typed flags; an unrecognized flag
        // must prompt rather than be silently granted).
        if (!dockerReadOnlyFlagsSafe(dockerSub, dockerTokens.slice(subIdx + 1))) return 'unsafe';
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
        // A `gh` read-only prefix can still exfiltrate via a foreign HOST/OWNER/REPO
        // spec (`gh pr list --repo evil.host/owner/repo`) or smuggle a write in a
        // compound. Revalidate the full command before auto-allowing (gh analogue of
        // the git guard above). Mirrors Claude's ghIsDangerousCallback.
        if (cmd.startsWith('gh ') && !isGhReadOnlyCommandSafe(unwrapped)) return 'unsafe';
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

    // sed without an in-place write flag is read-only (prints to stdout). Uses a
    // per-segment, quote-aware tokenized check so a `-...i` flag belonging to a
    // different command in a compound, or a quoted `-i` inside the sed script, no
    // longer forces a misclassification (parity: Claude's tokenized isReadOnlySed).
    if (isReadOnlySedCommand(trimmed)) return 'safe';
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
    const expanded = expandPath(filePath, cwd);
    // Special-file guard BEFORE realpath: lstat the target and bail for FIFOs,
    // sockets, and char/block devices. fs.realpathSync can block indefinitely on a
    // FIFO waiting for a writer (an unbounded hang with no timeout), so a special
    // file is never realpath-resolved — its expanded (NFC) form is returned as-is.
    // A non-existent path (ENOENT) or unreadable parent falls through to realpathNFC,
    // which tolerates it (write targets keep their expanded form). Mirrors Claude's
    // safeResolvePath special-file screen (utils/fsOperations.ts L148-161).
    try {
      const st = fs.lstatSync(expanded);
      if (st.isFIFO() || st.isSocket() || st.isCharacterDevice() || st.isBlockDevice()) {
        return expanded.normalize('NFC');
      }
    } catch {
      // not-yet-existing / unreadable — realpathNFC handles it below
    }
    // Canonicalize to the real location and re-apply NFC normalization: on an
    // NFD-storing filesystem fs.realpathSync returns the decomposed on-disk
    // spelling, which would not byte-match an NFC cwd/rule. A not-yet-existing
    // write target keeps its already-NFC expanded form. Shared helper so the
    // single realpath+NFC rule is not re-implemented per call site.
    return realpathNFC(expanded);
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
    // NFC-normalize before case-folding so an NFD-spelled cwd/root compares equal
    // to the NFC-canonicalized `resolved` candidate (resolveForCheck -> realpathNFC);
    // keeps the containment boundary Unicode-canonical on both sides.
    // macOS /private symlink-equivalence: /private/var→/var, /private/tmp→/tmp.
    // Lexical (not realpath) so it also folds a NON-EXISTENT write target, whose
    // real path cannot be resolved. Applied to both sides before case/NFC folding.
    // Mirrors Claude's pathInWorkingPath private-prefix normalization. On Linux the
    // prefixes never occur, so this is a no-op.
    const foldPrivate = (p: string) =>
      p.replace(/^\/private\/var\//, '/var/').replace(/^\/private\/tmp(\/|$)/, '/tmp$1');
    const strip = (p: string) =>
      normalizeCaseForComparison(foldPrivate(p.replace(/[/\\]+$/, '') || p).normalize('NFC'));
    const r = strip(resolved);
    const contains = (base: string): boolean => r === base || r.startsWith(base + path.sep);
    // A workspace base (cwd or a granted root) matches both in its NORMALIZED form
    // and its REALPATH-resolved form, so a launch cwd / root that is itself a
    // symlink (e.g. process.env.PWD symlinked to the real cwd) still contains the
    // realpath-canonicalized candidate `resolved`. realpath is wrapped in try/catch
    // for a not-yet-existing base. Mirrors Claude's symlinked-PWD working-dir
    // registration (permissionSetup.ts isSymlinkTo). This only ADDS matches — it
    // never widens past the real directory tree, so it cannot cause a false allow.
    for (const base of [path.normalize(cwd), ...this.allowedReadRoots.keys()]) {
      if (contains(strip(base))) return true;
      try {
        const real = fs.realpathSync(base);
        if (real !== base && contains(strip(real))) return true;
      } catch {
        // non-existent base — the normalized form above is the only candidate
      }
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
    if (detectPathExpansionHazard(filePath)) {
      // Deny-only guess BEFORE prompting (parity: Claude's checkDenyRuleForGuessedPath,
      // PowerShellTool/pathValidation.ts L984-L1008): strip the obscuring construct to a
      // best-effort literal and, if THAT resolves to a forbidden credential/system path,
      // DENY outright instead of merely prompting. Without this an obscured forbidden
      // path (`${HOME}/.ssh/id_rsa`, `` /x/`whoami`/../../.aws/credentials ``) downgrades
      // a hard deny to an "ask" — which an auto-allow session then silently grants (the
      // checkPathPermission auto-grant branch). A tilde-variant is excluded (the guess
      // for `~user` cannot be resolved to a concrete forbidden target) so it still
      // prompts. The guess is used ONLY to deny, never to allow.
      const guess = stripPathObscuring(filePath);
      if (guess && guess !== filePath) {
        const guessResolved = this.resolveForCheck(guess, cwd);
        if (this.isForbiddenReadPath(guessResolved) || this.isForbiddenAtAnyHop(guess, cwd)) {
          return 'deny';
        }
      }
      return 'prompt';
    }
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
  recordDenial(
    display?: string,
    source: RecentDenial['source'] = 'rule',
    reason?: string,
    classifier?: RecentDenial['classifier'],
  ): void {
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
      this._recentDenials.unshift({
        display, source, reason, riskLevel, timestamp: Date.now(),
        ...(classifier !== undefined ? { classifier } : {}),
      });
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
      // The manager's OWN managed-policy bypass killswitch ({@link setDisableBypass})
      // is a settings-category block — fold it in so the manager self-reports a
      // `settings` reason without the caller having to re-derive it. An explicit
      // `disabledBySettings` from the caller still wins when passed.
      disabledBySettings: opts.disabledBySettings ?? this._disableBypass,
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
   * Compose the user-facing circuit-breaker warning when the denial limit has been
   * hit, or `null` while the breaker is open. When the TOTAL limit is reached it
   * reports how many actions were blocked this session; otherwise it reports the
   * consecutive run. Both end with a prompt to review the transcript. This gives
   * the user an actionable sentence explaining WHY they are suddenly being asked to
   * review, instead of a bare `shouldAbort` boolean. Mirrors Claude Code's
   * denial-limit warning composition (utils/permissions/permissions.ts
   * L999-L1058): the total-limit message takes precedence over the consecutive one.
   * Pure read of the live counters — does not mutate or reset them.
   */
  denialLimitWarning(): string | null {
    if (!this.shouldAbort()) return null;
    if (this.totalDenials >= this.MAX_TOTAL) {
      return `${this.totalDenials} actions were blocked this session. Please review the transcript before continuing.`;
    }
    return `${this.consecutiveDenials} consecutive actions were blocked. Please review the transcript before continuing.`;
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
      this.traceMutation(`Refused mode change to '${mode}' (blocked by managed policy)`);
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
    const prev = this.mode;
    this.applyMode(mode, 'setMode');
    // Include the external-facing mode label so an introspection trace shows how the
    // mode is surfaced to external/SDK consumers (toExternalPermissionMode).
    this.traceMutation(`Setting mode to '${mode}' (external: ${toExternalPermissionMode(mode)}) (was '${prev}')`);
    return true;
  }

  /**
   * Register (or clear, with `undefined`) the single listener notified on every
   * real permission-mode transition. Replaces any previously-registered listener —
   * there is exactly one observer, so mode-change auditing has a single wiring point
   * and cannot be double-counted. Parity: Claude's `permissionModeListener` register.
   */
  setModeChangeListener(listener: PermissionModeChangeListener | undefined): void {
    this._modeChangeListener = listener;
  }

  /**
   * The ONE place that mutates {@link mode}. Every mutation path (constructor,
   * {@link setMode}, {@link exitPlanMode}'s fallback, {@link deserialize}) routes
   * through here so a change can never bypass the {@link _modeChangeListener}
   * notification. A no-op (`next === current`) neither assigns nor notifies — only a
   * genuine transition fires the listener, so default→bubble→default-style noise is
   * suppressed, mirroring Claude's `prevMode !== newMode` diff gate. The listener is
   * invoked defensively: a throwing listener must never leave `mode` unchanged or
   * abort the caller. Returns whether a real transition occurred.
   */
  private applyMode(next: PermissionMode, reason: string): boolean {
    const previousMode = this.mode;
    if (previousMode === next) return false;
    this.mode = next;
    // Transition-time context cleanup (parity: Claude's `transitionPermissionMode`,
    // getNextPermissionMode.ts L84-100): entering a MORE permissive mode strips any
    // dangerous code-execution `allow` rule so a broad interpreter grant obtained in
    // one mode (e.g. an `acceptEdits` session grant of `Bash(python:*)`) does not
    // silently compound once the user escalates to a blanket-approve mode. Load-time
    // untrusted-source stripping already guards committed rules; this closes the
    // runtime path where an in-session dangerous grant persists across an escalation.
    // Only runs on a genuine escalation INTO a permissive mode (not when leaving one,
    // and not on the `init` seed), so a steady-state mode keeps its rules untouched.
    if (reason !== 'init' &&
        isPermissivePermissionMode(next) && !isPermissivePermissionMode(previousMode)) {
      this.stripDangerousRulesOnEscalation(next);
    }
    if (this._modeChangeListener) {
      const change: PermissionModeChange = {
        previousMode,
        mode: next,
        previousExternalMode: toExternalPermissionMode(previousMode),
        externalMode: toExternalPermissionMode(next),
        reason,
      };
      try {
        this._modeChangeListener(change);
      } catch {
        /* a listener failure must never break a privilege transition */
      }
    }
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
      this.applyMode('ask', 'exitPlanMode');
    }
    return this.mode;
  }

  /**
   * Enable or disable the bypass-permissions guard.
   * When enabled, setMode() rejects 'bypassPermissions' and 'auto-allow'.
   *
   * `reason` is an optional human-readable explanation stored for the operator
   * (surfaced via {@link disableBypassReason} and, through
   * {@link autoModeUnavailableReason}, the Shift+Tab/auto-mode notification). It is
   * recorded only when disabling and cleared when re-enabling, so a stale reason
   * never lingers once the guard is lifted. Mirrors Claude's reason-differentiated
   * auto-mode-unavailable wording (settings vs circuit-breaker vs org-allowlist).
   */
  setDisableBypass(disable: boolean, reason?: string): void {
    this._disableBypass = disable;
    this._disableBypassReason = disable ? reason : undefined;
  }

  /**
   * Check whether bypass is disabled by managed policy.
   */
  isDisableBypass(): boolean {
    return this._disableBypass;
  }

  /**
   * The human-readable reason bypass/auto-allow is blocked, or `undefined` when
   * bypass is permitted (or was disabled without a reason). Lets `/permissions`
   * and the Shift+Tab message explain WHY auto-allow cannot be entered instead of
   * showing a bare `bypassDisabled` boolean the operator cannot act on. Parity:
   * Claude's `getAutoModeUnavailableNotification(reason)`.
   */
  disableBypassReason(): string | undefined {
    return this._disableBypassReason;
  }

  /**
   * Mark whether bypass-permissions mode was available when the session started
   * (Claude's `isBypassPermissionsModeAvailable`). The CLI sets this from a launch
   * flag; when true, a `plan`-mode run inherits bypass semantics in {@link check}.
   */
  setBypassModeAvailable(available: boolean): void {
    this._bypassModeAvailable = available;
  }

  /**
   * Whether bypass-permissions mode is available for this session (gates plan-mode
   * bypass inheritance). Mirrors Claude's ToolPermissionContext field.
   */
  isBypassModeAvailable(): boolean {
    return this._bypassModeAvailable;
  }

  /**
   * Get the current rule list.
   */
  getRules(): PermissionRule[] {
    return [...this.rules];
  }

  /**
   * Whether a rule can be edited/removed by the user. A rule loaded from managed/
   * enterprise policy ({@link isManagedPermissionSource}) is read-only; every other
   * source is user-editable. Lets a rule-management UI disable edit/delete and show
   * {@link ruleReadOnlyNotice} for locked rules. Parity: Claude's PermissionRuleList
   * policy-rule lockdown.
   */
  isRuleEditable(rule: PermissionRule): boolean {
    return !isManagedPermissionSource(rule.source);
  }

  /**
   * The read-only detail notice for a managed/policy rule, or `null` when the rule
   * is user-editable. Delegates to the pure {@link managedRuleReadOnlyNotice}.
   */
  ruleReadOnlyNotice(rule: PermissionRule): string | null {
    return managedRuleReadOnlyNotice(rule.source);
  }

  /**
   * Add a rule. It is prepended so that later-added rules take priority.
   * Under the managed-rules-only lockdown, a non-`policySettings` rule is refused
   * (a runtime session/CLI grant cannot bypass managed policy).
   */
  addRule(rule: PermissionRule): void {
    if (this._managedRulesOnly && !isManagedPermissionSource(rule.source)) {
      this.traceMutation(`Refused ${rule.action} rule (managed-rules-only lockdown): ${describeRule(rule)}`);
      return;
    }
    // Reject a bash ALLOW rule whose pattern bakes in a shell-DYNAMIC construct
    // ($(...), backtick, ${...}, $VAR, or a backslash path fragment): such a token
    // makes a persisted allow match MORE/different commands than the user saw, so it
    // must never become a reusable grant (parity: Claude's staticPrefix sanitization
    // of dynamic args before emitting a persistent prefix rule). Deny/ask rules are
    // unaffected — a dynamic token there only widens what is BLOCKED/PROMPTED, which
    // is not a privilege-escalation hazard.
    if (rule.action === 'allow' && normalizeToolName(rule.tool) === 'bash') {
      const dyn = dynamicAllowPatternToken(rule.pattern);
      if (dyn) {
        this.traceMutation(`Refused bash allow rule with a dynamic ${dyn} token (would over-match): ${describeRule(rule)}`);
        return;
      }
    }
    // Dedup: skip a rule already present under a normalized identity (so a legacy/
    // alias tool name — e.g. `Bash` — is recognized as the same rule as `bash`),
    // mirroring Claude's normalized-Set dedup in addPermissionRulesToSettings
    // (permissionsLoader.ts L244-L285). Prevents duplicate grants accumulating
    // across repeated "always allow" clicks or applyPermissionUpdate re-runs.
    const key = this.ruleDedupKey(rule);
    if (this.rules.some(r => this.ruleDedupKey(r) === key)) return;
    this.rules.unshift(rule);
    this.traceMutation(`Adding ${rule.action} rule: ${describeRule(rule)}`);
  }

  /**
   * Stable identity key for {@link addRule} dedup: the canonical rule-value string
   * (tool name normalized via {@link normalizeToolName} + paren-escaped pattern, so a
   * legacy alias matches its canonical form) plus action, source tier, and any
   * structured paramPattern. Two rules with the same key are duplicates.
   */
  private ruleDedupKey(rule: PermissionRule): string {
    const value = permissionRuleValueToString({ toolName: normalizeToolName(rule.tool), ruleContent: rule.pattern });
    const param = rule.paramPattern
      ? JSON.stringify(Object.keys(rule.paramPattern).sort().map(k => [k, rule.paramPattern![k]]))
      : '';
    return `${value}\0${rule.action}\0${rule.source ?? ''}\0${param}`;
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
    if (rt === '*' || rt === toolName) return true;
    if (rt.includes('*') && matchPattern(rt, toolName)) return true;
    // MCP server-level rule: a rule naming only the server (`mcp__server`) matches
    // EVERY tool from that server (`mcp__server__tool1`, `mcp__server__tool2`, …),
    // exactly like the explicit `mcp__server__*` form (already handled by the glob
    // branch above). A bare `mcp__server` carries no wildcard, so without this it
    // would match nothing. Mirrors Claude's toolMatchesRule MCP special-case
    // (utils/permissions/permissions.ts): rule-info toolName undefined/`*` + same
    // server ⇒ match.
    const ruleInfo = mcpInfoFromString(rt);
    const toolInfo = mcpInfoFromString(toolName);
    return (
      ruleInfo !== null &&
      toolInfo !== null &&
      (ruleInfo.toolName === undefined || ruleInfo.toolName === '*') &&
      ruleInfo.serverName === toolInfo.serverName
    );
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

  /**
   * Remove dangerous code-exec `allow` rules that originate from an UNTRUSTED,
   * shared/committable settings tier ({@link isSharedSettingSource} minus the
   * enterprise-managed `policySettings` tier, which is admin-owned and trusted) and
   * record them in {@link strippedDangerousRules}. A committed `.ami/permissions.json`
   * (or a slash-command's frontmatter) is attacker-PR-able, so a broad interpreter
   * grant it carries must never silently widen auto-approval — the check() code-exec
   * downgrade only moves such a rule to a prompt, whereas a hostile project rule
   * should not exist at all. Personal tiers (user/local/cliArg/session) and an
   * untagged rule are the user's own and are left intact. Returns the newly
   * stripped rules. Parity: Claude's dangerous-rule stripping into
   * `ToolPermissionContext.strippedDangerousRules`.
   */
  private stripDangerousRulesFromUntrustedSources(): PermissionRule[] {
    const stripped: PermissionRule[] = [];
    this.rules = this.rules.filter(r => {
      const untrustedShared = isSharedSettingSource(r.source) && !isManagedPermissionSource(r.source);
      if (untrustedShared && this.isDangerousCodeExecAllow(r)) {
        stripped.push(r);
        return false;
      }
      return true;
    });
    if (stripped.length > 0) {
      this.strippedDangerousRules.push(...stripped);
      for (const r of stripped) {
        this.traceMutation(`Stripped dangerous code-execution allow rule from an untrusted source: ${describeRule(r)}`);
      }
    }
    return stripped;
  }

  /**
   * Transition-time cleanup invoked by {@link applyMode} on escalation INTO a
   * permissive mode: drop every dangerous code-execution `allow` rule (regardless of
   * source tier) so a broad interpreter grant cannot compound with a blanket-approve
   * mode. Unlike {@link stripDangerousRulesFromUntrustedSources} (load-time, shared
   * untrusted tiers only), this fires at runtime on the actual escalation and also
   * covers an in-session grant. Stripped rules are recorded in
   * {@link strippedDangerousRules} for introspection and each is traced. Returns the
   * rules removed. Parity: Claude's `transitionPermissionMode` dangerous-permission
   * cleanup (getNextPermissionMode.ts L84-100).
   */
  private stripDangerousRulesOnEscalation(enteringMode: PermissionMode): PermissionRule[] {
    const stripped: PermissionRule[] = [];
    this.rules = this.rules.filter(r => {
      if (this.isDangerousCodeExecAllow(r)) {
        stripped.push(r);
        return false;
      }
      return true;
    });
    if (stripped.length > 0) {
      this.strippedDangerousRules.push(...stripped);
      for (const r of stripped) {
        this.traceMutation(`Stripped dangerous code-execution allow rule on escalation to '${enteringMode}': ${describeRule(r)}`);
      }
    }
    return stripped;
  }

  /**
   * Introspection: dangerous code-exec `allow` rules that were stripped from
   * untrusted shared settings tiers at load time ({@link stripDangerousRulesFromUntrustedSources}).
   * Lets a diagnostic / UI report WHICH committable rules were refused and why.
   */
  getStrippedDangerousRules(): PermissionRule[] {
    return [...this.strippedDangerousRules];
  }

  /**
   * Introspection: active `allow` rules that auto-approve EVERY bash command (a
   * pattern-less / `*` bash allow, or an all-tools `*` allow — the YOLO
   * equivalent). Lets a diagnostic / `/doctor` surface report WHICH rules silently
   * blanket-approve bash. Mirrors Claude's `findOverlyBroadBashPermissions`
   * (utils/permissions/permissionSetup.ts L378-412). Uses the single canonical
   * {@link isOverlyBroadBashAllowRule} detector (shared with cli config-resolver).
   */
  findOverlyBroadBashAllowRules(): PermissionRule[] {
    return this.rules.filter(r => isOverlyBroadBashAllowRule(r));
  }

  /**
   * Strip overly-broad bash `allow` rules that originate from an UNTRUSTED,
   * shared/committable settings tier ({@link isSharedSettingSource} minus the
   * admin-owned `policySettings`), recording them in
   * {@link strippedOverlyBroadBashRules}. A committed `.ami/permissions.json` (or a
   * slash-command's frontmatter) is attacker-PR-able, so a blanket `Bash` / `Bash(*)`
   * allow it carries must never silently grant the whole team YOLO bash auto-approval
   * — a hostile project rule of that breadth should not exist at all. Personal tiers
   * (user/local/cliArg/session) and an untagged rule are the user's own explicit
   * grant and are left intact. Returns the newly stripped rules. Parity: Claude's
   * `removeDangerousPermissions(findOverlyBroadBashPermissions(...))` re-run after a
   * settings sync (utils/settings/applySettingsChange.ts L50-59).
   */
  private stripOverlyBroadBashAllowRules(): PermissionRule[] {
    const stripped: PermissionRule[] = [];
    this.rules = this.rules.filter(r => {
      const untrustedShared = isSharedSettingSource(r.source) && !isManagedPermissionSource(r.source);
      if (untrustedShared && isOverlyBroadBashAllowRule(r)) {
        stripped.push(r);
        return false;
      }
      return true;
    });
    if (stripped.length > 0) {
      this.strippedOverlyBroadBashRules.push(...stripped);
      for (const r of stripped) {
        this.traceMutation(`Stripped overly-broad bash allow rule from an untrusted source: ${describeRule(r)}`);
      }
    }
    return stripped;
  }

  /**
   * Introspection: overly-broad bash allow rules stripped from untrusted shared
   * settings tiers at load / settings-sync time ({@link stripOverlyBroadBashAllowRules}).
   * Lets a diagnostic / UI report WHICH committable blanket-bash grants were refused.
   */
  getStrippedOverlyBroadBashRules(): PermissionRule[] {
    return [...this.strippedOverlyBroadBashRules];
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
        if (toolName === 'bash' && typeof input.command === 'string') {
          if (matchBashPattern(rule.pattern, input.command, 'deny')) return true;
        } else {
          // File-path tools: match the rule against the literal path AND every
          // symlink-resolved form so a deny cannot be bypassed via an in-workspace
          // symlink to a denied target.
          const ci = this.pathRuleCaseInsensitive(toolName);
          for (const form of this.fileRuleMatchStrings(toolName, input)) {
            if (matchPattern(rule.pattern, form, ci)) return true;
          }
        }
      } else {
        return true;
      }
    }
    return false;
  }

  /**
   * True when a FILE-PATH tool's deny pattern should be matched case-insensitively:
   * on a case-insensitive filesystem (macOS/Windows), `/x/.SSH/k` and `/x/.ssh/k`
   * name the SAME file, so a deny rule written in one casing must still block the
   * other — otherwise the deny is bypassable by a trivial case variation. No-op on a
   * case-sensitive FS (Linux) and for non-path tools (bash/grep are case-sensitive).
   * Mirrors Claude's `matchWildcardPattern(..., caseInsensitive)` usage.
   */
  private pathRuleCaseInsensitive(toolName: string): boolean {
    return FILE_PATH_RULE_TOOLS.has(normalizeToolName(toolName)) && isCaseInsensitiveFs();
  }

  /**
   * The path strings a FILE-PATH deny/ask RULE must be matched against: the raw
   * input path AND every symlink hop of its resolved real path
   * ({@link getPathsForPermissionCheck}). An in-workspace symlink pointing at a
   * denied target (`./link -> /etc/passwd`, `file_read(./link)`) must be caught by
   * a `file_read(/etc/**)` deny rule even though the literal argument is `./link`.
   * For a non-file tool (or when no file path is present) this is just the single
   * canonical input string, so bash/other rule matching is unchanged. Mirrors
   * Claude's getPathsForPermissionCheck threaded through deny/ask rule matching
   * (utils/permissions/filesystem.ts L1081-1101).
   */
  private fileRuleMatchStrings(toolName: string, input: Record<string, unknown>): string[] {
    const base = this.inputToString(toolName, input);
    if (!FILE_PATH_RULE_TOOLS.has(normalizeToolName(toolName))) return [base];
    const fp = input.file_path ?? input.path;
    if (typeof fp !== 'string' || !fp) return [base];
    const forms = new Set<string>([base]);
    try {
      for (const hop of getPathsForPermissionCheck(expandPath(fp, process.cwd()))) forms.add(hop);
    } catch { /* resolution failure (e.g. a non-existent write target) — literal only */ }
    return [...forms];
  }

  /**
   * ADDITIVE per-root match for a FILE-PATH rule whose pattern is written in a
   * ROOTED form (`//abs/**`, `~/.ssh/**`, `/src/**`) that AMI's raw-glob matcher
   * cannot resolve against an absolute candidate path. Resolves the pattern against
   * its root via {@link matchRootedFileRulePattern} and matches it against the
   * ABSOLUTE candidate path (and every symlink hop, so a rooted deny/ask/allow rule
   * cannot be bypassed via an in-workspace symlink). Returns `false` for bash/non-file
   * tools, content-less rules, or root-agnostic patterns (the existing raw matcher
   * already handles those unchanged). Mirrors Claude's `matchingRuleForInput`
   * (utils/permissions/filesystem.ts L955) per-root resolution step.
   */
  private matchFilePathRuleWithRoot(
    rule: Pick<PermissionRule, 'pattern'>,
    toolName: string,
    input: Record<string, unknown>,
  ): boolean {
    if (!rule.pattern) return false;
    if (!FILE_PATH_RULE_TOOLS.has(normalizeToolName(toolName))) return false;
    const fp = input.file_path ?? input.path;
    if (typeof fp !== 'string' || !fp) return false;
    const ci = this.pathRuleCaseInsensitive(toolName);
    const abs = path.resolve(process.cwd(), expandPath(fp, process.cwd()));
    const forms = new Set<string>([abs]);
    try {
      for (const hop of getPathsForPermissionCheck(abs)) forms.add(hop);
    } catch { /* resolution failure — literal absolute form only */ }
    for (const form of forms) {
      if (matchRootedFileRulePattern(rule.pattern, form, { caseInsensitive: ci }) === true) return true;
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
        if (toolName === 'bash' && typeof input.command === 'string') {
          if (matchBashPattern(rule.pattern, input.command, 'ask')) return true;
        } else {
          // File-path tools: an `ask` rule stays bypass-immune for the literal path
          // AND its symlink-resolved forms (parity with the deny matching above).
          for (const form of this.fileRuleMatchStrings(toolName, input)) {
            if (matchPattern(rule.pattern, form)) return true;
          }
        }
      } else {
        return true;
      }
    }
    return false;
  }

  /**
   * The human label of the ALLOW rule that auto-approved a tool call, or `undefined`
   * when no explicit allow rule grants it (the allow came from a mode / safety /
   * classifier fast-path, not a named rule). Scans the rule set with the SAME
   * first-match, deny-skipping semantics as {@link check}'s rule loop: a matching
   * `deny` rule is skipped (it never produces an allow), and if the FIRST matching
   * non-deny rule is an `ask` rule the result is `undefined` (that call would have
   * prompted, not auto-allowed). Intended to be called AFTER {@link check} /
   * {@link checkWithReason} already returned `allow`, so the executor can attribute the
   * silent approval to the specific standing rule that caused it — the provenance surface
   * Claude Code shows as `Auto-approved · matched "<rule>"` (classifierMatchedRule /
   * setClassifierApproval). The label uses the canonical `Tool(pattern)` permission-rule
   * syntax (or the bare tool name for a tool-wide rule). Pure read — never prompts or
   * mutates state.
   */
  matchedAllowRule(toolName: string, input: Record<string, unknown>): string | undefined {
    for (const rule of this.rules) {
      if (rule.action === 'deny') continue;
      if (!this.toolNameMatches(rule.tool, toolName)) continue;
      if (rule.paramPattern && !this.paramMatches(rule.paramPattern, input)) continue;
      const matched = rule.pattern
        ? (toolName === 'bash' && typeof input.command === 'string'
            ? matchBashPattern(rule.pattern, input.command, rule.action)
            : matchPattern(rule.pattern, this.inputToString(toolName, input)))
        : true;
      if (!matched) continue;
      // First matching non-deny rule decides (check() first-match). Only an `allow`
      // rule is auto-approval provenance; a matching `ask` rule would have prompted.
      if (rule.action !== 'allow') return undefined;
      return rule.pattern ? `${rule.tool}(${rule.pattern})` : rule.tool;
    }
    return undefined;
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
    const removed = before - this.rules.length;
    // Trace only when a rule was actually dropped, mirroring Claude's "Removing N
    // rule(s)" trace.
    if (removed > 0) this.traceMutation(`Removing ${removed} rule(s)`);
    return removed;
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
    // Validate at the trust boundary: updates reach here from untrusted surfaces
    // (a permission-prompt tool's JSON reply, a hook's `updatedPermissions`) where
    // the compile-time union does not hold at runtime. Drop a malformed update
    // fail-closed (grant nothing) rather than partially applying it or letting an
    // unknown `type` fall through the switch and return `undefined` (a mid-turn
    // crash in {@link applyUpdates}).
    const validation = validatePermissionUpdate(update);
    if (!validation.valid) {
      this.traceMutation(`Rejected invalid permission update: ${validation.error}`);
      return { applied: 0 };
    }
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
      // Only a SPECIFIC (content-bearing) allow rule can be "shadowed": e.g.
      // `bash(ls *)` sitting under a tool-wide `bash` deny/ask is genuinely
      // unreachable. A TOOL-WIDE allow (no pattern, or `*`) merely CONFLICTS with
      // a tool-wide deny/ask — it is not reported as shadowed, else every bare
      // `{tool, action:'allow'}` under a tool-wide deny is a false positive.
      // Mirrors Claude's shadowedRuleDetection (ruleContent === undefined ⇒ not
      // shadowed, shadowedRuleDetection.ts L118-122 / L166-170).
      const allowSpecific = !!allow.pattern && allow.pattern !== '*';
      if (!allowSpecific) continue;
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

      // Preserve any unrecognized top-level keys (hooks, env, or a future AMI field)
      // so a round-trip through saveRules doesn't silently destroy settings this
      // loader doesn't model. Mirrors Claude's `...settingsData` spread-on-write.
      if (data && typeof data === 'object' && !Array.isArray(data)) {
        for (const k of Object.keys(data)) {
          if (k !== 'mode' && k !== 'rules' && k !== 'additionalDirectories') {
            this._extraPersistedKeys[k] = (data as Record<string, unknown>)[k];
          }
        }
      }

      // Load persisted additional read directories (parity: Claude's
      // `permissions.additionalDirectories`) as real read-root grants tagged with
      // the project tier, so a committed `.ami/permissions.json` that lists extra
      // directories actually grants the agent read access to them on launch (and
      // round-trips back through saveRules). Non-string / empty entries are skipped;
      // registerAllowedRoot resolves + de-dupes. Previously this key was merely
      // round-tripped as an opaque extra key and never honored.
      if (Array.isArray((data as Record<string, unknown>).additionalDirectories)) {
        for (const dir of (data as Record<string, unknown>).additionalDirectories as unknown[]) {
          if (typeof dir === 'string' && dir !== '') this.registerAllowedRoot(dir, 'projectSettings');
          else warnings.push('An invalid permissions.additionalDirectories entry was ignored');
        }
      }

      if (data.mode && typeof data.mode === 'string') {
        if ((PERMISSION_MODES as readonly string[]).includes(data.mode)) {
          this.applyMode(data.mode as PermissionMode, 'deserialize');
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
        // Defense-in-depth: a committed/untrusted project rule cannot smuggle a
        // broad interpreter grant (`Bash(python:*)`) that would widen auto-approval.
        for (const r of this.stripDangerousRulesFromUntrustedSources()) {
          warnings.push(`Stripped dangerous code-execution allow rule from untrusted project settings: ${describeRule(r)}`);
        }
        // Defense-in-depth: a committed/untrusted project rule cannot smuggle a
        // blanket `Bash` / `Bash(*)` allow that would auto-approve EVERY bash
        // command (YOLO-equivalent) for the whole team.
        for (const r of this.stripOverlyBroadBashAllowRules()) {
          warnings.push(`Stripped overly-broad bash allow rule from untrusted project settings: ${describeRule(r)}`);
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
        return { warning: `Invalid permission rule "${canonical}" was skipped: ${formatPermissionRuleError(verdict)}` };
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
    // Re-screen the merged tiers for dangerous breadth so a disk edit (a committed
    // project file, a settings-change re-sync via syncRulesFromDisk) cannot silently
    // (re-)grant a blanket `Bash` / `Bash(*)` allow into the live context. Mirrors
    // Claude's applySettingsChange re-running removeDangerousPermissions after every
    // disk sync (applySettingsChange.ts L50-59).
    for (const r of this.stripOverlyBroadBashAllowRules()) {
      warnings.push(`Stripped overly-broad bash allow rule from untrusted settings: ${describeRule(r)}`);
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
      this.traceMutation(`Persisting ${rules.length} rule(s) to destination '${destination}': ${filePath}`);
      // The project-LOCAL destination is advertised to the user as "gitignored —
      // not shared with collaborators" (permission-save-destinations.ts). Make good
      // on that promise by actually adding the local file to the global gitignore,
      // exactly as Claude Code does on a `localSettings` write (settings.ts L508-514).
      // Best-effort + swallowed inside the helper: a gitignore write must never turn
      // a successful rule-persist into a failure.
      if (destination === 'localSettings') {
        const added = addFileGlobRuleToGitignore(path.basename(filePath), cwd);
        if (added) this.traceMutation(`Added **/${path.basename(filePath)} to the global gitignore for the project-local permissions file`);
      }
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
      // Spread preserved unknown keys FIRST so the canonical mode/rules always win,
      // while a user's (or a future version's) extra keys survive the round-trip.
      // Persist the source-tracked additional read directories (parity: Claude's
      // `permissions.additionalDirectories`): only the PERSISTABLE tiers are written
      // — an ephemeral `session`/`cliArg` grant (`/add-dir`, `--context-file`) stays
      // in-memory and is dropped on exit, exactly as before. De-duped + order-stable.
      const additionalDirectories = this.persistableReadRoots();
      const data: Record<string, unknown> = { ...this._extraPersistedKeys, mode: this.mode, rules: this.rules };
      if (additionalDirectories.length > 0) data.additionalDirectories = additionalDirectories;
      else delete data.additionalDirectories;
      // Crash-safe, owner-only persist (temp + fsync + atomic rename, mode 0o600):
      // a half-written permissions.json would be silently dropped by loadRules'
      // catch, wiping every "don't ask again" rule, and the file is not left
      // world-readable. Uses the ONE canonical atomic writer (utils/fs-atomic),
      // which also creates `.ami/` 0o700 — no separate mkdir needed.
      atomicWriteFileSync(filePath, JSON.stringify(data, null, 2) + '\n', { mode: OWNER_ONLY_FILE_MODE });
      this.traceMutation(`Persisting ${this.rules.length} rule(s) + mode '${this.mode}' to ${filePath}`);
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
