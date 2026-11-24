import * as fs from 'fs';
import 'crypto';
import { resolveConfigDir } from './reference-journal';
import './utils/string';
import * as path from 'path';
import * as os from 'os';
import { spawn as nodeSpawn } from 'child_process';
import type { ChildProcess, SpawnOptions } from 'child_process';

/** Injectable spawn seam for command hooks (tests force stdin destroy/write failures). */
export const hookSpawnHooks: {
  spawn: (
    command: string,
    args: readonly string[],
    options: SpawnOptions,
  ) => ChildProcess;
} = { spawn: nodeSpawn as typeof hookSpawnHooks.spawn };

/**
 * Injectable plugin-discovery seam for loadFromFile / reloadPluginHooks.
 * Production defaults to `require('./plugins').discoverPlugins`; tests inject a
 * throwing discoverer to force the categorized load-failure catch.
 */
export const hookPluginDiscoverHooks: {
  discoverPlugins: (dir: string) => Array<{ name: string; root: string; hooksFile?: string }>;
} = {
  discoverPlugins: (dir: string) => {
    const { discoverPlugins } = require('./plugins');
    return discoverPlugins(dir);
  },
};

/**
 * Injectable fs seam for plugin-affecting snapshot mtimes (and related best-
 * effort stats). Production uses real `fs.statSync`; tests inject a throwing
 * stat to hit the unreadable→mtime 0 catch.
 */
export const hookFsHooks: {
  statSync: (path: string) => { mtimeMs: number };
} = { statSync: fs.statSync as typeof hookFsHooks.statSync };
import type { Message } from './types';
import { log as coreLog } from './logger';
import { EventEmitter } from 'events';
import {
  canHookEvent,
  shouldSkipHookDueToTrust,
  canRunHookUnderManagedOnly,
  filterManagedOnlyHooks,
  describeProtectedEventBlock,
  captureHookSnapshot,
  detectHiddenHookAdditions,
  type HookSourceKind,
  type HookTier,
} from './hook-protection';
import { isRestrictedToPluginOnly, isSourceAdminTrusted } from './plugin-only-policy';
import { readPluginOptionValues, substituteUserConfigValues, buildPluginOptionEnv } from './plugins';
import { stopFailureErrorClass, STOP_FAILURE_ERROR_CLASSES, categorizeLoadFailure } from './error-classifier';
import { stripBom, resolveWorkspaceTrustAccepted } from './config';
import { clearCwdEnvFiles, invalidateSessionEnvCache, sessionEnvDirPath, hookEnvFragmentPath } from './session-environment';
import { verdictRank, wildcardMatch, PERMISSION_TOOL_ALIASES } from './permissions';
import type { PermissionUpdate } from './permissions';
import { validateUrlSafety, pinnedDnsLookup, SSRF_LOOPBACK_AFFORDANCE } from './tools/web-utils';
import './tools/bash-security';
import { getSmallFastModel } from './model-capabilities';
import './privacy-level';
import { killTree } from './utils/shell';
import {
  createVerificationOutputTool,
  parseVerificationToolInput,
  VERIFICATION_OUTPUT_TOOL_NAME,
} from './tools/synthetic-output';
import { memoizeWithLRU } from './utils/memoize';
import { HOOK_EVENTS, SAMPLING_ORIGINS, SESSION_END_REASONS, SESSION_START_SOURCES, SETUP_TRIGGERS, hasPendingToolCalls, hookJournalHooks, isSessionEndReason, isSessionStartSource, isSetupTrigger, sanitizeHookEnv, sanitizeHookOutput } from './hooks-session-context';
import type { CompactContext, HookContext, HookDecision, HookFn, PostToolUseContext, PostToolUseHookFn, PostToolUseHookResult, PreSamplingContext, PreSamplingHookFn, PreToolUseContext, PreToolUseHookFn, SessionContext, SubagentContext } from './hooks-session-context';
import { formatHookStoppedContinuation, isHookDecisionClassification } from './hooks-stop-hook-summary';
import type { ConfigChangeContext, ConfigChangeHookFn, CwdChangedContext, CwdChangedHookFn, ElicitationContext, ElicitationHookFn, ElicitationHookResult, FileChangeContext, FileChangeHookFn, InstructionsContext, InstructionsHookFn, NotificationContext, NotificationHookFn, PermissionContext, PermissionHookFn, PermissionHookResult, PostCompactHookFn, PreCompactHookFn, SessionHookFn, StopFailureContext, StopFailureHookFn, StopHookDisplay, StopHookFn, StopHookInfo, StopHookResult, StopHookSummary, SubagentHookFn, SubagentStartHookFn, TaskCompletedHookFn, TaskEventContext, TaskEventHookFn, TeammateContext, TeammateIdleHookFn, UserPromptContext, UserPromptHookFn, UserPromptSubmitHookResult, WorktreeContext, WorktreeCreateHookFn, WorktreeHookFn } from './hooks-stop-hook-summary';
import { DEFAULT_ASYNC_HOOK_TIMEOUT_MS, DEFAULT_CALLBACK_HOOK_TIMEOUT_MS, DEFAULT_HOOK_SHELL, DEFAULT_HOOK_TIMEOUT_MS, DEVICE_WRITE_RE, ERR_HTTP_HOOK_BLOCKED_ADDRESS, EventConcurrencyLimiter, HOOK_AGENT_DISALLOWED_TOOLS, MAX_AGENT_TURNS, MAX_HOOK_COMMAND_LENGTH, SYSTEM_CHMOD_RE, boundedHookAppend, combineHookAbort, formatShellPrefixCommand, formatStopHookSummaryMessage, getSessionEndHookTimeoutMs, isHttpHookUrlAllowed, raceAbort, resolveHookCwd, resolveHookTimeoutMs, runWithTimeout, truncateHookOutput, validateHookTimeout } from './hooks-event-concurrency-limiter';
import type { HookConfigError, ResolveHookShellOpts } from './hooks-event-concurrency-limiter';
import { buildToolUseSummary, extractHookBlockingError, formatHookDenyReason, hookEventNameMismatch, interpolateHookHeaderEnv, replHookContextFields, resolvePluginDataDir, resolvePluginId, serializeHookEvent, substitutePluginData, substitutePluginRoot, unknownHookDecisionError, validateHookDecisionOutput } from './hooks-validate-hook-decision-output';
import { HOOK_MATCHER_FIELD, asyncRewakeDeliveryMessage, buildAgentHookSystemPrompt, buildStructuredOutputEnforcementNudge, extractAdditionalContexts, extractSessionAdditionalContexts, extractSystemMessages, formatHookMatcherLabel, hookEventMetadataCacheKey, isHookKillSwitchEnv, mergeHookInstructions, parseElicitationResult, parseHookVerification, parseWatchPaths, sortMatchersByPriority, substituteHookArguments, tryParseHookJson } from './hooks-async-rewake-delivery-message';
import type { HookEventMetadata, HookEventMetadataOpts } from './hooks-async-rewake-delivery-message';
import { MAX_PENDING_HOOK_EVENTS, classifyHookOutcome, computeHookId, extractWorktreePath, isHookEqual, shouldEmitHookLifecycle, unwrapHookSpecificOutput } from './hooks-command-hook-config';
import type { AgentHookConfig, CommandHookConfig, FunctionHook, FunctionHookCallback, HookCallback, HookDecisionOutput, HookLifecycleEvent, HookLifecycleOutcome, HttpHookConfig, HttpHookPolicy, PromptHookConfig, RuntimeHookMeta } from './hooks-command-hook-config';
import { HOOK_MATCHER_EXTRA_ALIASES, describeHookContent, firstHookJsonLine, getHookDisplayText, hookContentLabel, hookSourceDescription, hookSourceDisplayString, hookSourceHeader, hookSourceInline, startHookProgressInterval, stopHookDisplayFromConfig } from './hooks-with-frontmatter-hook-plugin-root';
import type { EngineFactory, FrontmatterHookOptions, HookAuditRow, HookConfigEntry, HookConfigFile, HookDescriptor, HookEventDescription, HookEventMatcherGroup, HookProgressEvent } from './hooks-with-frontmatter-hook-plugin-root';
import { TOOL_IF_RE, buildPromptResponse, createPromptLineScanner, hookInputContent, matcherAppliesToCtx, matchesPattern, serializePromptResponse } from './hooks-create-prompt-line-scanner';
import type { HookOutcome, PromptRequestHandler } from './hooks-create-prompt-line-scanner';
export { HOOK_CONFIG_TYPES, HOOK_EVENTS, SAMPLING_ORIGINS, SENSITIVE_ENV_PATTERNS, SESSION_END_REASONS, SESSION_START_SOURCES, SETUP_TRIGGERS, hasPendingToolCalls, hookJournalHooks, isSamplingOrigin, isSessionEndReason, isSessionStartSource, isSetupTrigger, sanitizeHookEnv, sanitizeHookOutput } from './hooks-session-context';
export type { CompactContext, HookConfigType, HookContext, HookDecision, HookEvent, HookFn, PostToolUseContext, PostToolUseHookFn, PostToolUseHookResult, PreSamplingContext, PreSamplingHookFn, PreToolUseContext, PreToolUseHookFn, SamplingOrigin, SessionContext, SessionEndReason, SessionStartSource, SetupTrigger, SubagentContext } from './hooks-session-context';
export { DEFAULT_STOP_HOOK_PREVENT_REASON, ELICITATION_ACTIONS, HOOK_DECISION_CLASSIFICATIONS, formatHookStoppedContinuation, formatStopHookBlockingMessage, formatUserPromptStoppedMessage, formatUserPromptSubmitBlockedMessage, isHookDecisionClassification } from './hooks-stop-hook-summary';
export type { ConfigChangeContext, ConfigChangeHookFn, CwdChangedContext, CwdChangedHookFn, ElicitationAction, ElicitationContext, ElicitationHookFn, ElicitationHookResult, FileChangeContext, FileChangeHookFn, HookDecisionClassification, InstructionsContext, InstructionsHookFn, NotificationContext, NotificationHookFn, PermissionContext, PermissionHookFn, PermissionHookResult, PostCompactHookFn, PreCompactHookFn, SessionHookFn, StopFailureContext, StopFailureHookFn, StopHookDisplay, StopHookFn, StopHookInfo, StopHookResult, StopHookSummary, SubagentHookFn, SubagentStartHookFn, TaskCompletedHookFn, TaskEventContext, TaskEventHookFn, TeammateContext, TeammateIdleHookFn, UserPromptContext, UserPromptHookFn, UserPromptSubmitHookResult, WorktreeContext, WorktreeCreateHookFn, WorktreeHookFn } from './hooks-stop-hook-summary';
export { DEFAULT_ASYNC_HOOK_TIMEOUT_MS, DEFAULT_CALLBACK_HOOK_TIMEOUT_MS, DEFAULT_HOOK_SHELL, DEFAULT_HOOK_TIMEOUT_MS, DEFAULT_SESSIONEND_HOOK_TIMEOUT_MS, ERR_HTTP_HOOK_BLOCKED_ADDRESS, EventConcurrencyLimiter, HOOK_AGENT_DISALLOWED_TOOLS, HOOK_TIMING_DISPLAY_THRESHOLD_MS, MAX_AGENT_TURNS, MAX_CONCURRENT_PER_EVENT, MAX_HOOKS_PER_EVENT, MAX_HOOK_OUTPUT_LENGTH, MAX_HOOK_TIMEOUT_MS, boundedHookAppend, combineHookAbort, formatShellPrefixCommand, formatStopHookSummaryMessage, getSessionEndHookTimeoutMs, isHttpHookUrlAllowed, posixSingleQuote, raceAbort, resolveHookCwd, resolveHookTimeoutMs, runWithTimeout, sanitizeHeaderValue, truncateHookOutput, validateHookTimeout } from './hooks-event-concurrency-limiter';
export type { HookConfigError, ResolveHookShellOpts } from './hooks-event-concurrency-limiter';
export { buildToolUseSummary, extractHookBlockingError, formatHookDenyReason, hookEventNameMismatch, interpolateHookHeaderEnv, resolvePluginDataDir, resolvePluginId, serializeHookEvent, substitutePluginData, substitutePluginRoot, unknownHookDecisionError, validateHookDecisionOutput } from './hooks-validate-hook-decision-output';
export { HOOK_MATCHER_FIELD, HOOK_SOURCE_PRIORITY, asyncRewakeDeliveryMessage, buildAgentHookSystemPrompt, buildStructuredOutputEnforcementNudge, extractAdditionalContexts, extractSessionAdditionalContexts, extractSystemMessages, formatHookMatcherLabel, isHookKillSwitchEnv, mergeHookInstructions, parseElicitationResult, parseHookVerification, parseWatchPaths, sortMatchersByPriority, substituteHookArguments, tryParseHookJson } from './hooks-async-rewake-delivery-message';
export type { HookEventMetadata, HookEventMetadataOpts } from './hooks-async-rewake-delivery-message';
export { ALWAYS_EMITTED_HOOK_EVENTS, HOOK_LIFECYCLE_OUTCOMES, HOOK_PROGRESS_INTERVAL_MS, MAX_PENDING_HOOK_EVENTS, classifyHookOutcome, computeHookId, extractWorktreePath, isHookEqual, shouldEmitHookLifecycle, unwrapHookSpecificOutput } from './hooks-command-hook-config';
export type { AgentHookConfig, CommandHookConfig, FunctionHook, FunctionHookCallback, HookCallback, HookDecisionOutput, HookLifecycleEvent, HookLifecycleOutcome, HttpHookConfig, HttpHookPolicy, PromptHookConfig, RuntimeHookMeta } from './hooks-command-hook-config';
export { firstHookJsonLine, getHookDisplayText, hookContentLabel, hookSourceDescription, hookSourceDisplayString, hookSourceHeader, hookSourceInline, startHookProgressInterval, withFrontmatterHookPluginRoot } from './hooks-with-frontmatter-hook-plugin-root';
export type { EngineFactory, FrontmatterHookOptions, HookAuditRow, HookConfigEntry, HookConfigFile, HookDescriptor, HookEventDescription, HookEventMatcherGroup, HookProgressEvent, PromptRequestOption } from './hooks-with-frontmatter-hook-plugin-root';
export { buildPromptResponse, createPromptLineScanner, isValidPromptSelection, matcherAppliesToCtx, parsePromptRequestLine, serializePromptResponse, validatePromptRequest } from './hooks-create-prompt-line-scanner';
export type { HookOutcome, PromptLineScanner, PromptRequest, PromptRequestHandler, PromptResponse } from './hooks-create-prompt-line-scanner';
/** Default HTTP-hook deadline when the hook sets no `timeout` — 10 minutes,
 *  matching the reference's DEFAULT_HTTP_HOOK_TIMEOUT_MS (= TOOL_HOOK_EXECUTION_
 *  TIMEOUT_MS). A webhook round-trip may legitimately be slow; a too-short
 *  default (previously 10s) spuriously failed/denied slow hooks. */
export const DEFAULT_HTTP_HOOK_TIMEOUT_MS = 10 * 60 * 1000;

/**
 * Hard ceiling on how many bytes of a command hook's stdout / stderr are held in
 * memory while it runs (the reference's TaskOutput pipe-mode `#maxMemory`, 8 MiB).
 * A hook's decision JSON is parsed from the HEAD of stdout, so once this cap is
 * reached further output is dropped (head-preserving) rather than growing the
 * buffer unbounded — a runaway hook that floods stdout can no longer OOM the
 * agent. The low-order boundary is kept so the decision at the head always
 * survives.
 */
export const MAX_HOOK_COMMAND_OUTPUT_BYTES = 8 * 1024 * 1024;

/** Terminal marker appended to a command hook's stream when it overflowed the
 *  in-memory cap, mirroring TaskOutput's "Output truncated" notice. */
export function hookOutputTruncationNotice(stream: 'stdout' | 'stderr'): string {
  return `\n[hook ${stream} truncated - exceeded ${MAX_HOOK_COMMAND_OUTPUT_BYTES} bytes]`;
}

export const BLOCKED_HOOK_COMMANDS: ReadonlySet<string> = new Set([
  'rm -rf /',
  'mkfs',
  'dd if=',
  ':(){',
  'fork bomb',
  'shutdown',
  'reboot',
  'init 0',
  'init 6',
  'halt',
]);

export function validateHookCommand(command: string): { valid: boolean; reason?: string } {
  if (!command || !command.trim()) {
    return { valid: false, reason: 'Hook command must not be empty' };
  }
  if (command.length > MAX_HOOK_COMMAND_LENGTH) {
    return { valid: false, reason: `Hook command exceeds maximum length of ${MAX_HOOK_COMMAND_LENGTH} characters` };
  }
  for (const blocked of BLOCKED_HOOK_COMMANDS) {
    if (command.includes(blocked)) {
      return { valid: false, reason: `Hook command contains blocked pattern: ${blocked}` };
    }
  }
  if (DEVICE_WRITE_RE.test(command)) {
    return { valid: false, reason: 'Hook command must not write to device files' };
  }
  if (SYSTEM_CHMOD_RE.test(command)) {
    return { valid: false, reason: 'Hook command must not chmod 777 system directories' };
  }
  return { valid: true };
}

/** Interpreter selectors a command hook (or the default-shell override) may name. */
const HOOK_SHELL_SELECTORS: ReadonlySet<string> = new Set(['bash', 'sh', 'powershell', 'pwsh']);

/**
 * Resolve the DEFAULT command-hook interpreter when a hook sets no `shell`.
 * Honors the `SUPERINFERENCE_HOOK_SHELL` env override (bash | sh | powershell |
 * pwsh) so a deployment can change the default interpreter (the reference's
 * configurable DEFAULT_HOOK_SHELL) — e.g. a Windows host that wants `pwsh` by
 * default. An unset/unrecognized value falls back to DEFAULT_HOOK_SHELL.
 */
export function resolveDefaultHookShell(env: Record<string, string | undefined> = process.env): string {
  const raw = (env.SUPERINFERENCE_HOOK_SHELL ?? '').trim().toLowerCase();
  return HOOK_SHELL_SELECTORS.has(raw) ? raw : DEFAULT_HOOK_SHELL;
}

/**
 * Resolve a command hook's `shell` selector to a concrete spawn descriptor
 * (the reference's per-hook shell selection: `hook.shell ?? DEFAULT_HOOK_SHELL`).
 * PowerShell hooks spawn `pwsh -NoProfile -NonInteractive -Command <command>`
 * (non-interactive, no profile — matching the reference), every POSIX shell
 * uses `<shell> -c <command>`. Single source of truth so command execution
 * never hard-codes `bash`.
 *
 * Two the reference command-construction behaviors are applied for POSIX shells:
 *  - Windows `.sh` fallback: on win32 a bare `.sh` script won't execute without
 *    an explicit interpreter, so `bash ` is auto-prepended.
 *  - `CLAUDE_CODE_SHELL_PREFIX`: when set, the command is wrapped by the prefix
 *    (POSIX-quoted). PowerShell ignores the prefix (its quoting differs), matching
 *    the reference.
 */
export function resolveHookShell(
  command: string,
  shell: string | undefined,
  opts: ResolveHookShellOpts = {},
): { file: string; args: string[] } {
  const env = opts.env ?? process.env;
  const platform = opts.platform ?? process.platform;
  const sel = (shell ?? resolveDefaultHookShell(env)).toLowerCase();
  if (sel === 'powershell' || sel === 'pwsh') {
    return { file: 'pwsh', args: ['-NoProfile', '-NonInteractive', '-Command', command] };
  }
  const file = sel === 'sh' ? 'sh' : 'bash';
  let finalCommand = command;
  // Windows (bash) fallback: auto-prepend `bash ` for a .sh script so it
  // executes instead of opening in the default file handler (the reference).
  if (platform === 'win32' && /\.sh(\s|$|")/.test(command.trim()) && !command.trim().startsWith('bash ')) {
    finalCommand = `bash ${command}`;
  }
  // CLAUDE_CODE_SHELL_PREFIX wraps the command via POSIX quoting (not applied to
  // PowerShell above — its quoting model differs; matches the reference).
  const prefix = env.CLAUDE_CODE_SHELL_PREFIX;
  if (prefix) finalCommand = formatShellPrefixCommand(prefix, finalCommand);
  return { file, args: ['-c', finalCommand] };
}

/**
 * Build the user-facing message for an SSRF-blocked HTTP hook, mirroring the reference
 * Code's `ssrfError` wording (names the blocked host, explains loopback is
 * allowed) and tagging it with the stable {@link ERR_HTTP_HOOK_BLOCKED_ADDRESS}
 * code. `reason` is the detail from the URL-safety validator (which already names
 * the resolved private IP). The host is extracted from the URL so the message
 * names the offending target even when the validator reason is generic.
 */
export function formatHttpHookBlockedMessage(url: string, reason: string): string {
  let host = url;
  try { host = new URL(url).hostname || url; } catch { /* non-parseable URL: keep raw */ }
  return `HTTP hook blocked: ${host} — ${reason}. ${SSRF_LOOPBACK_AFFORDANCE} [${ERR_HTTP_HOOK_BLOCKED_ADDRESS}]`;
}

/**
 * Expected command-hook JSON output shape, surfaced as an affordance when a
 * hook emits malformed JSON (the reference's parseHookOutput error hint).
 */
export const HOOK_JSON_SCHEMA_HINT = JSON.stringify({
  continue: 'boolean (optional)',
  suppressOutput: 'boolean (optional)',
  stopReason: 'string (optional)',
  decision: '"approve" | "block" (optional)',
  reason: 'string (optional)',
  systemMessage: 'string (optional)',
  permissionDecision: '"allow" | "deny" | "ask" (optional)',
  additionalContext: 'string (optional)',
  updatedInput: 'object (optional)',
  updatedPermissions: 'array (optional)',
  // Abort the whole turn on a deny rather than skipping this one tool call.
  interrupt: 'boolean (optional)',
  // async OUTPUT response (the reference's { async: true, asyncTimeout }): a hook
  // may self-declare that it should keep running in the background, bounded by its
  // own asyncTimeout (milliseconds) instead of the synchronous hook deadline.
  async: 'boolean (optional)',
  asyncTimeout: 'number (optional, milliseconds — only with async:true)',
  hookSpecificOutput: 'object (optional)',
}, null, 2);

/**
 * Build a user-facing validation message for a command hook that emitted text
 * starting with `{` (i.e. intended to be JSON) which failed to parse. Mirrors
 * the reference's parseHookOutput `validationError` + schema affordance so a
 * malformed hook output is reported to the user instead of being silently
 * swallowed / treated as opaque plain text.
 */
export function formatHookValidationError(raw: string, err: unknown): string {
  const msg = err instanceof Error ? err.message : String(err);
  return `Hook JSON output validation failed: ${msg}\n\nThe hook's output was: ${raw}\n\nExpected schema:\n${HOOK_JSON_SCHEMA_HINT}`;
}

/**
 * System-prompt contract appended to prompt/agent (AI-driven) hooks so the model
 * knows exactly what to return: a single JSON object {"ok": boolean, "reason"?:
 * string}. Mirrors the reference's hookResponseSchema / createStructuredOutputTool.
 */
export const HOOK_VERIFICATION_CONTRACT =
  'You are a verification hook. Evaluate the condition described below against the ' +
  'event data. Respond with EXACTLY one JSON object and nothing else: ' +
  '{"ok": true} if the condition is met, or {"ok": false, "reason": "<why>"} if it is not.';

/** Static enumerations of allowed matcher values, keyed by event (the reference's
 *  per-event MatcherMetadata.values). Events whose valid set is dynamic (tool/
 *  agent/MCP scoped) are resolved from {@link HookEventMetadataOpts} instead. */
const HOOK_MATCHER_VALUES: Record<string, readonly string[]> = {
  sessionStart: SESSION_START_SOURCES,
  sessionEnd: SESSION_END_REASONS,
  setup: SETUP_TRIGGERS,
  preCompact: ['manual', 'auto'],
  postCompact: ['manual', 'auto'],
  preSampling: SAMPLING_ORIGINS,
  postSampling: SAMPLING_ORIGINS,
  // Single source: the StopFailure taxonomy lives in error-classifier.ts, where
  // stopFailureErrorClass() maps onto it, so the advertised matcher values and the
  // emitted `error` class can never drift.
  stopFailure: STOP_FAILURE_ERROR_CLASSES,
  configChange: ['user_settings', 'project_settings', 'local_settings', 'policy_settings', 'skills'],
  instructionsLoaded: ['session_start', 'nested_traversal', 'path_glob_match', 'include', 'compact'],
  notification: [
    'permission_prompt', 'idle_prompt', 'auth_success',
    'elicitation_dialog', 'elicitation_complete', 'elicitation_response',
  ],
};

/** Resolve the enumerated matcher values for an event given its matcher field:
 *  dynamic sets come from `opts`, otherwise the static {@link HOOK_MATCHER_VALUES}
 *  enumeration (or `[]` when the valid set is open-ended / unknown). */
function resolveMatcherValues(event: string, matcherField: string, opts: HookEventMetadataOpts): string[] {
  if (matcherField === 'toolName') return opts.toolNames ? [...opts.toolNames] : [];
  if (matcherField === 'mode') return opts.agentTypes ? [...opts.agentTypes] : [];
  if (matcherField === 'mcpServerName') return opts.mcpServerNames ? [...opts.mcpServerNames] : [];
  const stat = HOOK_MATCHER_VALUES[event];
  return stat ? [...stat] : [];
}

/** Whether a `matcher` is meaningful for this event (the reference's
 *  eventSupportsMatcher): true when the event has a matchable field (tool name
 *  or a non-tool fieldToMatch). Events without one treat a matcher as a no-op, so
 *  the introspection UI hides/ignores it. */
export function eventSupportsMatcher(event: string): boolean {
  return TOOL_MATCHER_EVENTS.has(event) || HOOK_MATCHER_FIELD[event] !== undefined;
}

/** Events whose matcher applies to the tool name (checked inline at dispatch,
 *  so they are not all in HOOK_MATCHER_FIELD). */
const TOOL_MATCHER_EVENTS: ReadonlySet<string> = new Set([
  'preToolUse', 'postToolUse', 'postToolUseFailure', 'permissionRequest', 'permissionDenied',
]);

const EXIT_CODE_BLOCKING =
  '\nExit code 0 — success (stdout parsed for a decision).' +
  '\nExit code 2 — blocking error: stderr is shown to the model and the action is blocked.' +
  '\nOther non-zero — non-blocking: stderr is surfaced but the action continues.';
const EXIT_CODE_OBSERVE =
  '\nExit code 0 — success (stdout not shown).' +
  '\nOther non-zero — stderr is surfaced to the user only.';

const HOOK_EVENT_SUMMARY_DESC: Record<string, { summary: string; description: string }> = {
  preToolUse: { summary: 'Before tool execution', description: 'Input: JSON of the tool call (tool_name, tool_input).' + EXIT_CODE_BLOCKING },
  postToolUse: { summary: 'After tool execution', description: 'Input: JSON with tool_name, tool_input, tool_response, duration_ms.' + EXIT_CODE_BLOCKING },
  postToolUseFailure: { summary: 'After a tool execution fails', description: 'Input: JSON with tool_name, is_error, duration_ms, error, is_interrupt, tool_use_id.' + EXIT_CODE_OBSERVE },
  permissionRequest: { summary: 'When a tool needs permission', description: 'Input: JSON with tool_name, tool_input, tool_use_id. Return a permissionDecision (allow|deny|ask).' + EXIT_CODE_BLOCKING },
  permissionDenied: { summary: 'After a permission is denied', description: 'Input: JSON with tool_name, tool_use_id. Return {retry:true} to let the model retry.' + EXIT_CODE_OBSERVE },
  preSampling: { summary: 'Before a model sampling call', description: 'Input: JSON event marker.' + EXIT_CODE_BLOCKING },
  postSampling: { summary: 'After a model sampling call', description: 'Input: JSON event marker.' + EXIT_CODE_OBSERVE },
  userPromptSubmit: { summary: 'When the user submits a prompt', description: 'Input: JSON with prompt, turn_count. May inject additionalContext or block.' + EXIT_CODE_BLOCKING },
  stop: { summary: 'When the agent turn is about to end', description: 'Input: JSON with stop_hook_active, last_assistant_message. May block turn-end.' + EXIT_CODE_BLOCKING },
  stopFailure: { summary: 'When the turn ends due to an API error (or a Stop hook itself errors)', description: `Input: JSON with error (failure class: ${STOP_FAILURE_ERROR_CLASSES.join('|')}), errorMessage, hookIndex.` + EXIT_CODE_OBSERVE },
  error: { summary: 'On an engine/turn error', description: 'Input: JSON event marker.' + EXIT_CODE_OBSERVE },
  sessionStart: { summary: 'At session start', description: `Input: JSON with session_id, source (${SESSION_START_SOURCES.join('|')}), model, agent_type. May return initialUserMessage / initialUserMessages[] / additionalContexts[] / watchPaths.` + EXIT_CODE_OBSERVE },
  sessionEnd: { summary: 'At session end', description: `Input: JSON with session_id, reason (${SESSION_END_REASONS.join('|')}).` + EXIT_CODE_OBSERVE },
  setup: { summary: 'On first-run setup', description: `Input: JSON with session_id, cwd, trigger (${SETUP_TRIGGERS.join('|')}). May return additionalContexts[].` + EXIT_CODE_OBSERVE },
  subagentStart: { summary: 'When a subagent starts', description: 'Input: JSON with prompt. May contribute additionalContexts.' + EXIT_CODE_OBSERVE },
  subagentStop: { summary: 'When a subagent stops', description: 'Input: JSON with subagent_session_id.' + EXIT_CODE_OBSERVE },
  preCompact: { summary: 'Before context compaction', description: 'Input: JSON with message_count, trigger, customInstructions. May override instructions.' + EXIT_CODE_OBSERVE },
  postCompact: { summary: 'After context compaction', description: 'Input: JSON with message_count, trigger (auto|manual), compact_summary. May return userDisplayMessage.' + EXIT_CODE_OBSERVE },
  notification: { summary: 'When a notification is sent', description: 'Input: JSON with message, notification type, title.' + EXIT_CODE_OBSERVE },
  configChange: { summary: 'When configuration changes', description: 'Input: JSON with source, filePath. A blocking hook vetoes the change.' + EXIT_CODE_BLOCKING },
  instructionsLoaded: { summary: 'When instruction files load', description: 'Input: JSON with filePath, loadReason.' + EXIT_CODE_OBSERVE },
  taskCreated: { summary: 'When a task is created', description: 'Input: JSON with taskId, subject.' + EXIT_CODE_OBSERVE },
  taskCompleted: { summary: 'When a task completes', description: 'Input: JSON with taskId, subject, status. A blocking hook vetoes completion.' + EXIT_CODE_BLOCKING },
  teammateIdle: { summary: 'When a teammate goes idle', description: 'Input: JSON with teammate_name, team_name. Fires when a teammate agent finishes its run and becomes idle.' + EXIT_CODE_OBSERVE },
  worktreeCreate: { summary: 'When a worktree is created', description: 'Input: JSON with name, worktreePath.' + EXIT_CODE_OBSERVE },
  worktreeRemove: { summary: 'When a worktree is removed', description: 'Input: JSON with name, worktreePath.' + EXIT_CODE_OBSERVE },
  fileChanged: { summary: 'When a watched file changes', description: 'Input: JSON with filePath, fileEvent. May return watchPaths.' + EXIT_CODE_OBSERVE },
  cwdChanged: { summary: 'When the working directory changes', description: 'Input: JSON with oldCwd, newCwd. May return watchPaths.' + EXIT_CODE_OBSERVE },
  elicitation: { summary: 'Before an MCP elicitation prompt', description: 'Input: JSON with mcp_server_name, message. May return action/content or block.' + EXIT_CODE_BLOCKING },
  elicitationResult: { summary: 'After an MCP elicitation reply', description: 'Input: JSON with mcp_server_name. May override action/content or block.' + EXIT_CODE_BLOCKING },
};

/** Compute (uncached) the declarative metadata for a KNOWN hook event — the
 *  caller (`getHookEventMetadata`) has already rejected unknown events, so
 *  `HOOK_EVENT_SUMMARY_DESC[event]` is guaranteed present here. */
function computeHookEventMetadata(event: string, opts: HookEventMetadataOpts): HookEventMetadata {
  const base = HOOK_EVENT_SUMMARY_DESC[event]!;
  const matcherField = TOOL_MATCHER_EVENTS.has(event) ? 'toolName' : HOOK_MATCHER_FIELD[event];
  const matcherValues = matcherField !== undefined ? resolveMatcherValues(event, matcherField, opts) : undefined;
  return {
    summary: base.summary,
    description: base.description,
    ...(matcherField ? { matcherField } : {}),
    ...(matcherValues ? { matcherValues } : {}),
  };
}

/** Memoized metadata resolver keyed by the sorted-join cache key — bounded LRU
 *  (the shared {@link memoizeWithLRU} primitive) so repeated identical tool/agent/
 *  MCP sets reuse the computed record and the cache can never grow unbounded. */
const memoizedHookEventMetadata = memoizeWithLRU(computeHookEventMetadata, hookEventMetadataCacheKey);

/** Return the declarative metadata (summary, description, matcher field +
 *  enumerated matcher values) for a hook event, or undefined for an unknown event.
 *  `opts` supplies the dynamic matcher-value sets (tool names, agent types, MCP
 *  server names) for events whose valid matcher set is runtime state. The known-
 *  event result is memoized (sorted-join cache key) so repeated same-set calls do
 *  not re-allocate the record. */
export function getHookEventMetadata(event: string, opts: HookEventMetadataOpts = {}): HookEventMetadata | undefined {
  if (!HOOK_EVENT_SUMMARY_DESC[event]) return undefined;
  return memoizedHookEventMetadata(event, opts);
}

/**
 * Translate the reference's map-keyed hooks.json into AMI's flat entry array:
 *   { "PreToolUse": [{ matcher?, hooks: [{ type, command, ... }] }] }
 * →  [{ event: 'preToolUse', matcher?, hook: { type, command, ... } }, ...]
 * Capitalized event names (PreToolUse) are lower-camel-cased (preToolUse).
 * Unknown shapes are skipped defensively.
 */
/** Map the reference's capitalized tool names to AMI tool names so a matcher like
 *  'Bash' or 'Edit|Write' actually gates the AMI tools at execution time. Shares
 *  the single canonical alias map with the permission-rule parser (no duplicate). */
const CLAUDE_TO_AMI_TOOL: Record<string, string> = PERMISSION_TOOL_ALIASES;

/** Translate the tool-name tokens in a hook matcher ('Edit|Write' → 'file_edit|
 *  file_edit'). Unknown tokens (already-AMI names, custom regex) pass through. */
export function translateHookMatcher(matcher: string): string {
  return matcher.split('|').map(tok => {
    const t = tok.trim();
    return HOOK_MATCHER_EXTRA_ALIASES[t] ?? CLAUDE_TO_AMI_TOOL[t] ?? t;
  }).join('|');
}

export function normalizeMapKeyedHooks(map: unknown): HookConfigEntry[] {
  if (!map || typeof map !== 'object' || Array.isArray(map)) return [];
  const out: HookConfigEntry[] = [];
  for (const [rawEvent, groups] of Object.entries(map as Record<string, unknown>)) {
    const event = rawEvent.charAt(0).toLowerCase() + rawEvent.slice(1);
    if (!Array.isArray(groups)) continue;
    for (const group of groups) {
      if (!group || typeof group !== 'object') continue;
      const rawMatcher = typeof (group as any).matcher === 'string' ? (group as any).matcher : undefined;
      const matcher = rawMatcher !== undefined ? translateHookMatcher(rawMatcher) : undefined;
      const innerHooks = (group as any).hooks;
      if (!Array.isArray(innerHooks)) continue;
      for (const hook of innerHooks) {
        if (hook && typeof hook === 'object') {
          out.push({ event, hook: hook as HookConfigEntry['hook'], ...(matcher ? { matcher } : {}) });
        }
      }
    }
  }
  return out;
}

/**
 * Parse a skill/agent frontmatter `hooks` value into AMI's flat {@link HookConfigEntry}
 * array (parity with the reference's per-skill/per-agent frontmatter `hooks` field,
 * registered scoped to the invocation/agent lifecycle by registerFrontmatterHooks).
 *
 * Accepts the authoring shapes used across AMI, reusing the single canonical
 * translators so the two never drift from the on-disk `hooks.json` loader:
 *   - A JSON STRING (inline or a `|` block scalar) holding either shape below —
 *     this is how a SKILL.md / agent frontmatter field is authored, mirroring the
 *     `mcp-servers` / `vars` JSON-string convention, because the line-based
 *     frontmatter parser surfaces nested config as a raw string. Parsed via
 *     `JSON.parse`; malformed JSON yields `[]`.
 *   - AMI's flat array form — `[{ event, hook: {type,...}, matcher? }]` — each entry is
 *     kept only when it has a string `event` and an object `hook`; the matcher's
 *     tool-name tokens are translated via {@link translateHookMatcher} so a
 *     the reference-style `Edit|Write` matcher gates the AMI tools.
 *   - the reference's map-keyed form — `{ PreToolUse: [{ matcher?, hooks: [...] }] }` —
 *     delegated verbatim to {@link normalizeMapKeyedHooks}.
 * Any other value (absent/number/boolean) yields `[]` so a malformed `hooks:` block
 * can never crash the loader or register a garbage hook.
 */
export function parseFrontmatterHooks(value: unknown): HookConfigEntry[] {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed.length === 0) return [];
    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      return []; // malformed JSON authoring → no hooks (never throws)
    }
    return parseFrontmatterHooks(parsed);
  }
  if (Array.isArray(value)) {
    const out: HookConfigEntry[] = [];
    for (const raw of value) {
      if (!raw || typeof raw !== 'object' || Array.isArray(raw)) continue;
      const e = raw as Record<string, unknown>;
      if (typeof e.event !== 'string' || e.event.trim().length === 0) continue;
      if (!e.hook || typeof e.hook !== 'object' || Array.isArray(e.hook)) continue;
      const matcher = typeof e.matcher === 'string' ? translateHookMatcher(e.matcher) : undefined;
      out.push({
        event: e.event.trim(),
        hook: e.hook as HookConfigEntry['hook'],
        ...(matcher ? { matcher } : {}),
      });
    }
    return out;
  }
  return normalizeMapKeyedHooks(value);
}

async function runCommandHookAsync(
  command: string,
  cwd: string,
  env: Record<string, string>,
  timeout: number,
  stdinData?: string,
  args?: string[],
  signal?: AbortSignal,
  shell?: string,
  // Interactive prompt-elicitation resolver (the reference's `requestPrompt`).
  // When provided, stdout is scanned line-by-line for `PromptRequest` JSON; each
  // request is resolved to a key, a `PromptResponse` is written back to the
  // hook's stdin, and the request lines are stripped from the captured stdout.
  // Its presence also keeps stdin open for the response round-trip.
  requestPrompt?: PromptRequestHandler,
  // Live-output sink (the reference's getHookDisplayText progress source): updated
  // with the current accumulated stdout/stderr on every chunk so a caller polling
  // it (startHookProgressInterval) can stream the hook's output while it runs.
  liveOutput?: { stdout: string; stderr: string },
): Promise<{ exitCode: number; stdout: string; stderr: string; output: string }> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) { reject(new Error('Hook cancelled')); return; }
    // Validate the working directory first — a stale/removed cwd would make
    // spawn throw ENOENT, masquerading as a hook failure (resolveHookCwd).
    const safeCwd = resolveHookCwd(cwd);
    const spawnDesc = args && args.length > 0
      ? { file: args[0], args: args.slice(1) }       // exec form — bypass the shell
      : resolveHookShell(command, shell);            // shell form — per-hook interpreter
    const proc = hookSpawnHooks.spawn(spawnDesc.file, spawnDesc.args, { cwd: safeCwd, env, stdio: ['pipe', 'pipe', 'pipe'] });

    // Capture stdout and stderr SEPARATELY (the reference contract): a command hook's
    // decision JSON is read from stdout, while the human-facing reason comes from
    // stderr. `output` keeps the combined stream for back-compat logging.
    let stdout = '';
    let stderr = '';
    // Bound in-memory capture so a hook that floods a stream cannot OOM the agent
    // (the reference TaskOutput pipe-mode memory boundary). Head-preserving: the
    // decision JSON parsed from the head of stdout always survives; the overflow
    // tail is dropped and a one-time notice is appended at close.
    let stdoutTruncated = false;
    let stderrTruncated = false;
    // Running UTF-8 byte counts threaded into boundedHookAppend so it never
    // re-measures the whole accumulated stream per chunk (avoids O(N·K) growth).
    let stdoutBytes = 0;
    let stderrBytes = 0;
    // Interactive prompt-elicitation scanner: only active when a resolver is
    // supplied. It detects PromptRequest lines in the streaming stdout, resolves
    // each to a key, and writes a PromptResponse back to the hook's stdin. A
    // resolver rejection (user cancelled) closes stdin so the hook does not hang.
    // Serialize prompt handling so responses are written back in request order
    // (the reference's `promptChain`).
    let promptChain: Promise<void> = Promise.resolve();
    const scanner = requestPrompt
      ? createPromptLineScanner((req) => {
          promptChain = promptChain.then(async () => {
            try {
              const selected = await requestPrompt(req);
              proc.stdin?.write(serializePromptResponse(buildPromptResponse(req, selected)), 'utf-8');
            } catch {
              // User cancelled / resolver failed — close stdin so the hook does
              // not hang waiting for a response it will never get.
              try { proc.stdin?.destroy(); } catch { /* already closed */ }
            }
          });
        })
      : undefined;
    proc.stdout?.on('data', (d: Buffer) => {
      const chunk = d.toString();
      scanner?.feed(chunk);
      const r = boundedHookAppend(stdout, chunk, MAX_HOOK_COMMAND_OUTPUT_BYTES, stdoutBytes);
      stdout = r.text; stdoutBytes = r.bytes; if (r.truncated) stdoutTruncated = true;
      if (liveOutput) liveOutput.stdout = stdout;
    });
    proc.stderr?.on('data', (d: Buffer) => {
      const r = boundedHookAppend(stderr, d.toString(), MAX_HOOK_COMMAND_OUTPUT_BYTES, stderrBytes);
      stderr = r.text; stderrBytes = r.bytes; if (r.truncated) stderrTruncated = true;
      if (liveOutput) liveOutput.stderr = stderr;
    });

    proc.stdin?.on('error', (err: NodeJS.ErrnoException) => {
      if (err.code !== 'EPIPE') coreLog('hooks', `Hook stdin error: ${err.message}`);
    });

    // When a prompt resolver is active, stdin MUST stay open so PromptResponse
    // lines can be written back (the reference parity: `requestPrompt` keeps stdin
    // open). Otherwise close stdin once the input payload is written so a hook
    // that reads-to-EOF does not block.
    if (stdinData) {
      try {
        // stdinData (serializeHookEvent output) is already newline-terminated, so
        // the hook's first line-read gets the whole event payload and a later
        // read gets the prompt response — never a stray empty line.
        proc.stdin?.write(stdinData, 'utf-8');
        if (!requestPrompt) proc.stdin?.end();
      } catch { if (!requestPrompt) proc.stdin?.end(); }
    } else if (!requestPrompt) {
      proc.stdin?.end();
    }

    // On timeout/abort, reap the WHOLE process tree (killTree), not just the
    // direct `bash -c` child: a hook that backgrounded a `sleep 30 &`, started a
    // server, or spawned a `docker`/`curl` grandchild would otherwise leak those
    // descendants after the hook is declared timed-out. killTree enumerates the
    // child's process tree (+ a process-group sweep) and SIGKILLs every member
    // (the reference's treeKill parity).
    const timer = setTimeout(() => {
      killTree(proc);
      reject(new Error(`Hook timed out after ${timeout}ms`));
    }, timeout);

    // Parent/user cancellation: kill the child tree and surface a distinct
    // "cancelled" outcome (vs timeout) so callers can tell them apart.
    const onAbort = () => {
      killTree(proc);
      clearTimeout(timer);
      reject(new Error('Hook cancelled'));
    };
    if (signal) signal.addEventListener('abort', onAbort, { once: true });
    const detach = () => { if (signal) signal.removeEventListener('abort', onAbort); };

    proc.on('close', (code) => {
      clearTimeout(timer);
      detach();
      // Strip handled prompt-request lines from the captured stdout so the
      // decision parser never sees the elicitation protocol chatter.
      if (scanner) stdout = scanner.stripProcessed(stdout);
      if (stdoutTruncated) stdout += hookOutputTruncationNotice('stdout');
      if (stderrTruncated) stderr += hookOutputTruncationNotice('stderr');
      resolve({ exitCode: code ?? 1, stdout, stderr, output: stdout + stderr });
    });

    proc.on('error', (err) => {
      clearTimeout(timer);
      detach();
      reject(err);
    });
  });
}

export class HookManager extends EventEmitter {
  private _engineFactory: EngineFactory | null = null;
  /** Re-entrancy depth of engine-backed (prompt/agent) hooks currently running on
   *  this manager. Non-zero means a hook sub-engine is live; {@link
   *  executeUserPromptSubmit} short-circuits so the sub-engine's own submit cannot
   *  re-fire UserPromptSubmit hooks and recurse (the reference's execPromptHook
   *  bypasses processUserInput for the same reason). */
  private _engineHookDepth = 0;
  private _providerConfig: any = null;
  /** Live conversation-history provider for engine-backed PROMPT hook evaluation.
   *  When set (wired by the engine to its own message list), {@link runEngineHook}
   *  prepends the returned messages before the hook's user message so the LLM judges
   *  the condition against the ACTUAL conversation — e.g. a prompt hook that must
   *  block only "if the user did not already agree earlier" needs to see the prior
   *  turns, not just the single event payload. Parity: the reference's execPromptHook
   *  optional `messages[]` → `messagesToQuery = [...messages, userMessage]`
   *  (execPromptHook.ts:L28,L44-48). AGENT hooks are deliberately excluded: they
   *  read the transcript FILE via a scoped Read grant instead (execAgentHook), so
   *  they must not ALSO get the history inlined (double context). Unset = current
   *  behavior (the self-contained prompt + event data only). */
  private _conversationHistoryProvider?: () => Message[];
  private _cwd: string = process.cwd();
  private _sessionId: string = '';
  private _workspaceTrusted = false;
  private _interactive = false;
  private _httpHookPolicy: HttpHookPolicy = {};
  // Interactive prompt-elicitation resolver (the reference's `requestPrompt`).
  // When set (interactive frontend), command hooks may ask the user to pick from
  // a labelled option list mid-run. Unset in non-interactive/detached mode so a
  // hook's PromptRequest is simply left unanswered (parity: no resolver → no
  // interactive prompt).
  private _promptRequestHandler: PromptRequestHandler | undefined;
  /** SSRF validator seam (defaults to the shared web-utils validateUrlSafety).
   *  Injectable so tests can exercise the HTTP-hook success/non-2xx paths against
   *  a loopback mock server that the real validator would (correctly) block. */
  private _validateUrlSafety: typeof validateUrlSafety = validateUrlSafety;
  /** Fetch used for HTTP hooks routed through an env-var proxy (HTTP(S)_PROXY,
   *  honoring NO_PROXY). Defaults lazily to the canonical provider proxy/mTLS
   *  dispatcher wrapper (no second proxy implementation); injectable so tests can
   *  exercise the proxied branch without standing up a real forward proxy. */
  private _proxiedHookFetch: typeof fetch | undefined;
  private _callbackHooks: Map<string, HookCallback[]> = new Map();
  /** Per-callback timeout (ms) for in-process ('function') hooks, keyed by the
   *  callback. Unset callbacks use DEFAULT_CALLBACK_HOOK_TIMEOUT_MS. */
  private _callbackTimeouts = new WeakMap<HookCallback, number>();
  /** Scoping metadata (matcher / skillRoot / sessionId / id) for runtime callback
   *  hooks, keyed by the callback. Drives matcher-gated dispatch in
   *  {@link executeCallbacks} and grouped teardown by skill root / session. */
  private _callbackMeta = new WeakMap<HookCallback, RuntimeHookMeta>();
  /** Function hooks indexed by id (the reference's removeFunctionHook id lookup):
   *  the firing event + the wrapper callback registered under it, so a function
   *  hook can be removed by the id {@link addFunctionHook} returned. */
  private _functionHooks = new Map<string, { event: string; callback: HookCallback }>();

  // Gap 1: Tier-aware verdict hold-over
  private _hookTiers = new WeakMap<Function, string>();
  /** Provenance of every config/frontmatter-registered hook fn (source tier +, for
   *  plugin-contributed hooks, the plugin bundle name/root). Captured in
   *  {@link processHookEntry} so a plugin's hooks can be attributed and surgically
   *  pruned/reloaded ({@link pruneRemovedPluginHooks}/{@link reloadPluginHooks},
   *  gh-36995). A WeakMap so a removed hook's entry is GC'd with the fn. */
  private _hookProvenance = new WeakMap<Function, { source: string; pluginName?: string; pluginRoot?: string }>();
  /** Per-Stop-hook display descriptor (command / promptText), keyed by the registered
   *  Stop-hook fn, so {@link executeStop} can attribute each per-hook timing/error row
   *  in the run summary to its backing config (the reference's StopHookInfo.command /
   *  promptText). A WeakMap so a removed/cleared Stop hook's entry is GC'd with the fn. */
  private _stopHookDisplay = new WeakMap<StopHookFn, StopHookDisplay>();
  private _denyRulesHold = false;
  private _concurrency = new EventConcurrencyLimiter();
  private _effortLevel: string = 'high';
  /** Absolute paths hooks have asked to watch for FileChanged (via watchPaths
   *  in sessionStart/cwdChanged/fileChanged output). Exposed for a file-watcher
   *  consumer and surfaced via the 'watchPaths' event. */
  private _watchedPaths = new Set<string>();

  /** File-loaded hook descriptors retained for introspection (describeHooks /
   *  describeEvents). Populated by processHookEntry and reset at the start of each
   *  loadFromFile so a reload reflects the current on-disk config. */
  private _descriptors: HookDescriptor[] = [];

  /** Startup fingerprint of the executable hook surface (event+source+matcher+
   *  content signatures), captured on the first {@link loadFromFile} and
   *  re-captured on a cwd change. Used to detect hidden hook modifications made
   *  mid-session. `undefined` until the first load. */
  private _hookSnapshotBaseline?: Set<string>;
  /** The cwd the {@link _hookSnapshotBaseline} was captured for; a change
   *  re-baselines rather than flagging the new workspace's hooks. */
  private _hookSnapshotCwd?: string;
  /** Hook entries that appeared or were mutated since the startup snapshot,
   *  surfaced via {@link getHiddenHookModifications}. */
  private _hiddenHookModifications: HookDescriptor[] = [];

  /** Fingerprint of the plugin-affecting settings the last plugin-hook reload ran
   *  against (sources + the managed/trust/kill-switch policy that gate them). Lets
   *  {@link reloadPluginHooksIfChanged} skip a redundant clear+re-register when
   *  nothing that affects plugin hooks actually changed (the reference's
   *  getPluginAffectingSettingsSnapshot + "skipping reload, plugin-affecting
   *  settings unchanged" debug log). `undefined` until the first gated reload. */
  private _pluginSettingsSnapshot?: string;

  /** Settings/hooks/config files that failed to parse during the last
   *  {@link loadFromFile}. Reset at the start of each load so it reflects the
   *  current on-disk state. Surfaced via {@link getConfigErrors} + the
   *  `configError` event + `/hooks` so a malformed file (which silently drops all
   *  its hooks) is visible instead of only logged. */
  private _configErrors: HookConfigError[] = [];

  /** The most recent Stop-hook run summary (per-hook timings + errors). Retained
   *  for introspection; also emitted live on the `stopHookSummary` event. */
  private _lastStopHookSummary: StopHookSummary | null = null;

  /** Bounded FIFO of hook-lifecycle events produced while no
   *  {@link registerHookEventHandler} subscriber is attached (the reference's
   *  `pendingEvents` ring). Drained in order on register so an early event is
   *  replayed, capped at {@link MAX_PENDING_HOOK_EVENTS} (oldest dropped). */
  private _pendingHookEvents: HookLifecycleEvent[] = [];
  /** The currently-attached hook-lifecycle subscriber, or null (buffer mode). */
  private _hookEventHandler: ((e: HookLifecycleEvent) => void) | null = null;

  /** Whether lifecycle events for ALL hook events are published, vs only the
   *  {@link ALWAYS_EMITTED_HOOK_EVENTS} allowlist (the reference's SDK
   *  `includeHookEvents` / remote-mode toggle). AMI defaults this ON — its richer
   *  all-events observability is an intentional superset of the reference's default-off
   *  gate; {@link setAllHookEventsEnabled}(false) opts DOWN to the reference's quiet
   *  allowlist-only behaviour for low-noise consumers. */
  private _allHookEventsEnabled = true;

  /**
   * When enabled, org/builtin deny verdicts cannot be loosened by user-tier hooks.
   */
  setDenyRulesHold(hold: boolean): void { this._denyRulesHold = hold; }

  constructor() {
    super();
    // Feed the replayable hook-lifecycle buffer from the live EventEmitter
    // channel so progress/resolved events emitted before a subscriber attaches
    // are not lost (the reference's hookEvents.ts pending-buffer guarantee).
    this.on('hookProgress', (p: HookProgressEvent) =>
      this.publishHookLifecycle({ phase: 'progress', event: p.event, hookType: p.hookType, label: p.label, hookId: p.hookId, hookName: p.hookName }));
    this.on('hookResolved', (p: HookProgressEvent & { success?: boolean }) =>
      this.publishHookLifecycle({ phase: 'resolved', event: p.event, hookType: p.hookType, label: p.label, success: p.success, hookId: p.hookId, hookName: p.hookName, outcome: p.outcome }));
  }

  /**
   * Publish a hook-lifecycle event to the attached subscriber, or buffer it in a
   * bounded FIFO (dropping the oldest past {@link MAX_PENDING_HOOK_EVENTS}) when
   * none is attached. Parity with the reference's hookEvents.ts `emit`.
   */
  private publishHookLifecycle(e: HookLifecycleEvent): void {
    // Honour the all-events gate (the reference's hookEvents.ts `shouldEmit`): when a
    // consumer has quieted the channel via setAllHookEventsEnabled(false), only the
    // ALWAYS_EMITTED_HOOK_EVENTS allowlist (sessionStart/setup) still flows.
    if (!shouldEmitHookLifecycle(e.event, this._allHookEventsEnabled)) return;
    if (this._hookEventHandler) { this._hookEventHandler(e); return; }
    this._pendingHookEvents.push(e);
    if (this._pendingHookEvents.length > MAX_PENDING_HOOK_EVENTS) this._pendingHookEvents.shift();
  }

  /**
   * Attach (or, with `null`, detach) a hook-lifecycle subscriber. On attach, any
   * events buffered while no subscriber was present are replayed IN ORDER and the
   * buffer is drained — guaranteeing a late subscriber still sees early events
   * (the reference's registerHookEventHandler). Returns a detach closure.
   */
  registerHookEventHandler(handler: ((e: HookLifecycleEvent) => void) | null): () => void {
    this._hookEventHandler = handler;
    if (handler && this._pendingHookEvents.length > 0) {
      for (const e of this._pendingHookEvents.splice(0)) handler(e);
    }
    return () => { this._hookEventHandler = null; };
  }

  /** Number of hook-lifecycle events currently buffered awaiting a subscriber
   *  (introspection surface). */
  getPendingHookEventCount(): number { return this._pendingHookEvents.length; }

  /**
   * Toggle whether lifecycle events for ALL hook events are published, or only the
   * {@link ALWAYS_EMITTED_HOOK_EVENTS} allowlist (the reference's `setAllHookEventsEnabled`
   * driven by the SDK `includeHookEvents` option / remote mode). Passing `false`
   * quiets the channel down to the reference's low-noise default (sessionStart/setup only);
   * `true` (AMI's default) restores full all-events observability. Takes effect for
   * subsequent lifecycle emissions; already-buffered events are unaffected.
   */
  setAllHookEventsEnabled(enabled: boolean): void { this._allHookEventsEnabled = enabled; }

  /** Whether all hook-event lifecycle notifications are currently published (vs the
   *  {@link ALWAYS_EMITTED_HOOK_EVENTS} allowlist only). Introspection surface. */
  isAllHookEventsEnabled(): boolean { return this._allHookEventsEnabled; }

  setEngineFactory(factory: EngineFactory): void { this._engineFactory = factory; }
  setProviderConfig(config: any): void { this._providerConfig = config; }
  /** Wire the live conversation history a PROMPT hook should evaluate against
   *  (see {@link _conversationHistoryProvider}). The engine passes a thunk over its
   *  own message list so the hook sees the conversation as it stands at fire time;
   *  pass `undefined` to clear it. */
  setConversationHistoryProvider(provider: (() => Message[]) | undefined): void { this._conversationHistoryProvider = provider; }
  setCwd(cwd: string): void { this._cwd = cwd; }
  setSessionId(id: string): void { this._sessionId = id; }

  setWorkspaceTrusted(trusted: boolean): void { this._workspaceTrusted = trusted; }
  setEffortLevel(level: string): void { this._effortLevel = level; }
  getEffortLevel(): string { return this._effortLevel; }
  isWorkspaceTrusted(): boolean { return this._workspaceTrusted; }

  /** Whether this is an interactive (TUI) session. Drives the workspace-trust
   *  gate: in interactive mode untrusted workspaces skip ALL hooks; in
   *  non-interactive/SDK mode trust is implicit (matches the reference). */
  setInteractive(interactive: boolean): void { this._interactive = interactive; }
  isInteractive(): boolean { return this._interactive; }

  /** Install the merged HTTP-hook policy (URL allowlist + header env allowlist). */
  setHttpHookPolicy(policy: HttpHookPolicy | undefined): void { this._httpHookPolicy = policy ?? {}; }
  getHttpHookPolicy(): HttpHookPolicy { return this._httpHookPolicy; }

  /** Permission mode for engine-backed (prompt/agent) hooks. Defaults to
   *  'auto-allow' but the session can set its real permission mode so a hook's
   *  sub-engine runs under the session's permission context, not blanket allow. */
  private _hookEnginePermissionMode: string = 'auto-allow';
  setHookEnginePermissionMode(mode: string): void { this._hookEnginePermissionMode = mode; }
  getHookEnginePermissionMode(): string { return this._hookEnginePermissionMode; }

  /** When true (org policy), ONLY admin/managed-settings hooks execute — project,
   *  user, and plugin hooks are not loaded (the reference's allowManagedHooksOnly). */
  private _allowManagedHooksOnly = false;
  setAllowManagedHooksOnly(on: boolean): void { this._allowManagedHooksOnly = on; }
  isAllowManagedHooksOnly(): boolean { return this._allowManagedHooksOnly; }

  /**
   * Read the raw `strictPluginOnlyCustomization` value from the managed/policy
   * settings file so {@link isRestrictedToPluginOnly} can decide whether the
   * `hooks` surface is locked to plugin/managed-only sources. Managed-sourced by
   * construction (this is the admin policy file). Never throws: a missing or
   * malformed file degrades to `undefined` (nothing locked). Parity: the reference's
   * `getSettingsForSource('policySettings')?.strictPluginOnlyCustomization`.
   */
  private readManagedPluginOnlyPolicy(managedPath: string): unknown {
    try {
      const parsed = JSON.parse(stripBom(fs.readFileSync(managedPath, 'utf-8')));
      return parsed && typeof parsed === 'object'
        ? (parsed as Record<string, unknown>).strictPluginOnlyCustomization
        : undefined;
    } catch {
      return undefined;
    }
  }

  /**
   * Whether the `hooks` customization surface is locked to admin/plugin-only
   * sources by `strictPluginOnlyCustomization` in the managed/policy settings file.
   * Read fresh from disk each call so frontmatter/bundle hook registration (which
   * can happen any time a skill/agent is invoked, after startup) observes the live
   * policy. Degrades to `false` (unlocked) when no managed path is set or the file
   * is absent/malformed. Parity: the reference's `isRestrictedToPluginOnly('hooks', …)`
   * computed from `policySettings.strictPluginOnlyCustomization`.
   */
  private isHooksSurfaceRestricted(): boolean {
    const managedPath = process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    if (!managedPath) return false;
    return isRestrictedToPluginOnly('hooks', this.readManagedPluginOnlyPolicy(managedPath));
  }

  /** Managed-policy kill-switch (the reference's disableAllHooks). When set via a
   *  managed source it skips ALL hooks, including managed ones. */
  private _disableAllHooks = false;

  /**
   * Set the disableAllHooks kill-switch with the reference's managed > non-managed
   * precedence: a MANAGED `disableAllHooks` disables ALL hooks (including
   * managed); a NON-managed `disableAllHooks` can only disable non-managed hooks
   * (i.e. it collapses to allowManagedHooksOnly — managed hooks still run) and can
   * never re-enable a managed disable. Consulted by loadFromFile before any hook
   * is registered.
   */
  setDisableAllHooks(disabled: boolean, managed = true): void {
    if (managed) this._disableAllHooks = disabled;
    else if (disabled) this._allowManagedHooksOnly = true;
  }
  /** True when every hook is disabled — either the managed `disableAllHooks`
   *  policy OR the `CLAUDE_CODE_SIMPLE` (AMI_SIMPLE / AMI_DISABLE_HOOKS) env
   *  kill-switch. The env switch is evaluated live so a hook-free session needs
   *  no settings file. */
  isAllHooksDisabled(): boolean { return this._disableAllHooks || isHookKillSwitchEnv(); }

  /** Override the HTTP-hook SSRF validator (tests only; defaults to the shared
   *  web-utils validateUrlSafety). */
  setUrlSafetyValidator(fn: typeof validateUrlSafety): void { this._validateUrlSafety = fn; }

  /** Override the proxied-HTTP-hook fetch (tests only; defaults lazily to the
   *  provider proxy/mTLS dispatcher wrapper via {@link runHttpHook}). */
  setProxiedHookFetch(fn: typeof fetch | undefined): void { this._proxiedHookFetch = fn; }

  registerCallback(event: string, callback: HookCallback, timeoutMs?: number, meta?: RuntimeHookMeta): () => void {
    const hooks = this._callbackHooks.get(event) || [];
    hooks.push(callback);
    this._callbackHooks.set(event, hooks);
    // Per-callback timeout (the reference addFunctionHook `timeout`): bound a
    // slow/stuck in-process callback so it cannot stall the turn.
    if (timeoutMs !== undefined) this._callbackTimeouts.set(callback, timeoutMs);
    // Scoping metadata (matcher/skillRoot/sessionId/id) for matcher-gated dispatch
    // and grouped teardown (the reference's SessionHookMatcher).
    if (meta) this._callbackMeta.set(callback, meta);
    return () => this.unregisterCallback(event, callback);
  }

  /** Remove one runtime callback from an event and drop all its side tables. */
  private unregisterCallback(event: string, callback: HookCallback): void {
    const current = this._callbackHooks.get(event) || [];
    const index = current.indexOf(callback);
    if (index >= 0) current.splice(index, 1);
    if (current.length === 0) this._callbackHooks.delete(event);
    this._callbackTimeouts.delete(callback);
    const meta = this._callbackMeta.get(callback);
    this._callbackMeta.delete(callback);
    if (meta?.id) this._functionHooks.delete(meta.id);
  }

  /**
   * Register a runtime ('function') hook: an in-memory callback that inspects the
   * conversation messages and returns `true` (pass) / `false` (block). Parity with
   * the reference's addFunctionHook — defaults the timeout to 5000ms, auto-generates
   * an id when none is given, and RETURNS the id so the hook can later be removed
   * via {@link removeFunctionHook}. When a `false` verdict blocks the event the
   * `errorMessage` is surfaced as the block reason. Re-registering an explicit id
   * replaces the prior hook (idempotent). The callback is matcher-gated via
   * {@link executeCallbacks} when an `options.matcher` is supplied.
   */
  addFunctionHook(
    event: string,
    callback: FunctionHookCallback,
    errorMessage: string,
    options?: { timeout?: number; id?: string; matcher?: string; skillRoot?: string; sessionId?: string; statusMessage?: string; internal?: boolean },
  ): string {
    // Build the canonical FunctionHook record (the reference's sessionHooks.ts
    // shape) so its id/timeout defaults and schema fields live in one place.
    const spec: FunctionHook = {
      type: 'function',
      id: options?.id || `function-hook-${Date.now()}-${Math.random()}`,
      timeout: options?.timeout ?? DEFAULT_CALLBACK_HOOK_TIMEOUT_MS,
      callback,
      errorMessage,
      ...(options?.statusMessage !== undefined ? { statusMessage: options.statusMessage } : {}),
    };
    const id = spec.id!;
    // Replace a prior hook registered under the same explicit id (last wins).
    if (this._functionHooks.has(id)) this.removeFunctionHook(id);
    const wrapper: HookCallback = async (_ev, data) => {
      const messages = (data && typeof data === 'object' ? (data as { messages?: Message[] }).messages : undefined) ?? [];
      const pass = await spec.callback(messages);
      return pass ? undefined : ({ decision: 'block', reason: spec.errorMessage } as HookDecisionOutput);
    };
    this.registerCallback(event, wrapper, spec.timeout, {
      id,
      ...(options?.matcher !== undefined ? { matcher: options.matcher } : {}),
      ...(options?.skillRoot !== undefined ? { skillRoot: options.skillRoot } : {}),
      ...(options?.sessionId !== undefined ? { sessionId: options.sessionId } : {}),
      ...(options?.internal ? { internal: true } : {}),
    });
    this._functionHooks.set(id, { event, callback: wrapper });
    return id;
  }

  /** Remove a function hook by the id {@link addFunctionHook} returned. Returns
   *  true when a hook was removed (the reference's removeFunctionHook). */
  removeFunctionHook(id: string): boolean {
    const entry = this._functionHooks.get(id);
    if (!entry) return false;
    this.unregisterCallback(entry.event, entry.callback);
    this._functionHooks.delete(id);
    return true;
  }

  /**
   * Register a session/agent-scoped runtime hook (the reference's addSessionHook via
   * registerFrontmatterHooks). The hook is tagged with `sessionId` so it can be
   * torn down as a group by {@link clearSessionHooks} when that session/agent ends.
   * With `isAgent:true` a `stop` hook is remapped to `subagentStop` (a subagent
   * fires SubagentStop, not Stop). Optional `matcher`/`skillRoot` scope + group it.
   * Returns an unregister closure.
   */
  addSessionHook(
    sessionId: string,
    event: string,
    callback: HookCallback,
    opts?: { matcher?: string; skillRoot?: string; isAgent?: boolean; timeoutMs?: number; internal?: boolean },
  ): () => void {
    const targetEvent = opts?.isAgent && event === 'stop' ? 'subagentStop' : event;
    return this.registerCallback(targetEvent, callback, opts?.timeoutMs, {
      sessionId,
      ...(opts?.matcher !== undefined ? { matcher: opts.matcher } : {}),
      ...(opts?.skillRoot !== undefined ? { skillRoot: opts.skillRoot } : {}),
      ...(opts?.internal ? { internal: true } : {}),
    });
  }

  /** Tear down every runtime callback scoped to a session/agent id (the reference's
   *  clearSessionHooks, called on sessionEnd/subagentStop). Returns the count removed. */
  clearSessionHooks(sessionId: string): number {
    return this.removeRuntimeHooksWhere(meta => meta.sessionId === sessionId);
  }

  /** Tear down every runtime callback registered by a given skill root (the reference
   *  Code's skill-scoped teardown on stance deactivation). Returns the count removed. */
  removeBySkillRoot(skillRoot: string): number {
    return this.removeRuntimeHooksWhere(meta => meta.skillRoot === skillRoot);
  }

  /** Remove every runtime callback whose scoping metadata satisfies `pred`. */
  private removeRuntimeHooksWhere(pred: (meta: RuntimeHookMeta) => boolean): number {
    let removed = 0;
    for (const [event, hooks] of Array.from(this._callbackHooks.entries())) {
      for (const cb of [...hooks]) {
        const meta = this._callbackMeta.get(cb);
        if (meta && pred(meta)) { this.unregisterCallback(event, cb); removed++; }
      }
    }
    return removed;
  }

  /**
   * Execute all registered callback hooks for the given event.
   * Returns the first non-void HookDecisionOutput if any callback returns one.
   * When `matchValue` is supplied, a callback registered with a non-empty `matcher`
   * fires ONLY if the matcher applies to it (the reference's SessionHookMatcher
   * gating; an empty/absent matcher is the `''` wildcard that always fires).
   *
   * An optional `signal` (the turn/abort signal) is threaded into each callback
   * for cooperative cancellation (the reference's `HookCallback` `abort` arg): a
   * callback can observe it to bail early, the caller stops awaiting a hung
   * callback the instant it aborts ({@link raceAbort}), and once aborted the
   * remaining callbacks are skipped rather than run against a cancelled turn.
   */
  async executeCallbacks(event: string, data: unknown, matchValue?: string, signal?: AbortSignal): Promise<HookDecisionOutput | void> {
    const all = this._callbackHooks.get(event);
    if (!all || all.length === 0) return;
    // Snapshot + matcher-filter so a hook removed mid-dispatch or gated out by its
    // matcher is skipped without mutating the live array during iteration.
    const hooks = all.filter(cb => {
      const matcher = this._callbackMeta.get(cb)?.matcher;
      if (!matcher || matchValue === undefined) return true;
      return matchesPattern(matcher, matchValue);
    });
    if (hooks.length === 0) return;
    return this.withConcurrencyGate(`cb:${event}`, async () => {
      for (const cb of hooks) {
        // Stop consulting callbacks once the turn is aborted (user interrupt):
        // a cancelled event runs no further in-process hooks (parity with
        // the reference's abort-aware runHooks loop; the Pre/PostToolUse arm-hook paths
        // already short-circuit on abort the same way).
        if (signal?.aborted) break;
        try {
          // Bound each in-process callback by its per-callback timeout (or the
          // default) so a stuck function hook cannot hang the event, AND race it
          // against the abort signal so a user interrupt stops the await early.
          const timeoutMs = this._callbackTimeouts.get(cb) ?? DEFAULT_CALLBACK_HOOK_TIMEOUT_MS;
          const result = await raceAbort(
            runWithTimeout(Promise.resolve(cb(event, data, signal)), timeoutMs, `Callback hook "${event}"`),
            signal,
          );
          if (result && typeof result === 'object') return result as HookDecisionOutput;
        } catch (err) {
          coreLog('hooks', `Callback hook error for event "${event}": ${err}`);
        }
      }
      return undefined as unknown as HookDecisionOutput | void;
    });
  }

  // ---- Async ('background') hook registry ----
  /** In-flight async hook promises keyed to their metadata (firing event +
   *  start timestamp), so they can be awaited (finalize) or cancelled (kill) at
   *  shutdown AND introspected while running (which events, how long) instead of
   *  leaking as untracked fire-and-forget promises. */
  private _pendingAsync = new Map<Promise<unknown>, { event: string; startedAt: number }>();
  /** Shutdown signal shared by all async hooks; aborting it tears down every
   *  in-flight async hook (command child killed / HTTP request aborted / engine
   *  hook query aborted). Reset after cancelAll so later hooks are not born
   *  pre-aborted. */
  private _asyncAbort = new AbortController();

  /** Live view of every in-flight async hook (introspection surface): the event
   *  it fired for and how long it has been running. the reference tracks pending
   *  background hooks with elapsed time so a stall is visible; this exposes the
   *  same (the richer companion to {@link getPendingAsyncCount}). */
  getPendingAsyncHooks(): Array<{ event: string; elapsedMs: number }> {
    const now = Date.now();
    return Array.from(this._pendingAsync.values()).map(r => ({ event: r.event, elapsedMs: now - r.startedAt }));
  }

  /** Number of async hooks still running (introspection surface). */
  getPendingAsyncCount(): number { return this._pendingAsync.size; }

  /**
   * Fire an async ('background') hook: run it under a shutdown-cancellable signal
   * (combined with any parent/turn signal), track the promise so it can be
   * finalized/cancelled at shutdown, and swallow its errors. Replaces the old
   * untracked fire-and-forget firing of async hooks.
   */
  private fireAsyncHook(
    config: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
    cwd: string,
    eventData: string,
    event: string,
    parentSignal?: AbortSignal,
    // Fired ONLY after the background hook completes successfully, so a
    // `once:true` async hook self-removes just like its synchronous sibling
    // (previously an async once-hook was never removed and re-fired every event).
    onSuccess?: () => void,
    // Per-hook env-file hint forwarded to runHookByType (CLAUDE_ENV_FILE parity).
    envFileHint?: { event: string; index: number },
    // Fired once the background hook settles (success or failure), so the running
    // -hook progress indicator can be cleared for async hooks too.
    onSettled?: (success: boolean) => void,
  ): void {
    const ac = new AbortController();
    const onAbort = () => ac.abort();
    if (this._asyncAbort.signal.aborted || parentSignal?.aborted) {
      ac.abort();
    } else {
      this._asyncAbort.signal.addEventListener('abort', onAbort, { once: true });
      parentSignal?.addEventListener('abort', onAbort, { once: true });
    }
    // A background hook is bounded on its OWN short clock (asyncTimeout, default
    // 15s), independent of the long synchronous command/HTTP default: once the
    // budget elapses the hook is aborted (child killed / request aborted) so a
    // runaway background hook cannot linger for minutes. A hook that settles
    // first clears the timer in the finally below.
    const asyncTimeoutMs = validateHookTimeout(config.asyncTimeout ?? DEFAULT_ASYNC_HOOK_TIMEOUT_MS);
    const timer = setTimeout(onAbort, asyncTimeoutMs);
    if (typeof timer.unref === 'function') timer.unref();
    const outcome: HookOutcome = { success: true };
    const p: Promise<unknown> = this.runHookByType(config, cwd, eventData, ac.signal, outcome, envFileHint)
      .then((output) => {
        if (outcome.success && onSuccess) onSuccess();
        // Deliver the COMPLETED background hook's result back into the turn once
        // (the reference's checkForAsyncHookResponses). Without this a backgrounded
        // hook that exits 0 with additionalContext / systemMessage / plain stdout
        // is silently dropped — the fire-and-forget path moved on without reading
        // its output. Gated on the hook's explicit `asyncRewake` opt-in, matching
        // the DENY/exit-2 re-wake gate; DENY outputs already re-woke at their
        // source (asyncRewakeDeliveryMessage returns undefined for them), so this
        // never double-fires.
        if (config.asyncRewake && typeof output === 'string') {
          const message = asyncRewakeDeliveryMessage(output);
          if (message) this.emit('hookRewake', { event: config.type, message, ...this.rewakeAttribution(config) });
        }
      })
      .catch(() => { /* async hooks never surface errors to the turn */ })
      .finally(() => {
        clearTimeout(timer);
        this._asyncAbort.signal.removeEventListener('abort', onAbort);
        parentSignal?.removeEventListener('abort', onAbort);
        this._pendingAsync.delete(p);
        onSettled?.(outcome.success);
        // Structured completion RECORD for EVERY async hook (not only the
        // DENY/exit-2/error re-wake above) — classify how it ended so a
        // user/introspection surface sees a clean success too. An aborted signal
        // means the hook was cancelled (shutdown) or hit its async-timeout budget;
        // otherwise `outcome.success` discriminates success vs error. Parity:
        // the reference's finalizeHook/emitHookResponse (AsyncHookRegistry.ts).
        const resolvedOutcome: HookLifecycleOutcome = classifyHookOutcome(outcome.success, ac.signal.aborted);
        this.emit('hookResponse', {
          event,
          outcome: resolvedOutcome,
          exitCode: outcome.exitCode,
          stdout: outcome.stdout,
          stderr: outcome.stderr,
          ...this.rewakeAttribution(config),
        });
      });
    this._pendingAsync.set(p, { event, startedAt: Date.now() });
  }

  /**
   * Canonical per-event hook runner for config-registered hooks. Runs the hook
   * (sync or fire-and-forget async) and handles `once:true` self-removal with
   * the reference's onHookSuccess semantics: the one-shot hook is spliced out ONLY
   * after a SUCCESSFUL execution (not when it errored, timed out, or was blocked
   * by an error), for BOTH the sync and async paths. Returns the flattened result
   * string; for an async hook it returns `''` (no blocking result is awaited),
   * which every event arm already handles identically to a no-output hook.
   */
  private async runArmHook<T>(
    config: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
    cwd: string,
    eventData: string,
    signal: AbortSignal | undefined,
    arr: T[],
    fn: T,
    event: string,
    // Force an `async:true` hook to run SYNCHRONOUSLY for this invocation (the reference
    // Code's forceSyncExecution). Threaded only by the sessionStart/setup arms from
    // SessionContext.forceSyncExecution; every other caller leaves it false so the
    // hook's own `async` flag governs as before.
    forceSync = false,
  ): Promise<string> {
    // Index of this hook within its event array — keys the per-hook env-file path
    // so two hooks on the same event export into distinct, deterministic fragments.
    const idx = arr.indexOf(fn);
    const envFileHint = idx >= 0 ? { event, index: idx } : undefined;
    // Running-hook progress affordance (the reference's hook_progress /
    // getHookDisplayText): announce a command/prompt/http/agent hook is about to
    // run and, when it settles, that it resolved — so a frontend can render a
    // "Running <event> hook…" indicator instead of a silent multi-second hang.
    const progress = this.hookProgressPayload(config, event, idx);
    this.emit('hookProgress', progress);
    // forceSync (the reference's forceSyncExecution) overrides the hook's `async`
    // opt-in: a sessionStart/setup hook that would normally be fire-and-forget is
    // awaited inline so its initialUserMessage / additionalContexts are captured
    // before the caller continues (a backgrounded result would arrive too late).
    if (config.async && !forceSync) {
      this.fireAsyncHook(
        config, cwd, eventData, event, signal,
        config.once ? () => this.removeHookOnce(arr, fn, event) : undefined,
        envFileHint,
        (success) => this.emit('hookResolved', { ...progress, success, outcome: classifyHookOutcome(success, signal?.aborted ?? false) }),
      );
      return '';
    }
    const outcome = { success: true };
    // Live progress streaming (the reference's emitHookProgress driven by
    // startHookProgressInterval): poll the hook's accumulating output while it
    // runs and publish each changed snapshot on the lifecycle channel, so a long
    // hook shows partial output instead of only a start + end. `live` is mutated
    // in place — by runCommandHookAsync for a command hook, and by the engine
    // sub-stream for a prompt/agent hook (the reference's execPromptHook /
    // execAgentHook live `setResponseLength` affordance while the model streams).
    // HTTP hooks are a single request/response with no incremental output, so
    // they keep the start+end-only lifecycle.
    const live = config.type === 'command' || config.type === 'prompt' || config.type === 'agent'
      ? { stdout: '', stderr: '' }
      : undefined;
    const stopStream = live
      ? this.streamHookProgress(progress, () => Promise.resolve({ stdout: live.stdout, stderr: live.stderr, output: live.stdout + live.stderr }))
      : undefined;
    try {
      const result = await this.runHookByType(config, cwd, eventData, signal, outcome, envFileHint, live);
      if (config.once && outcome.success) this.removeHookOnce(arr, fn, event);
      return result;
    } finally {
      stopStream?.();
      this.emit('hookResolved', { ...progress, success: outcome.success, outcome: classifyHookOutcome(outcome.success, signal?.aborted ?? false) });
    }
  }

  /** Build the {@link HookProgressEvent} payload for a hook about to run: the
   *  firing event name, the hook type, the author-facing display label
   *  (statusMessage when set, else the hook's sanitized content — the same
   *  getHookDisplayText the audit view uses, so progress + audit stay in sync),
   *  and the stable per-hook id/name ({@link computeHookId}) that correlates this
   *  hook's progress/streaming/resolved emissions (the reference hookId/hookName). */
  private hookProgressPayload(
    config: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
    event: string,
    index: number,
  ): HookProgressEvent {
    const label = getHookDisplayText({ statusMessage: config.statusMessage, content: describeHookContent(config) });
    return {
      event,
      hookType: config.type,
      label,
      hookId: computeHookId(event, config.type, describeHookContent(config), index),
      hookName: label,
    };
  }

  /** Wire the pure {@link startHookProgressInterval} to the lifecycle channel:
   *  poll `getOutput` on {@link HOOK_PROGRESS_INTERVAL_MS} and publish each changed
   *  snapshot as a streaming `progress` {@link HookLifecycleEvent} carrying the
   *  hook's id/name + incremental stdout/stderr/output. Honours the all-events gate
   *  (sessionStart/setup always stream; others only when enabled). Returns a stop
   *  closure. */
  private streamHookProgress(
    progress: HookProgressEvent,
    getOutput: () => Promise<{ stdout: string; stderr: string; output: string }>,
  ): () => void {
    return startHookProgressInterval({
      event: progress.event,
      allEnabled: this._allHookEventsEnabled,
      getOutput,
      onProgress: ({ stdout, stderr, output }) => this.publishHookLifecycle({
        phase: 'progress',
        event: progress.event,
        hookType: progress.hookType,
        label: progress.label,
        hookId: progress.hookId,
        hookName: progress.hookName,
        stdout,
        stderr,
        output,
      }),
    });
  }

  /** Await every in-flight async hook to settle (graceful shutdown). */
  async finalizePendingAsyncHooks(): Promise<void> {
    await Promise.all(Array.from(this._pendingAsync.keys()));
  }

  /** Cancel every in-flight async hook (hard shutdown) and reset the signal so
   *  subsequently-fired hooks are not born aborted. Returns the count cancelled. */
  cancelAllAsyncHooks(): number {
    const count = this._pendingAsync.size;
    this._asyncAbort.abort();
    this._asyncAbort = new AbortController();
    return count;
  }

  /** Dynamically-registered FileChanged watch paths contributed by hook output. */
  getWatchedPaths(): string[] { return Array.from(this._watchedPaths); }

  /**
   * Parse `watchPaths` from a hook's output, register any new absolute paths,
   * and emit a 'watchPaths' event so a file-watcher consumer can start watching
   * them. Returns the parsed paths (empty when none). Idempotent per path.
   */
  private registerWatchPaths(event: string, result: string): string[] {
    const paths = parseWatchPaths(result);
    if (paths.length === 0) return [];
    const added: string[] = [];
    for (const p of paths) {
      if (!this._watchedPaths.has(p)) { this._watchedPaths.add(p); added.push(p); }
    }
    if (added.length > 0) this.emit('watchPaths', { event, paths: added });
    return paths;
  }

  private buildHookEnv(event: string, toolName?: string, envFileHint?: { event: string; index: number }): Record<string, string> {
    const base: Record<string, string> = {
      ...sanitizeHookEnv(process.env as Record<string, string | undefined>),
      SUPERINFERENCE_PROJECT_DIR: this._cwd,
      SUPERINFERENCE_SESSION_ID: this._sessionId,
      SUPERINFERENCE_TOOL_NAME: toolName || '',
      SUPERINFERENCE_HOOK_EVENT: event,
      SUPERINFERENCE_CWD: this._cwd,
      SUPERINFERENCE_MODEL: this._providerConfig?.model ?? '',
      SUPERINFERENCE_PROVIDER: this._providerConfig?.provider ?? '',
      SUPERINFERENCE_EFFORT: this._effortLevel,
      // Session-environment handoff: a lifecycle hook can write an
      // `<event>-hook-<index>.sh` fragment (e.g. `export FOO=bar`,
      // `source .venv/bin/activate`) into this dir and it is folded into the
      // script prepended to subsequent bash commands, so hook-exported env
      // persists across stateless tool calls (the reference CLAUDE_ENV_FILE parity).
      SUPERINFERENCE_SESSION_ENV_DIR: sessionEnvDirPath(this._sessionId),
    };
    // Per-hook env-file path (the reference's hookIndex-derived CLAUDE_ENV_FILE):
    // an env-ingestible lifecycle hook (setup/sessionStart/cwdChanged/fileChanged)
    // gets the EXACT deterministic fragment path to append `export KEY=VAL` to,
    // so it does not have to reconstruct the `<event>-hook-<index>.sh` naming
    // convention itself. Re-ingested by getSessionEnvironmentScript for later bash
    // calls. The CLAUDE_ENV_FILE alias keeps cross-framework hooks working verbatim.
    if (envFileHint) {
      const envFile = hookEnvFragmentPath(this._sessionId, envFileHint.event, envFileHint.index);
      if (envFile) {
        // Ensure the session-env directory exists so the hook can append to the
        // handed path without first mkdir-ing it (a missing dir would make the
        // hook's `>> "$CLAUDE_ENV_FILE"` fail silently on the fail-open path).
        try { fs.mkdirSync(path.dirname(envFile), { recursive: true }); } catch { /* best-effort */ }
        base.SUPERINFERENCE_ENV_FILE = envFile;
        base.CLAUDE_ENV_FILE = envFile;
      }
    }
    return base;
  }

  /**
   * Resolve the current session's transcript (journal) file path so a verification
   * AGENT hook can be told where to read the prior conversation, and be granted a
   * scoped Read rule for exactly that file. Returns undefined when no session is
   * active (nothing to read) or the path cannot be resolved. Parity: the reference's
   * getTranscriptPath/getAgentTranscriptPath threaded into execAgentHook.
   */
  private resolveHookTranscriptPath(): string | undefined {
    if (!this._sessionId) return undefined;
    try {
      return hookJournalHooks.journalPathFor(this._cwd, this._sessionId);
    } catch {
      // A malformed cwd/session id (assertPathWithin reject) just means no scoped
      // transcript access — the agent hook still runs without it.
      return undefined;
    }
  }

  /**
   * Surface a hook EXECUTION failure (throw / hard-timeout / transport error /
   * non-2xx) as a visible `hookError` event so a crashed or misconfigured hook is
   * never silent on the fail-OPEN path — the reference always yields a
   * `hook_error_during_execution` attachment with the formatted error + hook name.
   * This is distinct from a hook's DENY *decision* (a successful run that chose to
   * block, already surfaced to the model). The engine forwards it to the operator.
   */
  private emitHookError(config: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig, error: string): void {
    this.emit('hookError', { hookType: config.type, hook: describeHookContent(config), error });
  }

  /**
   * Plugin provenance for a hook's rewake payload (the reference's per-hook
   * `pluginName`/`pluginId`): the installing plugin's name + stable id, derived
   * from the plugin root loadFromFile stamped onto the config. Empty object for a
   * filesystem/config hook (no plugin), so spreading it adds nothing. Lets the
   * engine attribute a backgrounded hook's `[hookRewake]` system message to its
   * plugin instead of an anonymous completion notice.
   */
  private rewakeAttribution(
    config: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
  ): { pluginName?: string; pluginId?: string } {
    const out: { pluginName?: string; pluginId?: string } = {};
    if (config.pluginName) out.pluginName = config.pluginName;
    const id = resolvePluginId(config.pluginRoot);
    if (id) out.pluginId = id;
    return out;
  }

  private postSamplingHooks: HookFn[] = [];
  private stopHooks: StopHookFn[] = [];
  /** Re-entrancy guard: a Stop hook (or its blocking-error re-entry) must not
   *  recursively trigger Stop hooks again (the reference's stopHookActive). */
  private _stopHookActive = false;
  private errorHooks: HookFn[] = [];
  private preToolUseHooks: PreToolUseHookFn[] = [];
  private postToolUseHooks: PostToolUseHookFn[] = [];
  private preSamplingHooks: PreSamplingHookFn[] = [];
  private sessionStartHooks: SessionHookFn[] = [];
  private sessionEndHooks: SessionHookFn[] = [];
  private subagentStartHooks: SubagentStartHookFn[] = [];
  private subagentStopHooks: SubagentHookFn[] = [];
  private preCompactHooks: PreCompactHookFn[] = [];
  private postCompactHooks: PostCompactHookFn[] = [];
  private permissionRequestHooks: PermissionHookFn[] = [];
  private permissionDeniedHooks: PermissionHookFn[] = [];
  private userPromptSubmitHooks: UserPromptHookFn[] = [];
  private taskCreatedHooks: TaskEventHookFn[] = [];
  private taskCompletedHooks: TaskCompletedHookFn[] = [];
  private teammateIdleHooks: TeammateIdleHookFn[] = [];
  private postToolUseFailureHooks: PostToolUseHookFn[] = [];
  private notificationHooks: NotificationHookFn[] = [];
  private setupHooks: SessionHookFn[] = [];
  private configChangeHooks: ConfigChangeHookFn[] = [];
  private instructionsLoadedHooks: InstructionsHookFn[] = [];
  private worktreeCreateHooks: WorktreeCreateHookFn[] = [];
  private worktreeRemoveHooks: WorktreeHookFn[] = [];
  private cwdChangedHooks: CwdChangedHookFn[] = [];
  private fileChangedHooks: FileChangeHookFn[] = [];
  private elicitationHooks: ElicitationHookFn[] = [];
  private elicitationResultHooks: ElicitationHookFn[] = [];
  private stopFailureHooks: StopFailureHookFn[] = [];

  /**
   * Load hooks from .ami/hooks.json.
   * Command hooks execute a shell command; exit code 0 = allow, non-zero = deny.
   */
  loadFromFile(cwd: string): void {
    // Remember the directory this load read from so a later no-arg
    // {@link reloadFromDisk}/{@link reloadPluginHooks} re-reads the SAME workspace
    // (the engine calls loadFromFile with config.cwd but does not setCwd first).
    this._cwd = cwd;
    // Reset the introspection descriptor set so a reload reflects the current
    // on-disk config (no stale accumulation across loads).
    this._descriptors = [];
    // Reset the parse-error set too, so a fixed file clears its prior error.
    this._configErrors = [];
    // Folder-trust grant (parity: the reference's checkHasTrustDialogAccepted): consult
    // the persisted `hasTrustDialogAccepted` project-config knob so a user who recorded
    // trust for this workspace actually gets project-tier hooks (and, interactively, all
    // hooks) loaded instead of silently skipped. Only PROMOTES trust — an explicitly-set
    // trusted flag is never demoted, and an unset/false knob keeps the untrusted default.
    if (!this._workspaceTrusted && resolveWorkspaceTrustAccepted(cwd)) {
      this._workspaceTrusted = true;
    }
    // Defense-in-depth: in an interactive session an untrusted workspace fires
    // NO hooks of any tier (the reference's shouldSkipHookDueToTrust). Project
    // entries are also gated below, but this blocks user/config-tier hooks too.
    if (shouldSkipHookDueToTrust(this._workspaceTrusted, this._interactive)) {
      coreLog('hooks', 'Skipping ALL hooks: workspace not trusted (interactive)');
      return;
    }
    // Kill-switch: a managed `disableAllHooks` policy OR the CLAUDE_CODE_SIMPLE
    // (AMI_SIMPLE / AMI_DISABLE_HOOKS) env switch skips EVERY hook, including
    // managed ones (the reference's shouldDisableAllHooksIncludingManaged).
    if (this.isAllHooksDisabled()) {
      coreLog('hooks', "Skipping ALL hooks: 'disableAllHooks' policy / CLAUDE_CODE_SIMPLE env is set");
      return;
    }
    const filePath = path.join(cwd, '.ami', 'hooks.json');
    const userHooksPath = path.join(os.homedir(), '.ami', 'hooks.json');

    // Each source carries a provenance `kind` (hook-protection.ts HookSourceKind)
    // that the managed-only policy filter consults. `managed` = admin/policy
    // settings; `plugin` = plugin-bundle contributed. Both are admin-trusted
    // sources that bypass the strictPluginOnlyCustomization hooks lock.
    const sources: Array<{ path: string; source: 'project' | 'user'; kind: HookSourceKind; pluginName?: string; pluginRoot?: string }> = [];
    // Admin/managed settings (highest authority) — the ONLY source honored when
    // allowManagedHooksOnly is set.
    const managedPath = process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    if (managedPath) sources.push({ path: managedPath, source: 'user', kind: 'managed' });
    sources.push(
      { path: filePath, source: 'project', kind: 'project' },
      // Per-directory LOCAL override tier: `.ami/settings.local.json` is the
      // developer's untracked, machine-local settings file (gitignored by
      // convention). It is read AFTER the shared project `.ami/hooks.json` so a
      // local entry layers on top with higher precedence — matching the reference's
      // `settings.local.json` `local` tier (addDirPluginSettings / settings-guard).
      { path: path.join(cwd, '.ami', 'settings.local.json'), source: 'project', kind: 'project' },
      { path: userHooksPath, source: 'user', kind: 'user' },
      // Claude Code compatibility: load hooks from .claude/settings.json
      // ({ "hooks": { "PreToolUse": [{ matcher, hooks: [...] }] } }). The
      // map-keyed `hooks` field is handled by normalizeMapKeyedHooks below, and
      // project-scoped entries are gated by workspace trust. (R2-M8)
      { path: path.join(cwd, '.claude', 'settings.json'), source: 'project', kind: 'project' },
      // the reference's per-directory local override: `.claude/settings.local.json`
      // is read right after `.claude/settings.json` (local-after-project), so a
      // developer's untracked local settings take precedence over the shared,
      // committed project settings — the three-layer user < project < local
      // precedence (updateConfig three-layer settings precedence).
      { path: path.join(cwd, '.claude', 'settings.local.json'), source: 'project', kind: 'project' },
      { path: path.join(os.homedir(), '.claude', 'settings.json'), source: 'user', kind: 'user' },
    );

    // Plugin bundles contribute hooks.json (project-scoped ones gated by trust).
    // Skipped entirely under allowManagedHooksOnly (only managed hooks run).
    if (canRunHookUnderManagedOnly('plugin', this._allowManagedHooksOnly)) {
      for (const s of this.discoverPluginHookSources(cwd)) sources.push(s);
    }

    // Org policy: when allowManagedHooksOnly is set, drop every non-managed source
    // so only admin/managed-settings hooks execute. The provenance-based decision
    // is owned by hook-protection.ts (filterManagedOnlyHooks), the single authority
    // for "may this source kind run under the policy".
    let effectiveSources = filterManagedOnlyHooks(sources, this._allowManagedHooksOnly);

    // Managed per-surface lock: `strictPluginOnlyCustomization` (admin/managed-only
    // policy) can confine the `hooks` surface to plugin- and managed-provided sources
    // only, dropping user/project filesystem hooks (.ami/.claude). Composes with
    // allowManagedHooksOnly (which is stricter — managed only). Parity: the reference's
    // strictPluginOnlyCustomization. The policy lives in the managed settings file.
    if (this.isHooksSurfaceRestricted()) {
      effectiveSources = effectiveSources.filter(s => s.kind === 'managed' || s.kind === 'plugin');
      coreLog('hooks', "strictPluginOnlyCustomization locks the 'hooks' surface: loading plugin/managed hooks only");
    }

    // Content-identity de-dup across overlapping sources: the same hook defined
    // in two places (e.g. .ami/hooks.json and .claude/settings.json) registers once.
    const accepted: Array<{ event: string; hook: HookConfigEntry['hook'] }> = [];
    const alreadyAccepted = (entry: HookConfigEntry): boolean =>
      accepted.some(a => a.event === entry.event && entry.hook && isHookEqual(a.hook, entry.hook));

    for (const { path: p, source, pluginName, pluginRoot } of effectiveSources) {
      let raw: string;
      try {
        raw = fs.readFileSync(p, 'utf-8');
      } catch {
        continue;
      }
      try {
        const config: HookConfigFile = JSON.parse(stripBom(raw));
        // Accept both AMI's array form and the reference's map-keyed form
        // ({ "PreToolUse": [{ matcher, hooks: [...] }] }).
        const entries = Array.isArray(config.hooks)
          ? config.hooks
          : normalizeMapKeyedHooks(config.hooks);
        if (!entries.length) continue;
        for (const entry of entries) {
          entry._source = source;
          // Carry the plugin bundle's name onto each entry it contributed so the
          // retained descriptor can attribute the hook to its plugin.
          if (pluginName) entry.pluginName = pluginName;
          // Carry the plugin bundle ROOT onto EVERY plugin hook (not just command
          // hooks) so a disabled/uninstalled plugin's hooks can be pruned by root at
          // runtime (gh-36995 — pruneRemovedPluginHooks / reloadPluginHooks).
          if (pluginRoot) entry.pluginRoot = pluginRoot;
          // Carry the plugin bundle's base directory onto a command hook so its
          // `${CLAUDE_PLUGIN_ROOT}` placeholders / CLAUDE_PLUGIN_ROOT env resolve to
          // the bundle root at execution time (the reference plugin-hook substitution).
          if (pluginRoot && entry.hook && entry.hook.type === 'command') entry.hook.pluginRoot = pluginRoot;
          // Carry plugin provenance (root + name) onto EVERY hook config type so a
          // backgrounded (asyncRewake) hook's completion/wake payload is attributable
          // to the plugin that installed it (the reference per-hook `pluginId`). The
          // runner only sees the config, so the attribution must live on it.
          if (entry.hook) {
            if (pluginRoot) entry.hook.pluginRoot = pluginRoot;
            if (pluginName) entry.hook.pluginName = pluginName;
          }
          if (source === 'project' && !this._workspaceTrusted) {
            coreLog('hooks', `Skipping project hook (workspace not trusted): ${entry.event}`);
            continue;
          }
          if (alreadyAccepted(entry)) {
            coreLog('hooks', `Skipping duplicate hook for event "${entry.event}"`);
            continue;
          }
          if (entry.hook) accepted.push({ event: entry.event, hook: entry.hook });
          this.processHookEntry(entry, cwd);
        }
      } catch (err) {
        this.recordConfigError(p, err);
      }
    }

    if (canRunHookUnderManagedOnly('config', this._allowManagedHooksOnly)) {
      const configPath = path.join(cwd, '.ami', 'config.json');
      try {
        if (fs.existsSync(configPath)) {
          const raw = fs.readFileSync(configPath, 'utf-8');
          const config = JSON.parse(stripBom(raw));
          if (config.hooks && Array.isArray(config.hooks)) {
            for (const entry of config.hooks) {
              entry._source = 'config';
              this.processHookEntry(entry, cwd);
            }
          }
        }
      } catch (err) {
        this.recordConfigError(configPath, err);
      }
    }

    // Hidden-hook-modification defense: fingerprint the executable hook surface
    // just loaded and compare it against the startup baseline. See
    // {@link updateHookConfigSnapshot}.
    this.updateHookConfigSnapshot(cwd);
  }

  /**
   * Discover the hooks.json sources contributed by currently-ENABLED plugin bundles
   * under the user (`~/.ami/plugins`) and project (`<cwd>/.ami/plugins`) plugin
   * trees. A bundle whose root carries a `.disabled` marker (or has no hooks file)
   * contributes nothing — so a disabled plugin's hooks are never re-registered on a
   * reload. Each returned source carries the bundle's name + root so a loaded hook
   * can be attributed to, and later pruned by, its plugin (gh-36995). Shared by
   * {@link loadFromFile} and {@link reloadPluginHooks} so the discovery rule has one
   * home. Never throws: a failed discovery (optional subsystem) logs a categorized
   * remediation hint and yields no plugin sources. Parity: the reference's
   * loadPluginHooks plugin enumeration.
   */
  private discoverPluginHookSources(
    cwd: string,
  ): Array<{ path: string; source: 'project' | 'user'; kind: HookSourceKind; pluginName: string; pluginRoot: string }> {
    const out: Array<{ path: string; source: 'project' | 'user'; kind: HookSourceKind; pluginName: string; pluginRoot: string }> = [];
    try {
      const pluginDirs: Array<[string, 'user' | 'project']> = [
        [path.join(resolveConfigDir(), 'plugins'), 'user'],
        [path.join(cwd, '.ami', 'plugins'), 'project'],
      ];
      for (const [dir, src] of pluginDirs) {
        for (const bundle of hookPluginDiscoverHooks.discoverPlugins(dir)) {
          // A disabled plugin must NOT contribute executable hooks: skip bundles
          // whose root carries a `.disabled` marker (mirrors skill-manager gating).
          if (fs.existsSync(path.join(bundle.root, '.disabled'))) {
            coreLog('hooks', `Skipping hooks from disabled plugin: ${bundle.root}`);
            continue;
          }
          if (bundle.hooksFile) out.push({ path: bundle.hooksFile, source: src, kind: 'plugin', pluginName: bundle.name, pluginRoot: bundle.root });
        }
      }
    } catch (err) {
      // Plugin discovery failed (optional subsystem) — don't crash the load, but
      // surface a CATEGORIZED remediation hint (network / permissions / config)
      // instead of swallowing it silently, so the user can tell WHY plugin hooks
      // did not load and HOW to fix it. Mirrors the reference's sessionStart
      // plugin-hook-load guidance.
      const { category, guidance } = categorizeLoadFailure(err);
      coreLog('hooks', `Failed to load plugin hooks (${category}): ${err instanceof Error ? err.message : String(err)}. ${guidance}`);
    }
    return out;
  }

  /**
   * Resolve the private per-event hook array that {@link processHookEntry}'s switch
   * registers a config/frontmatter hook into. Every lifecycle event maps 1:1 to one
   * array, so this is the inverse of that switch — used to (a) attribute a
   * just-registered hook fn to its originating source ({@link processHookEntry}) and
   * (b) surgically remove a plugin-contributed hook on prune/reload
   * ({@link removeConfigHook}). Returns `undefined` for an unknown event.
   */
  private configHookArray(event: string): Function[] | undefined {
    // Object map (not a `switch`) so this inverse never introduces duplicate
    // `case '<event>':` labels that a brittle source-scan test might slice on.
    const map: Record<string, Function[]> = {
      preToolUse: this.preToolUseHooks as unknown as Function[],
      postToolUse: this.postToolUseHooks as unknown as Function[],
      preSampling: this.preSamplingHooks as unknown as Function[],
      postSampling: this.postSamplingHooks as unknown as Function[],
      stop: this.stopHooks as unknown as Function[],
      error: this.errorHooks as unknown as Function[],
      sessionStart: this.sessionStartHooks as unknown as Function[],
      sessionEnd: this.sessionEndHooks as unknown as Function[],
      subagentStart: this.subagentStartHooks as unknown as Function[],
      subagentStop: this.subagentStopHooks as unknown as Function[],
      preCompact: this.preCompactHooks as unknown as Function[],
      postCompact: this.postCompactHooks as unknown as Function[],
      permissionRequest: this.permissionRequestHooks as unknown as Function[],
      permissionDenied: this.permissionDeniedHooks as unknown as Function[],
      userPromptSubmit: this.userPromptSubmitHooks as unknown as Function[],
      taskCreated: this.taskCreatedHooks as unknown as Function[],
      taskCompleted: this.taskCompletedHooks as unknown as Function[],
      postToolUseFailure: this.postToolUseFailureHooks as unknown as Function[],
      teammateIdle: this.teammateIdleHooks as unknown as Function[],
      notification: this.notificationHooks as unknown as Function[],
      setup: this.setupHooks as unknown as Function[],
      configChange: this.configChangeHooks as unknown as Function[],
      instructionsLoaded: this.instructionsLoadedHooks as unknown as Function[],
      worktreeCreate: this.worktreeCreateHooks as unknown as Function[],
      worktreeRemove: this.worktreeRemoveHooks as unknown as Function[],
      cwdChanged: this.cwdChangedHooks as unknown as Function[],
      fileChanged: this.fileChangedHooks as unknown as Function[],
      elicitation: this.elicitationHooks as unknown as Function[],
      elicitationResult: this.elicitationResultHooks as unknown as Function[],
      stopFailure: this.stopFailureHooks as unknown as Function[],
    };
    return Object.prototype.hasOwnProperty.call(map, event) ? map[event] : undefined;
  }

  /**
   * Remove one config-registered hook fn from its event array and drop its retained
   * introspection {@link HookDescriptor} + provenance. Returns true when the fn was
   * found and removed. The matching descriptor is removed by identity on
   * (event, type, matcher, content, pluginName) so {@link describeHooks} stays in
   * sync with the live hook set after a prune/reload.
   */
  private removeConfigHook(event: string, fn: Function): boolean {
    const arr = this.configHookArray(event);
    if (!arr) return false;
    const idx = arr.indexOf(fn);
    if (idx < 0) return false;
    arr.splice(idx, 1);
    const prov = this._hookProvenance.get(fn);
    this._hookProvenance.delete(fn);
    this._hookTiers.delete(fn);
    // Drop the first descriptor that matches this hook's (event, plugin) identity so
    // the introspection view reflects the removal (best-effort — descriptors carry no
    // fn handle, so match on provenance-derived fields).
    const di = this._descriptors.findIndex(d =>
      d.event === event && (prov?.pluginName ? d.pluginName === prov.pluginName : true));
    if (di >= 0) this._descriptors.splice(di, 1);
    return true;
  }

  /**
   * Remove every config-registered PLUGIN hook (atomic clear — the first half of
   * the reference's clear-then-register swap in loadPluginHooks). A hook qualifies when
   * its captured provenance carries a `pluginName`/`pluginRoot`; filesystem
   * (project/user/config) and runtime/frontmatter hooks are untouched. Returns the
   * number of hooks removed. Safe to call repeatedly (idempotent: a second call
   * removes nothing).
   */
  clearPluginHooks(): number {
    const removed = this.removeConfigHooksWhere(
      prov => prov.pluginName !== undefined || prov.pluginRoot !== undefined,
    );
    if (removed > 0) coreLog('hooks', `Cleared ${removed} plugin hook(s)`);
    return removed;
  }

  /**
   * Remove every config/file-registered hook whose captured provenance satisfies
   * `pred`, across all events. The single clear primitive shared by
   * {@link clearPluginHooks} (plugin-only predicate) and {@link clearConfigHooks}
   * (clear-all predicate) so neither re-implements the event-array walk + per-fn
   * provenance lookup. Runtime/engine-registered hooks carry NO provenance and are
   * therefore never matched (preserved). Returns the number removed.
   */
  private removeConfigHooksWhere(
    pred: (prov: { source: string; pluginName?: string; pluginRoot?: string }) => boolean,
  ): number {
    let removed = 0;
    for (const event of HOOK_EVENTS) {
      const arr = this.configHookArray(event);
      if (!arr) continue;
      for (const fn of [...arr]) {
        const prov = this._hookProvenance.get(fn);
        if (prov && pred(prov)) {
          if (this.removeConfigHook(event, fn)) removed++;
        }
      }
    }
    return removed;
  }

  /**
   * Remove EVERY file/config/plugin-sourced hook registration (anything
   * {@link processHookEntry} loaded from disk, identified by recorded provenance).
   * Runtime/engine-registered hooks (no provenance) are preserved. The clear half
   * of {@link reloadFromDisk}. Returns the number removed.
   */
  clearConfigHooks(): number {
    const removed = this.removeConfigHooksWhere(() => true);
    if (removed > 0) coreLog('hooks', `Cleared ${removed} config hook(s)`);
    return removed;
  }

  /**
   * Force a fresh mid-session re-read of the on-disk hook configuration (the reference
   * Code's updateHooksConfigSnapshot): clear every file/config/plugin-sourced hook
   * (via {@link clearConfigHooks}) then re-run {@link loadFromFile}, so an external
   * edit to `.ami/hooks.json` / `settings.json` made AFTER startup is picked up
   * without restarting the process. Runtime/engine-registered hooks are preserved.
   *
   * Unlike the reference, AMI's hook loader reads each source straight from disk with
   * `fs.readFileSync` on every load and keeps no settings cache, so this re-read
   * inherently reflects the current file contents and cannot be defeated by a file
   * watcher's stability-threshold debounce — the stale-cache race the upstream
   * `resetSettingsCache()` guards against does not exist here.
   *
   * Returns `{ cleared, registered }` (hooks removed, then re-registered from disk)
   * for the `/hooks refresh` summary. `cwd` defaults to the directory of the last
   * load (recorded by {@link loadFromFile}).
   */
  reloadFromDisk(cwd: string = this._cwd): { cleared: number; registered: number } {
    const cleared = this.clearConfigHooks();
    this.loadFromFile(cwd);
    let registered = 0;
    for (const event of HOOK_EVENTS) {
      const arr = this.configHookArray(event);
      if (!arr) continue;
      for (const fn of arr) if (this._hookProvenance.has(fn)) registered++;
    }
    coreLog('hooks', `reloadFromDisk: cleared ${cleared}, registered ${registered} config hook(s)`);
    return { cleared, registered };
  }

  /**
   * Immediately revoke the hooks of plugins that are no longer enabled (gh-36995):
   * drop every registered PLUGIN hook whose originating bundle root is NOT in
   * `enabledRoots`. Additions are deliberately NOT performed here — a newly enabled
   * plugin's hooks are picked up by the explicit {@link reloadPluginHooks}/`/reload-
   * plugins` pass — so a disable/uninstall takes effect at once while a trust-
   * sensitive add stays gated behind an explicit reload (the reference's prune-only
   * invalidation). A plugin hook with no recorded root is left in place (it cannot be
   * attributed to a root, so pruning it would be unsound). Returns the count removed.
   */
  pruneRemovedPluginHooks(enabledRoots: readonly string[]): number {
    const enabled = new Set(enabledRoots.map(r => path.resolve(r)));
    let removed = 0;
    for (const event of HOOK_EVENTS) {
      const arr = this.configHookArray(event);
      if (!arr) continue;
      for (const fn of [...arr]) {
        const prov = this._hookProvenance.get(fn);
        if (!prov?.pluginRoot) continue;
        if (!enabled.has(path.resolve(prov.pluginRoot))) {
          if (this.removeConfigHook(event, fn)) removed++;
        }
      }
    }
    if (removed > 0) coreLog('hooks', `Pruned ${removed} hook(s) from disabled/removed plugins`);
    return removed;
  }

  /**
   * Atomic clear-then-register swap for plugin hooks (the reference's loadPluginHooks
   * re-registration after a plugin management operation): remove ALL currently-
   * registered plugin hooks, then re-discover and re-register the hooks of every
   * still-enabled plugin bundle. Idempotent — re-running with no plugin changes
   * yields the same live set (a disabled plugin stays gone, an unchanged plugin is
   * registered exactly once). Returns a `{ cleared, registered }` breakdown for the
   * `/reload-plugins` summary. `cwd` defaults to the manager's session cwd.
   */
  reloadPluginHooks(cwd: string = this._cwd): { cleared: number; registered: number } {
    const cleared = this.clearPluginHooks();
    let registered = 0;
    if (!canRunHookUnderManagedOnly('plugin', this._allowManagedHooksOnly)) {
      coreLog('hooks', 'reloadPluginHooks: plugin hooks disabled by allowManagedHooksOnly policy');
      return { cleared, registered };
    }
    for (const { path: p, source, pluginName, pluginRoot } of this.discoverPluginHookSources(cwd)) {
      let raw: string;
      try {
        raw = fs.readFileSync(p, 'utf-8');
      } catch {
        continue;
      }
      try {
        const config: HookConfigFile = JSON.parse(stripBom(raw));
        const entries = Array.isArray(config.hooks) ? config.hooks : normalizeMapKeyedHooks(config.hooks);
        for (const entry of entries) {
          entry._source = source;
          entry.pluginName = pluginName;
          entry.pluginRoot = pluginRoot;
          if (entry.hook && entry.hook.type === 'command') entry.hook.pluginRoot = pluginRoot;
          if (source === 'project' && !this._workspaceTrusted) continue;
          this.processHookEntry(entry, cwd);
          registered++;
        }
      } catch (err) {
        this.recordConfigError(p, err);
      }
    }
    coreLog('hooks', `reloadPluginHooks: cleared ${cleared}, registered ${registered} plugin hook(s)`);
    return { cleared, registered };
  }

  /**
   * Deterministic, key-sorted fingerprint of every input that affects which plugin
   * hooks load: the discovered plugin hook sources (which plugins contribute hooks,
   * each tagged by source tier + plugin name + path) and the policy knobs that gate
   * them ({@link isAllowManagedHooksOnly}, {@link isAllHooksDisabled}, workspace
   * trust). Mirrors the reference's `getPluginAffectingSettingsSnapshot` — a stable
   * multi-field snapshot whose purpose is to decide whether a settings change
   * actually touches plugin hooks before paying for a clear+re-register. The
   * top-level keys AND the `sources` list are sorted so the string is
   * order-independent (two identical configs always produce the same fingerprint).
   */
  getPluginAffectingSettingsSnapshot(cwd: string = this._cwd): string {
    const sources = this.discoverPluginHookSources(cwd)
      .map((s) => {
        // Fold the file's mtime into the fingerprint so EDITING a plugin's hooks.json
        // in place (same path, changed content) also changes the snapshot — not just
        // adding/removing a plugin. An unreadable source contributes mtime 0.
        let mtimeMs = 0;
        try { mtimeMs = hookFsHooks.statSync(s.path).mtimeMs; } catch { /* unreadable → 0 */ }
        return `${s.source}:${s.pluginName ?? ''}:${s.path}:${mtimeMs}`;
      })
      .sort();
    const snap: Record<string, unknown> = {
      allowManagedHooksOnly: this._allowManagedHooksOnly,
      disableAllHooks: this.isAllHooksDisabled(),
      sources,
      workspaceTrusted: this._workspaceTrusted,
    };
    // Key-sorted serialization (the replacer fixes top-level key order); `sources`
    // is pre-sorted above, so the whole string is deterministic.
    return JSON.stringify(snap, Object.keys(snap).sort());
  }

  /**
   * Reload plugin hooks ONLY when the plugin-affecting settings actually changed
   * since the last gated reload — the automatic settings-change path (the reference's
   * loadPluginHooks subscriber). Takes the current
   * {@link getPluginAffectingSettingsSnapshot}; if it matches the stored one, logs
   * `Plugin hooks: skipping reload, plugin-affecting settings unchanged` and returns
   * `{cleared:0, registered:0, skipped:true}` (no clear+re-register churn). Otherwise
   * logs that it is reloading, runs {@link reloadPluginHooks}, stores the fresh
   * snapshot, and returns the counts with `skipped:false`. Unlike the unconditional
   * {@link reloadPluginHooks} (the explicit `/reload-plugins` force path), this is the
   * cheap "pick up an external settings edit" path safe to call on every refresh.
   */
  reloadPluginHooksIfChanged(cwd: string = this._cwd): { cleared: number; registered: number; skipped: boolean } {
    const snapshot = this.getPluginAffectingSettingsSnapshot(cwd);
    if (this._pluginSettingsSnapshot === snapshot) {
      coreLog('hooks', 'Plugin hooks: skipping reload, plugin-affecting settings unchanged');
      return { cleared: 0, registered: 0, skipped: true };
    }
    coreLog('hooks', 'Plugin hooks: reloading, plugin-affecting settings changed');
    const { cleared, registered } = this.reloadPluginHooks(cwd);
    // Re-snapshot AFTER the reload so a reload that itself changes the source set
    // (new plugin dir discovered) is reflected and does not re-trigger next time.
    this._pluginSettingsSnapshot = this.getPluginAffectingSettingsSnapshot(cwd);
    return { cleared, registered, skipped: false };
  }

  /**
   * Seed the plugin-affecting-settings baseline WITHOUT reloading, so the FIRST
   * subsequent {@link reloadPluginHooksIfChanged} compares against the real startup
   * state rather than `undefined` (which always reads as "changed" and forces a
   * spurious clear+re-register on the first unrelated settings write). Mirrors the reference
   * Code's setupPluginHookHotReload capturing `lastPluginSettingsSnapshot` at
   * subscribe time. Idempotent and side-effect-free apart from recording the baseline;
   * returns the captured snapshot so a caller/test can assert what was primed.
   */
  primePluginHookSnapshot(cwd: string = this._cwd): string {
    const snapshot = this.getPluginAffectingSettingsSnapshot(cwd);
    this._pluginSettingsSnapshot = snapshot;
    return snapshot;
  }

  /**
   * Capture (or re-capture) the startup hook-config snapshot and, on a reload at
   * the SAME cwd, record any hook entries that appeared or were mutated since the
   * baseline. Mirrors the reference's captureHooksConfigSnapshot /
   * updateHooksConfigSnapshot, whose purpose is "to avoid hidden hook
   * modifications": a settings file edited mid-session (e.g. via a
   * prompt-injected file edit or a tool call) cannot silently slip new
   * command-executing hooks past the startup audit baseline.
   *
   * Detection is non-blocking (the hooks still load) but surfaces via a `coreLog`
   * diagnostic and {@link getHiddenHookModifications} so an operator/`/hooks`
   * audit can see exactly which hooks changed after startup. A cwd change (worktree
   * chdir) re-baselines rather than flagging the new workspace's hooks.
   */
  private updateHookConfigSnapshot(cwd: string): void {
    if (this._hookSnapshotBaseline === undefined || this._hookSnapshotCwd !== cwd) {
      this._hookSnapshotBaseline = captureHookSnapshot(this._descriptors);
      this._hookSnapshotCwd = cwd;
      this._hiddenHookModifications = [];
      return;
    }
    const added = detectHiddenHookAdditions(this._hookSnapshotBaseline, this._descriptors);
    this._hiddenHookModifications = added.map((d) => ({ ...d }));
    for (const d of added) {
      coreLog(
        'hooks',
        `Hidden hook modification detected after startup: "${d.event}" hook from ${d.source} ` +
          `was not present in the startup snapshot (possible mid-session config tampering)`,
      );
    }
  }

  /**
   * Introspection: hook entries that appeared or were mutated since the startup
   * config snapshot (empty on a clean session). Returns copies. See
   * {@link updateHookConfigSnapshot}.
   */
  getHiddenHookModifications(): HookDescriptor[] {
    return this._hiddenHookModifications.map((d) => ({ ...d }));
  }

  /**
   * Record a settings/hooks/config file that failed to parse: log it, retain it
   * for {@link getConfigErrors}/`/hooks`, and emit a `configError` event so a live
   * consumer (e.g. a file-watcher reload) can surface it. A malformed file
   * silently disables EVERY hook it would contribute, so making it visible matters
   * (the reference's "silently disables ALL settings from that file" warning).
   */
  private recordConfigError(filePath: string, err: unknown): void {
    const base = err instanceof Error ? err.message : String(err);
    // Append a CATEGORIZED remediation hint (network / permissions / config) so the
    // retained error — surfaced via getConfigErrors()/`/hooks` and the `configError`
    // event — tells the user HOW to fix the malformed source, not just that it
    // failed. A broken hooks file silently disables every hook it would contribute,
    // so actionable guidance matters. Mirrors the reference's categorized load guidance.
    const { guidance } = categorizeLoadFailure(err);
    const message = `${base}. ${guidance}`;
    coreLog('hooks', `Invalid hooks config at ${filePath}: ${message}`);
    this._configErrors.push({ path: filePath, error: message });
    this.emit('configError', { path: filePath, error: message });
  }

  /**
   * Introspection: settings/hooks/config files that failed to parse during the
   * last {@link loadFromFile}. Returns copies so callers cannot mutate the set.
   * Surfaced via `/hooks` + `--introspect` (parity with the MCP manager's
   * getConfigErrors) so a broken config file is never a silent no-op.
   */
  getConfigErrors(): HookConfigError[] {
    return this._configErrors.map(e => ({ ...e }));
  }

  /** The most recent Stop-hook run summary (per-hook timings + errors), or null if
   *  no Stop hooks have run yet. Returns a deep copy (introspection surface). */
  getLastStopHookSummary(): StopHookSummary | null {
    const s = this._lastStopHookSummary;
    return s ? { ...s, hookInfos: s.hookInfos.map(h => ({ ...h })), hookErrors: [...s.hookErrors] } : null;
  }

  /** Operator-facing rendered summary of the most recent Stop-hook chain (count,
   *  wall-clock, slow/errored hooks, prevented continuation), or null when no Stop
   *  hooks have run or the last chain had nothing worth surfacing. Canonical consumer
   *  of {@link formatStopHookSummaryMessage}; introspection / diagnostic surface —
   *  never fed to the model. */
  getLastStopHookSummaryMessage(): string | null {
    if (!this._lastStopHookSummary) return null;
    const msg = formatStopHookSummaryMessage(this._lastStopHookSummary);
    return msg.length > 0 ? msg : null;
  }

  /**
   * Register a set of already-parsed {@link HookConfigEntry}s scoped to THIS engine
   * instance (parity with the reference's registerFrontmatterHooks, which installs a
   * skill/agent's own frontmatter hooks for the duration of its invocation). The
   * forked-skill / spawned-subagent sub-engine is a fresh short-lived engine, so a
   * hook registered here lives exactly as long as that engine — the invocation-scoped
   * lifecycle boundary — and is torn down naturally when the sub-engine shuts down
   * (no dangling global hook).
   *
   * The same managed kill-switches as {@link loadFromFile} apply: `disableAllHooks`
   * skips everything, and under `allowManagedHooksOnly` these author-sourced (non-
   * managed) hooks do NOT register. Each entry is registered through the shared
   * {@link processHookEntry} path (so event-protection tiers, introspection
   * descriptors, and matcher gating all behave identically to file-loaded hooks),
   * defaulting the provenance tier to `project` when the caller left `_source` unset.
   */
  registerConfigEntries(entries: readonly HookConfigEntry[], cwd: string, options: FrontmatterHookOptions = {}): void {
    if (!entries || entries.length === 0) return;
    if (this.isAllHooksDisabled()) {
      coreLog('hooks', 'Skipping frontmatter hooks: disableAllHooks policy / env is set');
      return;
    }
    if (this._allowManagedHooksOnly) {
      coreLog('hooks', 'Skipping frontmatter hooks: allowManagedHooksOnly policy is set');
      return;
    }
    const sourceName = options.sourceName;
    const srcSuffix = sourceName ? ` from ${sourceName}` : '';
    // Per-item fail-closed admin-trust gate (the reference's
    // `allowed = !isRestrictedToPluginOnly(surface) || isSourceAdminTrusted(item.source)`
    // used at frontmatter/bundle registration). The filesystem settings loader runs
    // its source-level lock in loadFromFile, but frontmatter hooks arrive from a
    // skill/agent/plugin bundle at invocation time, AFTER that gate — so when the
    // `hooks` surface is locked, each entry is admitted ONLY if it is admin-trusted:
    // a bundled/plugin-provided hook (admin-trusted source OR carries plugin
    // provenance) passes; a user/project-authored skill/agent hook (no provenance,
    // source undefined/project) is fail-closed BLOCKED. Computed once per batch.
    const surfaceLocked = this.isHooksSurfaceRestricted();
    let registered = 0;
    for (const raw of entries) {
      if (!raw || typeof raw.event !== 'string' || !raw.hook) continue;
      // Admin-trust gate: when the `hooks` surface is locked, drop any entry that is
      // neither from an admin-trusted source nor carries plugin provenance.
      if (surfaceLocked && !isSourceAdminTrusted(raw._source) && !raw.pluginRoot && !raw.pluginName && !(raw.hook && (raw.hook.pluginRoot || raw.hook.pluginName))) {
        coreLog('hooks', `strictPluginOnlyCustomization: skipping untrusted frontmatter hook (${raw.event})${srcSuffix}`);
        continue;
      }
      // isAgent remap (the reference registerFrontmatterHooks isAgent=true): a subagent
      // completing fires SubagentStop, NOT Stop — so an agent's frontmatter `stop` hook
      // would never run if left as-is. Convert it to `subagentStop` so the hook the
      // agent declared actually fires on that agent's completion. Skills (isAgent=false)
      // keep `stop` unchanged.
      let event = raw.event;
      if (options.isAgent && event === 'stop') {
        event = 'subagentStop';
        coreLog('hooks', `Converting Stop hook to SubagentStop${srcSuffix} (subagents trigger SubagentStop)`);
      }
      const entry: HookConfigEntry = { ...raw, event, _source: raw._source ?? 'project' };
      this.processHookEntry(entry, cwd);
      registered++;
    }
    if (registered > 0) {
      coreLog('hooks', `Registered ${registered} frontmatter hook(s)${srcSuffix}`);
    }
  }

  private processHookEntry(entry: HookConfigEntry, cwd: string): void {
        const hookConfig = entry.hook;
        if (!hookConfig) return;
        const matcher = entry.matcher;
        const tier = entry._source || 'user';

        // Gap 15: Block user-tier hooks on protected events. Log the ACTIONABLE
        // reason (names the event, explains why it is protected, states which
        // tiers may register it) rather than a terse one-liner.
        if (!canHookEvent(entry.event, tier as HookTier)) {
          coreLog('hooks', describeProtectedEventBlock(entry.event, tier as HookTier, describeHookContent(hookConfig)));
          return;
        }

        // Retain an introspection descriptor for every accepted (unblocked)
        // file-loaded hook so /hooks + --introspect can reveal its actual
        // command/prompt/url, matcher, and source tier.
        this._descriptors.push({
          event: entry.event,
          type: hookConfig.type,
          ...(matcher ? { matcher } : {}),
          source: tier,
          content: describeHookContent(hookConfig),
          ...(entry.pluginName ? { pluginName: entry.pluginName } : {}),
          ...(hookConfig.statusMessage ? { statusMessage: hookConfig.statusMessage } : {}),
        });

        switch (entry.event) {
        case 'preToolUse': {
          const fn: PreToolUseHookFn = async (ctx) => {
            if (hookConfig.if && !matchesHookIf(hookConfig.if, ctx.toolName, ctx.toolInput)) return { action: 'allow' };
            if (matcher && !matchesPattern(matcher, ctx.toolName)) return { action: 'allow' };
            const eventData = serializeHookEvent({
              event: 'preToolUse',
              toolName: ctx.toolName,
              toolInput: ctx.toolInput,
              // A one-line human summary of the pending call + the originating user
              // prompt give a guardrail hook the same context the permission UI has
              // (the reference's getToolUseSummary + requestPrompt).
              toolUseSummary: buildToolUseSummary(ctx.toolName, ctx.toolInput),
              ...(ctx.requestPrompt ? { requestPrompt: ctx.requestPrompt } : {}),
            });
            const result = await this.runArmHook(hookConfig, cwd, eventData, ctx.abortSignal, this.preToolUseHooks, fn, 'preToolUse');
            if (result.startsWith('DENY:')) return { action: 'deny', reason: result.slice(5).trim() };
            // ASK: a PreToolUse hook (decision/permissionDecision 'ask') forces an
            // interactive permission prompt rather than allowing/denying outright.
            // The sentinel carries the optional reason to show at the prompt; a
            // MODIFY-style updatedInput can also travel with it when the hook wants
            // the user to approve a rewritten input (the reference hookPermissionResult
            // { behavior:'ask', updatedInput, message }).
            if (result.startsWith('ASK:')) return { action: 'ask', reason: result.slice(4).trim() || undefined };
            if (result.startsWith('MODIFY:')) {
              try {
                const parsed = JSON.parse(result.slice(7));
                if (parsed.additionalContext && !parsed.updatedInput && Object.keys(parsed).length === 1) {
                  return { action: 'allow', additionalContext: parsed.additionalContext };
                }
                return { action: 'modify', updatedInput: parsed.updatedInput ?? parsed, additionalContext: parsed.additionalContext };
              } catch { /* ignore parse errors */ }
            }
            if (result.startsWith('SYSTEM:')) return { action: 'allow', additionalContext: result.slice(7).trim() };
            // A PreToolUse hook that EXPLICITLY approves (the reference's
            // `permissionDecision: 'allow'` / `decision: 'approve'`) PRE-APPROVES the
            // call: the decision-less JSON flows through runHookByType verbatim, so
            // recover it here as an allow carrying `permissionBypass` (the consumer
            // bypasses a would-be permission prompt). A bare/plain-text result is the
            // default no-objection allow (no bypass).
            const preApproveRaw = tryParseHookJson(result);
            const preApprove = preApproveRaw ? unwrapHookSpecificOutput(preApproveRaw) : undefined;
            if (preApprove && (preApprove.permissionDecision === 'allow' || preApprove.decision === 'approve')) {
              return {
                action: 'allow',
                permissionBypass: true,
                additionalContext: typeof preApprove.additionalContext === 'string' ? preApprove.additionalContext : undefined,
              };
            }
            return { action: 'allow' };
          };
          this.onPreToolUse(fn, tier);
          break;
        }
        case 'postToolUse': {
          const fn: PostToolUseHookFn = async (ctx) => {
            if (hookConfig.if && !matchesHookIf(hookConfig.if, ctx.toolName, ctx.toolInput)) return;
            if (matcher && !matchesPattern(matcher, ctx.toolName)) return;
            const eventData = serializeHookEvent({ event: 'postToolUse', toolName: ctx.toolName, toolInput: ctx.toolInput, toolOutput: ctx.toolOutput, isError: ctx.isError, duration_ms: ctx.durationMs });
            const result = await this.runArmHook(hookConfig, cwd, eventData, ctx.abortSignal, this.postToolUseHooks, fn, 'postToolUse');
            // A PostToolUse hook may block (DENY / exit-2) or request a stop
            // (continue:false) — both surface via the DENY sentinel; feed the
            // reason back to the model as a blocking error (the reference parity).
            const blockingError = extractHookBlockingError(result);
            if (blockingError) return { blockingError };
            const parsed = tryParseHookJson(result);
            if (parsed) {
              const out: PostToolUseHookResult = {};
              if (typeof parsed.additionalContext === 'string') out.additionalContext = parsed.additionalContext;
              if (typeof parsed.updatedToolOutput === 'string') out.updatedToolOutput = parsed.updatedToolOutput;
              // the reference's PostToolUse hooks rewrite MCP tool output via the
              // `updatedMCPToolOutput` key (typed `unknown`). Accept it as an alias
              // of updatedToolOutput — updatedToolOutput wins if both are present;
              // a non-string value is JSON-serialized into the string channel.
              else if (parsed.updatedMCPToolOutput !== undefined) {
                out.updatedToolOutput = typeof parsed.updatedMCPToolOutput === 'string'
                  ? parsed.updatedMCPToolOutput
                  : JSON.stringify(parsed.updatedMCPToolOutput);
              }
              if (parsed.continue === false) { out.preventContinuation = true; if (typeof parsed.stopReason === 'string') out.stopReason = parsed.stopReason; }
              if (out.additionalContext || out.updatedToolOutput || out.preventContinuation) return out;
            }
            return undefined;
          };
          this.onPostToolUse(fn);
          break;
        }
        case 'preSampling': {
          const fn: PreSamplingHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('preSampling', matcher, ctx as unknown as Record<string, unknown>)) return { action: 'allow' };
            const eventData = serializeHookEvent({ event: 'preSampling', samplingOrigin: ctx.samplingOrigin, isIdle: !hasPendingToolCalls(ctx.messages) });
            const result = await this.runArmHook(hookConfig, cwd, eventData, ctx.abortSignal, this.preSamplingHooks, fn, 'preSampling');
            if (result.startsWith('DENY:')) return { action: 'deny', reason: result.slice(5).trim() };
            return { action: 'allow' };
          };
          this.onPreSampling(fn, tier);
          break;
        }
        case 'postSampling': {
          const fn: HookFn = async (ctx) => {
            if (!matcherAppliesToCtx('postSampling', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({
              event: 'postSampling',
              samplingOrigin: ctx.samplingOrigin,
              isIdle: !hasPendingToolCalls(ctx.messages),
              ...replHookContextFields(ctx),
            });
            await this.runArmHook(hookConfig, cwd, eventData, ctx.abortSignal, this.postSamplingHooks, fn, 'postSampling');
          };
          this.onPostSampling(fn);
          break;
        }
        case 'stop': {
          const fn: StopHookFn = async (ctx) => {
            // the reference parity: a Stop hook reads `stop_hook_active` (to break a
            // block→retry recursion) and `last_assistant_message` (the final model
            // text) from its stdin without reading the transcript file.
            const lastText = ctx.lastAssistantMessage?.content?.trim() || undefined;
            const eventData = serializeHookEvent({
              event: 'stop',
              stop_hook_active: ctx.stopHookActive ?? false,
              ...(lastText ? { last_assistant_message: lastText } : {}),
              ...replHookContextFields(ctx),
            });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.stopHooks, fn, 'stop');
            const blockingError = extractHookBlockingError(result);
            if (blockingError) return { blockingError };
            return undefined;
          };
          this.onStop(fn, stopHookDisplayFromConfig(hookConfig));
          break;
        }
        case 'error': {
          const fn: HookFn = async () => {
            const eventData = serializeHookEvent({ event: 'error' });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.errorHooks, fn, 'error');
          };
          this.onError(fn);
          break;
        }
        case 'sessionStart': {
          const fn: SessionHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('sessionStart', matcher, ctx as unknown as Record<string, unknown>)) return;
            // `model` falls back to the session's configured model (the reference
            // includes it in the SessionStart payload); `agent_type` is supplied
            // by the engine when a non-default agent is active.
            const eventData = serializeHookEvent({ event: 'sessionStart', sessionId: ctx.sessionId, source: ctx.source, model: ctx.model ?? this._providerConfig?.model, agentType: ctx.agentType });
            // Thread the session lifecycle signal so a spawned SessionStart command
            // hook is killed when the session-start deadline/cancel fires (the reference
            // Code SessionContext signal parity).
            const result = await this.runArmHook(hookConfig, cwd, eventData, ctx.signal, this.sessionStartHooks, fn, 'sessionStart', ctx.forceSyncExecution ?? false);
            const watchPaths = this.registerWatchPaths('sessionStart', result);
            // the reference accumulates `additionalContexts[]` from every SessionStart
            // hook into a single hook_additional_context attachment — carry them on
            // every return branch so executeSessionStart can aggregate them.
            const additionalContexts = extractSessionAdditionalContexts(result);
            const base = additionalContexts.length > 0 ? { additionalContexts, watchPaths } : { watchPaths };
            try {
              const parsed = JSON.parse(result);
              // the reference's SessionStart hook result is an ARRAY of injectable
              // messages; accept `initialUserMessages: string[]` as well as the
              // single `initialUserMessage` / `systemMessage` forms.
              if (Array.isArray(parsed.initialUserMessages)) {
                const msgs = parsed.initialUserMessages.filter((m: unknown): m is string => typeof m === 'string' && m.length > 0);
                if (msgs.length > 0) return { ...base, initialUserMessages: msgs };
              }
              if (parsed.initialUserMessage) return { ...base, initialUserMessage: parsed.initialUserMessage };
              if (parsed.systemMessage) return { ...base, initialUserMessage: parsed.systemMessage };
            } catch {}
            if (result.startsWith('SYSTEM:')) return { ...base, initialUserMessage: result.slice(7).trim() };
            if (additionalContexts.length > 0 || watchPaths.length > 0) return base;
            return undefined;
          };
          this.onSessionStart(fn);
          break;
        }
        case 'sessionEnd': {
          const fn: SessionHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('sessionEnd', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'sessionEnd', sessionId: ctx.sessionId, reason: ctx.reason });
            // SessionEnd runs during shutdown: kill a slow command hook at the
            // same tight budget that bounds the overall executeSessionEnd run,
            // so a lingering child process cannot outlive teardown. When the caller
            // supplied an explicit lifecycle signal (an earlier /clear cancel), the
            // child is torn down on EITHER the caller's cancel OR the budget deadline
            // (the reference SessionContext signal parity).
            const budgetSignal = AbortSignal.timeout(getSessionEndHookTimeoutMs());
            const endSignal = ctx.signal ? AbortSignal.any([ctx.signal, budgetSignal]) : budgetSignal;
            await this.runArmHook(hookConfig, cwd, eventData, endSignal, this.sessionEndHooks, fn, 'sessionEnd');
          };
          this.onSessionEnd(fn);
          break;
        }
        case 'subagentStart': {
          const fn: SubagentStartHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('subagentStart', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'subagentStart', prompt: ctx.prompt });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.subagentStartHooks, fn, 'subagentStart');
            const contexts = extractAdditionalContexts(result);
            if (contexts.length > 0) return { additionalContexts: contexts };
            return undefined;
          };
          this.onSubagentStart(fn);
          break;
        }
        case 'subagentStop': {
          const fn: SubagentHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('subagentStop', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'subagentStop', subagentSessionId: ctx.subagentSessionId });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.subagentStopHooks, fn, 'subagentStop');
          };
          this.onSubagentStop(fn);
          break;
        }
        case 'preCompact': {
          const fn: PreCompactHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('preCompact', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'preCompact', messageCount: ctx.messageCount, trigger: ctx.trigger, customInstructions: ctx.customInstructions ?? undefined });
            // Thread the compaction's abort signal so a spawned PreCompact command
            // hook is killed the instant the user interrupts the compaction, rather
            // than lingering until its own timeout (the reference CompactContext signal).
            const result = await this.runArmHook(hookConfig, cwd, eventData, ctx.signal, this.preCompactHooks, fn, 'preCompact');
            const parsed = tryParseHookJson(result);
            if (parsed) {
              const out: { newCustomInstructions?: string; userDisplayMessage?: string } = {};
              if (typeof parsed.newCustomInstructions === 'string') out.newCustomInstructions = parsed.newCustomInstructions;
              if (typeof parsed.userDisplayMessage === 'string') out.userDisplayMessage = parsed.userDisplayMessage;
              if (out.newCustomInstructions || out.userDisplayMessage) return out;
            }
            return undefined;
          };
          this.onPreCompact(fn);
          break;
        }
        case 'postCompact': {
          const fn: PostCompactHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('postCompact', matcher, ctx as unknown as Record<string, unknown>)) return;
            // Hand the PostCompact hook the compaction `trigger` and the generated
            // `compactSummary` (the reference's executePostCompactHooks input) so a
            // post-compaction automation can read/log/annotate the summary, and let
            // it surface a `userDisplayMessage` back to the user.
            const eventData = serializeHookEvent({ event: 'postCompact', messageCount: ctx.messageCount, trigger: ctx.trigger, compactSummary: ctx.compactSummary });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.postCompactHooks, fn, 'postCompact');
            const parsed = tryParseHookJson(result);
            if (parsed && typeof parsed.userDisplayMessage === 'string' && parsed.userDisplayMessage) {
              return { userDisplayMessage: parsed.userDisplayMessage };
            }
            return undefined;
          };
          this.onPostCompact(fn);
          break;
        }
        case 'permissionRequest': {
          const fn: PermissionHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('permissionRequest', matcher, ctx as unknown as Record<string, unknown>)) return;
            // Surface the active posture + already-suggested rule updates to the
            // hook (the reference's permissionMode + suggestions params), and thread the
            // turn abort signal so a slow command hook is cancelled on interrupt.
            const eventData = serializeHookEvent({ event: 'permissionRequest', toolName: ctx.toolName, toolInput: ctx.toolInput, permissionMode: ctx.permissionMode, suggestions: ctx.suggestions, toolUseId: ctx.toolUseId });
            const result = await this.runArmHook(hookConfig, cwd, eventData, ctx.abortSignal, this.permissionRequestHooks, fn, 'permissionRequest');
            if (result.startsWith('DENY:')) return { action: 'deny' as const, reason: result.slice(5).trim() };
            if (result === 'ALLOW' || result.startsWith('ALLOW:')) return { action: 'allow' as const };
            // A command hook emitting `{permissionDecision:'allow', updatedInput}`
            // is flattened by runHookByType into the generic `MODIFY:<json>`
            // sentinel; recover it as an ALLOW that rewrites the tool input
            // (the reference's decision.updatedInput on allow). An additionalContext-only
            // MODIFY carries no rewrite, so allow without touching the input.
            if (result.startsWith('MODIFY:')) {
              const out: PermissionHookResult = { action: 'allow' as const };
              try {
                const payload = JSON.parse(result.slice(7));
                if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
                  const keys = Object.keys(payload);
                  if (!(keys.length === 1 && keys[0] === 'additionalContext')) out.updatedInput = payload as Record<string, unknown>;
                }
              } catch { /* non-JSON MODIFY payload → bare allow */ }
              return out;
            }
            try {
              const parsed: HookDecisionOutput = JSON.parse(result);
              if (parsed.permissionDecision === 'allow' || parsed.decision === 'approve') {
                // An ALLOW may rewrite the tool input and/or carry durable
                // permission-rule mutations the engine persists (the reference's
                // decision.updatedInput / decision.updatedPermissions).
                const out: PermissionHookResult = { action: 'allow' as const };
                if (parsed.updatedInput && typeof parsed.updatedInput === 'object' && !Array.isArray(parsed.updatedInput)) out.updatedInput = parsed.updatedInput;
                if (Array.isArray(parsed.updatedPermissions) && parsed.updatedPermissions.length > 0) out.updatedPermissions = parsed.updatedPermissions as PermissionUpdate[];
                if (isHookDecisionClassification(parsed.decisionClassification)) out.decisionClassification = parsed.decisionClassification;
                return out;
              }
              if (parsed.permissionDecision === 'deny' || parsed.decision === 'deny' || parsed.decision === 'block') {
                // A DENY may set `interrupt` to abort the whole turn rather than
                // just skipping this one tool (the reference's decision.interrupt).
                const out: PermissionHookResult = { action: 'deny' as const, reason: parsed.reason ?? 'Hook denied' };
                if (parsed.interrupt === true) out.interrupt = true;
                if (isHookDecisionClassification(parsed.decisionClassification)) out.decisionClassification = parsed.decisionClassification;
                return out;
              }
              // Strict rejection (the reference parseHookOutput): an unrecognized
              // decision/permissionDecision value is a validation error, not a
              // silent no-op. Fail CLOSED so a typo can't let a tool through.
              const invalid = unknownHookDecisionError(parsed as Record<string, unknown>);
              if (invalid) { coreLog('hooks', invalid); return { action: 'deny' as const, reason: invalid }; }
            } catch {}
            return undefined;
          };
          this.onPermissionRequest(fn);
          break;
        }
        case 'permissionDenied': {
          const fn: PermissionHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('permissionDenied', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'permissionDenied', toolName: ctx.toolName, toolUseId: ctx.toolUseId });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.permissionDeniedHooks, fn, 'permissionDenied');
            // the reference's PermissionDenied hook may grant a RETRY on exit 0 via
            // `{"hookSpecificOutput":{"hookEventName":"PermissionDenied","retry":true}}`
            // (or a flat top-level `retry:true`). Flatten the envelope and, when the
            // hook signalled retry, return the `allow` decision that
            // executePermissionDenied maps to `{ retry: true }` — so a stock
            // CC-format command hook can tell the model to retry the denied call.
            const parsed = tryParseHookJson(result);
            if (parsed) {
              const flat = unwrapHookSpecificOutput(parsed) as Record<string, unknown>;
              if (flat.retry === true) return { action: 'allow' };
            }
            return undefined;
          };
          this.onPermissionDenied(fn);
          break;
        }
        case 'userPromptSubmit': {
          const fn: UserPromptHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'userPromptSubmit', turnCount: ctx.turnCount, prompt: ctx.prompt });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.userPromptSubmitHooks, fn, 'userPromptSubmit');
            // A UserPromptSubmit hook can BLOCK the prompt (DENY / exit-2) or
            // request a stop (continue:false) — feed the reason back (the reference parity).
            const blockingError = extractHookBlockingError(result);
            if (blockingError) return { blockingError };
            const parsed = tryParseHookJson(result);
            if (parsed) {
              const out: UserPromptSubmitHookResult = {};
              if (typeof parsed.additionalContext === 'string') out.additionalContext = parsed.additionalContext;
              if (parsed.continue === false) { out.preventContinuation = true; if (typeof parsed.stopReason === 'string') out.stopReason = parsed.stopReason; }
              // A UserPromptSubmit hook can ALSO warn the human via `systemMessage`
              // (the reference's user-facing warning channel) — carried separately so
              // the engine surfaces it to the user WITHOUT injecting it into model
              // context the way `additionalContext` is.
              const sysMsgs = extractSystemMessages(result);
              if (sysMsgs.length > 0) out.systemMessage = sysMsgs.join('\n\n');
              if (out.additionalContext || out.preventContinuation || out.systemMessage) return out;
            }
            return undefined;
          };
          this.onUserPromptSubmit(fn);
          break;
        }
        case 'taskCreated': {
          const fn: TaskEventHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'taskCreated', taskId: ctx.taskId, subject: ctx.subject });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.taskCreatedHooks, fn, 'taskCreated');
            // A blocking taskCreated hook VETOES creation (the task tool rolls back).
            const blockingError = extractHookBlockingError(result);
            if (blockingError) return { blockingError };
            return undefined;
          };
          this.onTaskCreated(fn);
          break;
        }
        case 'taskCompleted': {
          const fn: TaskCompletedHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'taskCompleted', taskId: ctx.taskId, subject: ctx.subject, status: ctx.status });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.taskCompletedHooks, fn, 'taskCompleted');
            // A blocking TaskCompleted hook vetoes the status→completed transition.
            const blockingError = extractHookBlockingError(result);
            if (blockingError) return { blockingError };
            return undefined;
          };
          this.onTaskCompleted(fn);
          break;
        }
        case 'postToolUseFailure': {
          const fn: PostToolUseHookFn = async (ctx) => {
            if (hookConfig.if && !matchesHookIf(hookConfig.if, ctx.toolName, ctx.toolInput)) return;
            if (matcher && !matchesPattern(matcher, ctx.toolName)) return;
            const eventData = serializeHookEvent({ event: 'postToolUseFailure', toolName: ctx.toolName, isError: ctx.isError, duration_ms: ctx.durationMs, error: ctx.error, isInterrupt: ctx.isInterrupt, toolUseId: ctx.toolUseId });
            await this.runArmHook(hookConfig, cwd, eventData, ctx.abortSignal, this.postToolUseFailureHooks, fn, 'postToolUseFailure');
          };
          this.onPostToolUseFailure(fn);
          break;
        }
        case 'teammateIdle': {
          const fn: TeammateIdleHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('teammateIdle', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'teammateIdle', teammateName: ctx.teammateName, teamName: ctx.teamName });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.teammateIdleHooks, fn, 'teammateIdle');
          };
          this.onTeammateIdle(fn);
          break;
        }
        case 'notification': {
          const fn: NotificationHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('notification', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'notification', message: ctx.message, title: ctx.title, notificationType: ctx.notificationType });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.notificationHooks, fn, 'notification');
          };
          this.onNotification(fn);
          break;
        }
        case 'setup': {
          const fn: SessionHookFn = async (ctx) => {
            // Scope on the Setup `trigger` (init | maintenance) when a matcher is set.
            if (!matcherAppliesToCtx('setup', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'setup', sessionId: ctx.sessionId, cwd: ctx.cwd, trigger: ctx.trigger });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.setupHooks, fn, 'setup', ctx.forceSyncExecution ?? false);
            // Setup hooks, like SessionStart, may contribute model-facing
            // additionalContexts accumulated into one hook_additional_context
            // attachment (the reference's Setup additionalContexts aggregation).
            const additionalContexts = extractSessionAdditionalContexts(result);
            if (additionalContexts.length > 0) return { additionalContexts };
            return undefined;
          };
          this.onSetup(fn);
          break;
        }
        case 'configChange': {
          const fn: ConfigChangeHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('configChange', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'configChange', source: ctx.source, filePath: ctx.filePath });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.configChangeHooks, fn, 'configChange');
            const blockingError = extractHookBlockingError(result);
            if (blockingError) return { blockingError };
            return undefined;
          };
          this.onConfigChange(fn);
          break;
        }
        case 'instructionsLoaded': {
          const fn: InstructionsHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('instructionsLoaded', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'instructionsLoaded', filePath: ctx.filePath, loadReason: ctx.loadReason, memoryType: ctx.memoryType });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.instructionsLoadedHooks, fn, 'instructionsLoaded');
          };
          this.onInstructionsLoaded(fn);
          break;
        }
        case 'worktreeCreate': {
          const fn: WorktreeCreateHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'worktreeCreate', name: ctx.name, worktreePath: ctx.worktreePath });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.worktreeCreateHooks, fn, 'worktreeCreate');
            // A command/http WorktreeCreate hook acts as a provider: the path it
            // emits (stdout or hookSpecificOutput.worktreePath) overrides the
            // default worktree location (the reference parity).
            const worktreePath = extractWorktreePath(result);
            if (worktreePath) return { worktreePath };
            return undefined;
          };
          this.onWorktreeCreate(fn);
          break;
        }
        case 'worktreeRemove': {
          const fn: WorktreeHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'worktreeRemove', name: ctx.name, worktreePath: ctx.worktreePath });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.worktreeRemoveHooks, fn, 'worktreeRemove');
          };
          this.onWorktreeRemove(fn);
          break;
        }
        case 'cwdChanged': {
          const fn: CwdChangedHookFn = async (ctx) => {
            const eventData = serializeHookEvent({ event: 'cwdChanged', oldCwd: ctx.oldCwd, newCwd: ctx.newCwd });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.cwdChangedHooks, fn, 'cwdChanged');
            // Surface any USER-facing systemMessage the hook emitted (the reference's
            // fileChangedWatcher forwards each systemMessage via notifyCallback(msg,false)).
            for (const msg of extractSystemMessages(result)) this.emitHookNotify('cwdChanged', msg, false);
            const watchPaths = this.registerWatchPaths('cwdChanged', result);
            if (watchPaths.length > 0) return { watchPaths };
            return undefined;
          };
          this.onCwdChanged(fn);
          break;
        }
        case 'fileChanged': {
          const fn: FileChangeHookFn = async (ctx) => {
            if (hookConfig.if && !matchesPattern(hookConfig.if, ctx.filePath)) return;
            if (matcher && !matchesPattern(matcher, ctx.filePath)) return;
            const eventData = serializeHookEvent({ event: 'fileChanged', filePath: ctx.filePath, fileEvent: ctx.event });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.fileChangedHooks, fn, 'fileChanged');
            // Surface any USER-facing systemMessage the hook emitted (the reference's
            // fileChangedWatcher forwards each systemMessage via notifyCallback(msg,false)).
            for (const msg of extractSystemMessages(result)) this.emitHookNotify('fileChanged', msg, false);
            const watchPaths = this.registerWatchPaths('fileChanged', result);
            if (watchPaths.length > 0) return { watchPaths };
            return undefined;
          };
          this.onFileChanged(fn);
          break;
        }
        case 'elicitation': {
          const fn: ElicitationHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('elicitation', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'elicitation', mcpServerName: ctx.mcpServerName, message: ctx.message, mode: ctx.mode, requestedSchema: ctx.requestedSchema, url: ctx.url, elicitationId: ctx.elicitationId });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.elicitationHooks, fn, 'elicitation');
            return parseElicitationResult(result);
          };
          this.onElicitation(fn);
          break;
        }
        case 'elicitationResult': {
          const fn: ElicitationHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('elicitationResult', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'elicitationResult', mcpServerName: ctx.mcpServerName, message: ctx.message, mode: ctx.mode, requestedSchema: ctx.requestedSchema, url: ctx.url, elicitationId: ctx.elicitationId });
            const result = await this.runArmHook(hookConfig, cwd, eventData, undefined, this.elicitationResultHooks, fn, 'elicitationResult');
            return parseElicitationResult(result);
          };
          this.onElicitationResult(fn);
          break;
        }
        case 'stopFailure': {
          const fn: StopFailureHookFn = async (ctx) => {
            if (!matcherAppliesToCtx('stopFailure', matcher, ctx as unknown as Record<string, unknown>)) return;
            const eventData = serializeHookEvent({ event: 'stopFailure', error: ctx.error, errorMessage: ctx.errorMessage, hookIndex: ctx.hookIndex });
            await this.runArmHook(hookConfig, cwd, eventData, undefined, this.stopFailureHooks, fn, 'stopFailure');
          };
          this.onStopFailure(fn);
          break;
        }
        default:
          coreLog('hooks', `Unknown hook event: ${entry.event}`);
      }
      // Attribute the hook fn just registered by the switch (each event registers
      // exactly ONE fn, appended to the array {@link configHookArray} resolves) to
      // its source tier and — for a plugin-contributed hook — its bundle name/root.
      // This is what lets {@link pruneRemovedPluginHooks}/{@link reloadPluginHooks}
      // find and remove a specific plugin's hooks later (gh-36995).
      const registeredArr = this.configHookArray(entry.event);
      const registeredFn = registeredArr && registeredArr.length ? registeredArr[registeredArr.length - 1] : undefined;
      if (registeredFn) {
        this._hookProvenance.set(registeredFn, {
          source: tier,
          ...(entry.pluginName ? { pluginName: entry.pluginName } : {}),
          ...(entry.pluginRoot ? { pluginRoot: entry.pluginRoot } : {}),
        });
      }
  }

  // ---- Existing registration methods (backward-compatible) ----

  onPostSampling(fn: HookFn): void {
    this.postSamplingHooks.push(fn);
  }

  /** Register a Stop hook. An optional {@link StopHookDisplay} descriptor
   *  (command / promptText) identifies the hook in the end-of-chain run summary
   *  (the reference's StopHookInfo attribution) — config-registered command/prompt
   *  Stop hooks pass it so a slow/failing hook is named, not just numbered. */
  onStop(fn: StopHookFn, display?: StopHookDisplay): void {
    this.stopHooks.push(fn);
    if (display && (display.command || display.promptText)) {
      this._stopHookDisplay.set(fn, display);
    }
  }

  onError(fn: HookFn): void {
    this.errorHooks.push(fn);
  }

  // ---- New registration methods ----

  onPreToolUse(fn: PreToolUseHookFn, tier?: string): void {
    this.preToolUseHooks.push(fn);
    this._hookTiers.set(fn, tier || 'user');
  }

  onPostToolUse(fn: PostToolUseHookFn): void {
    this.postToolUseHooks.push(fn);
  }

  onPreSampling(fn: PreSamplingHookFn, tier?: string): void {
    this.preSamplingHooks.push(fn);
    this._hookTiers.set(fn, tier || 'user');
  }

  onSessionStart(fn: SessionHookFn): void { this.sessionStartHooks.push(fn); }
  onSessionEnd(fn: SessionHookFn): void { this.sessionEndHooks.push(fn); }
  onSubagentStart(fn: SubagentStartHookFn): void { this.subagentStartHooks.push(fn); }
  onSubagentStop(fn: SubagentHookFn): void { this.subagentStopHooks.push(fn); }
  onPreCompact(fn: PreCompactHookFn): void { this.preCompactHooks.push(fn); }
  onPostCompact(fn: PostCompactHookFn): void { this.postCompactHooks.push(fn); }
  onPermissionRequest(fn: PermissionHookFn): void { this.permissionRequestHooks.push(fn); }
  onPermissionDenied(fn: PermissionHookFn): void { this.permissionDeniedHooks.push(fn); }
  onUserPromptSubmit(fn: UserPromptHookFn): void { this.userPromptSubmitHooks.push(fn); }
  onTaskCreated(fn: TaskEventHookFn): void { this.taskCreatedHooks.push(fn); }
  onTaskCompleted(fn: TaskCompletedHookFn): void { this.taskCompletedHooks.push(fn); }
  onTeammateIdle(fn: TeammateIdleHookFn): void { this.teammateIdleHooks.push(fn); }
  onPostToolUseFailure(fn: PostToolUseHookFn): void { this.postToolUseFailureHooks.push(fn); }
  onNotification(fn: NotificationHookFn): void { this.notificationHooks.push(fn); }
  onSetup(fn: SessionHookFn): void { this.setupHooks.push(fn); }
  onConfigChange(fn: ConfigChangeHookFn): void { this.configChangeHooks.push(fn); }
  /**
   * Whether any ConfigChange hook is registered. A consumer that applies a detected
   * settings/config change to the live session uses this to decide between the cheap
   * synchronous direct-apply path (no hooks → nothing can veto) and the blocking
   * pre-apply veto path (run {@link executeConfigChange} first and skip applying the
   * change when it returns `blockingErrors`, mirroring the reference's
   * `hasBlockingResult` gate). Keeping the no-hook path synchronous preserves the
   * in-band re-enforcement (killswitch / permission re-sync) every existing caller
   * relies on. Introspection surface for the veto wiring.
   */
  hasConfigChangeHooks(): boolean { return this.configChangeHooks.length > 0; }
  onInstructionsLoaded(fn: InstructionsHookFn): void { this.instructionsLoadedHooks.push(fn); }
  onWorktreeCreate(fn: WorktreeCreateHookFn): void { this.worktreeCreateHooks.push(fn); }
  onWorktreeRemove(fn: WorktreeHookFn): void { this.worktreeRemoveHooks.push(fn); }
  onCwdChanged(fn: CwdChangedHookFn): void { this.cwdChangedHooks.push(fn); }
  onFileChanged(fn: FileChangeHookFn): void { this.fileChangedHooks.push(fn); }
  onElicitation(fn: ElicitationHookFn): void { this.elicitationHooks.push(fn); }
  onElicitationResult(fn: ElicitationHookFn): void { this.elicitationResultHooks.push(fn); }

  /**
   * Register (or clear, with `undefined`) the interactive prompt-elicitation
   * resolver (the reference's `requestPrompt`). An interactive frontend sets this
   * so a command hook that emits a {@link PromptRequest} line pauses and asks the
   * user to pick an option; the chosen key is written back to the hook's stdin.
   */
  setPromptRequestHandler(handler: PromptRequestHandler | undefined): void {
    this._promptRequestHandler = handler;
  }

  /** Whether an interactive prompt-elicitation resolver is currently registered. */
  hasPromptRequestHandler(): boolean {
    return this._promptRequestHandler !== undefined;
  }
  onStopFailure(fn: StopFailureHookFn): void { this.stopFailureHooks.push(fn); }

  // ---- Concurrency-gated execution helper ----

  private async withConcurrencyGate<T>(event: string, fn: () => Promise<T>): Promise<T> {
    await this._concurrency.acquire(event);
    try {
      return await fn();
    } finally {
      this._concurrency.release(event);
    }
  }

  // ---- Existing execution methods (backward-compatible, now with error logging) ----

  async executePostSampling(ctx: HookContext): Promise<void> {
    return this.withConcurrencyGate('postSampling', async () => {
      for (const hook of this.postSamplingHooks) {
        try {
          await hook(ctx);
        } catch (err) {
          coreLog('hooks', 'Hook error', { error: String(err) });
        }
      }
    });
  }

  /**
   * Execute Stop hooks. A Stop hook can BLOCK turn-end (blockingError, fed back
   * into the loop as a user message) or PREVENT CONTINUATION (continue the turn
   * instead of ending). Returns the aggregated {blockingErrors, preventContinuation,
   * stopReason} so the engine can re-enter the loop (the reference parity).
   * Re-entrant calls (while Stop hooks are already running) short-circuit to a
   * neutral result via the stopHookActive guard — a Stop hook cannot recursively
   * fire Stop hooks.
   */
  async executeStop(ctx: HookContext): Promise<StopHookResult> {
    if (this._stopHookActive) return { blockingErrors: [], preventContinuation: false };
    return this.withConcurrencyGate('stop', async (): Promise<StopHookResult> => {
      this._stopHookActive = true;
      const blockingErrors: string[] = [];
      // Per-hook timings + errors for the end-of-chain summary (the reference's
      // hookInfos). Wall-clock total is measured around the whole chain because
      // the reference prefers it over the summed per-hook time.
      const hookInfos: StopHookInfo[] = [];
      const hookErrors: string[] = [];
      const chainStart = Date.now();
      let preventContinuation = false;
      let stopReason: string | undefined;
      try {
        for (let i = 0; i < this.stopHooks.length; i++) {
          // Abort-signal awareness (the reference's stopHooks abort check): a user
          // interrupt (Ctrl-C / turn abort) mid-chain stops the Stop-hook chain
          // immediately — no further hooks run — and the in-flight per-hook child
          // is torn down via the abortSignal threaded into runArmHook. The partial
          // result accumulated so far is returned (not discarded), matching the
          // "aborting yields an interruption and stops the chain" contract.
          if (ctx.abortSignal?.aborted) {
            coreLog('hooks', 'Stop hooks interrupted by abort signal');
            break;
          }
          const hookStart = Date.now();
          // Attribute this per-hook row to its backing config (command/promptText)
          // when a display descriptor was registered (the reference's StopHookInfo).
          const display = this._stopHookDisplay.get(this.stopHooks[i]);
          try {
            const r = await this.stopHooks[i](ctx);
            hookInfos.push({ durationMs: Date.now() - hookStart, ...display });
            if (r) {
              if (r.blockingError) blockingErrors.push(r.blockingError);
              if (r.preventContinuation) { preventContinuation = true; if (r.stopReason) stopReason = r.stopReason; }
            }
          } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            hookInfos.push({ durationMs: Date.now() - hookStart, error: message, ...display });
            hookErrors.push(message);
            coreLog('hooks', 'Hook error', { error: message });
            // Classify the raw failure into the matchable StopFailure taxonomy so a
            // hook scoped on e.g. `rate_limit` actually fires; keep the raw detail.
            await this.executeStopFailure({ error: stopFailureErrorClass(message), errorMessage: message, hookIndex: i });
          }
        }
      } finally {
        this._stopHookActive = false;
      }
      // A prevented continuation always carries a displayable reason: fall back to
      // the canonical "Stop hook prevented continuation" wording when the hook was
      // terse (the reference's hook_stopped_continuation default — stopHooks.ts:L268-280).
      if (preventContinuation) stopReason = formatHookStoppedContinuation(stopReason);
      // Build + retain + emit the per-hook summary (operator-facing; NEVER fed to
      // the model). Only when hooks actually ran — a no-op turn gets no summary.
      if (hookInfos.length > 0) {
        const summary: StopHookSummary = {
          count: hookInfos.length,
          hookInfos,
          totalDurationMs: Date.now() - chainStart,
          hookErrors,
          preventContinuation,
          stopReason,
          // Output worth surfacing to the operator: a blocking error fed back into
          // the turn, a hook that errored, or a prevented continuation (the reference's
          // hasOutput flag). A silent, fast, non-blocking chain has nothing to show.
          hasOutput: blockingErrors.length > 0 || hookErrors.length > 0 || preventContinuation,
        };
        this._lastStopHookSummary = summary;
        this.emit('stopHookSummary', summary);
      }
      return { blockingErrors, preventContinuation, stopReason };
    });
  }

  async executeError(ctx: HookContext): Promise<void> {
    return this.withConcurrencyGate('error', async () => {
      for (const hook of this.errorHooks) {
        try {
          await hook(ctx);
        } catch (err) {
          coreLog('hooks', 'Hook error', { error: String(err) });
        }
      }
    });
  }

  // ---- New execution methods ----

  /**
   * Execute pre-tool-use hooks. First deny wins; modify decisions are accumulated
   * (later modify hooks can override earlier ones).
   */
  async executePreToolUse(ctx: PreToolUseContext): Promise<HookDecision> {
    return this.withConcurrencyGate('preToolUse', () =>
      this.executeDecisionHooks(this.preToolUseHooks, ctx));
  }

  /**
   * Execute post-tool-use hooks. Returns the last additionalContext/updatedToolOutput if any hook provides one.
   */
  async executePostToolUse(ctx: PostToolUseContext): Promise<PostToolUseHookResult | void> {
    return this.withConcurrencyGate('postToolUse', async (): Promise<PostToolUseHookResult | void> => {
      // Aggregate ALL contributions across every matching hook (the reference's
      // AggregatedHookResult collects additionalContexts[]/blockingErrors[]),
      // not last-wins: a later hook must not silently drop an earlier hook's
      // context or block reason. updatedToolOutput stays last-wins (a single
      // canonical rewrite of the tool output).
      const additionalContexts: string[] = [];
      const blockingErrors: string[] = [];
      let updatedToolOutput: string | undefined;
      let preventContinuation = false;
      let stopReason: string | undefined;
      for (const hook of this.postToolUseHooks) {
        try {
          const result = await hook(ctx);
          if (result?.additionalContext) additionalContexts.push(result.additionalContext);
          if (result?.updatedToolOutput) updatedToolOutput = result.updatedToolOutput;
          if (result?.blockingError) blockingErrors.push(result.blockingError);
          if (result?.preventContinuation) { preventContinuation = true; if (result.stopReason) stopReason = result.stopReason; }
        } catch (err) {
          coreLog('hooks', 'Hook error', { error: String(err) });
        }
      }
      const additionalContext = additionalContexts.length ? additionalContexts.join('\n\n') : undefined;
      const blockingError = blockingErrors.length ? blockingErrors.join('\n') : undefined;
      if (additionalContext || updatedToolOutput || blockingError || preventContinuation) {
        return { additionalContext, updatedToolOutput, blockingError, preventContinuation: preventContinuation || undefined, stopReason };
      }
    });
  }

  /**
   * Execute pre-sampling hooks. First deny wins; modify decisions are accumulated
   * (later modify hooks can override earlier ones).
   */
  async executePreSampling(ctx: PreSamplingContext): Promise<HookDecision> {
    return this.withConcurrencyGate('preSampling', () =>
      this.executeDecisionHooks(this.preSamplingHooks, ctx));
  }

  async executeSessionStart(ctx: SessionContext): Promise<{ initialUserMessage?: string; additionalContexts?: string[]; watchPaths?: string[] } | void> {
    if (ctx.source !== undefined && !isSessionStartSource(ctx.source)) {
      coreLog('hooks', `SessionStart: unrecognized source '${ctx.source}' (expected ${SESSION_START_SOURCES.join('|')})`);
    }
    return this.withConcurrencyGate('sessionStart', async (): Promise<{ initialUserMessage?: string; additionalContexts?: string[]; watchPaths?: string[] } | void> => {
      // Aggregate ALL messages from ALL SessionStart hooks (the reference injects
      // an array of hook messages, not just the last) — a hook may contribute one
      // (`initialUserMessage`) or many (`initialUserMessages[]`); all are joined.
      // Model-facing `additionalContexts[]` are accumulated across every hook and
      // returned as a single combined list for one hook_additional_context
      // attachment (the reference's SessionStart additionalContexts aggregation).
      const messages: string[] = [];
      const additionalContexts: string[] = [];
      const watchPaths: string[] = [];
      for (const hook of this.sessionStartHooks) {
        // Cooperative cancellation: a supplied deadline/cancel signal stops the
        // in-process SessionStart chain mid-flight so a slow callback cannot block
        // startup (the reference SessionContext signal parity).
        if (ctx.signal?.aborted) { coreLog('hooks', 'SessionStart hooks interrupted by abort signal'); break; }
        try {
          const result = await raceAbort(Promise.resolve(hook(ctx)), ctx.signal);
          if (result?.initialUserMessage) messages.push(result.initialUserMessage);
          if (result?.initialUserMessages?.length) messages.push(...result.initialUserMessages);
          if (result?.additionalContexts?.length) additionalContexts.push(...result.additionalContexts);
          if (result?.watchPaths?.length) watchPaths.push(...result.watchPaths);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      const initialUserMessage = messages.length ? messages.join('\n\n') : undefined;
      if (initialUserMessage || additionalContexts.length > 0 || watchPaths.length > 0) {
        return {
          initialUserMessage,
          additionalContexts: additionalContexts.length > 0 ? additionalContexts : undefined,
          watchPaths: watchPaths.length > 0 ? watchPaths : undefined,
        };
      }
    });
  }

  async executeSessionEnd(ctx: SessionContext): Promise<void> {
    if (ctx.reason !== undefined && !isSessionEndReason(ctx.reason)) {
      coreLog('hooks', `SessionEnd: unrecognized reason '${ctx.reason}' (expected ${SESSION_END_REASONS.join('|')})`);
    }
    return this.withConcurrencyGate('sessionEnd', async () => {
      // SessionEnd hooks run during shutdown/clear; bound the WHOLE run with a
      // tight budget (getSessionEndHookTimeoutMs) so a slow or hanging teardown
      // hook — command OR in-process callback — cannot block process exit. On
      // budget exhaustion we LOG and return (teardown proceeds) rather than
      // reject, matching the reference's "don't block shutdown" contract. Command
      // hooks additionally receive an AbortSignal at the same budget (see the
      // sessionEnd registration) so their child processes are actually killed.
      const budgetMs = getSessionEndHookTimeoutMs();
      // In-process SessionEnd callbacks get a cancellation handle: the caller's
      // explicit lifecycle signal when supplied, otherwise one synthesized from the
      // teardown budget — so a slow in-process cleanup callback is abandoned on the
      // deadline instead of blocking shutdown (the reference passes
      // AbortSignal.timeout(sessionEndTimeoutMs) into executeSessionEndHooks).
      const lifecycleSignal = ctx.signal ?? AbortSignal.timeout(budgetMs);
      const run = (async () => {
        for (const hook of this.sessionEndHooks) {
          if (lifecycleSignal.aborted) { coreLog('hooks', 'SessionEnd hooks interrupted by abort signal'); break; }
          try { await raceAbort(Promise.resolve(hook(ctx)), lifecycleSignal); } catch (err) {
            // A deadline/cancel abort is not a hook defect — the run was stopped on
            // purpose; log and move on without a spurious hookError.
            if (lifecycleSignal.aborted) { coreLog('hooks', 'SessionEnd hooks interrupted by abort signal'); break; }
            const message = err instanceof Error ? err.message : String(err);
            coreLog('hooks', 'Hook error', { error: message });
            // Surface the failure instead of swallowing it into the debug log:
            // the session UI is already unmounting, so a failed teardown hook would
            // otherwise be invisible (the reference writes 'SessionEnd hook failed: …'
            // to stderr — utils/hooks.ts:L4127-4134). Emitted on the same `hookError`
            // channel as command/http/agent hook failures.
            this.emit('hookError', { hookType: 'command', hook: 'SessionEnd hook', error: message });
          }
        }
      })();
      try {
        await runWithTimeout(run, budgetMs, 'sessionEnd hooks');
      } catch (err) {
        coreLog('hooks', 'SessionEnd hooks exceeded budget', { budgetMs, error: String(err) });
      }
    });
  }

  /**
   * Execute SubagentStart hooks and aggregate the additionalContexts they
   * contribute, which the Task tool prepends to the subagent's initial messages
   * (the reference parity).
   */
  async executeSubagentStart(ctx: SubagentContext): Promise<{ additionalContexts: string[] } | void> {
    return this.withConcurrencyGate('subagentStart', async (): Promise<{ additionalContexts: string[] } | void> => {
      const additionalContexts: string[] = [];
      for (const hook of this.subagentStartHooks) {
        try {
          const r = await hook(ctx);
          if (r?.additionalContexts?.length) additionalContexts.push(...r.additionalContexts);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (additionalContexts.length > 0) return { additionalContexts };
    });
  }

  async executeSubagentStop(ctx: SubagentContext): Promise<void> {
    return this.withConcurrencyGate('subagentStop', async () => {
      for (const hook of this.subagentStopHooks) {
        // Abort-signal awareness: a user interrupt stops the SubagentStop chain
        // mid-flight (the reference parity) — no further hooks run.
        if (ctx.abortSignal?.aborted) {
          coreLog('hooks', 'SubagentStop hooks interrupted by abort signal');
          break;
        }
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  /**
   * Execute PreCompact hooks. A hook can inject/override the compaction
   * instructions (newCustomInstructions, merged into the user's via
   * mergeHookInstructions) and surface a user-facing display message
   * (the reference parity). Returns the merged instructions + last display message.
   */
  async executePreCompact(ctx: CompactContext): Promise<{ newCustomInstructions?: string; userDisplayMessage?: string } | void> {
    return this.withConcurrencyGate('preCompact', async (): Promise<{ newCustomInstructions?: string; userDisplayMessage?: string } | void> => {
      coreLog('hooks', `[compact] Pre-compact triggered: ${ctx.messageCount} messages, ~${ctx.tokenEstimate} tokens`);
      let newCustomInstructions: string | undefined;
      let userDisplayMessage: string | undefined;
      for (const hook of this.preCompactHooks) {
        // Cooperative cancellation: the user aborting the compaction stops the
        // in-process PreCompact chain mid-flight (the reference CompactContext signal).
        if (ctx.signal?.aborted) { coreLog('hooks', 'PreCompact hooks interrupted by abort signal'); break; }
        try {
          const r = await raceAbort(Promise.resolve(hook(ctx)), ctx.signal);
          if (r?.newCustomInstructions) newCustomInstructions = mergeHookInstructions(newCustomInstructions, r.newCustomInstructions);
          if (r?.userDisplayMessage) userDisplayMessage = r.userDisplayMessage;
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (newCustomInstructions || userDisplayMessage) return { newCustomInstructions, userDisplayMessage };
    });
  }

  /**
   * Execute PostCompact hooks with the generated compaction summary + trigger
   * (`ctx.compactSummary` / `ctx.trigger`). A hook may surface a user-facing
   * display message; the messages from all hooks are joined (the reference combines
   * the PostCompact hook's `userDisplayMessage` into the result shown to the user).
   */
  async executePostCompact(ctx: CompactContext): Promise<{ userDisplayMessage?: string } | void> {
    return this.withConcurrencyGate('postCompact', async (): Promise<{ userDisplayMessage?: string } | void> => {
      const messages: string[] = [];
      for (const hook of this.postCompactHooks) {
        try {
          const r = await hook(ctx);
          if (r?.userDisplayMessage) messages.push(r.userDisplayMessage);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (messages.length > 0) return { userDisplayMessage: messages.join('\n') };
    });
  }

  async executePermissionRequest(ctx: PermissionContext): Promise<PermissionHookResult | void> {
    return this.withConcurrencyGate('permissionRequest', async (): Promise<PermissionHookResult | void> => {
      for (const hook of this.permissionRequestHooks) {
        // Stop consulting hooks once the turn is aborted (user interrupt): a
        // cancelled permission request yields no decision and falls through to
        // policy (the reference's abort-aware runHooks loop).
        if (ctx.abortSignal?.aborted) break;
        try {
          const result = await raceAbort(hook(ctx), ctx.abortSignal);
          if (result && result.action) return result;
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  async executePermissionDenied(ctx: PermissionContext): Promise<{ retry?: boolean } | void> {
    return this.withConcurrencyGate('permissionDenied', async (): Promise<{ retry?: boolean } | void> => {
      for (const hook of this.permissionDeniedHooks) {
        try {
          const result = await hook(ctx);
          if (result && (result as any).action === 'allow') {
            return { retry: true };
          }
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (ctx.retry) return { retry: true };
    });
  }

  /**
   * Execute UserPromptSubmit hooks. A hook may inject additionalContext, or BLOCK
   * the prompt (blockingError, surfaced to the user) / prevent continuation —
   * the reference parity. Returns the aggregated result.
   */
  async executeUserPromptSubmit(ctx: UserPromptContext): Promise<UserPromptSubmitHookResult | void> {
    // Recursion guard: when an engine-backed (prompt/agent) hook is running, its
    // sub-engine's submit must NOT re-fire UserPromptSubmit hooks — that would
    // re-enter runEngineHook and recurse until resources are exhausted (the reference
    // Code's execPromptHook bypasses processUserInput for exactly this reason).
    if (this._engineHookDepth > 0) {
      coreLog('hooks', 'userPromptSubmit skipped: inside an engine-backed hook run (recursion guard)');
      return;
    }
    return this.withConcurrencyGate('userPromptSubmit', async (): Promise<UserPromptSubmitHookResult | void> => {
      // Aggregate ALL contributions (the reference AggregatedHookResult), not
      // last-wins: every UserPromptSubmit hook's context + block reason is kept.
      const additionalContexts: string[] = [];
      const systemMessages: string[] = [];
      const blockingErrors: string[] = [];
      let preventContinuation = false;
      let stopReason: string | undefined;
      for (const hook of this.userPromptSubmitHooks) {
        try {
          const result = await hook(ctx);
          if (result?.additionalContext) additionalContexts.push(result.additionalContext);
          if (result?.systemMessage) systemMessages.push(result.systemMessage);
          if (result?.blockingError) blockingErrors.push(result.blockingError);
          if (result?.preventContinuation) { preventContinuation = true; if (result.stopReason) stopReason = result.stopReason; }
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      const additionalContext = additionalContexts.length ? additionalContexts.join('\n\n') : undefined;
      // User-facing warnings are aggregated like additionalContext (the reference
      // AggregatedHookResult) but routed to the human, not the model.
      const systemMessage = systemMessages.length ? systemMessages.join('\n\n') : undefined;
      const blockingError = blockingErrors.length ? blockingErrors.join('\n') : undefined;
      if (additionalContext || systemMessage || blockingError || preventContinuation) {
        // Bound hook-contributed context so a UserPromptSubmit hook that emits a
        // huge blob cannot blow out the context window (the reference's
        // MAX_HOOK_OUTPUT_LENGTH truncation with a marker).
        return {
          additionalContext: additionalContext !== undefined ? truncateHookOutput(additionalContext) : undefined,
          systemMessage: systemMessage !== undefined ? truncateHookOutput(systemMessage) : undefined,
          blockingError,
          preventContinuation: preventContinuation || undefined,
          stopReason,
        };
      }
    });
  }

  /**
   * Execute TaskCreated hooks. A blocking hook can VETO a just-created task; the
   * aggregated blockingErrors are returned so the task tool can roll the task back
   * and surface the reason (the reference parity). If the forwarded AbortSignal is
   * already aborted the loop short-circuits (cancellation boundary).
   */
  async executeTaskCreated(ctx: TaskEventContext): Promise<{ blockingErrors: string[] } | void> {
    return this.withConcurrencyGate('taskCreated', async (): Promise<{ blockingErrors: string[] } | void> => {
      const blockingErrors: string[] = [];
      for (const hook of this.taskCreatedHooks) {
        if (ctx.signal?.aborted) break;
        try {
          const r = await hook(ctx);
          if (r?.blockingError) blockingErrors.push(r.blockingError);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (blockingErrors.length > 0) return { blockingErrors };
    });
  }

  /**
   * Execute TaskCompleted hooks. A blocking hook can veto a status→completed
   * transition; the aggregated blockingErrors are returned so the task tool can
   * refuse to mutate the status and surface the reason (the reference parity).
   */
  async executeTaskCompleted(ctx: TaskEventContext): Promise<{ blockingErrors: string[] } | void> {
    return this.withConcurrencyGate('taskCompleted', async (): Promise<{ blockingErrors: string[] } | void> => {
      const blockingErrors: string[] = [];
      for (const hook of this.taskCompletedHooks) {
        // Cancellation boundary (the reference parity: executeTaskCompletedHooks is
        // driven by context.abortController.signal). An already-aborted turn runs
        // no further completion hooks, and a hook that blocks (e.g. spawns a
        // subprocess) is actively cancelled via raceAbort so it cannot leak past a
        // Ctrl-C / engine abort — the forwarded signal is also on ctx for the hook
        // itself to observe.
        if (ctx.signal?.aborted) break;
        try {
          const r = await raceAbort(hook(ctx), ctx.signal);
          if (r?.blockingError) blockingErrors.push(r.blockingError);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (blockingErrors.length > 0) return { blockingErrors };
    });
  }

  /**
   * Fan an event's OBSERVATIONAL (void-returning, non-decision) hooks out
   * CONCURRENTLY instead of running them one-after-another. the reference dispatches
   * every matching hook for an event in parallel (`matchingHooks.map(async …)` +
   * aggregation, utils/hooks.ts L2142) so N independent command/http/prompt hooks on
   * one event overlap rather than stacking their individual latencies; AMI had been
   * awaiting each in a serial `for` loop, so a slow notification/idle hook delayed
   * every sibling behind it. Per-hook timeout/abort already lives INSIDE each hook fn
   * (runArmHook → runWithTimeout / combineHookAbort), so this only swaps serial→
   * parallel for events whose hooks have no inter-hook ordering dependency (the
   * fire-and-forget observational events). Each hook is isolated: one that throws is
   * caught via {@link onError} and never rejects the batch or cancels its siblings —
   * identical failure semantics to the old loop, just overlapped.
   */
  private async runObservationalHooksConcurrently<C>(
    hooks: Array<(ctx: C) => Promise<unknown>>,
    ctx: C,
    onError: (err: unknown) => void,
  ): Promise<void> {
    await Promise.all(
      hooks.map(async (hook) => {
        try { await hook(ctx); } catch (err) { onError(err); }
      }),
    );
  }

  /**
   * Fire TeammateIdle hooks when a teammate agent finishes its run and goes idle.
   * Observational (fire-and-forget); mirrors the reference's TeammateIdle event with
   * the teammate/team identity payload (teammate_name / team_name).
   */
  async executeTeammateIdle(ctx: TeammateContext): Promise<void> {
    return this.withConcurrencyGate('teammateIdle', async () => {
      await this.runObservationalHooksConcurrently(this.teammateIdleHooks, ctx, (err) => coreLog('hooks', 'Hook error', { error: String(err) }));
    });
  }

  async executePostToolUseFailure(ctx: PostToolUseContext): Promise<void> {
    return this.withConcurrencyGate('postToolUseFailure', async () => {
      await this.runObservationalHooksConcurrently(this.postToolUseFailureHooks, ctx, (err) => coreLog('hooks', 'Hook error', { error: String(err) }));
    });
  }

  async executeNotification(ctx: NotificationContext): Promise<void> {
    return this.withConcurrencyGate('notification', async () => {
      await this.runObservationalHooksConcurrently(this.notificationHooks, ctx, (err) => coreLog('hooks', 'Hook error', { error: String(err) }));
    });
  }

  async executeSetup(ctx: SessionContext): Promise<{ additionalContexts?: string[] } | void> {
    if (ctx.trigger !== undefined && !isSetupTrigger(ctx.trigger)) {
      coreLog('hooks', `Setup: unrecognized trigger '${ctx.trigger}' (expected ${SETUP_TRIGGERS.join('|')})`);
    }
    return this.withConcurrencyGate('setup', async (): Promise<{ additionalContexts?: string[] } | void> => {
      // Accumulate model-facing additionalContexts across every Setup hook into a
      // single combined list for one hook_additional_context attachment (the reference
      // Code's Setup additionalContexts aggregation).
      const additionalContexts: string[] = [];
      for (const hook of this.setupHooks) {
        try {
          const result = await hook(ctx);
          if (result?.additionalContexts?.length) additionalContexts.push(...result.additionalContexts);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (additionalContexts.length > 0) return { additionalContexts };
    });
  }

  /**
   * Execute ConfigChange hooks. A blocking hook can VETO the change before it is
   * applied; the aggregated blockingErrors are returned so the config loader can
   * refuse to commit and surface the reason (the reference's hasBlockingResult).
   */
  async executeConfigChange(ctx: ConfigChangeContext): Promise<{ blockingErrors: string[] } | void> {
    return this.withConcurrencyGate('configChange', async (): Promise<{ blockingErrors: string[] } | void> => {
      const blockingErrors: string[] = [];
      for (const hook of this.configChangeHooks) {
        try {
          const r = await hook(ctx);
          if (r?.blockingError) blockingErrors.push(r.blockingError);
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
      if (blockingErrors.length > 0) return { blockingErrors };
    });
  }

  async executeInstructionsLoaded(ctx: InstructionsContext): Promise<void> {
    return this.withConcurrencyGate('instructionsLoaded', async () => {
      await this.runObservationalHooksConcurrently(this.instructionsLoadedHooks, ctx, (err) => coreLog('hooks', 'Hook error', { error: String(err) }));
    });
  }

  /**
   * Execute WorktreeCreate hooks. A provider hook may return a `worktreePath`
   * (from stdout or hookSpecificOutput.worktreePath); the FIRST non-empty path
   * wins and is returned so the caller uses it as the created worktree (the reference
   * Code's executeWorktreeCreateHook — first successful non-empty output). A plain
   * void return across all hooks is the observe-only case.
   */
  async executeWorktreeCreate(ctx: WorktreeContext): Promise<{ worktreePath?: string } | void> {
    return this.withConcurrencyGate('worktreeCreate', async (): Promise<{ worktreePath?: string } | void> => {
      for (const hook of this.worktreeCreateHooks) {
        try {
          const r = await hook(ctx);
          if (r?.worktreePath && r.worktreePath.trim().length > 0) return { worktreePath: r.worktreePath.trim() };
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  async executeWorktreeRemove(ctx: WorktreeContext): Promise<void> {
    return this.withConcurrencyGate('worktreeRemove', async () => {
      await this.runObservationalHooksConcurrently(this.worktreeRemoveHooks, ctx, (err) => coreLog('hooks', 'Hook error', { error: String(err) }));
    });
  }

  async executeCwdChanged(ctx: CwdChangedContext): Promise<void> {
    // No-op short-circuit (the reference `onCwdChangedForHooks`: `if (oldCwd ===
    // newCwd) return`). When the "change" lands on the same directory the cwd did
    // not actually move, so clearing the session-env cache and firing CwdChanged
    // hooks would be wasted work with observable side effects (a spurious env
    // re-read, a redundant hook run). Bail before any of it.
    if (ctx.oldCwd === ctx.newCwd) return;
    // The cwd changed: drop stale directory-scoped env fragments and invalidate
    // the persisted session-environment cache so the next bash command re-reads
    // it (the reference `clearCwdEnvFiles` / `invalidateSessionEnvCache` parity).
    await clearCwdEnvFiles(this._sessionId);
    invalidateSessionEnvCache();
    return this.withConcurrencyGate('cwdChanged', async () => {
      await this.runObservationalHooksConcurrently(this.cwdChangedHooks, ctx, (err) => this.emitWatcherHookError('CwdChanged hook', err));
    });
  }

  async executeFileChanged(ctx: FileChangeContext): Promise<void> {
    return this.withConcurrencyGate('fileChanged', async () => {
      await this.runObservationalHooksConcurrently(this.fileChangedHooks, ctx, (err) => this.emitWatcherHookError('FileChanged hook', err));
    });
  }

  /**
   * Surface a failed FileChanged / CwdChanged watcher hook on the visible
   * `hookError` channel every other hook failure uses AND on the human-facing
   * {@link emitHookNotify} notify channel (isError=true), instead of only
   * `coreLog`-ing it internally. These hooks fire from fire-and-forget fs-watch /
   * cwd-change call sites (no generator to yield from), so a misconfigured watcher
   * hook would otherwise fail silently forever (the reference's fileChangedWatcher.ts
   * routes each failed hook's error to the notify callback — L99-L105). Operator-
   * facing (debug log + analytics via the engine's `hookError` listener); never fed
   * to the model.
   */
  private emitWatcherHookError(hook: string, err: unknown): void {
    const error = err instanceof Error ? err.message : String(err);
    coreLog('hooks', 'Hook error', { error });
    this.emit('hookError', { hookType: 'command', hook, error });
    this.emitHookNotify(hook, error, true);
  }

  /**
   * Forward a FileChanged / CwdChanged watcher hook's USER-facing output on a
   * `hookNotify` channel the session renders, instead of silently dropping it.
   * Mirrors the reference's fileChangedWatcher `setEnvHookNotifier((text, isError) =>
   * …)`: a hook's `systemMessages` are surfaced with `isError=false` and a failed
   * hook's error/output with `isError=true` (fileChangedWatcher.ts L90-L105,
   * L162-L169). Watcher hooks fire from fire-and-forget call sites with no generator
   * to yield from, so without this channel their stdout and failures are invisible.
   * Empty messages are dropped so an observe-only hook emits no spurious notice.
   */
  private emitHookNotify(event: string, message: string, isError: boolean): void {
    if (!message) return;
    this.emit('hookNotify', { event, message, isError });
  }

  /**
   * Execute pre-response Elicitation hooks. The FIRST hook that returns an
   * action (programmatic response) or a blockingError short-circuits the user
   * prompt (the reference parity: a hook response resolves the elicitation
   * directly; a blockingError declines it).
   */
  async executeElicitation(ctx: ElicitationContext): Promise<ElicitationHookResult | void> {
    return this.withConcurrencyGate('elicitation', async (): Promise<ElicitationHookResult | void> => {
      for (const hook of this.elicitationHooks) {
        try {
          const r = await hook(ctx);
          if (r && (r.action || r.content || r.blockingError)) return r;
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  /**
   * Execute post-response ElicitationResult hooks. A hook can override the
   * action/content the user supplied, or block the reply (blockingError →
   * decline). The first hook that returns an override/block wins.
   */
  async executeElicitationResult(ctx: ElicitationContext): Promise<ElicitationHookResult | void> {
    return this.withConcurrencyGate('elicitationResult', async (): Promise<ElicitationHookResult | void> => {
      for (const hook of this.elicitationResultHooks) {
        try {
          const r = await hook(ctx);
          if (r && (r.action || r.content || r.blockingError)) return r;
        } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  async executeStopFailure(ctx: StopFailureContext): Promise<void> {
    return this.withConcurrencyGate('stopFailure', async () => {
      for (const hook of this.stopFailureHooks) {
        try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
      }
    });
  }

  /** Count registered runtime callbacks, partitioned by whether they are system
   *  ({@link RuntimeHookMeta.internal}) hooks. `wantInternal=false` counts the
   *  user-configured callbacks; `true` counts the hidden built-ins. */
  private countCallbackHooks(wantInternal: boolean): number {
    let n = 0;
    for (const hooks of this._callbackHooks.values()) {
      for (const cb of hooks) {
        if ((this._callbackMeta.get(cb)?.internal ?? false) === wantInternal) n++;
      }
    }
    return n;
  }

  getSummary(): Record<string, number> {
    return {
      preToolUse: this.preToolUseHooks.length,
      postToolUse: this.postToolUseHooks.length,
      preSampling: this.preSamplingHooks.length,
      postSampling: this.postSamplingHooks.length,
      stop: this.stopHooks.length,
      error: this.errorHooks.length,
      sessionStart: this.sessionStartHooks.length,
      sessionEnd: this.sessionEndHooks.length,
      subagentStart: this.subagentStartHooks.length,
      subagentStop: this.subagentStopHooks.length,
      preCompact: this.preCompactHooks.length,
      postCompact: this.postCompactHooks.length,
      permissionRequest: this.permissionRequestHooks.length,
      permissionDenied: this.permissionDeniedHooks.length,
      userPromptSubmit: this.userPromptSubmitHooks.length,
      taskCreated: this.taskCreatedHooks.length,
      taskCompleted: this.taskCompletedHooks.length,
      teammateIdle: this.teammateIdleHooks.length,
      postToolUseFailure: this.postToolUseFailureHooks.length,
      notification: this.notificationHooks.length,
      setup: this.setupHooks.length,
      configChange: this.configChangeHooks.length,
      instructionsLoaded: this.instructionsLoadedHooks.length,
      worktreeCreate: this.worktreeCreateHooks.length,
      worktreeRemove: this.worktreeRemoveHooks.length,
      cwdChanged: this.cwdChangedHooks.length,
      fileChanged: this.fileChangedHooks.length,
      elicitation: this.elicitationHooks.length,
      elicitationResult: this.elicitationResultHooks.length,
      stopFailure: this.stopFailureHooks.length,
      // User-facing runtime callback count EXCLUDES system-registered (`internal`)
      // hooks, so the `/hooks` view reflects only what the operator configured
      // (the reference hides `internal:true` callbacks). The hidden built-ins are
      // reported separately as `internalCallbackHooks` for full introspection.
      callbackHooks: this.countCallbackHooks(false),
      internalCallbackHooks: this.countCallbackHooks(true),
      functionHooks: this._functionHooks.size,
      pendingHookEvents: this._pendingHookEvents.length,
      // Interactive prompt-elicitation resolver presence (1 = command hooks may
      // ask the user to pick from a labelled option list).
      promptRequestHandler: this._promptRequestHandler ? 1 : 0,
    };
  }

  /**
   * Introspection: the ACTUAL file-loaded hooks (command/prompt/url, matcher,
   * source), not just counts (the reference's per-hook ViewHookMode details). Returns
   * a copy so callers cannot mutate the retained set.
   */
  describeHooks(): HookDescriptor[] {
    return this._descriptors.map(d => ({ ...d }));
  }

  /**
   * Full per-hook audit view (the reference's ViewHookMode): for every retained
   * hook, its typed content with the type-specific label (command→'Command',
   * http→'URL', prompt/agent→'Prompt'), a human-readable source description with
   * plugin attribution (`Plugin (name)`), and the progress display text
   * (statusMessage || content). This is the canonical consumer of
   * {@link hookContentLabel}, {@link hookSourceDisplayString} and
   * {@link getHookDisplayText}, so `/hooks` + `--introspect` can reveal exactly
   * what each configured hook runs, where it came from, and which plugin (if any)
   * installed it. Returns copies; callers cannot mutate the retained set.
   */
  getHookAuditView(): HookAuditRow[] {
    // Build from describeHooks() (the canonical retained-descriptor copy) rather
    // than re-reading _descriptors, so the two introspection surfaces share one
    // source of truth and the audit view can never drift from the raw view.
    return this.describeHooks().map(d => ({
      event: d.event,
      type: d.type,
      ...(d.matcher ? { matcher: d.matcher } : {}),
      source: d.source,
      sourceLabel: hookSourceDisplayString(d.source, d.pluginName),
      sourceDescription: hookSourceDescription(d.source, d.pluginName),
      sourceHeader: hookSourceHeader(d.source, d.pluginName),
      sourceInline: hookSourceInline(d.source),
      ...(d.pluginName ? { pluginName: d.pluginName } : {}),
      contentLabel: hookContentLabel(d.type),
      content: d.content,
      displayText: getHookDisplayText(d),
    }));
  }

  /**
   * Introspection view of active (count>0) hook events (the reference's
   * SelectEventMode): each event with its human-readable summary/description, the
   * matcher field + enumerated matcher values, whether it supports a matcher, and
   * its matcher groups (sorted by source priority). `opts` supplies the dynamic
   * matcher-value sets (tool names, agent types, MCP server names).
   */
  describeEvents(opts: HookEventMetadataOpts = {}): HookEventDescription[] {
    const summary = this.getSummary();
    // Group retained descriptors by event, then by matcher.
    const byEvent = new Map<string, HookDescriptor[]>();
    for (const d of this._descriptors) {
      const arr = byEvent.get(d.event) ?? [];
      arr.push(d);
      byEvent.set(d.event, arr);
    }
    const out: HookEventDescription[] = [];
    for (const [event, count] of Object.entries(summary)) {
      if (!count || event === 'callbackHooks' || event === 'internalCallbackHooks') continue;
      const meta = getHookEventMetadata(event, opts);
      const descs = byEvent.get(event) ?? [];
      const matcherMap = new Map<string, HookDescriptor[]>();
      for (const d of descs) {
        const key = d.matcher ?? '';
        const arr = matcherMap.get(key) ?? [];
        arr.push(d);
        matcherMap.set(key, arr);
      }
      const sourcesByMatcher: Record<string, string[]> = {};
      const pluginNamesByMatcher: Record<string, string[]> = {};
      for (const [m, arr] of matcherMap) {
        sourcesByMatcher[m] = Array.from(new Set(arr.map(a => a.source)));
        // Per-matcher plugin attribution (the reference's pluginName per grouped
        // hook): collect the distinct bundle names so the group can name WHICH
        // plugin(s) bound this matcher, not just surface a bare `plugin` tier.
        const names = Array.from(new Set(arr.map(a => a.pluginName).filter((n): n is string => typeof n === 'string' && n.length > 0)));
        if (names.length) pluginNamesByMatcher[m] = names;
      }
      const matchers: HookEventMatcherGroup[] = sortMatchersByPriority(
        Array.from(matcherMap.keys()),
        sourcesByMatcher,
      ).map(m => ({
        matcher: m,
        label: formatHookMatcherLabel(m),
        count: matcherMap.get(m)!.length,
        sources: sourcesByMatcher[m],
        ...(pluginNamesByMatcher[m] ? { pluginNames: pluginNamesByMatcher[m] } : {}),
      }));
      out.push({
        event,
        count,
        summary: meta?.summary,
        description: meta?.description,
        matcherField: meta?.matcherField,
        matcherValues: meta?.matcherValues,
        supportsMatcher: eventSupportsMatcher(event),
        matchers,
      });
    }
    out.sort((a, b) => a.event.localeCompare(b.event));
    return out;
  }

  /**
   * Shared decision-hook executor. First deny wins; modify decisions are
   * accumulated (later hooks override earlier ones).
   *
   * Gap 2: Fail-closed — if a hook throws, we return deny immediately rather
   *   than silently allowing the action through.
   *
   * Gap 1: Tier-aware hold-over — when _denyRulesHold is true, a user-tier
   *   hook cannot loosen a verdict set by a higher-privilege tier.
   */
  private async executeDecisionHooks<T extends HookContext>(
    hooks: Array<(ctx: T) => Promise<HookDecision>>,
    ctx: T,
  ): Promise<HookDecision> {
    let accumulated: HookDecision = { action: 'allow' };
    // Track the strictest verdict from non-user tiers for hold-over logic
    let preUserVerdict: HookDecision | null = null;

    for (const hook of hooks) {
      try {
        const decision = await hook(ctx);

        // Gap 1: When denyRulesHold is active, track pre-user-tier verdict
        if (this._denyRulesHold) {
          const hookTier = this._hookTiers.get(hook) || 'user';
          if (hookTier !== 'user' && hookTier !== 'project' && hookTier !== 'config') {
            // This is a builtin/org hook — record its verdict if strictest so far
            if (!preUserVerdict || verdictRank(decision.action) > verdictRank(preUserVerdict.action)) {
              preUserVerdict = decision;
            }
          }
        }

        if (decision.action === 'deny') {
          return decision;
        }
        if (decision.action === 'modify') {
          accumulated = {
            action: 'modify',
            updatedInput: decision.updatedInput ?? (accumulated.action === 'modify' ? accumulated.updatedInput : undefined),
            updatedMessages: decision.updatedMessages ?? (accumulated.action === 'modify' ? accumulated.updatedMessages : undefined),
            additionalContext: decision.additionalContext ?? accumulated.additionalContext,
          };
        } else if (decision.action === 'ask') {
          // 'ask' forces an interactive permission prompt. It is stricter than
          // allow/modify (verdictRank 2) so it holds against a later allow and is
          // only overridden by a deny (handled above). Carry its reason +
          // updatedInput so the user is prompted to approve the hook's rewrite.
          // The carry values are read (with explicit types) BEFORE reassigning so
          // the new value does not reference `accumulated` circularly.
          const carryReason: string | undefined = accumulated.action === 'ask' ? accumulated.reason : undefined;
          // Both the prior `ask` and `modify` arms carry `updatedInput`; read it
          // structurally (allow/deny arms simply have none → undefined).
          const carryInput: Record<string, unknown> | undefined = (accumulated as { updatedInput?: Record<string, unknown> }).updatedInput;
          const carryCtx: string | undefined = accumulated.additionalContext;
          accumulated = {
            action: 'ask',
            reason: decision.reason ?? carryReason,
            updatedInput: decision.updatedInput ?? carryInput,
            additionalContext: decision.additionalContext ?? carryCtx,
          };
        } else if (decision.action === 'allow') {
          // An explicit "allow" can loosen a prior "modify"; the hold-over
          // check at the end guards against user-tier loosening. A prior "ask"
          // (forced prompt) is NOT loosened by a later allow — keep the prompt,
          // just fold in any additionalContext.
          if (accumulated.action === 'ask') {
            accumulated = { action: 'ask', reason: accumulated.reason, updatedInput: accumulated.updatedInput, additionalContext: decision.additionalContext ?? accumulated.additionalContext };
          } else {
            // Carry an EXPLICIT permission-bypass across the allow accumulation: once
            // any matching PreToolUse hook pre-approved (permissionDecision allow), the
            // combined verdict stays a bypass (a later bare allow does not clear it).
            const priorBypass: boolean | undefined = accumulated.action === 'allow' ? accumulated.permissionBypass : undefined;
            const bypass: boolean | undefined = decision.permissionBypass || priorBypass;
            accumulated = { action: 'allow', additionalContext: decision.additionalContext ?? accumulated.additionalContext, ...(bypass ? { permissionBypass: true } : {}) };
          }
        }
      } catch (err) {
        // Gap 2: Fail-closed — hook errors deny the action
        coreLog('hooks', 'Hook error (fail-closed)', { error: String(err) });
        return { action: 'deny' as const, reason: `Hook verification failed (fail-closed): ${err instanceof Error ? err.message : String(err)}` };
      }
    }

    // Gap 1: If denyRulesHold is active and a privileged tier had a stricter
    // verdict than the accumulated result, hold the privileged verdict.
    if (this._denyRulesHold && preUserVerdict && verdictRank(preUserVerdict.action) > verdictRank(accumulated.action)) {
      return { action: 'deny' as const, reason: 'Org-tier verdict held over user-tier loosening (denyRulesHold)' };
    }

    return accumulated;
  }

  private async runHookByType(
    config: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
    cwd: string,
    eventData: string,
    signal?: AbortSignal,
    // Out-param success signal (mirrors the reference's onHookSuccess): set to false
    // when the hook ERRORS/times out/blocks-via-error so a `once:true` hook is NOT
    // consumed by a failed attempt and stays registered to retry. A clean run
    // (exit 0 / no engine error / 2xx) leaves it true even if the hook returned a
    // DENY *decision* — that is a successful execution that happened to block.
    outcome?: HookOutcome,
    // Per-hook env-file hint: the real lifecycle event + this hook's index, so an
    // env-ingestible command hook receives its deterministic CLAUDE_ENV_FILE path
    // (see buildHookEnv). Absent for non-arm callers / non-ingestible events.
    envFileHint?: { event: string; index: number },
    // Live-output sink for progress streaming: when supplied (sync command-hook
    // path, or a prompt/agent engine hook), the runner writes its accumulating
    // output here as it arrives — runCommandHookAsync streams stdout/stderr, and
    // {@link HookManager.runEngineHook} streams the model's accumulating text —
    // so {@link HookManager.streamHookProgress} can poll it mid-run.
    liveOutput?: { stdout: string; stderr: string },
  ): Promise<string> {
    switch (config.type) {
      case 'command': {
        // the reference hook contract: config.timeout is in SECONDS. Route through
        // resolveHookTimeoutMs so it is clamped to [100ms, MAX_HOOK_TIMEOUT_MS].
        const timeout = resolveHookTimeoutMs(config.timeout, DEFAULT_HOOK_TIMEOUT_MS);
        const hookEnv = this.buildHookEnv(
          'command',
          eventData ? (() => { try { return JSON.parse(eventData).toolName; } catch { return undefined; } })() : undefined,
          envFileHint,
        );
        // Plugin/skill-contributed command hooks resolve their own base directory:
        // inject CLAUDE_PLUGIN_ROOT (+ AMI_PLUGIN_ROOT alias) into the child env and
        // substitute the `${CLAUDE_PLUGIN_ROOT}` placeholder in the command + args so
        // the hook can reference files co-located with its bundle regardless of the
        // session cwd (the reference plugin-hook env/substitution + skillRoot parity).
        const pluginRoot = config.pluginRoot;
        // A plugin command hook also gets a PERSISTENT per-plugin data directory
        // (the reference's CLAUDE_PLUGIN_DATA / getPluginDataDir): a stable scratch
        // dir, separate from the read-only bundle root, that survives across
        // sessions so a hook can cache state (`~/.ami/plugin-data/<id>`).
        const pluginDataDir = resolvePluginDataDir(pluginRoot);
        if (pluginRoot) {
          hookEnv.CLAUDE_PLUGIN_ROOT = pluginRoot;
          hookEnv.AMI_PLUGIN_ROOT = pluginRoot;
        }
        if (pluginDataDir) {
          // Create the data dir best-effort so a hook's `> $CLAUDE_PLUGIN_DATA/x`
          // write does not fail on a missing parent (fail-open: a mkdir error just
          // leaves the dir absent, same as before this feature).
          try { fs.mkdirSync(pluginDataDir, { recursive: true }); } catch { /* best-effort */ }
          hookEnv.CLAUDE_PLUGIN_DATA = pluginDataDir;
          hookEnv.AMI_PLUGIN_DATA = pluginDataDir;
          coreLog('hooks', `Command hook plugin data dir: ${pluginDataDir}`);
        }
        // A plugin command hook ALSO receives its user-configured option VALUES
        // (read from the bundle's `.options.json` sidecar): each value is exported
        // as CLAUDE_PLUGIN_OPTION_<KEY> (+ the AMI_PLUGIN_OPTION_<KEY> alias) and
        // the `${user_config.X}` placeholders in the command + args are substituted,
        // so a hook can resolve configuration without reading the sidecar itself
        // (the reference's CLAUDE_PLUGIN_OPTION_<KEY> + ${user_config.X} substitution,
        // utils/hooks.ts). Absent/empty for a non-plugin or unconfigured hook.
        const pluginOptions = pluginRoot ? readPluginOptionValues(pluginRoot) : undefined;
        if (pluginOptions && Object.keys(pluginOptions).length > 0) {
          Object.assign(hookEnv, buildPluginOptionEnv(pluginOptions));
          coreLog('hooks', `Command hook plugin options: ${Object.keys(pluginOptions).join(', ')}`);
        }
        const command = substituteUserConfigValues(substitutePluginData(substitutePluginRoot(config.command, pluginRoot), pluginDataDir), pluginOptions);
        const args = config.args?.map(a => substituteUserConfigValues(substitutePluginData(substitutePluginRoot(a, pluginRoot), pluginDataDir), pluginOptions));
        try {
          const { exitCode, stdout, stderr } = await runCommandHookAsync(command, cwd, hookEnv, timeout, eventData, args, signal, config.shell, this._promptRequestHandler, liveOutput);
          // Capture the raw result on the shared outcome so an async hook's
          // `hook_response` completion record can report the exit code and the
          // SEPARATELY-captured streams (parity: the reference's split stdout/stderr +
          // exitCode in emitHookResponse), independent of the decision parsed below.
          if (outcome) { outcome.exitCode = exitCode; outcome.stdout = stdout; outcome.stderr = stderr; }
          // the reference exit-code contract:
          //   0           = success — parse stdout for a decision
          //   2           = BLOCKING error — reason from stderr → DENY
          //   other != 0  = NON-blocking error — surface stderr, do NOT block the tool
          if (exitCode === 2) {
            // Exit 2 is a BLOCKING error, not a clean run: a once hook must retry.
            if (outcome) outcome.success = false;
            if (config.asyncRewake) {
              this.emit('hookRewake', { event: 'command', message: (stderr || stdout).trim() || 'Background hook completed with exit code 2', ...this.rewakeAttribution(config) });
            }
            return `DENY:${(stderr || stdout || 'Hook blocked (exit 2)').trim()}`;
          }
          if (exitCode !== 0) {
            // Non-zero (non-2) is a NON-blocking error — also not a successful run.
            if (outcome) outcome.success = false;
            const msg = (stderr || stdout).trim();
            return msg ? `SYSTEM:${msg}` : '';
          }
          const trimmed = stdout.trim();
          let rawParsed: unknown;
          let haveDecision = false;
          try {
            rawParsed = JSON.parse(trimmed);
            haveDecision = true;
          } catch (parseErr) {
            // Whole-blob parse failed. A command hook may interleave ordinary log
            // lines with its single JSON decision line, so scan line-by-line for
            // the first non-async JSON object (the reference's
            // checkForAsyncHookResponses stdout line-scan) before giving up —
            // otherwise a real decision buried under log output is silently
            // dropped and the whole stdout is mistaken for opaque plain text.
            const scanned = firstHookJsonLine(stdout);
            if (scanned !== undefined) {
              rawParsed = scanned;
              haveDecision = true;
            } else if (trimmed.startsWith('{')) {
              // The output LOOKED like a single JSON object but was malformed, and
              // no embedded JSON decision line was found — surface it as a
              // validation error (the reference parseHookOutput), NOT opaque text.
              return `SYSTEM:${sanitizeHookOutput(formatHookValidationError(trimmed, parseErr))}`;
            }
          }
          if (haveDecision) {
            const firedEvent: string | undefined =
              (() => { try { return JSON.parse(eventData).hook_event_name; } catch { return undefined; } })();
            // hookEventName spoof check: a hook whose hookSpecificOutput names a
            // DIFFERENT event than the one it fired for is rejected (not acted
            // on) and surfaced as a validation error — a failed run, so a `once`
            // hook retries (the reference parity).
            const spoof = hookEventNameMismatch(rawParsed, firedEvent);
            if (spoof) {
              if (outcome) outcome.success = false;
              return `SYSTEM:${sanitizeHookOutput(spoof)}`;
            }
            const parsed: HookDecisionOutput = unwrapHookSpecificOutput(rawParsed);
            // Strict shape validation (the reference parseHookOutput): a KNOWN
            // decision field with a wrong-typed value is malformed output, not a
            // silent no-op. Surface it as a validation error + failed run so a
            // `once` hook retries instead of mis-acting on e.g. continue:"yes".
            // An unknown-decision ENUM error is deliberately NOT short-circuited
            // here — the specialized permission/decision executors handle it
            // fail-closed downstream (unknownHookDecisionError), so only TYPE
            // errors on the remaining fields block at this layer.
            const typeErr = validateHookDecisionOutput(parsed as Record<string, unknown>);
            if (typeErr && !unknownHookDecisionError(parsed as Record<string, unknown>)) {
              if (outcome) outcome.success = false;
              return `SYSTEM:${sanitizeHookOutput(typeErr)}`;
            }
            if (parsed.decision === 'block' || parsed.decision === 'deny') return `DENY:${formatHookDenyReason(firedEvent, parsed.reason)}`;
            if (parsed.decision === 'ask' || parsed.permissionDecision === 'ask') return `ASK:${parsed.reason ?? ''}`;
            if (parsed.continue === false) return `DENY:${formatHookDenyReason(firedEvent, parsed.stopReason)}`;
            if (parsed.updatedInput) return `MODIFY:${JSON.stringify(parsed.updatedInput)}`;
            if (parsed.additionalContext) return `MODIFY:${JSON.stringify({ additionalContext: parsed.additionalContext })}`;
            if (parsed.systemMessage) return `SYSTEM:${parsed.systemMessage}`;
            // suppressOutput: a JSON hook result with suppressOutput:true hides
            // the plain-text success surface — return '' instead of echoing the
            // (otherwise decision-less) JSON back as a system message. Explicit
            // decision/systemMessage results above take precedence (the reference).
            if (parsed.suppressOutput) return '';
          }
          // No JSON decision (plain-text / DENY:/SYSTEM: prefixes, or a decision-less
          // JSON object): surface the sanitized output verbatim. Malformed single-
          // object output already returned a validation error from the catch above.
          return sanitizeHookOutput(trimmed);
        } catch (err: unknown) {
          // A thrown command (timeout / spawn failure) is not a successful run.
          if (outcome) outcome.success = false;
          coreLog('hooks', `Command hook error: ${err}`);
          // Fail-closed: a guardrail hook that times out or errors must DENY,
          // not silently allow the action through.
          if ((config as CommandHookConfig).onFailure === 'block') {
            return `DENY:Hook execution failed (fail-closed): ${err instanceof Error ? err.message : String(err)}`;
          }
          // Fail-OPEN: the hook did not block, but a crashed/timed-out hook must
          // not be invisible — emit a visible error notice (the reference's
          // hook_error_during_execution) so a misconfigured hook is observable.
          this.emitHookError(config, err instanceof Error ? err.message : String(err));
          return '';
        }
      }
      case 'prompt':
        return this.runPromptHook(config, eventData, signal, outcome, liveOutput);
      case 'http':
        return this.runHttpHook(config, eventData, signal, outcome);
      case 'agent':
        return this.runAgentHook(config, eventData, signal, outcome, liveOutput);
      default:
        return '';
    }
  }

  private async runPromptHook(config: PromptHookConfig, eventData: string, signal?: AbortSignal, outcome?: HookOutcome, live?: { stdout: string; stderr: string }): Promise<string> {
    const r = await this.runEngineHook('prompt', config.prompt, 'Event data', config.model, config.timeout, 30_000, eventData, signal, live);
    // An engine error (throw / hard-timeout) is a failed run — a once hook retries.
    if (r.error && outcome) outcome.success = false;
    // Fail-OPEN: a prompt hook whose sub-engine errored/timed out returns '' —
    // surface the error visibly so it is not silent (the reference parity).
    if (r.error) this.emitHookError(config, r.error);
    if (config.asyncRewake && r.error) this.emit('hookRewake', { event: 'prompt', message: `Prompt hook error: ${r.error}`, ...this.rewakeAttribution(config) });
    if (config.asyncRewake && r.output.startsWith('DENY:')) this.emit('hookRewake', { event: 'prompt', message: r.output, ...this.rewakeAttribution(config) });
    return r.output;
  }

  /**
   * Shared runner for engine-backed hooks (prompt + agent). Spins up a hook
   * sub-engine with:
   *  - `timeout` interpreted as SECONDS (resolveHookTimeoutMs), matching command/http;
   *  - a HARD timeout + parent-signal AbortController passed to the engine so a
   *    stalled model call is actively torn down (not a cooperative between-events
   *    deadline that a pre-first-event hang never reaches);
   *  - HOOK_AGENT_DISALLOWED_TOOLS filtered out so a hook agent cannot spawn
   *    subagents or enter plan mode.
   */
  private async runEngineHook(
    kind: 'prompt' | 'agent',
    promptText: string,
    dataLabel: string,
    model: string | undefined,
    timeoutSeconds: number | undefined,
    defaultMs: number,
    eventData: string,
    signal?: AbortSignal,
    // Live-output sink (progress streaming): when supplied, the accumulating model
    // text is mirrored here on each `text_delta` so {@link streamHookProgress} can
    // publish incremental progress while the hook sub-engine runs — the reference's
    // execPromptHook / execAgentHook live `setResponseLength` affordance. Absent
    // for callers that don't stream progress (background/async firing).
    live?: { stdout: string; stderr: string },
    // Per-run options threaded from the typed hook config. `enforceStructuredOutput`
    // re-prompts an agent hook that stopped without a verification-tool verdict
    // (the reference's registerStructuredOutputEnforcement); agent-only.
    opts?: { enforceStructuredOutput?: boolean },
  ): Promise<{ output: string; error?: string }> {
    if (!this._engineFactory) {
      coreLog('hooks', `${kind} hook skipped: no engine factory available`);
      return { output: '' };
    }
    const providerConfig = { ...(this._providerConfig || {}) };
    if (model) {
      providerConfig.model = model;
    } else {
      // A prompt OR agent hook is a cheap verification verdict — default it to the
      // operator-configured small/fast model when one is set, so it doesn't burn the
      // main model's budget/latency. Parity: BOTH execPromptHook AND execAgentHook
      // default `model: hook.model ?? getSmallFastModel()` (execAgentHook.ts:L118).
      // With no env set, getSmallFastModel() returns undefined and the session model
      // is kept unchanged (current behavior preserved).
      const fast = getSmallFastModel();
      if (fast) providerConfig.model = fast;
    }

    const timeoutMs = resolveHookTimeoutMs(timeoutSeconds, defaultMs);
    const abort = combineHookAbort(signal, timeoutMs);

    // AGENT hooks are multi-turn stop/plan verifiers: give them a dedicated
    // verification system prompt, tell them where the conversation transcript is,
    // and scope-allow Read of exactly that transcript file so they can inspect the
    // prior conversation without a permission prompt (the reference execAgentHook
    // parity). Prompt hooks are single-shot verdicts and get none of this.
    const agentExtras: Record<string, unknown> = {};
    if (kind === 'agent') {
      const transcriptPath = this.resolveHookTranscriptPath();
      agentExtras.appendSystemPrompt = buildAgentHookSystemPrompt(transcriptPath);
      if (transcriptPath) {
        agentExtras.permissionRules = [{ tool: 'file_read', pattern: transcriptPath, action: 'allow' as const }];
      }
    }

    // PROMPT hooks evaluate a single-shot condition; when a conversation-history
    // provider is wired, seed the hook sub-engine with the live conversation so the
    // LLM judges the condition IN CONTEXT (e.g. "block only if the user did not
    // already agree earlier") rather than against the self-contained event payload
    // alone. The seeded messages become the sub-engine's prior turns and the hook's
    // composed user message is appended by submit() — exactly the reference's
    // execPromptHook `messagesToQuery = [...messages, userMessage]`. AGENT hooks are
    // excluded on purpose: they inspect the transcript FILE via the scoped Read
    // grant above, so inlining history too would double the context. An empty/absent
    // history leaves the prior (no-seed) behavior unchanged.
    const promptExtras: Record<string, unknown> = {};
    if (kind === 'prompt' && this._conversationHistoryProvider) {
      const history = this._conversationHistoryProvider();
      if (history.length > 0) promptExtras.initialMessages = [...history];
    }

    const engine = this._engineFactory({
      provider: providerConfig,
      cwd: this._cwd,
      // Advertise the reusable verification-result tool so an AI-driven hook can
      // return a one-call structured {ok, reason} verdict instead of free prose
      // (parity: the reference's createStructuredOutputTool, used by agent/prompt
      // hooks and background verification). The {ok,reason} text contract below
      // stays as the fallback, so a hook that answers in prose still parses.
      tools: [createVerificationOutputTool()],
      ...agentExtras,
      ...promptExtras,
      // Run under the session's permission context (not blanket auto-allow unless
      // that is the session's mode), so a hook agent cannot exceed the user's grant.
      permissionMode: this._hookEnginePermissionMode,
      abortController: abort.controller,
      disallowedTools: HOOK_AGENT_DISALLOWED_TOOLS,
      // Disable extended thinking for hook evaluation — a verification hook needs a
      // fast structured verdict, not a reasoning trace (the reference's
      // thinkingConfig:{type:'disabled'} for prompt/agent hooks).
      disableThinking: true,
      // Hard turn-count cap on agent (multi-turn) hooks so a verification agent
      // cannot loop indefinitely even inside the timeout budget (MAX_AGENT_TURNS).
      // Prompt hooks are single-shot, so the cap only applies to the agent kind.
      ...(kind === 'agent' ? { maxTurns: MAX_AGENT_TURNS } : {}),
    });

    // Compose the hook prompt, then prepend the {ok, reason} verification contract
    // so an AI-driven hook returns a parseable decision rather than free prose
    // (the reference's hookResponseSchema). When the prompt references the hook input
    // via a placeholder ($ARGUMENTS / $ARGUMENTS[n] / $n), substitute it in place
    // (the reference's addArgumentsToPrompt); otherwise append the event data under
    // the per-kind `dataLabel` ('Event data' / 'Context').
    const hasArgPlaceholder = /\$ARGUMENTS|\$\d/.test(promptText);
    const body = hasArgPlaceholder
      ? substituteHookArguments(promptText, eventData)
      : `${promptText}\n\n${dataLabel}:\n${eventData}`;
    const fullPrompt = `${HOOK_VERIFICATION_CONTRACT}\n\n${body}`;
    // Introspection debug trace (the reference execPromptHook's logForDebugging
    // "Processing prompt hook with prompt: …"): the exact processed prompt the
    // hook evaluates, so `--debug` / AMI_DEBUG_TO_STDERR reveals what it saw.
    coreLog('hooks', `${kind} hook processing prompt: ${body}`);
    let result = '';
    // Structured verdict captured from a verification-tool call (parity: the reference
    // Code's createStructuredOutputTool one-call verdict), preferred over the
    // free-text {ok,reason} contract when the hook used the tool.
    let toolVerdict: { ok: boolean; reason?: string } | undefined;
    let error: string | undefined;
    // Distinguish a cancelled/timed-out/errored run from a genuine completion: a
    // run that did not finish produced NO trustworthy verdict, so its partial
    // model text must NEVER be returned as a hook decision (the reference's
    // execPromptHook/execAgentHook return a 'cancelled'/non_blocking_error
    // outcome on abort/error, not the partial text).
    let errored = false;
    // Capture how the hook sub-engine terminated so a run that exhausted the
    // MAX_AGENT_TURNS cap (a multi-turn agent hook that never converged on a
    // verdict) gets a DISTINCT diagnostic — separate from a timeout/abort/error
    // or a conforming/non-conforming reply (the reference's execAgentHook logs
    // "Agent hook did not complete within N turns" as its own outcome, so an
    // operator can tell a stop/verify hook gave up on turns vs. on the clock).
    let exitReason: string | undefined;
    // Recursion guard (the reference's execPromptHook builds the user message directly
    // via createUserMessage "to avoid re-firing UserPromptSubmit hooks and cause
    // infinite recursion"): mark that an engine-backed hook is running so a sub-engine
    // sharing this HookManager cannot re-enter the UserPromptSubmit pipeline and
    // fork-bomb. {@link executeUserPromptSubmit} short-circuits while the depth is > 0.
    // Consume one engine submit stream into the shared accumulators (result /
    // toolVerdict / exitReason / error). Factored out so the structured-output
    // enforcement re-prompt can drive a SECOND submit through the identical event
    // handling without duplicating it.
    const consume = async (stream: AsyncIterable<{ type: string; text?: string; error?: string; toolName?: string; input?: Record<string, unknown>; exitReason?: string }>): Promise<void> => {
      for await (const event of stream) {
        if (abort.aborted()) break;
        if (event.type === 'text_delta' && event.text) {
          result += event.text;
          // Mirror the accumulating model text into the live sink so the progress
          // poll surfaces partial output/length while the hook sub-engine streams
          // (the reference's live setResponseLength affordance). No-op when the
          // caller supplied no sink (background/async firing).
          if (live) live.stdout = result;
        }
        // A verification-tool call carries the verdict directly in its input — the
        // last such call wins (a hook calls it exactly once per its instruction).
        if (event.type === 'tool_use_start' && event.toolName === VERIFICATION_OUTPUT_TOOL_NAME) {
          const v = parseVerificationToolInput(event.input);
          if (v) toolVerdict = v;
        }
        if (event.type === 'done') exitReason = event.exitReason;
        if (event.type === 'error') { error = event.error || 'hook engine error'; errored = true; break; }
      }
    };
    this._engineHookDepth++;
    try {
      await consume(engine.submit(fullPrompt));
      // Structured-output enforcement (the reference's registerStructuredOutputEnforcement,
      // hookHelpers.ts L66-L83): an agent hook that stopped WITHOUT ever calling the
      // verification tool produced no machine-readable verdict. When the hook opted in
      // via `enforceStructuredOutput`, re-prompt it ONCE with a precise nudge naming
      // the tool, instead of silently falling back to free-text. Only when the first
      // run completed cleanly (not aborted / errored) — a cancelled/errored run has no
      // model to nudge. Agent-only (prompt hooks are single-shot by contract).
      if (
        kind === 'agent' && opts?.enforceStructuredOutput &&
        !toolVerdict && !errored && !abort.aborted()
      ) {
        coreLog('hooks', `agent hook stopped without a ${VERIFICATION_OUTPUT_TOOL_NAME} verdict — enforcing structured output (one re-prompt)`);
        await consume(engine.submit(buildStructuredOutputEnforcementNudge()));
      }
    } catch (err) {
      error = String(err);
      errored = true;
      coreLog('hooks', `${kind} hook error: ${err}`);
    } finally {
      this._engineHookDepth--;
      abort.cleanup();
      engine.shutdown?.();
    }
    if (abort.timedOut()) coreLog('hooks', `${kind} hook hard-timeout after ${timeoutMs}ms`);
    // Turn-budget exhaustion diagnostic, distinct from a timeout: an agent hook
    // that ran out the MAX_AGENT_TURNS cap without converging on a verdict is a
    // different operator signal than a wall-clock timeout (the reference's "Agent
    // hook did not complete within N turns"). Only agent (multi-turn) hooks carry
    // the cap, so this is reported for `agent` kind only, and only when the run
    // produced no structured verdict (a hook that already answered via the
    // verification tool completed regardless of the engine's exit reason).
    // Surfaced on the observable `hookError` channel (engine → debug log +
    // analytics), never fed to the model, so the give-up is introspectable.
    if (
      kind === 'agent' && !toolVerdict && !abort.timedOut() && !abort.aborted() &&
      (exitReason === 'max_turns' || exitReason === 'max_steps')
    ) {
      const msg = `Agent hook did not complete within ${MAX_AGENT_TURNS} turns (exitReason: ${exitReason})`;
      coreLog('hooks', msg);
      this.emit('hookError', { hookType: 'agent', hook: 'agent hook', error: msg });
    }
    // Introspection debug trace (the reference's "Model response: …"): the raw
    // reply the hook produced, logged before any decision is derived from it.
    coreLog('hooks', `${kind} hook model reply: ${result}`);
    // A cancelled/timed-out/errored run yields no decision — discard the partial
    // output (never block/allow on half a reply) and report the run as failed so
    // a `once` hook retries instead of self-removing.
    if (abort.aborted() || errored) {
      const why = abort.timedOut() ? 'timed out' : abort.aborted() ? 'cancelled' : `errored: ${error}`;
      coreLog('hooks', `${kind} hook did not complete (${why}) — discarding partial output`);
      return { output: '', error: error ?? why };
    }
    // Structured verification result → block (ok:false) or allow (ok:true). A
    // one-call verdict from the verification tool is authoritative; otherwise fall
    // back to parsing a free-text {ok,reason} object from the model's reply.
    const verification = toolVerdict ?? parseHookVerification(result);
    if (verification) {
      // the reference wording parity: "<Prompt|Agent> hook condition was not met: …"
      const label = kind === 'agent' ? 'Agent' : 'Prompt';
      coreLog('hooks', verification.ok
        ? `${kind} hook condition was met`
        : `${kind} hook condition was not met: ${verification.reason ?? 'condition not met'}`);
      return {
        output: verification.ok ? '' : `DENY:${label} hook condition was not met: ${verification.reason ?? 'condition not met'}`,
        error,
      };
    }
    // Non-conforming output (did not match the {ok,reason} schema): surface a
    // distinct, non-blocking diagnostic (the reference's hook_non_blocking_error
    // "model response does not conform to expected schema") instead of silently
    // treating free-form prose as a verdict. AMI keeps its SYSTEM:/raw-context
    // passthrough superset here — the output is still applied as context, never
    // as a block — so the run stays non-blocking exactly as the reference's
    // non_blocking_error outcome does.
    coreLog('hooks', `${kind} hook output did not match the {ok,reason} schema — treating as non-blocking raw output`);
    return { output: sanitizeHookOutput(result), error };
  }

  private async runHttpHook(config: HttpHookConfig, eventData: string, signal?: AbortSignal, outcome?: HookOutcome): Promise<string> {
    const http = require('http');
    const https = require('https');
    // Fail-closed: an HTTP hook that times out or errors DENIES when onFailure:block.
    // Any failure path (allowlist/SSRF block, non-2xx, transport error, timeout) is
    // NOT a successful run — a once hook stays registered to retry (the reference parity).
    const onFail = () => { if (outcome) outcome.success = false; return config.onFailure === 'block' ? 'DENY:HTTP hook failed (fail-closed)' : ''; };
    // Fail-closed WITH the underlying cause: a transport error / timeout surfaces
    // its specific detail in the DENY reason and the debug log (the reference's
    // execHttpHook `logForDebugging("HTTP hook error: <msg>")` + distinct errorMsg),
    // instead of a generic "failed" with no diagnosable cause.
    const onFailWith = (detail: string) => {
      coreLog('hooks', `HTTP hook error: ${detail}`);
      if (outcome) outcome.success = false;
      // Fail-OPEN transport error/timeout would otherwise return '' silently —
      // surface it visibly (the reference parity). The block path already DENYs.
      if (config.onFailure !== 'block') this.emitHookError(config, detail);
      return config.onFailure === 'block' ? `DENY:HTTP hook failed: ${detail}` : '';
    };

    // (1) URL allowlist gate (three-state) — enforced BEFORE any I/O.
    if (!isHttpHookUrlAllowed(config.url, this._httpHookPolicy.allowedHttpHookUrls)) {
      coreLog('hooks', `HTTP hook blocked: ${config.url} not in allowedHttpHookUrls`);
      return onFail();
    }

    // (2) Header env-var interpolation, gated by the hook's allowedEnvVars ∩ the
    // global httpHookAllowedEnvVars setting. Built BEFORE the transport split so the
    // direct AND the proxied paths share the exact same interpolated/sanitized set.
    const envAllow = new Set(
      (config.allowedEnvVars ?? []).filter(v => (this._httpHookPolicy.httpHookAllowedEnvVars ?? []).includes(v)),
    );
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    for (const [k, v] of Object.entries(config.headers ?? {})) {
      // Surface the reference's explanatory warning (execHttpHook.ts:L89-108) when a
      // header references an env var that is NOT on the allowlist, so the user learns
      // WHY `$TOKEN` resolved to empty instead of being silently dropped.
      headers[k] = interpolateHookHeaderEnv(v, envAllow, process.env, (name) =>
        coreLog('hooks', `HTTP hook header env var $${name} not in allowedEnvVars, skipping interpolation`),
      );
    }

    // (3) Cancellation + hard timeout as ONE combined signal (the canonical
    // combineHookAbort): the request is torn down on parent/user cancel OR the
    // per-hook timeout, matching command/prompt/agent hooks.
    const timeoutMs = resolveHookTimeoutMs(config.timeout, DEFAULT_HTTP_HOOK_TIMEOUT_MS);
    const abort = combineHookAbort(signal, timeoutMs);

    // Everything between here and the request Promise can throw/reject BEFORE the
    // Promise's done() (which owns abort.cleanup()) is wired up: an unparseable
    // `config.url` makes validateUrlSafety (and the subsequent `new URL`) throw, and
    // a lazy proxy import can reject. Any such throw must still release the
    // combineHookAbort deadline timer — otherwise it is left armed (10-min default)
    // and pins the event loop until it fires (a resource leak surfaced by an
    // invalid-URL hook). Cleanup + rethrow keeps the caller-visible rejection intact.
    let resolvedIP: string;
    let url: URL;
    try {
      // (4) Env-var proxy routing (HTTP(S)_PROXY, honoring NO_PROXY) — the reference's
      // execHttpHook routes the POST through a configured proxy and, in that case,
      // SKIPS the local SSRF/pinned-DNS guard: the proxy performs DNS for the target
      // and enforces its own egress allowlist (applying the guard would instead
      // validate the proxy's own — possibly private, e.g. 10.0.0.1:3128 — address and
      // wrongly block a corporate proxy). The URL allowlist above still applies. The
      // proxy URL + dispatcher come from the canonical provider helpers (no second
      // proxy implementation), loaded lazily to avoid a static import of the AI-SDK
      // stack from this leaf hook module. The cheap env-presence gate (the SAME gate
      // the canonical withProxyDispatcher uses) short-circuits the common no-proxy
      // case so an unproxied http hook never pays the provider-import cost.
      const anyProxyEnv = !!(
        process.env.https_proxy ?? process.env.HTTPS_PROXY ??
        process.env.http_proxy ?? process.env.HTTP_PROXY
      );
      if (anyProxyEnv) {
        const { resolveProxyForUrl } = await import('./provider');
        if (resolveProxyForUrl(process.env, config.url)) {
          coreLog('hooks', `HTTP hook POST to ${config.url} (via env-var proxy)`);
          return this.runHttpHookViaProxy(config, eventData, headers, abort, timeoutMs, onFail, onFailWith);
        }
      }

      // (5) SSRF guard (direct connections only) — resolve + validate the hostname,
      // reusing the shared web-utils validator (blocks private/link-local/metadata
      // IPs) and pin the resolved IP to stop DNS-rebinding (TOCTOU). Loopback is
      // allowed (local dev webhook servers), matching the reference's ssrfGuardedLookup.
      const safety = await this._validateUrlSafety(config.url, undefined, { allowLoopback: true });
      if ('error' in safety) {
        // User-facing, code-tagged block message (the reference's ssrfError): always
        // inform why the hook was refused — fail-closed DENY when onFailure:block,
        // otherwise a non-blocking SYSTEM notice so the target is never silently
        // dropped (the affordance the reference surfaces to the user).
        const msg = formatHttpHookBlockedMessage(config.url, safety.error);
        coreLog('hooks', msg);
        if (outcome) outcome.success = false;
        abort.cleanup();
        return config.onFailure === 'block' ? `DENY:${msg}` : `SYSTEM:${msg}`;
      }
      resolvedIP = safety.resolvedIP;

      url = new URL(config.url);
    } catch (err) {
      // Release the still-armed deadline timer before the rejection propagates so a
      // failed-URL / failed-proxy-import hook cannot leave the event loop pinned.
      abort.cleanup();
      throw err;
    }
    const client = url.protocol === 'https:' ? https : http;
    // Introspection: log the outbound request target before any I/O (the reference's
    // execHttpHook `logForDebugging("Hooks: HTTP hook POST to <url>")`).
    coreLog('hooks', `HTTP hook POST to ${config.url}`);
    // Classify a transport failure: a combined-signal abort is a timeout/cancel
    // (name it so), anything else carries the Node error message.
    const failDetail = (err?: Error): string =>
      abort.timedOut() ? `timed out after ${timeoutMs}ms`
        : abort.aborted() ? 'request cancelled'
          : (err?.message || 'transport error');
    return new Promise((resolve) => {
      let settled = false;
      const done = (val: string) => { if (!settled) { settled = true; abort.cleanup(); resolve(val); } };
      const req = client.request(url, {
        method: 'POST',
        headers,
        signal: abort.signal,
        maxRedirects: 0,                       // stop redirect-based SSRF bypass
        lookup: pinnedDnsLookup(resolvedIP),
      }, (res: any) => {
        let body = '';
        res.on('data', (d: Buffer) => body += d);
        res.on('end', () => {
          // (4) Non-2xx is a failure — do NOT parse an error body as a decision.
          const status = res.statusCode ?? 0;
          // Introspection: response status + body length (the reference's
          // execHttpHook `logForDebugging("… response status <code>, body length <n>")`).
          coreLog('hooks', `HTTP hook response status ${status}, body length ${body.length}`);
          if (status < 200 || status >= 300) {
            // Non-2xx is "not ok" but not a transport error — keep the generic
            // fail-closed DENY (the specific status is already in the debug log).
            coreLog('hooks', `HTTP hook non-2xx (${status})`);
            done(onFail());
            return;
          }
          done(sanitizeHookOutput(body));
        });
        res.on('error', (err: Error) => done(onFailWith(failDetail(err))));
      });
      req.on('error', (err: Error) => done(onFailWith(failDetail(err))));
      req.write(eventData);
      req.end();
    });
  }

  /**
   * POST an HTTP hook through a configured env-var proxy (the reference's
   * execHttpHook proxy branch). The request is routed through the canonical
   * provider proxy/mTLS dispatcher wrapper ({@link withProxyDispatcher}) rather
   * than the raw http/https client — so there is ONE proxy implementation shared
   * with the model transport, not a second one here. The SSRF/pinned-DNS guard is
   * deliberately NOT applied: the proxy performs DNS for the target and enforces
   * its own egress allowlist (see the reordered gate in {@link runHttpHook}).
   * `redirect:'manual'` is the fetch equivalent of the direct path's
   * `maxRedirects:0`, keeping the redirect-based-SSRF-bypass protection.
   */
  private async runHttpHookViaProxy(
    config: HttpHookConfig,
    eventData: string,
    headers: Record<string, string>,
    abort: ReturnType<typeof combineHookAbort>,
    timeoutMs: number,
    onFail: () => string,
    onFailWith: (detail: string) => string,
  ): Promise<string> {
    const failDetail = (err?: Error): string =>
      abort.timedOut() ? `timed out after ${timeoutMs}ms`
        : abort.aborted() ? 'request cancelled'
          : (err?.message || 'transport error');
    try {
      // Default lazily to the provider proxy/mTLS dispatcher wrapper; a wrapper is
      // only installed when a proxy/mTLS env is present (it is here, by construction)
      // so it routes the request through the undici ProxyAgent. undici absent → the
      // wrapper is a no-op passthrough, which still succeeds for a reachable target.
      let doFetch = this._proxiedHookFetch;
      if (!doFetch) {
        const { withProxyDispatcher } = await import('./provider');
        doFetch = withProxyDispatcher(undefined, process.env) ?? fetch;
      }
      const res = await doFetch(config.url, {
        method: 'POST',
        headers,
        body: eventData,
        signal: abort.signal,
        redirect: 'manual', // == direct path's maxRedirects:0 (redirect-SSRF guard)
      });
      abort.cleanup();
      const body = await res.text();
      coreLog('hooks', `HTTP hook response status ${res.status}, body length ${body.length}`);
      if (res.status < 200 || res.status >= 300) {
        coreLog('hooks', `HTTP hook non-2xx (${res.status})`);
        return onFail();
      }
      return sanitizeHookOutput(body);
    } catch (err) {
      abort.cleanup();
      return onFailWith(failDetail(err instanceof Error ? err : undefined));
    }
  }

  private async runAgentHook(config: AgentHookConfig, eventData: string, signal?: AbortSignal, outcome?: HookOutcome, live?: { stdout: string; stderr: string }): Promise<string> {
    const r = await this.runEngineHook('agent', config.prompt, 'Context', config.model, config.timeout, 60_000, eventData, signal, live, { enforceStructuredOutput: config.enforceStructuredOutput });
    // An engine error (throw / hard-timeout) is a failed run — a once hook retries.
    if (r.error && outcome) outcome.success = false;
    // Fail-OPEN: surface a visible error notice when the agent hook errored so it
    // is not silent (the reference parity).
    if (r.error) this.emitHookError(config, r.error);
    return r.output;
  }

  private removeHookOnce<T>(hookArray: T[], hookFn: T, event?: string): void {
    const idx = hookArray.indexOf(hookFn);
    if (idx >= 0) {
      hookArray.splice(idx, 1);
      // Observable one-shot lifecycle (the reference's
      // "Removing one-shot hook for event <name>" debug line).
      if (event) coreLog('hooks', `Removing one-shot hook for event ${event}`);
    }
  }
}

/**
 * Evaluate a hook `if` condition. Beyond a bare tool-NAME regex, this supports
 * the reference's `Tool(content)` rule syntax (e.g. `Bash(git *)`,
 * `Edit(src/**)`) — it checks the tool name matches the rule's tool and then
 * glob-matches the inner content against the tool's input, so a hook can be
 * gated to e.g. only git commands or only certain paths rather than every Bash.
 */
export function matchesHookIf(
  ifCond: string,
  toolName: string,
  toolInput: Record<string, unknown>,
): boolean {
  const m = TOOL_IF_RE.exec(ifCond.trim());
  if (!m) return matchesPattern(ifCond, toolName);
  const ruleTool = translateHookMatcher(m[1]);
  const inner = m[2];
  if (!matchesPattern(ruleTool, toolName)) return false;
  if (inner === '' || inner === '*') return true;
  return wildcardMatch(inner, hookInputContent(toolInput));
}

