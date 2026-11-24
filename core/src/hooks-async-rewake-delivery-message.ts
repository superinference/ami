// Split from hooks.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import { isEnvTruthy } from './privacy-level';
import { VERIFICATION_OUTPUT_TOOL_NAME } from './tools/synthetic-output';
import type { ElicitationHookResult } from './hooks-stop-hook-summary';
import { extractHookBlockingError } from './hooks-validate-hook-decision-output';

/**
 * Decode a COMPLETED background ('async') hook's result sentinel — the string
 * {@link HookManager} (runHookByType) returns — into a human-facing re-wake
 * message, or `undefined` when there is nothing to deliver.
 *
 * This is the delivery side of the reference's `checkForAsyncHookResponses`: a
 * backgrounded hook that exits 0 (or non-blocking non-zero) with
 * `additionalContext` / `systemMessage` / `updatedInput` / plain stdout produces
 * a result that the fire-and-forget path would otherwise DISCARD (the turn has
 * already moved on without awaiting it). The hook's result is parsed once and
 * re-injected via the existing `hookRewake` channel.
 *
 * A DENY result (command exit 2, prompt engine-error / DENY) ALREADY emitted its
 * own `hookRewake` at the source inside runHookByType, so this returns
 * `undefined` for it — the completed-hook delivery never double-fires. A decoded
 * sentinel the command path can emit:
 *  - `SYSTEM:<msg>`   → the warning/non-blocking message
 *  - `ASK:<reason>`   → an approval-required notice
 *  - `MODIFY:{additionalContext}` → the additional context string
 *  - `MODIFY:<updatedInput>`      → an updated-tool-input notice
 *  - plain text       → delivered verbatim
 */
export function asyncRewakeDeliveryMessage(result: string): string | undefined {
  const trimmed = (result ?? '').trim();
  if (!trimmed) return undefined;
  // DENY / exit-2 / engine-error outputs already re-woke at their source.
  if (trimmed.startsWith('DENY:')) return undefined;
  if (trimmed.startsWith('SYSTEM:')) {
    const msg = trimmed.slice('SYSTEM:'.length).trim();
    return msg || undefined;
  }
  if (trimmed.startsWith('ASK:')) {
    const reason = trimmed.slice('ASK:'.length).trim();
    return reason ? `Requires approval: ${reason}` : 'Requires approval';
  }
  if (trimmed.startsWith('MODIFY:')) {
    const payload = trimmed.slice('MODIFY:'.length);
    let parsed: { additionalContext?: unknown };
    try {
      parsed = JSON.parse(payload);
    } catch {
      // Malformed MODIFY payload carries nothing to deliver.
      return undefined;
    }
    if (typeof parsed.additionalContext === 'string' && parsed.additionalContext.trim()) {
      return parsed.additionalContext.trim();
    }
    // Any other MODIFY payload is the raw updated-tool-input object.
    return `Updated tool input: ${payload}`;
  }
  // Plain-text success output from a background hook.
  return trimmed;
}

/** Global hook kill-switch via environment (the reference's `CLAUDE_CODE_SIMPLE`):
 *  when set truthy, EVERY hook source is skipped at load time — a fast,
 *  config-file-free escape hatch for a user who wants a hook-free session. The
 *  AMI-native aliases `AMI_SIMPLE` / `AMI_DISABLE_HOOKS` are honored too. Pure;
 *  consulted by {@link HookManager.isAllHooksDisabled} alongside the managed
 *  `disableAllHooks` policy. */
export function isHookKillSwitchEnv(env: Record<string, string | undefined> = process.env): boolean {
  return isEnvTruthy(env.CLAUDE_CODE_SIMPLE) || isEnvTruthy(env.AMI_SIMPLE) || isEnvTruthy(env.AMI_DISABLE_HOOKS);
}

/**
 * Canonical best-effort JSON decode of a hook's raw stdout. Returns the parsed
 * object (for reading structured fields like watchPaths / additionalContexts /
 * newCustomInstructions / elicitation action) or undefined when the output is
 * not a JSON object.
 */
export function tryParseHookJson(result: string): Record<string, unknown> | undefined {
  if (typeof result !== 'string') return undefined;
  const trimmed = result.trim();
  if (!trimmed || (trimmed[0] !== '{' && trimmed[0] !== '[')) return undefined;
  try {
    const parsed = JSON.parse(trimmed);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
  } catch { /* not JSON */ }
  return undefined;
}

/**
 * Canonical merge of user-supplied compaction instructions with hook-injected
 * ones (mirrors the reference's mergeHookInstructions): hook text is appended,
 * blank-line separated; either side may be empty.
 */
export function mergeHookInstructions(
  userInstructions: string | undefined | null,
  hookInstructions: string | undefined | null,
): string | undefined {
  if (!hookInstructions) return userInstructions || undefined;
  if (!userInstructions) return hookInstructions;
  return `${userInstructions}\n\n${hookInstructions}`;
}

/** Collect additional-context strings a hook contributed, from any of:
 *  `additionalContexts: string[]`, `additionalContext: string`,
 *  `systemMessage: string`, or the flattened `SYSTEM:<text>` sentinel. Shared by
 *  SubagentStart / other context-injecting consumers. */
export function extractAdditionalContexts(result: string): string[] {
  const out: string[] = [];
  const obj = tryParseHookJson(result);
  if (obj) {
    if (Array.isArray(obj.additionalContexts)) {
      for (const c of obj.additionalContexts) if (typeof c === 'string' && c) out.push(c);
    }
    if (typeof obj.additionalContext === 'string' && obj.additionalContext) out.push(obj.additionalContext);
    if (typeof obj.systemMessage === 'string' && obj.systemMessage) out.push(obj.systemMessage);
  } else if (typeof result === 'string' && result.startsWith('SYSTEM:')) {
    const text = result.slice(7).trim();
    if (text) out.push(text);
  }
  return out;
}

/** Collect ONLY the USER-facing `systemMessage` string(s) a hook contributed —
 *  the human-facing WARNING channel (the reference's `systemMessage`, described as
 *  "Warning message shown to the user" and yielded as a distinct
 *  `hook_system_message` attachment, NOT model-facing `additionalContext`).
 *  Accepts the JSON `systemMessage` field or the flattened `SYSTEM:<text>`
 *  sentinel that the command-hook path emits. Distinct from
 *  {@link extractAdditionalContexts} (which routes the same text INTO the model's
 *  context for the SubagentStart/SessionStart consumers): this is the channel a
 *  consumer uses to WARN the human ("this edit touches a protected file") without
 *  injecting it into the model prompt. Returns `[]` when the hook produced no
 *  user-facing warning. */
export function extractSystemMessages(result: string): string[] {
  const out: string[] = [];
  const obj = tryParseHookJson(result);
  if (obj) {
    if (typeof obj.systemMessage === 'string' && obj.systemMessage.trim()) out.push(obj.systemMessage.trim());
  } else if (typeof result === 'string' && result.startsWith('SYSTEM:')) {
    const text = result.slice('SYSTEM:'.length).trim();
    if (text) out.push(text);
  }
  return out;
}

/** Collect ONLY the model-facing `additionalContexts[]` / `additionalContext`
 *  strings a SessionStart/Setup hook contributed — distinct from
 *  {@link extractAdditionalContexts}, this deliberately EXCLUDES `systemMessage`
 *  and the `SYSTEM:` sentinel because SessionStart/Setup already route those into
 *  an `initialUserMessage`. the reference accumulates these across every hook into a
 *  single `hook_additional_context` attachment. */
export function extractSessionAdditionalContexts(result: string): string[] {
  const out: string[] = [];
  const obj = tryParseHookJson(result);
  if (obj) {
    if (Array.isArray(obj.additionalContexts)) {
      for (const c of obj.additionalContexts) if (typeof c === 'string' && c) out.push(c);
    }
    if (typeof obj.additionalContext === 'string' && obj.additionalContext) out.push(obj.additionalContext);
  }
  return out;
}

/** Decode an elicitation hook's output into a structured result: a programmatic
 *  `action`/`content` response (short-circuits the user prompt) or a
 *  `blockingError` (→ decline). A DENY sentinel maps to a blockingError. */
export function parseElicitationResult(result: string): ElicitationHookResult | undefined {
  const blockingError = extractHookBlockingError(result);
  if (blockingError) return { blockingError };
  const obj = tryParseHookJson(result);
  if (!obj) return undefined;
  const out: ElicitationHookResult = {};
  if (obj.action === 'accept' || obj.action === 'decline' || obj.action === 'cancel') out.action = obj.action;
  if (obj.content && typeof obj.content === 'object' && !Array.isArray(obj.content)) out.content = obj.content as Record<string, unknown>;
  if (typeof obj.blockingError === 'string' && obj.blockingError) out.blockingError = obj.blockingError;
  if (out.action || out.content || out.blockingError) return out;
  return undefined;
}

/**
 * System prompt for a multi-turn AGENT hook (stop/plan verification). Unlike a
 * single-shot prompt hook, an agent hook is a sub-engine that can use tools to
 * inspect the codebase AND read the conversation transcript to decide whether a
 * stop condition is met. This dedicated system prompt (passed via the sub-engine's
 * `appendSystemPrompt`) frames the verification task, tells the agent exactly where
 * the transcript lives (so it can analyze prior conversation when `transcriptPath`
 * is known), nudges it to be efficient, and restates the `{ok, reason}` output
 * contract so it returns a parseable verdict. Parity: the reference's execAgentHook
 * verification system prompt (utils/hooks/execAgentHook.ts) — AMI keeps its own
 * text-JSON `{ok, reason}` contract rather than a synthetic structured-output tool.
 */
export function buildAgentHookSystemPrompt(transcriptPath?: string): string {
  const transcriptLines = transcriptPath
    ? `The conversation transcript is available at: ${transcriptPath}\n` +
      'You may read this file to analyze the conversation history if needed.\n\n'
    : '';
  return (
    'You are verifying a stop condition. Your task is to verify that the agent ' +
    'completed the given plan or condition.\n\n' +
    transcriptLines +
    'Use the available tools to inspect the codebase and verify the condition.\n' +
    'Use as few steps as possible - be efficient and direct.\n\n' +
    'When done, respond with EXACTLY one JSON object and nothing else: ' +
    '{"ok": true} if the condition is met, or {"ok": false, "reason": "<why>"} if it is not.'
  );
}

/**
 * The exact re-prompt nudge used to enforce structured output when an agent hook
 * stops without emitting its machine-readable verdict. Mirrors the reference's
 * `registerStructuredOutputEnforcement` Stop-hook message (hookHelpers.ts L80:
 * `You MUST call the ${SYNTHETIC_OUTPUT_TOOL_NAME} tool to complete this request.
 * Call this tool now.`). Shared so the wording lives in ONE place and tests can
 * assert on it verbatim. `toolName` defaults to the verification tool AMI advertises
 * to hook sub-engines ({@link VERIFICATION_OUTPUT_TOOL_NAME}).
 */
export function buildStructuredOutputEnforcementNudge(toolName: string = VERIFICATION_OUTPUT_TOOL_NAME): string {
  return `You MUST call the ${toolName} tool to complete this request. Call this tool now.`;
}

/**
 * Substitute the hook input JSON into a prompt/agent hook's prompt template
 * (the reference's addArgumentsToPrompt / substituteArguments):
 *   - `$ARGUMENTS`        → the full hook-input JSON string
 *   - `$ARGUMENTS[n]`/`$n`→ the n-th whitespace-delimited token of the input
 *   - if NO placeholder appears, the input is appended as `\n\nARGUMENTS: <json>`
 * so an AI-driven hook can reference the event data positionally OR rely on the
 * append fallback. Indexed tokens use a simple whitespace split (the common case
 * for hook input); `$ARGUMENTS` always yields the verbatim JSON.
 */
export function substituteHookArguments(prompt: string, jsonInput: string): string {
  const tokens = jsonInput.trim().length ? jsonInput.trim().split(/\s+/) : [];
  const original = prompt;
  let out = prompt
    .replace(/\$ARGUMENTS\[(\d+)\]/g, (_m, i: string) => tokens[parseInt(i, 10)] ?? '')
    .replace(/\$(\d+)(?!\w)/g, (_m, i: string) => tokens[parseInt(i, 10)] ?? '');
  out = out.split('$ARGUMENTS').join(jsonInput);
  if (out === original && jsonInput) out = `${out}\n\nARGUMENTS: ${jsonInput}`;
  return out;
}

/**
 * Parse an AI-driven hook's output against the {ok, reason} verification schema.
 * Returns the structured result when the output is a valid verification object,
 * or undefined when it is free-form (callers fall back to raw-text handling).
 */
export function parseHookVerification(output: string): { ok: boolean; reason?: string } | undefined {
  const obj = tryParseHookJson(output);
  if (!obj || typeof obj.ok !== 'boolean') return undefined;
  const reason = typeof obj.reason === 'string' ? obj.reason : undefined;
  return { ok: obj.ok, reason };
}

/** Pull an array of absolute watch paths from a hook's JSON output (watchPaths),
 *  filtering to non-empty strings. Shared by sessionStart / cwdChanged /
 *  fileChanged which can register dynamic FileChanged watches.
 *
 *  Path scoping (the reference's FileChanged `watchPaths` contract): each entry MUST
 *  be ABSOLUTE. A relative path from untrusted hook output is ambiguous (resolved
 *  against whose cwd?) and is dropped rather than silently resolved — the host only
 *  ever watches absolute, hook-declared locations. */
export function parseWatchPaths(result: string): string[] {
  const obj = tryParseHookJson(result);
  const raw = obj?.watchPaths;
  if (!Array.isArray(raw)) return [];
  return raw.filter((p): p is string => typeof p === 'string' && p.length > 0 && path.isAbsolute(p));
}

/**
 * Per-event matcher field (the reference's hooksConfigManager fieldToMatch): for
 * non-tool events the `matcher` is applied against this context field instead of
 * the tool name, so a hook can be scoped to e.g. a specific MCP server, compact
 * trigger, config source, or subagent type. Events absent from this map (and the
 * tool events, handled inline against toolName) treat a matcher as a no-op.
 */
export const HOOK_MATCHER_FIELD: Record<string, string> = {
  notification: 'notificationType',
  subagentStart: 'mode',
  subagentStop: 'mode',
  preCompact: 'trigger',
  postCompact: 'trigger',
  // Sampling: scope on the query ORIGIN (the reference's MagicDocs querySource
  // boundary) so a pre/postSampling hook can fire for only `main` (the top-level
  // loop) or only `subagent` (forked/nested agents) — the re-entrancy guard that
  // lets a hook which itself samples the model exclude background work.
  preSampling: 'samplingOrigin',
  postSampling: 'samplingOrigin',
  // Session lifecycle: scope on the boundary cause (the reference's
  // SessionStart `source` / SessionEnd `reason` discriminators) so a hook can
  // fire for e.g. only `resume` or only `clear`.
  sessionStart: 'source',
  sessionEnd: 'reason',
  // Setup: scope on the boundary cause (the reference's Setup `trigger`
  // discriminator — init | maintenance).
  setup: 'trigger',
  // A StopFailure hook scopes on the failure CLASS (the reference's `error` field,
  // e.g. rate_limit / authentication_failed) — the emitter classifies the raw
  // failure into this taxonomy (stopFailureErrorClass) before firing.
  stopFailure: 'error',
  elicitation: 'mcpServerName',
  elicitationResult: 'mcpServerName',
  configChange: 'source',
  instructionsLoaded: 'loadReason',
  // TeammateIdle: scope on the teammate's name (the reference's TeammateIdle
  // teammate_name field) so a hook can fire for just one teammate.
  teammateIdle: 'teammateName',
  permissionRequest: 'toolName',
  permissionDenied: 'toolName',
};

/**
 * Declarative per-event metadata (the reference's getHookEventMetadata): a
 * human-readable summary + description (input JSON shape + command-hook exit-code
 * semantics) for every hook event. Drives `/hooks`-style introspection and the
 * `--introspect` manifest. The matcher field for each event is derived from the
 * single source HOOK_MATCHER_FIELD (tool events match on the tool name) so the
 * catalog can never drift from the actual matcher scoping.
 */
export interface HookEventMetadata {
  summary: string;
  description: string;
  /** The context field a `matcher` is applied against, or undefined when the
   *  event has no matchable field (a matcher is a no-op). */
  matcherField?: string;
  /** Enumerated allowed matcher values for this event's matcher field (the reference
   *  Code's MatcherMetadata.values). Present whenever `matcherField` is — a static
   *  enumeration (session sources, compact triggers, config sources, …) or, for
   *  dynamically-scoped events (tool names, agent types, MCP server names), the
   *  caller-supplied set via {@link HookEventMetadataOpts} (defaulting to `[]`).
   *  Drives matcher-value discovery in `/hooks` + `--introspect`. */
  matcherValues?: string[];
}

/** Dynamic matcher-value sources for {@link getHookEventMetadata}: the valid set
 *  for some events (tool/agent/MCP-scoped) is runtime state, not a fixed enum. */
export interface HookEventMetadataOpts {
  /** Tool names enumerated for tool-matched events (preToolUse, postToolUse, …). */
  toolNames?: string[];
  /** Agent types enumerated for subagentStart / subagentStop. */
  agentTypes?: string[];
  /** MCP server names enumerated for elicitation / elicitationResult. */
  mcpServerNames?: string[];
}

/** Display label for a hook matcher: an empty / absent matcher targets everything,
 *  rendered as '(all)' (the reference's matcher list fallback). */
export function formatHookMatcherLabel(matcher: string | undefined): string {
  return matcher && matcher.length > 0 ? matcher : '(all)';
}

/** Source-tier priority for ordering matchers/hooks in introspection (lower =
 *  higher authority). Mirrors the reference's SOURCES order with plugin/builtin
 *  pinned lowest. */
export const HOOK_SOURCE_PRIORITY: Record<string, number> = {
  managed: 0,
  project: 1,
  user: 2,
  config: 3,
  plugin: 999,
  builtin: 999,
};

/** Deterministically order matchers within an event (the reference's
 *  sortMatchersByPriority): by their highest-priority (lowest-number) contributing
 *  source first, tie-broken by matcher name. Pure — never mutates its input. */
export function sortMatchersByPriority(
  matchers: string[],
  sourcesByMatcher: Record<string, readonly string[]>,
): string[] {
  const pri = (s: string): number => HOOK_SOURCE_PRIORITY[s] ?? 500;
  const highest = (m: string): number => {
    const srcs = sourcesByMatcher[m] ?? [];
    return srcs.length ? Math.min(...srcs.map(pri)) : 1000;
  };
  return [...matchers].sort((a, b) => {
    const d = highest(a) - highest(b);
    return d !== 0 ? d : a.localeCompare(b);
  });
}

/** Stable cache key for {@link getHookEventMetadata}: the event plus each dynamic
 *  matcher-value source joined in SORTED order. Mirrors the reference's
 *  hooksConfigManager resolver `toolNames => toolNames.slice().sort().join(',')`
 *  (ref L26), extended to the agent/MCP sources AMI also scopes on. A caller that
 *  passes a freshly-allocated `toolNames` array with the same members every render
 *  (e.g. the `/hooks` menu) collapses onto ONE cache entry instead of leaking a
 *  new one per call. */
export function hookEventMetadataCacheKey(event: string, opts: HookEventMetadataOpts): string {
  const sortedJoin = (xs?: string[]): string => (xs ? [...xs].sort().join(',') : '');
  return [event, sortedJoin(opts.toolNames), sortedJoin(opts.agentTypes), sortedJoin(opts.mcpServerNames)].join('\u0000');
}
