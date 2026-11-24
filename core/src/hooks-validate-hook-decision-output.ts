// Split from hooks.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import * as os from 'os';
import { sanitizePluginIdSegment } from './plugins';
import type { HookContext } from './hooks-session-context';
import { sanitizeHeaderValue } from './hooks-event-concurrency-limiter';

/**
 * Interpolate `$VAR` / `${VAR}` references in HTTP-hook header values from the
 * process environment, gated by an allowlist (the hook's own `allowedEnvVars`
 * intersected with the global `httpHookAllowedEnvVars` setting). Any var not on
 * the allowlist resolves to the empty string so a malicious hooks.json cannot
 * exfiltrate arbitrary secrets through a header. The fully-interpolated value is
 * then CRLF/NUL-sanitized (sanitizeHeaderValue) so neither the template literal
 * NOR a resolved env value can inject an extra header.
 *
 * `onWarn` (when supplied) is invoked once per DISTINCT env var reference that was
 * skipped because it is not on the allowlist, so a caller can surface the reference's
 * explanatory "env var $<name> not in allowedEnvVars, skipping interpolation"
 * warning (execHttpHook.ts:L89-108) — otherwise a user who writes `$TOKEN` in a
 * header value silently gets the empty string with no hint why.
 */
export function interpolateHookHeaderEnv(
  value: string,
  allowed: Set<string>,
  env: Record<string, string | undefined> = process.env,
  onWarn?: (name: string) => void,
): string {
  const warned = new Set<string>();
  const interpolated = value.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)/g, (_m, braced, bare) => {
    const name = braced ?? bare;
    if (!allowed.has(name)) {
      if (onWarn && !warned.has(name)) { warned.add(name); onWarn(name); }
      return '';
    }
    return env[name] ?? '';
  });
  return sanitizeHeaderValue(interpolated);
}

/**
 * hookEventName spoof check (the reference's parseHookOutput validation): when a
 * hook's `hookSpecificOutput.hookEventName` is present it MUST equal the event
 * the hook actually fired for. A mismatch means the hook is trying to act as a
 * different event (e.g. a SessionStart hook claiming PreToolUse to inject a
 * permission decision) — reject it. Returns a human-readable validation message
 * on mismatch, or undefined when it matches / is absent / has no expected event.
 * `expectedPascalEvent` is the PascalCase event name (e.g. 'PreToolUse').
 */
export function hookEventNameMismatch(
  parsed: unknown,
  expectedPascalEvent: string | undefined,
): string | undefined {
  if (!expectedPascalEvent) return undefined;
  if (!parsed || typeof parsed !== 'object') return undefined;
  const inner = (parsed as Record<string, unknown>).hookSpecificOutput;
  if (!inner || typeof inner !== 'object') return undefined;
  const name = (inner as Record<string, unknown>).hookEventName;
  if (typeof name !== 'string' || name.length === 0) return undefined;
  if (name !== expectedPascalEvent) {
    return `Hook returned incorrect event name: expected '${expectedPascalEvent}' but got '${name}'.`;
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// Hook event serialization (stdin payload for command hooks)
// ---------------------------------------------------------------------------
// the reference hooks read a snake_case JSON object from stdin terminated by a
// newline (hook_event_name / tool_name / tool_input / tool_response / …). AMI
// historically emitted camelCase with an `event` key. serializeHookEvent emits
// BOTH: the original camelCase keys (back-compat for AMI's own consumers/tests)
// AND the reference snake_case aliases + a trailing newline, so stock CC hooks
// work unchanged. (R2-H4)
export const HOOK_EVENT_PASCAL: Record<string, string> = {
  preToolUse: 'PreToolUse', postToolUse: 'PostToolUse', userPromptSubmit: 'UserPromptSubmit',
  sessionStart: 'SessionStart', sessionEnd: 'SessionEnd', subagentStart: 'SubagentStart',
  subagentStop: 'SubagentStop', preCompact: 'PreCompact', postCompact: 'PostCompact',
  stop: 'Stop', error: 'Error', preSampling: 'PreSampling', postSampling: 'PostSampling',
  permissionRequest: 'PermissionRequest', permissionDenied: 'PermissionDenied',
  setup: 'Setup', teammateIdle: 'TeammateIdle',
};

export const HOOK_KEY_SNAKE: Record<string, string> = {
  toolName: 'tool_name', toolInput: 'tool_input', toolOutput: 'tool_response',
  sessionId: 'session_id', subagentSessionId: 'subagent_session_id',
  durationMs: 'duration_ms', turnCount: 'turn_count', messageCount: 'message_count',
  agentType: 'agent_type', teammateName: 'teammate_name', teamName: 'team_name',
  loadReason: 'load_reason', memoryType: 'memory_type',
  notificationType: 'notification_type',
  permissionMode: 'permission_mode',
  samplingOrigin: 'sampling_origin',
  // PostCompact input carries the generated summary (the reference's compactSummary);
  // emit the snake_case alias too so cross-framework hooks can read either form.
  compactSummary: 'compact_summary',
  // Correlation id for the pending tool call (the reference's `tool_use_id`): lets a
  // permission-prompt / PostToolUseFailure hook tie its decision to the exact call.
  toolUseId: 'tool_use_id',
  // Snake-case parity for a tool-failure payload (the reference PostToolUseFailure
  // emits is_error / is_interrupt; previously AMI emitted only the camelCase keys).
  isError: 'is_error', isInterrupt: 'is_interrupt',
  // Idle-boundary flag for pre/postSampling hooks (the reference's
  // hasToolCallsInLastAssistantTurn inverse) so a background hook can self-gate on
  // the serialized `is_idle` field.
  isIdle: 'is_idle',
  // PermissionRequest 'always allow' options (the reference's `permission_suggestions`
  // param) so a permission-prompt hook sees the standing-rule suggestions.
  suggestions: 'permission_suggestions',
  // MCP elicitation request params (the reference's elicitation payload): the
  // requested JSON-schema and the request correlation id.
  requestedSchema: 'requested_schema', elicitationId: 'elicitation_id',
  // PreToolUse decision context (the reference passes `tool.getToolUseSummary(input)`
  // and `toolUseContext.requestPrompt` into executePreToolHooks): a human-readable
  // one-line summary of the pending tool call and the originating user prompt, so a
  // guardrail hook can decide with the same context the permission UI sees.
  toolUseSummary: 'tool_use_summary', requestPrompt: 'request_prompt',
  // REPL hook context (the reference's REPLHookContext for post-sampling + stop
  // hooks): the active system prompt and the mutable user/system key-value context
  // bags, so a hook can read what the model was told and any session context keys.
  systemPrompt: 'system_prompt', userContext: 'user_context', systemContext: 'system_context',
};

/**
 * Substitute a plugin/skill bundle's base directory into a command-hook string
 * (command or an arg). Replaces the `${CLAUDE_PLUGIN_ROOT}` placeholder (and the
 * AMI-native `${AMI_PLUGIN_ROOT}` alias) with `pluginRoot`, so a plugin- or
 * skill-registered command hook can reference files co-located with its bundle
 * (`${CLAUDE_PLUGIN_ROOT}/scripts/check.sh`) regardless of the session cwd —
 * the reference's CLAUDE_PLUGIN_ROOT command substitution (utils/hooks.ts) and
 * registerSkillHooks skillRoot. Both `${VAR}` and bare `$VAR` forms are handled.
 * Returns the string unchanged when `pluginRoot` is empty. Canonical — reused by
 * every command-hook call site; never re-implement the substitution per site.
 */
/**
 * Replace the `${NAME}` and bare `$NAME` (word-bounded) forms of each var name in
 * `names` with `value`. Single engine behind {@link substitutePluginRoot} and
 * {@link substitutePluginData} so the two plugin-variable substitutions share ONE
 * implementation (no per-var copy of the replacement logic). The `$NAME`-form uses
 * the function replacement so a `value` containing `$`-patterns is inserted
 * literally (never re-interpreted as a `$1` back-reference).
 */
export function replacePluginVar(text: string, names: readonly string[], value: string): string {
  let out = text;
  for (const name of names) {
    out = out
      .replace(new RegExp(`\\$\\{${name}\\}`, 'g'), () => value)
      .replace(new RegExp(`\\$${name}\\b`, 'g'), () => value);
  }
  return out;
}

export function substitutePluginRoot(text: string, pluginRoot: string | undefined): string {
  if (!pluginRoot || !text) return text;
  return replacePluginVar(text, ['CLAUDE_PLUGIN_ROOT', 'AMI_PLUGIN_ROOT'], pluginRoot);
}

/**
 * Substitute a plugin bundle's PERSISTENT per-plugin data directory into a
 * command-hook string. Replaces `${CLAUDE_PLUGIN_DATA}` (and the AMI-native
 * `${AMI_PLUGIN_DATA}` alias, plus the bare `$CLAUDE_PLUGIN_DATA`/`$AMI_PLUGIN_DATA`
 * forms) with `dataDir`, so a plugin command hook can read/write a stable scratch
 * directory that survives across sessions and is separate from the read-only bundle
 * root (`${CLAUDE_PLUGIN_ROOT}`) — the reference's CLAUDE_PLUGIN_DATA substitution
 * (utils/hooks.ts getPluginDataDir). Returns the string unchanged when `dataDir`
 * is empty. Canonical — reused by the command-hook call site alongside
 * {@link substitutePluginRoot}; never re-implement the substitution per site.
 */
export function substitutePluginData(text: string, dataDir: string | undefined): string {
  if (!dataDir || !text) return text;
  return replacePluginVar(text, ['CLAUDE_PLUGIN_DATA', 'AMI_PLUGIN_DATA'], dataDir);
}

/**
 * Resolve the deterministic, persistent per-plugin data directory for a plugin
 * command hook whose bundle lives at `pluginRoot` (the reference's getPluginDataDir).
 * Keyed by the sanitized basename of the bundle root so the same plugin always maps
 * to the same `~/.ami/plugin-data/<id>` directory regardless of the session cwd, and
 * two different plugins never collide. Returns `undefined` when `pluginRoot` is empty
 * or its basename sanitizes to nothing (so a hook with no bundle gets no data dir
 * and no `${CLAUDE_PLUGIN_DATA}` substitution). The directory is NOT created here —
 * callers that export it to a hook create it best-effort first.
 */
export function resolvePluginDataDir(pluginRoot: string | undefined): string | undefined {
  const id = resolvePluginId(pluginRoot);
  if (!id) return undefined;
  return path.join(os.homedir(), '.ami', 'plugin-data', id);
}

/**
 * Resolve the stable plugin IDENTIFIER for a hook whose bundle lives at
 * `pluginRoot` — the sanitized basename of the bundle root (the reference's
 * per-hook `pluginId` provenance). Canonical: reused by {@link resolvePluginDataDir}
 * (the data-dir key) and by the hook rewake payload so a backgrounded plugin hook's
 * completion/wake message is attributable to the plugin that installed it. Returns
 * `undefined` when `pluginRoot` is empty or its basename sanitizes to nothing.
 */
export function resolvePluginId(pluginRoot: string | undefined): string | undefined {
  if (!pluginRoot || !pluginRoot.trim()) return undefined;
  const id = sanitizePluginIdSegment(path.basename(pluginRoot.trim()));
  return id || undefined;
}

export function serializeHookEvent(obj: Record<string, unknown>): string {
  const out: Record<string, unknown> = { ...obj };
  if (typeof obj.event === 'string') {
    out.hook_event_name = HOOK_EVENT_PASCAL[obj.event] ?? obj.event;
  }
  for (const [camel, snake] of Object.entries(HOOK_KEY_SNAKE)) {
    if (camel in obj && !(snake in obj)) out[snake] = obj[camel];
  }
  return JSON.stringify(out) + '\n';
}

/** Primary-argument keys, in precedence order, used to summarize a tool call.
 *  Mirrors the fields tools surface a human-readable summary on (a shell command,
 *  then a file path, then a url/pattern/query/prompt). */
export const TOOL_USE_SUMMARY_KEYS = ['command', 'file_path', 'path', 'url', 'pattern', 'query', 'prompt'] as const;

/** Max characters of a tool-use summary before it is elided (keeps the serialized
 *  hook payload bounded even for a huge command/prompt argument). */
export const TOOL_USE_SUMMARY_MAX = 120;

/**
 * Build a concise, human-readable summary of a pending tool call for a PreToolUse
 * hook's stdin payload — AMI's generic stand-in for the reference's per-tool
 * `tool.getToolUseSummary(input)`. Picks the most salient primary argument (shell
 * command → file path → url/pattern/query/prompt) and renders `toolName(arg)` on a
 * single collapsed line, elided to {@link TOOL_USE_SUMMARY_MAX}. Falls back to the
 * bare tool name when no recognizable primary argument is present. Deterministic
 * and side-effect free so the summary is stable for a given input.
 */
export function buildToolUseSummary(toolName: string, toolInput: Record<string, unknown>): string {
  for (const key of TOOL_USE_SUMMARY_KEYS) {
    const value = toolInput[key];
    if (typeof value === 'string' && value.trim()) {
      const oneLine = value.replace(/\s+/g, ' ').trim();
      const arg = oneLine.length > TOOL_USE_SUMMARY_MAX
        ? oneLine.slice(0, TOOL_USE_SUMMARY_MAX - 3) + '...'
        : oneLine;
      return `${toolName}(${arg})`;
    }
  }
  return toolName;
}

/**
 * Extract the REPL-hook context fields (the reference's REPLHookContext:
 * systemPrompt + the user/system key-value context bags) from a {@link HookContext}
 * for the post-sampling / stop hook stdin payload. Each field is included only when
 * the engine populated it (empty bags omitted), so a hook that doesn't need them
 * sees an unchanged payload while a context-aware hook can read what the model was
 * told and any attached session context keys.
 */
export function replHookContextFields(ctx: HookContext): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  if (ctx.systemPrompt) out.systemPrompt = ctx.systemPrompt;
  if (ctx.userContext && Object.keys(ctx.userContext).length > 0) out.userContext = ctx.userContext;
  if (ctx.systemContext && Object.keys(ctx.systemContext).length > 0) out.systemContext = ctx.systemContext;
  return out;
}

// ---------------------------------------------------------------------------
// Shared hook-output decoders (canonical — reused by every execute* method
// that consumes runHookByType's flattened result; never re-implement per site).
// ---------------------------------------------------------------------------

/**
 * Canonical extractor for a blocking reason from a flattened hook result.
 * runHookByType maps exit-2 / `decision:block` / `continue:false` to the
 * `DENY:<reason>` sentinel; this returns the reason when a hook blocked, else
 * undefined. Used by Stop / PostToolUse / TaskCompleted blocking consumers.
 */
export function extractHookBlockingError(result: string): string | undefined {
  if (typeof result === 'string' && result.startsWith('DENY:')) {
    return result.slice(5).trim();
  }
  return undefined;
}

/**
 * Strict validation of a hook's decision fields (the reference parseHookOutput: an
 * unrecognized `decision` or `permissionDecision` value is an ERROR, not a silent
 * no-op). Returns a human-readable validation message when the parsed output
 * carries a `decision`/`permissionDecision` field whose value is outside the
 * allowed set, else undefined. Reused by the permissionRequest parser so a typo'd
 * decision fails CLOSED (deny with the reason) instead of being ignored — which
 * would let an intended block silently fall through to the default permission
 * flow. Valid sets match AMI's HookDecisionOutput: decision ∈
 * {approve,deny,ask,block}, permissionDecision ∈ {allow,deny,ask}.
 */
export function unknownHookDecisionError(
  parsed: Record<string, unknown> | undefined,
): string | undefined {
  if (!parsed) return undefined;
  const dec = parsed.decision;
  if (dec !== undefined && dec !== 'approve' && dec !== 'deny' && dec !== 'ask' && dec !== 'block') {
    return `Unknown hook decision type: ${String(dec)}. Valid types are: approve, block`;
  }
  const pdec = parsed.permissionDecision;
  if (pdec !== undefined && pdec !== 'allow' && pdec !== 'deny' && pdec !== 'ask') {
    return `Unknown hook permissionDecision type: ${String(pdec)}. Valid types are: allow, deny, ask`;
  }
  return undefined;
}

/**
 * Strict shape/type validation of a parsed hook JSON output before any field is
 * acted on (the reference's parseHookOutput / hookJSONOutputSchema safeParse): a
 * hook that declares a KNOWN decision field with a wrong-typed value (e.g.
 * `continue: "yes"`, `updatedInput: []`, `asyncTimeout: "15"`) is malformed and
 * must be surfaced as a validation error rather than silently mis-interpreted
 * (`"yes"` is truthy, `continue:false` would never fire). Reuses
 * {@link unknownHookDecisionError} for the decision/permissionDecision enums (no
 * duplicate), then checks the remaining typed fields. Returns a human-readable
 * message for the first offending field, else undefined.
 *
 * Unknown/extra properties are intentionally NOT rejected (no
 * `additionalProperties:false`): AMI's forging defense already STRIPS unrecognized
 * fields (hook-forging-attack.test.ts), so a hook carrying extra keys is cleaned,
 * not errored — matching AMI's superset behavior while still type-gating every
 * recognized field.
 */
export function validateHookDecisionOutput(
  parsed: Record<string, unknown> | undefined,
): string | undefined {
  if (!parsed) return undefined;
  const enumErr = unknownHookDecisionError(parsed);
  if (enumErr) return enumErr;
  for (const k of ['reason', 'stopReason', 'systemMessage', 'additionalContext'] as const) {
    if (parsed[k] !== undefined && typeof parsed[k] !== 'string') {
      return `Hook output field "${k}" must be a string.`;
    }
  }
  for (const k of ['continue', 'suppressOutput', 'async', 'interrupt'] as const) {
    if (parsed[k] !== undefined && typeof parsed[k] !== 'boolean') {
      return `Hook output field "${k}" must be a boolean.`;
    }
  }
  if (parsed.updatedPermissions !== undefined && !Array.isArray(parsed.updatedPermissions)) {
    return `Hook output field "updatedPermissions" must be an array.`;
  }
  if (
    parsed.asyncTimeout !== undefined &&
    (typeof parsed.asyncTimeout !== 'number' || !Number.isFinite(parsed.asyncTimeout))
  ) {
    return `Hook output field "asyncTimeout" must be a number.`;
  }
  if (
    parsed.updatedInput !== undefined &&
    (typeof parsed.updatedInput !== 'object' ||
      parsed.updatedInput === null ||
      Array.isArray(parsed.updatedInput))
  ) {
    return `Hook output field "updatedInput" must be an object.`;
  }
  return undefined;
}

/**
 * Event-specific default deny/feedback wording (the reference's phase-specific
 * block messages): when a hook blocks WITHOUT supplying its own reason, the
 * surfaced text names the lifecycle phase instead of a bare 'Hook blocked', so a
 * user/model can tell WHICH guard fired. A hook-supplied `reason` always wins.
 */
export function formatHookDenyReason(event: string | undefined, reason?: string): string {
  const r = reason?.trim();
  if (r) return r;
  switch (event) {
    case 'PreToolUse':
    case 'preToolUse': return 'PreToolUse hook blocked this tool call';
    case 'PostToolUse':
    case 'postToolUse': return 'PostToolUse hook blocked the result';
    case 'UserPromptSubmit':
    case 'userPromptSubmit': return 'UserPromptSubmit hook blocked this prompt';
    case 'Stop':
    case 'stop': return 'Stop hook prevented the turn from ending';
    default: return event ? `${event} hook blocked this action` : 'Hook blocked this action';
  }
}
