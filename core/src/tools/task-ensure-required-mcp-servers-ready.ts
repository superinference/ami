// Split from task.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { ToolContext, Message } from '../types';
import { isModelFamilyAlias, resolveModelAlias, getDefaultSubagentModel } from '../model-registry';
import type { ProviderConfig } from '../types';
import { requiredMcpServerReadiness } from '../skills';
import { MemoryManager } from '../memory';

/**
 * The names of the NAMED agent types this tool context can delegate to (built-in
 * roster agents + user/project/plugin-contributed agent definitions), de-duplicated
 * and sorted (locale-aware). Reads the optional skill manager's `listAgents()` roster
 * — the SAME accessor the spawn path already uses to validate a `subagent_type` and
 * to list "Available agents" in its not-found error — so there is no second
 * agent-discovery path. Returns `[]` when there is no manager, it lacks the accessor,
 * or the call throws, so callers never have to guard. Used to populate the
 * coordinator's worker-capability affordance ({@link buildCoordinatorWorkerContext}).
 */
export function availableAgentTypeNames(context: ToolContext): string[] {
  const mgr = context._skillManager;
  if (!mgr || typeof mgr.listAgents !== 'function') return [];
  try {
    const names = mgr.listAgents()
      .map(a => a && typeof a.name === 'string' ? a.name.trim() : '')
      .filter(n => n.length > 0);
    return [...new Set(names)].sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

/**
 * Resolve the project-instruction (CLAUDE.md/AGENTS.md) context a NAMED subagent
 * OPTS INTO via its `omitClaudeMd` knob (parity with the reference's agent `omitClaudeMd`,
 * inverted — see {@link AgentDefinition.omitClaudeMd}). AMI's default subagent prompt
 * is deliberately lean: it carries NEITHER the project CLAUDE.md/AGENTS.md NOR the
 * session-start git status (a token-saving superset perk). This knob lets an agent
 * definition request the project instruction context back:
 *   - `omitClaudeMd === false` → return the project instructions (opt-in);
 *   - `undefined` / `true`     → return `''` (keep the lean default).
 * Returns `''` whenever omission stays in effect, there are no instruction files, or
 * the read throws, so the caller never has to guard. Reads via {@link MemoryManager}'s
 * {@link MemoryManager.loadProjectInstructions} — the SAME source the full system
 * prompt uses — so there is no second instruction-discovery path.
 */
export function resolveAgentProjectContext(cwd: string, omitClaudeMd: boolean | undefined): string {
  if (omitClaudeMd !== false) return '';
  try {
    const instructions = new MemoryManager(cwd).loadProjectInstructions();
    return instructions ? `# Project Instructions\n\n${instructions.trim()}` : '';
  } catch {
    return '';
  }
}

/**
 * Names of the currently CONNECTED MCP servers reachable from this tool context,
 * de-duplicated and sorted (locale-aware). Reads the optional MCP manager's
 * `listAllServers()` roster — the same accessor `tool_search` uses — and keeps only
 * entries in the `connected` state (a pending/failed server offers no tools to a
 * worker yet). Returns `[]` when there is no manager, it lacks the accessor, or the
 * call throws, so callers never have to guard. Used to populate the coordinator's
 * worker-capability affordance ({@link buildCoordinatorWorkerContext}).
 */
export function connectedMcpServerNames(context: ToolContext): string[] {
  const mgr = context._mcpManager as { listAllServers?: () => Array<{ name: string; state: string }> } | undefined;
  if (!mgr || typeof mgr.listAllServers !== 'function') return [];
  try {
    const names = mgr.listAllServers()
      .filter(s => s && s.state === 'connected')
      .map(s => s.name);
    return [...new Set(names)].sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

/**
 * Poll cadence + ceiling for the pre-spawn required-MCP-server readiness gate.
 * Parity with the reference's AgentTool requiredMcpServers wait (AgentTool.tsx:L376-391):
 * poll every 500ms for up to 30s while a required server is still connecting.
 */
export const REQUIRED_MCP_POLL_INTERVAL_MS = 500;

export const REQUIRED_MCP_MAX_WAIT_MS = 30_000;

/**
 * Normalize a raw MCP server/client state into the `connected|failed|pending`
 * vocabulary {@link requiredMcpServerReadiness} understands. A server is
 * `connected` only when fully ready; still-connecting states (`pending`/`connecting`)
 * are `pending`; every terminal non-ready state (`error`, `needs-auth`,
 * `disconnected`, `stopped`, `disabled`) is `failed` so the gate fails fast on a
 * server that will never come up rather than polling the full ceiling.
 */
export function normalizeMcpServerState(raw: string): 'connected' | 'failed' | 'pending' {
  switch (raw) {
    case 'ready':
    case 'connected':
      return 'connected';
    case 'pending':
    case 'connecting':
      return 'pending';
    default:
      return 'failed';
  }
}

/**
 * Read the current `{ serverName -> normalized-state }` map from the tool context's
 * MCP manager (empty when no manager / `listAllServers` is wired). Normalizes each
 * raw state via {@link normalizeMcpServerState} so {@link ensureRequiredMcpServersReady}
 * can classify readiness without knowing the manager's internal state vocabulary.
 */
export function readMcpServerStates(context: ToolContext): Record<string, string> {
  const mgr = context._mcpManager as { listAllServers?: () => Array<{ name: string; state: string }> } | undefined;
  if (!mgr || typeof mgr.listAllServers !== 'function') return {};
  try {
    const out: Record<string, string> = {};
    for (const s of mgr.listAllServers()) {
      if (s && typeof s.name === 'string' && typeof s.state === 'string') {
        out[s.name] = normalizeMcpServerState(s.state);
      }
    }
    return out;
  } catch {
    return {};
  }
}

export const defaultMcpGateSleep = (ms: number): Promise<void> => new Promise(resolve => { setTimeout(resolve, ms); });

/** Knobs for {@link ensureRequiredMcpServersReady} (poll ceiling + injectable clock/sleep for tests). */
export interface RequiredMcpGateOptions {
  maxWaitMs?: number;
  pollIntervalMs?: number;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
}

/**
 * Pre-spawn gate: block a subagent spawn until its agent definition's
 * `requiredMcpServers` are all connected. While ANY required server is still
 * connecting (and none has failed yet) it polls up to a bounded ceiling, EARLY-EXITING
 * the moment a required server enters a failed state (no point waiting on siblings
 * when the check will fail regardless). Returns `{ ready: true }` when every
 * requirement is satisfied (or there are no requirements), and
 * `{ ready: false, error }` — carrying the unsatisfied server patterns, the list of
 * connected servers, and a `/mcp` configure hint — when a required server is
 * failed/missing (or still pending after the ceiling). Mirrors the reference's
 * AgentTool requiredMcpServers readiness gate (AgentTool.tsx:L367-410). The state
 * reader is injected (not the manager) so this stays a pure, testable building block.
 */
export async function ensureRequiredMcpServersReady(
  required: readonly string[] | undefined,
  readStates: () => Record<string, string>,
  agentLabel: string,
  opts: RequiredMcpGateOptions = {},
): Promise<{ ready: true } | { ready: false; error: string }> {
  if (!required || required.length === 0) return { ready: true };
  const maxWaitMs = opts.maxWaitMs ?? REQUIRED_MCP_MAX_WAIT_MS;
  const pollIntervalMs = opts.pollIntervalMs ?? REQUIRED_MCP_POLL_INTERVAL_MS;
  const sleep = opts.sleep ?? defaultMcpGateSleep;
  const now = opts.now ?? Date.now;

  let readiness = requiredMcpServerReadiness(required, readStates());
  // Wait only while something is still connecting AND nothing has already failed —
  // a failed/missing requirement can never be satisfied by waiting.
  if (!readiness.ready && readiness.pending.length > 0 && readiness.failed.length === 0) {
    const deadline = now() + maxWaitMs;
    while (now() < deadline) {
      await sleep(pollIntervalMs);
      readiness = requiredMcpServerReadiness(required, readStates());
      if (readiness.ready) break;
      if (readiness.failed.length > 0) break; // early-exit: a required server failed
      if (readiness.pending.length === 0) break; // resolved to failed/missing
    }
  }
  if (readiness.ready) return { ready: true };
  const connected = Object.entries(readStates())
    .filter(([, st]) => st === 'connected')
    .map(([name]) => name)
    .sort((a, b) => a.localeCompare(b));
  const unsatisfied = [...readiness.failed, ...readiness.missing, ...readiness.pending];
  return {
    ready: false,
    error:
      `Error: agent "${agentLabel}" requires MCP server(s) matching: ${unsatisfied.join(', ')}. `
      + `Connected MCP servers: ${connected.length > 0 ? connected.join(', ') : 'none'}. `
      + 'Use /mcp to configure and authenticate the required MCP servers.',
  };
}

/** The sentinel a sub-agent model request uses to mean "inherit the parent's model". */
export const INHERIT_MODEL_SENTINEL = 'inherit';

/**
 * Resolve a sub-agent's model from a requested value, with ANTI-DOWNGRADE:
 * when the request is a bare family alias (sonnet/opus/haiku) and the PARENT is
 * already running a model of that same family, inherit the parent's EXACT model
 * string instead of the generic alias default. So a parent on
 * `claude-opus-4-8` that spawns `task({ model: 'opus' })` keeps the child on
 * `claude-opus-4-8` rather than silently downgrading it to `claude-opus-4`.
 * A full model name is returned verbatim; an unknown alias passes through.
 *
 * Alias recognition/resolution is delegated to the single canonical alias table
 * in model-registry (`isModelFamilyAlias` / `resolveModelAlias`) so the subagent
 * path, validateModel, and any CLI/slash path all resolve the same short names.
 */
export function resolveSubagentModel(requested: string | undefined, parentModel: string | undefined): string | undefined {
  if (!requested) return undefined;
  // Normalize the request for sentinel/alias MATCHING only: a provider tool-call
  // or an agent-definition `model` field may emit a family alias or the inherit
  // sentinel in any case and with stray surrounding whitespace ("Opus", " INHERIT ").
  // A full model name or unknown value is still returned verbatim (the final
  // branch), so exact provider model IDs are never lower-cased or trimmed away.
  const normalized = requested.trim().toLowerCase();
  // 'inherit' is an explicit sentinel meaning "run on the SAME model as the main
  // conversation" (the reference's default subagent model). Resolve it to the parent's
  // exact model string — never to a generic alias default — so a parent on
  // claude-opus-4-8 that spawns task({ model: 'inherit' }) keeps the child there.
  if (normalized === INHERIT_MODEL_SENTINEL) return parentModel;
  if (isModelFamilyAlias(normalized) && parentModel && parentModel.toLowerCase().includes(normalized)) {
    return parentModel;
  }
  return resolveModelAlias(normalized) ?? requested;
}

/**
 * Resolve the PERSISTED `subagentDefaultModel` config knob (EngineConfig →
 * ToolContext._subagentDefaultModel) into a concrete model id. Applied ONLY when a
 * spawn names no model from any higher-priority source (operator env override →
 * per-call `model` → agent-definition model → explore fast default). Tri-state,
 * parity with the reference's getDefaultTeammateModel (tools/shared/spawnMultiAgent.ts:L72-82):
 *   - `null`      → user picked "Default (follow leader)" in /config → inherit the
 *                   PARENT's exact model (`parentModel`), so a config choice never
 *                   downgrades an otherwise-inheriting subagent;
 *   - a string    → that configured model becomes the default (the `inherit` sentinel
 *                   and family aliases resolve through {@link resolveSubagentModel}, so
 *                   this path reuses the one canonical alias/anti-downgrade table —
 *                   no second resolver);
 *   - `undefined` → knob never set → return undefined so the existing provider-aware
 *                   fallback chain ({@link defaultSubagentModelFallback}) stays in charge.
 * This WIRES the previously-inert EngineConfig.subagentDefaultModel into real spawn
 * behavior without duplicating the hardcoded provider-aware fallback (that stays in
 * {@link getDefaultSubagentModel}).
 */
export function resolveSubagentDefaultModel(
  configured: string | null | undefined,
  parentModel: string | undefined,
): string | undefined {
  if (configured === undefined) return undefined;
  if (configured === null) return parentModel;
  // A blank/whitespace-only knob is treated as unset (fall through to the fallback),
  // never dispatched as an empty model id.
  if (configured.trim() === '') return undefined;
  return resolveSubagentModel(configured, parentModel);
}

/**
 * Final subagent-model fallback: return `currentModel` when it names a model, else
 * the provider-aware default from {@link getDefaultSubagentModel}. Used after every
 * higher-priority source (env override → per-call model → agent-def model → explore
 * default → inherited parent model) has had its say, so a subagent spawned with no
 * model configured ANYWHERE runs on a concrete provider-correct fast model instead
 * of dispatching an empty model string. AMI KEEPS its parent-inheritance perk (a
 * non-empty `currentModel` passes through untouched); this only ADDS the hardcoded
 * default for the genuinely-unset case — parity with the reference's
 * getHardcodedTeammateModelFallback without regressing the inherit behavior.
 */
export function defaultSubagentModelFallback(
  currentModel: string | undefined,
  providerConfig: ProviderConfig,
): string {
  const trimmed = (currentModel ?? '').trim();
  return trimmed !== '' ? trimmed : getDefaultSubagentModel(providerConfig);
}

/** Operator override for the model an `explore`-mode sub-agent runs on. Set to a
 *  model id/alias to force one, or the `inherit` sentinel to keep the parent's
 *  model (behavior-only knob — mirrors the reference's `USER_TYPE==='ant' ? inherit
 *  : haiku` gate on the explore agent's model). */
export const EXPLORE_MODEL_ENV = 'AMI_EXPLORE_MODEL';

/**
 * The model a mode-only `explore` sub-agent should default to when the caller set
 * no explicit model (no per-call `model`, no agent-def model, no
 * SUPERINFERENCE_SUBAGENT_MODEL override). Mirrors the reference's explore agent,
 * which pins external runs to the fast/cheap `haiku` model for speed because
 * read-only exploration does not need the main model's capability. Returns the
 * resolved fast model id, or `undefined` to keep the parent's model.
 *
 * Rules:
 *  - {@link EXPLORE_MODEL_ENV} wins when set: `inherit` → keep parent (undefined);
 *    any other value → that model id/alias (alias-resolved).
 *  - A NON-the reference parent (e.g. a GPT/Gemini run) keeps its own model — forcing a
 *    the reference `haiku` onto a different provider's session would be wrong. Only a
 *    the reference-family (or absent) parent is auto-downshifted to the fast model.
 */
export function defaultExploreModel(
  parentModel: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
): string | undefined {
  const override = env[EXPLORE_MODEL_ENV]?.trim();
  if (override) {
    if (override.toLowerCase() === INHERIT_MODEL_SENTINEL) return undefined;
    return resolveModelAlias(override) ?? override;
  }
  if (parentModel && !/claude/i.test(parentModel)) return undefined;
  return resolveModelAlias('haiku');
}

/**
 * Sentinels AMI writes into an assistant message's content when a turn did NOT
 * finish cleanly: the engine's server-error interrupt
 * (`[Response interrupted by server error]`) and the tool-call interrupt family
 * (`[Tool call interrupted …]`). AMI's Message type carries no `stop_reason`
 * field, so these in-band markers are the only discriminator for "this assistant
 * turn is incomplete/in-progress" — the same marker set `session-search`
 * recognizes for its rendered-as-sentinel exclusion.
 */
export const INCOMPLETE_ASSISTANT_SENTINEL_RE =
  /\[(?:Response interrupted by server error|Tool call interrupted[^\]]*)\]/;

/**
 * Whether a message is an assistant turn that did not complete cleanly (an
 * interrupted/in-progress turn). Mirrors the reference's `stop_reason === null`
 * discriminator in `buildSideQuestionFallbackParams`; since AMI messages have no
 * `stop_reason`, it detects the in-band interrupt sentinels instead.
 */
export function isIncompleteAssistantMessage(msg: Message): boolean {
  return (
    msg.role === 'assistant' &&
    typeof msg.content === 'string' &&
    INCOMPLETE_ASSISTANT_SENTINEL_RE.test(msg.content)
  );
}

/**
 * Drop a TRAILING in-progress/interrupted assistant message before a conversation
 * is forked into a sub-agent (mirrors the reference's
 * `buildSideQuestionFallbackParams`, which discards the last message when it is an
 * assistant turn with `stop_reason === null`). A fork can be requested mid-turn,
 * and an unterminated/interrupted assistant block would otherwise leak a corrupt
 * half-answer into the forked context. Only the trailing message is considered —
 * an interrupted turn deeper in history was already followed by a completed one.
 */
export function stripIncompleteTrailingMessages(messages: Message[]): Message[] {
  if (messages.length === 0) return messages;
  const last = messages[messages.length - 1];
  return isIncompleteAssistantMessage(last) ? messages.slice(0, -1) : messages;
}

/**
 * Drop assistant messages that carry a tool_call with no matching tool result
 * (orphaned/incomplete tool_use) before the parent conversation is seeded into a
 * `fork` child. A fork can be requested mid-turn, so the trailing assistant turn
 * may hold tool_use blocks whose results have not arrived yet; sending those to the
 * child's provider would raise an orphaned-tool_use API error. Build the set of
 * answered tool-call ids (every `tool` message's `tool_call_id`) and drop any
 * assistant message with an unanswered call, keeping everything else intact.
 * Mirrors the reference's `filterIncompleteToolCalls` (runAgent.ts).
 */
export function filterIncompleteToolCalls(messages: Message[]): Message[] {
  const answered = new Set<string>();
  for (const m of messages) {
    if (m.role === 'tool' && m.tool_call_id) answered.add(m.tool_call_id);
  }
  return messages.filter(m => {
    if (m.role === 'assistant' && m.tool_calls && m.tool_calls.length > 0) {
      return !m.tool_calls.some(tc => !answered.has(tc.id));
    }
    return true;
  });
}

/**
 * Drop `tool` (tool_result) messages whose `tool_call_id` no longer matches any
 * assistant tool_call in the transcript — ORPHANED tool results. This is the exact
 * complement of {@link filterIncompleteToolCalls}: that pass removes an assistant
 * turn when ANY of its tool_calls is unanswered, which strands the tool_results of
 * its OTHER (answered) calls — a result block with no preceding tool_use. A provider
 * rejects an orphaned tool_result just as it rejects an orphaned tool_use, so the
 * seed must be reconciled in BOTH directions before it is replayed. Mirrors the reference
 * Code's `filterOrphanedToolResults` (resumeAgent.ts:L70-74), the sibling of its
 * `filterUnresolvedToolUses`.
 */
export function filterOrphanedToolResults(messages: Message[]): Message[] {
  const calledIds = new Set<string>();
  for (const m of messages) {
    if (m.role === 'assistant' && m.tool_calls) {
      for (const tc of m.tool_calls) calledIds.add(tc.id);
    }
  }
  return messages.filter(m => {
    if (m.role === 'tool' && m.tool_call_id) return calledIds.has(m.tool_call_id);
    return true;
  });
}

/**
 * Reconcile a persisted/parent transcript into an API-valid seed before it is
 * replayed into a `fork` child or a resumed sub-engine. Applies the full
 * sanitization pipeline in order: drop a trailing interrupted assistant turn
 * ({@link stripIncompleteTrailingMessages}), drop assistant turns with an unanswered
 * tool_use ({@link filterIncompleteToolCalls}), then drop the tool_results that
 * dropping those turns ORPHANED ({@link filterOrphanedToolResults}). The last step
 * is what the prior fork seed (which only ran the first two) omitted, leaking an
 * orphaned-tool_result into the child's provider request. Mirrors the reference's
 * resume/fork transcript reconciliation (`filterUnresolvedToolUses` +
 * `filterOrphanedToolResults`, resumeAgent.ts:L70-74) so the child never inherits a
 * half-answered tool exchange.
 */
export function sanitizeTranscriptForReplay(messages: Message[]): Message[] {
  return filterOrphanedToolResults(
    filterIncompleteToolCalls(stripIncompleteTrailingMessages(messages)),
  );
}
