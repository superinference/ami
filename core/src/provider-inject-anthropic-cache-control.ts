// Split from provider.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { buildToolCacheBreakpoints } from './provider-transform';
import type { CacheBreakpoint } from './provider-transform';
import { isEnvTruthy } from './analytics';
import { partitionAndSortTools } from './tools/tool-utils';
import type { ProviderConfig, ToolDefinition } from './types';
import type { OpenAIToolFunction } from './provider-interactive-auth-status';
import { deepMergeRecords, isPlainRecord, resolveExtraMetadata, resolveMetadataUserId } from './provider-with-eager-tool-streaming';

/**
 * Convert AMI tool definitions to the OpenAI function-tool wire shape, applying
 * the per-tool structured-output `strict` flag and the experimental-betas kill
 * switch exactly as the reference does at its tool-schema choke point (utils/api.ts).
 *
 * - `strict: true` is emitted ONLY when the tool opts in (`ToolDefinition.strict`),
 *   a `model` is supplied, AND {@link modelSupportsStructuredOutputs} accepts it —
 *   no model, or an unsupported model, means strict is dropped (a wrong `strict`
 *   makes the API reject the request). Mirrors the reference's three-part gate.
 * - When {@link experimentalBetasDisabled} is set, every field not in the base
 *   allowlist (`name`/`description`/`parameters`/`outputSchema`) is stripped at this
 *   single choke point, so a proxy/gateway that 400s on experimental tool-schema
 *   fields (e.g. `strict`) never sees them — including fields added in the future.
 */
export function toolDefinitionsToOpenAI(tools: ToolDefinition[], model?: string): OpenAIToolFunction[] {
  const stripExperimental = experimentalBetasDisabled();
  const strictAllowed = !stripExperimental && !!model && modelSupportsStructuredOutputs(model);
  // Deterministic partition-sort for prompt-cache stability: built-ins form a
  // contiguous alphabetically-sorted prefix, MCP tools follow in a stable order,
  // so the advertised tool-schema cache prefix does not shift between turns as
  // MCP servers connect on demand (parity: the reference's mergeAndFilterTools).
  return partitionAndSortTools(tools).map((tool) => {
    const fn: OpenAIToolFunction['function'] = {
      name: tool.name,
      description: tool.description,
      parameters: tool.inputSchema,
    };
    // Object-root-only guard: only advertise outputSchema when its root type is
    // exactly `object`. A non-object root is dropped (not exposed) rather than
    // emitted as an invalid structured-output declaration. Parity: the reference's
    // mcp.ts, which exposes outputSchema only for object-rooted schemas.
    if (tool.outputSchema && tool.outputSchema.type === 'object') {
      fn.outputSchema = tool.outputSchema;
    }
    // Per-tool strict / structured-outputs flag — model-capability gated, and
    // suppressed entirely by the experimental-betas kill switch above.
    if (strictAllowed && tool.strict === true) {
      fn.strict = true;
    }
    return { type: 'function' as const, function: fn };
  });
}

/**
 * Compute the final request-metadata user-id VALUE forwarded to the provider. When
 * extra metadata is present the value becomes a JSON object string
 * `{ ...extra, user_id? }` (parity: the reference folds extra fields + the id into one
 * JSON `user_id` blob); with no extra metadata it is the plain user id (unchanged
 * behavior). `undefined` ⇒ nothing is attached.
 */
export function resolveMetadataUserIdValue(
  config: ProviderConfig,
  env: NodeJS.ProcessEnv = process.env,
): string | undefined {
  const userId = resolveMetadataUserId(config, env);
  const extra = resolveExtraMetadata(config, env);
  if (extra) {
    return JSON.stringify({ ...extra, ...(userId ? { user_id: userId } : {}) });
  }
  return userId;
}

/**
 * Build the AI SDK `providerOptions` fragment that forwards the metadata user id to
 * the active provider: Anthropic expects `providerOptions.anthropic.metadata.userId`,
 * every OpenAI-compatible provider expects `providerOptions.openai.user`. Returns an
 * empty object when there is no id, so it spreads to nothing.
 */
export function buildMetadataProviderOptions(
  userId: string | undefined,
  isAnthropic: boolean,
): Record<string, unknown> {
  if (!userId) return {};
  return isAnthropic
    ? { providerOptions: { anthropic: { metadata: { userId } } } }
    : { providerOptions: { openai: { user: userId } } };
}

/**
 * Deep-merge the `providerOptions` of several call-option fragments into a single
 * `{ providerOptions }` object (or `{}` when none contribute). Prevents a later
 * spread from clobbering an earlier `providerOptions` key — e.g. the prompt-cache,
 * metadata, and thinking fragments can all target `providerOptions.{anthropic|openai}`
 * and must COMBINE, not overwrite.
 */
export function mergeProviderOptionFragments(
  ...fragments: Array<Record<string, unknown>>
): Record<string, unknown> {
  let merged: Record<string, unknown> = {};
  for (const frag of fragments) {
    const po = frag.providerOptions;
    if (isPlainRecord(po)) merged = deepMergeRecords(merged, po);
  }
  return Object.keys(merged).length > 0 ? { providerOptions: merged } : {};
}

/**
 * Inject Anthropic prompt-cache breakpoints into a RAW Anthropic Messages request
 * body (the wire format the Vertex endpoint receives). The @ai-sdk/google-vertex
 * provider does NOT forward providerOptions.cacheControl to the wire (measured:
 * 0% cache), so we add cache_control at the HTTP layer. Markers are placed on the
 * stable prefix in Anthropic's hierarchy — last tool, system, and the last
 * message (rolling tail) — staying within Anthropic's 4-breakpoint cap
 * (tools + system + last-message = 3). Returns the original text on non-JSON
 * bodies. Exported for testing.
 */
// Must match common/src/system-prompt.ts SYSTEM_PROMPT_DYNAMIC_BOUNDARY. Core
// cannot import from common (IP split), so the sentinel is duplicated here.
export const SYSTEM_PROMPT_DYNAMIC_BOUNDARY = '\n\n---DYNAMIC_CONTEXT---\n\n';

export function injectAnthropicCacheControl(bodyText: string, cacheControl: Record<string, unknown>): string {
  let body: any; // eslint-disable-line @typescript-eslint/no-explicit-any
  try { body = JSON.parse(bodyText); } catch { return bodyText; }
  if (!body || typeof body !== 'object') return bodyText;
  const cc = cacheControl;
  // Tools block: mark the last tool (tools are first in the cache hierarchy).
  // Route through the canonical breakpoint-index helper so the SAME TTL/scope-aware
  // breakpoint shape (`cc`, which may carry `ttl:'1h'`/`scope`) is emitted here as on
  // the SDK path — one source for "which tool to cache", no inline re-derivation.
  if (Array.isArray(body.tools) && body.tools.length > 0) {
    const toolBps = buildToolCacheBreakpoints(
      body.tools as unknown as ToolDefinition[],
      true,
      cc as unknown as CacheBreakpoint,
    );
    for (const [i, bp] of Object.entries(toolBps)) {
      const tool = body.tools[Number(i)];
      if (tool && typeof tool === 'object') tool.cache_control = bp;
    }
  }
  // System block: cache ONLY the STATIC prefix (before the dynamic boundary). The
  // dynamic context (memory/workspace/skills) grows each turn and would bust the
  // cache if included — so split it into an uncached trailing block. This is what
  // makes cache READS work (byte-stable cached prefix across turns).
  if (typeof body.system === 'string' && body.system.trim() !== '') {
    const idx = body.system.indexOf(SYSTEM_PROMPT_DYNAMIC_BOUNDARY);
    if (idx >= 0) {
      const staticPart = body.system.slice(0, idx);
      const dynamicPart = body.system.slice(idx + SYSTEM_PROMPT_DYNAMIC_BOUNDARY.length);
      body.system = [
        { type: 'text', text: staticPart, cache_control: cc },
        ...(dynamicPart ? [{ type: 'text', text: dynamicPart }] : []),
      ];
    } else {
      body.system = [{ type: 'text', text: body.system, cache_control: cc }];
    }
  } else if (Array.isArray(body.system) && body.system.length > 0) {
    const first = body.system[0];
    if (first && typeof first === 'object') first.cache_control = cc;
  }
  // Rolling conversation breakpoint (H15): mark the LAST message's final content
  // block. The marker moves to the new tail every turn, but caching reads the
  // LONGEST stable prefix ending at ANY prior breakpoint — so the previous turn's
  // tail (now a stable interior prefix) read-hits on this turn, and we write a
  // fresh cache through the current tail. This keeps the growing conversation
  // cached, not just the tools+static-system prefix. Stays within Anthropic's
  // 4-breakpoint cap (last-tool + static-system + last-message = 3).
  if (Array.isArray(body.messages) && body.messages.length > 0) {
    const lastMsg = body.messages[body.messages.length - 1];
    if (lastMsg && typeof lastMsg === 'object') {
      if (typeof lastMsg.content === 'string' && lastMsg.content !== '') {
        lastMsg.content = [{ type: 'text', text: lastMsg.content, cache_control: cc }];
      } else if (Array.isArray(lastMsg.content) && lastMsg.content.length > 0) {
        const lastBlock = lastMsg.content[lastMsg.content.length - 1];
        if (lastBlock && typeof lastBlock === 'object') lastBlock.cache_control = cc;
      }
    }
  }
  return JSON.stringify(body);
}

/** Anthropic beta token that unlocks the 1M-context window. */
export const ONE_MILLION_CONTEXT_BETA = 'context-1m-2025-08-07';

/**
 * Comma-join Anthropic beta tokens without duplicating one that is already
 * present. `anthropic-beta` is a comma-separated list, so an existing
 * `extended-cache-ttl-2025-04-11` and a new `context-1m-2025-08-07` must both
 * survive as `extended-cache-ttl-2025-04-11,context-1m-2025-08-07`.
 */
export function mergeBetaTokens(existing: string | null | undefined, token: string | string[]): string {
  const tokens = (existing ?? '')
    .split(',')
    .map(t => t.trim())
    .filter(Boolean);
  for (const t of Array.isArray(token) ? token : [token]) {
    const trimmed = t.trim();
    if (trimmed && !tokens.includes(trimmed)) tokens.push(trimmed);
  }
  return tokens.join(',');
}

/**
 * Shape a well-formed Anthropic beta token must have: it starts with an
 * alphanumeric and contains only letters, digits, `.`, `_` and `-`
 * (e.g. `context-1m-2025-08-07`). This is exactly the character set the real
 * beta tokens use, and — critically — it excludes whitespace, commas, and
 * CR/LF/control characters.
 */
export const SAFE_BETA_TOKEN = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;

/**
 * True when `token` is a structurally valid, header-injection-safe
 * `anthropic-beta` value. Mirrors the defensive input-validation the reference
 * applies to externally-supplied betas (utils/betas.ts `filterAllowedSdkBetas`),
 * adapted to AMI's model: AMI treats `ANTHROPIC_BETAS` as a trusted *operator*
 * escape hatch (not a semi-trusted SDK-consumer boundary), so instead of a hard
 * value allowlist that would defeat the escape hatch it validates the token
 * SHAPE. That still closes the real failure modes a raw passthrough leaves open:
 * a stray space / comma / control char or a `\r\n`-carrying value that would
 * either 400 the request or smuggle an extra header line.
 */
export function isSafeBetaToken(token: string): boolean {
  return SAFE_BETA_TOKEN.test(token);
}

/**
 * User escape-hatch beta tokens from the `ANTHROPIC_BETAS` env var: a
 * comma-separated list appended to the `anthropic-beta` header regardless of
 * model, so a user can opt into any API beta without a code change. Mirrors
 * the reference's provider-neutral ANTHROPIC_BETAS knob (comma-split, trimmed,
 * empties dropped).
 *
 * Each token is additionally validated for header-safety via
 * {@link isSafeBetaToken}: a malformed entry (embedded whitespace, control
 * chars, or a smuggled `\r\n` header line) is dropped so one bad env value can
 * neither 400 every request nor inject an extra HTTP header. Well-formed betas
 * — including ones AMI has no built-in constant for — pass through unchanged, so
 * the operator escape hatch is preserved.
 */
export function getExtraAnthropicBetas(): string[] {
  return (process.env.ANTHROPIC_BETAS ?? '')
    .split(',')
    .map(t => t.trim())
    .filter(Boolean)
    .filter(isSafeBetaToken);
}

/** Anthropic beta token for 1h extended prompt-cache TTL. */
export const EXTENDED_CACHE_TTL_BETA = 'extended-cache-ttl-2025-04-11';

/**
 * Anthropic beta token that unlocks a cross-session prompt-cache `scope`
 * (`global` / `org`) on `cache_control`. Mirrors the reference's
 * `PROMPT_CACHING_SCOPE_BETA_HEADER` (constants/betas.ts). The scope widens cache
 * reuse beyond the single API key: an `org`-scoped block can be served from a
 * cache written by another key in the same organization, and `global` is broader
 * still. Required whenever a scoped `cache_control` is sent — without it the API
 * rejects the `scope` field.
 */
export const PROMPT_CACHING_SCOPE_BETA = 'prompt-caching-scope-2026-01-05';

/**
 * Anthropic beta token that lets the model interleave `thinking` blocks between
 * tool calls within a single turn (Claude 4+ families). Mirrors the reference's
 * `INTERLEAVED_THINKING_BETA_HEADER`.
 */
export const INTERLEAVED_THINKING_BETA = 'interleaved-thinking-2025-05-14';

/**
 * Anthropic beta token identifying the reference client surface
 * (the reference's `CLAUDE_CODE_20250219_BETA_HEADER`). Retained as a named
 * constant so the Vertex countTokens allowlist can reference the exact token
 * the reference's own allowlist permits.
 */
export const CLAUDE_CODE_BETA = 'claude-code-20250219';

/**
 * Anthropic beta token enabling provider-native (server-side) context editing
 * (`context_management.edits`). Mirrors the reference's
 * `CONTEXT_MANAGEMENT_BETA_HEADER` and must accompany a request that carries a
 * `context_management` body (see {@link buildAnthropicContextManagement}).
 */
export const CONTEXT_MANAGEMENT_BETA = 'context-management-2025-06-27';

/**
 * Anthropic beta token for token-efficient tool-call encoding — a cost knob that
 * reduces input/output tokens spent on tool calls. Mirrors the reference's
 * `TOKEN_EFFICIENT_TOOLS_BETA_HEADER`. Opt-in (experimental).
 */
export const TOKEN_EFFICIENT_TOOLS_BETA = 'token-efficient-tools-2026-03-28';

/**
 * Anthropic beta token that turns on API-enforced structured outputs — the model
 * is constrained to produce arguments that exactly match a tool's `strict` JSON
 * schema (and enables the native response-format path). Mirrors the reference's
 * `STRUCTURED_OUTPUTS_BETA_HEADER` (constants/betas.ts). Experimental: gated by
 * {@link structuredOutputsEnabled} and model capability, stripped by the kill switch.
 */
export const STRUCTURED_OUTPUTS_BETA = 'structured-outputs-2025-12-15';

/**
 * Interleaved thinking is served on Claude 4+ Opus/Sonnet families. Mirrors
 * the reference's `modelSupportsISP` (which gates on the opus-4/sonnet-4 canonical
 * families); AMI applies the same family match, open-ended for future 4x/5x+ ids.
 */
export function supportsInterleavedThinking(modelId: string): boolean {
  if (typeof modelId !== 'string' || !modelId) return false;
  return /claude-(?:opus|sonnet)-(?:[4-9]|\d{2,})/i.test(modelId);
}

/**
 * Whether a model supports API-enforced structured outputs (the `strict` tool
 * flag + {@link STRUCTURED_OUTPUTS_BETA}). Mirrors the reference's
 * `modelSupportsStructuredOutputs` (utils/betas.ts): only Claude 4.1+ Opus and
 * 4.5+ Sonnet/Haiku generations support it — Claude 4.0 Sonnet and all 3.x models
 * do NOT. The match is version-aware so a bare `claude-sonnet-4` (4.0) is excluded
 * while `claude-sonnet-4-5`/`-4-6` and `claude-opus-4-1`/`-4-5`/`-4-6` qualify.
 */
export function modelSupportsStructuredOutputs(modelId: string): boolean {
  if (typeof modelId !== 'string' || !modelId) return false;
  const m = modelId.toLowerCase();
  return (
    /claude-sonnet-4-(?:[5-9]|\d{2,})/.test(m) ||
    /claude-haiku-4-(?:[5-9]|\d{2,})/.test(m) ||
    /claude-opus-4-(?:[1-9]|\d{2,})/.test(m)
  );
}

/**
 * Structured-outputs opt-in (experimental; stripped by {@link experimentalBetasDisabled}).
 * Off by default because it changes tool-call argument encoding; a user opts in via
 * `SUPERINFERENCE_STRUCTURED_OUTPUTS` (or the Claude-compatible
 * `CLAUDE_CODE_STRUCTURED_OUTPUTS`).
 */
export function structuredOutputsEnabled(): boolean {
  return (
    isEnvTruthy(process.env.SUPERINFERENCE_STRUCTURED_OUTPUTS) ||
    isEnvTruthy(process.env.CLAUDE_CODE_STRUCTURED_OUTPUTS)
  );
}

/**
 * Proactive user kill switch for experimental/first-party-only Anthropic betas.
 * A user hitting a proxy/gateway that 400s on unknown `anthropic-beta` tokens can
 * strip them all at once instead of relying on reactive error-retry. Mirrors
 * the reference's `CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS`; the AMI-native
 * `SUPERINFERENCE_DISABLE_EXPERIMENTAL_BETAS` is honored too.
 */
export function experimentalBetasDisabled(): boolean {
  return (
    isEnvTruthy(process.env.CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS) ||
    isEnvTruthy(process.env.SUPERINFERENCE_DISABLE_EXPERIMENTAL_BETAS)
  );
}

/** Interleaved-thinking opt-out (the reference parity `DISABLE_INTERLEAVED_THINKING`). */
export function interleavedThinkingDisabled(): boolean {
  return (
    isEnvTruthy(process.env.DISABLE_INTERLEAVED_THINKING) ||
    isEnvTruthy(process.env.SUPERINFERENCE_DISABLE_INTERLEAVED_THINKING)
  );
}

/** Token-efficient-tools opt-in (experimental; stripped by the kill switch). */
export function tokenEfficientToolsEnabled(): boolean {
  return (
    isEnvTruthy(process.env.SUPERINFERENCE_TOKEN_EFFICIENT_TOOLS) ||
    isEnvTruthy(process.env.CLAUDE_CODE_TOKEN_EFFICIENT_TOOLS)
  );
}

/**
 * Opt-in: after each completed tool batch, generate an LLM-written, PAST-TENSE
 * "what this batch accomplished" summary label (parity: the reference's
 * toolUseSummaryGenerator, gated by `CLAUDE_CODE_EMIT_TOOL_USE_SUMMARIES`). Off by
 * default — it costs one extra cheap-model call per tool turn, so an operator opts
 * in explicitly. The AMI-native `SUPERINFERENCE_EMIT_TOOL_USE_SUMMARIES` is honored
 * too. When on, the engine attaches the label to its `tool_summary` analytics event
 * and reports the query's outcome as a `tool_use_summary` {@link AuxQueryResult}.
 */
export function toolUseSummaryLabelEnabled(): boolean {
  return (
    isEnvTruthy(process.env.SUPERINFERENCE_EMIT_TOOL_USE_SUMMARIES) ||
    isEnvTruthy(process.env.CLAUDE_CODE_EMIT_TOOL_USE_SUMMARIES)
  );
}

/**
 * Fine-grained tool streaming (`eager_input_streaming`) opt-in. Without it the
 * Anthropic API buffers the ENTIRE tool-input JSON before emitting any
 * `input_json_delta`, so a tool with a large input (a big file write, a long
 * patch) can hang for minutes with no visible progress. With it the provider
 * streams the input eagerly as it is generated. Off by default (experimental +
 * first-party-only); opt in via `SUPERINFERENCE_FINE_GRAINED_TOOL_STREAMING`
 * (or the Claude-compatible `CLAUDE_CODE_ENABLE_FINE_GRAINED_TOOL_STREAMING`).
 * Mirrors the reference's `tengu_fgts` / env gate (utils/api.ts).
 */
export function fineGrainedToolStreamingEnabled(): boolean {
  return (
    isEnvTruthy(process.env.SUPERINFERENCE_FINE_GRAINED_TOOL_STREAMING) ||
    isEnvTruthy(process.env.CLAUDE_CODE_ENABLE_FINE_GRAINED_TOOL_STREAMING)
  );
}

/** One `anthropic-beta` token AMI may send, with the condition under which it is
 *  activated. The introspectable complement to {@link computeAnthropicBetas} (which
 *  needs a concrete model + per-request config). */
export interface AnthropicBetaDescriptor {
  /** The literal `anthropic-beta` token value. */
  token: string;
  /** Human-readable activation condition (model/config/env gate). */
  condition: string;
}
