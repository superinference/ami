// Split from provider.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { jsonSchema } from 'ai';
import type { ModelMessage, ToolSet } from 'ai';
import { isEnvTruthy } from './analytics';
import { partitionAndSortTools } from './tools/tool-utils';
import { memoizeToolSchema } from './tools/tool-schema-cache';
import type { Message, ToolDefinition } from './types';
import { SYSTEM_PROMPT_DYNAMIC_BOUNDARY } from './provider-inject-anthropic-cache-control';

/**
 * L22: strip the `---DYNAMIC_CONTEXT---` prompt-cache boundary sentinel from the
 * outgoing request body on NON-vertex providers, so the model never sees a stray
 * marker line. On anthropic-vertex the boundary is CONSUMED by
 * injectAnthropicCacheControl (it splits the system into cached + dynamic blocks),
 * so this wrapper is applied ONLY to non-vertex fetches. It operates on the raw
 * serialized JSON (replacing the JSON-escaped boundary with a plain `\n\n`
 * separator), so it is body-shape agnostic — it fixes both Anthropic's `system`
 * string and the OpenAI-style system message in `messages[]`. No-op when the
 * sentinel is absent.
 */
export function withDynamicBoundaryStrip(
  baseFetch: typeof globalThis.fetch | undefined,
): typeof globalThis.fetch {
  const base = baseFetch ?? globalThis.fetch;
  // The boundary as it appears inside a JSON string (newlines escaped), derived
  // from the const so it never drifts.
  const escaped = JSON.stringify(SYSTEM_PROMPT_DYNAMIC_BOUNDARY).slice(1, -1);
  const escapedSep = JSON.stringify('\n\n').slice(1, -1);
  return (async (input: any, init?: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
    if (init && typeof init.body === 'string' && init.body.includes('---DYNAMIC_CONTEXT---')) {
      init = { ...init, body: init.body.split(escaped).join(escapedSep) };
    }
    return base(input, init);
  }) as typeof globalThis.fetch;
}

/** Default Vertex AI region waterfall: CLOUD_ML_REGION → GOOGLE_VERTEX_LOCATION → us-east5. */
export function getDefaultVertexRegion(): string {
  return process.env.CLOUD_ML_REGION || process.env.GOOGLE_VERTEX_LOCATION || 'us-east5';
}

// Model prefix → Vertex region override env var. Order matters: more specific
// prefixes must precede less specific ones (e.g. 'claude-opus-4-1' before
// 'claude-opus-4') so startsWith resolves the right override first. Mirrors
// the reference's VERTEX_REGION_OVERRIDES (utils/envUtils.ts) — a given the reference
// model may only be GA in a subset of Vertex regions.
export const VERTEX_REGION_OVERRIDES: ReadonlyArray<readonly [string, string]> = [
  ['claude-haiku-4-5', 'VERTEX_REGION_CLAUDE_HAIKU_4_5'],
  ['claude-3-5-haiku', 'VERTEX_REGION_CLAUDE_3_5_HAIKU'],
  ['claude-3-5-sonnet', 'VERTEX_REGION_CLAUDE_3_5_SONNET'],
  ['claude-3-7-sonnet', 'VERTEX_REGION_CLAUDE_3_7_SONNET'],
  ['claude-opus-4-1', 'VERTEX_REGION_CLAUDE_4_1_OPUS'],
  ['claude-opus-4', 'VERTEX_REGION_CLAUDE_4_0_OPUS'],
  ['claude-sonnet-4-6', 'VERTEX_REGION_CLAUDE_4_6_SONNET'],
  ['claude-sonnet-4-5', 'VERTEX_REGION_CLAUDE_4_5_SONNET'],
  ['claude-sonnet-4', 'VERTEX_REGION_CLAUDE_4_0_SONNET'],
];

/**
 * Resolve the Vertex AI region for a specific model: a per-model override env
 * var (when set) wins, otherwise the default region waterfall. Exported for
 * direct unit testing; consumed by the anthropic-vertex branch of resolveModel.
 */
export function getVertexRegionForModel(model: string | undefined): string {
  if (model) {
    const match = VERTEX_REGION_OVERRIDES.find(([prefix]) => model.startsWith(prefix));
    if (match) return process.env[match[1]] || getDefaultVertexRegion();
  }
  return getDefaultVertexRegion();
}

// ---------------------------------------------------------------------------
// Convert our ToolDefinition[] to AI SDK ToolSet
// ---------------------------------------------------------------------------

export type SdkToolEntry = { description: string; inputSchema: ReturnType<typeof jsonSchema> };

export function convertToolsForSDK(tools: ToolDefinition[]): ToolSet {
  const sdkTools: Record<string, SdkToolEntry> = {};
  // Insert in the deterministic built-in-then-MCP sorted order (parity with
  // toolDefinitionsToOpenAI) so the AI-SDK ToolSet key order — and therefore the
  // Anthropic tool-block cache prefix — stays stable across turns as MCP servers
  // connect on demand.
  //
  // `jsonSchema()` re-wraps a tool's JSON schema on every request (the hot path
  // runs once per model turn). The rendered entry is memoized in the session-scoped
  // tool-schema cache keyed by NAME + stable schema (see tool-schema-cache.ts), so
  // the frozen bytes survive an MCP reconnect that hands us a fresh ToolDefinition
  // object (identity keying missed that) or a key-reordered-but-equal schema — the
  // provider tool-block cache prefix no longer busts mid-session.
  for (const tool of partitionAndSortTools(tools)) {
    sdkTools[tool.name] = memoizeToolSchema<SdkToolEntry>(tool, () => ({
      description: tool.description,
      inputSchema: jsonSchema(tool.inputSchema as any),
      // NO execute function — we handle execution in our engine loop
    }));
  }
  return sdkTools;
}

/**
 * Attach an Anthropic cache_control marker to the LAST tool so the whole tool
 * block is cached. Tools precede system+messages in Anthropic's cache hierarchy,
 * so a single marker on the final tool caches every tool schema before it — the
 * biggest, most stable per-turn prefix block. Returns a new ToolSet (does not
 * mutate the input). No-op for an empty set.
 */
export function markLastToolCacheControl(sdkTools: ToolSet, cacheControl: unknown): ToolSet {
  const names = Object.keys(sdkTools);
  if (names.length === 0) return sdkTools;
  const last = names[names.length - 1];
  return {
    ...sdkTools,
    [last]: {
      ...(sdkTools[last] as Record<string, unknown>),
      providerOptions: { anthropic: { cacheControl } },
    },
  } as ToolSet;
}

// ---------------------------------------------------------------------------
// Convert our Message[] to AI SDK ModelMessage[]
// ---------------------------------------------------------------------------

export function convertMessages(messages: Message[]): ModelMessage[] {
  const toolNameById = new Map<string, string>();
  for (const msg of messages) {
    if (msg.role === 'assistant' && msg.tool_calls) {
      for (const tc of msg.tool_calls) toolNameById.set(tc.id, tc.function.name);
    }
  }

  return messages.map((msg, i): ModelMessage => {
    if (msg.role === 'tool') {
      const toolName = toolNameById.get(msg.tool_call_id) ?? '';
      return {
        role: 'tool',
        content: [
          {
            type: 'tool-result',
            toolCallId: msg.tool_call_id,
            toolName: toolName || 'unknown',
            // Surface a failed tool result (e.g. the synthetic placeholder healed in
            // for an unanswered/interrupted tool_call) as an error output so the model
            // sees the tool FAILED, mirroring Anthropic's tool_result is_error flag.
            output: msg.isError
              ? { type: 'error-text', value: msg.content }
              : { type: 'text', value: msg.content },
          },
        ],
      };
    }

    if (msg.role === 'assistant' && msg.tool_calls) {
      return {
        role: 'assistant',
        content: [
          ...(msg.content?.trim() ? [{ type: 'text' as const, text: msg.content }] : []),
          ...msg.tool_calls.map((tc) => ({
            type: 'tool-call' as const,
            toolCallId: tc.id,
            toolName: tc.function.name,
            input: (() => { try { return JSON.parse(tc.function.arguments); } catch { return {}; } })(),
          })),
        ],
      };
    }

    if (msg.role === 'assistant') {
      return { role: 'assistant', content: msg.content ?? '' };
    }

    if (msg.role === 'system') {
      return { role: 'system', content: msg.content };
    }

    // 'user' — handle both plain string and mixed content (text + image)
    if (Array.isArray(msg.content)) {
      const parts = msg.content.map((block) => {
        if (block.type === 'text') {
          return { type: 'text' as const, text: block.text };
        }
        // DocumentContent -> AI SDK file part. A PDF (or other document) rides as a
        // `file` part (base64 data + mediaType), which the Anthropic/Gemini providers
        // translate into a native `document` block — so the model reads the file
        // directly instead of a base64 text blob. Parity: the reference's `document` block.
        if (block.type === 'document') {
          return {
            type: 'file' as const,
            data: block.source.data,
            mediaType: block.source.media_type,
          };
        }
        // ImageContent -> AI SDK image part. The AI SDK v6 ImagePart field is
        // `mediaType` (not `mimeType`); using the wrong name drops the type.
        return {
          type: 'image' as const,
          image: block.source.data,
          mediaType: block.source.media_type,
        };
      });
      return { role: 'user' as const, content: parts } as ModelMessage;
    }
    return { role: 'user', content: msg.content };
  });
}

/**
 * Split the stable leading system prompt from the message stream for the top-level
 * `system` parameter.
 *
 * H3: only the LEADING run of role:'system' messages (the base system prompt) is
 * hoisted into `system` — that string is part of the cached prompt prefix on EVERY
 * cache breakpoint. A role:'system' message that appears AFTER the conversation has
 * begun is an ephemeral/dynamic nudge (budget warnings, hook rewakes, cron notices,
 * …); hoisting it would change the cached prefix every turn and bust the prompt cache.
 * Such messages are kept IN the stream as user-role `<system-reminder>` blocks so the
 * model still sees them, but the cached prefix stays byte-stable turn to turn. This is
 * the reference's behavior (reminders ride as role:'user'). Introspection: the count of
 * demoted nudges is returned as `demoted` for callers/telemetry.
 */
export function hoistSystemPrompt(
  messages: Message[],
): { system?: string; rest: Message[]; demoted: number } {
  let system: string | undefined;
  let started = false;
  let demoted = 0;
  const rest: Message[] = [];
  for (const m of messages) {
    if (m.role === 'system' && !started && typeof m.content === 'string') {
      system = (system ? system + '\n\n' : '') + m.content;
      continue;
    }
    if (m.role === 'system') {
      const wrapped = typeof m.content === 'string'
        ? `<system-reminder>${m.content}</system-reminder>`
        : m.content;
      rest.push({ ...m, role: 'user', content: wrapped });
      started = true;
      demoted++;
      continue;
    }
    started = true;
    rest.push(m);
  }
  return { system, rest, demoted };
}

// ---------------------------------------------------------------------------
// Extract reasoning / thinking tokens from AI SDK usage objects
// ---------------------------------------------------------------------------

/**
 * Pull reasoning/thinking token counts out of the AI SDK usage object.
 * OpenAI-compatible vLLM (Qwen `reasoning_effort`) reports them as
 * `completion_tokens_details.reasoning_tokens`, which the SDK maps to
 * `usage.reasoningTokens` and/or `usage.outputTokenDetails.reasoningTokens`.
 */
/**
 * Cache-read (cached prompt) tokens from a finish part. The AI SDK normalizes
 * usage inconsistently across providers: the Anthropic provider (incl. via
 * google-vertex) reports cache reads under providerMetadata.anthropic, not the
 * normalized usage.cachedInputTokens — so reading only usage.* reports 0 for a
 * WORKING cache (observed on anthropic-vertex). Check every known location.
 */
export function extractCachedPromptTokens(part: unknown): number {
  const p = (part ?? {}) as Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any
  const u = (p.usage ?? {}) as Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any
  const pm = ((p.providerMetadata as Record<string, any> | undefined)?.anthropic ?? {}) as Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any
  const n = (v: unknown): number | undefined =>
    typeof v === 'number' && Number.isFinite(v) && v >= 0 ? v : undefined;
  return (
    n(u.cachedInputTokens) ??
    n(u.inputTokenDetails?.cacheReadTokens) ??
    n(pm.cacheReadInputTokens) ??
    n(pm.cache_read_input_tokens) ??
    0
  );
}

/**
 * Cache-WRITE (cache_creation) tokens from a finish part. Anthropic bills these
 * separately from both uncached input and cache reads (at 1.25×/2× the input
 * rate). The AI SDK surfaces them under providerMetadata.anthropic for the
 * Anthropic provider (incl. via google-vertex); check every known location so a
 * working cache is never priced as free.
 */
export function extractCacheCreationTokens(part: unknown): number {
  const p = (part ?? {}) as Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any
  const u = (p.usage ?? {}) as Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any
  const pm = ((p.providerMetadata as Record<string, any> | undefined)?.anthropic ?? {}) as Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any
  const n = (v: unknown): number | undefined =>
    typeof v === 'number' && Number.isFinite(v) && v >= 0 ? v : undefined;
  return (
    n(u.cacheCreationInputTokens) ??
    n(u.inputTokenDetails?.cacheCreationTokens) ??
    n(pm.cacheCreationInputTokens) ??
    n(pm.cache_creation_input_tokens) ??
    0
  );
}

/**
 * Per-TTL split of cache-WRITE tokens (parity: the reference's emptyUsage
 * `cache_creation.{ephemeral_1h_input_tokens, ephemeral_5m_input_tokens}`).
 * Anthropic prices 1-hour and 5-minute prompt-cache writes differently, so the
 * two TTL buckets must be surfaced independently rather than lumped together. The
 * AI SDK exposes them under `providerMetadata.anthropic.cache_creation`
 * (snake_case) or `.cacheCreation` (camelCase). When the provider surfaces NO
 * per-TTL breakdown we attribute the whole cache-creation aggregate
 * ({@link extractCacheCreationTokens}) to the 5-minute bucket — the standard
 * ephemeral TTL — so no cache-write tokens are ever dropped. Each arm floors
 * non-numeric / negative values to 0 (parity with the aggregate extractor).
 */
export function extractCacheCreationSplit(part: unknown): { ephemeral5m: number; ephemeral1h: number } {
  const p = (part ?? {}) as Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any
  const pm = ((p.providerMetadata as Record<string, any> | undefined)?.anthropic ?? {}) as Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any
  const cc = (pm.cache_creation ?? pm.cacheCreation ?? {}) as Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any
  const n = (v: unknown): number | undefined =>
    typeof v === 'number' && Number.isFinite(v) && v >= 0 ? v : undefined;
  const h1 = n(cc.ephemeral_1h_input_tokens) ?? n(cc.ephemeral1hInputTokens);
  const m5 = n(cc.ephemeral_5m_input_tokens) ?? n(cc.ephemeral5mInputTokens);
  if (h1 !== undefined || m5 !== undefined) {
    return { ephemeral5m: m5 ?? 0, ephemeral1h: h1 ?? 0 };
  }
  return { ephemeral5m: extractCacheCreationTokens(part), ephemeral1h: 0 };
}

export function extractReasoningTokens(usage: unknown): number {
  if (!usage || typeof usage !== 'object') return 0;
  const u = usage as Record<string, unknown>;

  const pick = (value: unknown): number | undefined => {
    if (typeof value === 'number' && Number.isFinite(value) && value > 0) return value;
    return undefined;
  };

  const fromTop = pick(u['reasoningTokens']);
  if (fromTop !== undefined) return fromTop;

  const details = u['outputTokenDetails'];
  if (details && typeof details === 'object') {
    const fromDetails = pick((details as Record<string, unknown>)['reasoningTokens']);
    if (fromDetails !== undefined) return fromDetails;
  }

  const raw = u['raw'];
  if (raw && typeof raw === 'object') {
    const rawObj = raw as Record<string, unknown>;
    const fromRaw = pick(rawObj['reasoning_tokens']);
    if (fromRaw !== undefined) return fromRaw;
    const ctd = rawObj['completion_tokens_details'];
    if (ctd && typeof ctd === 'object') {
      const fromCtd = pick((ctd as Record<string, unknown>)['reasoning_tokens']);
      if (fromCtd !== undefined) return fromCtd;
    }
  }
  return 0;
}

/**
 * User-visible placeholder surfaced when the provider returns a REDACTED (encrypted)
 * thinking block — reasoning the safety systems encrypted so it cannot be displayed.
 * Without a placeholder the user sees a silent gap (the block carries no readable
 * text). Parity: the reference's `AssistantRedactedThinkingMessage` ("✻ Thinking…").
 * AMI appends "(redacted)" so the encrypted nature is explicit.
 */
export const REDACTED_THINKING_PLACEHOLDER = '✻ Thinking… (redacted)';

/**
 * Detect a redacted/encrypted reasoning part and return its opaque `redactedData`
 * blob, or `undefined` for a normal (readable) reasoning part. The `@ai-sdk/anthropic`
 * provider maps a `redacted_thinking` content block to a `reasoning-start` fullStream
 * part carrying `providerMetadata.anthropic.redactedData` (the encrypted payload) and
 * NO subsequent `reasoning-delta` text. A normal thinking block's `reasoning-start`
 * has no such field (its text arrives via `reasoning-delta`). Isolated here so the
 * stream loop and tests share one detection rule. A non-object / absent metadata
 * yields `undefined`.
 */
export function extractRedactedThinkingData(part: unknown): string | undefined {
  if (!part || typeof part !== 'object') return undefined;
  const meta = (part as { providerMetadata?: unknown }).providerMetadata;
  if (!meta || typeof meta !== 'object') return undefined;
  const anthropic = (meta as { anthropic?: unknown }).anthropic;
  if (!anthropic || typeof anthropic !== 'object') return undefined;
  const redacted = (anthropic as { redactedData?: unknown }).redactedData;
  return typeof redacted === 'string' && redacted.length > 0 ? redacted : undefined;
}

// ---------------------------------------------------------------------------
// Build provider-specific thinking / reasoning options
// ---------------------------------------------------------------------------

// Maps a THINKING_LEVELS value to an Anthropic adaptive-thinking effort. 'off' never
// reaches the adaptive branch (filtered by the enabled check); every other level maps
// 1:1. ('xhigh' exists in the API effort scale but not in THINKING_LEVELS, so it is not
// produced here — adding it to THINKING_LEVELS would auto-extend this via the level key.)
export const ADAPTIVE_EFFORT_BY_LEVEL: Record<string, 'low' | 'medium' | 'high' | 'xhigh' | 'max'> = {
  low: 'low', medium: 'medium', high: 'high', max: 'max',
};

/**
 * Env knobs that FORCE the reasoning-effort parameter on for a model AMI's static
 * capability detection did not recognize as a reasoning model (e.g. a brand-new
 * third-party OpenAI-compatible reasoning model). Without this, an unrecognized
 * model falls through to no-thinking even with `--thinking` set. AMI-native name
 * first, the reference's name as a drop-in alias. Mirrors the reference's
 * `modelSupportsEffort()` CLAUDE_CODE_ALWAYS_ENABLE_EFFORT gate (utils/effort.ts).
 */
export const ALWAYS_ENABLE_EFFORT_ENV_VARS = ['SUPERINFERENCE_ALWAYS_ENABLE_EFFORT', 'CLAUDE_CODE_ALWAYS_ENABLE_EFFORT'] as const;

/** True when any {@link ALWAYS_ENABLE_EFFORT_ENV_VARS} knob is set truthy. */
export function alwaysEnableEffort(env: NodeJS.ProcessEnv = process.env): boolean {
  return ALWAYS_ENABLE_EFFORT_ENV_VARS.some(name => isEnvTruthy(env[name]));
}

/** Map a thinking level onto an OpenAI-compatible `reasoning_effort` value (max→high). */
export const OPENAI_EFFORT_BY_LEVEL: Record<string, string> = { low: 'low', medium: 'medium', high: 'high', max: 'high' };
