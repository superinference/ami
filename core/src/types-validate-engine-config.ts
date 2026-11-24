// Split from types.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { RateLimitTrackerOptions } from './rate-limiter';
import type { DocumentContent, ImageContent, SystemMessage } from './types-transcript-meta';
import type { ToolDefinition } from './types-tool-definition';
import type { ProviderConfig, ThinkingConfig } from './types-provider-config';
import type { EngineConfig, PermissionSubsystem, SandboxPolicy } from './types';

/** Resolve a tool's open-world (SSRF/egress-surface) status for a SPECIFIC input,
 *  the single place {@link ToolDefinition.isOpenWorld} is consulted so both forms —
 *  a plain `boolean` and the reference's input-aware `isOpenWorld?(input)` predicate — are
 *  handled uniformly. A throwing predicate degrades to `false` (closed-world), never
 *  propagating, mirroring the reference's safe `tool.isOpenWorld?.(input) || false`
 *  consult (cli/print.ts, MCPToolDetailView). Absent field ⇒ `false`.
 *  `input` defaults to `{}` for a tool-level query (the badge/introspection case,
 *  parity with the reference's `tool.isOpenWorld?.({})`). */
export function resolveToolOpenWorld(
  tool: Pick<ToolDefinition, 'isOpenWorld'>,
  input: Record<string, unknown> = {},
): boolean {
  const flag = tool.isOpenWorld;
  if (typeof flag === 'function') {
    try {
      return flag(input) === true;
    } catch {
      return false;
    }
  }
  return flag === true;
}

/** Whether a {@link SystemMessage} is a proactive `'suggestion'` (e.g. a
 *  skill-improvement confirmation), so a renderer can style it apart from ordinary
 *  system output. Parity: the reference's `'suggestion'` message subtype. */
export function isSuggestionMessage(message: Pick<SystemMessage, 'kind'>): boolean {
  return message.kind === 'suggestion';
}

/** Final permission-check decision for a tool call given an upstream auto-approval
 *  verdict and a {@link ToolContext.requireCanUseTool} override: the override FORCES a
 *  real check (returns `true`) even when `autoApproved` is `true`; otherwise an
 *  auto-approval skips the check (returns `false`). Parity: the reference's forked-agent
 *  `requireCanUseTool` that re-asserts the permission gate past hook auto-approval. */
export function shouldForcePermissionCheck(opts: {
  requireCanUseTool?: boolean;
  autoApproved?: boolean;
}): boolean {
  if (opts.requireCanUseTool === true) return true;
  return opts.autoApproved !== true;
}

/** Persisted metadata attached to a conversation-summary (compaction) marker so it is
 *  re-rendered inline in history: how many messages were summarized, the direction of
 *  the summary, and the optional user focus/context note. Validate raw/persisted input
 *  with {@link validateSummarizeMetadata}. Parity: the reference's `summarizeMetadata`
 *  ({messagesSummarized, direction, userContext}) on the summary user message. */
export interface SummarizeMetadata {
  /** Number of prior messages folded into the summary. */
  messagesSummarized: number;
  /** Which end of the transcript was summarized. Parity: the reference's `direction`. */
  direction?: 'before' | 'after';
  /** Optional free-text focus/context the user supplied to steer the summary
   *  (re-rendered as `Context: "…"`). */
  userContext?: string;
}

/** Validate + normalize a raw/persisted {@link SummarizeMetadata} object (e.g. loaded
 *  from a resumed session). Returns a clean metadata object, or `null` when the shape
 *  is unusable (missing/negative/non-integer count). Pure guard — no display. */
export function validateSummarizeMetadata(raw: unknown): SummarizeMetadata | null {
  if (typeof raw !== 'object' || raw === null) return null;
  const o = raw as Record<string, unknown>;
  const count = o.messagesSummarized;
  if (typeof count !== 'number' || !Number.isInteger(count) || count < 0) return null;
  const meta: SummarizeMetadata = { messagesSummarized: count };
  if (o.direction === 'before' || o.direction === 'after') meta.direction = o.direction;
  if (typeof o.userContext === 'string') {
    const ctx = o.userContext.trim();
    if (ctx.length > 0) meta.userContext = ctx;
  }
  return meta;
}

// ---------------------------------------------------------------------------
// DI subsystem interfaces — typed groups of related EngineConfig fields
// ---------------------------------------------------------------------------

export interface ProviderSubsystem {
  primary: ProviderConfig;
  fallbackModel?: string;
  /** Ordered fallback chain (normalized from fallbackModels + fallbackModel, deduped, capped). */
  fallbackModels?: string[];
  compactionModel?: string;
  /** Architect model for dual-model (plan + execute) mode. */
  architectModel?: string;
  thinking?: ThinkingConfig;
  /**
   * Rate-limit tracker tuning (warn/error thresholds + proactive early-warning
   * ladders). Threaded from {@link EngineConfig.rateLimit} so the coordinator's
   * {@link RateLimitTracker} is configurable rather than always defaulting;
   * undefined = provider defaults.
   */
  rateLimit?: RateLimitTrackerOptions;
}

export interface SessionSubsystem {
  sessionId?: string;
  sessionDir?: string;
  tokenBudget: number;
  maxTurns: number;
  maxSteps?: number;
}

export function buildSubsystems(config: EngineConfig): {
  provider: ProviderSubsystem;
  permissions: PermissionSubsystem;
  session: SessionSubsystem;
} {
  return {
    provider: {
      primary: config.provider,
      fallbackModel: config.fallbackModel,
      fallbackModels: (() => {
        const chain = [...(config.fallbackModels ?? []), ...(config.fallbackModel ? [config.fallbackModel] : [])]
          .filter((m): m is string => !!m);
        return [...new Set(chain)].slice(0, 3);
      })(),
      compactionModel: config.compactionModel,
      architectModel: config.architectModel,
      thinking: config.thinking,
      rateLimit: config.rateLimit,
    },
    permissions: {
      mode: config.permissionMode ?? 'ask',
      rules: config.permissionRules ?? [],
      promptHandler: config.permissionPromptHandler,
    },
    session: {
      sessionId: config.sessionId,
      sessionDir: config.sessionDir,
      tokenBudget: config.tokenBudget ?? 100_000,
      maxTurns: (config.maxTurns && config.maxTurns > 0 && Number.isFinite(config.maxTurns))
        ? config.maxTurns
        : 100,
      maxSteps: config.maxSteps,
    },
  };
}

export interface EngineSubmitOptions {
  systemPromptOverride?: string;
  /** Per-turn SOFT replacement of the base system prompt: replaces the default
   *  (or session-level {@link EngineConfig.systemPrompt}) base for this one turn,
   *  while dynamic overlays and `appendSystemPrompt` STILL apply on top. This is a
   *  distinct, weaker tier than {@link systemPromptOverride} (which is a HARD,
   *  byte-exact replace that drops append and every dynamic section). Parity:
   *  the reference's `customSystemPrompt` (`--system-prompt`) vs `overrideSystemPrompt`
   *  priority order in `utils/systemPrompt.ts` — custom replaces the default but
   *  append is still honored, whereas override short-circuits everything. */
  customSystemPrompt?: string;
  /** Text appended to the END of the active system prompt (default / override / agent)
   *  for this turn, preserving the full prompt rather than replacing it. Parity:
   *  the reference's per-call `appendSystemPrompt`. */
  appendSystemPrompt?: string;
  contextFiles?: ContextFile[];
  /** JSON Schema for structured output. When provided, the model output is validated against this schema. */
  outputSchema?: Record<string, unknown>;
  /** Max retries for schema validation (default 3). */
  maxStructuredRetries?: number;
  /**
   * Image attachments for this user turn (e.g. drag-dropped or path-referenced
   * files). Attached to the user message as multimodal content when the active
   * model supports vision; otherwise ignored with a note.
   */
  images?: ImageContent[];
  /**
   * Document attachments (e.g. PDFs) for this user turn. Attached to the user
   * message as native `document` content blocks when the active model supports
   * documents ({@link modelSupportsDocuments}); otherwise ignored with a note so
   * the model can tell the user. Parity: the reference's `document` content block emitted
   * by the Read tool for PDFs.
   */
  documents?: DocumentContent[];
  /**
   * Per-turn total output-token budget for this submit. When set, the engine sizes
   * the request `max_tokens` against the remaining budget and decrements it across
   * context-manager compaction boundaries ({@link nextTaskBudgetRemaining}) so a long,
   * multi-compaction turn stays under the ceiling. Parity: the reference's query-time
   * `taskBudget: { total }` with server-side `task_budget.remaining` countdown
   * (query.ts beta `task-budgets-2026-03-13`).
   */
  taskBudget?: TaskBudget;
}

/**
 * A per-turn output-token budget carried on {@link EngineSubmitOptions.taskBudget}.
 * `total` is the ceiling for the whole turn; the engine tracks a separate remaining
 * figure that is decremented at each compaction boundary. Parity: the reference's
 * `OutputConfig.task_budget` ({ total, remaining }).
 */
export interface TaskBudget {
  /** Total output-token ceiling for the turn. */
  total: number;
}

/**
 * Compute the remaining output-token budget after a context-manager compaction.
 * Mirrors the reference's per-compact carryover
 * `remaining = max(0, (remaining ?? total) - preCompactContextTokens)` (query.ts):
 * the first compaction subtracts from `total`, each subsequent one from the prior
 * remaining, and the result never goes negative. Pure.
 */
export function nextTaskBudgetRemaining(
  budget: TaskBudget,
  preCompactContextTokens: number,
  currentRemaining?: number,
): number {
  const base = currentRemaining ?? budget.total;
  return Math.max(0, base - Math.max(0, preCompactContextTokens));
}

export interface ContextFile {
  path: string;
  content: string;
  language?: string;
}

// ---------------------------------------------------------------------------
// EngineConfig validation & sanitization
// ---------------------------------------------------------------------------

/** Canonical bounded-integer/number knob validator. Pushes a warning and returns
 *  `false` (invalid) when `value` is out of the hard [min,max] range or (when
 *  `integer`) non-integer; pushes a soft advisory (still valid) past `softMax`.
 *  Single source for every numeric EngineConfig/sandbox knob check so the range
 *  logic is never re-implemented per site (dedup). */
export function validateNumericKnob(
  value: number,
  name: string,
  opts: { min: number; max?: number; softMax?: number; integer?: boolean },
  warnings: string[],
): boolean {
  if (opts.integer && !Number.isInteger(value)) {
    warnings.push(`${name} must be an integer`);
    return false;
  }
  if (!Number.isFinite(value) || value < opts.min) {
    warnings.push(`${name} must be >= ${opts.min}`);
    return false;
  }
  if (opts.max !== undefined && value > opts.max) {
    warnings.push(`${name} must be <= ${opts.max}`);
    return false;
  }
  if (opts.softMax !== undefined && value > opts.softMax) {
    warnings.push(`${name} exceeds ${opts.softMax} — likely misconfigured`);
  }
  return true;
}

/** @internal Test seam — `opts.max` is unused by current call sites (softMax only). */
export function __validateNumericKnobForTest(
  value: number,
  name: string,
  opts: { min: number; max?: number; softMax?: number; integer?: boolean },
  warnings: string[],
): boolean {
  return validateNumericKnob(value, name, opts, warnings);
}

/** True when `n` is a usable TCP port (integer in 1..65535). */
export function isValidPort(n: number): boolean {
  return Number.isInteger(n) && n >= 1 && n <= 65535;
}

/** Validate a {@link SandboxPolicy}: mode enum, proxy ports, host/path list
 *  shapes, and cross-field sanity (fail-closed only meaningful when enabled).
 *  Pushes warnings and returns `false` on a hard error. Reached by
 *  {@link validateEngineConfig}. */
export function validateSandboxPolicy(sandbox: SandboxPolicy, warnings: string[]): boolean {
  let valid = true;
  if (sandbox.mode !== 'off' && sandbox.mode !== 'container') {
    warnings.push(`sandbox.mode must be 'off' or 'container'`);
    valid = false;
  }
  const net = sandbox.network;
  if (net) {
    for (const port of [['httpProxyPort', net.httpProxyPort], ['socksProxyPort', net.socksProxyPort]] as const) {
      if (port[1] !== undefined && !isValidPort(port[1])) {
        warnings.push(`sandbox.network.${port[0]} must be a port in 1-65535`);
        valid = false;
      }
    }
    for (const [field, list] of [['allowedHosts', net.allowedHosts], ['deniedHosts', net.deniedHosts], ['managedAllowedHosts', net.managedAllowedHosts]] as const) {
      if (list && list.some((h) => typeof h !== 'string' || h.trim() === '')) {
        warnings.push(`sandbox.network.${field} entries must be non-empty strings`);
        valid = false;
      }
    }
  }
  const fs = sandbox.filesystem;
  if (fs) {
    for (const [field, list] of [['allowWrite', fs.allowWrite], ['denyWrite', fs.denyWrite], ['denyRead', fs.denyRead], ['allowRead', fs.allowRead], ['managedAllowRead', fs.managedAllowRead]] as const) {
      if (list && list.some((p) => typeof p !== 'string' || p.trim() === '')) {
        warnings.push(`sandbox.filesystem.${field} entries must be non-empty paths`);
        valid = false;
      }
    }
  }
  if (sandbox.seccomp?.profilePath !== undefined) {
    if (typeof sandbox.seccomp.profilePath !== 'string' || sandbox.seccomp.profilePath.trim() === '') {
      warnings.push('sandbox.seccomp.profilePath must be a non-empty path');
      valid = false;
    }
  }
  if (sandbox.failIfUnavailable && sandbox.mode === 'off') {
    warnings.push('sandbox.failIfUnavailable has no effect when sandbox.mode is off');
  }
  if (sandbox.ripgrep !== undefined) {
    if (typeof sandbox.ripgrep.command !== 'string' || sandbox.ripgrep.command.trim() === '') {
      warnings.push('sandbox.ripgrep.command must be a non-empty string');
      valid = false;
    }
    if (sandbox.ripgrep.args && sandbox.ripgrep.args.some((a) => typeof a !== 'string' || a.trim() === '')) {
      warnings.push('sandbox.ripgrep.args entries must be non-empty strings');
      valid = false;
    }
  }
  return valid;
}

export function validateEngineConfig(config: Partial<EngineConfig>): { valid: boolean; warnings: string[] } {
  const warnings: string[] = [];
  let valid = true;

  if (config.maxTurns !== undefined) {
    if (!validateNumericKnob(config.maxTurns, 'maxTurns', { min: 1, softMax: 10000, integer: true }, warnings)) valid = false;
  }
  if (config.maxSessionMs !== undefined) {
    if (config.maxSessionMs <= 0) { warnings.push('maxSessionMs must be positive'); valid = false; }
    else if (config.maxSessionMs > 86_400_000) { warnings.push('maxSessionMs exceeds 24 hours'); valid = false; }
    else if (config.maxSessionMs > 3_600_000) { warnings.push('maxSessionMs exceeds 1 hour — are you sure?'); }
  }
  if (config.maxBudgetUsd !== undefined) {
    if (config.maxBudgetUsd <= 0) { warnings.push('maxBudgetUsd must be positive'); valid = false; }
    else if (config.maxBudgetUsd > 1000) { warnings.push('maxBudgetUsd exceeds $1000'); valid = false; }
    else if (config.maxBudgetUsd > 100) { warnings.push('maxBudgetUsd exceeds $100 — are you sure?'); }
  }
  if (config.maxAgentDepth !== undefined) {
    if (!Number.isInteger(config.maxAgentDepth) || config.maxAgentDepth < 1 || config.maxAgentDepth > 20) {
      warnings.push('maxAgentDepth must be 1-20');
      valid = false;
    } else if (config.maxAgentDepth > 10) {
      warnings.push('maxAgentDepth > 10 — deep nesting may be expensive');
    }
  }
  if (config.maxUsageLimitWaitMs !== undefined && config.maxUsageLimitWaitMs < 0) {
    warnings.push('maxUsageLimitWaitMs must be non-negative');
    valid = false;
  }
  if (config.requestTimeoutMs !== undefined) {
    if (config.requestTimeoutMs < 1000 || config.requestTimeoutMs > 600_000) {
      warnings.push('requestTimeoutMs must be 1000-600000');
      valid = false;
    } else if (config.requestTimeoutMs > 300_000) {
      warnings.push('requestTimeoutMs > 5min may cause timeouts elsewhere');
    }
  }
  if (config.tokenBudget !== undefined && config.tokenBudget <= 0) {
    warnings.push('tokenBudget must be positive');
    valid = false;
  }
  if (config.maxWebSearchesPerSession !== undefined) {
    if (!validateNumericKnob(config.maxWebSearchesPerSession, 'maxWebSearchesPerSession', { min: 1, softMax: 10_000, integer: true }, warnings)) valid = false;
  }
  if (config.bashOutputMaxChars !== undefined) {
    if (!validateNumericKnob(config.bashOutputMaxChars, 'bashOutputMaxChars', { min: 1, softMax: 10_000_000, integer: true }, warnings)) valid = false;
  }
  if (config.taskOutputMaxChars !== undefined) {
    if (!validateNumericKnob(config.taskOutputMaxChars, 'taskOutputMaxChars', { min: 1, softMax: 10_000_000, integer: true }, warnings)) valid = false;
  }
  if (config.lspRecommendationIgnoredCount !== undefined) {
    if (!validateNumericKnob(config.lspRecommendationIgnoredCount, 'lspRecommendationIgnoredCount', { min: 0, integer: true }, warnings)) valid = false;
  }
  if (config.maxSkillProgressMessages !== undefined) {
    if (!validateNumericKnob(config.maxSkillProgressMessages, 'maxSkillProgressMessages', { min: 0, integer: true }, warnings)) valid = false;
  }
  if (config.sandbox !== undefined) {
    if (!validateSandboxPolicy(config.sandbox, warnings)) valid = false;
  }

  return { valid, warnings };
}
