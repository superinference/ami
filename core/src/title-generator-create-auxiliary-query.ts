// Split from title-generator.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { randomUUID } from 'crypto';
import type { ProviderConfig, Message } from './types';
import { streamChatCompletion } from './provider';
import { extractJsonStringField, runAuxQuery } from './title-generator-run-aux-query';
import type { AuxQueryFailure, AuxQueryOutcome, AuxQueryResult, AuxQuerySource, OnAuxQueryResult } from './title-generator-run-aux-query';

/**
 * Emit the structured {@link AuxQueryResult} for a finished background query
 * (parity: the reference's two-tier `logResult`). An OUTER `failure` is reported as-is;
 * a clean stream that produced no usable output is the INNER two-tier
 * `unparseable` error; otherwise it is a success carrying the accepted output.
 * Fire-and-forget: a throwing sink never breaks the query.
 */
export function buildAuxResult(
  source: AuxQuerySource,
  model: string,
  messageId: string | undefined,
  failure: AuxQueryFailure | undefined,
  usable: string,
): AuxQueryResult {
  const uuid = randomUUID();
  if (failure) {
    return { type: 'error', source, model, reason: failure.reason, error: failure.error, messageId, uuid };
  }
  if (!usable) {
    return { type: 'error', source, model, reason: 'unparseable', messageId, uuid };
  }
  return { type: 'success', source, model, result: usable, messageId, uuid };
}

export function reportAuxResult(
  onResult: OnAuxQueryResult | undefined,
  source: AuxQuerySource,
  model: string,
  messageId: string | undefined,
  failure: AuxQueryFailure | undefined,
  usable: string,
): void {
  if (!onResult) return;
  const result = buildAuxResult(source, model, messageId, failure, usable);
  try { onResult(result); } catch {}
}

// ---------------------------------------------------------------------------
// Generic declarative auxiliary-query factory
// ---------------------------------------------------------------------------

/**
 * Declarative config for one kind of auxiliary (non-streaming) background query.
 * Parity: the reference's `ApiQueryHookConfig` + `createApiQueryHook<TResult>`
 * (utils/hooks/apiQueryHookHelper.ts) — a single reusable factory drives EVERY
 * side-query against the model from a data structure instead of per-site stream
 * loops. Here it composes the canonical {@link runAuxQuery} primitive (shared with
 * {@link generateTitle} / {@link generateBranchName}) with a per-query
 * `parseResponse` and the uniform {@link AuxQueryResult} introspection record, so a
 * new auxiliary query (e.g. compaction-summary generation) is declared, not
 * re-implemented.
 */
export interface AuxiliaryQueryConfig<TResult> {
  /** Named source tag for the structured {@link AuxQueryResult}. */
  source: AuxQuerySource;
  /** Turn the raw concatenated model output into the typed result, or `undefined`
   *  when the clean output is unusable (surfaced as the two-tier `unparseable`
   *  inner failure). */
  parseResponse: (raw: string) => TResult | undefined;
  /** Render the parsed result as the string carried in a success record
   *  (defaults to `String(parsed)`). */
  resultText?: (parsed: TResult) => string;
  /** Wall-clock cap for the query; pass `0` (or non-finite) to run untimed. */
  timeoutMs?: number;
}

/** The complete, observable outcome of one declarative auxiliary query. */
export interface AuxiliaryQueryRun<TResult> {
  /** Raw stream outcome (raw text, classified outer failure, correlation id). */
  outcome: AuxQueryOutcome;
  /** Parsed typed result, or `undefined` on failure / unusable output. */
  parsed: TResult | undefined;
  /** Uniform structured record (also delivered to the `onResult` sink). */
  result: AuxQueryResult;
}

/**
 * Build a reusable auxiliary-query runner from a declarative {@link
 * AuxiliaryQueryConfig}. The returned function runs ONE deterministic background
 * query through the shared {@link runAuxQuery} primitive, parses it per config,
 * emits the uniform {@link AuxQueryResult} to an optional introspection sink
 * (fire-and-forget), and returns the full {@link AuxiliaryQueryRun} so the caller
 * can both observe the outcome and branch on it (e.g. retry / fall back).
 */
export function createAuxiliaryQuery<TResult>(
  config: AuxiliaryQueryConfig<TResult>,
): (
  provider: ProviderConfig,
  messages: Message[],
  streamImpl?: typeof streamChatCompletion,
  signal?: AbortSignal,
  onResult?: OnAuxQueryResult,
) => Promise<AuxiliaryQueryRun<TResult>> {
  return async (provider, messages, streamImpl = streamChatCompletion, signal, onResult) => {
    const outcome = await runAuxQuery(provider, messages, streamImpl, signal, config.timeoutMs);
    const parsed = outcome.failure ? undefined : config.parseResponse(outcome.raw);
    const usable = parsed === undefined
      ? ''
      : (config.resultText ? config.resultText(parsed) : String(parsed));
    const result = buildAuxResult(config.source, provider.model, outcome.messageId, outcome.failure, usable);
    if (onResult) {
      try { onResult(result); } catch { /* fire-and-forget: a throwing sink never breaks the query */ }
    }
    return { outcome, parsed, result };
  };
}

/**
 * Coerce ANY string into a safe, readable git-branch slug (kebab-case): lowercase,
 * ASCII `[a-z0-9]` words joined by single dashes, no leading/trailing dashes,
 * truncated on a dash boundary to `max` chars. Parity with the reference's
 * teleport `generateTitleAndBranch` branch shape ("all lower case, words
 * separated by dashes") and its truncation fallback, minus the vendor `claude/`
 * prefix — AMI's worktree branches live under `worktree/<slug>`, so the slug here
 * is the descriptive tail only. A single leading `prefix/` segment the model may
 * have emitted (e.g. `claude/` or `feature/`) is dropped so it never leaks into
 * the name. Used by {@link generateBranchName} to make the model's raw output a
 * valid slug by construction (so it passes the enter_worktree slug validators),
 * and to build the deterministic fallback from the task description itself.
 * Returns '' when nothing usable survives (caller substitutes its own fallback).
 */
export function sanitizeBranchSlug(raw: string, max = 40): string {
  if (!raw) return '';
  let t = raw.trim().toLowerCase();
  // Drop a single leading `<segment>/` prefix (vendor/namespace like `claude/`).
  t = t.replace(/^[a-z0-9][a-z0-9-]*\//, '');
  // Everything that is not an ASCII alnum becomes a word boundary.
  t = t.replace(/[^a-z0-9]+/g, '-').replace(/-+/g, '-').replace(/^-+|-+$/g, '');
  if (t.length > max) {
    t = t.slice(0, max);
    const lastDash = t.lastIndexOf('-');
    if (lastDash >= Math.floor(max * 0.6)) t = t.slice(0, lastDash);
    t = t.replace(/-+$/, '');
  }
  return t;
}

/**
 * Parse a structured branch slug out of the model's raw output: the
 * branch-name prompt asks for `{"branch": "..."}`. Delegates to the shared
 * {@link extractJsonStringField} reader (same tolerant JSON handling as the title
 * path) so there is one canonical structured-output reader. Returns `undefined`
 * when no usable `branch` string is present.
 */
export function parseBranchJson(raw: string): string | undefined {
  return extractJsonStringField(raw, ['branch']);
}

// ---------------------------------------------------------------------------
// Tool-use summary label (LLM-written "what this batch accomplished") — parity
// with the reference's toolUseSummaryGenerator.ts generateToolUseSummary().
// ---------------------------------------------------------------------------

/**
 * Max chars of a single tool's input/output JSON fed into the summary prompt.
 * Parity: the reference's `truncateJson(…, 300)` (toolUseSummaryGenerator.ts) — keeps the
 * prompt small/cheap while preserving the distinctive head of each value.
 */
export const TOOL_USE_SUMMARY_IO_MAX = 300;

/**
 * Max rendered length of a generated tool-use summary label. the reference's label row
 * "truncates around 30 characters" (the system prompt below); AMI caps the STORED
 * label at a small budget so a verbose model reply can't bloat the analytics /
 * progress-affordance surface. Applied via the shared {@link truncateTitle}
 * (word-boundary + ellipsis affordance), not a bare slice.
 */
export const TOOL_USE_SUMMARY_LABEL_MAX = 40;

/** One completed tool call fed into {@link generateToolUseSummaryLabel} (parity:
 *  the reference's `ToolInfo` — name + input + output). */
export interface ToolUseSummaryInput {
  name: string;
  input: unknown;
  output: unknown;
}

/**
 * Serialize a value to JSON truncated to `maxLength` chars for the summary prompt.
 * Parity: the reference's `truncateJson` — a non-serializable value (circular ref, bigint,
 * or a value that stringifies to `undefined` like a bare function/`undefined`)
 * yields the literal `[unable to serialize]` rather than throwing. A truncated value
 * gets a trailing `...` so the model sees it was clipped.
 */
export function truncateJsonForSummary(value: unknown, maxLength = TOOL_USE_SUMMARY_IO_MAX): string {
  let str: string | undefined;
  try {
    str = JSON.stringify(value);
  } catch {
    return '[unable to serialize]';
  }
  if (str === undefined) return '[unable to serialize]';
  if (str.length <= maxLength) return str;
  return str.slice(0, maxLength - 3) + '...';
}

/**
 * Build the USER prompt for a tool-use summary query (parity: the reference's
 * generateToolUseSummary body): one `Tool/Input/Output` block per completed call,
 * optionally prefixed by the user's intent lifted from the assistant's last message
 * (first 200 chars). The fixed instruction lives in the SYSTEM message
 * ({@link TOOL_USE_SUMMARY_PROMPT}) so untrusted tool output can't masquerade as an
 * instruction (same prompt-injection isolation as {@link generateTitle}).
 */
export function buildToolUseSummaryPrompt(
  tools: readonly ToolUseSummaryInput[],
  lastAssistantText?: string,
): string {
  const toolSummaries = tools
    .map(t => `Tool: ${t.name}\nInput: ${truncateJsonForSummary(t.input)}\nOutput: ${truncateJsonForSummary(t.output)}`)
    .join('\n\n');
  const contextPrefix = lastAssistantText && lastAssistantText.trim()
    ? `User's intent (from assistant's last message): ${lastAssistantText.slice(0, 200)}\n\n`
    : '';
  return `${contextPrefix}Tools completed:\n\n${toolSummaries}\n\nLabel:`;
}
