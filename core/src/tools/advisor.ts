import { ToolDefinition, ToolResult, ToolContext, Message, ProviderConfig } from '../types';
import { streamChatCompletion } from '../provider';
import { runAuxQuery } from '../title-generator';
import { resolveModelAlias } from '../model-registry';
import { isEnvTruthy } from '../privacy-level';
import { contentToTexts } from '../session';

/**
 * On-demand ADVISOR tool: a model-initiated second opinion from a stronger
 * reviewer model, with the whole conversation auto-forwarded.
 *
 * Parity target: Claude Code's `advisor` server-tool (references/claude/
 * claude-code-main/src/utils/advisor.ts) — a tool the MAIN model calls at
 * junctures it chooses (before committing to an approach, when stuck, before
 * declaring done); the entire conversation (task, every tool call, every result)
 * is forwarded to a STRONGER reviewer model which returns ADVICE while the same
 * agent stays in control. AMI has adjacent-but-distinct primitives — the
 * SuperInference LLM critic is framework-initiated and only scores a STOP, the
 * `task` fork clone DOES work and returns a result, `/architect` is a separate
 * plan-vs-execute model — but none is a tool a model can call MID-TASK to get a
 * stronger-model review of its current approach. This tool fills that gap.
 *
 * AMI is multi-provider, so rather than replaying raw provider-specific
 * tool_use/tool_result blocks (which every provider validates differently) the
 * forwarded conversation is flattened into a provider-neutral text transcript
 * digest — the reviewer still sees the task, every tool call (name + arguments),
 * and every result, which is exactly what Claude's advisor forwards. A single
 * reviewer completion is made via the canonical {@link runAuxQuery} side-query
 * primitive (shared with session-title / branch-name generation — no parallel
 * stream loop), and the reviewer's advice is returned as the tool result so the
 * caller can act on it and keep going.
 */

/** Operator override of the reviewer model (alias — e.g. `opus` — or a concrete
 *  id). Mirrors Claude Code's configurable `advisorModel` (getInitialAdvisorSetting
 *  / getExperimentAdvisorModels). Unset → the default reviewer selection applies. */
export const ADVISOR_MODEL_ENV = 'AMI_ADVISOR_MODEL';

/** Off-switch for the advisor tool (parity: Claude Code's
 *  `CLAUDE_CODE_DISABLE_ADVISOR_TOOL`, which that alias is also honored for
 *  cross-framework compatibility). Truthy → the tool is dropped from the
 *  advertised set. Unset/falsey (default) → the tool is available. */
export const ADVISOR_DISABLE_ENV = 'AMI_DISABLE_ADVISOR_TOOL';

/** Cross-framework alias for {@link ADVISOR_DISABLE_ENV}. */
export const ADVISOR_DISABLE_ENV_ALIAS = 'CLAUDE_CODE_DISABLE_ADVISOR_TOOL';

/** Wall-clock budget for a single reviewer completion. A review of a long
 *  transcript legitimately runs longer than a terse title query, so this is
 *  generous; a wedged provider still cannot hang the turn forever. */
export const ADVISOR_TIMEOUT_MS = 180_000;

/** Max characters of a single tool result included in the forwarded digest — a
 *  huge result (e.g. a full file read) is tail-trimmed so one call cannot blow the
 *  reviewer's context, while recent output (the part that matters) is preserved. */
const MAX_RESULT_CHARS = 4_000;

/** Overall cap on the forwarded digest. When the transcript exceeds it, the HEAD
 *  (original task framing) and the TAIL (most recent context) are kept and the
 *  middle is elided — the reviewer needs the goal and the current state most. */
const MAX_DIGEST_CHARS = 200_000;

/** The reviewer framing. Kept provider-neutral; the natural phrase "second
 *  opinion" is the stable marker the reviewer request carries. */
export const ADVISOR_REVIEWER_PREAMBLE =
  'You are a senior engineering reviewer giving a second opinion to another AI agent that is working a task. ' +
  'Below is the full conversation so far — the task, every tool call the agent made, and every result it saw. ' +
  'Review the agent\'s current approach and give concrete, actionable advice. ' +
  'Do NOT perform the task yourself or produce the deliverable — only advise. ' +
  'Be specific: call out wrong assumptions, risks, missing checks, and a better approach if one exists. ' +
  'If the approach looks sound, say so briefly and name the single biggest risk to watch.';

/** True when the advisor tool is disabled by env (AMI or the Claude-compat alias). */
export function isAdvisorDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[ADVISOR_DISABLE_ENV]) || isEnvTruthy(env[ADVISOR_DISABLE_ENV_ALIAS]);
}

/** True when `config` targets a first-party Anthropic (claude) model. */
function isAnthropicModel(config: ProviderConfig): boolean {
  if (config.provider === 'anthropic' || config.provider === 'anthropic-vertex') return true;
  return (config.model ?? '').toLowerCase().startsWith('claude');
}

/**
 * Resolve the reviewer model. An explicit {@link ADVISOR_MODEL_ENV} always wins
 * (alias-resolved, else used verbatim). With no override, a first-party Anthropic
 * run is UPGRADED to the strongest same-family reviewer (the `opus` alias) — the
 * parity behavior of "a STRONGER reviewer model"; any other provider reuses the
 * caller's own model (a fresh clean-context reviewer pass still yields a genuine
 * second opinion) so a claude-opus id is never sent to a non-Anthropic endpoint.
 */
export function resolveAdvisorModel(config: ProviderConfig, env: NodeJS.ProcessEnv = process.env): string {
  const override = env[ADVISOR_MODEL_ENV]?.trim();
  if (override) return resolveModelAlias(override) ?? override;
  if (isAnthropicModel(config)) return resolveModelAlias('opus') ?? config.model;
  return config.model;
}

/** Render one message's text (string or text-block array) joined with newlines. */
function messageText(content: unknown): string {
  return contentToTexts(content).join('\n').trim();
}

/** Tail-trim a tool result to {@link MAX_RESULT_CHARS}, flagging the elision. */
function capResult(text: string): string {
  if (text.length <= MAX_RESULT_CHARS) return text;
  return `[... ${text.length - MAX_RESULT_CHARS} earlier chars elided ...]\n` + text.slice(-MAX_RESULT_CHARS);
}

/**
 * Flatten the forwarded conversation into a provider-neutral transcript digest:
 * every user/assistant/system turn's text, every assistant tool call (name +
 * arguments), and every tool result (error-flagged, size-capped). This is what the
 * reviewer sees in place of raw provider tool blocks.
 */
export function buildTranscriptDigest(messages: Message[]): string {
  const blocks: string[] = [];
  for (const m of messages) {
    if (m.role === 'user') {
      const t = messageText(m.content);
      if (t) blocks.push(`## User\n${t}`);
    } else if (m.role === 'system') {
      const t = messageText(m.content);
      if (t) blocks.push(`## System\n${t}`);
    } else if (m.role === 'assistant') {
      const t = messageText(m.content);
      const parts: string[] = [];
      if (t) parts.push(t);
      for (const tc of m.tool_calls ?? []) {
        parts.push(`→ called tool \`${tc.function.name}\`(${tc.function.arguments || '{}'})`);
      }
      if (parts.length > 0) blocks.push(`## Assistant\n${parts.join('\n')}`);
    } else if (m.role === 'tool') {
      const label = m.isError ? 'Tool result (error)' : 'Tool result';
      blocks.push(`## ${label}\n${capResult(m.content ?? '')}`);
    }
  }
  const digest = blocks.join('\n\n');
  if (digest.length <= MAX_DIGEST_CHARS) return digest;
  const head = Math.floor(MAX_DIGEST_CHARS * 0.3);
  const tail = MAX_DIGEST_CHARS - head;
  return `${digest.slice(0, head)}\n\n[... middle of conversation elided to fit the reviewer's context ...]\n\n${digest.slice(-tail)}`;
}

/** Instructions surfaced to the main model about WHEN/HOW to call the advisor. */
export const ADVISOR_TOOL_INSTRUCTIONS = `You have access to an \`advisor\` tool backed by a stronger reviewer model. Your entire conversation history is automatically forwarded — the advisor sees the task, every tool call you've made, and every result you've seen. The optional \`focus\` argument lets you steer the review or ask a specific question.

Call advisor BEFORE substantive work — before writing code, before committing to an interpretation, before building on an assumption. Orientation (finding files, reading code) is not substantive work; writing, editing, and declaring an answer are.

Also call advisor:
- When you believe the task is complete. BEFORE the call, make your deliverable durable (write the file, stage the change) — the call takes time and a durable result persists if the session ends.
- When stuck — errors recurring, approach not converging, results that don't fit.
- When considering a change of approach.

On tasks longer than a few steps, call advisor at least once before committing to an approach and once before declaring done. On short reactive tasks, the advisor adds most of its value on the first call.

Give the advice serious weight, but adapt if a step fails empirically or you have primary-source evidence that contradicts a specific claim. If your retrieved data points one way and the advisor points another, don't silently switch — surface the conflict in one more advisor call (set \`focus\` to "I found X, you suggest Y, which constraint breaks the tie?").`;

export const advisorTool: ToolDefinition = {
  name: 'advisor',
  description:
    'Get a second opinion from a stronger reviewer model on your current approach. Your entire conversation (task, every tool call, every result) is auto-forwarded; the reviewer returns advice and you stay in control. Call it before committing to an approach, when stuck, when changing direction, or before declaring done.',
  prompt: ADVISOR_TOOL_INSTRUCTIONS,
  inputSchema: {
    type: 'object',
    properties: {
      focus: {
        type: 'string',
        description:
          'Optional: steer the review or ask the reviewer a specific question (e.g. a reconcile question when your evidence conflicts with earlier advice). Omit to request a general review.',
      },
    },
    required: [],
  },
  isReadOnly: true,
  isConcurrencySafe: true,
  // The reviewer call reaches an external, non-deterministic model API.
  isOpenWorld: true,
  searchHint: 'second opinion reviewer stronger model mentor critique sanity check review',
  isEnabled: () => !isAdvisorDisabled(),
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    if (isAdvisorDisabled()) {
      return { output: `Error: the advisor tool is disabled (${ADVISOR_DISABLE_ENV}).`, isError: true };
    }

    const providerConfig = context._providerConfig;
    if (!providerConfig) {
      return { output: 'Error: provider configuration not available for the advisor tool.', isError: true };
    }

    const parent = context._parentMessages ?? [];
    const digest = buildTranscriptDigest(parent);
    if (!digest) {
      return { output: 'Error: no conversation history available to review.', isError: true };
    }

    const focus = typeof input.focus === 'string' ? input.focus.trim() : '';
    const closing = focus
      ? `The agent specifically asks: ${focus}\n\nGive your review now.`
      : 'Give your review now.';
    const reviewMessages: Message[] = [
      { role: 'user', content: `${ADVISOR_REVIEWER_PREAMBLE}\n\n--- CONVERSATION ---\n${digest}\n--- END CONVERSATION ---\n\n${closing}` },
    ];

    const reviewerModel = resolveAdvisorModel(providerConfig);
    const reviewerConfig: ProviderConfig = { ...providerConfig, model: reviewerModel };

    // Test seam: an injected stream impl (never set in production — mirrors the
    // _webFetchHttpGet / _webSearchHooks test-only seams typed `unknown`) lets a
    // unit test drive the reviewer stream deterministically; production falls
    // through to the real streaming client.
    const streamImpl = (context._advisorStreamImpl as typeof streamChatCompletion | undefined) ?? streamChatCompletion;

    const { raw, failure } = await runAuxQuery(
      reviewerConfig,
      reviewMessages,
      streamImpl,
      context.abortSignal,
      ADVISOR_TIMEOUT_MS,
    );

    if (failure) {
      const detail = failure.error ? `: ${failure.error}` : '';
      return { output: `Error: advisor review failed (${failure.reason}${detail}).`, isError: true };
    }

    const advice = raw.trim();
    if (!advice) {
      return { output: 'Error: the advisor returned no advice.', isError: true };
    }

    return {
      output: advice,
      metadata: { advisorModel: reviewerModel, advisorForwardedChars: digest.length },
    };
  },
};
