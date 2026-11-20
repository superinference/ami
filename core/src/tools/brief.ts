import { ToolDefinition, ToolResult, ToolContext } from '../types';

const INTENTS = ['normal', 'proactive'] as const;
type Intent = (typeof INTENTS)[number];

/**
 * PARITY (validated brief config with safe-parse fallback) — the reference's BriefTool
 * is backed by a schema-validated config knob (`briefConfigSchema.safeParse` →
 * `DEFAULT_BRIEF_CONFIG`) so a malformed remote config can never PARTIALLY
 * enable/corrupt the feature: any invalid field drops the WHOLE object back to
 * the hardcoded defaults. AMI's brief tool mirrors that guarantee with an
 * operator env override (`AMI_BRIEF_CONFIG`, a JSON object) parsed through
 * {@link resolveBriefConfig} with strict per-field validation. The config tunes
 * the model/caller-controlled topic cap and the default topic — both previously
 * hardcoded — with NO route to a zero/NaN/negative cap or an empty default.
 */
interface BriefConfig {
  /** Max length of a caller-supplied topic before it is truncated (so an
   *  unbounded value cannot bloat the prompt). Positive integer. */
  maxTopicLen: number;
  /** Fallback topic used when none is supplied (or an invalid/empty one is). */
  defaultTopic: string;
}

const DEFAULT_BRIEF_CONFIG: BriefConfig = {
  maxTopicLen: 300,
  defaultTopic: 'current conversation',
};

/** Operator override: a JSON object `{ maxTopicLen?, defaultTopic? }`. A
 *  malformed/absent value falls through to {@link DEFAULT_BRIEF_CONFIG}. */
const BRIEF_CONFIG_ENV = 'AMI_BRIEF_CONFIG';

/** A usable topic cap: a positive, finite integer. Rejects 0, negatives, NaN,
 *  Infinity, and non-numbers so there is no route to a zero/NaN/Infinity cap. */
function isValidTopicLen(v: unknown): v is number {
  return typeof v === 'number' && Number.isInteger(v) && v > 0;
}

/**
 * Parse a raw (untrusted) brief-config value with STRICT per-field validation,
 * falling back ENTIRELY to {@link DEFAULT_BRIEF_CONFIG} on ANY malformed input
 * so a bad config can never partially enable/corrupt the tool. Mirrors the reference's
 * `briefConfigSchema().safeParse(raw)` → default-on-failure semantics.
 */
function parseBriefConfig(raw: unknown): BriefConfig {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) return DEFAULT_BRIEF_CONFIG;
  const o = raw as Record<string, unknown>;
  if (!isValidTopicLen(o.maxTopicLen)) return DEFAULT_BRIEF_CONFIG;
  if (typeof o.defaultTopic !== 'string' || o.defaultTopic.length === 0) return DEFAULT_BRIEF_CONFIG;
  return { maxTopicLen: o.maxTopicLen, defaultTopic: o.defaultTopic };
}

/**
 * Resolve the active brief config from the operator env override, safe-parsing
 * the JSON: an absent value, non-JSON string, or any field that fails
 * validation falls through ENTIRELY to the hardcoded defaults.
 */
function resolveBriefConfig(): BriefConfig {
  const raw = process.env[BRIEF_CONFIG_ENV];
  if (!raw) return DEFAULT_BRIEF_CONFIG;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return DEFAULT_BRIEF_CONFIG;
  }
  return parseBriefConfig(parsed);
}

export const briefTool: ToolDefinition = {
  name: 'brief',
  description:
    'Provide a one-sentence summary of the current conversation or a specific topic. Use for quick status checks. ' +
    "Set status to 'proactive' when surfacing something the user has not asked for (a blocker, a completion while they are away), " +
    "or 'normal' when replying to something they just said.",
  inputSchema: {
    type: 'object',
    properties: {
      topic: { type: 'string', description: 'Optional topic to summarize. If omitted, summarizes the conversation.' },
      status: {
        type: 'string',
        enum: [...INTENTS],
        description: "Intent label: 'normal' (reply) or 'proactive' (unsolicited surfacing). Default: normal.",
      },
    },
  },
  isReadOnly: true,
  async execute(input: Record<string, unknown>, _context: ToolContext): Promise<ToolResult> {
    // Strict input: reject unknown keys so a typo'd parameter is surfaced, not ignored.
    const allowed = new Set(['topic', 'status']);
    const unknown = Object.keys(input).filter((k) => !allowed.has(k));
    if (unknown.length > 0) {
      return { output: `Error: unknown parameter(s): ${unknown.join(', ')}. Allowed: topic, status.`, isError: true };
    }

    const rawStatus = typeof input.status === 'string' ? input.status : undefined;
    if (rawStatus !== undefined && !(INTENTS as readonly string[]).includes(rawStatus)) {
      return { output: `Error: invalid status "${rawStatus}". Options: ${INTENTS.join(', ')}.`, isError: true };
    }
    const status: Intent = (rawStatus as Intent) ?? 'normal';

    const config = resolveBriefConfig();

    // Type-guard (not just `as string`): a truthy non-string must not be coerced into the
    // prompt; fall back to the default and bound the length of a valid topic.
    const topic =
      typeof input.topic === 'string' && input.topic
        ? input.topic.slice(0, config.maxTopicLen)
        : config.defaultTopic;
    const lead =
      status === 'proactive'
        ? `Proactively surface the key point about "${topic}" the user needs to know now, in exactly one sentence.`
        : `Summarize "${topic}" in exactly one sentence.`;
    // Surface the normal-vs-proactive intent as a STRUCTURED discriminator on the
    // result (not just baked into the prose), so a frontend can route a 'proactive'
    // brief to a notification / unsolicited-surface affordance the way the reference's
    // BriefTool drives `logEvent('tengu_brief_send', { proactive })`. Parity: the
    // status enum is a machine-readable signal, carried on ToolResult.metadata.
    return {
      output: `${lead} Be concise and factual.`,
      metadata: { status, proactive: status === 'proactive' },
    };
  },
};
