import { ToolDefinition, ToolResult, ToolContext } from '../types';

const INTENTS = ['normal', 'proactive'] as const;
type Intent = (typeof INTENTS)[number];

// Cap the model/caller-controlled topic so an unbounded value cannot bloat the prompt.
const MAX_TOPIC_LEN = 300;

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

    // Type-guard (not just `as string`): a truthy non-string must not be coerced into the
    // prompt; fall back to the default and bound the length of a valid topic.
    const topic =
      typeof input.topic === 'string' && input.topic ? input.topic.slice(0, MAX_TOPIC_LEN) : 'current conversation';
    const lead =
      status === 'proactive'
        ? `Proactively surface the key point about "${topic}" the user needs to know now, in exactly one sentence.`
        : `Summarize "${topic}" in exactly one sentence.`;
    return { output: `${lead} Be concise and factual.` };
  },
};
