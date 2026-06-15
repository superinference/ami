import { ToolDefinition, ToolResult, ToolContext } from '../types';

const MAX_SLEEP_MS = 600_000; // 10 minutes

export const sleepTool: ToolDefinition = {
  name: 'sleep',
  description:
    'Wait for a specified duration before continuing. Useful for polling external state ' +
    'or giving background processes time to complete. Maximum 600000ms (10 minutes).',
  inputSchema: {
    type: 'object',
    properties: {
      duration_ms: {
        type: 'number',
        description: 'Duration to sleep in milliseconds (1–600000).',
      },
      reason: {
        type: 'string',
        description: 'Short explanation of why the sleep is needed.',
      },
    },
    required: ['duration_ms'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const rawDuration = input.duration_ms as number;
    const reason = (input.reason as string) || '';

    if (typeof rawDuration !== 'number' || isNaN(rawDuration) || rawDuration <= 0) {
      return { output: 'Error: duration_ms must be a positive number.', isError: true };
    }

    const duration = Math.min(Math.round(rawDuration), MAX_SLEEP_MS);

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(resolve, duration);

      if (context.abortSignal) {
        if (context.abortSignal.aborted) {
          clearTimeout(timer);
          resolve();
          return;
        }
        context.abortSignal.addEventListener('abort', () => {
          clearTimeout(timer);
          resolve();
        }, { once: true });
      }
    });

    const sleptSec = (duration / 1000).toFixed(1);
    const msg = reason
      ? `Slept for ${sleptSec}s. Reason: ${reason}`
      : `Slept for ${sleptSec}s.`;

    return { output: msg };
  },
};
