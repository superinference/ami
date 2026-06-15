import { ToolDefinition, ToolResult } from '../types';
import { getScheduler } from '../cron';

export const scheduleWakeupTool: ToolDefinition = {
  name: 'schedule_wakeup',
  description:
    'Schedule a one-shot wakeup after a delay. Used for /loop dynamic pacing. ' +
    'The prompt fires once after delaySeconds, then auto-deletes.',
  inputSchema: {
    type: 'object',
    properties: {
      delaySeconds: {
        type: 'number',
        description: 'Seconds from now to wake up. Clamped to [60, 3600].',
      },
      prompt: {
        type: 'string',
        description: 'The prompt to fire on wake-up.',
      },
      reason: {
        type: 'string',
        description: 'Short explanation of the chosen delay (shown to user).',
      },
    },
    required: ['delaySeconds', 'prompt', 'reason'],
  },
  isReadOnly: false,

  async execute(input, context): Promise<ToolResult> {
    const delaySeconds = Math.max(60, Math.min(3600, (input.delaySeconds as number) || 60));
    const prompt = input.prompt as string;
    const reason = input.reason as string;

    if (!prompt || !prompt.trim()) {
      return { output: 'Error: prompt must not be empty.', isError: true };
    }

    const now = new Date();
    const fireAt = new Date(now.getTime() + delaySeconds * 1000);
    const cronExpr = `${fireAt.getMinutes()} ${fireAt.getHours()} ${fireAt.getDate()} ${fireAt.getMonth() + 1} *`;

    const sched = getScheduler(context.cwd);
    const job = sched.create({ cron: cronExpr, prompt, recurring: false, durable: false });

    return {
      output: `Wakeup scheduled: ${job.id}\n  Delay: ${delaySeconds}s\n  Fire at: ${fireAt.toLocaleTimeString()}\n  Reason: ${reason}`,
    };
  },
};
