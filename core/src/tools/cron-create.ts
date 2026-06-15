import { ToolDefinition, ToolResult } from '../types';
import { getScheduler, resetScheduler, CronScheduler } from '../cron';

/** @deprecated Use getScheduler from '../cron' directly */
export function getCronScheduler(cwd: string): CronScheduler {
  return getScheduler(cwd);
}

export function resetCronScheduler(): void {
  resetScheduler();
}

export const cronCreateTool: ToolDefinition = {
  name: 'cron_create',
  description:
    'Schedule a prompt to run at a future time or on a recurring schedule. ' +
    'Uses standard 5-field cron: minute hour day-of-month month day-of-week. ' +
    'Recurring jobs auto-expire after 7 days.',
  inputSchema: {
    type: 'object',
    properties: {
      cron: {
        type: 'string',
        description: 'Standard 5-field cron expression (e.g. "*/5 * * * *" for every 5 minutes).',
      },
      prompt: {
        type: 'string',
        description: 'The prompt to enqueue at each fire time.',
      },
      recurring: {
        type: 'boolean',
        description: 'true = fire on every cron match (default). false = fire once then auto-delete.',
        default: true,
      },
      durable: {
        type: 'boolean',
        description: 'true = persist to disk and survive session restarts. false = session-only (default).',
        default: false,
      },
    },
    required: ['cron', 'prompt'],
  },
  isReadOnly: false,

  async execute(input, context): Promise<ToolResult> {
    const cron = input.cron as string;
    const prompt = input.prompt as string;
    const recurring = (input.recurring as boolean) ?? true;
    const durable = (input.durable as boolean) ?? false;

    if (!cron || cron.trim().split(/\s+/).length !== 5) {
      return { output: 'Error: cron must be a valid 5-field cron expression.', isError: true };
    }
    if (!prompt || !prompt.trim()) {
      return { output: 'Error: prompt must not be empty.', isError: true };
    }

    const sched = getScheduler(context.cwd);
    const job = sched.create({ cron, prompt, recurring, durable });
    const schedule = sched.formatSchedule(cron);
    const nextRun = job.nextRun ? new Date(job.nextRun).toLocaleTimeString() : 'unknown';

    return {
      output: `Scheduled job ${job.id}\n  Schedule: ${schedule}\n  Next run: ${nextRun}\n  Recurring: ${recurring}\n  Durable: ${durable}`,
    };
  },
};
