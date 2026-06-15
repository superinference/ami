import { ToolDefinition, ToolResult } from '../types';
import { getScheduler } from '../cron';

export const cronListTool: ToolDefinition = {
  name: 'cron_list',
  description: 'List all scheduled cron jobs (both durable and session-only).',
  inputSchema: {
    type: 'object',
    properties: {},
  },
  isReadOnly: true,

  async execute(_input, context): Promise<ToolResult> {
    const sched = getScheduler(context.cwd);
    const jobs = sched.list();

    if (jobs.length === 0) {
      return { output: 'No scheduled jobs.' };
    }

    const lines = jobs.map(j => {
      const schedule = sched.formatSchedule(j.cron);
      const next = j.nextRun ? new Date(j.nextRun).toLocaleString() : 'N/A';
      const flags = [
        j.recurring ? 'recurring' : 'one-shot',
        j.durable ? 'durable' : 'session-only',
      ].join(', ');
      return `  ${j.id}: ${schedule} (${flags})\n    Next: ${next}\n    Prompt: ${j.prompt.slice(0, 100)}`;
    });

    return { output: `${jobs.length} scheduled job(s):\n${lines.join('\n')}` };
  },
};
