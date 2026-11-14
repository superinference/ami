import { ToolDefinition, ToolResult } from '../types';
import { getScheduler } from '../cron';

/** Single-line, ellipsis-marked truncation of a prompt for list display. */
function truncatePrompt(prompt: string, max = 100): string {
  const firstLine = prompt.split('\n')[0];
  const single = firstLine.length < prompt.length ? `${firstLine} …` : firstLine;
  return single.length > max ? `${single.slice(0, max - 1)}…` : single;
}

export const cronListTool: ToolDefinition = {
  name: 'cron_list',
  description: 'List scheduled cron jobs (durable and session-only). A teammate sees only its own jobs; the lead sees all.',
  inputSchema: {
    type: 'object',
    properties: {},
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(input, context): Promise<ToolResult> {
    // Strict input: this tool takes no arguments.
    const keys = Object.keys(input);
    if (keys.length > 0) {
      return { output: `Error: cron_list takes no parameters (got: ${keys.join(', ')}).`, isError: true };
    }

    const sched = getScheduler(context.cwd);
    // Teammate-scoped visibility: a sub-agent sees only global + its own jobs.
    const jobs = sched.list(context.agentId);

    if (jobs.length === 0) {
      return { output: 'No scheduled jobs.' };
    }

    const lines = jobs.map(j => {
      const schedule = sched.formatSchedule(j.cron);
      const next = j.nextRun ? new Date(j.nextRun).toLocaleString() : 'N/A';
      const flags = [
        j.recurring ? 'recurring' : 'one-shot',
        j.durable ? 'durable' : 'session-only',
        ...(j.permanent ? ['permanent'] : []),
      ].join(', ');
      return `  ${j.id}: ${schedule} (${flags})\n    Next: ${next} (local)\n    Prompt: ${truncatePrompt(j.prompt)}`;
    });

    return { output: `${jobs.length} scheduled job(s):\n${lines.join('\n')}` };
  },
};
