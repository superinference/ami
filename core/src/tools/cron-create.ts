import { ToolDefinition, ToolResult } from '../types';
import { getScheduler, resetScheduler, CronScheduler, validateCronExpression } from '../cron';
import { coerceBool } from '../utils/coerce';

/** @deprecated Use getScheduler from '../cron' directly */
export function getCronScheduler(cwd: string): CronScheduler {
  return getScheduler(cwd);
}

export function resetCronScheduler(): void {
  resetScheduler();
}

const ALLOWED_KEYS = new Set(['cron', 'prompt', 'recurring', 'durable', 'permanent']);

export const cronCreateTool: ToolDefinition = {
  name: 'cron_create',
  description:
    'Schedule a prompt to run at a future time or on a recurring schedule. ' +
    'Uses standard 5-field cron in LOCAL time: minute hour day-of-month month day-of-week. ' +
    'Examples: "*/5 * * * *" every 5 minutes; "0 9 * * 1-5" at 9:00am on weekdays; ' +
    'for a one-shot, pin the date fields and pass recurring:false (e.g. "30 14 25 12 *" = 2:30pm on Dec 25). ' +
    'Prefer OFF-MINUTE times (avoid :00 and :30) to dodge the thundering-herd of everyone\'s round-number schedules. ' +
    'Recurring jobs auto-expire after 7 days unless permanent:true — tell the user about this limit. ' +
    'Pass durable:true only when the schedule must survive a restart (it is persisted to disk); ' +
    'session-only (default) is cheaper. Cancel a job with cron_delete.',
  inputSchema: {
    type: 'object',
    properties: {
      cron: {
        type: 'string',
        description: 'Standard 5-field cron expression in local time (e.g. "*/5 * * * *" for every 5 minutes).',
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
      permanent: {
        type: 'boolean',
        description: 'true = exempt this recurring job from the 7-day auto-expiry. Default false.',
        default: false,
      },
    },
    required: ['cron', 'prompt'],
  },
  isReadOnly: false,

  async execute(input, context): Promise<ToolResult> {
    // Operator kill-switch: cron scheduling can be disabled entirely.
    if (CronScheduler.isDisabled()) {
      return { output: 'Error: cron scheduling is disabled by operator policy.', isError: true };
    }
    // Strict input: reject unknown keys so a typo'd parameter is surfaced.
    const unknown = Object.keys(input).filter(k => !ALLOWED_KEYS.has(k));
    if (unknown.length > 0) {
      return { output: `Error: unknown parameter(s): ${unknown.join(', ')}.`, isError: true };
    }

    const cron = input.cron as string;
    const prompt = input.prompt as string;
    const recurring = input.recurring === undefined ? true : coerceBool(input.recurring);
    const durable = coerceBool(input.durable);
    const permanent = coerceBool(input.permanent);

    // Full field-level validation (not just field count) with a clear error.
    if (typeof cron !== 'string' || !cron.trim()) {
      return { output: 'Error: cron must be a 5-field cron expression: minute hour day-of-month month day-of-week.', isError: true };
    }
    const fieldError = validateCronExpression(cron);
    if (fieldError) {
      return { output: `Error: invalid 5-field cron expression (${fieldError}). Layout: minute hour day-of-month month day-of-week.`, isError: true };
    }
    if (!prompt || !prompt.trim()) {
      return { output: 'Error: prompt must not be empty.', isError: true };
    }

    const sched = getScheduler(context.cwd);

    // Satisfiability check: a structurally valid cron that never matches (e.g.
    // Feb 30) is rejected with a distinct, clean message rather than silently
    // scheduling a job that can never fire.
    if (sched.computeNextRun(cron) === null) {
      return { output: `Error: cron "${cron}" never matches a real date (e.g. an impossible calendar date).`, isError: true };
    }

    let job;
    try {
      job = sched.create({ cron, prompt, recurring, durable, permanent, agentId: context.agentId });
    } catch (err) {
      // MAX_JOBS cap and the minimum-interval floor surface as clean validation.
      return { output: `Error: ${err instanceof Error ? err.message : String(err)}`, isError: true };
    }
    const schedule = sched.formatSchedule(cron);
    const nextRun = job.nextRun ? new Date(job.nextRun).toLocaleString() : 'unknown';
    const expiry = recurring && !permanent ? '\n  Auto-expires in 7 days (pass permanent:true to keep it).' : '';
    const durabilityNote = durable ? '\n  Persisted to disk (survives restarts).' : '\n  Session-only (lost on restart).';

    return {
      output:
        `Scheduled ${recurring ? 'recurring' : 'one-shot'} job ${job.id}\n` +
        `  Schedule: ${schedule} (local time)\n  Next run: ${nextRun}\n` +
        `  Recurring: ${recurring}\n  Durable: ${durable}${expiry}${durabilityNote}\n` +
        '  Cancel with cron_delete.',
    };
  },
};
