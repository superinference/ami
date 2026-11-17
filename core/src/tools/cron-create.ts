import { ToolDefinition, ToolResult } from '../types';
import { getScheduler, resetScheduler, CronScheduler, validateCronExpression, MAX_AGE_DAYS, type CronJob } from '../cron';
import { coerceBool } from '../utils/coerce';
import { isCronEnabled } from './tool-gates';

/** @deprecated Use getScheduler from '../cron' directly */
export function getCronScheduler(cwd: string): CronScheduler {
  return getScheduler(cwd);
}

export function resetCronScheduler(): void {
  resetScheduler();
}

const ALLOWED_KEYS = new Set(['cron', 'prompt', 'recurring', 'durable', 'permanent', 'maxAgeDays', 'utc']);

// PARITY (additive): upper bound on a scheduled prompt's length. A prompt far
// larger than this is almost always an accident (e.g. a whole file pasted in)
// and would bloat the persisted job store, so it is refused as clean validation
// instead of silently writing a multi-megabyte entry to scheduled_tasks.json.
export const MAX_CRON_PROMPT_LEN = 10_000;

/**
 * PARITY (additive): autonomous-loop (fork-bomb) sentinel. A scheduled prompt
 * that itself re-invokes the cron scheduler compounds without bound — every fire
 * schedules one (or more) further jobs. Detect the literal `cron_create` tool
 * token in the prompt so such self-replicating schedules can be refused up front.
 * Matches `cron_create`, `cron-create`, and `cron create` (case-insensitive).
 */
export function isSelfSchedulingPrompt(prompt: string): boolean {
  return /\bcron[_\s-]?create\b/i.test(prompt);
}

/**
 * PARITY (additive): duplicate-job detection. Return an existing job with an
 * identical schedule + prompt + recurrence (within the caller's agent scope), or
 * undefined. Prevents silently stacking an unbounded pile of identical reminders
 * when the model re-issues the same request.
 */
export function findDuplicateCronJob(
  jobs: CronJob[],
  cron: string,
  prompt: string,
  recurring: boolean,
): CronJob | undefined {
  return jobs.find(j => j.cron === cron && j.prompt === prompt && j.recurring === recurring);
}

export const cronCreateTool: ToolDefinition = {
  name: 'cron_create',
  // PARITY (additive): declarative per-tool gate for the cron subsystem.
  isEnabled: () => isCronEnabled(),
  description:
    'Schedule a prompt to run at a future time or on a recurring schedule. ' +
    'Uses standard 5-field cron in LOCAL time: minute hour day-of-month month day-of-week. ' +
    'Examples: "*/5 * * * *" every 5 minutes; "0 9 * * 1-5" at 9:00am on weekdays; ' +
    'for a one-shot, pin the date fields and pass recurring:false (e.g. "30 14 25 12 *" = 2:30pm on Dec 25). ' +
    'Prefer OFF-MINUTE times (avoid :00 and :30) to dodge the thundering-herd of everyone\'s round-number schedules. ' +
    `Recurring jobs auto-expire after ${MAX_AGE_DAYS} days unless permanent:true (or a custom maxAgeDays, 0=never) — tell the user about this limit. ` +
    'Durability: use durable:true ONLY when the user explicitly asks for a schedule that survives a restart ' +
    '(it is persisted to disk). Most "remind me in 5 minutes" / "check back in an hour" requests should stay ' +
    'session-only (the default) — it is cheaper and avoids leaving stray jobs on disk. Cancel a job with cron_delete.',
  inputSchema: {
    type: 'object',
    properties: {
      cron: {
        type: 'string',
        description:
          'Standard 5-field cron expression in local time: minute hour day-of-month month day-of-week ' +
          '(e.g. "*/5 * * * *" = every 5 minutes; "30 14 28 2 *" = Feb 28 at 2:30pm local, once).',
      },
      prompt: {
        type: 'string',
        description: 'The prompt to enqueue at each fire time.',
      },
      recurring: {
        type: 'boolean',
        description:
          'true = fire on every cron match (default). false = fire once then auto-delete. ' +
          'Use false for "remind me at X" one-shot requests with pinned minute/hour/day-of-month/month.',
        default: true,
      },
      durable: {
        type: 'boolean',
        description:
          'true = persist to disk and survive session restarts. false = session-only (default). ' +
          'Use true only when the user asks the task to survive across sessions.',
        default: false,
      },
      permanent: {
        type: 'boolean',
        description: `true = exempt this recurring job from the ${MAX_AGE_DAYS}-day auto-expiry. Default false.`,
        default: false,
      },
      maxAgeDays: {
        type: 'number',
        description:
          `Custom auto-expiry window (whole days) for a recurring job, overriding the ${MAX_AGE_DAYS}-day default. ` +
          'Use 0 for "never expire" (equivalent to permanent:true). Ignored for one-shot jobs.',
      },
      utc: {
        type: 'boolean',
        description:
          'true = interpret the cron expression in UTC instead of local time, so the job fires on the same ' +
          'absolute instant regardless of the host timezone (use for server/remote-defined schedules). ' +
          'Default false (local time). The list view renders a UTC job in your local time with a TZ label.',
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
    const utc = coerceBool(input.utc);
    // Optional per-job expiry window. Validate here for a clean tool-level error;
    // create() re-validates as the engine-boundary guarantee. 0 = never expire.
    let maxAgeDays: number | undefined;
    if (input.maxAgeDays !== undefined) {
      const n = Number(input.maxAgeDays);
      if (!Number.isInteger(n) || n < 0) {
        return { output: `Error: maxAgeDays must be a non-negative integer number of days (got ${String(input.maxAgeDays)}); use 0 for never-expire.`, isError: true };
      }
      maxAgeDays = n;
    }

    // Full field-level validation (not just field count) with a clear error.
    if (typeof cron !== 'string' || !cron.trim()) {
      return { output: 'Error: cron must be a 5-field cron expression: minute hour day-of-month month day-of-week.', isError: true };
    }
    const fieldError = validateCronExpression(cron);
    if (fieldError) {
      // Echo the offending expression so the model can self-correct the exact
      // token that failed, matching the reference's actionable error text.
      return { output: `Error: invalid cron expression "${cron}" (${fieldError}). Expected 5 fields: minute hour day-of-month month day-of-week.`, isError: true };
    }
    if (!prompt || !prompt.trim()) {
      return { output: 'Error: prompt must not be empty.', isError: true };
    }
    // PARITY (additive): refuse an oversized prompt as clean validation.
    if (prompt.length > MAX_CRON_PROMPT_LEN) {
      return {
        output: `Error: prompt is too long (${prompt.length} chars; max ${MAX_CRON_PROMPT_LEN}). Schedule a short instruction, not a full document.`,
        isError: true,
      };
    }
    // PARITY (additive): refuse a self-replicating schedule (autonomous-loop
    // sentinel) — a scheduled prompt that re-invokes cron_create fires forever.
    if (isSelfSchedulingPrompt(prompt)) {
      return {
        output: 'Error: prompt would schedule more cron jobs (references cron_create), creating a self-replicating loop. Describe the task itself, not a request to schedule it again.',
        isError: true,
      };
    }

    const sched = getScheduler(context.cwd);

    // Satisfiability check: a structurally valid cron that never matches (e.g.
    // Feb 30) is rejected with a distinct, clean message rather than silently
    // scheduling a job that can never fire.
    if (sched.computeNextRun(cron, undefined, utc) === null) {
      return { output: `Error: cron "${cron}" never matches a real date (e.g. an impossible calendar date).`, isError: true };
    }

    // PARITY (additive): duplicate-job detection — point the caller at the
    // existing identical job instead of stacking another copy of it.
    const dup = findDuplicateCronJob(sched.list(context.agentId), cron, prompt, recurring);
    if (dup) {
      return {
        output: `Error: an identical ${recurring ? 'recurring' : 'one-shot'} job already exists (${dup.id}) with the same schedule and prompt. Delete it first with cron_delete if you want to recreate it.`,
        isError: true,
      };
    }

    let job;
    try {
      job = sched.create({ cron, prompt, recurring, durable, permanent, maxAgeDays, utc, agentId: context.agentId });
    } catch (err) {
      // MAX_JOBS cap and the minimum-interval floor surface as clean validation.
      return { output: `Error: ${err instanceof Error ? err.message : String(err)}`, isError: true };
    }
    const schedule = sched.formatSchedule(cron, { utc });
    const nextRun = job.nextRun ? new Date(job.nextRun).toLocaleString() : 'unknown';
    // Reflect the EFFECTIVE expiry window: permanent / maxAgeDays:0 never expire;
    // a custom maxAgeDays overrides the MAX_AGE_DAYS default. Interpolated from the
    // single source constant so the message can never drift from expireOld().
    const neverExpires = permanent || maxAgeDays === 0;
    const effectiveAgeDays = maxAgeDays ?? MAX_AGE_DAYS;
    const expiry = !recurring
      ? '\n  Fires once then auto-deletes.'
      : neverExpires
        ? '\n  Never auto-expires.'
        : `\n  Auto-expires in ${effectiveAgeDays} day${effectiveAgeDays === 1 ? '' : 's'} (pass permanent:true or maxAgeDays:0 to keep it).`;
    // Report the EFFECTIVE durability (job.durable), which the scheduler may have
    // degraded to session-only when the durable kill-switch is active. If the
    // model requested durable:true but it was gated off, say so explicitly.
    const effectiveDurable = job.durable;
    const degraded = durable && !effectiveDurable;
    const durabilityNote = effectiveDurable
      ? '\n  Persisted to disk (survives restarts).'
      : degraded
        ? '\n  Durable persistence is disabled by operator policy — this job is session-only (lost on restart).'
        : '\n  Session-only (lost on restart).';

    return {
      output:
        `Scheduled ${recurring ? 'recurring' : 'one-shot'} job ${job.id}\n` +
        `  Schedule: ${schedule} (${utc ? 'defined in UTC, shown in local time' : 'local time'})\n  Next run: ${nextRun}\n` +
        `  Recurring: ${recurring}\n  Durable: ${effectiveDurable}${expiry}${durabilityNote}\n` +
        '  Cancel with cron_delete.',
    };
  },
};
