import { ToolDefinition, ToolResult } from '../types';
import { getScheduler } from '../cron';
import type { CronJob } from '../cron';
import { isCronEnabled } from './tool-gates';

/** Single-line, ellipsis-marked truncation of a prompt for list display. */
function truncatePrompt(prompt: string, max = 100): string {
  const firstLine = prompt.split('\n')[0];
  const single = firstLine.length < prompt.length ? `${firstLine} …` : firstLine;
  return single.length > max ? `${single.slice(0, max - 1)}…` : single;
}

/** Optional status filter accepted by cron_list. */
export type CronListStatus = 'all' | 'active' | 'inactive';

export const CRON_LIST_STATUSES: readonly CronListStatus[] = ['all', 'active', 'inactive'];

/** A job is "active" when it still has an upcoming fire time (nextRun). A job
 *  goes "inactive" once it has no future match: a one-shot that already fired,
 *  a recurring job whose cron no longer resolves, or a job awaiting cleanup.
 *  Surfacing this lets a user tell a live reminder from a dead one at a glance. */
export function isActiveJob(job: CronJob): boolean {
  return typeof job.nextRun === 'number';
}

/** Deterministic display order (parity): soonest upcoming fire first so the next
 *  thing to run is at the top; inactive jobs (no nextRun) sink to the bottom;
 *  ties broken by id for stable, reproducible output. Map insertion order is
 *  NOT meaningful to a user, so we never rely on it. */
export function sortJobsForDisplay(jobs: CronJob[]): CronJob[] {
  return [...jobs].sort((a, b) => {
    const aActive = isActiveJob(a);
    const bActive = isActiveJob(b);
    if (aActive !== bActive) return aActive ? -1 : 1;
    if (aActive && bActive && a.nextRun !== b.nextRun) {
      return (a.nextRun as number) - (b.nextRun as number);
    }
    return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
  });
}

export const cronListTool: ToolDefinition = {
  name: 'cron_list',
  // PARITY (additive): declarative per-tool gate for the cron subsystem.
  isEnabled: () => isCronEnabled(),
  description:
    'List scheduled cron jobs (durable and session-only). A teammate sees only its own jobs; the lead sees all. ' +
    'Jobs are ordered by soonest next fire; an optional "status" filter (all|active|inactive) narrows the list.',
  inputSchema: {
    type: 'object',
    properties: {
      status: {
        type: 'string',
        enum: ['all', 'active', 'inactive'],
        description:
          'Filter by lifecycle status: "active" (has an upcoming fire), "inactive" (no future fire), or "all" (default).',
      },
    },
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(input, context): Promise<ToolResult> {
    // Strict-but-additive input: only the optional "status" filter is accepted.
    const keys = Object.keys(input);
    const unknown = keys.filter(k => k !== 'status');
    if (unknown.length > 0) {
      return {
        output: `Error: cron_list takes no parameters other than "status" (all|active|inactive); got unknown: ${unknown.join(', ')}.`,
        isError: true,
      };
    }

    let status: CronListStatus = 'all';
    if (input.status !== undefined) {
      if (!CRON_LIST_STATUSES.includes(input.status as CronListStatus)) {
        return {
          output: `Error: cron_list "status" must be one of: ${CRON_LIST_STATUSES.join(', ')} (got: ${String(input.status)}).`,
          isError: true,
        };
      }
      status = input.status as CronListStatus;
    }

    const sched = getScheduler(context.cwd);
    // Teammate-scoped visibility: a sub-agent sees only global + its own jobs.
    let jobs = sched.list(context.agentId);
    if (status === 'active') jobs = jobs.filter(isActiveJob);
    else if (status === 'inactive') jobs = jobs.filter(j => !isActiveJob(j));

    if (jobs.length === 0) {
      // Reflect the filter back in the empty-state so the result is unambiguous.
      const scope = status === 'all' ? '' : `${status} `;
      return { output: `No ${scope}scheduled jobs.` };
    }

    const lines = sortJobsForDisplay(jobs).map(j => {
      const schedule = sched.formatSchedule(j.cron);
      const active = isActiveJob(j);
      const next = active ? new Date(j.nextRun as number).toLocaleString() : 'N/A';
      const flags = [
        j.recurring ? 'recurring' : 'one-shot',
        j.durable ? 'durable' : 'session-only',
        ...(j.permanent ? ['permanent'] : []),
        ...(active ? [] : ['inactive']),
      ].join(', ');
      // Surface the last fire so a user can tell a job that has run from one that
      // never has; omitted entirely for jobs that have not yet fired.
      const lastLine = typeof j.lastRun === 'number'
        ? `\n    Last: ${new Date(j.lastRun).toLocaleString()} (local)`
        : '';
      return `  ${j.id}: ${schedule} (${flags})\n    Next: ${next} (local)${lastLine}\n    Prompt: ${truncatePrompt(j.prompt)}`;
    });

    return { output: `${jobs.length} scheduled job(s):\n${lines.join('\n')}` };
  },
};
