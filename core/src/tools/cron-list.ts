import { ToolDefinition, ToolResult } from '../types';
import { getScheduler, localTimeZoneLabel, MAX_AGE_DAYS } from '../cron';
import type { CronJob } from '../cron';
import { isCronEnabled } from './tool-gates';
import { graphemes } from './text-truncate';

/** Single-line, ellipsis-marked, grapheme-safe truncation of a prompt for list
 *  display. Collapses a multi-line prompt to its first line (appending an ellipsis
 *  marker so the collapse is visible) and caps the width at `max` grapheme clusters,
 *  replacing the tail with a single ellipsis. Counting by grapheme — not code unit —
 *  keeps emoji/CJK/combining-mark prompts from being cut mid-character. */
function truncatePrompt(prompt: string, max = 100): string {
  const firstLine = prompt.split('\n')[0];
  const collapsed = firstLine.length < prompt.length;
  const cells = graphemes(firstLine);
  if (cells.length > max) {
    return `${cells.slice(0, max - 1).join('')}…`;
  }
  return collapsed ? `${firstLine} …` : firstLine;
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

/** Human-readable one-line flag summary for a job (recurring/one-shot, durable/
 *  session-only, permanent, UTC, expiry window, inactive). Shared by cron_list and
 *  cron_get so the two surfaces stay consistent (one canonical formatter). */
export function formatJobFlags(job: CronJob): string {
  const active = isActiveJob(job);
  // Surface the EFFECTIVE expiry window so a user can see at a glance when a
  // recurring job will auto-expire (and therefore stop firing) — the custom
  // maxAgeDays when set, otherwise the MAX_AGE_DAYS default; "never-expires" when
  // maxAgeDays:0 was set without the permanent flag. Permanent jobs carry the
  // 'permanent' flag instead, so expiry is omitted for them. Mirrors the reference's
  // guidance to make the N-day auto-expiry visible (src/tools/ScheduleCronTool/prompt.ts).
  const ageFlag = job.recurring && !job.permanent
    ? [(() => {
        const days = typeof job.maxAgeDays === 'number' ? job.maxAgeDays : MAX_AGE_DAYS;
        return days === 0 ? 'never-expires' : `expires in ${days}d`;
      })()]
    : [];
  return [
    job.recurring ? 'recurring' : 'one-shot',
    job.durable ? 'durable' : 'session-only',
    ...(job.permanent ? ['permanent'] : []),
    ...(job.utc ? ['UTC'] : []),
    ...ageFlag,
    ...(active ? [] : ['inactive']),
  ].join(', ');
}

export const cronListTool: ToolDefinition = {
  name: 'cron_list',
  // PARITY (additive): declarative per-tool gate for the cron subsystem.
  isEnabled: () => isCronEnabled(),
  description:
    'List scheduled cron jobs (durable and session-only). A teammate sees only its own jobs; the lead sees all. ' +
    'Jobs are ordered by soonest next fire; an optional "status" filter (all|active|inactive) narrows the list.',
  // PARITY (additive): a rich tool prompt (appended to the model-facing
  // description, like the reference's buildCronListPrompt) that names the durable
  // persistence path so the model can tell persisted jobs from session-only ones.
  prompt:
    'Jobs come from two stores: the durable store (scheduled_tasks.json under the ' +
    'project state dir, persisted across restarts) when durable persistence is enabled, ' +
    'and the in-memory session store (lost on exit). Fire times are shown in the host ' +
    'local timezone.',
  // PARITY (additive): bound the serialized result so a very large job list cannot
  // blow the context window — the engine (tool-executor spill) honors this cap,
  // mirroring the reference's `maxResultSizeChars: 100_000` on CronListTool.
  maxResultSizeChars: 100_000,
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
    // Label displayed instants with the host timezone so a next/last fire time
    // cannot be mistaken for UTC (parity: reference surfaces the resolved tz).
    const tz = localTimeZoneLabel();
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
      // A UTC-defined job renders its schedule in the host's local time with a TZ
      // label so a server/remote schedule reads as "when it fires here".
      const schedule = sched.formatSchedule(j.cron, { utc: j.utc });
      const active = isActiveJob(j);
      const next = active ? `${new Date(j.nextRun as number).toLocaleString()} (local, ${tz})` : 'N/A';
      const flags = formatJobFlags(j);
      // Surface the last fire so a user can tell a job that has run from one that
      // never has; omitted entirely for jobs that have not yet fired.
      const lastLine = typeof j.lastRun === 'number'
        ? `\n    Last: ${new Date(j.lastRun).toLocaleString()} (local, ${tz})`
        : '';
      // Surface the human-readable name (when set) alongside the id so a job is
      // identifiable by purpose at a glance, not only by its opaque id.
      const label = j.name ? ` "${j.name}"` : '';
      return `  ${j.id}${label}: ${schedule} (${flags})\n    Next: ${next}${lastLine}\n    Prompt: ${truncatePrompt(j.prompt)}`;
    });

    return { output: `${jobs.length} scheduled job(s):\n${lines.join('\n')}` };
  },
};
