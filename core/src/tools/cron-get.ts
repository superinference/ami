import { ToolDefinition, ToolResult } from '../types';
import { getScheduler } from '../cron';
import { isCronEnabled } from './tool-gates';
import { isActiveJob, formatJobFlags } from './cron-list';

const ID_RE = /^[\w-]+$/;
const ALLOWED_KEYS = new Set(['id', 'name']);

/**
 * cron_get — fetch ONE scheduled job's full, untruncated detail by id.
 *
 * PARITY (additive): the reference's RemoteTrigger exposes a `get` action
 * (GET /v1/code/triggers/{id}, prompt.ts:L10) to retrieve a single trigger's full
 * detail by id. AMI previously only had cron_list, whose per-job prompt is clipped
 * to ~100 chars — there was no id-parameterized way for the model to read a single
 * job's complete prompt/schedule. This tool wires the already-present
 * CronScheduler.get(id) through as a read-only tool returning the whole job,
 * including the untruncated prompt, plus a machine-readable outputSchema.
 */
export const cronGetTool: ToolDefinition = {
  name: 'cron_get',
  // PARITY (additive): declarative per-tool gate for the cron subsystem.
  isEnabled: () => isCronEnabled(),
  description:
    'Get the full, untruncated detail of ONE scheduled cron job by its ID (as shown by cron_list). ' +
    'Unlike cron_list, which clips each job\'s prompt for a compact overview, cron_get returns the ' +
    'complete stored prompt plus the schedule, next/last fire times and flags. ' +
    'You can only read a job you own; a teammate\'s job is reported as not found.',
  inputSchema: {
    type: 'object',
    properties: {
      id: {
        type: 'string',
        description: 'The job ID returned by cron_create / shown by cron_list.',
      },
      name: {
        type: 'string',
        description:
          'Alternatively, the human-readable name given at creation. Exactly one of id or name is required; ' +
          'an ambiguous name (shared by several jobs) is rejected — use the id instead.',
      },
    },
    // Exactly one of id | name is required; enforced in execute so the "pick one" /
    // ambiguity errors are surfaced cleanly.
  },
  // PARITY (additive): declared structured-result schema so an SDK/automation caller
  // can consume the job detail without parsing the human-readable block.
  outputSchema: {
    type: 'object',
    properties: {
      id: { type: 'string' },
      name: { type: 'string', description: 'The human-readable job name (absent when unnamed).' },
      cron: { type: 'string', description: 'The raw cron expression.' },
      prompt: { type: 'string', description: 'The full, untruncated scheduled prompt.' },
      recurring: { type: 'boolean' },
      durable: { type: 'boolean' },
      active: { type: 'boolean', description: 'True when the job still has an upcoming fire time.' },
      nextRun: { type: 'number', description: 'Epoch ms of the next fire (absent when inactive).' },
      lastRun: { type: 'number', description: 'Epoch ms of the last fire (absent when it never fired).' },
      errorCode: { type: 'number', description: 'Present on failure: 1 = validation / not-found.' },
    },
    required: ['id'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(input, context): Promise<ToolResult> {
    // Strict input: reject unknown keys so a typo'd parameter is surfaced.
    const unknown = Object.keys(input).filter(k => !ALLOWED_KEYS.has(k));
    if (unknown.length > 0) {
      return { output: `Error: unknown parameter(s): ${unknown.join(', ')}.`, isError: true, metadata: { errorCode: 1 } };
    }

    const hasId = input.id !== undefined;
    const hasName = input.name !== undefined;
    // Exactly one selector (id | name). Zero → nothing to look up; both → ambiguous.
    if (hasId === hasName) {
      return {
        output: 'Error: provide exactly one of id or name.',
        isError: true,
        metadata: { errorCode: 1 },
      };
    }

    const sched = getScheduler(context.cwd);
    let job;
    let selector: string;
    if (hasId) {
      const id = input.id;
      if (!id || typeof id !== 'string' || !ID_RE.test(id)) {
        return {
          output: 'Error: id is required and must match ^[\\w-]+$.',
          isError: true,
          metadata: { errorCode: 1 },
        };
      }
      selector = id;
      const found = sched.get(id);
      // Teammate-scoped visibility (mirrors cron_list): a sub-agent may only read a
      // global job or one it owns; another teammate's job is reported as not found so
      // its full prompt is never leaked across agents.
      const visible = found && (!found.agentId || context.agentId === undefined || found.agentId === context.agentId);
      job = visible ? found : undefined;
    } else {
      const name = input.name;
      if (!name || typeof name !== 'string') {
        return {
          output: 'Error: name must be a non-empty string.',
          isError: true,
          metadata: { errorCode: 1 },
        };
      }
      selector = name;
      // Name lookup is already teammate-scoped by findByName (via list(agentId)).
      const matches = sched.findByName(name, context.agentId);
      if (matches.length > 1) {
        return {
          output: `Error: name "${name}" is ambiguous — ${matches.length} jobs share it (${matches.map(j => j.id).join(', ')}). Use cron_get with the id instead.`,
          isError: true,
          metadata: { errorCode: 1 },
        };
      }
      job = matches[0];
    }
    if (!job) {
      return {
        output: `Error: no scheduled job with ${hasId ? 'id' : 'name'} "${selector}" (not found).`,
        isError: true,
        metadata: { errorCode: 1 },
      };
    }

    const active = isActiveJob(job);
    const schedule = sched.formatSchedule(job.cron, { utc: job.utc });
    const next = active ? new Date(job.nextRun as number).toLocaleString() : 'N/A';
    const lastLine = typeof job.lastRun === 'number'
      ? `\nLast: ${new Date(job.lastRun).toLocaleString()} (local)`
      : '';
    const nameLine = job.name ? ` "${job.name}"` : '';
    const output =
      `${job.id}${nameLine}: ${schedule} (${formatJobFlags(job)})\n` +
      `Next: ${next} (local)${lastLine}\n` +
      `Prompt:\n${job.prompt}`;

    return {
      output,
      metadata: {
        id: job.id,
        ...(job.name ? { name: job.name } : {}),
        cron: job.cron,
        prompt: job.prompt,
        recurring: !!job.recurring,
        durable: !!job.durable,
        active,
        ...(active ? { nextRun: job.nextRun } : {}),
        ...(typeof job.lastRun === 'number' ? { lastRun: job.lastRun } : {}),
      },
    };
  },
};
