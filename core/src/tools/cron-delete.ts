import { ToolDefinition, ToolResult } from '../types';
import { getScheduler } from '../cron';
import { coerceBool } from '../utils/coerce';
import { isCronEnabled } from './tool-gates';

const ID_RE = /^[\w-]+$/;
const ALLOWED_KEYS = new Set(['id', 'ids', 'all', 'ignoreMissing']);

export const cronDeleteTool: ToolDefinition = {
  name: 'cron_delete',
  // PARITY (additive): declarative per-tool gate for the cron subsystem.
  isEnabled: () => isCronEnabled(),
  description:
    'Cancel scheduled cron job(s) by ID (as shown by cron_list). ' +
    'Delete one job with id, several at once with ids:[...], or every job you own with all:true. ' +
    'You can only delete jobs you own; a job created by another teammate is rejected. ' +
    'Pass ignoreMissing:true to treat an already-deleted / unknown id as a no-op (idempotent) ' +
    'instead of an error.',
  // PARITY (additive): long-form when-to-use guidance kept ALONGSIDE the terse
  // description (Claude splits CRON_DELETE_DESCRIPTION vs buildCronDeletePrompt).
  // Surfaced to the model when AMI's tools are exposed over MCP (mcp-server.ts
  // appends `prompt` to `description`). Mirrors Claude's durable-aware wording:
  // a cancel removes the job previously scheduled by cron_create from the durable
  // on-disk store OR the in-memory session store, depending on how it was created.
  prompt:
    'Cancel a cron job previously scheduled with cron_create. For a durable job this removes it ' +
    'from the persisted on-disk scheduler store (scheduled_tasks.json); for a session-only job it ' +
    'removes it from the in-memory session store — so the job stops firing in both cases. ' +
    'Pass the id (or ids / all) exactly as shown by cron_list. A teammate/sub-agent can only cancel ' +
    'its own jobs. Cancelling an already-removed job is an error unless you pass ignoreMissing:true.',
  inputSchema: {
    type: 'object',
    properties: {
      id: {
        type: 'string',
        description: 'A single job ID returned by cron_create / shown by cron_list.',
      },
      ids: {
        type: 'array',
        items: { type: 'string' },
        description: 'A list of job IDs to delete in one call (bulk delete).',
      },
      all: {
        type: 'boolean',
        description: 'true = delete every job you own (scoped to the calling agent). Mutually exclusive with id/ids.',
        default: false,
      },
      ignoreMissing: {
        type: 'boolean',
        description: 'true = a not-found / already-deleted id is a success no-op (idempotent) rather than an error.',
        default: false,
      },
    },
    // Exactly one selector (id | ids | all) is required; enforced in execute so the
    // mutual-exclusivity / "pick one" error is surfaced cleanly.
  },
  // PARITY (additive): declared structured-result schema (Claude's CronDeleteTool
  // outputSchema). Object-rooted so the provider advertises it
  // (toolDefinitionsToOpenAI exposes object-root outputSchema only). Lets a
  // programmatic / SDK / resume caller branch on the deleted id(s) + count +
  // errorCode without parsing the human-readable message string.
  outputSchema: {
    type: 'object',
    properties: {
      deleted: { type: 'number', description: 'How many jobs were actually cancelled by this call.' },
      deletedIds: {
        type: 'array',
        items: { type: 'string' },
        description: 'The ids of the jobs that were cancelled (empty when none matched).',
      },
      errorCode: {
        type: 'number',
        description: 'Present on failure: 1 = validation / not-found, 2 = ownership/authorization refusal.',
      },
      alreadyDeleted: {
        type: 'boolean',
        description: 'True on an ignoreMissing no-op where the single target id was already gone.',
      },
      inFlightCancelled: {
        type: 'number',
        description:
          'How many already-enqueued/in-flight scheduled runs were also aborted by this cancel ' +
          '(a run a tick had already dispatched before the delete). 0 when the delete only stopped future scheduling.',
      },
    },
    required: ['deleted'],
  },
  isReadOnly: false,

  async execute(input, context): Promise<ToolResult> {
    // Strict input: reject unknown keys so a typo'd parameter is surfaced.
    const unknown = Object.keys(input).filter(k => !ALLOWED_KEYS.has(k));
    if (unknown.length > 0) {
      return { output: `Error: unknown parameter(s): ${unknown.join(', ')}.`, isError: true };
    }

    const ignoreMissing = coerceBool(input.ignoreMissing);
    const all = coerceBool(input.all);
    const hasId = input.id !== undefined;
    const hasIds = input.ids !== undefined;

    // Exactly one selector. Zero → nothing to do; more than one → ambiguous.
    const selectors = [hasId, hasIds, all].filter(Boolean).length;
    if (selectors === 0) {
      return {
        output: 'Error: provide one of id, ids:[...], or all:true.',
        isError: true,
        metadata: { errorCode: 1 },
      };
    }
    if (selectors > 1) {
      return {
        output: 'Error: id, ids, and all are mutually exclusive — provide exactly one.',
        isError: true,
        metadata: { errorCode: 1 },
      };
    }

    const sched = getScheduler(context.cwd);

    // Resolve the target id set for each mode.
    let ids: string[];
    if (all) {
      // Scoped to what this agent owns/sees — never deletes a teammate's job.
      ids = sched.list(context.agentId).map(j => j.id);
      if (ids.length === 0) {
        // Idempotent no-op: nothing scheduled is not an error.
        return { output: 'No scheduled jobs to delete.', metadata: { deleted: 0, deletedIds: [] } };
      }
    } else if (hasIds) {
      const raw = input.ids;
      if (!Array.isArray(raw)) {
        return { output: 'Error: ids must be an array of job IDs.', isError: true, metadata: { errorCode: 1 } };
      }
      if (raw.length === 0) {
        return { output: 'Error: ids must not be empty.', isError: true, metadata: { errorCode: 1 } };
      }
      ids = raw as string[];
    } else {
      ids = [input.id as string];
    }

    // Validate id format for caller-supplied ids (all-mode ids come from the
    // scheduler and are trusted). Reject duplicates so the deleted count is exact.
    if (!all) {
      const seen = new Set<string>();
      for (const id of ids) {
        if (!id || typeof id !== 'string' || !ID_RE.test(id)) {
          return {
            output: 'Error: id is required and must match ^[\\w-]+$.',
            isError: true,
            metadata: { errorCode: 1 },
          };
        }
        if (seen.has(id)) {
          return { output: `Error: duplicate id "${id}" in request.`, isError: true, metadata: { errorCode: 1 } };
        }
        seen.add(id);
      }
    }

    // Apply deletions, collecting outcomes so a single failure doesn't abort the
    // rest of a bulk request. Authorization failures are collected, never swallowed.
    const deleted: string[] = [];
    const missing: string[] = [];
    const unauthorized: Array<{ id: string; message: string }> = [];
    // Count of already-enqueued/in-flight runs aborted alongside the deletes, so a
    // cancel is a true abort (drain pending work) not just "stop future scheduling"
    // — mirrors Claude's killShellTasks dequeueAllMatching alongside clearing timers.
    let inFlightCancelled = 0;
    for (const id of ids) {
      try {
        if (sched.delete(id, context.agentId)) {
          deleted.push(id);
          // Only a job that actually existed could have a pending/in-flight run.
          inFlightCancelled += sched.cancelPending(id);
        } else {
          missing.push(id);
        }
      } catch (err) {
        unauthorized.push({ id, message: err instanceof Error ? err.message : String(err) });
      }
    }
    // Suffix appended to any success message when a dispatched run was also aborted.
    const inFlightNote = inFlightCancelled > 0
      ? ` Also aborted ${inFlightCancelled} already-dispatched run(s).`
      : '';

    // Legacy single-id path: preserve the exact output + errorCodes, plus the new
    // ignoreMissing idempotency behavior.
    const singleMode = hasId; // selectors===1 guarantees only id is set here
    if (singleMode) {
      const id = ids[0];
      if (unauthorized.length > 0) {
        return { output: `Error: ${unauthorized[0].message}`, isError: true, metadata: { errorCode: 2 } };
      }
      if (missing.length > 0) {
        if (ignoreMissing) {
          return { output: `Job "${id}" was already deleted (nothing to do).`, metadata: { deleted: 0, deletedIds: [], alreadyDeleted: true } };
        }
        // Name the entity so the message matches the "scheduled job" vocabulary of
        // the description (parity: Claude's `No scheduled job with id '<id>'`).
        return { output: `Error: no scheduled job with id "${id}" (not found).`, isError: true, metadata: { errorCode: 1 } };
      }
      // Verb aligned with the tool's "Cancel" description (parity: Claude's
      // `Cancelled job <id>.`).
      return {
        output: `Cancelled job ${id}.${inFlightNote}`,
        metadata: { deleted: 1, deletedIds: [id], inFlightCancelled },
      };
    }

    // Bulk / all path: aggregate a summary. Authorization failures are always a
    // hard error; missing ids are an error only when ignoreMissing is false.
    const isError = unauthorized.length > 0 || (missing.length > 0 && !ignoreMissing);
    const parts: string[] = [];
    // Verb aligned with the tool's "Cancel" description (parity).
    if (deleted.length > 0) parts.push(`Cancelled ${deleted.length} job(s): ${deleted.join(', ')}.`);
    if (missing.length > 0) {
      parts.push(
        ignoreMissing
          ? `Skipped ${missing.length} already-deleted job(s): ${missing.join(', ')}.`
          : `Not found: ${missing.join(', ')}.`,
      );
    }
    if (unauthorized.length > 0) {
      parts.push(`Not authorized to delete: ${unauthorized.map(u => u.id).join(', ')}.`);
    }
    const body = parts.join(' ');

    if (isError) {
      return {
        output: `Error: ${body}`,
        isError: true,
        // Authorization failures (errorCode 2) outrank not-found (errorCode 1).
        metadata: { errorCode: unauthorized.length > 0 ? 2 : 1, deleted: deleted.length, deletedIds: deleted },
      };
    }
    return {
      output: `${body}${inFlightNote}`,
      metadata: { deleted: deleted.length, deletedIds: deleted, inFlightCancelled },
    };
  },
};
