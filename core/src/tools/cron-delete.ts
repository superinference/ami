import { ToolDefinition, ToolResult } from '../types';
import { getScheduler } from '../cron';
import { coerceBool } from '../utils/coerce';

const ID_RE = /^[\w-]+$/;
const ALLOWED_KEYS = new Set(['id', 'ids', 'all', 'ignoreMissing']);

export const cronDeleteTool: ToolDefinition = {
  name: 'cron_delete',
  description:
    'Cancel scheduled cron job(s) by ID (as shown by cron_list). ' +
    'Delete one job with id, several at once with ids:[...], or every job you own with all:true. ' +
    'You can only delete jobs you own; a job created by another teammate is rejected. ' +
    'Pass ignoreMissing:true to treat an already-deleted / unknown id as a no-op (idempotent) ' +
    'instead of an error.',
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
        return { output: 'No scheduled jobs to delete.', metadata: { deleted: 0 } };
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
    for (const id of ids) {
      try {
        if (sched.delete(id, context.agentId)) deleted.push(id);
        else missing.push(id);
      } catch (err) {
        unauthorized.push({ id, message: err instanceof Error ? err.message : String(err) });
      }
    }

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
          return { output: `Job "${id}" was already deleted (nothing to do).`, metadata: { deleted: 0, alreadyDeleted: true } };
        }
        return { output: `Error: job "${id}" not found.`, isError: true, metadata: { errorCode: 1 } };
      }
      return { output: `Deleted job ${id}.`, metadata: { deleted: 1 } };
    }

    // Bulk / all path: aggregate a summary. Authorization failures are always a
    // hard error; missing ids are an error only when ignoreMissing is false.
    const isError = unauthorized.length > 0 || (missing.length > 0 && !ignoreMissing);
    const parts: string[] = [];
    if (deleted.length > 0) parts.push(`Deleted ${deleted.length} job(s): ${deleted.join(', ')}.`);
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
        metadata: { errorCode: unauthorized.length > 0 ? 2 : 1, deleted: deleted.length },
      };
    }
    return { output: body, metadata: { deleted: deleted.length } };
  },
};
