import type {
  ToolDefinition,
  ToolResult,
  ToolContext,
  ToolValidationResult,
} from '../types';
import {
  validateAgainstSchema,
  validateSchemaShape,
  type JsonSchema,
} from './synthetic-output';

/**
 * task_complete — signal that the agent's task is finished.
 *
 * Beyond the single-shot "produce your final response" signal, this tool is a
 * functional SUPERSET that captures the OUTCOME of the task so a consumer (engine,
 * orchestrator, parent sub-agent) can read it programmatically:
 *
 *   - STATUS TRANSITION VALIDATION — an optional `status` is validated against a
 *     closed set BEFORE the body runs (via validateInput); success vs. incomplete
 *     vs. failure are distinguished so the model cannot silently self-certify a
 *     failing run as "complete".
 *   - RESULT CAPTURE — `summary`, `status`, `result`, and a completion timestamp
 *     are surfaced in `metadata` for structured consumption.
 *   - IDEMPOTENT COMPLETION — a second completion of the same (session[, agent][,
 *     taskId]) is acknowledged as a no-op instead of re-signalling.
 *   - UNKNOWN / INVALID ID HANDLING — a malformed `taskId` is rejected with a
 *     machine-readable validation error rather than being silently accepted.
 *   - ERROR SURFACING — a `failed` status surfaces as an error result; `partial` /
 *     `blocked` surface an honest "not fully complete" signal.
 *   - SCHEMA-VALIDATED FINAL OUTPUT — an optional caller-supplied `resultSchema`
 *     is validated (shape + payload) in validateInput so the concluding tool can
 *     enforce a machine-readable `result`, mirroring the reference's SyntheticOutputTool
 *     Ajv-backed conclusion ("Output does not match required schema: …").
 *   - COMPLETION-HOOK VETO — a blocking `taskCompleted` hook can refuse a clean
 *     completion and feed its reason back to the model (parity: the reference's
 *     executeTaskCompletedHooks / getTaskCompletedHookMessage).
 *   - ANTI-SELF-CERTIFICATION — a clean completion that did not affirm a
 *     verification step gets a nudge to verify before the final summary (parity:
 *     the reference's verificationNudge).
 *
 * (parity: the reference's task-completion signalling — status, result capture,
 * idempotency, validateInput, schema-validated output, completion hooks.)
 */

/**
 * Anti-self-certification nudge appended to a clean completion the model did not
 * affirm as verified. Mirrors the reference's verification-agent nudge: a caveat in the
 * summary is not a substitute for actually running and observing the checks pass.
 */
const VERIFICATION_NUDGE =
  'NOTE: You have not confirmed a verification step (set verified=true only after ' +
  'you have actually run and observed the tests/checks pass). Verify the work before ' +
  'your final summary — you cannot self-assign a PARTIAL outcome by listing caveats ' +
  'in your summary; report an honest status instead of claiming success on unproven work.';

/** Statuses the model may report. Normalised into canonical buckets below. */
const SUCCESS_STATUSES = ['completed', 'complete', 'success', 'done'] as const;
const INCOMPLETE_STATUSES = ['partial', 'blocked'] as const;
const FAILURE_STATUSES = ['failed', 'failure', 'error'] as const;

/** Every accepted status token (closed set — anything else is a validation error). */
export const VALID_TASK_STATUSES: readonly string[] = [
  ...SUCCESS_STATUSES,
  ...INCOMPLETE_STATUSES,
  ...FAILURE_STATUSES,
];

/** Canonical status bucket the tool reasons about. */
export type CanonicalTaskStatus = 'completed' | 'partial' | 'blocked' | 'failed';

/**
 * Map a raw status token to its canonical bucket. Returns undefined when the
 * token is not in {@link VALID_TASK_STATUSES}. Matching is case-insensitive and
 * trims surrounding whitespace. (parity: status transition validation)
 */
export function normalizeTaskStatus(raw: unknown): CanonicalTaskStatus | undefined {
  if (typeof raw !== 'string') return undefined;
  const s = raw.trim().toLowerCase();
  if ((SUCCESS_STATUSES as readonly string[]).includes(s)) return 'completed';
  if (s === 'partial') return 'partial';
  if (s === 'blocked') return 'blocked';
  if ((FAILURE_STATUSES as readonly string[]).includes(s)) return 'failed';
  return undefined;
}

/**
 * Registry of already-completed task keys, backing IDEMPOTENT completion. Dedup
 * only engages when the context carries a sessionId; bare/test contexts without
 * one are never tracked, preserving the legacy single-shot behaviour.
 */
const completedTasks = new Set<string>();

function completionKey(
  context: ToolContext,
  taskId: string | undefined,
): string | undefined {
  if (!context.sessionId) return undefined;
  let key = context.agentId
    ? `${context.sessionId}::${context.agentId}`
    : context.sessionId;
  if (taskId) key += `::task:${taskId}`;
  return key;
}

/**
 * Clear the idempotency registry. Exposed for the engine (new session) and for
 * tests that need a clean per-case slate. (parity: introspection/reset hook)
 */
export function resetCompletedTasks(): void {
  completedTasks.clear();
}

/**
 * Whether the task for the given context (optionally scoped to a taskId) has
 * already been marked complete. Returns false for untracked (no-sessionId)
 * contexts. (parity: completion-state introspection)
 */
export function isTaskCompleted(
  context: ToolContext,
  taskId?: string,
): boolean {
  const key = completionKey(context, taskId);
  return key !== undefined && completedTasks.has(key);
}

/** Normalise the optional taskId: a trimmed non-empty string, else undefined. */
function readTaskId(input: Record<string, unknown>): string | undefined {
  const raw = input.taskId;
  if (typeof raw !== 'string') return undefined;
  const t = raw.trim();
  return t.length > 0 ? t : undefined;
}

export const taskCompleteTool: ToolDefinition = {
  name: 'task_complete',
  description:
    'Signal that the task is complete. Call this ONLY when the work is actually done and verified — ' +
    'do not self-certify: never call it to claim success on failing tests, an unproven fix, or incomplete work. ' +
    'Report the honest outcome via `status` (completed | partial | blocked | failed) when it is anything other ' +
    'than a clean success. After calling this tool, produce a final text summary of what you changed. ' +
    'Do not make any more tool calls.',
  inputSchema: {
    type: 'object',
    properties: {
      summary: {
        type: 'string',
        description: 'Brief summary of what was done and what changed.',
      },
      status: {
        type: 'string',
        enum: [...VALID_TASK_STATUSES],
        description:
          'Outcome of the task: completed (default), partial, blocked, or failed. ' +
          'Use partial/blocked/failed to honestly signal incomplete work instead of claiming success.',
      },
      taskId: {
        type: 'string',
        description:
          'Optional identifier of the specific task being completed. Scopes idempotency so distinct ' +
          'tasks in one session can each be completed exactly once.',
      },
      result: {
        type: 'object',
        additionalProperties: true,
        description:
          'Optional structured result/outcome payload captured in the tool result metadata for the caller.',
      },
      resultSchema: {
        type: 'object',
        additionalProperties: true,
        description:
          'Optional JSON Schema the `result` payload MUST satisfy. When present, the ' +
          'result is validated against it before completion and a precise ' +
          '"Output does not match required schema" error is returned on mismatch.',
      },
      verified: {
        type: 'boolean',
        description:
          'Set true ONLY after you have actually run and observed the relevant ' +
          'tests/checks pass. A clean completion without this gets a verification nudge.',
      },
    },
    required: ['summary'],
  },
  isReadOnly: true,

  /**
   * Pre-body validation (parity: the reference's validateInput). Rejects an unknown
   * `status` token and a malformed `taskId` with a machine-readable error BEFORE
   * the tool runs, so the model gets a clean, self-correcting correction.
   */
  async validateInput(
    input: Record<string, unknown>,
  ): Promise<ToolValidationResult> {
    // Edge-case rejection: a present `summary` must be a string, so a non-string
    // (object/array/number) can never be interpolated as "[object Object]" into
    // the result. (parity: the reference's Tool.validateInput boundary type check.)
    if (input.summary !== undefined && typeof input.summary !== 'string') {
      return {
        result: false,
        message: 'Invalid summary: must be a string.',
        errorCode: 1,
      };
    }
    if (input.status !== undefined && input.status !== null) {
      if (normalizeTaskStatus(input.status) === undefined) {
        return {
          result: false,
          message:
            `Invalid status "${String(input.status)}". Valid values: ` +
            `${VALID_TASK_STATUSES.join(', ')}.`,
          errorCode: 1,
        };
      }
    }
    if (input.taskId !== undefined && input.taskId !== null) {
      if (typeof input.taskId !== 'string' || input.taskId.trim().length === 0) {
        return {
          result: false,
          message: 'Invalid taskId: must be a non-empty string.',
          errorCode: 1,
        };
      }
    }
    // Schema-validated final output: when a caller-supplied `resultSchema` is
    // present, validate its shape then the `result` payload against it, returning
    // a precise diagnostic on mismatch. (parity: the reference's SyntheticOutputTool
    // Ajv-backed schema conclusion.)
    if (input.resultSchema !== undefined && input.resultSchema !== null) {
      const shapeErr = validateSchemaShape(input.resultSchema);
      if (shapeErr) {
        return {
          result: false,
          message: `Invalid resultSchema: ${shapeErr}.`,
          errorCode: 1,
        };
      }
      const errors = validateAgainstSchema(
        input.result,
        input.resultSchema as JsonSchema,
        'result',
      );
      if (errors.length > 0) {
        return {
          result: false,
          message: `Output does not match required schema: ${errors.join(', ')}`,
          errorCode: 1,
        };
      }
    }
    return { result: true };
  },

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    // Type-safe read: a non-string that slipped past validateInput (e.g. a direct
    // execute() call) falls back to the default rather than being interpolated.
    const summary =
      (typeof input.summary === 'string' ? input.summary : '') || 'Task complete.';
    const taskId = readTaskId(input);

    // Status: default to a clean completion when unspecified. An invalid token
    // that slipped past validateInput (e.g. direct execute() calls) also falls
    // back to 'completed' so the legacy signal is never broken.
    const status: CanonicalTaskStatus =
      input.status === undefined || input.status === null
        ? 'completed'
        : normalizeTaskStatus(input.status) ?? 'completed';
    const success = status === 'completed';

    const key = completionKey(context, taskId);

    // 1. IDEMPOTENT completion: a repeat completion of the same tracked
    //    (session[, agent][, taskId]) is a no-op ack, not a re-signal.
    if (key !== undefined && completedTasks.has(key)) {
      return {
        output:
          `Task already marked complete${taskId ? ` (taskId: ${taskId})` : ''}. ` +
          `Produce your final response now. Do not make any more tool calls.`,
        isError: false,
        metadata: {
          taskComplete: true,
          alreadyComplete: true,
          status,
          ...(taskId ? { taskId } : {}),
        },
      };
    }

    // 1b. COMPLETION-HOOK VETO: a blocking `taskCompleted` hook can refuse a CLEAN
    //     completion and feed its reason back to the model, exactly like
    //     task_update's status->completed gate. An honest failed/partial/blocked
    //     signal is already "not done" and is never vetoed. A throwing hook never
    //     blocks (best-effort). The task is NOT recorded, so the model can retry.
    if (success && context._hookManager) {
      let blocking: string[] = [];
      try {
        const r = await context._hookManager.executeTaskCompleted({
          taskId: taskId ?? context.agentId ?? context.sessionId ?? 'task',
          subject: summary,
          status: 'completed',
          signal: context.abortSignal,
        });
        const errs = (r as { blockingErrors?: unknown } | void)?.blockingErrors;
        if (Array.isArray(errs)) {
          blocking = errs.filter((e): e is string => typeof e === 'string');
        }
      } catch {
        /* a throwing completion hook must not block the signal */
      }
      if (blocking.length > 0) {
        return {
          output:
            `Task completion was BLOCKED by a TaskCompleted hook: ${blocking.join('; ')}\n\n` +
            `The task is NOT complete. Address the feedback above, then signal completion ` +
            `again. [reason: completion_blocked]`,
          isError: true,
          metadata: {
            taskComplete: false,
            completionBlocked: true,
            status,
            ...(taskId ? { taskId } : {}),
            blockingErrors: blocking,
          },
        };
      }
    }

    const completedAt = new Date().toISOString();
    if (key !== undefined) completedTasks.add(key);

    const verified = input.verified === true;

    // 2. RESULT CAPTURE: surface the structured outcome for the caller.
    const metadata: Record<string, unknown> = {
      taskComplete: true,
      status,
      success,
      summary,
      verified,
      completedAt,
    };
    if (taskId) metadata.taskId = taskId;
    if (input.result !== undefined) metadata.result = input.result;

    // 3. ERROR SURFACING: distinguish success from an honest failure/partial.
    if (status === 'failed') {
      return {
        output:
          `Task reported as FAILED. Summary: ${summary}\n\n` +
          `The work is NOT complete. Produce a final response explaining what failed ` +
          `and what remains. Do not make any more tool calls.`,
        isError: true,
        metadata,
      };
    }

    if (status === 'partial' || status === 'blocked') {
      return {
        output:
          `Task marked ${status} — NOT fully complete. Summary: ${summary}\n\n` +
          `Produce a final response explaining what remains and why. ` +
          `Do not make any more tool calls.`,
        isError: false,
        metadata,
      };
    }

    // 4. Clean completion — the legacy signal, plus an anti-self-certification
    //    verification nudge when the model did not affirm it verified the work.
    const base = `Task complete. Summary: ${summary}\n\nProduce your final response now. Do not make any more tool calls.`;
    return {
      output: verified ? base : `${base}\n\n${VERIFICATION_NUDGE}`,
      isError: false,
      metadata,
    };
  },
};
