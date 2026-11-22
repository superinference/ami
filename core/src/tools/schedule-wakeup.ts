import { ToolDefinition, ToolResult } from '../types';
import { getScheduler } from '../cron';
import { isCronEnabled } from './tool-gates';
import { errorMessage } from '../credential-guard';


/** Hard bounds on the self-paced loop cadence (seconds). A wakeup may fire no
 *  sooner than 60s (abuse floor) and no later than 3600s (so the loop never
 *  silently parks for longer than an hour). */
export const WAKEUP_MIN_DELAY_SECONDS = 60;
export const WAKEUP_MAX_DELAY_SECONDS = 3600;

/** The sentinel continuation prompt. When the model omits the prompt on a
 *  self-paced wakeup, the loop fires this verbatim instead of ending — downstream
 *  consumers recognise this exact string as an auto-generated continuation
 *  (not a user turn) via the `autoContinuation` metadata flag. */
export const LOOP_CONTINUE_SENTINEL = 'Continue the current task.';

/** After this many CONSECUTIVE no-op wakeups the output carries an escalation
 *  hint suggesting the loop may be idle and could be stopped. */
export const NOOP_STREAK_WARN_THRESHOLD = 3;

/** Per-cwd count of consecutive no-op wakeups. A real (non-noop) wakeup — or a
 *  stop — resets the streak to zero. Module-scoped so it survives across the
 *  discrete, stateless tool invocations that make up one loop. */
const noopStreaks = new Map<string, number>();

/** Test/loop-teardown helper: clear the noop streak for one cwd, or all when no
 *  cwd is given. Keeps the pacing state from leaking across sessions. */
export function resetLoopPacingState(cwd?: string): void {
  if (cwd === undefined) noopStreaks.clear();
  else noopStreaks.delete(cwd);
}

/** Coerce an arbitrary delay input into an integer second count clamped to the
 *  legal window. Non-finite / missing / sub-minimum values collapse to the
 *  floor; fractional values round to the nearest whole second. */
export function clampWakeupDelay(raw: unknown): number {
  const n = typeof raw === 'number' ? raw : Number(raw);
  if (!Number.isFinite(n)) return WAKEUP_MIN_DELAY_SECONDS;
  const rounded = Math.round(n);
  return Math.max(WAKEUP_MIN_DELAY_SECONDS, Math.min(WAKEUP_MAX_DELAY_SECONDS, rounded));
}

export const scheduleWakeupTool: ToolDefinition = {
  name: 'schedule_wakeup',
  // PARITY (additive): declarative per-tool gate for the cron subsystem.
  isEnabled: () => isCronEnabled(),
  description:
    'Schedule a one-shot wakeup after a delay. Used for /loop dynamic pacing. ' +
    'The prompt fires once after delaySeconds, then auto-deletes. Pass stop:true ' +
    'to end the loop without scheduling anything; pass noop:true when a wakeup ' +
    'did no useful work (consecutive no-ops are tracked and surfaced).',
  inputSchema: {
    type: 'object',
    properties: {
      delaySeconds: {
        type: 'number',
        description: 'Seconds from now to wake up. Clamped to [60, 3600].',
      },
      prompt: {
        type: 'string',
        description: 'The prompt to fire on wake-up.',
      },
      reason: {
        type: 'string',
        description: 'Short explanation of the chosen delay (shown to user).',
      },
      stop: {
        type: 'boolean',
        description:
          'End the self-paced loop. When true, NO wakeup is scheduled and all ' +
          'other fields (delaySeconds/prompt) are ignored.',
      },
      noop: {
        type: 'boolean',
        description:
          'Mark this wakeup as a no-op (nothing changed since last turn). Still ' +
          'schedules the next wakeup but increments the consecutive no-op streak.',
      },
    },
    required: ['delaySeconds', 'reason'],
  },
  isReadOnly: false,

  async execute(input, context): Promise<ToolResult> {
    const cwdKey = context.cwd ?? '';
    const reason = (input.reason as string | undefined) ?? '';

    // --- stop short-circuit (ignore delaySeconds/prompt/noop entirely) --------
    // A self-paced loop ends by calling with stop:true. Do NOT schedule another
    // wakeup, and reset the pacing state so a later loop starts clean.
    if (input.stop === true) {
      resetLoopPacingState(cwdKey);
      return {
        output: `Loop stopped — no further wakeup scheduled.` + (reason ? `\n  Reason: ${reason}` : ''),
        metadata: { stopped: true },
      };
    }

    const delaySeconds = clampWakeupDelay(input.delaySeconds);
    const rawPrompt = input.prompt as string | undefined;

    // A self-paced loop re-supplies the prompt on every turn. If the model omits
    // it (or sends an empty/whitespace prompt), DO NOT silently end the loop —
    // fire the sentinel continuation prompt instead, so the loop survives a
    // malformed tool call (B3). `autoContinuation` lets downstream distinguish
    // this synthesised turn from a real model-authored prompt.
    const hasPrompt = !!(rawPrompt && rawPrompt.trim());
    const prompt = hasPrompt ? rawPrompt! : LOOP_CONTINUE_SENTINEL;

    // --- noop streak accounting ----------------------------------------------
    const isNoop = input.noop === true;
    const noopStreak = isNoop ? (noopStreaks.get(cwdKey) ?? 0) + 1 : 0;
    if (isNoop) noopStreaks.set(cwdKey, noopStreak);
    else noopStreaks.delete(cwdKey);
    const noopEscalating = isNoop && noopStreak >= NOOP_STREAK_WARN_THRESHOLD;

    const now = new Date();
    const fireAt = new Date(now.getTime() + delaySeconds * 1000);
    const cronExpr = `${fireAt.getMinutes()} ${fireAt.getHours()} ${fireAt.getDate()} ${fireAt.getMonth() + 1} *`;

    // Surface scheduler failures (job cap reached, etc.) as a tool error rather
    // than letting them propagate as an opaque unhandled rejection.
    let jobId: string;
    try {
      const sched = getScheduler(context.cwd);
      const job = sched.create({ cron: cronExpr, prompt, recurring: false, durable: false });
      jobId = job.id;
    } catch (err) {
      const msg = errorMessage(err);
      return {
        output: `Failed to schedule wakeup: ${msg}`,
        isError: true,
        metadata: { stopped: false, error: msg },
      };
    }

    const lines = [
      `Wakeup scheduled: ${jobId}`,
      `  Delay: ${delaySeconds}s`,
      `  Fire at: ${fireAt.toLocaleTimeString()}`,
      `  Reason: ${reason}`,
    ];
    if (!hasPrompt) lines.push('  (no prompt supplied — using fallback continuation prompt)');
    if (isNoop) lines.push(`  No-op streak: ${noopStreak}`);
    if (noopEscalating) {
      lines.push(
        `  (${noopStreak} consecutive no-op wakeups — the loop may be idle; ` +
          `consider ending it with stop:true if there is nothing left to do)`,
      );
    }

    return {
      output: lines.join('\n'),
      metadata: {
        jobId,
        delaySeconds,
        fireAt: fireAt.getTime(),
        stopped: false,
        noop: isNoop,
        noopStreak,
        autoContinuation: !hasPrompt,
      },
    };
  },
};
