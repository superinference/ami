import { ToolDefinition, ToolResult, ToolContext } from '../types';

const MAX_SLEEP_MS = 600_000; // 10 minutes

const DURATION_SUFFIX_MS: Record<string, number> = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
};

/**
 * Parse a human-friendly duration token into milliseconds, or null if it is not
 * a plain duration. Mirrors the shell `sleep` affordance (`5s`, `2m`, `1h`) that
 * the reference advertises in its sleep CommandSpec: a decimal NUMBER with an
 * optional unit suffix `ms|s|m|h`. A BARE number is interpreted as SECONDS to
 * match how a human invokes `sleep 5` — distinct from the millisecond-valued
 * `duration_ms` field. Returns null for anything that is not a single duration
 * token (variables, expressions, flags, …) so the caller can reject it rather
 * than guess.
 */
export function parseSleepDuration(value: string): number | null {
  const m = value.trim().match(/^(\d*\.?\d+)\s*(ms|s|m|h)?$/i);
  if (!m) return null;
  const n = parseFloat(m[1]);
  if (!Number.isFinite(n)) return null;
  const unit = (m[2] || 's').toLowerCase(); // bare number → seconds
  return n * DURATION_SUFFIX_MS[unit];
}

export const sleepTool: ToolDefinition = {
  name: 'sleep',
  description:
    'Wait for a specified duration before continuing. Use this when the user tells you to rest, ' +
    'when you have nothing to do right now, or to give background processes / external state time ' +
    'to progress before polling again.\n' +
    'Prefer this over Bash(sleep ...): it does not hold a shell process open while waiting. ' +
    'You can call this concurrently with other tools. The user can interrupt the sleep at any time, ' +
    'in which case the result reports how long you actually slept and that it was interrupted.\n' +
    'Do not sleep between commands that can run immediately — just run them. For a long-running task, ' +
    'start it with the bash run_in_background option instead of sleeping; you will be notified when it ' +
    'completes, so do not poll it. Do not retry a failing command in a sleep loop — diagnose the root ' +
    'cause instead. If you must poll external state, run a check command rather than sleeping first; ' +
    'when you do sleep, keep it short (1–5s) unless you are deliberately waiting out a longer external process.\n' +
    'While idle you may receive periodic scheduled check-in prompts — look for useful work to do before sleeping. ' +
    'If a new user message is queued while you are sleeping, the sleep wakes early (within ~1s) so you can ' +
    'respond promptly instead of waiting out the full duration.\n' +
    'Cost note: each wake-up costs an API call, but the prompt cache expires after ~5 minutes of ' +
    'inactivity — balance the sleep duration against cache re-warming accordingly. ' +
    'Maximum 600000ms (10 minutes).',
  inputSchema: {
    type: 'object',
    properties: {
      duration_ms: {
        type: 'number',
        description:
          'Duration to sleep in milliseconds (1–600000). As a guide: 5s = 5000, 2m = 120000, 1h would exceed the 10-minute cap.',
      },
      duration: {
        type: 'string',
        description:
          'Alternative to duration_ms: a human-friendly duration with an optional unit suffix (seconds or 5s, 2m, 1h; a bare number means seconds). Capped at 10 minutes. If both are given, duration_ms wins.',
      },
      reason: {
        type: 'string',
        description: 'Short explanation of why the sleep is needed.',
      },
    },
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const reason = (input.reason as string) || '';

    // Parity (functional): accept either the millisecond-valued `duration_ms` or a
    // human-friendly `duration` string (`5s`, `2m`, `1h`) matching the reference's
    // shell-`sleep` affordance. `duration_ms` wins when both are supplied.
    let rawDuration: number | null = null;
    if (typeof input.duration_ms === 'number') {
      rawDuration = input.duration_ms;
    } else if (typeof input.duration === 'string' && input.duration.trim() !== '') {
      rawDuration = parseSleepDuration(input.duration);
      if (rawDuration === null) {
        return {
          output: `Error: could not parse duration "${input.duration}". Use milliseconds in duration_ms, or a value like 5s, 2m, 1h in duration.`,
          isError: true,
        };
      }
    }

    // Parity (functional): reject non-finite durations. `Infinity` / `-Infinity`
    // are typeof 'number' and not NaN, so the old `isNaN(...) || <= 0` guard let
    // `Infinity` through — it was then silently clamped to MAX and the tool slept
    // the full 10 minutes. Treat non-finite as invalid input instead of a request.
    if (
      rawDuration === null ||
      typeof rawDuration !== 'number' ||
      !Number.isFinite(rawDuration) ||
      rawDuration <= 0
    ) {
      return { output: 'Error: duration_ms must be a positive number (finite, > 0).', isError: true };
    }

    // Parity (functional): clamp to a minimum of 1ms so a sub-millisecond request
    // (e.g. 0.4) that passes the > 0 check does not round down to a 0ms no-op.
    const requested = Math.max(1, Math.round(rawDuration));
    const duration = Math.min(requested, MAX_SLEEP_MS);
    // Parity (functional): surface when the requested duration exceeded the cap so
    // the model knows it slept less than it asked for (the old message said only
    // "Slept for Xs" with no indication the request was truncated).
    const capped = requested > MAX_SLEEP_MS;
    const start = Date.now();

    // Parity (functional): early-wake polling. When the engine supplies
    // `hasQueuedInput`, poll it so a message queued mid-sleep wakes the agent
    // promptly (≤ one poll interval) instead of blocking the full duration —
    // mirroring the reference's SleepTool polling hasCommandsInQueue() and waking <1s.
    const POLL_INTERVAL_MS = 250;
    const hasQueuedInput =
      typeof context.hasQueuedInput === 'function' ? context.hasQueuedInput : undefined;

    const outcome = await new Promise<'done' | 'interrupted' | 'input'>((resolve) => {
      const signal = context.abortSignal;
      let onAbort: (() => void) | undefined;
      let poll: ReturnType<typeof setInterval> | undefined;

      const cleanup = () => {
        clearTimeout(timer);
        if (poll) clearInterval(poll);
        if (signal && onAbort) signal.removeEventListener('abort', onAbort);
      };

      const timer = setTimeout(() => {
        cleanup();
        resolve('done');
      }, duration);
      // Do not keep the event loop alive solely for a pending sleep.
      if (typeof timer.unref === 'function') timer.unref();

      if (hasQueuedInput) {
        if (hasQueuedInput()) {
          cleanup();
          resolve('input');
          return;
        }
        poll = setInterval(() => {
          if (hasQueuedInput()) {
            cleanup();
            resolve('input');
          }
        }, Math.min(POLL_INTERVAL_MS, duration));
        if (typeof poll.unref === 'function') poll.unref();
      }

      if (signal) {
        if (signal.aborted) {
          cleanup();
          resolve('interrupted');
          return;
        }
        onAbort = () => {
          cleanup();
          resolve('interrupted');
        };
        signal.addEventListener('abort', onAbort, { once: true });
      }
    });

    const interrupted = outcome === 'interrupted';
    const wokeForInput = outcome === 'input';

    const sleptSec = ((Date.now() - start) / 1000).toFixed(1);
    const reqSec = (duration / 1000).toFixed(1);
    const reasonSuffix = reason ? ` Reason: ${reason}` : '';
    const cappedSuffix = capped
      ? ` (requested ${(requested / 1000).toFixed(1)}s was capped to ${(MAX_SLEEP_MS / 1000).toFixed(1)}s)`
      : '';
    let msg: string;
    if (interrupted) {
      msg = `Sleep interrupted after ${sleptSec}s (requested ${reqSec}s).${cappedSuffix}${reasonSuffix}`;
    } else if (wokeForInput) {
      msg = `Sleep woke early after ${sleptSec}s because new input was queued (requested ${reqSec}s).${cappedSuffix}${reasonSuffix}`;
    } else {
      msg = `Slept for ${sleptSec}s.${cappedSuffix}${reasonSuffix}`;
    }

    return {
      output: msg,
      metadata: {
        requestedMs: duration,
        originalRequestedMs: requested,
        capped,
        sleptMs: Date.now() - start,
        interrupted,
        wokeForInput,
      },
    };
  },
};
