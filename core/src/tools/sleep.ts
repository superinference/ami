import { ToolDefinition, ToolResult, ToolContext } from '../types';

const MAX_SLEEP_MS = 600_000; // 10 minutes

export const sleepTool: ToolDefinition = {
  name: 'sleep',
  description:
    'Wait for a specified duration before continuing. Use this when the user tells you to rest, ' +
    'when you have nothing to do right now, or to give background processes / external state time ' +
    'to progress before polling again.\n' +
    'Prefer this over Bash(sleep ...): it does not hold a shell process open while waiting. ' +
    'You can call this concurrently with other tools. The user can interrupt the sleep at any time, ' +
    'in which case the result reports how long you actually slept and that it was interrupted.\n' +
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
      reason: {
        type: 'string',
        description: 'Short explanation of why the sleep is needed.',
      },
    },
    required: ['duration_ms'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const rawDuration = input.duration_ms as number;
    const reason = (input.reason as string) || '';

    // Parity (functional): reject non-finite durations. `Infinity` / `-Infinity`
    // are typeof 'number' and not NaN, so the old `isNaN(...) || <= 0` guard let
    // `Infinity` through — it was then silently clamped to MAX and the tool slept
    // the full 10 minutes. Treat non-finite as invalid input instead of a request.
    if (
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

    const interrupted = await new Promise<boolean>((resolve) => {
      const signal = context.abortSignal;
      let onAbort: (() => void) | undefined;

      const timer = setTimeout(() => {
        if (signal && onAbort) signal.removeEventListener('abort', onAbort);
        resolve(false);
      }, duration);
      // Do not keep the event loop alive solely for a pending sleep.
      if (typeof timer.unref === 'function') timer.unref();

      if (signal) {
        if (signal.aborted) {
          clearTimeout(timer);
          resolve(true);
          return;
        }
        onAbort = () => {
          clearTimeout(timer);
          resolve(true);
        };
        signal.addEventListener('abort', onAbort, { once: true });
      }
    });

    const sleptSec = ((Date.now() - start) / 1000).toFixed(1);
    const reasonSuffix = reason ? ` Reason: ${reason}` : '';
    const cappedSuffix = capped
      ? ` (requested ${(requested / 1000).toFixed(1)}s was capped to ${(MAX_SLEEP_MS / 1000).toFixed(1)}s)`
      : '';
    const msg = interrupted
      ? `Sleep interrupted after ${sleptSec}s (requested ${(duration / 1000).toFixed(1)}s).${cappedSuffix}${reasonSuffix}`
      : `Slept for ${sleptSec}s.${cappedSuffix}${reasonSuffix}`;

    return {
      output: msg,
      metadata: {
        requestedMs: duration,
        originalRequestedMs: requested,
        capped,
        sleptMs: Date.now() - start,
        interrupted,
      },
    };
  },
};
