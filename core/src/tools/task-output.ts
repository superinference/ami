import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { coerceBool, coerceNumber } from '../utils/coerce';

/** Default cap on how long task_output({wait:true}) blocks for a task to finish. */
const DEFAULT_WAIT_TIMEOUT_MS = 10 * 60 * 1000;
/** Hard upper bound on the blocking wait timeout (mirrors Claude's 600000 cap). */
const MAX_WAIT_TIMEOUT_MS = 10 * 60 * 1000;
const WAIT_POLL_INTERVAL_MS = 400;
/** Hard character cap on returned task output so a chatty task can't flood context. */
const MAX_OUTPUT_CHARS = 100_000;
/** Default number of tail lines returned when none is requested (mirrors Claude). */
const DEFAULT_TAIL_LINES = 50;

/**
 * Byte/char cap: truncate from the FRONT (keep the latest tail) and mark it, so a
 * chatty task can't flood the model's context. Shared by the whole-buffer read and
 * the incremental (since_offset) read paths.
 */
function capBody(body: string): string {
  if (body.length <= MAX_OUTPUT_CHARS) return body;
  return `... [output truncated to the last ${MAX_OUTPUT_CHARS} chars]\n` + body.slice(body.length - MAX_OUTPUT_CHARS);
}

/** Apply a regex line filter then a tail to a raw chunk of output (incremental path). */
function filterAndTail(chunk: string, re: RegExp | undefined, tail: number): string {
  let lines = chunk.split('\n');
  if (re) lines = lines.filter((line) => re.test(line));
  if (tail > 0) lines = lines.slice(-tail);
  return lines.join('\n');
}

export const taskOutputTool: ToolDefinition = {
  name: 'task_output',
  // PARITY (additive): dedicated tool-search hint + back-compat aliases so a model
  // or an old transcript/SDK caller using Claude's renamed `AgentOutputTool` /
  // `BashOutputTool` names still resolves to this tool.
  searchHint: 'read output/logs from a background task',
  aliases: ['AgentOutputTool', 'BashOutputTool'],
  description:
    'Get status and output from a background task (task/agent/bash). Returns status, exit code (if finished), ' +
    'elapsed time, and the tail of output. Set wait:true to BLOCK until the task finishes and return its final ' +
    'result in a single call — prefer this over repeatedly polling (it is read-only, so several task_output(wait) ' +
    'calls in one turn run concurrently and all return together when their tasks complete). Pass filter to keep ' +
    'only output lines matching a regex, or since_offset to retrieve ONLY the bytes appended since the last read ' +
    '(the response reports the next offset to pass back for a cursor-style incremental tail).',
  inputSchema: {
    type: 'object',
    properties: {
      task_id: {
        type: 'string',
        description: 'The task ID returned by bash/task with run_in_background.',
      },
      tail: {
        type: 'number',
        description: 'Number of lines from the end of output to return. Defaults to 50. Use 0 for all buffered lines.',
      },
      filter: {
        type: 'string',
        description: 'Regular expression; only output lines matching it are returned (applied before tail). ' +
          'Mirrors Claude Code BashOutput\'s filter. An invalid pattern is reported as an error.',
      },
      since_offset: {
        type: 'number',
        description: 'Byte offset of the last read. When set, only output appended since that offset is returned, ' +
          'and the response reports "Next offset" to pass on the next call for an incremental (since-last-read) tail.',
      },
      wait: {
        type: 'boolean',
        description: 'Block until the task reaches a terminal state (completed/failed/killed), then return its final output. Avoids a poll loop.',
      },
      timeout_ms: {
        type: 'number',
        description: 'Max time to block when wait:true (default 600000 = 10 min).',
      },
    },
    required: ['task_id'],
  },
  isReadOnly: true,

  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    if (!context.processManager) {
      return { output: 'Error: Background process manager is not available.', isError: true };
    }

    const taskId = input.task_id as string;
    // FUNCTIONAL (parity): validate tail. A bare `(input.tail as number) ?? 50`
    // let a NaN / string / negative tail through, yielding "last NaN lines" or a
    // broken slice. coerceNumber accepts only a finite number (or numeric string),
    // floors to an int, clamps at 0, and otherwise falls back to the default.
    const tail = coerceNumber(input.tail, DEFAULT_TAIL_LINES, { integer: true, min: 0 });
    const wait = coerceBool(input.wait);

    // FUNCTIONAL (parity): optional regex line filter, mirroring Claude Code's
    // BashOutput `filter`. Compile in the tool so an INVALID pattern surfaces a
    // clear error (instead of silently matching nothing) and the valid RegExp is
    // handed to the process manager, which filters BEFORE applying the tail.
    let filterRe: RegExp | undefined;
    const rawFilter = input.filter;
    if (rawFilter !== undefined && rawFilter !== null && rawFilter !== '') {
      if (typeof rawFilter !== 'string') {
        return { output: 'Error: filter must be a regular-expression string.', isError: true };
      }
      try {
        filterRe = new RegExp(rawFilter);
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return { output: `Error: invalid filter regex ${JSON.stringify(rawFilter)}: ${msg}`, isError: true };
      }
    }

    // FUNCTIONAL (parity): incremental / since-last-read cursor. When since_offset
    // is supplied the body is the bytes appended since that offset (via the process
    // manager's getOutputTail), and the response reports the next offset so the
    // model can keep a cursor instead of re-reading the whole buffer each poll.
    const hasSince = input.since_offset !== undefined && input.since_offset !== null;
    const sinceOffset = hasSince ? coerceNumber(input.since_offset, 0, { integer: true, min: 0 }) : 0;

    // Clamp the blocking wait to [1ms, MAX]; a request above the cap is clamped,
    // not honored, so the model can't park a call for an unbounded time.
    const requestedTimeout = typeof input.timeout_ms === 'number' && input.timeout_ms > 0
      ? input.timeout_ms : DEFAULT_WAIT_TIMEOUT_MS;
    const waitTimeoutMs = Math.min(requestedTimeout, MAX_WAIT_TIMEOUT_MS);

    let result = context.processManager.getOutput(taskId, tail, filterRe);
    if (!result) {
      return { output: `Error: No background task found with ID "${taskId}". Use task_list to see all tasks.`, isError: true };
    }

    // Blocking wait: poll internally until terminal or timeout/abort, so the model
    // spends ONE tool call instead of a multi-turn poll loop (each parent turn is
    // a full model round-trip). task_output is read-only, so several wait calls in
    // one turn run concurrently and all settle when their tasks finish.
    if (wait && result.status === 'running') {
      const deadline = Date.now() + waitTimeoutMs;
      while (result && result.status === 'running' && Date.now() < deadline) {
        if (context.abortSignal?.aborted) break;
        // Live progress while blocking so the TUI shows the wait isn't a hang
        // (mirrors Claude's "waiting for task" progress line). Emitted each poll
        // with the task id, status, and elapsed time.
        const secs = (result.elapsedMs / 1000).toFixed(0);
        context.onProgress?.(`waiting for task ${taskId} (${result.status}, ${secs}s elapsed)`);
        await new Promise(r => setTimeout(r, WAIT_POLL_INTERVAL_MS));
        result = context.processManager.getOutput(taskId, tail, filterRe);
      }
      if (!result) {
        return { output: `Error: background task "${taskId}" disappeared while waiting.`, isError: true };
      }
    }

    const elapsed = result.elapsedMs < 1000
      ? `${result.elapsedMs}ms`
      : result.elapsedMs < 60000
        ? `${(result.elapsedMs / 1000).toFixed(1)}s`
        : `${(result.elapsedMs / 60000).toFixed(1)}m`;

    const lines = [
      `Task: ${taskId}`,
      `Status: ${result.status}`,
      `Elapsed: ${elapsed}`,
    ];

    if (filterRe) {
      lines.push(`Filter: ${filterRe} (matching lines only)`);
    }

    // Distinct retrieval status: a wait that returned while still running means the
    // timeout elapsed (not that the task is ready) — call that out explicitly so
    // the model doesn't mistake it for a completed result.
    if (wait && result.status === 'running') {
      lines.push(`Retrieval: wait timed out after ${waitTimeoutMs}ms — task still running (call task_output again to keep waiting).`);
    }

    if (result.exitCode !== null) {
      lines.push(`Exit code: ${result.exitCode}`);
    }

    // FUNCTIONAL (parity): explicit success/failure verdict for a terminal task so
    // a non-zero exit reads as a failure signal rather than just a bare number the
    // model has to interpret. Only emitted once the task has actually finished.
    if (result.status !== 'running' && result.exitCode !== null) {
      lines.push(result.exitCode === 0 ? 'Result: success' : `Result: failure (exit code ${result.exitCode})`);
    }

    const tailLabel = tail > 0 ? `last ${tail} lines` : 'all lines';

    // Incremental (since_offset) read path: show only what was appended since the
    // caller's offset and report the cursor to resume from. Falls back to the
    // whole-buffer path when the process manager can't do byte-offset reads.
    if (hasSince && typeof context.processManager.getOutputTail === 'function') {
      const inc = context.processManager.getOutputTail(taskId, sinceOffset);
      if (inc) {
        lines.push(`Next offset: ${inc.offset}`);
        if (inc.reset) {
          lines.push('Note: output shrank/rotated since the last read — re-read from the start (offset reset).');
        }
        const body = filterAndTail(inc.output, filterRe, tail).trim();
        if (body) {
          lines.push('', `--- New output (since offset ${sinceOffset}, ${tailLabel}) ---`, capBody(body));
        } else {
          lines.push('', filterRe ? '(no new output matching filter)' : '(no new output since last read)');
        }
        return { output: lines.join('\n'), isError: false };
      }
    }

    if (result.output.trim()) {
      const body = capBody(result.output.trim());
      lines.push('', `--- Output (${tailLabel}) ---`, body);
    } else {
      lines.push('', filterRe ? '(no output matching filter yet)' : '(no output yet)');
    }

    return { output: lines.join('\n'), isError: false };
  },
};
