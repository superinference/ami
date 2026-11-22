import * as fs from 'fs';
import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { coerceBool, coerceNumber } from '../utils/coerce';
import { formatSize } from './spill-output';
import { formatUsageClause } from '../process-manager';
import { agentResultPath } from './task';
import { validateBoundedIntEnvVar } from '../config';
import { errorMessage } from '../credential-guard';


/**
 * Default cap on how long a blocking task_output wait runs before giving up and
 * returning the still-running status. Mirrors the reference's TaskOutputTool
 * `timeout: z.number().default(30000)` — a 30s block by default (not the 10-minute
 * ceiling), so a block-by-default poll can't park the turn for minutes.
 */
const DEFAULT_WAIT_TIMEOUT_MS = 30 * 1000;
/** Hard upper bound on the blocking wait timeout (mirrors the reference's 600000 cap). */
const MAX_WAIT_TIMEOUT_MS = 10 * 60 * 1000;
const WAIT_POLL_INTERVAL_MS = 400;
/**
 * Default hard character cap on returned task output so a chatty task can't flood
 * context. Operator-tunable via `AMI_TASK_MAX_OUTPUT_CHARS` (see `maxOutputChars`).
 */
const MAX_OUTPUT_CHARS_DEFAULT = 100_000;
/** Floor the cap can be lowered to; a lower env value is rejected to the default. */
const MAX_OUTPUT_CHARS_MIN = 1_000;
/** Ceiling the cap can be raised to; a higher env value is clamped to this. */
const MAX_OUTPUT_CHARS_MAX = 1_000_000;

/**
 * PARITY (additive): operator-tunable hard character cap on returned task output,
 * mirroring the reference's `getMaxTaskOutputLength()` (outputFormatting.ts), which
 * reads `TASK_MAX_OUTPUT_LENGTH` through `validateBoundedIntEnvVar`. AMI reads
 * `AMI_TASK_MAX_OUTPUT_CHARS` through the SAME canonical bounded-int validator so
 * the knob can never disable the cap or set an unbounded value: absent/garbage →
 * default, below the floor → default, above the ceiling → clamped. Re-read per call
 * so a `/env`-style session override takes effect without a restart.
 */
export function maxOutputChars(): number {
  return validateBoundedIntEnvVar('AMI_TASK_MAX_OUTPUT_CHARS', process.env.AMI_TASK_MAX_OUTPUT_CHARS, {
    default: MAX_OUTPUT_CHARS_DEFAULT,
    min: MAX_OUTPUT_CHARS_MIN,
    max: MAX_OUTPUT_CHARS_MAX,
  }).effective;
}
/** Default number of tail lines returned when none is requested (mirrors the reference). */
const DEFAULT_TAIL_LINES = 50;
/**
 * Byte ceiling for the task-output read. The process manager reads only the last
 * `TASK_OUTPUT_READ_BYTES` of the on-disk log (a positioned tail read) instead of
 * loading the whole file into memory on every poll — bounding memory/IO no matter
 * how large the log grew (mirrors the reference's tailFile byte cap). Set well above
 * the default char cap so the char cap, not the byte window, is the effective limit
 * for normal output while still reporting how much earlier output was elided.
 */
const TASK_OUTPUT_READ_BYTES = 512 * 1024;
/**
 * Hard ceiling for a per-call `tail_bytes` byte window. A model can request a
 * larger or smaller on-disk tail read than the default window, but never above
 * this cap — mirrors the reference's DEFAULT_MAX_READ_BYTES (~8MB) upper bound so a
 * single read can't be asked to slurp an unbounded log into memory.
 */
const MAX_READ_BYTES = 8 * 1024 * 1024;

/** Max length of a recognizable task id in an agent-output file path. */
const AGENT_OUTPUT_TASK_ID_MAX = 20;
/** Charset a task id must match to be recognized in a path (mirrors the reference). */
const AGENT_OUTPUT_TASK_ID_RE = /^[a-zA-Z0-9_-]+$/;
const AGENT_OUTPUT_SUFFIX = '.output';

/**
 * PARITY (additive): recognize an agent/background-task output FILE path and extract
 * its task id. Mirrors the reference's `getAgentOutputTaskId` (FileReadTool/UI.tsx):
 * an agent output file follows the layout `…/tasks/<taskId>.output`, and the id is
 * validated as non-empty, at most 20 chars, and `[a-zA-Z0-9_-]` only. AMI writes
 * these as `projectStateDir(cwd,'tasks')/<taskId>.output` (task.ts), so we match a
 * `.output` file living directly under a `tasks/` directory and validate the stem.
 *
 * Returns the task id when the path is a recognized agent-output file, else null —
 * letting a display/summary layer show the compact task id instead of the raw
 * (long, state-dir) path when the model reads a task's transcript via file_read.
 */
export function getAgentOutputTaskId(filePath: unknown): string | null {
  if (typeof filePath !== 'string' || !filePath.endsWith(AGENT_OUTPUT_SUFFIX)) return null;
  const norm = filePath.replace(/\\/g, '/');
  const slash = norm.lastIndexOf('/');
  if (slash < 0) return null;
  const dir = norm.slice(0, slash);
  // The file must sit directly inside a `tasks/` directory.
  if (dir !== 'tasks' && !dir.endsWith('/tasks')) return null;
  const taskId = norm.slice(slash + 1, -AGENT_OUTPUT_SUFFIX.length);
  if (taskId.length > 0 && taskId.length <= AGENT_OUTPUT_TASK_ID_MAX && AGENT_OUTPUT_TASK_ID_RE.test(taskId)) {
    return taskId;
  }
  return null;
}

/**
 * Byte/char cap: truncate from the FRONT (keep the latest tail) and mark it, so a
 * chatty task can't flood the model's context. Shared by the whole-buffer read and
 * the incremental (since_offset) read paths.
 */
function capBody(body: string): string {
  const cap = maxOutputChars();
  if (body.length <= cap) return body;
  return `... [output truncated to the last ${cap} chars]\n` + body.slice(body.length - cap);
}

/** Apply a regex line filter then a tail to a raw chunk of output (incremental path). */
function filterAndTail(chunk: string, re: RegExp | undefined, tail: number): string {
  let lines = chunk.split('\n');
  if (re) lines = lines.filter((line) => re.test(line));
  if (tail > 0) lines = lines.slice(-tail);
  return lines.join('\n');
}

/**
 * PARITY (additive): truncation / total-size indicator with a full-output pointer.
 * the reference's shell-detail view detects that the shown tail is only a window into
 * a larger file (`bytesTotal > content.length`) and renders "Showing N lines of
 * <size>". We do the same for the tool result: when the process manager can report
 * the total bytes written and the emitted tail is smaller than that total, append
 * a quantitative "showing N lines of <total>, <omitted> of earlier output omitted"
 * line plus the on-disk output path so the model knows the view is incomplete and
 * can Read the full log. Skipped when a filter is active (the shown set is a
 * deliberate subset, not a truncation) or when the manager can't introspect size.
 */
function truncationIndicator(
  context: ToolContext,
  taskId: string,
  result: { output: string; totalLinesEstimate?: number },
  filtered: boolean,
): string | undefined {
  if (filtered) return undefined;
  const pm = context.processManager;
  if (!pm || typeof pm.getOutputSize !== 'function') return undefined;
  const shownBody = result.output;
  const bytesTotal = pm.getOutputSize(taskId);
  const shownBytes = Buffer.byteLength(shownBody, 'utf-8');
  if (!(bytesTotal > shownBytes)) return undefined;
  const trimmedBody = shownBody.replace(/\n+$/, '');
  const shownLines = trimmedBody === '' ? 0 : trimmedBody.split('\n').length;
  const omitted = bytesTotal - shownBytes;
  let msg = `Showing ${shownLines} lines (${formatSize(shownBytes)}) of ${formatSize(bytesTotal)} total — tail only, ${formatSize(omitted)} of earlier output omitted`;
  // PARITY (additive): when the process manager extrapolated a total line count
  // from the tail sample (byte-bounded read), surface it so the model has a sense
  // of how many lines exist above the shown window (the reference's progress counter).
  if (typeof result.totalLinesEstimate === 'number' && result.totalLinesEstimate > shownLines) {
    msg += ` (~${result.totalLinesEstimate} lines total)`;
  }
  const outputPath = typeof pm.get === 'function' ? pm.get(taskId)?.outputPath : undefined;
  if (outputPath) msg += `; read ${outputPath} for the full log`;
  return msg + '.';
}

export const taskOutputTool: ToolDefinition = {
  name: 'task_output',
  // PARITY (additive): dedicated tool-search hint + back-compat aliases so a model
  // or an old transcript/SDK caller using the reference's renamed `AgentOutputTool` /
  // `BashOutputTool` names still resolves to this tool.
  searchHint: 'read output/logs from a background task',
  aliases: ['AgentOutputTool', 'BashOutputTool'],
  description:
    'Get status and output from a background task (task/agent/bash). Returns status, exit code (if finished), ' +
    'elapsed time, and the tail of output. BLOCKS by default until the task reaches a terminal state (or the ' +
    'timeout elapses) and returns its final result in a single call — it is read-only, so several task_output ' +
    'calls in one turn run concurrently and all return together when their tasks complete. Pass wait:false for a ' +
    'quick, non-blocking status snapshot of a still-running task. Pass filter to keep only output lines matching a ' +
    'regex, or since_offset to retrieve ONLY the bytes appended since the last read (the response reports the next ' +
    'offset to pass back for a cursor-style incremental tail).',
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
          'Mirrors the reference BashOutput\'s filter. An invalid pattern is reported as an error.',
      },
      since_offset: {
        type: 'number',
        description: 'Byte offset of the last read. When set, only output appended since that offset is returned, ' +
          'and the response reports "Next offset" to pass on the next call for an incremental (since-last-read) tail.',
      },
      wait: {
        type: 'boolean',
        // PARITY: mirror the reference's `block: z.boolean().default(true)` — a
        // task_output call BLOCKS for completion by default. Pass false for a
        // one-shot non-blocking status snapshot of a still-running task.
        description: 'Block until the task reaches a terminal state (completed/failed/killed), then return its final output (default true). Pass false for a quick non-blocking status snapshot.',
      },
      tail_bytes: {
        type: 'number',
        // PARITY (additive): mirror the reference's byte-bounded tail read
        // (ShellDetailDialog reads only the last N bytes / DEFAULT_MAX_READ_BYTES).
        // Lets the model peek at just the last few KB of a huge log, or widen the
        // window, without pulling the whole file. Clamped to [1, MAX_READ_BYTES].
        minimum: 1,
        maximum: MAX_READ_BYTES,
        description: 'Read only the last N BYTES of the on-disk output log (a byte-bounded tail). ' +
          'Defaults to a 512KB window; use a small value (e.g. 8192) for a cheap last-few-KB peek at a ' +
          'large log, applied before the line tail. Clamped to a hard ceiling of 8MB.',
      },
      timeout_ms: {
        type: 'number',
        // PARITY: mirror the reference's `timeout: z.number().min(0).max(600000)`
        // schema bound. The runtime also clamps to this ceiling, but advertising
        // the bound lets the provider/validator reject an out-of-range value up
        // front instead of silently honoring an unbounded wait.
        minimum: 0,
        maximum: MAX_WAIT_TIMEOUT_MS,
        description: 'Max time to block while waiting (default 30000 = 30s). Clamped to a hard ceiling of 600000ms (10 min).',
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
    // FUNCTIONAL (parity): block-by-default. Mirrors the reference's TaskOutputTool
    // `block: semanticBoolean(z.boolean().default(true))` — task_output BLOCKS until
    // the task reaches a terminal state unless the model explicitly opts out with
    // wait:false (a quick non-blocking status poll). A missing/null wait defaults to
    // true; an explicit value (including the provider-serialized string "true"/"false")
    // is coerced via coerceBool as before.
    const wait = input.wait === undefined || input.wait === null ? true : coerceBool(input.wait);

    // FUNCTIONAL (parity): optional regex line filter, mirroring the reference's
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
        const msg = errorMessage(e);
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

    // FUNCTIONAL (parity, additive): optional per-call byte window for the on-disk
    // tail read. Defaults to the standard TASK_OUTPUT_READ_BYTES window; a supplied
    // value is floored to an int and clamped to [1, MAX_READ_BYTES] (a 0/garbage
    // value falls back to the default). Mirrors the reference's byte-bounded tail read.
    const readBytes = coerceNumber(input.tail_bytes, TASK_OUTPUT_READ_BYTES, {
      integer: true,
      min: 1,
      max: MAX_READ_BYTES,
    });

    let result = context.processManager.getOutput(taskId, tail, filterRe, readBytes);
    if (!result) {
      return { output: `Error: No background task found with ID "${taskId}". Use task_list to see all tasks.`, isError: true };
    }

    // Blocking wait: poll internally until terminal or timeout/abort, so the model
    // spends ONE tool call instead of a multi-turn poll loop (each parent turn is
    // a full model round-trip). task_output is read-only, so several wait calls in
    // one turn run concurrently and all settle when their tasks finish.
    let waitAborted = false;
    if (wait && result.status === 'running') {
      const deadline = Date.now() + waitTimeoutMs;
      while (result && result.status === 'running' && Date.now() < deadline) {
        if (context.abortSignal?.aborted) { waitAborted = true; break; }
        // Live progress while blocking so the TUI shows the wait isn't a hang
        // (mirrors the reference's "waiting for task" progress line). Emitted each poll
        // with the task id, status, and elapsed time.
        const secs = (result.elapsedMs / 1000).toFixed(0);
        // The "esc to give additional instructions" affordance mirrors the reference
        // Code's renderToolUseProgressMessage — it tells the user the blocking
        // wait is interruptible (AMI honours esc→abort at L227), instead of the
        // wait looking like an opaque hang.
        context.onProgress?.(`waiting for task ${taskId} (${result.status}, ${secs}s elapsed; esc to give additional instructions)`);
        await new Promise(r => setTimeout(r, WAIT_POLL_INTERVAL_MS));
        result = context.processManager.getOutput(taskId, tail, filterRe, readBytes);
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

    // Introspection marker (parity: the reference's resumeAgent resumed-vs-fresh
    // distinction): tell the model this agent is a RESUMED continuation of a prior
    // session, not a first run, so it can interpret the output/stats accordingly.
    // Defensive typeof guard so a minimal manager stub (tests) is a no-op.
    const taskMeta = typeof context.processManager.get === 'function'
      ? context.processManager.get(taskId)
      : null;
    if (taskMeta?.resumed) lines.push('Invocation: resumed');

    if (filterRe) {
      lines.push(`Filter: ${filterRe} (matching lines only)`);
    }

    // Distinct retrieval status. A wait that returned while still running is NOT a
    // ready result — discriminate the two non-ready causes so the model doesn't
    // mistake a partial read for a completed result:
    //   - aborted: the wait was interrupted (abort signal) before the task finished
    //     (mirrors the reference throwing AbortError from waitForTaskCompletion);
    //   - timed out: the wait deadline elapsed while the task kept running.
    if (waitAborted) {
      lines.push('Retrieval: wait aborted — interrupted before the task reached a terminal state (task still running; output below is a partial snapshot).');
    } else if (wait && result.status === 'running') {
      lines.push(`Retrieval: wait timed out after ${waitTimeoutMs}ms — task still running (call task_output again with a larger timeout_ms to keep waiting, or use task_list to see all tasks).`);
    }

    if (result.exitCode !== null) {
      lines.push(`Exit code: ${result.exitCode}`);
    }

    // FUNCTIONAL (parity): explicit success/failure verdict for a terminal task so
    // a non-zero exit reads as a failure signal rather than just a bare number the
    // model has to interpret. Only emitted once the task has actually finished.
    if (result.status !== 'running' && result.exitCode !== null) {
      if (result.exitCode === 0) {
        lines.push('Result: success');
      } else {
        lines.push(`Result: failure (exit code ${result.exitCode})`);
        // FUNCTIONAL (parity): actionable remediation hint on a failed task
        // (mirrors the reference's failed-task notifications, which point the user at
        // a fallback / retry instead of just reporting the failure). A bare exit
        // code leaves the model to guess the next step; attach a concrete one —
        // point at the full on-disk log when its path is known so the model/user
        // can inspect the whole output, and always suggest re-running.
        lines.push(
          taskMeta?.outputPath
            ? `Remediation: inspect the full output at ${taskMeta.outputPath}, fix the cause, then re-run the command.`
            : 'Remediation: review the output above, fix the cause, then re-run the command.',
        );
      }
    }

    // Structured worker-result USAGE block (parity with the reference's coordinator
    // result envelope, which documents `usage: { total_tokens, tool_uses,
    // duration_ms }`). For a TERMINAL agent task, append a single canonical usage
    // line — duration + total tokens + tool uses — built from the tracked entry's
    // final totals (resultTokenCount/resultToolCount, falling back to the live
    // tokenCount/toolCount counters). Reuses `formatUsageClause` so the usage
    // wording has ONE source shared with the background-completion note (no
    // duplicate formatter). Emitted only when token/tool stats exist, so a bash
    // task (no AI usage) does not get a line that merely restates Elapsed.
    if (result.status !== 'running' && taskMeta) {
      const usageTokens = taskMeta.resultTokenCount ?? taskMeta.tokenCount;
      const usageTools = taskMeta.resultToolCount ?? taskMeta.toolCount;
      if (typeof usageTokens === 'number' || typeof usageTools === 'number') {
        const clause = formatUsageClause({
          durationMs: result.elapsedMs,
          tokenCount: usageTokens,
          toolCount: usageTools,
        }).trim();
        if (clause) lines.push(clause);
      }
    }

    // Dedup (parity with the reference's TaskOutputTool flipping `task.notified` on a
    // successful read): once the model has consumed a TERMINAL task's output, latch
    // it so the engine's completion-drain does not re-announce a result the model
    // already has (which would invite a wasteful re-poll). Running tasks are left
    // un-notified so their eventual completion is still surfaced. Defensive typeof
    // guard so a minimal manager stub (tests) is a no-op.
    if (result.status !== 'running' && typeof context.processManager.markNotified === 'function') {
      context.processManager.markNotified(taskId);
    }

    // Clean final-answer preference (parity: the reference's TaskOutputTool returning
    // the agent's in-memory final answer over the raw transcript). When a TERMINAL
    // agent task has written a `.result` sidecar — its last text-bearing turn only —
    // hand THAT back as the deliverable instead of the noisy streamed transcript, and
    // point at the full log for drill-down. Skipped for a filter / incremental read
    // (the caller asked for specific transcript lines, not the summary) and for a
    // bash task (which never writes the sidecar). Best-effort: a read failure falls
    // through to the normal transcript tail.
    if (!filterRe && !hasSince && result.status !== 'running' && taskMeta?.outputPath) {
      try {
        const clean = fs.readFileSync(agentResultPath(taskMeta.outputPath), 'utf-8');
        if (clean.trim()) {
          lines.push('', '--- Final result ---', capBody(clean.trim()));
          lines.push(`(Clean final answer; the full streamed transcript is at ${taskMeta.outputPath}.)`);
          return { output: lines.join('\n'), isError: false };
        }
      } catch { /* no sidecar / unreadable — fall through to the transcript tail */ }
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
      const indicator = truncationIndicator(context, taskId, result, filterRe !== undefined);
      if (indicator) lines.push(indicator);
    } else {
      lines.push('', filterRe ? '(no output matching filter yet)' : '(no output yet)');
    }

    // FUNCTIONAL (parity with the reference's UserLocalCommandOutputMessage keeping
    // stderr a DISTINCT, separately-labelled block): surface the task's stderr in its
    // own section so a command whose only output is a diagnostic on stderr is still
    // visible and distinguishable from stdout (AMI's merged output file alone cannot
    // tell them apart). Only when non-empty, and only for the whole-buffer read.
    // Defensive typeof guard so a minimal manager stub (tests) is a no-op.
    if (typeof context.processManager.getStderr === 'function') {
      const stderr = context.processManager.getStderr(taskId, tail > 0 ? tail : 0).trim();
      if (stderr) {
        lines.push('', '--- stderr ---', capBody(stderr));
      }
    }

    // outputFileRedundant optimization (parity with the reference's TaskOutput
    // deleting the on-disk copy when the whole output already fit inline into the
    // tool_result): a TERMINAL task whose ENTIRE output was read here (bytesOmitted
    // === 0 — nothing was tail-truncated) no longer needs its duplicate on-disk
    // file; the model already holds every byte above. Drop it so small background
    // outputs don't accumulate on disk. Skipped for a filtered/incremental read
    // (the caller read a subset, not the full inline output). Defensive typeof
    // guard so a minimal manager stub (tests) is a no-op.
    if (
      !filterRe && !hasSince && result.status !== 'running' &&
      result.bytesOmitted === 0 &&
      typeof context.processManager.deleteRedundantOutput === 'function'
    ) {
      context.processManager.deleteRedundantOutput(taskId, result.bytesOmitted);
    }

    return { output: lines.join('\n'), isError: false };
  },
};
