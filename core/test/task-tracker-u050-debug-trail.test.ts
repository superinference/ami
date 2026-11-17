import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { taskTrackerTool, resetTaskState } from '../src/tools/task-tracker';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U050 gap — lifecycle debug introspection trail for task pickup/claim/release.
//
// Claude Code's task watcher emits a logForDebugging trail ('Found available task
// #N', 'Failed to claim task #N: <reason>', 'releasing claim') so an operator can
// introspect why an autonomous/teammate agent did or did not pick up a task. AMI's
// claim/release/owner mutations were silent. The trail is routed through the shared
// `log()` channel; here we capture it via the `AMI_DEBUG_TO_STDERR` route (which
// streams every log line to stderr and bypasses the file writer) and assert the
// lifecycle transitions appear.
// ---------------------------------------------------------------------------

function ctx(over: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...over,
  } as ToolContext;
}

/** Run `fn` with stderr captured and AMI_DEBUG_TO_STDERR forced on. */
async function captureTrail(fn: () => Promise<void>): Promise<string> {
  const prev = process.env.AMI_DEBUG_TO_STDERR;
  process.env.AMI_DEBUG_TO_STDERR = '1';
  const originalWrite = process.stderr.write.bind(process.stderr);
  let buf = '';
  (process.stderr as NodeJS.WriteStream).write = ((chunk: string | Uint8Array): boolean => {
    buf += typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf-8');
    return true;
  }) as typeof process.stderr.write;
  try {
    await fn();
  } finally {
    (process.stderr as NodeJS.WriteStream).write = originalWrite;
    if (prev === undefined) delete process.env.AMI_DEBUG_TO_STDERR;
    else process.env.AMI_DEBUG_TO_STDERR = prev;
  }
  return buf;
}

beforeEach(() => resetTaskState());
afterEach(() => resetTaskState());

describe('task_tracker — claim/release/pickup debug trail (U050)', () => {
  it('logs "Found available task" on next and "Claimed task" on a successful claim', async () => {
    const trail = await captureTrail(async () => {
      await taskTrackerTool.execute({ action: 'create', subject: 'build the widget' }, ctx());
      await taskTrackerTool.execute({ action: 'next' }, ctx());
      await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'alice' }, ctx());
    });
    assert.match(trail, /\[task-tracker\]/);
    assert.match(trail, /Found available task #1: build the widget/);
    assert.match(trail, /Claimed task #1: build the widget/);
  });

  it('logs "Failed to claim task" with the structured reason on a blocked/already-claimed claim', async () => {
    const trail = await captureTrail(async () => {
      await taskTrackerTool.execute({ action: 'create', subject: 'first' }, ctx());
      await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'alice' }, ctx());
      // Second claim must fail (already in_progress) and log the reason.
      await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'bob' }, ctx());
      // A missing id must fail with task_not_found and log it.
      await taskTrackerTool.execute({ action: 'claim', task_id: '999' }, ctx());
    });
    assert.match(trail, /Failed to claim task #1: already_claimed/);
    assert.match(trail, /Failed to claim task #999: task_not_found/);
  });

  it('logs "Releasing claim" when a claimed task is released', async () => {
    const trail = await captureTrail(async () => {
      await taskTrackerTool.execute({ action: 'create', subject: 'later' }, ctx());
      await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'alice' }, ctx());
      await taskTrackerTool.execute({ action: 'release', task_id: '1' }, ctx());
    });
    assert.match(trail, /Releasing claim on task #1: later/);
  });

  it('logs "No available task" when next finds nothing claimable', async () => {
    const trail = await captureTrail(async () => {
      await taskTrackerTool.execute({ action: 'next', owner: 'nobody' }, ctx());
    });
    assert.match(trail, /No available task to pick up/);
  });
});
