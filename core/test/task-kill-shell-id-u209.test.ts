import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskKillTool } from '../src/tools/task-kill';
import type { ToolContext } from '../src/types';

/**
 * PARITY (U209): the deprecated `shell_id` input alias (the reference's KillShell
 * backward-compat parameter). Claude's TaskStop accepts both `task_id` and a
 * deprecated `shell_id` and coalesces `id = task_id ?? shell_id`, so an old
 * KillShell-style transcript/SDK call keeps resolving. AMI must do the same.
 *
 * upstreamRef: references/claude/claude-code-main/src/tools/TaskStopTool/TaskStopTool.ts:L10-L18,L60-63
 */
describe('taskKillTool — shell_id deprecated alias (U209)', () => {
  const runningManager = (seen: { id?: string } = {}) =>
    ({
      cwd: '/tmp',
      processManager: {
        get: (id: string) => {
          seen.id = id;
          return id === 'bg-7' ? { status: 'running', pid: 4242, command: 'npm run dev' } : undefined;
        },
        kill: () => true,
      },
    }) as unknown as ToolContext;

  it('advertises shell_id as an optional input property', () => {
    const props = taskKillTool.inputSchema.properties as Record<string, unknown>;
    assert.ok(props.shell_id, 'shell_id should be declared in the inputSchema');
    // It is a deprecated alias, never a required field.
    assert.ok(!(taskKillTool.inputSchema.required ?? []).includes('shell_id'));
  });

  it('accepts shell_id without a strict-schema unknown-key rejection', async () => {
    const ctx = runningManager();
    const r = await taskKillTool.execute!({ shell_id: 'bg-7' }, ctx);
    assert.ok(!r.isError, `shell_id-only call should succeed, got: ${r.output}`);
    assert.ok(r.output.includes('killed successfully'));
    assert.equal((r.metadata as { reason?: string } | undefined)?.reason, 'killed');
    assert.equal((r.metadata as { taskId?: string } | undefined)?.taskId, 'bg-7');
  });

  it('coalesces id = task_id ?? shell_id (task_id wins when both present)', async () => {
    const seen: { id?: string } = {};
    const ctx = runningManager(seen);
    // task_id resolves bg-7; shell_id (ignored) points elsewhere.
    const r = await taskKillTool.execute!({ task_id: 'bg-7', shell_id: 'other' }, ctx);
    assert.ok(!r.isError);
    assert.equal(seen.id, 'bg-7', 'task_id must take precedence over shell_id');
  });

  it('rejects a call with neither task_id nor shell_id as invalid_input', async () => {
    const ctx = runningManager();
    const r = await taskKillTool.execute!({}, ctx);
    assert.ok(r.isError);
    assert.match(r.output, /\[code: invalid_input\]/);
    assert.equal((r.metadata as { reason?: string } | undefined)?.reason, 'invalid_input');
  });

  it('rejects a non-string shell_id as invalid_input', async () => {
    const ctx = runningManager();
    const r = await taskKillTool.execute!({ shell_id: 123 as unknown as string }, ctx);
    assert.ok(r.isError);
    assert.match(r.output, /\[code: invalid_input\]/);
  });
});
