import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskKillTool, type TaskKillMetadata } from '../src/tools/task-kill';
import { toolDefinitionsToOpenAI } from '../src/provider';
import type { ToolContext } from '../src/types';

/**
 * U209 — PARITY (additive): task_kill declares a machine-readable
 * `outputSchema` describing its structured stop result, mirroring the reference's
 * TaskStopTool `outputSchema` (message / task_id / task_type / command fields).
 *
 * The schema describes exactly the fields AMI already carries on
 * `ToolResult.metadata` (`TaskKillMetadata`): a non-LLM caller / resume path can
 * consume the structured outcome without parsing the human message string, and
 * the provider advertises it (object-root-only guard in toolDefinitionsToOpenAI).
 *
 * Red-green: with no `outputSchema` on the tool these assertions fail; adding it
 * (and only an object-rooted one flows through the provider) turns them green.
 */

function mockCtx(pm: Record<string, unknown>): ToolContext {
  return { cwd: '/tmp', processManager: pm } as unknown as ToolContext;
}

describe('U209 — task_kill structured outputSchema (parity)', () => {
  it('declares an object-rooted outputSchema with the structured stop fields', () => {
    const schema = taskKillTool.outputSchema;
    assert.ok(schema, 'task_kill declares outputSchema');
    assert.equal(schema!.type, 'object');
    const props = schema!.properties as Record<string, unknown>;
    for (const key of ['reason', 'taskId', 'taskType', 'kind', 'command', 'status', 'pid', 'exitCode']) {
      assert.ok(key in props, `outputSchema declares ${key}`);
    }
    // `reason` is the stable discriminator present on every branch, so it is required.
    assert.deepEqual(schema!.required, ['reason']);
  });

  it('flows through the provider exposure path (object-root guard)', () => {
    const [out] = toolDefinitionsToOpenAI([taskKillTool]);
    assert.deepEqual(out.function.outputSchema, taskKillTool.outputSchema);
  });

  it('every metadata field a real execute() returns is declared in the schema (no undeclared field)', async () => {
    const props = taskKillTool.outputSchema!.properties as Record<string, unknown>;
    const ctx = mockCtx({
      get: () => ({ status: 'running', pid: 4242, command: 'sleep 999' }),
      kill: () => true,
      taskType: () => 'bash',
      taskKind: () => 'bash',
    });
    const r = await taskKillTool.execute!({ task_id: 't' }, ctx);
    const m = r.metadata as unknown as TaskKillMetadata;
    assert.ok(m, 'result carries metadata');
    for (const key of Object.keys(m)) {
      assert.ok(key in props, `metadata key "${key}" must be declared in outputSchema`);
    }
  });
});
