import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { toolDefinitionsToOpenAI } from '../src/provider';
import { exitWorktreeTool } from '../src/tools/exit-worktree';
import type { ToolDefinition } from '../src/types';

// ---------------------------------------------------------------------------
// U006 — Per-tool outputSchema exposure with an object-root-only guard. A
// ToolDefinition.outputSchema is advertised in the OpenAI tool payload only when
// its root type is exactly `object` (parity: the reference's mcp.ts). Non-object /
// absent outputSchema is dropped, never emitted.
//
// Red-green: revert the `tool.outputSchema.type === 'object'` guard in
// toolDefinitionsToOpenAI and a non-object outputSchema leaks through (or absence
// stops being undefined) — these assertions fail.
// ---------------------------------------------------------------------------

function tool(partial: Partial<ToolDefinition>): ToolDefinition {
  return {
    name: 'x',
    description: 'desc',
    inputSchema: { type: 'object', properties: {} },
    execute: async () => ({ output: '' }),
    ...partial,
  } as ToolDefinition;
}

describe('U006 — toolDefinitionsToOpenAI outputSchema exposure guard', () => {
  it('emits outputSchema for an object-rooted schema', () => {
    const t = tool({
      outputSchema: { type: 'object', properties: { ok: { type: 'boolean' } }, required: ['ok'] },
    });
    const [out] = toolDefinitionsToOpenAI([t]);
    assert.deepEqual(out.function.outputSchema, t.outputSchema);
  });

  it('omits outputSchema when the tool declares none', () => {
    const [out] = toolDefinitionsToOpenAI([tool({})]);
    assert.equal(out.function.outputSchema, undefined);
    // base fields still emitted
    assert.equal(out.function.name, 'x');
    assert.deepEqual(out.function.parameters, { type: 'object', properties: {} });
  });

  it('drops a non-object-rooted outputSchema (guard)', () => {
    // A malformed/external outputSchema whose root is not `object` must not be exposed.
    const t = tool({ outputSchema: { type: 'string' } as unknown as ToolDefinition['outputSchema'] });
    const [out] = toolDefinitionsToOpenAI([t]);
    assert.equal(out.function.outputSchema, undefined);
  });

  it('exit_worktree declares a valid object-rooted structured outputSchema (real consumer)', () => {
    assert.ok(exitWorktreeTool.outputSchema, 'exit_worktree declares outputSchema');
    assert.equal(exitWorktreeTool.outputSchema!.type, 'object');
    const props = exitWorktreeTool.outputSchema!.properties;
    for (const key of ['action', 'originalCwd', 'worktreePath', 'worktreeBranch', 'discardedFiles', 'discardedCommits', 'message']) {
      assert.ok(key in props, `outputSchema declares ${key}`);
    }
    assert.deepEqual(exitWorktreeTool.outputSchema!.required, ['action', 'originalCwd', 'worktreePath', 'message']);
    // And it flows through the provider exposure path end to end.
    const [out] = toolDefinitionsToOpenAI([exitWorktreeTool]);
    assert.deepEqual(out.function.outputSchema, exitWorktreeTool.outputSchema);
  });
});
