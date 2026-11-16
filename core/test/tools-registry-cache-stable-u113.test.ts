import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { ToolRegistry } from '../src/tools/index';
import type { ToolDefinition } from '../src/types';

// U113 — parity for core/core/src/tools/index.ts:
//  1. Cache-stable tool ordering when merging built-in + MCP/installed tools
//     (Claude's tools.ts:assembleToolPool): built-ins sorted as a contiguous
//     prefix, MCP/installed tools sorted as a separate suffix partition — so the
//     prompt cache breakpoint after the last built-in stays stable regardless of
//     MCP insertion order.
//  2. Built-ins-win deduplication on a cross-source tool-name collision
//     (Claude's uniqBy(builtInsFirst, 'name')): a plugin/MCP tool must NOT
//     overwrite an incumbent built-in of the same name.

function mkTool(name: string): ToolDefinition {
  return {
    name,
    description: `tool ${name}`,
    inputSchema: { type: 'object', properties: {}, required: [] },
    isReadOnly: true,
    isConcurrencySafe: true,
    execute: async () => ({ output: 'ok', isError: false }),
  } as ToolDefinition;
}

describe('U113 — ToolRegistry cache-stable assembly (assembleToolPool parity)', () => {
  it('keeps built-ins as a sorted contiguous prefix and sorts MCP tools as a suffix', () => {
    const registry = new ToolRegistry();
    // Register out of alphabetical order + interleave built-in / MCP sources.
    registry.register(mkTool('delta'), 'core');
    registry.register(mkTool('mcp__srv__zeta'), 'mcp');
    registry.register(mkTool('alpha'), 'core');
    registry.register(mkTool('mcp__srv__beta'), 'mcp');
    registry.register(mkTool('charlie'), 'core');

    const order = registry.assembleToolPool().map((t) => t.name);
    // Built-ins (core) sorted first, then MCP/installed sorted.
    assert.deepEqual(order, [
      'alpha',
      'charlie',
      'delta',
      'mcp__srv__beta',
      'mcp__srv__zeta',
    ]);
  });

  it('is insertion-order independent (prompt-cache stable)', () => {
    const a = new ToolRegistry();
    a.register(mkTool('bash'), 'core');
    a.register(mkTool('mcp__x'), 'mcp');
    a.register(mkTool('glob'), 'core');

    const b = new ToolRegistry();
    b.register(mkTool('mcp__x'), 'mcp');
    b.register(mkTool('glob'), 'core');
    b.register(mkTool('bash'), 'core');

    assert.deepEqual(
      a.assembleToolPool().map((t) => t.name),
      b.assembleToolPool().map((t) => t.name),
    );
  });

  it('toOpenAIFormat uses the cache-stable ordering', () => {
    const registry = new ToolRegistry();
    registry.register(mkTool('zzz'), 'core');
    registry.register(mkTool('mcp__srv__a'), 'mcp');
    registry.register(mkTool('aaa'), 'core');

    const names = registry.toOpenAIFormat().map((f) => f.function.name);
    assert.deepEqual(names, ['aaa', 'zzz', 'mcp__srv__a']);
  });
});

describe('U113 — ToolRegistry built-ins-win on cross-source collision', () => {
  it('keeps the incumbent built-in and skips a same-name MCP tool', () => {
    const registry = new ToolRegistry();
    const builtIn = mkTool('bash');
    builtIn.description = 'builtin-bash';
    registry.register(builtIn, 'core');

    const impostor = mkTool('bash');
    impostor.description = 'mcp-bash';
    registry.register(impostor, 'mcp');

    assert.equal(registry.get('bash')!.description, 'builtin-bash');
    assert.equal(registry.getAll().length, 1);
    // The collision is surfaced as a conflict, not silently dropped.
    assert.ok(registry.getConflicts().some((c) => c.toolName === 'bash'));
  });

  it('still allows same-source re-registration to update the tool', () => {
    const registry = new ToolRegistry();
    const v1 = mkTool('dup');
    v1.description = 'v1';
    const v2 = mkTool('dup');
    v2.description = 'v2';
    registry.register(v1, 'core');
    registry.register(v2, 'core');
    assert.equal(registry.get('dup')!.description, 'v2');
    assert.equal(registry.getAll().length, 1);
  });
});
