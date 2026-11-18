/**
 * Unit + integration tests for the session-scoped provider tool-schema cache
 * (src/tools/tool-schema-cache.ts) and its two wired consumers:
 *   - convertToolsForSDK (src/provider.ts) — byte-stable per-tool SDK schema;
 *   - McpManager.invalidateToolCache (src/mcp/manager.ts) — clear-on-reconnect hook.
 *
 * Parity with Claude Code's utils/toolSchemaCache.ts: the rendered tool-schema
 * array sits at server position 2, so its bytes must stay stable across a session
 * (MCP reconnects hand us a fresh ToolDefinition object; a server can re-advertise
 * a key-reordered-but-equal schema) — freezing them at first render keeps the
 * provider prompt-cache prefix intact. AMI's previous identity-keyed WeakMap missed
 * both cases; this cache keys by name + stable schema instead.
 */

import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  toolSchemaCacheKey,
  getToolSchemaCache,
  clearToolSchemaCache,
  memoizeToolSchema,
  toolSchemaCacheStats,
} from '../src/tools/tool-schema-cache';
import { convertToolsForSDK } from '../src/provider';
import { McpManager } from '../src/mcp/manager';
import type { ToolDefinition, ToolInputSchema } from '../src/types';

function makeTool(name: string, description: string, schema?: ToolInputSchema): ToolDefinition {
  return {
    name,
    description,
    inputSchema: schema ?? {
      type: 'object',
      properties: { value: { type: 'string', description: 'A value' } },
      required: ['value'],
    },
    isReadOnly: true,
    execute: async () => ({ output: 'ok' }),
  };
}

describe('tool-schema-cache: toolSchemaCacheKey', () => {
  it('keys by bare name when no schema is supplied', () => {
    assert.equal(toolSchemaCacheKey('bash'), 'bash');
  });

  it('includes the schema so same-name tools with different schemas never collide', () => {
    const a = toolSchemaCacheKey('t', { type: 'object', properties: { a: { type: 'string' } } });
    const b = toolSchemaCacheKey('t', { type: 'object', properties: { b: { type: 'number' } } });
    assert.notEqual(a, b);
  });

  it('is key-order insensitive — a reordered-but-equal schema maps to the SAME key', () => {
    const k1 = toolSchemaCacheKey('t', {
      type: 'object',
      properties: { a: { type: 'string' }, b: { type: 'number' } },
      required: ['a', 'b'],
    });
    const k2 = toolSchemaCacheKey('t', {
      // same content, different property insertion order
      properties: { b: { type: 'number' }, a: { type: 'string' } },
      required: ['a', 'b'],
      type: 'object',
    } as ToolInputSchema);
    assert.equal(k1, k2);
  });
});

describe('tool-schema-cache: memoizeToolSchema', () => {
  beforeEach(() => clearToolSchemaCache());

  it('renders once and reuses the frozen value across DISTINCT objects (MCP reconnect)', () => {
    let renders = 0;
    const render = () => { renders++; return { tag: 'frozen' }; };

    const first = makeTool('mcp_tool', 'desc');
    const r1 = memoizeToolSchema(first, render);
    // A reconnect re-creates the tool as a brand-new object with the same name+schema.
    const reconnected = makeTool('mcp_tool', 'desc');
    const r2 = memoizeToolSchema(reconnected, render);

    assert.equal(renders, 1, 'render runs once despite a new object identity');
    assert.strictEqual(r1, r2, 'same frozen entry is reused — bytes stay stable');
  });

  it('reuses the frozen value for a key-reordered-but-equal schema', () => {
    let renders = 0;
    const render = () => { renders++; return {}; };
    const ordered = makeTool('t', 'd', {
      type: 'object',
      properties: { a: { type: 'string' }, b: { type: 'number' } },
    });
    const reordered = makeTool('t', 'd', {
      properties: { b: { type: 'number' }, a: { type: 'string' } },
      type: 'object',
    } as ToolInputSchema);
    memoizeToolSchema(ordered, render);
    memoizeToolSchema(reordered, render);
    assert.equal(renders, 1);
  });

  it('re-renders a different name or schema (distinct keys)', () => {
    let renders = 0;
    const render = () => { renders++; return {}; };
    memoizeToolSchema(makeTool('a', 'd'), render);
    memoizeToolSchema(makeTool('b', 'd'), render);
    assert.equal(renders, 2);
  });

  it('getToolSchemaCache + toolSchemaCacheStats reflect the frozen set; clear empties it', () => {
    memoizeToolSchema(makeTool('x', 'd'), () => ({}));
    memoizeToolSchema(makeTool('y', 'd'), () => ({}));
    assert.equal(toolSchemaCacheStats().size, 2);
    assert.equal(getToolSchemaCache().size, 2);
    clearToolSchemaCache();
    assert.equal(toolSchemaCacheStats().size, 0);
  });
});

describe('tool-schema-cache: convertToolsForSDK is byte-stable across object re-creation', () => {
  beforeEach(() => clearToolSchemaCache());

  // RED-GREEN: with the old identity-keyed WeakMap, two separate ToolDefinition
  // objects (same name + schema) produced two DISTINCT SDK entries — reverting the
  // name-keyed cache makes `entryA === entryB` false and this test fails.
  it('returns the SAME entry object for a re-created tool with identical name+schema', () => {
    const entryA = convertToolsForSDK([makeTool('bash', 'Run a command')]).bash;
    const entryB = convertToolsForSDK([makeTool('bash', 'Run a command')]).bash;
    assert.ok(entryA && entryB);
    assert.strictEqual(entryA, entryB, 'frozen SDK entry reused across distinct objects');
  });

  it('still produces a fresh entry after the cache is cleared', () => {
    const before = convertToolsForSDK([makeTool('grep', 'Search')]).grep;
    clearToolSchemaCache();
    const after = convertToolsForSDK([makeTool('grep', 'Search')]).grep;
    assert.notStrictEqual(before, after, 'clear forces a re-render');
  });
});

describe('tool-schema-cache: McpManager clears the cache on tool invalidation', () => {
  beforeEach(() => clearToolSchemaCache());

  // RED-GREEN: reverting the clearToolSchemaCache() call in invalidateToolCache
  // leaves the frozen schema in place after a server lifecycle event, so the size
  // assertion of 0 fails.
  it('removeServer drops the session tool-schema cache (reconnect re-render hook)', () => {
    const mgr = new McpManager();
    // Seed the cache as if a turn had rendered tool schemas.
    memoizeToolSchema(makeTool('mcp__srv__do', 'desc'), () => ({}));
    assert.equal(toolSchemaCacheStats().size, 1);
    mgr.removeServer('srv');
    assert.equal(toolSchemaCacheStats().size, 0, 'schema cache cleared on tool invalidation');
  });
});
