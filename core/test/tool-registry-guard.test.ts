import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  detectToolConflict,
  isReservedToolName,
  RESERVED_TOOL_PREFIXES,
  ToolRegistryGuard,
} from '../src/tools/tool-registry-guard';

describe('detectToolConflict', () => {
  it('returns null for a new tool with an empty registry', () => {
    const registry = new Map<string, string>();
    assert.equal(detectToolConflict('my_tool', 'plugin-a', registry), null);
  });

  it('detects duplicate registration from a different source', () => {
    const registry = new Map<string, string>([['my_tool', 'plugin-a']]);
    const conflict = detectToolConflict('my_tool', 'plugin-b', registry);
    assert.ok(conflict !== null);
    assert.equal(conflict.toolName, 'my_tool');
    assert.equal(conflict.existingSource, 'plugin-a');
    assert.equal(conflict.conflictingSource, 'plugin-b');
  });

  it('allows same source to re-register', () => {
    const registry = new Map<string, string>([['my_tool', 'plugin-a']]);
    assert.equal(detectToolConflict('my_tool', 'plugin-a', registry), null);
  });

  it('detects reserved prefix conflict for non-core source', () => {
    const registry = new Map<string, string>();
    const conflict = detectToolConflict('bash', 'user-plugin', registry);
    assert.ok(conflict !== null);
    assert.equal(conflict.reason.includes('reserved prefix'), true);
  });

  it('detects reserved prefix conflict for prefixed name', () => {
    const registry = new Map<string, string>();
    const conflict = detectToolConflict('file_read_extended', 'user-plugin', registry);
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('file_read'));
  });

  it('allows core source to use reserved prefix', () => {
    const registry = new Map<string, string>();
    assert.equal(detectToolConflict('bash', 'core', registry), null);
  });

  it('allows builtin source to use reserved prefix', () => {
    const registry = new Map<string, string>();
    assert.equal(detectToolConflict('file_edit', 'builtin', registry), null);
  });

  it('allows non-core source to use unrelated names', () => {
    const registry = new Map<string, string>();
    assert.equal(detectToolConflict('my_custom_tool', 'user-plugin', registry), null);
  });

  it('duplicate check takes priority over prefix check', () => {
    const registry = new Map<string, string>([['bash', 'plugin-a']]);
    const conflict = detectToolConflict('bash', 'plugin-b', registry);
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('already registered'));
  });
});

describe('isReservedToolName', () => {
  it('recognizes exact match', () => {
    assert.equal(isReservedToolName('bash'), true);
    assert.equal(isReservedToolName('grep'), true);
    assert.equal(isReservedToolName('glob'), true);
    assert.equal(isReservedToolName('file_read'), true);
  });

  it('recognizes prefixed match with underscore', () => {
    assert.equal(isReservedToolName('task_create'), true);
    assert.equal(isReservedToolName('agent_spawn'), true);
    assert.equal(isReservedToolName('file_edit_multi'), true);
  });

  it('returns false for unrelated names', () => {
    assert.equal(isReservedToolName('my_tool'), false);
    assert.equal(isReservedToolName('custom_grep_like'), false);
    assert.equal(isReservedToolName('search'), false);
  });

  it('returns false for partial matches without underscore boundary', () => {
    assert.equal(isReservedToolName('bashing'), false);
    assert.equal(isReservedToolName('grepper'), false);
    assert.equal(isReservedToolName('globbing'), false);
  });
});

describe('RESERVED_TOOL_PREFIXES', () => {
  it('contains expected entries', () => {
    const prefixes = [...RESERVED_TOOL_PREFIXES];
    assert.ok(prefixes.includes('bash'));
    assert.ok(prefixes.includes('file_read'));
    assert.ok(prefixes.includes('file_write'));
    assert.ok(prefixes.includes('file_edit'));
    assert.ok(prefixes.includes('grep'));
    assert.ok(prefixes.includes('glob'));
    assert.ok(prefixes.includes('web_fetch'));
    assert.ok(prefixes.includes('web_search'));
    assert.ok(prefixes.includes('notebook_edit'));
    assert.ok(prefixes.includes('task_'));
    assert.ok(prefixes.includes('agent_'));
  });
});

describe('ToolRegistryGuard', () => {
  let guard: ToolRegistryGuard;

  beforeEach(() => {
    guard = new ToolRegistryGuard();
  });

  it('register succeeds for a new tool', () => {
    const conflict = guard.register('my_tool', 'plugin-a');
    assert.equal(conflict, null);
  });

  it('register rejects duplicate from different source', () => {
    guard.register('my_tool', 'plugin-a');
    const conflict = guard.register('my_tool', 'plugin-b');
    assert.ok(conflict !== null);
    assert.equal(conflict.toolName, 'my_tool');
  });

  it('register rejects reserved prefix for non-core source', () => {
    const conflict = guard.register('bash', 'user-plugin');
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('reserved prefix'));
  });

  it('isRegistered returns true after register', () => {
    guard.register('my_tool', 'plugin-a');
    assert.equal(guard.isRegistered('my_tool'), true);
  });

  it('isRegistered returns false for unknown tool', () => {
    assert.equal(guard.isRegistered('unknown'), false);
  });

  it('getSource returns correct source', () => {
    guard.register('my_tool', 'plugin-a');
    assert.equal(guard.getSource('my_tool'), 'plugin-a');
  });

  it('getSource returns undefined for unknown tool', () => {
    assert.equal(guard.getSource('unknown'), undefined);
  });

  it('getConflicts tracks all conflicts', () => {
    guard.register('my_tool', 'plugin-a');
    guard.register('my_tool', 'plugin-b');
    guard.register('bash', 'user-plugin');
    const conflicts = guard.getConflicts();
    assert.equal(conflicts.length, 2);
  });

  it('getRegisteredTools returns a copy of the registry', () => {
    guard.register('tool_a', 'src-a');
    guard.register('tool_b', 'src-b');
    const tools = guard.getRegisteredTools();
    assert.equal(tools.size, 2);
    assert.equal(tools.get('tool_a'), 'src-a');
    tools.set('tool_c', 'src-c');
    assert.equal(guard.isRegistered('tool_c'), false);
  });

  it('clear resets everything', () => {
    guard.register('my_tool', 'plugin-a');
    guard.register('my_tool', 'plugin-b');
    guard.clear();
    assert.equal(guard.isRegistered('my_tool'), false);
    assert.equal(guard.getConflicts().length, 0);
  });

  it('rejected tool is not added to registry', () => {
    guard.register('my_tool', 'plugin-a');
    guard.register('my_tool', 'plugin-b');
    assert.equal(guard.getSource('my_tool'), 'plugin-a');
  });

  it('multiple registrations from same source succeed', () => {
    assert.equal(guard.register('tool_1', 'plugin-a'), null);
    assert.equal(guard.register('tool_2', 'plugin-a'), null);
    assert.equal(guard.register('tool_3', 'plugin-a'), null);
    assert.equal(guard.getRegisteredTools().size, 3);
  });

  it('re-registering same tool and source succeeds', () => {
    guard.register('my_tool', 'plugin-a');
    assert.equal(guard.register('my_tool', 'plugin-a'), null);
    assert.equal(guard.getConflicts().length, 0);
  });
});
