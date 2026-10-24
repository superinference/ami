import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  detectToolConflict,
  isReservedToolName,
  ToolRegistryGuard,
} from '../src/tools/tool-registry-guard';

// These tests target the ternary `prefix.endsWith('_') ? prefix : prefix + '_'`
// inside detectToolConflict. Existing tests only exercise this ternary's
// true-branch via isReservedToolName; here we drive it through the guard's
// conflict detection so the branch is covered where it actually gates security.

describe('detectToolConflict — underscore-terminated reserved prefixes', () => {
  it('blocks a non-core tool whose name starts with the "task_" prefix', () => {
    const registry = new Map<string, string>();
    const conflict = detectToolConflict('task_spawn', 'user-plugin', registry);
    assert.ok(conflict !== null);
    // boundary for "task_" is "task_" itself (already ends with _), so
    // "task_spawn".startsWith("task_") must trip the reserved-prefix guard.
    assert.ok(conflict.reason.includes('reserved prefix "task_"'));
    assert.equal(conflict.existingSource, 'core');
    assert.equal(conflict.conflictingSource, 'user-plugin');
  });

  it('blocks a non-core tool whose name starts with the "agent_" prefix', () => {
    const registry = new Map<string, string>();
    const conflict = detectToolConflict('agent_run', 'third-party', registry);
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('reserved prefix "agent_"'));
  });

  it('blocks a non-core tool whose name is exactly an underscore prefix', () => {
    // Exercises the `toolName === prefix` arm for an already-underscored prefix,
    // where boundary === prefix and the startsWith arm would also match.
    const registry = new Map<string, string>();
    const conflict = detectToolConflict('task_', 'user-plugin', registry);
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('reserved prefix "task_"'));
  });

  it('does NOT block a name that merely contains "task_" but does not start with it', () => {
    // Red-green: if the guard used includes() instead of startsWith(), this
    // would wrongly be blocked. Asserting null pins the boundary semantics.
    const registry = new Map<string, string>();
    assert.equal(detectToolConflict('my_task_helper', 'user-plugin', registry), null);
  });

  it('does NOT block "taskmanager" (no underscore boundary after task_)', () => {
    const registry = new Map<string, string>();
    assert.equal(detectToolConflict('taskmanager', 'user-plugin', registry), null);
    assert.equal(detectToolConflict('agentic_flow', 'user-plugin', registry), null);
  });

  it('allows core/builtin to use the underscore-prefixed reserved names', () => {
    const registry = new Map<string, string>();
    assert.equal(detectToolConflict('task_spawn', 'core', registry), null);
    assert.equal(detectToolConflict('agent_run', 'builtin', registry), null);
  });
});

describe('ToolRegistryGuard — underscore reserved prefixes through register()', () => {
  it('records a conflict when a plugin registers a task_ tool', () => {
    const guard = new ToolRegistryGuard();
    const conflict = guard.register('task_delegate', 'user-plugin');
    assert.ok(conflict !== null);
    assert.equal(guard.isRegistered('task_delegate'), false);
    assert.equal(guard.getConflicts().length, 1);
  });
});

describe('isReservedToolName — boundary for underscore prefixes', () => {
  it('matches exact underscore prefix but not a longer word fragment', () => {
    assert.equal(isReservedToolName('task_'), true);
    assert.equal(isReservedToolName('agent_'), true);
    // no underscore boundary -> not reserved
    assert.equal(isReservedToolName('tasked'), false);
    assert.equal(isReservedToolName('agented'), false);
  });
});
