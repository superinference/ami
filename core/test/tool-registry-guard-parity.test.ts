import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  detectToolConflict,
  validateToolName,
  validateToolDefinition,
  filterAvailableTools,
  ToolRegistryGuard,
  TOOL_NAME_PATTERN,
  MAX_TOOL_NAME_LENGTH,
  type RegistrableToolLike,
} from '../src/tools/tool-registry-guard';

// ---------------------------------------------------------------------------
// FIX 1: tool-name validation (Anthropic/OpenAI provider name constraint).
// RED on revert: without the validateToolName gate in detectToolConflict, a name
// with spaces / dots / over-length would wrongly return null (no conflict).
// ---------------------------------------------------------------------------
describe('FIX 1 — tool-name validation', () => {
  it('validateToolName accepts conformant names, rejects bad ones', () => {
    assert.equal(validateToolName('my_tool-2'), null);
    assert.equal(validateToolName(''), 'Tool name must be a non-empty string');
    assert.ok(validateToolName('has space')?.includes('invalid characters'));
    assert.ok(validateToolName('dot.name')?.includes('invalid characters'));
    assert.ok(validateToolName('x'.repeat(MAX_TOOL_NAME_LENGTH + 1))?.includes('limit'));
  });

  it('exposes the provider pattern + length cap as constants', () => {
    assert.ok(TOOL_NAME_PATTERN.test('abc_DEF-123'));
    assert.equal(TOOL_NAME_PATTERN.test('nope!'), false);
    assert.equal(MAX_TOOL_NAME_LENGTH, 64);
  });

  it('detectToolConflict rejects an invalid name regardless of source/registry', () => {
    const registry = new Map<string, string>();
    const conflict = detectToolConflict('bad name', 'plugin-a', registry);
    assert.ok(conflict !== null, 'invalid name must produce a conflict');
    assert.equal(conflict.toolName, 'bad name');
    assert.equal(conflict.conflictingSource, 'plugin-a');
    assert.ok(conflict.reason.includes('invalid characters'));
  });

  it('name validation takes priority over the duplicate check', () => {
    // A name that is both a duplicate AND invalid reports the name problem first.
    const registry = new Map<string, string>([['bad name', 'plugin-a']]);
    const conflict = detectToolConflict('bad name', 'plugin-b', registry);
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('invalid characters'));
    assert.equal(conflict.reason.includes('already registered'), false);
  });

  it('still allows a well-formed brand-new name (no regression)', () => {
    assert.equal(detectToolConflict('well_formed_tool', 'plugin-a', new Map()), null);
  });
});

// ---------------------------------------------------------------------------
// FIX 2: source validation — a tool must be registered with a non-empty source.
// RED on revert: without the empty-source guard, an empty source returns null.
// ---------------------------------------------------------------------------
describe('FIX 2 — source validation', () => {
  it('rejects an empty source', () => {
    const conflict = detectToolConflict('my_tool', '', new Map());
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('non-empty source'));
  });
});

// ---------------------------------------------------------------------------
// FIX 3: structural tool-definition validation (schema presence check).
// RED on revert: validateToolDefinition would not exist / not flag bad schemas.
// ---------------------------------------------------------------------------
describe('FIX 3 — validateToolDefinition (schema presence)', () => {
  const good: RegistrableToolLike = {
    name: 'ok_tool',
    inputSchema: { type: 'object', properties: {} },
  };

  it('accepts a well-formed definition', () => {
    assert.equal(validateToolDefinition(good), null);
  });

  it('rejects a missing inputSchema', () => {
    assert.ok(validateToolDefinition({ name: 'no_schema' })?.includes('missing an inputSchema'));
  });

  it('rejects a non-object schema type', () => {
    const bad: RegistrableToolLike = { name: 'bad_type', inputSchema: { type: 'string', properties: {} } };
    assert.ok(validateToolDefinition(bad)?.includes('must be "object"'));
  });

  it('rejects missing properties', () => {
    const bad: RegistrableToolLike = { name: 'no_props', inputSchema: { type: 'object' } };
    assert.ok(validateToolDefinition(bad)?.includes('properties must be an object'));
  });

  it('rejects an invalid name before touching the schema', () => {
    assert.ok(validateToolDefinition({ name: '', inputSchema: { type: 'object', properties: {} } })
      ?.includes('non-empty string'));
  });
});

// ---------------------------------------------------------------------------
// FIX 4: alias-aware registration, conflict detection, and resolution.
// RED on revert: the guard would not track aliases, so resolveToolName would not
// resolve an alias and an alias collision would not be reported.
// ---------------------------------------------------------------------------
describe('FIX 4 — alias conflicts + resolution', () => {
  it('registers aliases and resolves them to the canonical name', () => {
    const guard = new ToolRegistryGuard();
    assert.equal(guard.register('task_stop', 'core', ['KillShell', 'kill_shell']), null);
    assert.equal(guard.resolveToolName('task_stop'), 'task_stop');
    assert.equal(guard.resolveToolName('KillShell'), 'task_stop');
    assert.equal(guard.resolveToolName('kill_shell'), 'task_stop');
    assert.equal(guard.resolveToolName('unknown_thing'), undefined);
    assert.equal(guard.isAvailable('KillShell'), true);
    assert.equal(guard.isAvailable('nope'), false);
    assert.equal(guard.getAliases().get('KillShell'), 'task_stop');
  });

  it('rejects an alias that shadows an already-registered tool', () => {
    const guard = new ToolRegistryGuard();
    guard.register('real_tool', 'plugin-a');
    const conflict = guard.register('other_tool', 'plugin-b', ['real_tool']);
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('shadows a registered tool'));
    // tool must NOT be half-registered when an alias conflicts
    assert.equal(guard.isRegistered('other_tool'), false);
  });

  it('rejects an alias already claimed by a different tool', () => {
    const guard = new ToolRegistryGuard();
    guard.register('tool_one', 'plugin-a', ['shared_alias']);
    const conflict = guard.register('tool_two', 'plugin-b', ['shared_alias']);
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('already an alias of'));
  });

  it('rejects an alias that lands on a reserved prefix for a non-core source', () => {
    const guard = new ToolRegistryGuard();
    const conflict = guard.register('my_tool', 'plugin-a', ['bash']);
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('reserved prefix'));
  });

  it('rejects a structurally invalid alias name', () => {
    const guard = new ToolRegistryGuard();
    const conflict = guard.register('my_tool', 'plugin-a', ['bad alias']);
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('is invalid'));
  });

  it('treats a self-alias as a harmless no-op', () => {
    const guard = new ToolRegistryGuard();
    assert.equal(guard.register('selfy', 'plugin-a', ['selfy']), null);
    assert.equal(guard.isRegistered('selfy'), true);
    // a self-alias is not stored as a separate alias mapping
    assert.equal(guard.getAliases().has('selfy'), false);
  });

  it('clear() also wipes aliases', () => {
    const guard = new ToolRegistryGuard();
    guard.register('t', 'core', ['a_t']);
    guard.clear();
    assert.equal(guard.resolveToolName('a_t'), undefined);
  });
});

// ---------------------------------------------------------------------------
// FIX 5: availability gating (isEnabled) with an always-present protected set.
// RED on revert: filterAvailableTools would not exist / would not keep protected
// tools nor drop disabled ones.
// ---------------------------------------------------------------------------
describe('FIX 5 — filterAvailableTools (tier always-present + isEnabled)', () => {
  const tools = [
    { name: 'always_on' },
    { name: 'flagged_off', isEnabled: () => false },
    { name: 'flagged_on', isEnabled: () => true },
    { name: 'skill', isEnabled: () => false }, // disabled but protected (orchestration)
  ];

  it('drops disabled tools, keeps tools with no predicate', () => {
    const names = filterAvailableTools(tools).map((t) => t.name);
    assert.deepEqual(names.sort(), ['always_on', 'flagged_on']);
  });

  it('ALWAYS keeps a protected (ESSENTIAL/ORCHESTRATION) tool even when disabled', () => {
    const names = filterAvailableTools(tools, undefined, ['skill']).map((t) => t.name);
    assert.ok(names.includes('skill'), 'protected tool must survive gating');
    assert.equal(names.includes('flagged_off'), false);
  });

  it('accepts the protected set as a Set as well as an array', () => {
    const names = filterAvailableTools(tools, undefined, new Set(['skill'])).map((t) => t.name);
    assert.ok(names.includes('skill'));
  });

  it('passes context through to isEnabled', () => {
    const ctxTools = [
      { name: 'ctx_tool', isEnabled: (c?: { allow: boolean }) => c?.allow === true },
    ];
    assert.equal(filterAvailableTools(ctxTools, { allow: true }).length, 1);
    assert.equal(filterAvailableTools(ctxTools, { allow: false }).length, 0);
  });
});
