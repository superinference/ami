import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  detectToolConflict,
  isReservedToolName,
  canonicalToolName,
  filterAvailableTools,
  ToolRegistryGuard,
} from '../src/tools/tool-registry-guard';
import {
  DENY_TOOLS_ENV,
  parseDeniedTools,
  isToolDenied,
} from '../src/tools/tool-gates';
import { createDefaultTools } from '../src/tools/index';

// U275 — tool-registry-guard parity: canonical name matching, conflict-reason
// outcome+remedy wording, and the operator-configurable deny knob.

describe('U275 gap: canonical name normalization before reserved/conflict matching', () => {
  it('canonicalToolName folds case, collapses "_" runs, strips a leading "_"', () => {
    assert.equal(canonicalToolName('Bash'), 'bash');
    assert.equal(canonicalToolName('BASH'), 'bash');
    assert.equal(canonicalToolName('__bash'), 'bash');
    assert.equal(canonicalToolName('task__create'), 'task_create');
    assert.equal(canonicalToolName('_task_create'), 'task_create');
    // Trailing "_" is significant (namespace boundary marker) — NOT stripped.
    assert.equal(canonicalToolName('task_'), 'task_');
    assert.equal(canonicalToolName('my_task_helper'), 'my_task_helper');
  });

  it('catches a reserved prefix dodged by a case variant (non-core source)', () => {
    // Red before fix: raw case-sensitive matching let "Bash" sail past the guard.
    const conflict = detectToolConflict('Bash', 'plugin-a', new Map());
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('reserved prefix'));
  });

  it('catches a reserved prefix dodged by a doubled/leading underscore', () => {
    assert.ok(detectToolConflict('__bash', 'plugin-a', new Map()) !== null);
    const doubled = detectToolConflict('task__spawn', 'plugin-a', new Map());
    assert.ok(doubled !== null);
    assert.ok(doubled.reason.includes('reserved prefix "task_"'));
  });

  it('isReservedToolName is canonical: "TASK_x" and "__grep" are reserved', () => {
    assert.equal(isReservedToolName('TASK_spawn'), true);
    assert.equal(isReservedToolName('__grep'), true);
    assert.equal(isReservedToolName('Web_Fetch'), true);
  });

  it('does NOT over-match: "taskmanager"/"my_task_helper" stay allowed', () => {
    assert.equal(detectToolConflict('taskmanager', 'plugin-a', new Map()), null);
    assert.equal(detectToolConflict('my_task_helper', 'plugin-a', new Map()), null);
  });

  it('flags a cross-source collision that only differs after canonicalization', () => {
    // core owns "my_tool"; a plugin's "My__Tool" canonicalizes to the same name.
    const registry = new Map<string, string>([['my_tool', 'core']]);
    const conflict = detectToolConflict('My__tool', 'plugin-a', registry);
    assert.ok(conflict !== null);
    assert.equal(conflict.existingSource, 'core');
    assert.ok(conflict.reason.includes('canonicalization'));
  });

  it('does NOT flag a canonical collision registered by the SAME source', () => {
    const registry = new Map<string, string>([['my_tool', 'plugin-a']]);
    // Same source re-using a canonically-equal variant is not a cross-source clash.
    assert.equal(detectToolConflict('My_tool', 'plugin-a', registry), null);
  });
});

describe('U275 gap: tool conflict reason wording — outcome + remedy guidance', () => {
  it('already-registered reason states the outcome and the remedy', () => {
    const registry = new Map<string, string>([['my_tool', 'plugin-a']]);
    const conflict = detectToolConflict('my_tool', 'plugin-b', registry);
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('already registered'));
    assert.ok(conflict.reason.includes('stays active'));
    assert.ok(conflict.reason.includes('rename'));
  });

  it('reserved-prefix reason states the outcome and the remedy', () => {
    const conflict = detectToolConflict('bash_run', 'plugin-a', new Map());
    assert.ok(conflict !== null);
    assert.ok(conflict.reason.includes('reserved prefix "bash"'));
    assert.ok(conflict.reason.includes('ignored'));
    assert.ok(conflict.reason.includes('rename'));
  });
});

describe('U275 gap: configurable tool deny knob', () => {
  it('parseDeniedTools returns empty for unset/blank', () => {
    assert.equal(parseDeniedTools({}).size, 0);
    assert.equal(parseDeniedTools({ [DENY_TOOLS_ENV]: '   ' }).size, 0);
  });

  it('parseDeniedTools splits, trims and de-dups', () => {
    const set = parseDeniedTools({ [DENY_TOOLS_ENV]: ' web_search , bash ,,web_search ' });
    assert.deepEqual([...set].sort(), ['bash', 'web_search']);
  });

  it('isToolDenied matches exact names, "prefix*" wildcards and mcp server rules', () => {
    const denied = new Set(['web_search', 'cron_*', 'mcp__github']);
    assert.equal(isToolDenied('web_search', denied), true);
    assert.equal(isToolDenied('cron_create', denied), true);
    assert.equal(isToolDenied('cron_list', denied), true);
    assert.equal(isToolDenied('mcp__github__create_issue', denied), true);
    // non-matches
    assert.equal(isToolDenied('bash', denied), false);
    assert.equal(isToolDenied('web_fetch', denied), false);
    assert.equal(isToolDenied('mcp__gitlab__x', denied), false);
    assert.equal(isToolDenied('anything', new Set<string>()), false);
  });

  it('filterAvailableTools drops denied tools but keeps protected ones', () => {
    const tools = [{ name: 'bash' }, { name: 'web_search' }, { name: 'skill' }];
    const denied = new Set(['web_search', 'skill']);
    const kept = filterAvailableTools(
      tools,
      undefined,
      ['skill'], // protected — can never be gated out
      (name) => isToolDenied(name, denied),
    ).map((t) => t.name);
    assert.deepEqual(kept.sort(), ['bash', 'skill']);
  });
});

describe('U275 e2e: createDefaultTools honors AMI_DENY_TOOLS', () => {
  afterEach(() => {
    delete process.env[DENY_TOOLS_ENV];
  });

  it('strips a denied tool from the registered pool before the model sees it', () => {
    const before = new Set(createDefaultTools('/tmp').getAll().map((t) => t.name));
    assert.ok(before.has('web_search'), 'baseline: web_search is in the default pool');
    assert.ok(before.has('web_fetch'), 'baseline: web_fetch is in the default pool');

    process.env[DENY_TOOLS_ENV] = 'web_search, web_fetch';
    const after = new Set(createDefaultTools('/tmp').getAll().map((t) => t.name));
    assert.equal(after.has('web_search'), false);
    assert.equal(after.has('web_fetch'), false);
    // unrelated tools stay
    assert.ok(after.has('bash'));
  });

  it('a "prefix*" rule drops a whole family from the pool', () => {
    process.env[DENY_TOOLS_ENV] = 'cron_*';
    const names = new Set(createDefaultTools('/tmp').getAll().map((t) => t.name));
    assert.equal(names.has('cron_create'), false);
    assert.equal(names.has('cron_delete'), false);
    assert.equal(names.has('cron_list'), false);
    assert.ok(names.has('bash'));
  });
});
