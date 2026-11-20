import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { isToolReadOnlyForInput, type CategorizableTool } from '../src/tools/index';
import { configTool } from '../src/tools/config-tool';

// U048 — dynamic read-vs-write permission classification for the config tool
// (parity: the reference's ConfigTool.isReadOnly(input) = `value === undefined`). A config
// `get` only reads, so it must be reported read-only (→ auto-allowed without a
// prompt), while a `set` that carries a value mutates on-disk config and must stay
// writable (→ permission-asking path).

describe('isToolReadOnlyForInput — per-invocation read-only verdict', () => {
  it('falls back to the static isReadOnly flag when no per-input hook is declared', () => {
    const roTool: CategorizableTool = { name: 'ro', isReadOnly: true };
    const rwTool: CategorizableTool = { name: 'rw', isReadOnly: false };
    assert.equal(isToolReadOnlyForInput(roTool, {}), true);
    assert.equal(isToolReadOnlyForInput(rwTool, {}), false);
    // Missing isReadOnly (undefined) is treated as writable.
    assert.equal(isToolReadOnlyForInput({ name: 'x' }, {}), false);
  });

  it('uses the tool’s isReadOnlyForInput hook when present (overriding the static flag)', () => {
    const dyn: CategorizableTool = {
      name: 'dyn',
      // Static flag says writable, but the hook decides per call.
      isReadOnly: false,
      isReadOnlyForInput: (input) => input.action === 'get',
    };
    assert.equal(isToolReadOnlyForInput(dyn, { action: 'get' }), true);
    assert.equal(isToolReadOnlyForInput(dyn, { action: 'set' }), false);
  });
});

describe('configTool.isReadOnlyForInput — get reads, set writes', () => {
  it('declares a per-invocation read-only hook', () => {
    assert.equal(typeof configTool.isReadOnlyForInput, 'function');
  });

  it('classifies a get (read all / read one) as read-only', () => {
    assert.equal(configTool.isReadOnlyForInput!({ action: 'get' }), true);
    assert.equal(configTool.isReadOnlyForInput!({ action: 'get', key: 'model' }), true);
  });

  it('classifies a set with a value as writable', () => {
    assert.equal(
      configTool.isReadOnlyForInput!({ action: 'set', key: 'model', value: 'opus' }),
      false,
    );
    // "default" resets (deletes) the key — still a mutation, so writable.
    assert.equal(
      configTool.isReadOnlyForInput!({ action: 'set', key: 'model', value: 'default' }),
      false,
    );
    // A boolean/number value (typed value field) is still a write.
    assert.equal(
      configTool.isReadOnlyForInput!({ action: 'set', key: 'safeMode', value: true }),
      false,
    );
  });

  it('classifies a set WITHOUT a value as read-only (it is a no-op error, writes nothing)', () => {
    // Mirrors the reference's value-presence rule: no value → nothing is persisted.
    assert.equal(configTool.isReadOnlyForInput!({ action: 'set', key: 'model' }), true);
  });
});
