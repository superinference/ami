/**
 * U010 / w102 — config.ts parity additions:
 *   - isVoiceModeEnabled / voiceEnabled knob (Claude Code's isVoiceModeEnabled gate)
 *   - hasAutoModeOptIn / autoAllowOptIn knob (Claude Code's hasAutoModeOptIn signal)
 *   - omitWhenEqualsDefault + toPersistableEffort (store-as-default-sentinel hygiene)
 *   - migrateConfigKey + runConfigKeyMigrations (idempotent, guarded config-key rename)
 *   - addItemToJSONCArray (comment-preserving JSONC array append)
 *
 * RED-GREEN discriminators:
 *  - "re-derives on model switch" fails if toPersistableEffort pins an absolute level
 *    instead of storing undefined when the value equals the model default.
 *  - "does not clobber an already-set new key" fails if migrateConfigKey overwrites a
 *    user's explicit successor value.
 *  - "preserves a trailing comment" fails if addItemToJSONCArray re-stringifies and
 *    strips the comment.
 *
 * Run: cd core/core && node --import tsx --test test/config-u010-w102.test.ts
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  isVoiceModeEnabled,
  hasAutoModeOptIn,
  omitWhenEqualsDefault,
  toPersistableEffort,
  migrateConfigKey,
  runConfigKeyMigrations,
  addItemToJSONCArray,
  mergeConfigs,
  validateProjectConfigValues,
  findUnknownConfigKeys,
  KNOWN_CONFIG_KEYS,
  getConfigJSONSchema,
  parseJsonc,
  type ProjectConfig,
} from '../src/config';

const tmpdirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-w102-'));
  tmpdirs.push(d);
  return d;
}
afterEach(() => {
  for (const d of tmpdirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// voiceEnabled / isVoiceModeEnabled
// ---------------------------------------------------------------------------
describe('isVoiceModeEnabled', () => {
  it('is false by default (unset / null / undefined)', () => {
    assert.equal(isVoiceModeEnabled(undefined), false);
    assert.equal(isVoiceModeEnabled(null), false);
    assert.equal(isVoiceModeEnabled({}), false);
    assert.equal(isVoiceModeEnabled({ voiceEnabled: false }), false);
  });
  it('is true only on an explicit opt-in', () => {
    assert.equal(isVoiceModeEnabled({ voiceEnabled: true }), true);
  });
});

// ---------------------------------------------------------------------------
// autoAllowOptIn / hasAutoModeOptIn
// ---------------------------------------------------------------------------
describe('hasAutoModeOptIn', () => {
  it('is false unless explicitly opted in', () => {
    assert.equal(hasAutoModeOptIn(undefined), false);
    assert.equal(hasAutoModeOptIn({}), false);
    assert.equal(hasAutoModeOptIn({ autoAllowOptIn: false }), false);
    assert.equal(hasAutoModeOptIn({ autoAllowOptIn: true }), true);
  });
});

// ---------------------------------------------------------------------------
// omitWhenEqualsDefault / toPersistableEffort
// ---------------------------------------------------------------------------
describe('omitWhenEqualsDefault', () => {
  it('omits (undefined) when the value equals the default', () => {
    assert.equal(omitWhenEqualsDefault('high', 'high'), undefined);
    assert.equal(omitWhenEqualsDefault(5, 5), undefined);
    assert.equal(omitWhenEqualsDefault(undefined, undefined), undefined);
  });
  it('keeps the value verbatim when it differs from the default', () => {
    assert.equal(omitWhenEqualsDefault('low', 'high'), 'low');
    assert.equal(omitWhenEqualsDefault('high', undefined), 'high');
  });
});

describe('toPersistableEffort', () => {
  it('stores undefined when the chosen level equals the model default (re-derives on model switch)', () => {
    // RED-GREEN: a model whose default is "high" should persist nothing so that a later
    // switch to a model defaulting to "medium" naturally re-derives "medium".
    assert.equal(toPersistableEffort('high', 'high'), undefined);
  });
  it('persists an explicit non-default level', () => {
    assert.equal(toPersistableEffort('low', 'high'), 'low');
  });
  it('keeps a concrete level when no model default is known', () => {
    assert.equal(toPersistableEffort('medium', undefined), 'medium');
  });
  it('an unset value stays unset', () => {
    assert.equal(toPersistableEffort(undefined, 'high'), undefined);
  });
});

// ---------------------------------------------------------------------------
// migrateConfigKey
// ---------------------------------------------------------------------------
describe('migrateConfigKey', () => {
  it('copies + coerces + deletes the old key when old is present and new is unset', () => {
    const cfg = { replBridgeEnabled: 'yes' } as unknown as ProjectConfig;
    const out = migrateConfigKey(cfg, 'replBridgeEnabled', 'remoteControlAtStartup', Boolean) as Record<string, unknown>;
    assert.equal(out.remoteControlAtStartup, true, 'value copied + coerced to boolean');
    assert.equal('replBridgeEnabled' in out, false, 'stale key deleted');
    assert.notEqual(out, cfg, 'returns a new object when it migrates');
  });
  it('is a no-op (same reference) when the old key is absent', () => {
    const cfg = { model: 'x' } as ProjectConfig;
    assert.equal(migrateConfigKey(cfg, 'replBridgeEnabled', 'remoteControlAtStartup'), cfg);
  });
  it('does NOT clobber an already-set new key (and leaves the old key untouched → no-op)', () => {
    const cfg = { replBridgeEnabled: true, remoteControlAtStartup: false } as unknown as ProjectConfig;
    const out = migrateConfigKey(cfg, 'replBridgeEnabled', 'remoteControlAtStartup', Boolean);
    assert.equal(out, cfg, 'same reference — never overwrites the user value');
    assert.equal((out as Record<string, unknown>).remoteControlAtStartup, false);
  });
  it('is idempotent: re-running after a successful migration is a no-op', () => {
    const cfg = { replBridgeEnabled: true } as unknown as ProjectConfig;
    const once = migrateConfigKey(cfg, 'replBridgeEnabled', 'remoteControlAtStartup', Boolean);
    const twice = migrateConfigKey(once, 'replBridgeEnabled', 'remoteControlAtStartup', Boolean);
    assert.equal(twice, once, 'second run finds no old key → no-op');
  });
  it('identity coerce by default', () => {
    const out = migrateConfigKey({ a: 'v' } as unknown as ProjectConfig, 'a', 'b') as Record<string, unknown>;
    assert.equal(out.b, 'v');
  });
});

// ---------------------------------------------------------------------------
// runConfigKeyMigrations (durable, lock-safe read-modify-write)
// ---------------------------------------------------------------------------
describe('runConfigKeyMigrations', () => {
  it('applies a registry of renames to the config file and reports the migrated keys', () => {
    const dir = mkTmp();
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, JSON.stringify({ replBridgeEnabled: 1, model: 'keep' }));
    const migrated = runConfigKeyMigrations(p, [
      { from: 'replBridgeEnabled', to: 'remoteControlAtStartup', coerce: Boolean },
    ]);
    assert.deepEqual(migrated, ['replBridgeEnabled']);
    const after = JSON.parse(fs.readFileSync(p, 'utf8'));
    assert.equal(after.remoteControlAtStartup, true);
    assert.equal('replBridgeEnabled' in after, false);
    assert.equal(after.model, 'keep', 'unrelated keys preserved');
  });
  it('is a no-op (empty result) when there is no resolvable config path', () => {
    assert.deepEqual(runConfigKeyMigrations(null, [{ from: 'a', to: 'b' }]), []);
  });
  it('is a no-op when the registry is empty (the default — no deprecated keys yet)', () => {
    const dir = mkTmp();
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'x' }));
    assert.deepEqual(runConfigKeyMigrations(p, []), []);
    // file unchanged
    assert.deepEqual(JSON.parse(fs.readFileSync(p, 'utf8')), { model: 'x' });
  });
  it('reports nothing migrated when the deprecated key is absent', () => {
    const dir = mkTmp();
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'x' }));
    assert.deepEqual(runConfigKeyMigrations(p, [{ from: 'gone', to: 'new' }]), []);
  });
});

// ---------------------------------------------------------------------------
// addItemToJSONCArray (comment-preserving append)
// ---------------------------------------------------------------------------
describe('addItemToJSONCArray', () => {
  it('creates a fresh single-element array from empty/whitespace content', () => {
    assert.deepEqual(parseJsonc(addItemToJSONCArray('', 'a')), ['a']);
    assert.deepEqual(parseJsonc(addItemToJSONCArray('   \n', 'a')), ['a']);
  });
  it('inserts at index 0 of an empty array', () => {
    assert.deepEqual(parseJsonc(addItemToJSONCArray('[]', 'a')), ['a']);
    assert.deepEqual(parseJsonc(addItemToJSONCArray('[\n]', 'a')), ['a']);
  });
  it('appends after the last element of a non-empty array', () => {
    const out = addItemToJSONCArray('[\n  "a"\n]', 'b');
    assert.deepEqual(parseJsonc(out), ['a', 'b']);
  });
  it('preserves a leading block comment (does not re-stringify)', () => {
    const src = '// managed allowlist\n[\n  "a"\n]\n';
    const out = addItemToJSONCArray(src, 'b');
    assert.ok(out.includes('// managed allowlist'), 'the top comment survived');
    assert.deepEqual(parseJsonc(out), ['a', 'b']);
  });
  it('preserves a trailing per-line comment', () => {
    const src = '[\n  "a" // first\n]\n';
    const out = addItemToJSONCArray(src, 'b');
    assert.ok(out.includes('// first'), 'the inline comment survived');
    assert.deepEqual(parseJsonc(out), ['a', 'b']);
  });
  it('tolerates an existing trailing comma', () => {
    const out = addItemToJSONCArray('[\n  "a",\n]\n', 'b');
    assert.deepEqual(parseJsonc(out), ['a', 'b']);
  });
  it('appends after a nested-object element without corrupting it', () => {
    const out = addItemToJSONCArray('[\n  { "a": 1, "b": 2 }\n]\n', { c: 3 });
    assert.deepEqual(parseJsonc(out), [{ a: 1, b: 2 }, { c: 3 }]);
  });
  it('replaces non-array JSON content with a fresh single-element array', () => {
    assert.deepEqual(parseJsonc(addItemToJSONCArray('{"x":1}', 'a')), ['a']);
    assert.deepEqual(parseJsonc(addItemToJSONCArray('42', 'a')), ['a']);
  });
  it('falls back to a full re-stringify on unparseable content', () => {
    assert.deepEqual(parseJsonc(addItemToJSONCArray('[ this is broken', 'a')), ['a']);
  });
  it('serializes object items and keeps them valid', () => {
    const out = addItemToJSONCArray('[]', { type: 'command', command: './x.sh' });
    assert.deepEqual(parseJsonc(out), [{ type: 'command', command: './x.sh' }]);
  });
});

// ---------------------------------------------------------------------------
// Plumbing: merge + validation + known keys + schema for the new fields
// ---------------------------------------------------------------------------
describe('voiceEnabled / autoAllowOptIn config plumbing', () => {
  it('are recognized known keys (no spurious unknown-key warning)', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('voiceEnabled'));
    assert.ok(KNOWN_CONFIG_KEYS.has('autoAllowOptIn'));
    assert.deepEqual(findUnknownConfigKeys({ voiceEnabled: true, autoAllowOptIn: true }), []);
  });
  it('are copied through the merge with higher-priority-layer-wins semantics', () => {
    // projectConfig (higher priority) overrides globalConfig (lower); an unset knob in
    // the higher layer falls through to the lower layer's value.
    const merged = mergeConfigs(
      {},
      {},
      { voiceEnabled: true },
      { voiceEnabled: false, autoAllowOptIn: false },
    );
    assert.equal(merged.voiceEnabled, true, 'higher-priority project layer wins');
    assert.equal(merged.autoAllowOptIn, false, 'lower-layer value retained when not overridden');
  });
  it('base-type validate as booleans', () => {
    assert.deepEqual(
      validateProjectConfigValues({ voiceEnabled: true, autoAllowOptIn: false }),
      [],
    );
    const issues = validateProjectConfigValues({ voiceEnabled: 'yes' as unknown as boolean });
    assert.ok(issues.some((i) => i.field === 'voiceEnabled'), 'a non-boolean is flagged');
  });
  it('appear in the generated JSON schema', () => {
    const schema = getConfigJSONSchema();
    assert.equal(schema.properties.voiceEnabled?.type, 'boolean');
    assert.equal(schema.properties.autoAllowOptIn?.type, 'boolean');
  });
});
