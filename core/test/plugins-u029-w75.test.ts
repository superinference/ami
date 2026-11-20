import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  coercePluginOptionBool,
  coercePluginOptionValues,
  buildPluginOptionPrefill,
  validatePluginOptionValues,
  getUnconfiguredPluginOptions,
  pluginErrorKey,
  mergePluginErrors,
  PLUGIN_EDITABLE_SCOPES,
  isPersistablePluginScope,
  resolvePluginEditableScopes,
  formatPluginPathTraversalWarning,
  getPluginErrorMessage,
  loadCommands,
  discoverPluginsDetailed,
  type PluginOptionSchema,
  type PluginError,
  type PluginScopeSource,
} from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 w75 — the reference-parity runtime building blocks on core/core/src/plugins.ts:
//  1. Plugin option VALUE coercion/validation/prefill (PluginOptionsDialog
//     buildFinalValues + initialFor; pluginOptionsStorage validateUserConfig /
//     getUnconfiguredOptions) — blank-number omit, NaN→string, boolean allow-list,
//     sensitive skip-on-reconfigure, required/type/min/max value validation.
//  2. Plugin-error merge/dedup with source preservation (refresh.ts
//     mergePluginErrors / errorKey).
//  3. Editable-scope ownership resolution (pluginStartupCheck
//     getPluginEditableScopes / isPersistableScope).
//  4. Realpath dedup of already-loaded command files (loadPluginCommands
//     isDuplicatePath).
//  5. Path-traversal containment warning (lspPluginIntegration security message).
// ---------------------------------------------------------------------------

function tmpDir(prefix = 'ami-plugins-w75-'): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

describe('U029 w75 — coercePluginOptionBool (Claude isEnvTruthy allow-list)', () => {
  it('is truthy only for an explicit affirmative token (case/space-insensitive)', () => {
    for (const v of ['1', 'true', 'TRUE', ' yes ', 'On']) {
      assert.equal(coercePluginOptionBool(v), true, `${JSON.stringify(v)} should be true`);
    }
  });
  it('is false for blank and every unrecognized value (deny-list differs)', () => {
    for (const v of ['', '0', 'false', 'no', 'off', 'maybe', 'random', '2']) {
      assert.equal(coercePluginOptionBool(v), false, `${JSON.stringify(v)} should be false`);
    }
  });
});

describe('U029 w75 — coercePluginOptionValues (buildFinalValues edge cases)', () => {
  const schema: PluginOptionSchema = {
    NAME: { type: 'string', title: 'Name', description: 'n' },
    PORT: { type: 'number', title: 'Port', description: 'p' },
    FLAG: { type: 'boolean', title: 'Flag', description: 'f' },
    KEY: { type: 'string', title: 'Key', description: 'k', sensitive: true },
  };
  const fields = Object.keys(schema);

  it('coerces a numeric string to a number', () => {
    const out = coercePluginOptionValues(fields, { NAME: 'x', PORT: '8080', FLAG: 'yes', KEY: 'abc' }, schema);
    assert.deepEqual(out, { NAME: 'x', PORT: 8080, FLAG: true, KEY: 'abc' });
  });
  it('omits a blank number instead of storing 0', () => {
    const out = coercePluginOptionValues(['PORT'], { PORT: '   ' }, schema);
    assert.ok(!('PORT' in out), 'blank number must be omitted');
  });
  it('falls back to the raw string when a number input is non-numeric', () => {
    const out = coercePluginOptionValues(['PORT'], { PORT: 'abc' }, schema);
    assert.equal(out.PORT, 'abc');
  });
  it('omits a blank sensitive field on reconfigure (keeps the saved secret)', () => {
    const out = coercePluginOptionValues(['KEY'], { KEY: '' }, schema, { KEY: 'old-secret' });
    assert.ok(!('KEY' in out), 'blank sensitive on reconfigure must be omitted');
  });
  it('stores a blank sensitive field as empty on first config (no saved value)', () => {
    const out = coercePluginOptionValues(['KEY'], { KEY: '' }, schema, {});
    assert.equal(out.KEY, '');
  });
  it('treats a missing collected entry as empty string', () => {
    const out = coercePluginOptionValues(['NAME'], {}, schema);
    assert.equal(out.NAME, '');
  });
});

describe('U029 w75 — buildPluginOptionPrefill (initialFor)', () => {
  const schema: PluginOptionSchema = {
    NAME: { type: 'string', title: 'Name', description: 'n' },
    PORT: { type: 'number', title: 'Port', description: 'p' },
    KEY: { type: 'string', title: 'Key', description: 'k', sensitive: true },
  };
  it('prefills non-sensitive saved values (stringified) and blanks sensitive ones', () => {
    const out = buildPluginOptionPrefill(schema, { NAME: 'bob', PORT: 8080, KEY: 'secret' });
    assert.deepEqual(out, { NAME: 'bob', PORT: '8080', KEY: '' });
  });
  it('yields empty strings when nothing is saved', () => {
    assert.deepEqual(buildPluginOptionPrefill(schema), { NAME: '', PORT: '', KEY: '' });
  });
});

describe('U029 w75 — validatePluginOptionValues', () => {
  const schema: PluginOptionSchema = {
    NAME: { type: 'string', title: 'Name', description: 'n', required: true },
    PORT: { type: 'number', title: 'Port', description: 'p', min: 1, max: 100 },
    TAGS: { type: 'string', title: 'Tags', description: 't', multiple: true },
    OPT: { type: 'string', title: 'Opt', description: 'o' },
  };
  it('passes when all required present and types valid', () => {
    const r = validatePluginOptionValues({ NAME: 'x', PORT: 50, TAGS: ['a'] }, schema);
    assert.deepEqual(r, { valid: true, errors: [] });
  });
  it('fails a missing/blank required field', () => {
    assert.equal(validatePluginOptionValues({ NAME: '' }, schema).valid, false);
    assert.equal(validatePluginOptionValues({}, schema).valid, false);
  });
  it('allows an absent optional field', () => {
    assert.equal(validatePluginOptionValues({ NAME: 'x' }, schema).valid, true);
  });
  it('rejects a non-numeric number and an out-of-range number', () => {
    assert.equal(validatePluginOptionValues({ NAME: 'x', PORT: 'nope' }, schema).valid, false);
    assert.equal(validatePluginOptionValues({ NAME: 'x', PORT: 0 }, schema).valid, false);
    assert.equal(validatePluginOptionValues({ NAME: 'x', PORT: 101 }, schema).valid, false);
    assert.equal(validatePluginOptionValues({ NAME: 'x', PORT: '42' }, schema).valid, true);
  });
  it('rejects a multiple option that is not a string array, and a scalar given an array', () => {
    assert.equal(validatePluginOptionValues({ NAME: 'x', TAGS: 'a' as unknown as string[] }, schema).valid, false);
    assert.equal(validatePluginOptionValues({ NAME: 'x', OPT: ['a'] as unknown as string }, schema).valid, false);
  });
});

describe('U029 w75 — getUnconfiguredPluginOptions', () => {
  const schema: PluginOptionSchema = {
    NAME: { type: 'string', title: 'Name', description: 'n', required: true },
    PORT: { type: 'number', title: 'Port', description: 'p', required: true },
  };
  it('returns empty when nothing is declared', () => {
    assert.deepEqual(getUnconfiguredPluginOptions({}, {}), {});
  });
  it('returns empty when all saved values validate', () => {
    assert.deepEqual(getUnconfiguredPluginOptions(schema, { NAME: 'x', PORT: 5 }), {});
  });
  it('returns only the still-missing fields (skips configured ones)', () => {
    const out = getUnconfiguredPluginOptions(schema, { NAME: 'x' });
    assert.deepEqual(Object.keys(out), ['PORT']);
  });
});

describe('U029 w75 — mergePluginErrors / pluginErrorKey', () => {
  const lspErr: PluginError = { type: 'lsp-config-invalid', source: 'lsp-manager', plugin: 'p', serverName: 's', validationError: 'bad' };
  const pluginErr: PluginError = { type: 'hook-load-failed', source: 'plugin:foo', plugin: 'foo', hookPath: 'h', reason: 'r' };
  const transientErr: PluginError = { type: 'network-error' as never, source: 'registry', error: 'x' } as unknown as PluginError;

  it('keys generic-error by source+message, typed by type:source', () => {
    assert.equal(pluginErrorKey({ type: 'generic-error', source: 'plugin:a', error: 'boom' }), 'generic-error:plugin:a:boom');
    assert.equal(pluginErrorKey(pluginErr), 'hook-load-failed:plugin:foo');
  });
  it('preserves lsp-manager and plugin:* errors across a reload', () => {
    const merged = mergePluginErrors([lspErr, pluginErr, transientErr], []);
    assert.ok(merged.includes(lspErr), 'lsp-manager error preserved');
    assert.ok(merged.includes(pluginErr), 'plugin:* error preserved');
    assert.ok(!merged.includes(transientErr), 'non-preserved source dropped');
  });
  it('fresh copy wins over a preserved error with the same key', () => {
    const fresh: PluginError = { type: 'hook-load-failed', source: 'plugin:foo', plugin: 'foo', hookPath: 'h2', reason: 'new' };
    const merged = mergePluginErrors([pluginErr], [fresh]);
    assert.deepEqual(merged, [fresh]);
  });
});

describe('U029 w75 — editable scope ownership resolution', () => {
  it('declares the precedence-ordered scope list with managed first and flag last', () => {
    assert.deepEqual([...PLUGIN_EDITABLE_SCOPES], ['managed', 'user', 'project', 'local', 'flag']);
  });
  it('isPersistablePluginScope is true for all but flag', () => {
    assert.equal(isPersistablePluginScope('user'), true);
    assert.equal(isPersistablePluginScope('managed'), true);
    assert.equal(isPersistablePluginScope('flag'), false);
  });
  it('resolves the highest source that enables a plugin as its owner', () => {
    const sources: PluginScopeSource[] = [
      { scope: 'managed', enabledPlugins: { 'a@m': true } },
      { scope: 'user', enabledPlugins: { 'a@m': true, 'b@m': true } },
      { scope: 'project', enabledPlugins: { 'b@m': true } },
    ];
    const map = resolvePluginEditableScopes(sources);
    assert.equal(map.get('a@m'), 'user');
    assert.equal(map.get('b@m'), 'project');
  });
  it('a later false disables (removes) a plugin enabled lower down', () => {
    const map = resolvePluginEditableScopes([
      { scope: 'user', enabledPlugins: { 'a@m': true } },
      { scope: 'local', enabledPlugins: { 'a@m': false } },
    ]);
    assert.equal(map.has('a@m'), false);
  });
  it('ignores ids without an @ marketplace marker', () => {
    const map = resolvePluginEditableScopes([{ scope: 'user', enabledPlugins: { bare: true } }]);
    assert.equal(map.size, 0);
  });
});

describe('U029 w75 — realpath dedup in loadCommands', () => {
  it('loads a file reachable through two sources exactly once', () => {
    const root = tmpDir();
    const primary = path.join(root, 'commands');
    fs.mkdirSync(primary, { recursive: true });
    fs.writeFileSync(path.join(primary, 'hello.md'), '# Hello\n\nrun hello', 'utf-8');

    // extraPaths points back at the SAME dir — without realpath dedup this would
    // yield the command twice.
    const cmds = loadCommands(primary, 'plug', [primary]);
    const hello = cmds.filter(c => c.name.endsWith('hello'));
    assert.equal(hello.length, 1, 'hello command must be loaded once');
  });

  it('dedups a symlinked duplicate of an already-loaded command file', () => {
    const root = tmpDir();
    const primary = path.join(root, 'commands');
    fs.mkdirSync(primary, { recursive: true });
    const real = path.join(primary, 'real.md');
    fs.writeFileSync(real, '# Real\n\nrun', 'utf-8');
    // A symlink in the same dir pointing at real.md — same canonical path.
    try {
      fs.symlinkSync(real, path.join(primary, 'link.md'));
    } catch {
      return; // symlinks unsupported on this platform — skip
    }
    const cmds = loadCommands(primary, 'plug');
    const canonical = cmds.filter(c => c.name.endsWith('real') || c.name.endsWith('link'));
    assert.equal(canonical.length, 1, 'symlinked duplicate must be deduped by realpath');
  });
});

describe('U029 w75 — path-traversal containment warning', () => {
  it('formats a security warning mentioning the plugin, path, and component', () => {
    const msg = formatPluginPathTraversalWarning('evil', '../../etc', 'skills');
    assert.match(msg, /Security/);
    assert.match(msg, /evil/);
    assert.match(msg, /\.\.\/\.\.\/etc/);
    assert.match(msg, /skills/);
  });

  it('getPluginErrorMessage renders a path-traversal-blocked error', () => {
    const err: PluginError = { type: 'path-traversal-blocked', source: '/p', plugin: 'evil', declaredPath: 'outside', component: 'commands' };
    assert.match(getPluginErrorMessage(err), /Security.*evil.*outside/);
  });

  it('discoverPluginsDetailed surfaces a symlink-escaping component dir as a failure', () => {
    const base = tmpDir();
    // A target directory OUTSIDE any plugin root.
    const outside = path.join(base, 'outside-skills');
    fs.mkdirSync(outside, { recursive: true });
    fs.writeFileSync(path.join(outside, 'x.md'), 'x', 'utf-8');

    const pluginsDir = path.join(base, 'plugins');
    const root = path.join(pluginsDir, 'evil');
    fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(path.join(root, '.ami-plugin', 'plugin.json'), JSON.stringify({ name: 'evil', version: '1.0.0' }), 'utf-8');
    // The plugin's skills/ is a symlink pointing OUTSIDE its own root.
    try {
      fs.symlinkSync(outside, path.join(root, 'skills'), 'dir');
    } catch {
      return; // symlinks unsupported — skip
    }

    const { bundles, failures } = discoverPluginsDetailed(pluginsDir);
    // The bundle still loads (without the escaping skills dir).
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0].skillsDir, undefined, 'escaping skills dir must not be set');
    const traversal = failures.filter(f => f.error?.type === 'path-traversal-blocked');
    assert.equal(traversal.length, 1, 'path-traversal warning must be surfaced');
    assert.equal(traversal[0].error?.type === 'path-traversal-blocked' && traversal[0].error.component, 'skills');
  });
});
