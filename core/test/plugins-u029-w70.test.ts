import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  validatePluginManifest,
  validatePluginContents,
  validatePluginHooks,
  discoverPlugins,
  USER_CONFIG_OPTION_TYPES,
} from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 w70 — three Claude-parity building blocks on core/core/src/plugins.ts:
//  1. `userConfig` manifest validation (schemas.ts L575-654 — user-prompted config
//     options: type enum, required title/description, optional
//     required/default/multiple/sensitive/min/max, identifier-safe keys).
//  2. `hooks.json` STRUCTURAL schema validation (validatePlugin.ts L640-711 —
//     PluginHooksSchema hard-parses at load time; previously AMI only caught JSON
//     syntax errors).
//  3. manifest `agentsPaths` (loadPluginAgents.ts L275-335 — additional agent
//     directories/files surfaced on the discovered bundle, hardened like
//     commandsPaths/skillsPaths).
// ---------------------------------------------------------------------------

function tmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-plugins-w70-'));
}
function writeManifest(root: string, manifest: Record<string, unknown>): void {
  fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(path.join(root, '.ami-plugin', 'plugin.json'), JSON.stringify(manifest), 'utf-8');
}
const errPaths = (r: { errors: { path: string }[] }) => r.errors.map(e => e.path);

describe('U029 w70 — userConfig manifest validation', () => {
  it('accepts a well-formed userConfig with every optional knob', () => {
    const r = validatePluginManifest({
      name: 'my-plugin',
      userConfig: {
        API_KEY: { type: 'string', title: 'API Key', description: 'Your key', required: true, sensitive: true },
        PORT: { type: 'number', title: 'Port', description: 'Listen port', min: 1, max: 65535, default: 8080 },
        TAGS: { type: 'string', title: 'Tags', description: 'Labels', multiple: true, default: ['a', 'b'] },
        DIR: { type: 'directory', title: 'Dir', description: 'Workdir' },
      },
    });
    assert.equal(r.valid, true, JSON.stringify(r.errors));
  });

  it('exposes the five Claude option types', () => {
    assert.deepEqual([...USER_CONFIG_OPTION_TYPES], ['string', 'number', 'boolean', 'directory', 'file']);
  });

  it('rejects a non-object userConfig', () => {
    assert.deepEqual(errPaths(validatePluginManifest({ name: 'p', userConfig: ['nope'] })), ['userConfig']);
  });

  it('flags a bad type, missing title/description, and a non-identifier key', () => {
    const r = validatePluginManifest({
      name: 'p',
      userConfig: {
        '1bad': { type: 'enum', title: '', description: '' },
      },
    });
    assert.equal(r.valid, false);
    const paths = errPaths(r);
    assert.ok(paths.includes('userConfig.1bad'), 'key identifier error');
    assert.ok(paths.includes('userConfig.1bad.type'), 'type enum error');
    assert.ok(paths.includes('userConfig.1bad.title'), 'title error');
    assert.ok(paths.includes('userConfig.1bad.description'), 'description error');
  });

  it('flags non-boolean flags, non-number min/max, and a bad default', () => {
    const r = validatePluginManifest({
      name: 'p',
      userConfig: {
        OPT: { type: 'number', title: 'T', description: 'D', required: 'yes', min: 'low', default: { nope: 1 } },
      },
    });
    const paths = errPaths(r);
    assert.ok(paths.includes('userConfig.OPT.required'));
    assert.ok(paths.includes('userConfig.OPT.min'));
    assert.ok(paths.includes('userConfig.OPT.default'));
  });

  it('flags a non-object option entry', () => {
    assert.deepEqual(errPaths(validatePluginManifest({ name: 'p', userConfig: { OPT: 'scalar' } })), ['userConfig.OPT']);
  });

  it('absent userConfig is fine', () => {
    assert.equal(validatePluginManifest({ name: 'p' }).valid, true);
  });
});

describe('U029 w70 — hooks.json structural schema validation', () => {
  it('accepts the AMI flat-array form', () => {
    assert.deepEqual(validatePluginHooks([{ event: 'preToolUse', type: 'command', command: 'echo hi' }]), []);
  });

  it('accepts the Claude map-keyed form (with and without the hooks wrapper)', () => {
    const matcher = { matcher: 'Bash', hooks: [{ type: 'command', command: 'echo hi' }] };
    assert.deepEqual(validatePluginHooks({ PreToolUse: [matcher] }), []);
    assert.deepEqual(validatePluginHooks({ hooks: { PreToolUse: [matcher] } }), []);
  });

  it('rejects a non-object/array top level', () => {
    assert.deepEqual(validatePluginHooks('nope').map(e => e.path), ['hooks']);
  });

  it('rejects a non-array value for an event', () => {
    assert.deepEqual(validatePluginHooks({ preToolUse: 'not-an-array' }).map(e => e.path), ['hooks.preToolUse']);
  });

  it('rejects a non-object hook entry', () => {
    assert.ok(validatePluginHooks([42]).some(e => e.path === 'hooks[0]'));
  });

  it('flags an unknown action type and a command hook missing its command', () => {
    const issues = validatePluginHooks([
      { type: 'teleport' },
      { type: 'command' },
    ]);
    assert.ok(issues.some(e => e.path === 'hooks[0].type'));
    assert.ok(issues.some(e => e.path === 'hooks[1].command'));
  });

  it('recurses into the nested matcher.hooks array', () => {
    const issues = validatePluginHooks({ PreToolUse: [{ matcher: '*', hooks: [{ type: 'command', command: '' }] }] });
    assert.ok(issues.some(e => e.path === 'hooks.PreToolUse[0].hooks[0].command'));
  });

  it('validatePluginContents surfaces a structurally-bad hooks.json (not just syntax)', () => {
    const dir = tmpDir();
    try {
      writeManifest(dir, { name: 'hooky' });
      // Parses as valid JSON but is structurally wrong (event → non-array).
      fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ preToolUse: 'x' }), 'utf-8');
      const r = validatePluginContents(dir);
      assert.equal(r.valid, false);
      assert.ok(r.errors.some(e => e.path === 'hooks.json:hooks.preToolUse'), JSON.stringify(r.errors));
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('validatePluginContents accepts a well-formed hooks.json', () => {
    const dir = tmpDir();
    try {
      writeManifest(dir, { name: 'hooky' });
      fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify([{ event: 'stop', type: 'command', command: 'echo done' }]), 'utf-8');
      assert.equal(validatePluginContents(dir).valid, true);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('U029 w70 — manifest agentsPaths', () => {
  it('surfaces resolved agentsPaths (string or array) on the discovered bundle', () => {
    const parent = tmpDir();
    try {
      const root = path.join(parent, 'plug');
      fs.mkdirSync(path.join(root, 'extra-agents'), { recursive: true });
      fs.writeFileSync(path.join(root, 'extra-agents', 'helper.md'), '---\ndescription: x\n---\nbody', 'utf-8');
      fs.writeFileSync(path.join(root, 'solo.md'), '---\ndescription: y\n---\nbody', 'utf-8');
      writeManifest(root, { name: 'plug', agentsPaths: ['extra-agents', 'solo.md'] });
      const [bundle] = discoverPlugins(parent);
      assert.ok(bundle);
      assert.equal(bundle!.agentsPaths?.length, 2);
      assert.ok(bundle!.agentsPaths!.some(p => p.endsWith('extra-agents')));
      assert.ok(bundle!.agentsPaths!.some(p => p.endsWith('solo.md')));
    } finally {
      fs.rmSync(parent, { recursive: true, force: true });
    }
  });

  it('drops a nonexistent agentsPaths entry (undefined when none survive)', () => {
    const parent = tmpDir();
    try {
      const root = path.join(parent, 'plug');
      fs.mkdirSync(root, { recursive: true });
      writeManifest(root, { name: 'plug', agentsPaths: ['does-not-exist'] });
      const [bundle] = discoverPlugins(parent);
      assert.equal(bundle!.agentsPaths, undefined);
    } finally {
      fs.rmSync(parent, { recursive: true, force: true });
    }
  });

  it('treats an agentsPaths with ".." as a hard manifest error (bundle rejected)', () => {
    const r = validatePluginManifest({ name: 'plug', agentsPaths: ['../escape'] });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'agentsPaths[0]'));
  });
});
