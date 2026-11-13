/**
 * POV: core/core/src/config.ts — config loaders + merge matrix (round 5)
 *
 * Behavioral regression matrix for the project/global/managed config loaders and
 * the layered merge precedence. Every expected value is COMPUTED by calling the
 * real loaders against real on-disk fixtures (fs.mkdtempSync temp dirs) or a real
 * managed-config file path — never by reading the .ts source and .includes().
 *
 *   - loadProjectConfig: present / absent / corrupt (-> null) / JSONC comments
 *   - loadLocalConfig: present / absent
 *   - loadGlobalConfig: HOME present / HOME missing (-> null)
 *   - loadManagedConfig: absent (null) / present (parsed) / corrupt (throws ManagedConfigError)
 *   - mergeConfigs: layer precedence (global < project < local < env < cli),
 *     object/array replace semantics, undefined skip, managed-authoritative allowlists
 *   - stripJsoncComments: string-literal preservation (URLs), block + line comments
 */
import { describe, it, afterEach, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';

import {
  loadProjectConfig,
  loadLocalConfig,
  loadGlobalConfig,
  loadManagedConfig,
  ManagedConfigError,
  mergeConfigs,
  stripJsoncComments,
  type ProjectConfig,
} from '../src/config';

// ---------------------------------------------------------------------------
// temp-dir bookkeeping
// ---------------------------------------------------------------------------

const temps: string[] = [];
function mkTemp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'pov-cfg-'));
  temps.push(d);
  return d;
}
/** Write <cwd>/.ami/<name> with raw contents, creating the .ami dir. */
function writeAmi(cwd: string, name: string, contents: string): string {
  const dir = path.join(cwd, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  const p = path.join(dir, name);
  fs.writeFileSync(p, contents, 'utf-8');
  return p;
}

afterEach(() => {
  while (temps.length) {
    const d = temps.pop()!;
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

// ---------------------------------------------------------------------------
// loadProjectConfig
// ---------------------------------------------------------------------------

describe('loadProjectConfig', () => {
  it('returns null when .ami/config.json is absent', () => {
    const cwd = mkTemp();
    assert.equal(loadProjectConfig(cwd), null);
  });

  it('parses a present config into the ProjectConfig shape', () => {
    const cwd = mkTemp();
    const cfg: ProjectConfig = { model: 'opus', maxTurns: 7, deferTools: true };
    writeAmi(cwd, 'config.json', JSON.stringify(cfg));
    const loaded = loadProjectConfig(cwd);
    assert.deepEqual(loaded, cfg);
  });

  it('reads the exact file under <cwd>/.ami/config.json (not elsewhere)', () => {
    const cwd = mkTemp();
    writeAmi(cwd, 'config.json', JSON.stringify({ provider: 'anthropic' }));
    // a config.local.json alongside must NOT bleed into loadProjectConfig
    writeAmi(cwd, 'config.local.json', JSON.stringify({ provider: 'openai' }));
    assert.equal(loadProjectConfig(cwd)!.provider, 'anthropic');
  });

  it('returns null (defaults) on corrupt / non-JSON content rather than throwing', () => {
    const cwd = mkTemp();
    writeAmi(cwd, 'config.json', '{ this is : not json ,,, ');
    assert.equal(loadProjectConfig(cwd), null);
  });

  it('strips JSONC // and /* */ comments before parsing', () => {
    const cwd = mkTemp();
    writeAmi(cwd, 'config.json', [
      '{',
      '  // leading comment',
      '  "model": "sonnet", /* inline block */',
      '  "baseUrl": "https://example.com/v1" // trailing',
      '}',
    ].join('\n'));
    const loaded = loadProjectConfig(cwd)!;
    assert.equal(loaded.model, 'sonnet');
    // URL double-slash inside a string literal must survive the comment stripper
    assert.equal(loaded.baseUrl, 'https://example.com/v1');
  });

  it('preserves nested object/array fields verbatim', () => {
    const cwd = mkTemp();
    const cfg: ProjectConfig = {
      superinference: { enforcing: true, useLLMCritic: false },
      fallbackModels: ['a', 'b', 'c'],
      managedMcpServers: { srv: { url: 'x' } },
    };
    writeAmi(cwd, 'config.json', JSON.stringify(cfg));
    assert.deepEqual(loadProjectConfig(cwd), cfg);
  });
});

// ---------------------------------------------------------------------------
// loadLocalConfig
// ---------------------------------------------------------------------------

describe('loadLocalConfig', () => {
  it('returns null when config.local.json is absent', () => {
    assert.equal(loadLocalConfig(mkTemp()), null);
  });

  it('reads <cwd>/.ami/config.local.json', () => {
    const cwd = mkTemp();
    writeAmi(cwd, 'config.local.json', JSON.stringify({ thinkingLevel: 'high' }));
    assert.equal(loadLocalConfig(cwd)!.thinkingLevel, 'high');
  });
});

// ---------------------------------------------------------------------------
// loadGlobalConfig (HOME-driven)
// ---------------------------------------------------------------------------

describe('loadGlobalConfig', () => {
  const savedHome = process.env.HOME;
  const savedProfile = process.env.USERPROFILE;

  afterEach(() => {
    if (savedHome === undefined) delete process.env.HOME; else process.env.HOME = savedHome;
    if (savedProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = savedProfile;
  });

  it('loads ~/.ami/config.json from HOME', () => {
    const home = mkTemp();
    process.env.HOME = home;
    writeAmi(home, 'config.json', JSON.stringify({ model: 'global-model' }));
    assert.equal(loadGlobalConfig()!.model, 'global-model');
  });

  it('returns null when HOME points at a dir with no config', () => {
    process.env.HOME = mkTemp();
    delete process.env.USERPROFILE;
    assert.equal(loadGlobalConfig(), null);
  });

  it('returns null when neither HOME nor USERPROFILE is set', () => {
    delete process.env.HOME;
    delete process.env.USERPROFILE;
    assert.equal(loadGlobalConfig(), null);
  });

  it('falls back to USERPROFILE when HOME is unset', () => {
    delete process.env.HOME;
    const profile = mkTemp();
    process.env.USERPROFILE = profile;
    writeAmi(profile, 'config.json', JSON.stringify({ provider: 'from-profile' }));
    assert.equal(loadGlobalConfig()!.provider, 'from-profile');
  });
});

// ---------------------------------------------------------------------------
// loadManagedConfig (SUPERINFERENCE_MANAGED_CONFIG-driven)
// ---------------------------------------------------------------------------

describe('loadManagedConfig', () => {
  const saved = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  afterEach(() => {
    if (saved === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = saved;
  });

  it('returns null when the managed file does not exist', () => {
    process.env.SUPERINFERENCE_MANAGED_CONFIG = path.join(mkTemp(), 'nope.json');
    assert.equal(loadManagedConfig(), null);
  });

  it('parses a present managed file', () => {
    const dir = mkTemp();
    const p = path.join(dir, 'managed.json');
    fs.writeFileSync(p, JSON.stringify({ enforceAvailableModels: ['x', 'y'] }), 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    assert.deepEqual(loadManagedConfig()!.enforceAvailableModels, ['x', 'y']);
  });

  it('FAILS CLOSED (throws ManagedConfigError) on a corrupt managed file', () => {
    const dir = mkTemp();
    const p = path.join(dir, 'managed.json');
    fs.writeFileSync(p, '{ broken', 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    assert.throws(() => loadManagedConfig(), ManagedConfigError);
  });

  it('strips JSONC comments in the managed file too', () => {
    const dir = mkTemp();
    const p = path.join(dir, 'managed.json');
    fs.writeFileSync(p, '{ "model": "m" /* ok */ }', 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = p;
    assert.equal(loadManagedConfig()!.model, 'm');
  });
});

// ---------------------------------------------------------------------------
// mergeConfigs — layer precedence & semantics
// ---------------------------------------------------------------------------

describe('mergeConfigs', () => {
  it('returns an empty object when every layer is empty/null', () => {
    assert.deepEqual(mergeConfigs({}, {}, null, null), {});
  });

  it('project overrides global for the same key', () => {
    const merged = mergeConfigs({}, {}, { model: 'proj' }, { model: 'glob' });
    assert.equal(merged.model, 'proj');
  });

  it('global supplies a key that project does not define', () => {
    const merged = mergeConfigs({}, {}, { model: 'proj' }, { provider: 'glob-prov' });
    assert.equal(merged.model, 'proj');
    assert.equal(merged.provider, 'glob-prov');
  });

  it('full precedence: cli > env > local > project > global', () => {
    const merged = mergeConfigs(
      { model: 'cli' },
      { model: 'env', provider: 'env' },
      { model: 'proj', provider: 'proj', baseUrl: 'proj' },
      { model: 'glob', provider: 'glob', baseUrl: 'glob', thinkingLevel: 'glob' },
      { model: 'local', provider: 'local' },
    );
    assert.equal(merged.model, 'cli');      // cli wins
    assert.equal(merged.provider, 'env');   // env beats local/project/global
    assert.equal(merged.baseUrl, 'proj');   // project beats global
    assert.equal(merged.thinkingLevel, 'glob'); // only global had it
  });

  it('local beats project but loses to env/cli', () => {
    const merged = mergeConfigs(
      {},
      { maxTurns: 3 },
      { maxTurns: 1, skill: 'proj-skill' },
      {},
      { maxTurns: 2, skill: 'local-skill' },
    );
    assert.equal(merged.maxTurns, 3);        // env wins
    assert.equal(merged.skill, 'local-skill'); // local beats project
  });

  it('undefined values do not clobber a lower-layer defined value', () => {
    const merged = mergeConfigs(
      { model: undefined },
      {},
      { model: 'proj' },
      null,
    );
    assert.equal(merged.model, 'proj');
  });

  it('object fields are replaced wholesale by the higher layer (not deep-merged)', () => {
    const merged = mergeConfigs(
      {},
      {},
      { superinference: { enforcing: true } },
      { superinference: { enforcing: false, useLLMCritic: true } },
    );
    // project layer wins entirely; global's useLLMCritic is dropped
    assert.deepEqual(merged.superinference, { enforcing: true });
  });

  it('array fields are replaced wholesale by the higher layer', () => {
    const merged = mergeConfigs(
      {},
      {},
      { fallbackModels: ['p1', 'p2'] },
      { fallbackModels: ['g1'] },
    );
    assert.deepEqual(merged.fallbackModels, ['p1', 'p2']);
  });

  it('managed enforceAvailableModels is authoritative and NOT overridable by cli/env/project', () => {
    const merged = mergeConfigs(
      { enforceAvailableModels: ['cli-model'] },
      { enforceAvailableModels: ['env-model'] },
      { enforceAvailableModels: ['proj-model'] },
      { enforceAvailableModels: ['glob-model'] },
      { enforceAvailableModels: ['local-model'] },
      { enforceAvailableModels: ['managed-only'] },
    );
    assert.deepEqual(merged.enforceAvailableModels, ['managed-only']);
  });

  it('managed allowedMcpServers + managedMcpServers are authoritative', () => {
    const merged = mergeConfigs(
      {},
      {},
      { allowedMcpServers: ['proj'] },
      null,
      { allowedMcpServers: ['local'] },
      { allowedMcpServers: ['mgr'], managedMcpServers: { s: { on: true } } },
    );
    assert.deepEqual(merged.allowedMcpServers, ['mgr']);
    assert.deepEqual(merged.managedMcpServers, { s: { on: true } });
  });

  it('without a managed layer, allowedMcpServers follows normal precedence', () => {
    const merged = mergeConfigs(
      {},
      {},
      { allowedMcpServers: ['proj'] },
      null,
      { allowedMcpServers: ['local'] },
    );
    assert.deepEqual(merged.allowedMcpServers, ['local']);
  });

  it('merges many distinct scalar keys across layers simultaneously', () => {
    const merged = mergeConfigs(
      { permissionMode: 'auto-allow' },
      { tokenBudget: 1000 },
      { maxBudgetUsd: 5, safeMode: true, cacheTtl: '1h' },
      { statusLine: 'echo hi', bashOutputMaxChars: 42 },
    );
    assert.equal(merged.permissionMode, 'auto-allow');
    assert.equal(merged.tokenBudget, 1000);
    assert.equal(merged.maxBudgetUsd, 5);
    assert.equal(merged.safeMode, true);
    assert.equal(merged.cacheTtl, '1h');
    assert.equal(merged.statusLine, 'echo hi');
    assert.equal(merged.bashOutputMaxChars, 42);
  });

  it('round-trips loaded-from-disk configs through mergeConfigs', () => {
    const projDir = mkTemp();
    const homeDir = mkTemp();
    writeAmi(projDir, 'config.json', JSON.stringify({ model: 'proj-m', maxTurns: 9 }));
    writeAmi(homeDir, 'config.json', JSON.stringify({ model: 'glob-m', provider: 'glob-p' }));
    const project = loadProjectConfig(projDir);

    const savedHome = process.env.HOME;
    process.env.HOME = homeDir;
    const global = loadGlobalConfig();
    if (savedHome === undefined) delete process.env.HOME; else process.env.HOME = savedHome;

    const merged = mergeConfigs({}, {}, project, global);
    assert.equal(merged.model, 'proj-m');   // project wins
    assert.equal(merged.maxTurns, 9);        // project only
    assert.equal(merged.provider, 'glob-p'); // global only
  });
});

// ---------------------------------------------------------------------------
// stripJsoncComments — context-aware stripping
// ---------------------------------------------------------------------------

describe('stripJsoncComments', () => {
  it('removes a line comment', () => {
    const out = stripJsoncComments('{"a":1} // tail');
    assert.deepEqual(JSON.parse(out), { a: 1 });
  });

  it('removes a block comment', () => {
    const out = stripJsoncComments('{"a":/* mid */1}');
    assert.deepEqual(JSON.parse(out), { a: 1 });
  });

  it('preserves // inside a string literal (URL safety)', () => {
    const out = stripJsoncComments('{"u":"http://x/y"}');
    assert.deepEqual(JSON.parse(out), { u: 'http://x/y' });
  });

  it('preserves escaped quotes inside strings', () => {
    const out = stripJsoncComments('{"s":"a\\"b"}');
    assert.deepEqual(JSON.parse(out), { s: 'a"b' });
  });

  it('is a no-op on comment-free input', () => {
    const src = '{"x":[1,2,3],"y":{"z":true}}';
    assert.equal(stripJsoncComments(src), src);
  });

  it('handles an empty string', () => {
    assert.equal(stripJsoncComments(''), '');
  });
});
