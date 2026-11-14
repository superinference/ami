import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { stripJsoncComments, loadProjectConfig, loadGlobalConfig, mergeConfigs, ConfigService, applyConfigEnv, CONFIG_ENV_TIERS, loadConfigFileChecked, backupCorruptConfig, findMostRecentBackup, collectConfigDiagnostics, validateProjectConfigValues, type ProjectConfig } from '../src/config';

// ---------------------------------------------------------------------------
// stripJsoncComments
// ---------------------------------------------------------------------------
describe('stripJsoncComments', () => {
  it('returns plain JSON unchanged', () => {
    const input = '{"key": "value"}';
    assert.equal(stripJsoncComments(input), input);
  });

  it('strips single-line comments', () => {
    const input = '{\n  "key": "value" // this is a comment\n}';
    const result = stripJsoncComments(input);
    assert.ok(!result.includes('//'));
    assert.ok(result.includes('"key"'));
  });

  it('strips block comments', () => {
    const input = '{\n  /* block comment */\n  "key": "value"\n}';
    const result = stripJsoncComments(input);
    assert.ok(!result.includes('/*'));
    assert.ok(!result.includes('*/'));
    assert.ok(result.includes('"key"'));
  });

  it('preserves URLs inside strings (does not treat // as comment inside strings)', () => {
    const input = '{"url": "https://example.com/path"}';
    const result = stripJsoncComments(input);
    assert.equal(result, input);
  });

  it('preserves /* inside strings', () => {
    const input = '{"note": "use /* carefully */"}';
    const result = stripJsoncComments(input);
    assert.equal(result, input);
  });

  it('handles escaped quotes inside strings', () => {
    const input = '{"msg": "say \\"hello\\""}';
    const result = stripJsoncComments(input);
    assert.equal(result, input);
  });

  it('handles backslash at end of string value', () => {
    const input = '{"path": "C:\\\\dir\\\\"}';
    const result = stripJsoncComments(input);
    assert.equal(result, input);
  });

  it('handles multiline block comment', () => {
    const input = '{\n  /*\n   * multi\n   * line\n   */\n  "key": 1\n}';
    const result = stripJsoncComments(input);
    assert.ok(!result.includes('multi'));
    assert.ok(result.includes('"key"'));
  });

  it('strips comment that fills entire line', () => {
    const input = '// comment\n{"a": 1}';
    const result = stripJsoncComments(input);
    const parsed = JSON.parse(result);
    assert.equal(parsed.a, 1);
  });

  it('handles empty string', () => {
    assert.equal(stripJsoncComments(''), '');
  });

  it('handles string with only a comment', () => {
    const result = stripJsoncComments('// just a comment');
    assert.equal(result.trim(), '');
  });

  it('handles slash not followed by / or *', () => {
    // A lone slash (not in a comment) should be preserved
    const input = '{"math": "5/3"}';
    const result = stripJsoncComments(input);
    assert.equal(result, input);
  });

  it('handles multiple single-line comments', () => {
    const input = '{\n// comment 1\n"a": 1,\n// comment 2\n"b": 2\n}';
    const result = stripJsoncComments(input);
    const parsed = JSON.parse(result);
    assert.equal(parsed.a, 1);
    assert.equal(parsed.b, 2);
  });

  it('handles block comment at the very end without newline', () => {
    const input = '{"a":1}/* trailing */';
    const result = stripJsoncComments(input);
    const parsed = JSON.parse(result);
    assert.equal(parsed.a, 1);
  });
});

// ---------------------------------------------------------------------------
// loadProjectConfig
// ---------------------------------------------------------------------------
describe('loadProjectConfig', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-config-test-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns null when config file does not exist', () => {
    const result = loadProjectConfig(tmpDir);
    assert.equal(result, null);
  });

  it('loads valid config', () => {
    const configDir = path.join(tmpDir, '.ami');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(path.join(configDir, 'config.json'), JSON.stringify({ model: 'gpt-4o', maxTurns: 5 }));
    const result = loadProjectConfig(tmpDir);
    assert.notEqual(result, null);
    assert.equal(result!.model, 'gpt-4o');
    assert.equal(result!.maxTurns, 5);
  });

  it('loads config with JSONC comments', () => {
    const configDir = path.join(tmpDir, '.ami');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(path.join(configDir, 'config.json'), '{\n  // comment\n  "model": "claude-opus-4"\n}');
    const result = loadProjectConfig(tmpDir);
    assert.notEqual(result, null);
    assert.equal(result!.model, 'claude-opus-4');
  });

  it('returns null on invalid JSON', () => {
    const configDir = path.join(tmpDir, '.ami');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(path.join(configDir, 'config.json'), '{invalid json!!!');
    const result = loadProjectConfig(tmpDir);
    assert.equal(result, null);
  });
});

// ---------------------------------------------------------------------------
// loadGlobalConfig
// ---------------------------------------------------------------------------
describe('loadGlobalConfig', () => {
  let originalHome: string | undefined;
  let originalUserProfile: string | undefined;
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-global-config-'));
    originalHome = process.env.HOME;
    originalUserProfile = process.env.USERPROFILE;
  });

  afterEach(() => {
    if (originalHome !== undefined) process.env.HOME = originalHome;
    else delete process.env.HOME;
    if (originalUserProfile !== undefined) process.env.USERPROFILE = originalUserProfile;
    else delete process.env.USERPROFILE;
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns null when HOME is empty', () => {
    delete process.env.HOME;
    delete process.env.USERPROFILE;
    const result = loadGlobalConfig();
    assert.equal(result, null);
  });

  it('returns null when config does not exist', () => {
    process.env.HOME = tmpDir;
    const result = loadGlobalConfig();
    assert.equal(result, null);
  });

  it('loads valid global config', () => {
    process.env.HOME = tmpDir;
    const configDir = path.join(tmpDir, '.ami');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(path.join(configDir, 'config.json'), '{"provider": "anthropic"}');
    const result = loadGlobalConfig();
    assert.notEqual(result, null);
    assert.equal(result!.provider, 'anthropic');
  });

  it('falls back to USERPROFILE when HOME is not set', () => {
    delete process.env.HOME;
    process.env.USERPROFILE = tmpDir;
    const configDir = path.join(tmpDir, '.ami');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(path.join(configDir, 'config.json'), '{"model": "gpt-4o"}');
    const result = loadGlobalConfig();
    assert.notEqual(result, null);
    assert.equal(result!.model, 'gpt-4o');
  });
});

// ---------------------------------------------------------------------------
// mergeConfigs
// ---------------------------------------------------------------------------
describe('mergeConfigs', () => {
  it('returns empty config when all sources are empty', () => {
    const result = mergeConfigs({}, {}, null, null);
    assert.deepEqual(result, {});
  });

  it('uses global config as baseline', () => {
    const global: ProjectConfig = { model: 'global-model', provider: 'openai' };
    const result = mergeConfigs({}, {}, null, global);
    assert.equal(result.model, 'global-model');
    assert.equal(result.provider, 'openai');
  });

  it('preserves safeMode/maxWebSearchesPerSession/deferTools/alwaysLoadTools/fallbackModels', () => {
    const project: ProjectConfig = {
      safeMode: true,
      maxWebSearchesPerSession: 3,
      deferTools: true,
      alwaysLoadTools: ['grep'],
      fallbackModels: ['m1', 'm2'],
    };
    const result = mergeConfigs({}, {}, project, null);
    assert.equal(result.safeMode, true);
    assert.equal(result.maxWebSearchesPerSession, 3);
    assert.equal(result.deferTools, true);
    assert.deepEqual(result.alwaysLoadTools, ['grep']);
    assert.deepEqual(result.fallbackModels, ['m1', 'm2']);
  });

  it('applies the new fields through higher-priority layers (cli/local override project)', () => {
    const project: ProjectConfig = { safeMode: false, maxWebSearchesPerSession: 1, deferTools: false, alwaysLoadTools: ['a'], fallbackModels: ['p'] };
    const local: ProjectConfig = { safeMode: true, deferTools: true };
    const cli: Partial<ProjectConfig> = { maxWebSearchesPerSession: 9, alwaysLoadTools: ['b'], fallbackModels: ['c'] };
    const result = mergeConfigs(cli, {}, project, null, local);
    assert.equal(result.safeMode, true, 'local overrides project');
    assert.equal(result.deferTools, true, 'local overrides project');
    assert.equal(result.maxWebSearchesPerSession, 9, 'cli overrides project');
    assert.deepEqual(result.alwaysLoadTools, ['b'], 'cli overrides project');
    assert.deepEqual(result.fallbackModels, ['c'], 'cli overrides project');
  });

  it('merges maxBudgetUsd and lets higher-priority layers override it', () => {
    const project: ProjectConfig = { maxBudgetUsd: 10 };
    assert.equal(mergeConfigs({}, {}, project, null).maxBudgetUsd, 10);
    const cli: Partial<ProjectConfig> = { maxBudgetUsd: 2 };
    assert.equal(mergeConfigs(cli, {}, project, null).maxBudgetUsd, 2, 'cli overrides project');
  });

  it('merges the superinference reflection-gate block', () => {
    const project: ProjectConfig = { superinference: { enforcing: true, useLLMCritic: true } };
    const result = mergeConfigs({}, {}, project, null);
    assert.deepEqual(result.superinference, { enforcing: true, useLLMCritic: true });
  });

  it('merges the statusLine custom command (SL1)', () => {
    const project: ProjectConfig = { statusLine: 'echo hi' };
    const result = mergeConfigs({}, {}, project, null);
    assert.equal(result.statusLine, 'echo hi');
    // higher layer overrides
    const cli: Partial<ProjectConfig> = { statusLine: 'echo bye' };
    assert.equal(mergeConfigs(cli, {}, project, null).statusLine, 'echo bye');
  });

  it('merges sandboxExcludedCommands across layers (audit #4)', () => {
    const project: ProjectConfig = { sandboxExcludedCommands: ['git', 'ls'] };
    assert.deepEqual(mergeConfigs({}, {}, project, null).sandboxExcludedCommands, ['git', 'ls']);
    const cli: Partial<ProjectConfig> = { sandboxExcludedCommands: ['docker'] };
    assert.deepEqual(mergeConfigs(cli, {}, project, null).sandboxExcludedCommands, ['docker']);
  });

  it('project config overrides global config', () => {
    const global: ProjectConfig = { model: 'global-model', provider: 'openai' };
    const project: ProjectConfig = { model: 'project-model' };
    const result = mergeConfigs({}, {}, project, global);
    assert.equal(result.model, 'project-model');
    assert.equal(result.provider, 'openai'); // preserved from global
  });

  it('env vars override project config', () => {
    const project: ProjectConfig = { model: 'project-model', maxTurns: 10 };
    const env: Partial<ProjectConfig> = { model: 'env-model' };
    const result = mergeConfigs({}, env, project, null);
    assert.equal(result.model, 'env-model');
    assert.equal(result.maxTurns, 10); // preserved from project
  });

  it('CLI args have highest priority', () => {
    const global: ProjectConfig = { model: 'global' };
    const project: ProjectConfig = { model: 'project' };
    const env: Partial<ProjectConfig> = { model: 'env' };
    const cli: Partial<ProjectConfig> = { model: 'cli' };
    const result = mergeConfigs(cli, env, project, global);
    assert.equal(result.model, 'cli');
  });

  it('merges all fields across layers', () => {
    const global: ProjectConfig = { model: 'g', provider: 'openai' };
    const project: ProjectConfig = { baseUrl: 'http://localhost', permissionMode: 'ask' };
    const env: Partial<ProjectConfig> = { thinkingLevel: 'high' };
    const cli: Partial<ProjectConfig> = { fallbackModel: 'fb', compactionModel: 'cm', maxTurns: 3, tokenBudget: 5000, skill: 'dev' };
    const result = mergeConfigs(cli, env, project, global);
    assert.equal(result.model, 'g');
    assert.equal(result.provider, 'openai');
    assert.equal(result.baseUrl, 'http://localhost');
    assert.equal(result.permissionMode, 'ask');
    assert.equal(result.thinkingLevel, 'high');
    assert.equal(result.fallbackModel, 'fb');
    assert.equal(result.compactionModel, 'cm');
    assert.equal(result.maxTurns, 3);
    assert.equal(result.tokenBudget, 5000);
    assert.equal(result.skill, 'dev');
  });

  it('ignores undefined values in higher layers', () => {
    const global: ProjectConfig = { model: 'global-model' };
    const cli: Partial<ProjectConfig> = { model: undefined };
    const result = mergeConfigs(cli, {}, null, global);
    assert.equal(result.model, 'global-model');
  });

  it('handles null project and global configs', () => {
    const cli: Partial<ProjectConfig> = { model: 'only-cli' };
    const result = mergeConfigs(cli, {}, null, null);
    assert.equal(result.model, 'only-cli');
  });
});

// ---------------------------------------------------------------------------
// ConfigService
// ---------------------------------------------------------------------------

describe('ConfigService', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-config-svc-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('loads config from .ami/config.json', () => {
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "test-model"}');

    const svc = new ConfigService(tmpDir);
    assert.equal(svc.get().model, 'test-model');
  });

  it('returns empty config when file does not exist', () => {
    const svc = new ConfigService(tmpDir);
    const config = svc.get();
    assert.equal(config.model, undefined);
  });

  it('returns defensive copy from get()', () => {
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "m1"}');

    const svc = new ConfigService(tmpDir);
    const c1 = svc.get();
    c1.model = 'mutated';
    assert.equal(svc.get().model, 'm1');
  });

  it('reload() detects changes and notifies listeners', () => {
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v1"}');

    const svc = new ConfigService(tmpDir);
    let notified = false;
    let receivedNew: ProjectConfig | null = null;
    let receivedOld: ProjectConfig | null = null;

    svc.onChange((n, o) => { notified = true; receivedNew = n; receivedOld = o; });

    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v2"}');
    const changed = svc.reload();

    assert.equal(changed, true);
    assert.equal(notified, true);
    assert.equal(receivedNew!.model, 'v2');
    assert.equal(receivedOld!.model, 'v1');
  });

  it('reload() returns false when config unchanged', () => {
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "same"}');

    const svc = new ConfigService(tmpDir);
    assert.equal(svc.reload(), false);
  });

  it('onChange() returns unsubscribe function', () => {
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v1"}');

    const svc = new ConfigService(tmpDir);
    let callCount = 0;
    const unsub = svc.onChange(() => { callCount++; });

    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v2"}');
    svc.reload();
    assert.equal(callCount, 1);

    unsub();
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v3"}');
    svc.reload();
    assert.equal(callCount, 1);
  });

  it('stop() cleans up watcher', () => {
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{}');

    const svc = new ConfigService(tmpDir);
    svc.watch();
    svc.stop();
    // Should not throw
    svc.stop();
  });

  it('watch() is safe when directory does not exist', () => {
    const svc = new ConfigService(path.join(tmpDir, 'nonexistent'));
    svc.watch(); // should not throw
    svc.stop();
  });

  it('watch() triggers reload on config file change', async () => {
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "before"}');

    const svc = new ConfigService(tmpDir);
    let notified = false;
    svc.onChange(() => { notified = true; });
    svc.watch();

    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "after"}');
    await new Promise(r => setTimeout(r, 600));
    svc.stop();
    assert.equal(svc.get().model, 'after');
    assert.equal(notified, true);
  });

  it('watch() debounces rapid config changes', async () => {
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v0"}');

    const svc = new ConfigService(tmpDir);
    let changeCount = 0;
    svc.onChange(() => { changeCount++; });
    svc.watch();

    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v1"}');
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v2"}');
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v3"}');
    await new Promise(r => setTimeout(r, 600));
    svc.stop();
    assert.equal(svc.get().model, 'v3');
    assert.ok(changeCount <= 3, 'debounce should limit notifications');
  });

  it('handles listener errors gracefully', () => {
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v1"}');

    const svc = new ConfigService(tmpDir);
    svc.onChange(() => { throw new Error('listener error'); });

    let secondCalled = false;
    svc.onChange(() => { secondCalled = true; });

    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v2"}');
    svc.reload();
    assert.equal(secondCalled, true);
  });
});

describe('mergeConfigs — managed allowlist + bashOutputMaxChars', () => {
  it('carries managed enforceAvailableModels and makes it NON-overridable by cli/project', () => {
    const merged = mergeConfigs(
      { enforceAvailableModels: ['cli-tries-this'] }, // cliArgs (should NOT win)
      {},
      { enforceAvailableModels: ['project-tries'] },  // projectConfig (should NOT win)
      null, null,
      { enforceAvailableModels: ['managed-x', 'managed-y'] }, // managedConfig (authoritative)
    );
    assert.deepEqual(merged.enforceAvailableModels, ['managed-x', 'managed-y']);
  });
  it('merges bashOutputMaxChars from project config (overridable)', () => {
    const merged = mergeConfigs({}, {}, { bashOutputMaxChars: 4096 }, null);
    assert.equal(merged.bashOutputMaxChars, 4096);
  });
  it('merges cacheTtl from config so it is reachable', () => {
    const merged = mergeConfigs({}, {}, { cacheTtl: '1h' }, null);
    assert.equal(merged.cacheTtl, '1h');
  });
  it('merges maxEffortLevel from project config', () => {
    const merged = mergeConfigs({}, {}, { maxEffortLevel: 'high' }, null);
    assert.equal(merged.maxEffortLevel, 'high');
  });
  it('CLI maxEffortLevel overrides project config', () => {
    const merged = mergeConfigs({ maxEffortLevel: 'low' }, {}, { maxEffortLevel: 'high' }, null);
    assert.equal(merged.maxEffortLevel, 'low');
  });

  it('merges env blocks key-wise across layers (higher priority wins per key)', () => {
    const merged = mergeConfigs(
      {},
      {},
      { env: { A: 'proj', B: 'proj' } },
      { env: { A: 'global', C: 'global' } },
      { env: { B: 'local' } },
    );
    assert.deepEqual(merged.env, { A: 'proj', B: 'local', C: 'global' });
  });
});

// ---------------------------------------------------------------------------
// applyConfigEnv — trust-tiered settings env application
// ---------------------------------------------------------------------------
describe('applyConfigEnv', () => {
  it('CONFIG_ENV_TIERS maps project/local to untrusted, managed/global to trusted', () => {
    const map = Object.fromEntries(CONFIG_ENV_TIERS);
    assert.equal(map.managed, 'trusted');
    assert.equal(map.global, 'trusted');
    assert.equal(map.project, 'project');
    assert.equal(map.local, 'project');
  });

  it('applies trusted-source env (incl. routing vars) but filters project-source to safe allowlist', () => {
    const target: NodeJS.ProcessEnv = {};
    const result = applyConfigEnv(
      {
        global: { env: { ANTHROPIC_BASE_URL: 'http://corp', BASH_MAX_TIMEOUT_MS: '10' } },
        project: { env: { ANTHROPIC_BASE_URL: 'http://evil', BASH_MAX_OUTPUT_LENGTH: '5' } },
      },
      target,
    );
    // trusted global routing var applied; project routing var rejected
    assert.equal(target.ANTHROPIC_BASE_URL, 'http://corp');
    assert.equal(target.BASH_MAX_TIMEOUT_MS, '10');
    assert.equal(target.BASH_MAX_OUTPUT_LENGTH, '5');
    assert.ok(result.rejected.some(r => r.key === 'ANTHROPIC_BASE_URL' && r.tier === 'project' && r.reason === 'unsafe-untrusted'));
    assert.equal(result.applied.ANTHROPIC_BASE_URL, 'http://corp');
  });

  it('higher-trust source wins on key collisions', () => {
    const target: NodeJS.ProcessEnv = {};
    applyConfigEnv(
      {
        managed: { env: { BASH_MAX_TIMEOUT_MS: 'managed' } },
        global: { env: { BASH_MAX_TIMEOUT_MS: 'global' } },
      },
      target,
    );
    assert.equal(target.BASH_MAX_TIMEOUT_MS, 'managed');
  });

  it('strips provider vars from all sources when host owns routing', () => {
    const target: NodeJS.ProcessEnv = { SUPERINFERENCE_PROVIDER_MANAGED_BY_HOST: '1' };
    const result = applyConfigEnv({ global: { env: { ANTHROPIC_BASE_URL: 'http://corp', BASH_MAX_TIMEOUT_MS: '10' } } }, target);
    assert.equal(target.ANTHROPIC_BASE_URL, undefined);
    assert.equal(target.BASH_MAX_TIMEOUT_MS, '10');
    assert.ok(result.rejected.some(r => r.reason === 'provider-managed'));
  });

  it('no-op for layers without env', () => {
    const target: NodeJS.ProcessEnv = {};
    const result = applyConfigEnv({ project: { model: 'x' }, global: null }, target);
    assert.deepEqual(result.applied, {});
    assert.deepEqual(Object.keys(target), []);
  });
});

// ---------------------------------------------------------------------------
// Config corruption recovery
// ---------------------------------------------------------------------------
describe('loadConfigFileChecked / corruption recovery', () => {
  let dir: string;
  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-corrupt-')); });
  afterEach(() => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ } });

  it('absent file → null config, no diagnostic', () => {
    const r = loadConfigFileChecked(path.join(dir, 'nope.json'));
    assert.equal(r.config, null);
    assert.equal(r.diagnostic, undefined);
  });

  it('valid file → parsed config, no diagnostic', () => {
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'gpt-4o' }));
    const r = loadConfigFileChecked(p);
    assert.deepEqual(r.config, { model: 'gpt-4o' });
    assert.equal(r.diagnostic, undefined);
  });

  it('corrupt JSON → diagnostic + the corrupt file is backed up (not lost)', () => {
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, '{ this is not json ');
    const r = loadConfigFileChecked(p);
    assert.equal(r.config, null);
    assert.ok(r.diagnostic);
    assert.equal(r.diagnostic!.kind, 'parse-error');
    assert.ok(r.diagnostic!.backupPath);
    assert.equal(fs.readFileSync(r.diagnostic!.backupPath!, 'utf-8'), '{ this is not json ');
  });

  it('non-object JSON (array) → not-object diagnostic', () => {
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, '[1,2,3]');
    const r = loadConfigFileChecked(p);
    assert.equal(r.config, null);
    assert.equal(r.diagnostic!.kind, 'not-object');
  });

  it('findMostRecentBackup returns the newest .corrupt- sibling', () => {
    const p = path.join(dir, 'config.json');
    assert.equal(findMostRecentBackup(p), null);
    fs.writeFileSync(`${p}.corrupt-2026-01-01T00-00-00-000Z`, 'a');
    fs.writeFileSync(`${p}.corrupt-2026-02-01T00-00-00-000Z`, 'b');
    assert.equal(findMostRecentBackup(p), `${p}.corrupt-2026-02-01T00-00-00-000Z`);
  });

  it('backupCorruptConfig writes a timestamped sibling', () => {
    const p = path.join(dir, 'config.json');
    const backup = backupCorruptConfig(p, 'raw');
    assert.ok(backup && backup.startsWith(`${p}.corrupt-`));
    assert.equal(fs.readFileSync(backup!, 'utf-8'), 'raw');
  });

  it('collectConfigDiagnostics reports a corrupt project config with a backup pointer', () => {
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'config.json'), 'not json {{{');
    const diags = collectConfigDiagnostics(dir);
    assert.ok(diags.length >= 1);
    const projDiag = diags.find(d => d.path.endsWith(path.join('.ami', 'config.json')));
    assert.ok(projDiag);
    assert.ok(projDiag!.backupPath);
  });

  it('valid JSON object with a bad VALUE → config still loads + invalid-value diagnostic', () => {
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'gpt-4o', permissionMode: 'wide-open', maxTurns: -3 }));
    const r = loadConfigFileChecked(p);
    // The valid parts still load — validation is advisory, not fatal.
    assert.equal(r.config!.model, 'gpt-4o');
    assert.ok(r.diagnostic);
    assert.equal(r.diagnostic!.kind, 'invalid-value');
    const fields = (r.diagnostic!.issues ?? []).map(i => i.field).sort();
    assert.deepEqual(fields, ['maxTurns', 'permissionMode']);
    assert.match(r.diagnostic!.message, /permissionMode/);
  });

  it('collectConfigDiagnostics surfaces invalid-value diagnostics across tiers', () => {
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'config.json'), JSON.stringify({ cacheTtl: '2h' }));
    const diags = collectConfigDiagnostics(dir);
    const d = diags.find(x => x.path.endsWith(path.join('.ami', 'config.json')));
    assert.ok(d);
    assert.equal(d!.kind, 'invalid-value');
    assert.equal(d!.issues![0]!.field, 'cacheTtl');
  });
});

describe('validateProjectConfigValues', () => {
  it('returns no issues for a valid config', () => {
    assert.deepEqual(
      validateProjectConfigValues({ permissionMode: 'ask', cacheTtl: '1h', maxTurns: 10, tokenBudget: 1000, maxBudgetUsd: 5 }),
      [],
    );
  });

  it('returns no issues for an empty config', () => {
    assert.deepEqual(validateProjectConfigValues({}), []);
  });

  it('flags an invalid permissionMode enum', () => {
    const issues = validateProjectConfigValues({ permissionMode: 'nope' as ProjectConfig['permissionMode'] });
    assert.equal(issues.length, 1);
    assert.equal(issues[0]!.field, 'permissionMode');
    assert.match(issues[0]!.message, /ask, auto-allow, deny-all/);
  });

  it('flags an invalid cacheTtl enum', () => {
    const issues = validateProjectConfigValues({ cacheTtl: '10m' as ProjectConfig['cacheTtl'] });
    assert.equal(issues.length, 1);
    assert.equal(issues[0]!.field, 'cacheTtl');
  });

  it('flags out-of-range numeric knobs via the canonical validateEngineConfig', () => {
    const issues = validateProjectConfigValues({ maxTurns: -1, tokenBudget: 0, maxBudgetUsd: -5 });
    const fields = issues.map(i => i.field).sort();
    assert.deepEqual(fields, ['maxBudgetUsd', 'maxTurns', 'tokenBudget']);
  });

  it('does not flag valid numeric knobs', () => {
    assert.deepEqual(validateProjectConfigValues({ maxTurns: 50, tokenBudget: 200000, maxBudgetUsd: 10 }), []);
  });
});
