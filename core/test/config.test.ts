import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { stripJsoncComments, loadProjectConfig, loadGlobalConfig, mergeConfigs, ConfigService, type ProjectConfig } from '../src/config';

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
    const configDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(path.join(configDir, 'config.json'), JSON.stringify({ model: 'gpt-4o', maxTurns: 5 }));
    const result = loadProjectConfig(tmpDir);
    assert.notEqual(result, null);
    assert.equal(result!.model, 'gpt-4o');
    assert.equal(result!.maxTurns, 5);
  });

  it('loads config with JSONC comments', () => {
    const configDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(path.join(configDir, 'config.json'), '{\n  // comment\n  "model": "claude-opus-4"\n}');
    const result = loadProjectConfig(tmpDir);
    assert.notEqual(result, null);
    assert.equal(result!.model, 'claude-opus-4');
  });

  it('returns null on invalid JSON', () => {
    const configDir = path.join(tmpDir, '.superinference');
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
    const configDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(path.join(configDir, 'config.json'), '{"provider": "anthropic"}');
    const result = loadGlobalConfig();
    assert.notEqual(result, null);
    assert.equal(result!.provider, 'anthropic');
  });

  it('falls back to USERPROFILE when HOME is not set', () => {
    delete process.env.HOME;
    process.env.USERPROFILE = tmpDir;
    const configDir = path.join(tmpDir, '.superinference');
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
    const cli: Partial<ProjectConfig> = { fallbackModel: 'fb', compactionModel: 'cm', maxTurns: 3, tokenBudget: 5000, persona: 'dev' };
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
    assert.equal(result.persona, 'dev');
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

  it('loads config from .superinference/config.json', () => {
    const dir = path.join(tmpDir, '.superinference');
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
    const dir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "m1"}');

    const svc = new ConfigService(tmpDir);
    const c1 = svc.get();
    c1.model = 'mutated';
    assert.equal(svc.get().model, 'm1');
  });

  it('reload() detects changes and notifies listeners', () => {
    const dir = path.join(tmpDir, '.superinference');
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
    const dir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "same"}');

    const svc = new ConfigService(tmpDir);
    assert.equal(svc.reload(), false);
  });

  it('onChange() returns unsubscribe function', () => {
    const dir = path.join(tmpDir, '.superinference');
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
    const dir = path.join(tmpDir, '.superinference');
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

  it('handles listener errors gracefully', () => {
    const dir = path.join(tmpDir, '.superinference');
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
