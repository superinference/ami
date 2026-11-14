import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  configTool,
  SUPPORTED_SETTINGS,
  isSupported,
  getOptionsForSetting,
  getSettingPath,
  getAllSettingKeys,
} from '../src/tools/config-tool';
import type { ToolContext } from '../src/types';

describe('configTool', () => {
  let tmpDir: string;
  let ctx: ToolContext;

  const freshCtx = () => {
    const d = fs.mkdtempSync(path.join(tmpDir, 'c-'));
    return { cwd: d } as ToolContext;
  };

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'config-tool-test-'));
    ctx = { cwd: tmpDir } as ToolContext;
  });

  after(() => { fs.rmSync(tmpDir, { recursive: true }); });

  it('has correct name and schema', () => {
    assert.equal(configTool.name, 'config');
    assert.equal(configTool.isReadOnly, false);
    assert.deepEqual(configTool.inputSchema.required, ['action']);
  });

  it('description dynamically enumerates the supported settings and their options', () => {
    assert.ok(configTool.description.includes('model'));
    assert.ok(configTool.description.includes('permissionMode'));
    // enum options are listed inline
    assert.ok(configTool.description.includes('ask | auto-allow | deny-all'));
    // booleans advertise true|false
    assert.ok(configTool.description.includes('true | false'));
  });

  describe('registry helpers', () => {
    it('isSupported gates keys to the registry', () => {
      assert.equal(isSupported('model'), true);
      assert.equal(isSupported('nope'), false);
      // prototype keys must not leak through
      assert.equal(isSupported('toString'), false);
      assert.equal(isSupported('constructor'), false);
    });
    it('getOptionsForSetting returns a copy or undefined', () => {
      assert.deepEqual(getOptionsForSetting('permissionMode'), ['ask', 'auto-allow', 'deny-all']);
      assert.equal(getOptionsForSetting('model'), undefined);
      assert.equal(getOptionsForSetting('nope'), undefined);
    });
    it('getSettingPath splits dotted keys', () => {
      assert.deepEqual(getSettingPath('model'), ['model']);
      assert.deepEqual(getSettingPath('superinference.enforcing'), ['superinference', 'enforcing']);
      assert.deepEqual(getSettingPath('nope'), ['nope']);
    });
    it('getAllSettingKeys lists every registry key', () => {
      assert.deepEqual(getAllSettingKeys().sort(), Object.keys(SUPPORTED_SETTINGS).sort());
    });
  });

  it('get returns "no config" when config file does not exist', async () => {
    const c = freshCtx();
    const result = await configTool.execute!({ action: 'get' }, c);
    assert.ok(result.output.includes('No project config'));
  });

  it('set creates config file and sets value', async () => {
    const result = await configTool.execute!({ action: 'set', key: 'model', value: 'gpt-4o' }, ctx);
    assert.ok(result.output.includes('model'));
    assert.ok(result.output.includes('gpt-4o'));
    const configPath = path.join(tmpDir, '.ami', 'config.json');
    assert.ok(fs.existsSync(configPath));
    const config = JSON.parse(fs.readFileSync(configPath, 'utf-8'));
    assert.equal(config.model, 'gpt-4o');
  });

  it('get returns full config when no key specified', async () => {
    const result = await configTool.execute!({ action: 'get' }, ctx);
    assert.ok(result.output.includes('model'));
    assert.ok(result.output.includes('gpt-4o'));
    assert.equal(result.metadata?.operation, 'get');
  });

  it('get returns specific key value with metadata', async () => {
    const result = await configTool.execute!({ action: 'get', key: 'model' }, ctx);
    assert.ok(result.output.includes('model'));
    assert.ok(result.output.includes('gpt-4o'));
    assert.equal(result.metadata?.setting, 'model');
  });

  it('set overwrites existing value and reports the previous value', async () => {
    const result = await configTool.execute!({ action: 'set', key: 'model', value: 'claude-3-opus' }, ctx);
    assert.ok(result.output.includes('was'));
    assert.equal(result.metadata?.previousValue, 'gpt-4o');
    assert.equal(result.metadata?.newValue, 'claude-3-opus');
    const got = await configTool.execute!({ action: 'get', key: 'model' }, ctx);
    assert.ok(got.output.includes('claude-3-opus'));
  });

  it('returns error for invalid action usage (set without key)', async () => {
    const result = await configTool.execute!({ action: 'set' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('Usage'));
  });

  it('returns usage error for unknown action', async () => {
    const result = await configTool.execute!({ action: 'delete' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('Usage'));
  });

  describe('unknown-key rejection', () => {
    it('rejects an unknown key on set and lists valid keys', async () => {
      const c = freshCtx();
      const result = await configTool.execute!({ action: 'set', key: 'bogus', value: 'x' }, c);
      assert.ok(result.isError);
      assert.ok(result.output.includes('Unknown setting'));
      assert.ok(result.output.includes('model'));
    });
    it('rejects an unknown key on get', async () => {
      const c = freshCtx();
      await configTool.execute!({ action: 'set', key: 'model', value: 'm' }, c);
      const result = await configTool.execute!({ action: 'get', key: 'bogus' }, c);
      assert.ok(result.isError);
      assert.ok(result.output.includes('Unknown setting'));
    });
  });

  describe('options/enum validation', () => {
    it('rejects an invalid enum value and lists the options', async () => {
      const c = freshCtx();
      const result = await configTool.execute!({ action: 'set', key: 'permissionMode', value: 'wideopen' }, c);
      assert.ok(result.isError);
      assert.ok(result.output.includes('Options: ask, auto-allow, deny-all'));
    });
    it('accepts a valid enum value', async () => {
      const c = freshCtx();
      const result = await configTool.execute!({ action: 'set', key: 'permissionMode', value: 'auto-allow' }, c);
      assert.equal(result.isError, undefined);
      assert.equal(result.metadata?.newValue, 'auto-allow');
    });
  });

  describe('type coercion + validation', () => {
    it('coerces string "true"/"false" to boolean', async () => {
      const c = freshCtx();
      await configTool.execute!({ action: 'set', key: 'safeMode', value: 'true' }, c);
      const cfg = JSON.parse(fs.readFileSync(path.join(c.cwd, '.ami', 'config.json'), 'utf-8'));
      assert.equal(cfg.safeMode, true);
    });
    it('accepts a native boolean value', async () => {
      const c = freshCtx();
      await configTool.execute!({ action: 'set', key: 'deferTools', value: false }, c);
      const cfg = JSON.parse(fs.readFileSync(path.join(c.cwd, '.ami', 'config.json'), 'utf-8'));
      assert.equal(cfg.deferTools, false);
    });
    it('rejects a non-boolean value for a boolean setting', async () => {
      const c = freshCtx();
      const result = await configTool.execute!({ action: 'set', key: 'safeMode', value: 'maybe' }, c);
      assert.ok(result.isError);
      assert.ok(result.output.includes('requires true or false'));
    });
    it('coerces a numeric string to a number', async () => {
      const c = freshCtx();
      await configTool.execute!({ action: 'set', key: 'maxTurns', value: '42' }, c);
      const cfg = JSON.parse(fs.readFileSync(path.join(c.cwd, '.ami', 'config.json'), 'utf-8'));
      assert.equal(cfg.maxTurns, 42);
    });
    it('accepts a native number value', async () => {
      const c = freshCtx();
      await configTool.execute!({ action: 'set', key: 'tokenBudget', value: 1000 }, c);
      const cfg = JSON.parse(fs.readFileSync(path.join(c.cwd, '.ami', 'config.json'), 'utf-8'));
      assert.equal(cfg.tokenBudget, 1000);
    });
    it('rejects a non-numeric value for a number setting', async () => {
      const c = freshCtx();
      const result = await configTool.execute!({ action: 'set', key: 'maxTurns', value: 'lots' }, c);
      assert.ok(result.isError);
      assert.ok(result.output.includes('requires a number'));
    });
    it('coerces boolean/number to string for string settings', async () => {
      const c = freshCtx();
      await configTool.execute!({ action: 'set', key: 'model', value: 123 }, c);
      const cfg = JSON.parse(fs.readFileSync(path.join(c.cwd, '.ami', 'config.json'), 'utf-8'));
      assert.equal(cfg.model, '123');
    });
    it('rejects a non-primitive value for a string setting', async () => {
      const c = freshCtx();
      const result = await configTool.execute!({ action: 'set', key: 'model', value: { a: 1 } }, c);
      assert.ok(result.isError);
      assert.ok(result.output.includes('requires a string'));
    });
  });

  describe('validateOnWrite', () => {
    it('rejects an empty model string', async () => {
      const c = freshCtx();
      const result = await configTool.execute!({ action: 'set', key: 'model', value: '   ' }, c);
      assert.ok(result.isError);
      assert.ok(result.output.includes('non-empty'));
    });
    it('rejects a non-positive number', async () => {
      const c = freshCtx();
      const result = await configTool.execute!({ action: 'set', key: 'maxTurns', value: '0' }, c);
      assert.ok(result.isError);
      assert.ok(result.output.includes('positive'));
    });
  });

  describe('nested dotted keys', () => {
    it('writes a nested key as a nested object', async () => {
      const c = freshCtx();
      await configTool.execute!({ action: 'set', key: 'superinference.enforcing', value: 'true' }, c);
      const cfg = JSON.parse(fs.readFileSync(path.join(c.cwd, '.ami', 'config.json'), 'utf-8'));
      assert.deepEqual(cfg.superinference, { enforcing: true });
    });
    it('reads a nested key back', async () => {
      const c = freshCtx();
      await configTool.execute!({ action: 'set', key: 'superinference.useLLMCritic', value: 'true' }, c);
      const result = await configTool.execute!({ action: 'get', key: 'superinference.useLLMCritic' }, c);
      assert.ok(result.output.includes('true'));
    });
    it('overwriting a non-object parent replaces it with an object', async () => {
      const c = freshCtx();
      fs.mkdirSync(path.join(c.cwd, '.ami'), { recursive: true });
      fs.writeFileSync(path.join(c.cwd, '.ami', 'config.json'), JSON.stringify({ superinference: 'scalar' }));
      await configTool.execute!({ action: 'set', key: 'superinference.enforcing', value: 'true' }, c);
      const cfg = JSON.parse(fs.readFileSync(path.join(c.cwd, '.ami', 'config.json'), 'utf-8'));
      assert.deepEqual(cfg.superinference, { enforcing: true });
    });
  });

  describe('reset-to-default', () => {
    it('deletes a top-level key and prunes', async () => {
      const c = freshCtx();
      await configTool.execute!({ action: 'set', key: 'model', value: 'm' }, c);
      const result = await configTool.execute!({ action: 'set', key: 'model', value: 'default' }, c);
      assert.ok(result.output.includes('Reset'));
      const cfg = JSON.parse(fs.readFileSync(path.join(c.cwd, '.ami', 'config.json'), 'utf-8'));
      assert.equal('model' in cfg, false);
    });
    it('deletes a nested key and prunes the now-empty parent', async () => {
      const c = freshCtx();
      await configTool.execute!({ action: 'set', key: 'superinference.enforcing', value: 'true' }, c);
      await configTool.execute!({ action: 'set', key: 'superinference.enforcing', value: 'default' }, c);
      const cfg = JSON.parse(fs.readFileSync(path.join(c.cwd, '.ami', 'config.json'), 'utf-8'));
      assert.equal('superinference' in cfg, false);
    });
    it('is a no-op when the key is already at default', async () => {
      const c = freshCtx();
      const result = await configTool.execute!({ action: 'set', key: 'model', value: 'default' }, c);
      assert.ok(result.output.includes('already at its default'));
    });
  });

  describe('no-op short-circuit', () => {
    it('reports no change when the value is unchanged', async () => {
      const c = freshCtx();
      await configTool.execute!({ action: 'set', key: 'model', value: 'm' }, c);
      const result = await configTool.execute!({ action: 'set', key: 'model', value: 'm' }, c);
      assert.ok(result.output.includes('no change'));
    });
  });
});
