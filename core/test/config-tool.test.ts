import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { configTool } from '../src/tools/config-tool';
import type { ToolContext } from '../src/types';

describe('configTool', () => {
  let tmpDir: string;
  let ctx: ToolContext;

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

  it('get returns "no config" when config file does not exist', async () => {
    const result = await configTool.execute!({ action: 'get' }, ctx);
    assert.ok(result.output.includes('No project config'));
  });

  it('set creates config file and sets value', async () => {
    const result = await configTool.execute!({ action: 'set', key: 'model', value: 'gpt-4o' }, ctx);
    assert.ok(result.output.includes('model'));
    assert.ok(result.output.includes('gpt-4o'));
    const configPath = path.join(tmpDir, '.superinference', 'config.json');
    assert.ok(fs.existsSync(configPath));
    const config = JSON.parse(fs.readFileSync(configPath, 'utf-8'));
    assert.equal(config.model, 'gpt-4o');
  });

  it('get returns full config when no key specified', async () => {
    const result = await configTool.execute!({ action: 'get' }, ctx);
    assert.ok(result.output.includes('model'));
    assert.ok(result.output.includes('gpt-4o'));
  });

  it('get returns specific key value', async () => {
    const result = await configTool.execute!({ action: 'get', key: 'model' }, ctx);
    assert.ok(result.output.includes('model'));
    assert.ok(result.output.includes('gpt-4o'));
  });

  it('set overwrites existing value', async () => {
    await configTool.execute!({ action: 'set', key: 'model', value: 'claude-3-opus' }, ctx);
    const result = await configTool.execute!({ action: 'get', key: 'model' }, ctx);
    assert.ok(result.output.includes('claude-3-opus'));
  });

  it('returns error for invalid action usage (set without key)', async () => {
    const result = await configTool.execute!({ action: 'set' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('Usage'));
  });

  it('returns error for unknown action', async () => {
    const result = await configTool.execute!({ action: 'delete' }, ctx);
    assert.ok(result.isError);
  });
});
