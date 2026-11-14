import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { configTool } from '../src/tools/config-tool';
import type { ToolContext } from '../src/types';

// Branch coverage for the enhanced config-tool: corrupt-file guards (refuse to
// clobber, distinguish absent vs corrupt), value-missing guidance, and the
// write-failure path.
describe('configTool corrupt/guard coverage', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'config-tool-w1-'));
  });

  after(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('set with a key but no value asks for a value (does not write)', async () => {
    const dir = fs.mkdtempSync(path.join(tmpDir, 'novalue-'));
    const ctx = { cwd: dir } as ToolContext;
    const result = await configTool.execute!({ action: 'set', key: 'model' }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Provide a value'));
    assert.equal(fs.existsSync(path.join(dir, '.ami', 'config.json')), false);
  });

  it('set with an explicit undefined value asks for a value', async () => {
    const dir = fs.mkdtempSync(path.join(tmpDir, 'explicit-undef-'));
    const ctx = { cwd: dir } as ToolContext;
    const result = await configTool.execute!({ action: 'set', key: 'model', value: undefined }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Provide a value'));
    assert.equal(fs.existsSync(path.join(dir, '.ami', 'config.json')), false);
  });

  it('set REFUSES to clobber an existing corrupt config (protects user data)', async () => {
    const dir = fs.mkdtempSync(path.join(tmpDir, 'corrupt-'));
    const ctx = { cwd: dir } as ToolContext;
    const configDir = path.join(dir, '.ami');
    const configPath = path.join(configDir, 'config.json');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(configPath, '{ this is not: valid json ]');

    const result = await configTool.execute!({ action: 'set', key: 'provider', value: 'anthropic' }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Refusing to write'));
    // Corrupt content must be preserved untouched, not overwritten.
    assert.equal(fs.readFileSync(configPath, 'utf-8'), '{ this is not: valid json ]');
  });

  it('get distinguishes a corrupt config from an absent one', async () => {
    const dir = fs.mkdtempSync(path.join(tmpDir, 'corrupt-get-'));
    const ctx = { cwd: dir } as ToolContext;
    const configDir = path.join(dir, '.ami');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(path.join(configDir, 'config.json'), '}}not json{{');

    const result = await configTool.execute!({ action: 'get', key: 'model' }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('corrupt'));
  });

  it('set returns an isError result when the config file cannot be written', async () => {
    const dir = fs.mkdtempSync(path.join(tmpDir, 'writefail-'));
    const ctx = { cwd: dir } as ToolContext;
    // Create a FILE named ".ami" so the write path hits ENOTDIR.
    fs.writeFileSync(path.join(dir, '.ami'), 'blocker');

    const result = await configTool.execute!({ action: 'set', key: 'model', value: 'gpt-4o' }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Error writing config'));
  });
});
