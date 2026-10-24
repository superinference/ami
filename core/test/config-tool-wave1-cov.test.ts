import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { configTool } from '../src/tools/config-tool';
import type { ToolContext } from '../src/types';

// Wave 1 coverage: exercises the remaining uncovered branches of config-tool.ts
//   - set with key present but value === undefined  -> falls through to Usage error
//   - set when existing config.json is corrupt JSON -> inner JSON.parse catch, resets to {}
//   - set when the write path cannot be created     -> outer catch, isError output
describe('configTool wave1 coverage', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'config-tool-w1-'));
  });

  after(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('set with a key but no value short-circuits to the Usage error', async () => {
    const dir = fs.mkdtempSync(path.join(tmpDir, 'novalue-'));
    const ctx = { cwd: dir } as ToolContext;
    const result = await configTool.execute!({ action: 'set', key: 'model' }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Usage'));
    // The guard must NOT have written a config file for an undefined value.
    assert.equal(fs.existsSync(path.join(dir, '.superinference', 'config.json')), false);
  });

  it('set with an explicit undefined value short-circuits to the Usage error', async () => {
    const dir = fs.mkdtempSync(path.join(tmpDir, 'explicit-undef-'));
    const ctx = { cwd: dir } as ToolContext;
    const result = await configTool.execute!({ action: 'set', key: 'model', value: undefined }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Usage'));
    assert.equal(fs.existsSync(path.join(dir, '.superinference', 'config.json')), false);
  });

  it('set recovers when the existing config.json holds corrupt JSON', async () => {
    const dir = fs.mkdtempSync(path.join(tmpDir, 'corrupt-'));
    const ctx = { cwd: dir } as ToolContext;
    const configDir = path.join(dir, '.superinference');
    const configPath = path.join(configDir, 'config.json');
    fs.mkdirSync(configDir, { recursive: true });
    // Pre-existing garbage that JSON.parse cannot handle -> inner catch resets to {}.
    fs.writeFileSync(configPath, '{ this is not: valid json ]');

    const result = await configTool.execute!({ action: 'set', key: 'provider', value: 'anthropic' }, ctx);
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('provider'));
    assert.ok(result.output.includes('anthropic'));

    // The file must have been rewritten as valid JSON containing only the new key,
    // proving the corrupt content was discarded (not merged).
    const parsed = JSON.parse(fs.readFileSync(configPath, 'utf-8'));
    assert.deepEqual(parsed, { provider: 'anthropic' });
  });

  it('get recovers (no-config message) when config.json holds corrupt JSON', async () => {
    const dir = fs.mkdtempSync(path.join(tmpDir, 'corrupt-get-'));
    const ctx = { cwd: dir } as ToolContext;
    const configDir = path.join(dir, '.superinference');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(path.join(configDir, 'config.json'), '}}not json{{');

    const result = await configTool.execute!({ action: 'get', key: 'model' }, ctx);
    assert.ok(result.output.includes('No project config'));
  });

  it('set returns an isError result when the config directory cannot be created', async () => {
    const dir = fs.mkdtempSync(path.join(tmpDir, 'writefail-'));
    const ctx = { cwd: dir } as ToolContext;
    // Create a FILE named ".superinference" so mkdirSync(dirname) hits ENOTDIR
    // and the outer catch returns an error result instead of throwing.
    fs.writeFileSync(path.join(dir, '.superinference'), 'blocker');

    const result = await configTool.execute!({ action: 'set', key: 'model', value: 'gpt-4o' }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.startsWith('Error:'));
  });
});
