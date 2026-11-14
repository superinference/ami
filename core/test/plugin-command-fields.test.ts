import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { loadCommands } from '../src/plugins';

let dir: string;
beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cmdfields-')); });
afterEach(() => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* best effort */ } });

describe('loadCommands — optional command fields', () => {
  it('parses the behavioral knobs alongside description/prompt', () => {
    fs.writeFileSync(path.join(dir, 'deploy.toml'), [
      'description = "Deploy the app"',
      'prompt = "Run the deploy"',
      'argument-hint = "<env>"',
      'model = "claude-opus-4-8"',
      'effort = "high"',
      'when-to-use = "when shipping"',
      'version = "2.1.0"',
      'allowed-tools = ["bash", "file_read"]',
      'disable-model-invocation = true',
      'user-invocable = false',
    ].join('\n'));
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.name, 'deploy');
    assert.equal(cmd.description, 'Deploy the app');
    assert.equal(cmd.prompt, 'Run the deploy');
    assert.equal(cmd.argumentHint, '<env>');
    assert.equal(cmd.model, 'claude-opus-4-8');
    assert.equal(cmd.effort, 'high');
    assert.equal(cmd.whenToUse, 'when shipping');
    assert.equal(cmd.version, '2.1.0');
    assert.deepEqual(cmd.allowedTools, ['bash', 'file_read']);
    assert.equal(cmd.disableModelInvocation, true);
    assert.equal(cmd.userInvocable, false);
  });

  it('omits every optional field when only description/prompt are present', () => {
    fs.writeFileSync(path.join(dir, 'min.toml'), 'description = "d"\nprompt = "p"\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.argumentHint, undefined);
    assert.equal(cmd.allowedTools, undefined);
    assert.equal(cmd.disableModelInvocation, undefined);
    assert.equal(cmd.userInvocable, undefined);
    assert.equal(cmd.model, undefined);
  });

  it('accepts the snake_case when_to_use alias and an empty allowed-tools array', () => {
    fs.writeFileSync(path.join(dir, 'alias.toml'), 'description = "d"\nprompt = "p"\nwhen_to_use = "x"\nallowed-tools = []\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.whenToUse, 'x');
    assert.deepEqual(cmd.allowedTools, []);
  });
});
