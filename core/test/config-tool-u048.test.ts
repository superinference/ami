import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  configTool,
  generateModelSection,
  SUPPORTED_SETTINGS,
} from '../src/tools/config-tool';
import type { ToolContext } from '../src/types';

describe('configTool U048 parity additions', () => {
  let tmpDir: string;

  const freshCtx = (): ToolContext => {
    const d = fs.mkdtempSync(path.join(tmpDir, 'c-'));
    return { cwd: d } as ToolContext;
  };

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'config-tool-u048-'));
  });

  after(() => { fs.rmSync(tmpDir, { recursive: true }); });

  describe('worked input examples in the tool description', () => {
    it('surfaces concrete get/set example payloads proactively', () => {
      const d = configTool.description;
      assert.ok(/Examples:/i.test(d), 'description should contain an Examples block');
      // concrete get + set shapes, so the model has the correct tool-call form
      assert.ok(d.includes('action="get"'), 'should show a get example');
      assert.ok(d.includes('action="set"'), 'should show a set example');
      assert.ok(d.includes('value="default"'), 'should show the reset-to-default example');
    });
  });

  describe('dynamic model section', () => {
    it('enumerates known provider model ids and aliases in the description', () => {
      const d = configTool.description;
      assert.ok(/## Model/i.test(d), 'description should contain a Model section');
      // a real, static, known-good model id from the registry
      assert.ok(d.includes('claude-opus-4-6'), 'should list a known anthropic model id');
      // provider grouping
      assert.ok(d.includes('anthropic'), 'should group by provider');
      // short aliases so the model can pick a friendly value
      assert.ok(d.includes('opus'), 'should surface at least one alias');
    });

    it('surfaces the model-facing capability/cost guidance for each alias', () => {
      const d = configTool.description;
      // The agent reads this tool description when it sets the `model` key for a
      // sub-task, so the model-facing descriptionForModel blurb must be present —
      // not just the terse human label.
      assert.ok(/everyday/i.test(d), 'should surface the sonnet model-facing blurb');
      assert.ok(/less capable/i.test(d), 'should surface the haiku model-facing blurb');
    });

    it('degrades to a static fallback when the model lister throws', () => {
      const thrower = () => { throw new Error('registry blew up'); };
      const section = generateModelSection(thrower);
      assert.ok(section.includes('## Model'), 'still has a Model header');
      assert.ok(/\/model/.test(section), 'fallback points at the /model command');
      // must NOT throw and must not leak the raw error
      assert.ok(!section.includes('registry blew up'));
    });
  });

  describe('formatOnRead for model (null -> "default")', () => {
    it('reports an unset model as "default" on get', async () => {
      const ctx = freshCtx();
      fs.mkdirSync(path.join(ctx.cwd, '.ami'), { recursive: true });
      // a config that has other keys but no model
      fs.writeFileSync(path.join(ctx.cwd, '.ami', 'config.json'), JSON.stringify({ provider: 'anthropic' }));
      const res = await configTool.execute({ action: 'get', key: 'model' }, ctx);
      assert.equal(res.isError, undefined);
      assert.ok(res.output.includes('model = "default"'), `expected default, got: ${res.output}`);
      assert.equal((res.metadata as { value?: unknown }).value, 'default');
    });

    it('reports the actual model when set', async () => {
      const ctx = freshCtx();
      fs.mkdirSync(path.join(ctx.cwd, '.ami'), { recursive: true });
      fs.writeFileSync(path.join(ctx.cwd, '.ami', 'config.json'), JSON.stringify({ model: 'claude-opus-4-6' }));
      const res = await configTool.execute({ action: 'get', key: 'model' }, ctx);
      assert.ok(res.output.includes('"claude-opus-4-6"'), `got: ${res.output}`);
    });

    it('declares a formatOnRead hook on the model setting', () => {
      assert.equal(typeof SUPPORTED_SETTINGS.model.formatOnRead, 'function');
    });
  });
});
