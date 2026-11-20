import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { configTool } from '../src/tools/config-tool';
import { getGlobalConfigPath, loadGlobalConfig } from '../src/config';
import type { ToolContext } from '../src/types';

describe('configTool U048/w51 — value union, scope routing, affordance text', () => {
  let tmpDir: string;

  const freshCtx = (): ToolContext => {
    const d = fs.mkdtempSync(path.join(tmpDir, 'c-'));
    return { cwd: d } as ToolContext;
  };

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'config-tool-w51-'));
  });

  after(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  describe('value parameter is a string|boolean|number union (parity: the reference union)', () => {
    it('declares the value schema as an array-type union', () => {
      const valueProp = configTool.inputSchema.properties.value;
      assert.ok(Array.isArray(valueProp.type), `value.type should be a union array, got: ${JSON.stringify(valueProp.type)}`);
      assert.deepEqual([...(valueProp.type as string[])].sort(), ['boolean', 'number', 'string']);
    });

    it('accepts a NATIVE boolean value for a boolean setting (no string coercion needed)', async () => {
      const ctx = freshCtx();
      const res = await configTool.execute({ action: 'set', key: 'safeMode', value: true }, ctx);
      assert.equal(res.isError, undefined, res.output);
      assert.equal((res.metadata as { newValue?: unknown }).newValue, true);
      const written = JSON.parse(fs.readFileSync(path.join(ctx.cwd, '.ami', 'config.json'), 'utf-8'));
      assert.equal(written.safeMode, true);
    });

    it('accepts a NATIVE number value for a number setting', async () => {
      const ctx = freshCtx();
      const res = await configTool.execute({ action: 'set', key: 'maxWebSearchesPerSession', value: 40 }, ctx);
      assert.equal(res.isError, undefined, res.output);
      assert.equal((res.metadata as { newValue?: unknown }).newValue, 40);
      const written = JSON.parse(fs.readFileSync(path.join(ctx.cwd, '.ami', 'config.json'), 'utf-8'));
      assert.equal(written.maxWebSearchesPerSession, 40);
    });
  });

  describe('storage-scope affordance text in the tool description', () => {
    it('documents the get-vs-set contract and the global/project scope split', () => {
      const d = configTool.description;
      assert.ok(/Omit value to read a setting; include value to write it/i.test(d), 'should spell out get-vs-set contract');
      assert.ok(/Storage scope/i.test(d), 'should have a storage-scope section');
      assert.ok(d.includes('~/.ami/config.json'), 'should name the global store path');
      assert.ok(d.includes('.ami/config.json'), 'should name the project store path');
      assert.ok(/scope="global"/.test(d), 'should show a scope=global example/affordance');
    });
  });

  describe('scope routing: project (default) vs global (~/.ami/config.json)', () => {
    let savedHome: string | undefined;
    let savedUserProfile: string | undefined;
    let homeDir: string;

    before(() => {
      savedHome = process.env.HOME;
      savedUserProfile = process.env.USERPROFILE;
      homeDir = fs.mkdtempSync(path.join(tmpDir, 'home-'));
      process.env.HOME = homeDir;
      delete process.env.USERPROFILE;
    });

    after(() => {
      if (savedHome === undefined) delete process.env.HOME; else process.env.HOME = savedHome;
      if (savedUserProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = savedUserProfile;
    });

    it('getGlobalConfigPath resolves under HOME', () => {
      assert.equal(getGlobalConfigPath(), path.join(homeDir, '.ami', 'config.json'));
    });

    it('set scope="global" writes to the global store, NOT the project store', async () => {
      const ctx = freshCtx();
      const res = await configTool.execute({ action: 'set', key: 'provider', value: 'openai', scope: 'global' }, ctx);
      assert.equal(res.isError, undefined, res.output);
      assert.equal((res.metadata as { scope?: string }).scope, 'global');
      // landed in the global store that the config waterfall reads
      const globalFile = path.join(homeDir, '.ami', 'config.json');
      assert.ok(fs.existsSync(globalFile), 'global config file should exist');
      assert.equal(JSON.parse(fs.readFileSync(globalFile, 'utf-8')).provider, 'openai');
      assert.equal(loadGlobalConfig()!.provider, 'openai');
      // did NOT touch the project store
      assert.equal(fs.existsSync(path.join(ctx.cwd, '.ami', 'config.json')), false);
    });

    it('get scope="global" reads the global store', async () => {
      const ctx = freshCtx();
      fs.writeFileSync(path.join(homeDir, '.ami', 'config.json'), JSON.stringify({ provider: 'google' }));
      const res = await configTool.execute({ action: 'get', key: 'provider', scope: 'global' }, ctx);
      assert.equal(res.isError, undefined, res.output);
      assert.ok(res.output.includes('"google"'), res.output);
      assert.equal((res.metadata as { scope?: string }).scope, 'global');
    });

    it('project scope (default) stays isolated from the global store', async () => {
      const ctx = freshCtx();
      await configTool.execute({ action: 'set', key: 'provider', value: 'anthropic' }, ctx);
      const projFile = path.join(ctx.cwd, '.ami', 'config.json');
      assert.equal(JSON.parse(fs.readFileSync(projFile, 'utf-8')).provider, 'anthropic');
    });

    it('get scope="global" on an absent global store reports the global path', async () => {
      const ctx = freshCtx();
      fs.rmSync(path.join(homeDir, '.ami', 'config.json'), { force: true });
      const res = await configTool.execute({ action: 'get', scope: 'global' }, ctx);
      assert.ok(/global config/i.test(res.output), res.output);
      assert.equal((res.metadata as { scope?: string }).scope, 'global');
    });
  });

  describe('global scope with no resolvable home', () => {
    it('errors clearly instead of writing to a bogus path', async () => {
      const savedHome = process.env.HOME;
      const savedUserProfile = process.env.USERPROFILE;
      delete process.env.HOME;
      delete process.env.USERPROFILE;
      try {
        assert.equal(getGlobalConfigPath(), null);
        const ctx = freshCtx();
        const res = await configTool.execute({ action: 'set', key: 'provider', value: 'openai', scope: 'global' }, ctx);
        assert.equal(res.isError, true);
        assert.ok(/no home directory/i.test(res.output), res.output);
      } finally {
        if (savedHome === undefined) delete process.env.HOME; else process.env.HOME = savedHome;
        if (savedUserProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = savedUserProfile;
      }
    });
  });
});
