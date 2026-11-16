import { describe, it, before, after, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  updateConfigFile,
  loadConfigFileChecked,
  type ProjectConfig,
} from '../src/config';
import { configTool } from '../src/tools/config-tool';
import type { ToolContext } from '../src/types';

/**
 * Covers the guarded functional config writer `updateConfigFile` (parity:
 * Claude Code's saveGlobalConfig/saveConfigWithLock lost-update + auth-loss
 * guard) and its wiring into the `config` tool's write path.
 */
describe('updateConfigFile (guarded functional read-modify-write)', () => {
  let tmpDir: string;
  let cfgPath: string;

  before(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-update-')); });
  after(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });
  beforeEach(() => {
    cfgPath = path.join(fs.mkdtempSync(path.join(tmpDir, 'd-')), 'config.json');
  });

  it('creates a new file from an absent config (updater sees {})', () => {
    let seen: ProjectConfig | undefined;
    const res = updateConfigFile(cfgPath, cur => { seen = cur; return { ...cur, model: 'gpt-x' }; });
    assert.deepEqual(seen, {});
    assert.equal(res.written, true);
    assert.equal(res.reason, 'written');
    assert.deepEqual(loadConfigFileChecked(cfgPath).config, { model: 'gpt-x' });
  });

  it('merges onto the FRESH on-disk state, preserving a concurrently-added key (lost-update guard)', () => {
    // Initial content.
    updateConfigFile(cfgPath, cur => ({ ...cur, a: 1 } as ProjectConfig));
    // Simulate another process adding `c` AFTER a caller last read the file.
    const stale = loadConfigFileChecked(cfgPath).config as ProjectConfig; // {a:1}
    fs.writeFileSync(cfgPath, JSON.stringify({ a: 1, c: 3 }), 'utf-8');
    // The updater must receive the FRESH disk state (with c:3), not the stale base.
    let sawC: unknown;
    const res = updateConfigFile(cfgPath, cur => {
      sawC = (cur as Record<string, unknown>).c;
      return { ...cur, d: 4 } as ProjectConfig;
    });
    assert.equal(sawC, 3, 'updater must see the concurrently-written key, proving a fresh re-read');
    assert.equal(res.written, true);
    const onDisk = loadConfigFileChecked(cfgPath).config as Record<string, unknown>;
    assert.deepEqual(onDisk, { a: 1, c: 3, d: 4 }, 'concurrent key c:3 must survive the write');
    // The stale base never participated in the write.
    assert.equal((stale as Record<string, unknown>).c, undefined);
  });

  it('treats a same-reference updater return as a no-op (no write)', () => {
    updateConfigFile(cfgPath, cur => ({ ...cur, a: 1 } as ProjectConfig));
    const before = fs.statSync(cfgPath).mtimeMs;
    const res = updateConfigFile(cfgPath, cur => cur); // identity → same ref
    assert.equal(res.written, false);
    assert.equal(res.reason, 'no-change');
    assert.equal(fs.statSync(cfgPath).mtimeMs, before, 'file must be untouched on a no-op');
  });

  it('REFUSES to write over a present-but-corrupt file (unparseable JSON)', () => {
    fs.writeFileSync(cfgPath, '{ this is : not json ]', 'utf-8');
    const raw = fs.readFileSync(cfgPath, 'utf-8');
    const res = updateConfigFile(cfgPath, () => ({ model: 'wiped' }));
    assert.equal(res.written, false);
    assert.equal(res.reason, 'corrupt');
    assert.ok(res.diagnostic && res.diagnostic.kind === 'parse-error');
    // The corrupt (recoverable) content must be left intact — never clobbered.
    assert.equal(fs.readFileSync(cfgPath, 'utf-8'), raw);
  });

  it('REFUSES to write over a file that parsed to a non-object', () => {
    fs.writeFileSync(cfgPath, '[1,2,3]', 'utf-8');
    const res = updateConfigFile(cfgPath, () => ({ model: 'x' }));
    assert.equal(res.written, false);
    assert.equal(res.reason, 'corrupt');
    assert.ok(res.diagnostic && res.diagnostic.kind === 'not-object');
  });

  it('still writes when only ADVISORY (invalid-value / unknown-key) diagnostics are present', () => {
    // Unknown top-level key is advisory — the file loads and an update proceeds.
    fs.writeFileSync(cfgPath, JSON.stringify({ model: 'm', totallyUnknownKey: 1 }), 'utf-8');
    const res = updateConfigFile(cfgPath, cur => ({ ...cur, provider: 'openai' } as ProjectConfig));
    assert.equal(res.written, true);
    const onDisk = loadConfigFileChecked(cfgPath).config as Record<string, unknown>;
    assert.equal(onDisk.provider, 'openai');
    assert.equal(onDisk.model, 'm');
  });
});

describe('config tool write path routes through updateConfigFile', () => {
  let tmpRoot: string;

  before(() => { tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-tool-update-')); });
  after(() => { fs.rmSync(tmpRoot, { recursive: true, force: true }); });

  const freshCtx = (): ToolContext => {
    const d = fs.mkdtempSync(path.join(tmpRoot, 'c-'));
    return { cwd: d } as ToolContext;
  };
  const cfgOf = (ctx: ToolContext) => path.join(ctx.cwd, '.ami', 'config.json');

  it('setting a key preserves a sibling key already on disk (merge onto fresh read)', async () => {
    const ctx = freshCtx();
    fs.mkdirSync(path.dirname(cfgOf(ctx)), { recursive: true });
    fs.writeFileSync(cfgOf(ctx), JSON.stringify({ provider: 'openai' }), 'utf-8');
    const res = await configTool.execute({ action: 'set', key: 'model', value: 'gpt-x' }, ctx);
    assert.equal(res.isError, undefined);
    const onDisk = JSON.parse(fs.readFileSync(cfgOf(ctx), 'utf-8'));
    assert.deepEqual(onDisk, { provider: 'openai', model: 'gpt-x' });
  });

  it('refuses to set when the existing file is corrupt (does not clobber it)', async () => {
    const ctx = freshCtx();
    fs.mkdirSync(path.dirname(cfgOf(ctx)), { recursive: true });
    fs.writeFileSync(cfgOf(ctx), '{ broken ]', 'utf-8');
    const res = await configTool.execute({ action: 'set', key: 'model', value: 'gpt-x' }, ctx);
    assert.equal(res.isError, true);
    assert.match(res.output, /corrupt/i);
    assert.equal(fs.readFileSync(cfgOf(ctx), 'utf-8'), '{ broken ]');
  });

  it('reset-to-default removes a key through the guarded writer', async () => {
    const ctx = freshCtx();
    await configTool.execute({ action: 'set', key: 'model', value: 'gpt-x' }, ctx);
    await configTool.execute({ action: 'set', key: 'provider', value: 'openai' }, ctx);
    const res = await configTool.execute({ action: 'set', key: 'model', value: 'default' }, ctx);
    assert.equal(res.isError, undefined);
    const onDisk = JSON.parse(fs.readFileSync(cfgOf(ctx), 'utf-8'));
    assert.equal(onDisk.model, undefined);
    assert.equal(onDisk.provider, 'openai');
  });
});
