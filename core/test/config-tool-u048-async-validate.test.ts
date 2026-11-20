import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { configTool, SUPPORTED_SETTINGS } from '../src/tools/config-tool';
import type { ToolContext, ProviderConfig } from '../src/types';

/**
 * U048 — async per-setting validateOnWrite (model API check).
 *
 * The config tool must refuse to persist a model id that the configured provider
 * does not serve (parity: the reference's ConfigTool.validateOnWrite), reusing the
 * shared validateModel check. Without resolved provider credentials it must
 * soft-allow (offline edits / bare engine must still work).
 */
describe('configTool U048 — async validateOnWrite for model-valued settings', () => {
  let tmpDir: string;

  // A static-list provider (anthropic) so validateModel resolves offline against
  // the built-in MODEL_PREFERENCE list rather than hitting the network.
  const anthropicConfig: ProviderConfig = {
    provider: 'anthropic',
    apiKey: 'sk-ant-test',
    baseUrl: 'https://api.anthropic.com',
    model: 'claude-opus-4-6',
  };

  const freshCtx = (providerConfig?: ProviderConfig): ToolContext => {
    const d = fs.mkdtempSync(path.join(tmpDir, 'c-'));
    return { cwd: d, _providerConfig: providerConfig } as ToolContext;
  };

  const readCfg = (ctx: ToolContext) =>
    JSON.parse(fs.readFileSync(path.join(ctx.cwd, '.ami', 'config.json'), 'utf-8'));

  before(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'config-tool-async-')); });
  after(() => { fs.rmSync(tmpDir, { recursive: true }); });

  it('declares an asyncValidateOnWrite hook on the model-valued settings', () => {
    assert.equal(typeof SUPPORTED_SETTINGS.model.asyncValidateOnWrite, 'function');
    assert.equal(typeof SUPPORTED_SETTINGS.fallbackModel.asyncValidateOnWrite, 'function');
    assert.equal(typeof SUPPORTED_SETTINGS.compactionModel.asyncValidateOnWrite, 'function');
  });

  it('rejects an unknown model id before persisting when a provider is configured', async () => {
    const ctx = freshCtx(anthropicConfig);
    const result = await configTool.execute!(
      { action: 'set', key: 'model', value: 'totally-bogus-model-9000' },
      ctx,
    );
    assert.ok(result.isError, 'an invalid model must be rejected');
    assert.ok(result.output.includes('not a valid model'), result.output);
    // It must surface the valid options so the model can self-correct.
    assert.ok(result.output.includes('Available:'), result.output);
    // Nothing must have been written to disk.
    assert.equal(fs.existsSync(path.join(ctx.cwd, '.ami', 'config.json')), false);
  });

  it('persists a model id the provider actually serves', async () => {
    const ctx = freshCtx(anthropicConfig);
    const result = await configTool.execute!(
      { action: 'set', key: 'model', value: 'claude-opus-4-6' },
      ctx,
    );
    assert.equal(result.isError, undefined);
    assert.equal(readCfg(ctx).model, 'claude-opus-4-6');
  });

  it('applies the same validation to fallbackModel and compactionModel', async () => {
    const ctx = freshCtx(anthropicConfig);
    const bad = await configTool.execute!(
      { action: 'set', key: 'fallbackModel', value: 'nope-not-real' },
      ctx,
    );
    assert.ok(bad.isError);
    const good = await configTool.execute!(
      { action: 'set', key: 'compactionModel', value: 'claude-haiku-4-5-20251001' },
      ctx,
    );
    assert.equal(good.isError, undefined);
    assert.equal(readCfg(ctx).compactionModel, 'claude-haiku-4-5-20251001');
  });

  it('soft-allows any well-formed model id when no provider config is present', async () => {
    const ctx = freshCtx(/* no provider config */);
    const result = await configTool.execute!(
      { action: 'set', key: 'model', value: 'some-private-selfhosted-model' },
      ctx,
    );
    assert.equal(result.isError, undefined, 'offline edits must not be blocked');
    assert.equal(readCfg(ctx).model, 'some-private-selfhosted-model');
  });

  it('does not round-trip the validator on a no-op (value unchanged)', async () => {
    const ctx = freshCtx(anthropicConfig);
    await configTool.execute!({ action: 'set', key: 'model', value: 'claude-opus-4-6' }, ctx);
    // Re-setting the SAME value must short-circuit as a no-op even though the
    // value itself is valid — proving the no-op guard precedes async validation.
    const result = await configTool.execute!(
      { action: 'set', key: 'model', value: 'claude-opus-4-6' },
      { ...ctx, _providerConfig: undefined } as ToolContext,
    );
    assert.ok(result.output.includes('no change'), result.output);
  });

  it('still enforces the synchronous non-empty guard before async validation', async () => {
    const ctx = freshCtx(anthropicConfig);
    const result = await configTool.execute!({ action: 'set', key: 'model', value: '   ' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('non-empty'), result.output);
  });
});
