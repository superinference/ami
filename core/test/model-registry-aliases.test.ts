import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import {
  MODEL_ALIASES,
  MODEL_FAMILY_ALIASES,
  isModelAlias,
  isModelFamilyAlias,
  resolveModelAlias,
  validateModel,
} from '../src/model-registry';
import { resolveSubagentModel } from '../src/tools/task';
import type { ProviderConfig } from '../src/types';

function cfg(overrides: Partial<ProviderConfig>): ProviderConfig {
  return { provider: 'openai', apiKey: 'k', baseUrl: '', model: 'm', ...overrides } as ProviderConfig;
}

describe('model aliases — canonical table', () => {
  it('MODEL_ALIASES is Claude Code\'s canonical 7-value set', () => {
    assert.deepEqual([...MODEL_ALIASES], [
      'sonnet', 'opus', 'haiku', 'best', 'sonnet[1m]', 'opus[1m]', 'opusplan',
    ]);
  });

  it('MODEL_FAMILY_ALIASES is the bare family trio', () => {
    assert.deepEqual([...MODEL_FAMILY_ALIASES], ['sonnet', 'opus', 'haiku']);
  });

  it('isModelAlias recognizes every alias, case-insensitively and trimmed', () => {
    assert.equal(isModelAlias('opus'), true);
    assert.equal(isModelAlias('OPUS'), true);
    assert.equal(isModelAlias('  opus  '), true);
    assert.equal(isModelAlias('opus[1m]'), true);
    assert.equal(isModelAlias('best'), true);
    assert.equal(isModelAlias('opusplan'), true);
    // Full/unknown ids are NOT aliases.
    assert.equal(isModelAlias('claude-opus-4-6'), false);
    assert.equal(isModelAlias('gpt-4o'), false);
    assert.equal(isModelAlias(''), false);
  });

  it('isModelFamilyAlias is true only for the bare family trio', () => {
    for (const f of ['sonnet', 'opus', 'haiku', 'SONNET', ' opus ']) {
      assert.equal(isModelFamilyAlias(f), true, `${f} should be a family alias`);
    }
    for (const n of ['best', 'opusplan', 'sonnet[1m]', 'opus[1m]', 'claude-opus-4-6', '']) {
      assert.equal(isModelFamilyAlias(n), false, `${n} should NOT be a family alias`);
    }
  });

  it('resolveModelAlias maps each alias to its canonical family-default id', () => {
    assert.equal(resolveModelAlias('sonnet'), 'claude-sonnet-4');
    assert.equal(resolveModelAlias('Opus'), 'claude-opus-4');
    assert.equal(resolveModelAlias('  HAIKU '), 'claude-haiku-4-5');
    assert.equal(resolveModelAlias('best'), 'claude-opus-4');
    assert.equal(resolveModelAlias('opusplan'), 'claude-opus-4');
    assert.equal(resolveModelAlias('opus[1m]'), 'claude-opus-4');
    assert.equal(resolveModelAlias('sonnet[1m]'), 'claude-sonnet-4');
    // A full/unknown id is not an alias — resolver returns undefined so the
    // caller passes it through verbatim.
    assert.equal(resolveModelAlias('claude-opus-4-6'), undefined);
    assert.equal(resolveModelAlias('gpt-4o'), undefined);
    assert.equal(resolveModelAlias(''), undefined);
  });
});

describe('model aliases — consolidated subagent resolution (tools/task)', () => {
  // The subagent path now shares the canonical table, so the EXPANDED aliases
  // ('best'/'opusplan'/[1m]) resolve there too (previously only sonnet/opus/haiku
  // were understood and anything else passed through verbatim).
  it('resolves the expanded alias set for a subagent model override', () => {
    assert.equal(resolveSubagentModel('best', undefined), 'claude-opus-4');
    assert.equal(resolveSubagentModel('opusplan', undefined), 'claude-opus-4');
    assert.equal(resolveSubagentModel('opus[1m]', undefined), 'claude-opus-4');
  });

  it('still honors anti-downgrade + verbatim pass-through for full ids', () => {
    // bare family alias inherits the parent's exact same-family model
    assert.equal(resolveSubagentModel('opus', 'claude-opus-4-8'), 'claude-opus-4-8');
    // non-family alias does NOT trigger anti-downgrade
    assert.equal(resolveSubagentModel('best', 'claude-opus-4-8'), 'claude-opus-4');
    // full/unknown id returned verbatim (case preserved)
    assert.equal(resolveSubagentModel('GPT-4o', 'claude-opus-4-8'), 'GPT-4o');
  });
});

describe('model aliases — validateModel resolves aliases before matching', () => {
  it('resolves a non-substring alias ("best") to a concrete suggestion (static provider)', async () => {
    // Anthropic uses a static built-in list. "best" shares no substring with any
    // listed id, so WITHOUT alias resolution there is no suggestion; WITH it, the
    // alias resolves to claude-opus-4 and the opus family model is suggested.
    const res = await validateModel(cfg({ provider: 'anthropic', baseUrl: '' }), 'best');
    assert.equal(res.valid, false);
    assert.ok(res.suggestion && res.suggestion.includes('opus'),
      `expected an opus-family suggestion, got ${res.suggestion}`);
  });

  let server: http.Server;
  let port = 0;
  before(async () => {
    server = http.createServer((_req, res) => {
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ data: [{ id: 'claude-opus-4' }, { id: 'gpt-4o' }] }));
    });
    await new Promise<void>(r => server.listen(0, () => r()));
    port = (server.address() as import('net').AddressInfo).port;
  });
  after(async () => { await new Promise<void>(r => server.close(() => r())); });

  it('accepts a bare alias whose resolved id the provider actually lists', async () => {
    // WITHOUT alias resolution, "opus" is not an exact id and only partial-matches
    // → valid:false. WITH resolution it becomes claude-opus-4, an exact listed id
    // → valid:true.
    const res = await validateModel(cfg({ baseUrl: `http://127.0.0.1:${port}/v1` }), 'opus');
    assert.equal(res.valid, true);
  });

  it('still validates a full concrete id unchanged (non-alias pass-through)', async () => {
    const res = await validateModel(cfg({ baseUrl: `http://127.0.0.1:${port}/v1` }), 'gpt-4o');
    assert.equal(res.valid, true);
  });
});
