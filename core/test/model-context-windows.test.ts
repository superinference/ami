import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { getContextWindow, getModelCapabilities, readAdvertisedContextWindow, collectModelRecords, pickAdvertisedContextWindow, readHuggingFaceContextWindow, huggingfaceRepoId, huggingfaceRepoCandidates } from '../src/model-capabilities';

describe('Model context windows', () => {
  it('returns correct window for Claude models', () => {
    assert.equal(getContextWindow('claude-opus-4-20250514'), 200000);
    assert.equal(getContextWindow('claude-sonnet-4-6'), 200000);
    assert.equal(getContextWindow('claude-opus-5-5'), 1_000_000);
    assert.equal(getContextWindow('claude-sonnet-5-5'), 1_000_000);
    assert.equal(getContextWindow('claude-fable-5-1'), 1_000_000);
    assert.equal(getContextWindow('claude-haiku-4-5-20251001'), 200000);
  });

  it('returns correct window for GPT models', () => {
    assert.equal(getContextWindow('gpt-4o'), 128000);
    assert.equal(getContextWindow('gpt-4-turbo-preview'), 128000);
    assert.equal(getContextWindow('gpt-4.1'), 1048576);
    assert.equal(getContextWindow('gpt-6-astra'), 1_050_000);
    assert.equal(getContextWindow('gpt-6.1-sol'), 1_050_000);
    assert.equal(getContextWindow('gpt-6-luna'), 1_050_000);
  });

  it('returns correct window for Gemini models', () => {
    assert.equal(getContextWindow('gemini-2.0-flash'), 1048576);
    assert.equal(getContextWindow('gemini-2.5-pro'), 1048576);
    assert.equal(getContextWindow('gemini-3.1-pro-preview'), 2097152);
  });

  it('returns default 128K for unknown models', () => {
    assert.equal(getContextWindow('unknown-model-xyz'), 128000);
  });

  it('returns published xAI windows, with the longer id winning', () => {
    assert.equal(getContextWindow('grok-4.7'), 500_000);
    assert.equal(getContextWindow('grok-4.3'), 1_000_000);
    assert.equal(getContextWindow('grok-4.20-0309-reasoning'), 1_000_000);
    assert.equal(getContextWindow('grok-build-0.1'), 256_000);
  });

  it('returns correct window for O-series models', () => {
    assert.equal(getContextWindow('o1-preview'), 200000);
    assert.equal(getContextWindow('o3-mini'), 200000);
    assert.equal(getContextWindow('o4-mini'), 200000);
    // A 2-character key must not match inside another id, and a separator still counts.
    assert.equal(getContextWindow('vendoro1'), 128000);
    assert.equal(getContextWindow('vendor-o1'), 200000);
    assert.equal(getContextWindow('vendoro3'), 128000);
    assert.equal(getContextWindow('vendor-o3'), 200000);
  });

  it('returns 256k for Qwen3 / Qwen3.8', () => {
    assert.equal(getContextWindow('Qwen/Qwen3.8-27B'), 262144);
    assert.equal(getContextWindow('Qwen/Qwen3-235B-A22B'), 262144);
  });

  it('returns correct context for Qwen2.5 models (size-specific)', () => {
    assert.equal(getContextWindow('Qwen/Qwen2.5-0.5B-Instruct'), 32768);
    assert.equal(getContextWindow('Qwen/Qwen2.5-1.5B-Instruct'), 32768);
    assert.equal(getContextWindow('Qwen/Qwen2.5-3B-Instruct'), 32768);
    assert.equal(getContextWindow('Qwen/Qwen2.5-7B-Instruct'), 131072);
    assert.equal(getContextWindow('Qwen/Qwen2.5-14B'), 131072);
    assert.equal(getContextWindow('Qwen/Qwen2.5-32B'), 131072);
    assert.equal(getContextWindow('Qwen/Qwen2.5-72B-Instruct'), 131072);
  });

  it('case-insensitive lookup handles mixed-case HuggingFace IDs', () => {
    assert.equal(getContextWindow('QWEN2.5-0.5B'), 32768);
    assert.equal(getContextWindow('qwen2.5-72b-instruct'), 131072);
    assert.equal(getContextWindow('Qwen/QWEN3-8B'), 262144);
  });

  it('returns correct context for small open-weight models', () => {
    assert.equal(getContextWindow('meta-llama/Llama-3.2-1B-Instruct'), 131072);
    assert.equal(getContextWindow('microsoft/phi-3-mini-4k-instruct'), 4096);
    assert.equal(getContextWindow('microsoft/Phi-3.5-mini-instruct'), 131072);
  });
});

describe('advertised context window', () => {
  it('reads provider field names and ignores a zero or a completion cap', () => {
    assert.equal(readAdvertisedContextWindow({ max_input_tokens: 1_000_000, max_tokens: 128_000 }), 1_000_000);
    assert.equal(readAdvertisedContextWindow({ max_model_len: 32_768 }), 32_768);
    assert.equal(readAdvertisedContextWindow({ context_length: 200_000 }), 200_000);
    assert.equal(readAdvertisedContextWindow({ inputTokenLimit: 1_048_576 }), 1_048_576);
    assert.equal(readAdvertisedContextWindow({ input_token_limit: 77_000 }), 77_000);
    assert.equal(readAdvertisedContextWindow({ context_window: 4096 }), 4096);
    assert.equal(readAdvertisedContextWindow({ max_context_length: 2048 }), 2048);
    assert.equal(readAdvertisedContextWindow({ max_position_embeddings: 8192.9 }), 8192);
    assert.equal(readAdvertisedContextWindow({ max_model_len: '32000' }), undefined);
    assert.equal(readAdvertisedContextWindow({ max_input_tokens: 0, max_tokens: 8192 }), undefined);
    assert.equal(readAdvertisedContextWindow({ max_model_len: Number.NaN }), undefined);
    assert.equal(readAdvertisedContextWindow({ model_info: { max_input_tokens: 65_536 } }), 65_536);
    assert.equal(readAdvertisedContextWindow({ model_info: ['nope'] }), undefined);
    assert.equal(collectModelRecords(null).length, 0);
    assert.equal(collectModelRecords({}).length, 0);
    assert.equal(collectModelRecords({ id: 'only', max_model_len: 4096 }).length, 1);
  });

  it('uses the matching model, then a single-server length, then nothing', () => {
    const payload = {
      data: [
        { id: 'gpt-4o', object: 'model' },
        { id: 'gpt-6-astra', max_model_len: 1_050_000 },
      ],
    };
    const records = collectModelRecords(payload);
    assert.equal(pickAdvertisedContextWindow(records, 'gpt-6-astra'), 1_050_000);
    assert.equal(pickAdvertisedContextWindow(records, 'gpt-4o'), undefined);
    assert.equal(pickAdvertisedContextWindow(
      collectModelRecords({ data: [{ id: 'Qwen/Qwen3-8B', max_model_len: 32_768 }] }),
      'served-alias',
    ), 32_768);
    assert.equal(pickAdvertisedContextWindow(
      collectModelRecords({ models: [{ name: 'models/gemini-3.8-flash', inputTokenLimit: 1_048_576 }] }),
      'gemini-3.8-flash',
    ), 1_048_576);
    assert.equal(pickAdvertisedContextWindow(
      [{ id: 'a', max_model_len: 10 }, { id: 'b', max_model_len: 20 }],
      '',
    ), undefined);
    assert.equal(pickAdvertisedContextWindow(
      [{ max_model_len: 10 }, { id: 'b', max_model_len: 20 }],
      'nope',
    ), undefined);
    assert.equal(pickAdvertisedContextWindow(
      [{ id: 'models/', max_model_len: 10 }, { name: 'models/', max_model_len: 20 }],
      'models/',
    ), undefined);
  });
});

describe('Hugging Face model card', () => {
  it('uses the longest published length and ignores sentinels', () => {
    assert.equal(readHuggingFaceContextWindow({
      max_position_embeddings: 32_768,
      sliding_window: 131_072,
      use_sliding_window: false,
      original_max_position_embeddings: 4096,
    }), 131_072);
    assert.equal(readHuggingFaceContextWindow({
      max_position_embeddings: 32_768,
      sliding_window: 4096,
    }), 32_768);
    assert.equal(readHuggingFaceContextWindow({ model_max_length: 1e30 }), undefined);
    assert.equal(readHuggingFaceContextWindow({ model_max_length: 1_000_000_000 }), undefined);
    assert.equal(readHuggingFaceContextWindow({ model_max_length: 999_999_999 }), 999_999_999);
    assert.equal(readHuggingFaceContextWindow({ original_max_position_embeddings: 8192 }), undefined);
    assert.equal(readHuggingFaceContextWindow({ max_position_embeddings: 0 }), undefined);
    assert.equal(readHuggingFaceContextWindow({ max_position_embeddings: '32768' }), undefined);
    assert.equal(readHuggingFaceContextWindow(null), undefined);
    for (const [key, value] of [
      ['max_sequence_length', 100],
      ['max_sequence_len', 101],
      ['seq_length', 102],
      ['seq_len', 103],
      ['n_positions', 104],
      ['n_ctx', 105],
      ['model_max_length', 106],
      ['context_length', 107],
      ['max_input_tokens', 108],
    ] as const) {
      assert.equal(readHuggingFaceContextWindow({ [key]: value }), value);
    }
  });

  it('reads nested config and card data', () => {
    assert.equal(readHuggingFaceContextWindow({ text_config: { max_position_embeddings: 8192 } }), 8192);
    assert.equal(readHuggingFaceContextWindow({ llm_config: { seq_length: 4096 } }), 4096);
    assert.equal(readHuggingFaceContextWindow({ language_config: { n_positions: 2048 } }), 2048);
    assert.equal(readHuggingFaceContextWindow({ config: { n_ctx: 1024 } }), 1024);
    assert.equal(readHuggingFaceContextWindow({ cardData: { context_length: 65_536 } }), 65_536);
    assert.equal(readHuggingFaceContextWindow({ tokenizer_config: { model_max_length: 32_768 } }), 32_768);
    assert.equal(readHuggingFaceContextWindow({
      cardData: { context_length: 222 },
      config: { max_position_embeddings: 1000 },
    }), 1000);
  });

  it('accepts owner/name repos and a single served repo, not a catalog', () => {
    assert.equal(huggingfaceRepoId('Qwen/Qwen2.5-0.5B-Instruct'), 'Qwen/Qwen2.5-0.5B-Instruct');
    assert.equal(huggingfaceRepoId('models/Qwen/Qwen2.5-0.5B-Instruct'), 'Qwen/Qwen2.5-0.5B-Instruct');
    assert.equal(huggingfaceRepoId('gpt-4o'), undefined);
    assert.equal(huggingfaceRepoId('runpod/Qwen/Qwen3-8B'), undefined);
    assert.equal(huggingfaceRepoId('http://huggingface.co/org/name'), undefined);
    assert.equal(huggingfaceRepoId('org/../name'), undefined);
    assert.deepEqual(
      huggingfaceRepoCandidates('alias', [{ id: 'Qwen/Qwen3-8B', name: 'models/Qwen/Qwen3-8B' }]),
      ['Qwen/Qwen3-8B'],
    );
    assert.deepEqual(
      huggingfaceRepoCandidates('gpt-4o', [{ id: 'gpt-4o' }, { id: 'org/secret' }]),
      [],
    );
    assert.deepEqual(huggingfaceRepoCandidates('org/name', []), ['org/name']);
  });

  it('rejects ids that are not one owner and one name', () => {
    assert.equal(huggingfaceRepoId(''), undefined);
    assert.equal(huggingfaceRepoId('   '), undefined);
    assert.equal(huggingfaceRepoId('models/'), undefined);
    assert.equal(huggingfaceRepoId('org/my model'), undefined);
    assert.equal(huggingfaceRepoId('org\\name'), undefined);
    assert.equal(huggingfaceRepoId('org/..'), undefined);
    assert.equal(huggingfaceRepoId('../name'), undefined);
    assert.equal(huggingfaceRepoId('org/.'), undefined);
    assert.equal(huggingfaceRepoId('MODELS/Org/Name'), 'Org/Name');
    assert.equal(huggingfaceRepoId('org/name/'), 'org/name');
    assert.equal(huggingfaceRepoId('/'), undefined);
  });

  it('reads a config nested twice and ignores anything deeper', () => {
    assert.equal(readHuggingFaceContextWindow({
      text_config: { llm_config: { max_position_embeddings: 55 } },
    }), 55);
    assert.equal(readHuggingFaceContextWindow({
      text_config: { llm_config: { language_config: { max_position_embeddings: 77 } } },
    }), undefined);
    assert.equal(readHuggingFaceContextWindow({ text_config: ['nope'] }), undefined);
    assert.equal(readHuggingFaceContextWindow({ max_position_embeddings: -5 }), undefined);
    assert.equal(readHuggingFaceContextWindow({ max_position_embeddings: Number.POSITIVE_INFINITY }), undefined);
    assert.equal(readHuggingFaceContextWindow([]), undefined);
  });

  it('keeps one repo when the alias only differs by case', () => {
    assert.deepEqual(
      huggingfaceRepoCandidates('Org/Name', [{ id: 'org/name', name: 'ORG/NAME' }]),
      ['Org/Name'],
    );
    assert.deepEqual(
      huggingfaceRepoCandidates('alias', [{ id: 'org/one', name: 'org/two' }]),
      ['org/one', 'org/two'],
    );
    assert.deepEqual(huggingfaceRepoCandidates('alias', [{ id: 5, name: null }]), []);
  });
});

describe('Model capabilities', () => {
  it('detects Claude as reasoning model', () => {
    const caps = getModelCapabilities('claude-sonnet-4-6');
    assert.ok(caps);
    assert.ok(caps.supportsAdaptiveThinking);
  });

  it('detects Gemini 2.5 as reasoning model', () => {
    const caps = getModelCapabilities('gemini-2.5-pro');
    assert.ok(caps);
    assert.ok(caps.reasoning);
  });

  it('returns null for non-reasoning models', () => {
    assert.equal(getModelCapabilities('gpt-4o'), null);
  });
});
