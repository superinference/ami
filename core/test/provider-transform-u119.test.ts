import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  healOrphanedToolCalls,
  stripEmptyTextBlocks,
  stripExcessMediaItems,
  getPromptCachingEnabled,
  buildConversationCachePoints,
  buildToolCacheBreakpoints,
  SYNTHETIC_TOOL_RESULT_PLACEHOLDER,
  MAX_MEDIA_PER_REQUEST,
} from '../src/provider-transform';
import { streamChatCompletion } from '../src/provider';
import type { Message, ToolDefinition, ImageContent } from '../src/types';
import type { StreamChunk } from '../src/provider';

// ---------------------------------------------------------------------------
// U119 — gaps on provider-transform.ts
// ---------------------------------------------------------------------------

const img = (data = 'AAAA'): ImageContent => ({
  type: 'image',
  source: { type: 'base64', media_type: 'image/png', data },
});

// Gap: synthetic orphan tool-result flagged as error + cause-specific wording
describe('healOrphanedToolCalls — is_error + cause wording', () => {
  it('flags the synthetic placeholder tool_result with isError:true', () => {
    const msgs: Message[] = [
      {
        role: 'assistant',
        content: null,
        tool_calls: [{ id: 'c1', type: 'function', function: { name: 'bash', arguments: '{}' } }],
      },
    ];
    const out = healOrphanedToolCalls(msgs);
    const tool = out.find((m) => m.role === 'tool') as { role: 'tool'; content: string; isError?: boolean };
    assert.ok(tool, 'synthetic tool result present');
    assert.equal(tool.isError, true);
    assert.equal(tool.content, SYNTHETIC_TOOL_RESULT_PLACEHOLDER);
  });

  it('uses the caller-supplied cause wording for synthetic results', () => {
    const msgs: Message[] = [
      {
        role: 'assistant',
        content: null,
        tool_calls: [{ id: 'c1', type: 'function', function: { name: 'bash', arguments: '{}' } }],
      },
    ];
    const out = healOrphanedToolCalls(msgs, { placeholder: 'Result dropped during context compaction' });
    const tool = out.find((m) => m.role === 'tool') as { role: 'tool'; content: string; isError?: boolean };
    assert.equal(tool.content, 'Result dropped during context compaction');
    assert.equal(tool.isError, true);
  });
});

// Gap: drop messages left with empty content after block stripping
describe('stripEmptyTextBlocks — drop emptied non-assistant messages', () => {
  it('drops a non-assistant message whose text blocks were all empty', () => {
    const msgs = [
      { role: 'user', content: [{ type: 'text', text: '   ' }] },
      { role: 'user', content: [{ type: 'text', text: 'keep' }] },
    ];
    const out = stripEmptyTextBlocks(msgs);
    assert.equal(out.length, 1);
    assert.deepEqual(out[0].content, [{ type: 'text', text: 'keep' }]);
  });

  it('keeps an emptied assistant message as (continued)', () => {
    const msgs = [{ role: 'assistant', content: [{ type: 'text', text: '' }] }];
    const out = stripEmptyTextBlocks(msgs);
    assert.equal(out.length, 1);
    assert.deepEqual(out[0].content, [{ type: 'text', text: '(continued)' }]);
  });

  it('returns the filtered list (does not just mutate)', () => {
    const msgs = [{ role: 'tool', content: [{ type: 'text', text: '' }] }];
    const out = stripEmptyTextBlocks(msgs);
    assert.equal(out.length, 0);
  });
});

// Gap: proactive per-request media cap
describe('stripExcessMediaItems', () => {
  it('returns the same reference when at/under the limit', () => {
    const msgs: Message[] = [{ role: 'user', content: [img(), { type: 'text', text: 'hi' }] }];
    assert.equal(stripExcessMediaItems(msgs, 5), msgs);
  });

  it('strips the OLDEST media beyond the limit', () => {
    const msgs: Message[] = [
      { role: 'user', content: [img('old1'), img('old2')] },
      { role: 'user', content: [img('new1')] },
    ];
    const out = stripExcessMediaItems(msgs, 1);
    const remaining: string[] = [];
    for (const m of out) {
      if (m.role === 'user' && Array.isArray(m.content)) {
        for (const b of m.content) if (b.type === 'image') remaining.push(b.source.data);
      }
    }
    assert.deepEqual(remaining, ['new1']);
  });

  it('defaults the limit to MAX_MEDIA_PER_REQUEST', () => {
    assert.equal(MAX_MEDIA_PER_REQUEST, 100);
    const msgs: Message[] = [{ role: 'user', content: [img()] }];
    assert.equal(stripExcessMediaItems(msgs), msgs);
  });

  it('leaves string-content and assistant messages untouched', () => {
    const msgs: Message[] = [
      { role: 'assistant', content: 'a' },
      { role: 'user', content: 'plain' },
      { role: 'user', content: [img('x'), img('y')] },
    ];
    const out = stripExcessMediaItems(msgs, 1);
    assert.equal(out[0], msgs[0]);
    assert.equal(out[1], msgs[1]);
  });
});

// Gap: flag/parameter to disable prompt caching (global + per-model)
describe('getPromptCachingEnabled', () => {
  const KEYS = ['DISABLE_PROMPT_CACHING', 'DISABLE_PROMPT_CACHING_HAIKU', 'DISABLE_PROMPT_CACHING_SONNET', 'DISABLE_PROMPT_CACHING_OPUS'];
  afterEach(() => { for (const k of KEYS) delete process.env[k]; });

  it('defaults to enabled', () => {
    assert.equal(getPromptCachingEnabled('claude-opus-4-6'), true);
  });

  it('global DISABLE_PROMPT_CACHING disables for any model', () => {
    process.env.DISABLE_PROMPT_CACHING = '1';
    assert.equal(getPromptCachingEnabled('claude-opus-4-6'), false);
    assert.equal(getPromptCachingEnabled('gpt-5'), false);
  });

  it('per-model knobs disable only the matching family', () => {
    process.env.DISABLE_PROMPT_CACHING_HAIKU = 'true';
    assert.equal(getPromptCachingEnabled('claude-haiku-4-5'), false);
    assert.equal(getPromptCachingEnabled('claude-opus-4-6'), true);

    process.env.DISABLE_PROMPT_CACHING_SONNET = 'true';
    assert.equal(getPromptCachingEnabled('claude-sonnet-4-5'), false);

    process.env.DISABLE_PROMPT_CACHING_OPUS = 'true';
    assert.equal(getPromptCachingEnabled('claude-opus-4-6'), false);
  });

  it('respects env falsiness (0/false/off) as enabled', () => {
    process.env.DISABLE_PROMPT_CACHING = '0';
    assert.equal(getPromptCachingEnabled('claude-opus-4-6'), true);
  });
});

describe('cache-point builders honor enablePromptCaching', () => {
  it('buildConversationCachePoints returns {} when caching disabled', () => {
    const msgs = [{ role: 'user' }, { role: 'assistant' }];
    assert.deepEqual(buildConversationCachePoints(msgs, false), {});
    assert.notDeepEqual(buildConversationCachePoints(msgs, true), {});
  });

  it('buildToolCacheBreakpoints returns {} when caching disabled', () => {
    const tools = [{ name: 'a' } as unknown as ToolDefinition];
    assert.deepEqual(buildToolCacheBreakpoints(tools, false), {});
    assert.notDeepEqual(buildToolCacheBreakpoints(tools, true), {});
  });
});

// ---------------------------------------------------------------------------
// Integration — streamChatCompletion request-building path (real Engine seam)
// ---------------------------------------------------------------------------
describe('U119 integration: streamChatCompletion request building', () => {
  const KEYS = ['DISABLE_PROMPT_CACHING'];
  afterEach(() => { for (const k of KEYS) delete process.env[k]; });

  async function capture(messages: Message[], model = 'claude-opus-4-6'): Promise<Record<string, unknown>> {
    let captured: Record<string, unknown> = {};
    const fakeStreamText = (cfg: Record<string, unknown>) => {
      captured = cfg;
      return { stream: (async function* () { /* no parts */ })() };
    };
    const config = { provider: 'anthropic', apiKey: 'sk-x', model } as never;
    const gen = streamChatCompletion(config, messages, [], new AbortController().signal, {
      streamTextImpl: fakeStreamText as never,
    });
    const out: StreamChunk[] = [];
    for await (const c of gen) out.push(c);
    return captured;
  }

  it('caps media in the outbound request to the limit (oldest dropped)', async () => {
    // 101 images across user turns must be reduced to 100 (the oldest 1 stripped).
    const messages: Message[] = [];
    for (let i = 0; i < 101; i++) messages.push({ role: 'user', content: [img('d' + i)] });
    const cfg = await capture(messages);
    const sent = cfg.messages as Array<{ role: string; content: unknown }>;
    let imgCount = 0;
    for (const m of sent) {
      if (Array.isArray(m.content)) for (const b of m.content as Array<{ type: string }>) if (b.type === 'image') imgCount++;
    }
    assert.equal(imgCount, 100);
  });

  it('translates an isError tool message to an error-text tool-result output', async () => {
    const messages: Message[] = [
      { role: 'user', content: 'go' },
      { role: 'assistant', content: null, tool_calls: [{ id: 'c1', type: 'function', function: { name: 'bash', arguments: '{}' } }] },
      // no tool result -> healOrphanedToolCalls synthesizes an isError tool message
    ];
    const cfg = await capture(messages);
    const sent = cfg.messages as Array<{ role: string; content: unknown }>;
    const toolMsg = sent.find((m) => m.role === 'tool');
    assert.ok(toolMsg, 'tool message present');
    const part = (toolMsg!.content as Array<{ type: string; output?: { type: string } }>)[0];
    assert.equal(part.output!.type, 'error-text');
  });

  it('omits cache_control markers when prompt caching is disabled', async () => {
    process.env.DISABLE_PROMPT_CACHING = '1';
    const messages: Message[] = [
      { role: 'system', content: 'sys' },
      { role: 'user', content: 'hello' },
    ];
    const cfg = await capture(messages);
    const sent = cfg.messages as Array<{ providerOptions?: unknown }>;
    const anyCache = sent.some((m) => !!m.providerOptions);
    assert.equal(anyCache, false);
  });

  it('emits cache_control markers when prompt caching is enabled (default)', async () => {
    const messages: Message[] = [
      { role: 'system', content: 'sys' },
      { role: 'user', content: 'hello' },
    ];
    const cfg = await capture(messages);
    const sent = cfg.messages as Array<{ providerOptions?: unknown }>;
    const anyCache = sent.some((m) => !!m.providerOptions);
    assert.equal(anyCache, true);
  });
});
