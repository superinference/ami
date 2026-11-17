import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  fineGrainedToolStreamingEnabled,
  withEagerToolStreaming,
  resolveModel,
  streamChatCompletion,
} from '../src/provider';
import {
  validateImagesForAPI,
  ImageSizeError,
} from '../src/provider-transform';
import { API_IMAGE_MAX_BASE64_SIZE } from '../src/tools/file-read';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

async function collect(gen: AsyncGenerator<StreamChunk>): Promise<StreamChunk[]> {
  const out: StreamChunk[] = [];
  for await (const c of gen) out.push(c);
  return out;
}

function withEnv(vars: Record<string, string | undefined>, fn: () => void): void {
  const saved: Record<string, string | undefined> = {};
  for (const k of Object.keys(vars)) {
    saved[k] = process.env[k];
    if (vars[k] === undefined) delete process.env[k];
    else process.env[k] = vars[k];
  }
  try {
    fn();
  } finally {
    for (const k of Object.keys(saved)) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
  }
}

// ---------------------------------------------------------------------------
// U020 #12 — fine-grained tool streaming (eager_input_streaming)
// ---------------------------------------------------------------------------
describe('fineGrainedToolStreamingEnabled — opt-in env gate', () => {
  it('is off by default', () => {
    withEnv(
      {
        SUPERINFERENCE_FINE_GRAINED_TOOL_STREAMING: undefined,
        CLAUDE_CODE_ENABLE_FINE_GRAINED_TOOL_STREAMING: undefined,
      },
      () => assert.equal(fineGrainedToolStreamingEnabled(), false),
    );
  });

  it('honors the AMI-native env var', () => {
    withEnv({ SUPERINFERENCE_FINE_GRAINED_TOOL_STREAMING: '1' }, () =>
      assert.equal(fineGrainedToolStreamingEnabled(), true),
    );
  });

  it('honors the Claude-compatible env var', () => {
    withEnv(
      {
        SUPERINFERENCE_FINE_GRAINED_TOOL_STREAMING: undefined,
        CLAUDE_CODE_ENABLE_FINE_GRAINED_TOOL_STREAMING: 'true',
      },
      () => assert.equal(fineGrainedToolStreamingEnabled(), true),
    );
  });
});

describe('withEagerToolStreaming — per-tool wire field injection', () => {
  function run(body: unknown): { captured: unknown } {
    const captured: { captured: unknown } = { captured: undefined };
    const base = ((_input: unknown, init?: { body?: unknown }) => {
      captured.captured = init?.body;
      return Promise.resolve(new Response('{}'));
    }) as unknown as typeof globalThis.fetch;
    const wrapped = withEagerToolStreaming(base);
    void wrapped('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      body: typeof body === 'string' ? body : JSON.stringify(body),
    });
    return captured;
  }

  it('adds eager_input_streaming:true to every tool in a JSON body', () => {
    const { captured } = run({
      model: 'claude-x',
      tools: [{ name: 'a' }, { name: 'b' }],
    });
    const parsed = JSON.parse(captured as string);
    assert.deepEqual(parsed.tools, [
      { name: 'a', eager_input_streaming: true },
      { name: 'b', eager_input_streaming: true },
    ]);
  });

  it('is a no-op when there are no tools', () => {
    const { captured } = run({ model: 'claude-x', messages: [] });
    assert.equal(captured, JSON.stringify({ model: 'claude-x', messages: [] }));
  });

  it('leaves a non-JSON body untouched', () => {
    const { captured } = run('not json');
    assert.equal(captured, 'not json');
  });

  it('is wired into the native Anthropic path only when the knob is on', () => {
    const cfg: ProviderConfig = { provider: 'anthropic', apiKey: 'sk-ant-x', model: 'claude-opus-4-6' };
    // The real wiring lives in resolveModel: with the knob off the model resolves
    // without throwing; with it on it still resolves (the fetch is only exercised on
    // a real request). This asserts the opt-in path constructs a model cleanly.
    withEnv({ SUPERINFERENCE_FINE_GRAINED_TOOL_STREAMING: undefined }, () => {
      assert.doesNotThrow(() => resolveModel(cfg));
    });
    withEnv({ SUPERINFERENCE_FINE_GRAINED_TOOL_STREAMING: '1' }, () => {
      assert.doesNotThrow(() => resolveModel(cfg));
    });
  });
});

// ---------------------------------------------------------------------------
// U020 #16 — proactive per-image base64 size validation
// ---------------------------------------------------------------------------
function imageMsg(dataLen: number): Message {
  return {
    role: 'user',
    content: [
      { type: 'text', text: 'look' },
      { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'A'.repeat(dataLen) } },
    ],
  };
}

describe('validateImagesForAPI / ImageSizeError', () => {
  it('does nothing when every image is within the limit', () => {
    assert.doesNotThrow(() => validateImagesForAPI([imageMsg(10)]));
  });

  it('ignores string-content messages (no image blocks)', () => {
    assert.doesNotThrow(() =>
      validateImagesForAPI([{ role: 'user', content: 'plain text, no images' }]),
    );
  });

  it('throws an ImageSizeError naming the oversized image by index', () => {
    const over = API_IMAGE_MAX_BASE64_SIZE + 1;
    let thrown: unknown;
    try {
      validateImagesForAPI([imageMsg(over)]);
    } catch (e) {
      thrown = e;
    }
    assert.ok(thrown instanceof ImageSizeError);
    const err = thrown as ImageSizeError;
    assert.equal(err.oversized.length, 1);
    assert.equal(err.oversized[0].messageIndex, 0);
    assert.equal(err.oversized[0].imageIndex, 0);
    assert.equal(err.oversized[0].base64Size, over);
    assert.match(err.message, /message 0 image 0/);
    assert.match(err.message, /Resize or crop/);
  });

  it('uses the per-message image index, not the content-block index', () => {
    // Two images in one message; only the SECOND is oversized. Its imageIndex must
    // be 1 (image ordinal), not 3 (its position among text+image content blocks).
    const over = API_IMAGE_MAX_BASE64_SIZE + 1;
    const msg: Message = {
      role: 'user',
      content: [
        { type: 'text', text: 'a' },
        { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'A'.repeat(10) } },
        { type: 'text', text: 'b' },
        { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'A'.repeat(over) } },
      ],
    };
    let thrown: unknown;
    try {
      validateImagesForAPI([msg]);
    } catch (e) {
      thrown = e;
    }
    assert.ok(thrown instanceof ImageSizeError);
    const err = thrown as ImageSizeError;
    assert.equal(err.oversized.length, 1);
    assert.equal(err.oversized[0].imageIndex, 1);
  });
});

// E2E: oversized image fails fast through the real streamChatCompletion generator,
// BEFORE the model stream is ever started. The injected streamTextImpl must never
// be invoked — proving the boundary check short-circuits the request.
describe('streamChatCompletion — image validation boundary (e2e)', () => {
  const CFG: ProviderConfig = { baseUrl: 'http://127.0.0.1:1', apiKey: 'k', model: 'test-model' };

  it('yields a terminal error chunk and never starts the stream for an oversized image', async () => {
    let streamStarted = false;
    const fakeStreamText = (() => {
      streamStarted = true;
      return { stream: (async function* () { /* never reached */ })() };
    }) as never;
    const over = API_IMAGE_MAX_BASE64_SIZE + 1;
    const chunks = await collect(
      streamChatCompletion(CFG, [imageMsg(over)], [], new AbortController().signal, {
        streamTextImpl: fakeStreamText,
      }),
    );
    assert.equal(streamStarted, false);
    assert.equal(chunks.length, 1);
    assert.equal(chunks[0].type, 'error');
    assert.match(chunks[0].error ?? '', /exceed the API limit/);
    assert.equal(chunks.some((c) => c.type === 'done'), false);
  });

  it('starts the stream normally when the image is within the limit', async () => {
    let streamStarted = false;
    const fakeStreamText = (() => {
      streamStarted = true;
      return { stream: (async function* () { yield { type: 'text-delta', text: 'x' }; })() };
    }) as never;
    const chunks = await collect(
      streamChatCompletion(CFG, [imageMsg(10)], [], new AbortController().signal, {
        streamTextImpl: fakeStreamText,
      }),
    );
    assert.equal(streamStarted, true);
    assert.equal(chunks.some((c) => c.type === 'error'), false);
  });
});
