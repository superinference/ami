import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { agenticFetchTool } from '../src/tools/agentic-fetch';
import type { ToolContext, EngineEvent } from '../src/types';

function ctxWith(factory: any, signal?: AbortSignal): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: signal ?? new AbortController().signal,
    _engineFactory: factory,
    _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' } as any,
  } as ToolContext;
}

function yieldFactory(events: EngineEvent[]) {
  let captured: any = null;
  const factory = (config: any) => {
    captured = config;
    return {
      submit: async function* () { for (const e of events) yield e; },
      shutdown: () => {},
    };
  };
  return { factory, getConfig: () => captured };
}

function baseCtx(): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
  };
}

function makeMockEngineFactory(events: EngineEvent[]) {
  return (_config: any) => ({
    submit: async function* (_prompt: string) {
      for (const e of events) yield e;
    },
    shutdown: () => {},
  });
}

describe('agenticFetchTool — definition', () => {
  it('has the correct name', () => {
    assert.equal(agenticFetchTool.name, 'agentic_fetch');
  });

  it('is read-only', () => {
    assert.equal(agenticFetchTool.isReadOnly, true);
  });

  it('requires query', () => {
    assert.ok(agenticFetchTool.inputSchema.required?.includes('query'));
  });
});

describe('agenticFetchTool — no engine factory', () => {
  it('returns error when _engineFactory is not available', async () => {
    const result = await agenticFetchTool.execute(
      { query: 'test query' },
      baseCtx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Engine factory not available'));
  });
});

describe('agenticFetchTool — successful execution', () => {
  it('returns synthesized result from subengine', async () => {
    const events: EngineEvent[] = [
      { type: 'text_delta', text: 'Here are the ' } as any,
      { type: 'text_delta', text: 'search results.' } as any,
    ];
    const ctx: ToolContext = {
      ...baseCtx(),
      _engineFactory: makeMockEngineFactory(events) as any,
      _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' } as any,
    };

    const result = await agenticFetchTool.execute({ query: 'test' }, ctx);
    assert.equal(result.isError, undefined);
    assert.equal(result.output, 'Here are the search results.');
  });

  it('passes max_pages to subengine prompt', async () => {
    let capturedPrompt = '';
    const factory = (_config: any) => ({
      submit: async function* (prompt: string) {
        capturedPrompt = prompt;
        yield { type: 'text_delta', text: 'result' } as any;
      },
      shutdown: () => {},
    });

    const ctx: ToolContext = {
      ...baseCtx(),
      _engineFactory: factory as any,
      _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' } as any,
    };

    await agenticFetchTool.execute({ query: 'AI news', max_pages: 5 }, ctx);
    assert.ok(capturedPrompt.includes('5'), 'Prompt should include max_pages value');
    assert.ok(capturedPrompt.includes('AI news'), 'Prompt should include the query');
  });

  it('defaults max_pages to 3', async () => {
    let capturedPrompt = '';
    const factory = (_config: any) => ({
      submit: async function* (prompt: string) {
        capturedPrompt = prompt;
        yield { type: 'text_delta', text: 'ok' } as any;
      },
      shutdown: () => {},
    });

    const ctx: ToolContext = {
      ...baseCtx(),
      _engineFactory: factory as any,
      _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' } as any,
    };

    await agenticFetchTool.execute({ query: 'test' }, ctx);
    assert.ok(capturedPrompt.includes('3'), 'Default max_pages should be 3');
  });
});

describe('agenticFetchTool — empty result fallback', () => {
  it('returns fallback message when subengine produces no text', async () => {
    const events: EngineEvent[] = [
      { type: 'done', totalTurns: 1 } as any,
    ];
    const ctx: ToolContext = {
      ...baseCtx(),
      _engineFactory: makeMockEngineFactory(events) as any,
      _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' } as any,
    };

    const result = await agenticFetchTool.execute({ query: 'obscure topic' }, ctx);
    assert.ok(result.output.includes('No results found'));
    assert.ok(result.output.includes('obscure topic'));
  });
});

describe('agenticFetchTool — engine factory config', () => {
  it('creates subengine with auto-allow permissions and bounded turns', async () => {
    let capturedConfig: any = null;
    const factory = (config: any) => {
      capturedConfig = config;
      return {
        submit: async function* () {
          yield { type: 'text_delta', text: 'done' } as any;
        },
        shutdown: () => {},
      };
    };

    const ctx: ToolContext = {
      ...baseCtx(),
      _engineFactory: factory as any,
      _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' } as any,
    };

    await agenticFetchTool.execute({ query: 'test' }, ctx);
    assert.equal(capturedConfig.permissionMode, 'auto-allow');
    assert.equal(capturedConfig.maxTurns, 5);
    assert.equal(capturedConfig.tokenBudget, 16000);
  });

  it('forwards the parent permission rules + prompt handler so domain deny rules apply', async () => {
    let capturedConfig: any = null;
    const factory = (config: any) => {
      capturedConfig = config;
      return {
        submit: async function* () { yield { type: 'text_delta', text: 'ok' } as any; },
        shutdown: () => {},
      };
    };
    const denyRules = [{ tool: 'web_fetch', pattern: 'evil.example.com', action: 'deny' as const }];
    const promptHandler = { requestPermission: async () => ({ action: 'deny' as const }) } as any;
    const ctx: ToolContext = {
      ...baseCtx(),
      _engineFactory: factory as any,
      _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' } as any,
      _parentPermissionRules: denyRules,
      _permissionPromptHandler: promptHandler,
    };

    await agenticFetchTool.execute({ query: 'test' }, ctx);
    // Still autonomous (auto-allow) but inherits the parent's deny rules + handler,
    // so a per-domain denylist still blocks the subagent's web_fetch/web_search.
    assert.equal(capturedConfig.permissionMode, 'auto-allow');
    assert.deepEqual(capturedConfig.permissionRules, denyRules);
    assert.equal(capturedConfig.permissionPromptHandler, promptHandler);
  });

  it('calls shutdown on the subengine', async () => {
    let shutdownCalled = false;
    const factory = (_config: any) => ({
      submit: async function* () {
        yield { type: 'text_delta', text: 'ok' } as any;
      },
      shutdown: () => { shutdownCalled = true; },
    });

    const ctx: ToolContext = {
      ...baseCtx(),
      _engineFactory: factory as any,
      _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' } as any,
    };

    await agenticFetchTool.execute({ query: 'test' }, ctx);
    assert.ok(shutdownCalled, 'shutdown must be called on the subengine');
  });
});

describe('agenticFetchTool — input validation', () => {
  it('rejects a missing/non-string query', async () => {
    const { factory } = yieldFactory([{ type: 'text_delta', text: 'x' } as any]);
    const r = await agenticFetchTool.execute({ query: 123 as any }, ctxWith(factory));
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('query'));
  });

  it('rejects an empty / whitespace-only query', async () => {
    const { factory } = yieldFactory([{ type: 'text_delta', text: 'x' } as any]);
    const r = await agenticFetchTool.execute({ query: '   ' }, ctxWith(factory));
    assert.equal(r.isError, true);
  });

  it('rejects a too-short (1-char) query', async () => {
    const { factory } = yieldFactory([{ type: 'text_delta', text: 'x' } as any]);
    const r = await agenticFetchTool.execute({ query: 'a' }, ctxWith(factory));
    assert.equal(r.isError, true);
  });

  it('rejects a non-finite max_pages', async () => {
    const { factory } = yieldFactory([{ type: 'text_delta', text: 'x' } as any]);
    const r = await agenticFetchTool.execute({ query: 'valid', max_pages: 'abc' as any }, ctxWith(factory));
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('max_pages'));
  });

  it('clamps max_pages above the limit to 10', async () => {
    let prompt = '';
    const factory = (_c: any) => ({
      submit: async function* (p: string) { prompt = p; yield { type: 'text_delta', text: 'ok' } as any; },
      shutdown: () => {},
    });
    await agenticFetchTool.execute({ query: 'valid', max_pages: 50 }, ctxWith(factory));
    assert.ok(prompt.includes('up to 10'), prompt);
  });

  it('clamps a negative max_pages to 1', async () => {
    let prompt = '';
    const factory = (_c: any) => ({
      submit: async function* (p: string) { prompt = p; yield { type: 'text_delta', text: 'ok' } as any; },
      shutdown: () => {},
    });
    await agenticFetchTool.execute({ query: 'valid', max_pages: -5 }, ctxWith(factory));
    assert.ok(prompt.includes('up to 1'), prompt);
  });

  it('floors a fractional max_pages', async () => {
    let prompt = '';
    const factory = (_c: any) => ({
      submit: async function* (p: string) { prompt = p; yield { type: 'text_delta', text: 'ok' } as any; },
      shutdown: () => {},
    });
    await agenticFetchTool.execute({ query: 'valid', max_pages: 2.9 }, ctxWith(factory));
    assert.ok(prompt.includes('up to 2'), prompt);
  });

  it('treats null max_pages as the default', async () => {
    let prompt = '';
    const factory = (_c: any) => ({
      submit: async function* (p: string) { prompt = p; yield { type: 'text_delta', text: 'ok' } as any; },
      shutdown: () => {},
    });
    await agenticFetchTool.execute({ query: 'valid', max_pages: null as any }, ctxWith(factory));
    assert.ok(prompt.includes('up to 3'), prompt);
  });

  it('advertises minLength and max bounds in the input schema', () => {
    const props: any = agenticFetchTool.inputSchema.properties;
    assert.equal(props.query.minLength, 2);
    assert.equal(props.max_pages.maximum, 10);
  });
});

describe('agenticFetchTool — quoted/garbage max_pages coercion (semanticNumber parity)', () => {
  function promptFactory() {
    let prompt = '';
    const factory = (_c: any) => ({
      submit: async function* (p: string) { prompt = p; yield { type: 'text_delta', text: 'ok' } as any; },
      shutdown: () => {},
    });
    return { factory, getPrompt: () => prompt };
  }

  it('coerces a model-quoted numeric string max_pages ("5")', async () => {
    const { factory, getPrompt } = promptFactory();
    const r = await agenticFetchTool.execute({ query: 'valid', max_pages: '5' as any }, ctxWith(factory));
    assert.equal(r.isError, undefined);
    assert.ok(getPrompt().includes('up to 5'), getPrompt());
  });

  it('coerces a quoted fractional string then floors it ("2.9" -> 2)', async () => {
    const { factory, getPrompt } = promptFactory();
    await agenticFetchTool.execute({ query: 'valid', max_pages: '2.9' as any }, ctxWith(factory));
    assert.ok(getPrompt().includes('up to 2'), getPrompt());
  });

  // Discriminators: a bare Number() coerced these to 0 (then clamped to 1) or 1,
  // silently injecting a bogus page count. coerceNumber rejects them instead.
  it('rejects an empty-string max_pages instead of silently clamping to 1', async () => {
    const { factory } = promptFactory();
    const r = await agenticFetchTool.execute({ query: 'valid', max_pages: '' as any }, ctxWith(factory));
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('max_pages'));
  });

  it('rejects a boolean max_pages (false no longer becomes 1)', async () => {
    const { factory } = promptFactory();
    const r = await agenticFetchTool.execute({ query: 'valid', max_pages: false as any }, ctxWith(factory));
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('max_pages'));
  });

  it('rejects a boolean true max_pages (no longer coerces to 1)', async () => {
    const { factory } = promptFactory();
    const r = await agenticFetchTool.execute({ query: 'valid', max_pages: true as any }, ctxWith(factory));
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('max_pages'));
  });

  it('rejects an array max_pages (Number([]) no longer becomes 0/1)', async () => {
    const { factory } = promptFactory();
    const r = await agenticFetchTool.execute({ query: 'valid', max_pages: [] as any }, ctxWith(factory));
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('max_pages'));
  });
});

describe('agenticFetchTool — provider guard', () => {
  it('errors when provider config is missing even if a factory exists', async () => {
    const ctx = {
      cwd: process.cwd(),
      abortSignal: new AbortController().signal,
      _engineFactory: yieldFactory([]).factory,
    } as ToolContext;
    const r = await agenticFetchTool.execute({ query: 'valid' }, ctx);
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('Provider configuration not available'));
  });
});

describe('agenticFetchTool — prompt guidance', () => {
  it('includes current-year grounding, a Sources section, and redirect/gh hints', async () => {
    let prompt = '';
    const factory = (_c: any) => ({
      submit: async function* (p: string) { prompt = p; yield { type: 'text_delta', text: 'ok' } as any; },
      shutdown: () => {},
    });
    await agenticFetchTool.execute({ query: 'valid' }, ctxWith(factory));
    assert.ok(prompt.includes(String(new Date().getFullYear())), 'should ground on current year');
    assert.ok(prompt.includes('Sources:'), 'should require a Sources section');
    assert.ok(/redirect/i.test(prompt), 'should mention redirects');
    assert.ok(/\bgh\b/.test(prompt), 'should mention the gh CLI');
  });
});

describe('agenticFetchTool — abort propagation', () => {
  it('threads an AbortController into the sub-engine config', async () => {
    const { factory, getConfig } = yieldFactory([{ type: 'text_delta', text: 'ok' } as any]);
    await agenticFetchTool.execute({ query: 'valid' }, ctxWith(factory));
    assert.ok(getConfig().abortController instanceof AbortController);
  });

  it('returns an aborted error when the caller signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const { factory } = yieldFactory([{ type: 'text_delta', text: 'never' } as any]);
    const r = await agenticFetchTool.execute({ query: 'valid' }, ctxWith(factory, ac.signal));
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('aborted'));
    assert.ok(!r.output.includes('never'), 'must not append events after abort');
  });

  it('stops mid-stream and returns partial results when aborted during the loop', async () => {
    const ac = new AbortController();
    const factory = (_c: any) => ({
      submit: async function* () {
        yield { type: 'text_delta', text: 'partial' } as any;
        ac.abort();
        yield { type: 'text_delta', text: '-after' } as any;
      },
      shutdown: () => {},
    });
    const r = await agenticFetchTool.execute({ query: 'valid' }, ctxWith(factory, ac.signal));
    assert.equal(r.isError, true);
    assert.ok(r.output.startsWith('partial'));
    assert.ok(!r.output.includes('-after'), 'must break before appending post-abort text');
  });
});

describe('agenticFetchTool — failure-mode discrimination', () => {
  it('returns isError when the subengine emits an error event and no text', async () => {
    const { factory } = yieldFactory([{ type: 'error', error: 'boom' } as any]);
    const r = await agenticFetchTool.execute({ query: 'valid' }, ctxWith(factory));
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('boom'));
  });

  it('prefers partial text over an error event when some text arrived', async () => {
    const { factory } = yieldFactory([
      { type: 'text_delta', text: 'answer' } as any,
      { type: 'error', error: 'late-warning' } as any,
    ]);
    const r = await agenticFetchTool.execute({ query: 'valid' }, ctxWith(factory));
    assert.equal(r.isError, undefined);
    assert.equal(r.output, 'answer');
  });

  it('distinguishes early-exhaustion exit from an empty result', async () => {
    const { factory } = yieldFactory([{ type: 'done', totalTurns: 5, exitReason: 'max_turns' } as any]);
    const r = await agenticFetchTool.execute({ query: 'valid' }, ctxWith(factory));
    assert.equal(r.isError, undefined);
    assert.ok(r.output.includes('max_turns'));
    assert.ok(!r.output.includes('No results found'));
  });

  it('falls back to "No results found" on a clean empty completion', async () => {
    const { factory } = yieldFactory([{ type: 'done', totalTurns: 1, exitReason: 'completed' } as any]);
    const r = await agenticFetchTool.execute({ query: 'obscure' }, ctxWith(factory));
    assert.ok(r.output.includes('No results found'));
    assert.ok(r.output.includes('obscure'));
  });

  it('returns isError with the message when the subengine throws an Error', async () => {
    const factory = (_c: any) => ({
      submit: async function* () { throw new Error('kaboom'); },
      shutdown: () => {},
    });
    const r = await agenticFetchTool.execute({ query: 'valid' }, ctxWith(factory));
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('kaboom'));
  });

  it('stringifies a non-Error throw from the subengine', async () => {
    const factory = (_c: any) => ({
      submit: async function* () { throw 'raw-string-failure'; },
      shutdown: () => {},
    });
    const r = await agenticFetchTool.execute({ query: 'valid' }, ctxWith(factory));
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('raw-string-failure'));
  });

  it('still shuts the subengine down when it throws', async () => {
    let shutdown = false;
    const factory = (_c: any) => ({
      submit: async function* () { throw new Error('x'); },
      shutdown: () => { shutdown = true; },
    });
    await agenticFetchTool.execute({ query: 'valid' }, ctxWith(factory));
    assert.ok(shutdown, 'finally must shut down the subengine on throw');
  });
});

describe('agenticFetchTool — wall-clock timeout', () => {
  it('aborts and returns a timed-out partial result when the budget elapses', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const factory = (_c: any) => ({
      submit: async function* () {
        yield { type: 'text_delta', text: 'partial' } as any;
        // Hang well past the research budget; only the mocked timer releases it.
        await new Promise((resolve) => setTimeout(resolve, 10_000_000));
        yield { type: 'text_delta', text: '-after-timeout' } as any;
      },
      shutdown: () => {},
    });

    const p = agenticFetchTool.execute({ query: 'valid' }, ctxWith(factory));
    // Let the generator emit 'partial' and reach its hang before advancing time.
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));
    // Fire every pending timer (research budget + the generator's hang).
    t.mock.timers.tick(10_000_000);
    const r = await p;

    assert.equal(r.isError, true);
    assert.ok(r.output.startsWith('partial'), r.output);
    assert.ok(/timed out/i.test(r.output), r.output);
    assert.ok(!r.output.includes('-after-timeout'), 'must break before appending post-timeout text');
  });
});
