import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { agenticFetchTool } from '../src/tools/agentic-fetch';
import type { ToolContext, EngineEvent } from '../src/types';

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
