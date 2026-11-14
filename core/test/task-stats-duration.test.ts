import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { taskTool } from '../src/tools/task';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

describe('taskTool — completed-agent usage trailer', () => {
  it('reports elapsed duration alongside tool calls + tokens', async () => {
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'done' };
      yield { type: 'turn_complete' as const };
    }

    const result = await taskTool.execute(
      { prompt: 'measure me', mode: 'explore' },
      ctx({
        _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
        _engineFactory: (_cfg) => ({ submit: fakeSubmit }),
      }),
    );

    assert.ok(!result.isError);
    // Trailer must carry the duration token (Ns) in addition to calls + tokens.
    // Without the duration change this assertion fails (old trailer ended at "tokens]").
    assert.match(result.output, /\[Agent stats: \d+ tool calls, \d+ tokens, \d+(?:\.\d+)?s\]/);
  });
});
