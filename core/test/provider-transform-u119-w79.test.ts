import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  healOrphanedToolCalls,
  getStrictToolResultPairing,
  STRICT_TOOL_PAIRING_ENV,
  ToolPairingError,
  buildToolPairingDiagnosticSummary,
  filterOrphanedThinkingOnlyMessages,
  type ToolPairingRepairDiagnostics,
} from '../src/provider-transform';
import { streamChatCompletion } from '../src/provider';
import type { Message } from '../src/types';
import type { StreamChunk } from '../src/provider';

// ---------------------------------------------------------------------------
// U119 / w79 — strict + observable tool-pairing repair, orphaned-thinking filter
// (gaps: strict tool-pairing mode, repair diagnostics, repair observability,
//  filter orphaned thinking-only assistant messages before resume)
// ---------------------------------------------------------------------------

const asm = (id: string): Message => ({
  role: 'assistant',
  content: null,
  tool_calls: [{ id, type: 'function', function: { name: 'bash', arguments: '{}' } }],
});

describe('getStrictToolResultPairing — env gate', () => {
  afterEach(() => { delete process.env[STRICT_TOOL_PAIRING_ENV]; });

  it('defaults OFF when the env knob is unset', () => {
    delete process.env[STRICT_TOOL_PAIRING_ENV];
    assert.equal(getStrictToolResultPairing(), false);
  });

  it('is ON for a truthy value', () => {
    assert.equal(getStrictToolResultPairing({ [STRICT_TOOL_PAIRING_ENV]: '1' }), true);
    assert.equal(getStrictToolResultPairing({ [STRICT_TOOL_PAIRING_ENV]: 'true' }), true);
  });

  it('is OFF for a falsy value', () => {
    assert.equal(getStrictToolResultPairing({ [STRICT_TOOL_PAIRING_ENV]: '0' }), false);
    assert.equal(getStrictToolResultPairing({ [STRICT_TOOL_PAIRING_ENV]: '' }), false);
  });

  it('reads process.env by default', () => {
    process.env[STRICT_TOOL_PAIRING_ENV] = 'yes';
    assert.equal(getStrictToolResultPairing(), true);
  });
});

describe('buildToolPairingDiagnosticSummary', () => {
  it('summarizes assistant tool_uses, tool results, and plain roles by index', () => {
    const messages: Message[] = [
      { role: 'user', content: 'go' },
      asm('c1'),
      { role: 'tool', tool_call_id: 'c1', content: 'ok' },
    ];
    assert.equal(
      buildToolPairingDiagnosticSummary(messages),
      '[0] user; [1] assistant(tool_uses=[c1]); [2] tool(tool_result=c1)',
    );
  });

  it('renders an assistant with no tool_calls as a plain role', () => {
    assert.equal(
      buildToolPairingDiagnosticSummary([{ role: 'assistant', content: 'hi' }]),
      '[0] assistant',
    );
  });
});

describe('healOrphanedToolCalls — observability (onRepair)', () => {
  it('invokes onRepair with synthetic-result diagnostics for an unanswered tool_call', () => {
    const diags: ToolPairingRepairDiagnostics[] = [];
    const messages: Message[] = [
      { role: 'user', content: 'go' },
      asm('c1'), // no tool result → synthetic inserted
    ];
    const healed = healOrphanedToolCalls(messages, { onRepair: (d) => diags.push(d) });
    assert.equal(diags.length, 1);
    assert.equal(diags[0].syntheticResultsInserted, 1);
    assert.equal(diags[0].orphanedResultsDropped, 0);
    assert.equal(diags[0].duplicateIdsRemoved, 0);
    assert.equal(diags[0].messageCountBefore, 2);
    assert.equal(diags[0].messageCountAfter, 3);
    assert.ok(diags[0].messageStructure.includes('assistant(tool_uses=[c1])'));
    // The repair still happened (behavior preserved).
    assert.ok(healed.some(m => m.role === 'tool' && m.tool_call_id === 'c1'));
  });

  it('reports orphaned-result drops', () => {
    const diags: ToolPairingRepairDiagnostics[] = [];
    const messages: Message[] = [
      { role: 'user', content: 'go' },
      { role: 'assistant', content: 'done' },
      { role: 'tool', tool_call_id: 'ghost', content: 'orphan' }, // no matching call
    ];
    healOrphanedToolCalls(messages, { onRepair: (d) => diags.push(d) });
    assert.equal(diags.length, 1);
    assert.equal(diags[0].orphanedResultsDropped, 1);
  });

  it('reports duplicate id removals', () => {
    const diags: ToolPairingRepairDiagnostics[] = [];
    const messages: Message[] = [
      { role: 'user', content: 'go' },
      asm('dup'),
      { role: 'tool', tool_call_id: 'dup', content: 'ok' },
      asm('dup'), // duplicate tool_use id
      { role: 'tool', tool_call_id: 'dup', content: 'again' }, // duplicate result
    ];
    healOrphanedToolCalls(messages, { onRepair: (d) => diags.push(d) });
    assert.equal(diags.length, 1);
    assert.ok(diags[0].duplicateIdsRemoved >= 1, 'duplicate ids counted');
  });

  it('reports the leading-orphan placeholder substitution', () => {
    const diags: ToolPairingRepairDiagnostics[] = [];
    const messages: Message[] = [
      { role: 'tool', tool_call_id: 'gone', content: 'orphan' },
      { role: 'assistant', content: 'continuing' },
    ];
    healOrphanedToolCalls(messages, { onRepair: (d) => diags.push(d) });
    assert.equal(diags.length, 1);
    assert.equal(diags[0].leadingOrphanPlaceholderInserted, true);
  });

  it('does NOT invoke onRepair for a well-formed transcript', () => {
    let called = 0;
    const messages: Message[] = [
      { role: 'user', content: 'go' },
      asm('c1'),
      { role: 'tool', tool_call_id: 'c1', content: 'ok' },
    ];
    healOrphanedToolCalls(messages, { onRepair: () => { called++; } });
    assert.equal(called, 0);
  });
});

describe('healOrphanedToolCalls — strict mode', () => {
  it('THROWS ToolPairingError with diagnostics on corruption', () => {
    const messages: Message[] = [
      { role: 'user', content: 'go' },
      asm('c1'), // unanswered → would repair
    ];
    assert.throws(
      () => healOrphanedToolCalls(messages, { strict: true }),
      (err: unknown) => {
        assert.ok(err instanceof ToolPairingError);
        assert.equal(err.diagnostics.syntheticResultsInserted, 1);
        assert.ok(err.message.includes('strict mode'));
        assert.ok(err.message.includes('assistant(tool_uses=[c1])'));
        return true;
      },
    );
  });

  it('does NOT throw in strict mode for a well-formed transcript', () => {
    const messages: Message[] = [
      { role: 'user', content: 'go' },
      asm('c1'),
      { role: 'tool', tool_call_id: 'c1', content: 'ok' },
    ];
    assert.doesNotThrow(() => healOrphanedToolCalls(messages, { strict: true }));
  });

  it('strict throw wins over onRepair (onRepair not called when it throws)', () => {
    let called = 0;
    const messages: Message[] = [{ role: 'user', content: 'go' }, asm('c1')];
    assert.throws(() => healOrphanedToolCalls(messages, { strict: true, onRepair: () => { called++; } }));
    assert.equal(called, 0);
  });
});

describe('filterOrphanedThinkingOnlyMessages', () => {
  it('drops an assistant message with null content and no tool_calls', () => {
    const messages: Message[] = [
      { role: 'user', content: 'go' },
      { role: 'assistant', content: null }, // thinking-only orphan
      { role: 'user', content: 'again' },
    ];
    const out = filterOrphanedThinkingOnlyMessages(messages);
    assert.equal(out.length, 2);
    assert.ok(!out.some(m => m.role === 'assistant'));
  });

  it('drops a whitespace-only assistant message', () => {
    const messages: Message[] = [
      { role: 'assistant', content: '   \n  ' },
      { role: 'user', content: 'hi' },
    ];
    assert.deepEqual(filterOrphanedThinkingOnlyMessages(messages), [{ role: 'user', content: 'hi' }]);
  });

  it('keeps an assistant message that has text content', () => {
    const messages: Message[] = [{ role: 'assistant', content: 'real answer' }];
    assert.equal(filterOrphanedThinkingOnlyMessages(messages).length, 1);
  });

  it('keeps an assistant message with tool_calls even when content is null', () => {
    const messages: Message[] = [asm('c1'), { role: 'tool', tool_call_id: 'c1', content: 'ok' }];
    assert.equal(filterOrphanedThinkingOnlyMessages(messages).length, 2);
  });

  it('returns the SAME reference when nothing is dropped', () => {
    const messages: Message[] = [{ role: 'user', content: 'go' }, { role: 'assistant', content: 'a' }];
    assert.equal(filterOrphanedThinkingOnlyMessages(messages), messages);
  });

  it('does not drop user/tool messages with empty-ish content', () => {
    const messages: Message[] = [
      { role: 'tool', tool_call_id: 'x', content: '' },
      { role: 'user', content: '' },
    ];
    assert.equal(filterOrphanedThinkingOnlyMessages(messages).length, 2);
  });
});

// E2E through the real provider request-building path (streamTextImpl DI seam).
describe('U119/w79 E2E: streamChatCompletion strict tool-pairing', () => {
  afterEach(() => { delete process.env[STRICT_TOOL_PAIRING_ENV]; });

  async function run(messages: Message[]): Promise<{ chunks: StreamChunk[]; dispatched: boolean }> {
    let dispatched = false;
    const fakeStreamText = () => {
      dispatched = true;
      return { stream: (async function* () { /* no parts */ })() };
    };
    const config = { provider: 'anthropic', apiKey: 'sk-x', model: 'claude-opus-4-6' } as never;
    const gen = streamChatCompletion(config, messages, [], new AbortController().signal, {
      streamTextImpl: fakeStreamText as never,
    });
    const chunks: StreamChunk[] = [];
    for await (const c of gen) chunks.push(c);
    return { chunks, dispatched };
  }

  const corrupt: Message[] = [
    { role: 'user', content: 'go' },
    asm('c1'), // unanswered tool_call → pairing must be repaired
  ];

  it('strict ON: a corrupt transcript yields a terminal error and never dispatches', async () => {
    process.env[STRICT_TOOL_PAIRING_ENV] = '1';
    const { chunks, dispatched } = await run(corrupt);
    assert.equal(dispatched, false, 'request must NOT be dispatched');
    const err = chunks.find(c => c.type === 'error') as { type: 'error'; error: string } | undefined;
    assert.ok(err, 'an error chunk is emitted');
    assert.ok(err!.error.includes('strict mode'));
  });

  it('strict OFF: the same transcript is repaired and dispatched (no pairing error)', async () => {
    delete process.env[STRICT_TOOL_PAIRING_ENV];
    const { chunks, dispatched } = await run(corrupt);
    assert.equal(dispatched, true, 'request IS dispatched after repair');
    // The only error (if any) comes from the empty fake stream, never the pairing guard.
    const pairingErr = chunks.find(
      c => c.type === 'error' && (c as { error: string }).error.includes('strict mode'),
    );
    assert.equal(pairingErr, undefined, 'no strict-mode pairing error when disabled');
  });
});
