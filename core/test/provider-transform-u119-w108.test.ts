import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  stripUnavailableToolReferences,
  UNAVAILABLE_TOOL_REFERENCE_PLACEHOLDER,
  type UnavailableToolReferenceDiagnostics,
} from '../src/provider-transform';
import { streamChatCompletion } from '../src/provider';
import type { Message, ToolDefinition } from '../src/types';
import type { StreamChunk } from '../src/provider';

// ---------------------------------------------------------------------------
// U119 / w108 — strip references to tools no longer in the request's tool set.
// Gap: healOrphanedToolCalls only reconciles id-pairing; it never validates the
// referenced tool NAME against the available set, so a tool_call/tool_result for a
// disconnected MCP server or removed plugin tool is still forwarded to the provider
// (which rejects it). Mirrors Claude's stripUnavailableToolReferencesFromUserMessage.
// ---------------------------------------------------------------------------

const asmCall = (id: string, name: string): Message => ({
  role: 'assistant',
  content: null,
  tool_calls: [{ id, type: 'function', function: { name, arguments: '{}' } }],
});
const toolResult = (id: string, content = 'ok'): Message => ({
  role: 'tool',
  tool_call_id: id,
  content,
});

describe('stripUnavailableToolReferences — unit', () => {
  it('drops a tool_call referencing an unavailable tool AND its paired result', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'go' },
      asmCall('c1', 'ghost_mcp_tool'),
      toolResult('c1', 'ghost output'),
    ];
    const out = stripUnavailableToolReferences(msgs, new Set(['bash']));
    // The assistant turn had only the ghost call + no text -> becomes placeholder.
    assert.equal(out.length, 2);
    const asst = out[1];
    assert.equal(asst.role, 'assistant');
    assert.equal((asst as { tool_calls?: unknown }).tool_calls, undefined);
    assert.equal((asst as { content: string }).content, UNAVAILABLE_TOOL_REFERENCE_PLACEHOLDER);
    assert.ok(!out.some(m => m.role === 'tool'), 'orphaned ghost result dropped');
  });

  it('keeps available tool_calls/results while dropping unavailable ones in one turn', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'go' },
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'c1', type: 'function', function: { name: 'bash', arguments: '{}' } },
          { id: 'c2', type: 'function', function: { name: 'ghost', arguments: '{}' } },
        ],
      },
      toolResult('c1', 'bash out'),
      toolResult('c2', 'ghost out'),
    ];
    const out = stripUnavailableToolReferences(msgs, new Set(['bash']));
    const asst = out.find(m => m.role === 'assistant') as { tool_calls: Array<{ id: string }> };
    assert.equal(asst.tool_calls.length, 1);
    assert.equal(asst.tool_calls[0].id, 'c1');
    const results = out.filter(m => m.role === 'tool') as Array<{ tool_call_id: string }>;
    assert.deepEqual(results.map(r => r.tool_call_id), ['c1']);
  });

  it('preserves the assistant text content when only its tool_calls are stripped', () => {
    const msgs: Message[] = [
      {
        role: 'assistant',
        content: 'here is my plan',
        tool_calls: [{ id: 'c1', type: 'function', function: { name: 'ghost', arguments: '{}' } }],
      },
      toolResult('c1'),
    ];
    const out = stripUnavailableToolReferences(msgs, new Set(['bash']));
    const asst = out[0] as { content: string; tool_calls?: unknown };
    assert.equal(asst.content, 'here is my plan');
    assert.equal(asst.tool_calls, undefined);
  });

  it('returns the SAME reference (no-op) when every referenced tool is available', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'go' },
      asmCall('c1', 'bash'),
      toolResult('c1'),
    ];
    const out = stripUnavailableToolReferences(msgs, new Set(['bash', 'grep']));
    assert.strictEqual(out, msgs);
  });

  it('accepts an array (not only a Set) for availableToolNames', () => {
    const msgs: Message[] = [asmCall('c1', 'ghost')];
    const out = stripUnavailableToolReferences(msgs, ['bash']);
    assert.equal((out[0] as { content: string }).content, UNAVAILABLE_TOOL_REFERENCE_PLACEHOLDER);
  });

  it('honors a custom placeholder', () => {
    const msgs: Message[] = [asmCall('c1', 'ghost')];
    const out = stripUnavailableToolReferences(msgs, new Set(['bash']), { placeholder: 'X' });
    assert.equal((out[0] as { content: string }).content, 'X');
  });

  it('fires onStrip with accurate diagnostics only when something was removed', () => {
    const seen: UnavailableToolReferenceDiagnostics[] = [];
    const onStrip = (d: UnavailableToolReferenceDiagnostics) => seen.push(d);

    // No-op: onStrip must NOT fire.
    stripUnavailableToolReferences([asmCall('c1', 'bash'), toolResult('c1')], ['bash'], { onStrip });
    assert.equal(seen.length, 0);

    // One ghost call + result + an emptied turn -> fire once with exact counts.
    stripUnavailableToolReferences(
      [asmCall('c1', 'ghost'), toolResult('c1'), asmCall('c2', 'ghost2'), toolResult('c2')],
      ['bash'],
      { onStrip },
    );
    assert.equal(seen.length, 1);
    assert.equal(seen[0].toolCallsDropped, 2);
    assert.equal(seen[0].toolResultsDropped, 2);
    assert.equal(seen[0].placeholdersInserted, 2);
    assert.deepEqual(seen[0].droppedToolNames.sort(), ['ghost', 'ghost2']);
  });

  it('drops an orphaned unavailable tool_result even with no matching tool_call', () => {
    // Result whose call was already compacted away but still references a ghost tool id.
    const msgs: Message[] = [
      asmCall('c1', 'ghost'),
      toolResult('c1'),
      toolResult('c1', 'dup'),
    ];
    const out = stripUnavailableToolReferences(msgs, ['bash']);
    assert.ok(!out.some(m => m.role === 'tool'));
  });
});

// ---------------------------------------------------------------------------
// Integration / e2e-redgreen — the strip is wired into streamChatCompletion so a
// stale-tool reference never reaches the provider. On revert of the wiring the ghost
// tool name leaks into the outbound request (this test goes RED).
// ---------------------------------------------------------------------------
describe('U119/w108 integration: stale-tool references never reach the provider', () => {
  const KEYS = ['AMI_DIAGNOSTICS_FILE'];
  afterEach(() => { for (const k of KEYS) delete process.env[k]; });

  const tool = (name: string): ToolDefinition => ({
    name,
    description: 'd',
    inputSchema: { type: 'object', properties: {} },
    isReadOnly: true,
  });

  async function capture(messages: Message[], tools: ToolDefinition[]): Promise<Record<string, unknown>> {
    let captured: Record<string, unknown> = {};
    const fakeStreamText = (cfg: Record<string, unknown>) => {
      captured = cfg;
      return { stream: (async function* () { /* no parts */ })() };
    };
    const config = { provider: 'anthropic', apiKey: 'sk-x', model: 'claude-opus-4-6' } as never;
    const gen = streamChatCompletion(config, messages, tools, new AbortController().signal, {
      streamTextImpl: fakeStreamText as never,
    });
    const out: StreamChunk[] = [];
    for await (const c of gen) out.push(c);
    return captured;
  }

  it('strips a disconnected-MCP tool_call + result before dispatch', async () => {
    const messages: Message[] = [
      { role: 'user', content: 'use the mcp tool' },
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'c1', type: 'function', function: { name: 'mcp__gone__fetch', arguments: '{}' } },
          { id: 'c2', type: 'function', function: { name: 'bash', arguments: '{}' } },
        ],
      },
      toolResult('c1', 'secret mcp payload'),
      toolResult('c2', 'bash ok'),
    ];
    const cfg = await capture(messages, [tool('bash')]);
    const serialized = JSON.stringify(cfg.messages);
    assert.ok(!serialized.includes('mcp__gone__fetch'), 'ghost tool name must not reach provider');
    assert.ok(!serialized.includes('secret mcp payload'), 'ghost result must not reach provider');
    assert.ok(serialized.includes('bash ok'), 'available tool result preserved');
  });

  it('leaves historical tool turns untouched when the request carries no tools', async () => {
    // A deliberately tool-less request keeps its transcript (guard: tools.length>0).
    const messages: Message[] = [
      { role: 'user', content: 'go' },
      asmCall('c1', 'bash'),
      toolResult('c1', 'kept bash output'),
    ];
    const cfg = await capture(messages, []);
    const serialized = JSON.stringify(cfg.messages);
    assert.ok(serialized.includes('kept bash output'), 'tool-less request keeps history');
  });
});
