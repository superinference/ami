import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  advisorTool,
  isAdvisorDisabled,
  resolveAdvisorModel,
  buildTranscriptDigest,
  ADVISOR_MODEL_ENV,
  ADVISOR_DISABLE_ENV,
  ADVISOR_DISABLE_ENV_ALIAS,
} from '../src/tools/advisor';
import { createDefaultTools } from '../src/tools/index';
import type { ToolContext, Message, ProviderConfig, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// Test seam: a fake stream impl matching streamChatCompletion's shape that
// runAuxQuery drives — it is called as streamImpl(provider, messages, [], signal).
// ---------------------------------------------------------------------------
type FakeChunk = Partial<StreamChunk> & { type: StreamChunk['type'] };
function fakeStream(chunks: FakeChunk[], onCall?: (provider: ProviderConfig, messages: Message[]) => void) {
  return async function* (provider: ProviderConfig, messages: Message[]) {
    onCall?.(provider, messages);
    for (const c of chunks) yield c as StreamChunk;
  };
}

function ctx(overrides: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'test-model', provider: 'openai' } as ProviderConfig,
    _parentMessages: [{ role: 'user', content: 'Fix the failing test in foo.ts' }],
    ...overrides,
  } as ToolContext;
}

const ENV_KEYS = [ADVISOR_MODEL_ENV, ADVISOR_DISABLE_ENV, ADVISOR_DISABLE_ENV_ALIAS];
const saved: Record<string, string | undefined> = {};
for (const k of ENV_KEYS) saved[k] = process.env[k];
afterEach(() => {
  for (const k of ENV_KEYS) {
    if (saved[k] === undefined) delete process.env[k];
    else process.env[k] = saved[k];
  }
});

describe('advisorTool — definition', () => {
  it('is named advisor and read-only', () => {
    assert.equal(advisorTool.name, 'advisor');
    assert.equal(advisorTool.isReadOnly, true);
  });
  it('is open-world (external non-deterministic reviewer call)', () => {
    assert.equal(advisorTool.isOpenWorld, true);
  });
  it('takes no required parameters (focus is optional)', () => {
    assert.deepEqual(advisorTool.inputSchema.required, []);
    assert.ok(advisorTool.inputSchema.properties?.focus);
  });
  it('carries proactive when-to-use instructions and a search hint', () => {
    assert.match(advisorTool.prompt ?? '', /before committing to an approach/i);
    assert.match(advisorTool.searchHint ?? '', /second opinion/i);
  });
});

describe('advisorTool — isAdvisorDisabled', () => {
  it('is enabled by default', () => {
    delete process.env[ADVISOR_DISABLE_ENV];
    delete process.env[ADVISOR_DISABLE_ENV_ALIAS];
    assert.equal(isAdvisorDisabled(), false);
    assert.equal(advisorTool.isEnabled?.(), true);
  });
  it('honors the AMI off-switch', () => {
    process.env[ADVISOR_DISABLE_ENV] = '1';
    assert.equal(isAdvisorDisabled(), true);
    assert.equal(advisorTool.isEnabled?.(), false);
  });
  it('honors the Claude-compat alias off-switch', () => {
    delete process.env[ADVISOR_DISABLE_ENV];
    process.env[ADVISOR_DISABLE_ENV_ALIAS] = 'yes';
    assert.equal(isAdvisorDisabled(), true);
  });
});

describe('advisorTool — resolveAdvisorModel', () => {
  const anthropic = { model: 'claude-sonnet-4', provider: 'anthropic' } as ProviderConfig;
  const openai = { model: 'gpt-5.6', provider: 'openai' } as ProviderConfig;

  it('an explicit env alias override wins and is alias-resolved', () => {
    process.env[ADVISOR_MODEL_ENV] = 'opus';
    assert.equal(resolveAdvisorModel(openai), 'claude-opus-4');
  });
  it('an explicit non-alias override is used verbatim', () => {
    process.env[ADVISOR_MODEL_ENV] = 'my-custom-model';
    assert.equal(resolveAdvisorModel(openai), 'my-custom-model');
  });
  it('upgrades a first-party Anthropic run to the strongest same-family reviewer', () => {
    delete process.env[ADVISOR_MODEL_ENV];
    assert.equal(resolveAdvisorModel(anthropic), 'claude-opus-4');
  });
  it('detects a claude model id even without an explicit provider', () => {
    delete process.env[ADVISOR_MODEL_ENV];
    assert.equal(resolveAdvisorModel({ model: 'claude-haiku-4-5' } as ProviderConfig), 'claude-opus-4');
  });
  it('reuses the caller model for a non-Anthropic provider', () => {
    delete process.env[ADVISOR_MODEL_ENV];
    assert.equal(resolveAdvisorModel(openai), 'gpt-5.6');
  });
});

describe('advisorTool — buildTranscriptDigest', () => {
  it('returns empty string for an empty transcript', () => {
    assert.equal(buildTranscriptDigest([]), '');
  });
  it('renders user, assistant text, tool calls, tool results, and system turns', () => {
    const msgs: Message[] = [
      { role: 'system', content: 'You are a coding agent.' },
      { role: 'user', content: 'Add a cache' },
      { role: 'assistant', content: 'Let me look', tool_calls: [{ id: 't1', type: 'function', function: { name: 'grep', arguments: '{"pattern":"cache"}' } }] },
      { role: 'tool', tool_call_id: 't1', content: 'found 3 matches' },
      { role: 'tool', tool_call_id: 't2', content: 'boom', isError: true },
    ];
    const d = buildTranscriptDigest(msgs);
    assert.match(d, /## System\nYou are a coding agent\./);
    assert.match(d, /## User\nAdd a cache/);
    assert.match(d, /## Assistant\nLet me look\n→ called tool `grep`\(\{"pattern":"cache"\}\)/);
    assert.match(d, /## Tool result\nfound 3 matches/);
    assert.match(d, /## Tool result \(error\)\nboom/);
  });
  it('tail-trims an oversized tool result', () => {
    const big = 'x'.repeat(10_000);
    const d = buildTranscriptDigest([{ role: 'tool', tool_call_id: 't', content: big }]);
    assert.match(d, /earlier chars elided/);
    assert.ok(d.length < big.length);
  });
  it('renders text-block array content', () => {
    const d = buildTranscriptDigest([
      { role: 'user', content: [{ type: 'text', text: 'block one' }] } as Message,
    ]);
    assert.match(d, /## User\nblock one/);
  });
});

describe('advisorTool — execute', () => {
  it('errors when provider config is missing', async () => {
    const r = await advisorTool.execute({}, ctx({ _providerConfig: undefined }));
    assert.equal(r.isError, true);
    assert.match(r.output, /provider configuration not available/i);
  });
  it('errors when there is no conversation history', async () => {
    const r = await advisorTool.execute({}, ctx({ _parentMessages: [] }));
    assert.equal(r.isError, true);
    assert.match(r.output, /no conversation history/i);
  });
  it('errors when the tool is disabled', async () => {
    process.env[ADVISOR_DISABLE_ENV] = '1';
    const r = await advisorTool.execute({}, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /disabled/i);
  });
  it('returns the reviewer advice on success and forwards the conversation', async () => {
    let seenMessages: Message[] = [];
    const r = await advisorTool.execute(
      {},
      ctx({
        _parentMessages: [
          { role: 'user', content: 'Refactor the parser' },
          { role: 'assistant', content: 'I will rewrite it from scratch' },
        ],
        _advisorStreamImpl: fakeStream(
          [{ type: 'content_delta', text: 'Reviewer advice: ' }, { type: 'content_delta', text: 'write a test first.' }, { type: 'done' }],
          (_p, m) => { seenMessages = m; },
        ),
      }),
    );
    assert.equal(r.isError, undefined);
    assert.equal(r.output, 'Reviewer advice: write a test first.');
    assert.equal(r.metadata?.advisorModel, 'test-model');
    // The reviewer saw the task + the agent's approach, as a single forwarded turn.
    assert.equal(seenMessages.length, 1);
    assert.match(String((seenMessages[0] as { content: string }).content), /Refactor the parser/);
    assert.match(String((seenMessages[0] as { content: string }).content), /rewrite it from scratch/);
    assert.match(String((seenMessages[0] as { content: string }).content), /second opinion/i);
  });
  it('threads the focus argument into the reviewer request', async () => {
    let seen = '';
    await advisorTool.execute(
      { focus: 'is recursion safe here?' },
      ctx({
        _advisorStreamImpl: fakeStream(
          [{ type: 'content_delta', text: 'ok' }, { type: 'done' }],
          (_p, m) => { seen = String((m[0] as { content: string }).content); },
        ),
      }),
    );
    assert.match(seen, /The agent specifically asks: is recursion safe here\?/);
  });
  it('uses the resolved reviewer model override', async () => {
    process.env[ADVISOR_MODEL_ENV] = 'opus';
    let seenModel = '';
    const r = await advisorTool.execute(
      {},
      ctx({
        _advisorStreamImpl: fakeStream(
          [{ type: 'content_delta', text: 'advice' }, { type: 'done' }],
          (p) => { seenModel = p.model; },
        ),
      }),
    );
    assert.equal(seenModel, 'claude-opus-4');
    assert.equal(r.metadata?.advisorModel, 'claude-opus-4');
  });
  it('surfaces a reviewer stream failure as an error', async () => {
    const r = await advisorTool.execute(
      {},
      ctx({ _advisorStreamImpl: fakeStream([{ type: 'error', error: 'rate limited' }]) }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /advisor review failed/i);
    assert.match(r.output, /rate limited/);
  });
  it('errors when the reviewer returns no advice', async () => {
    const r = await advisorTool.execute(
      {},
      ctx({ _advisorStreamImpl: fakeStream([{ type: 'content_delta', text: '   ' }, { type: 'done' }]) }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /no advice/i);
  });
});

describe('advisorTool — registration', () => {
  it('is registered in the default tool pool', () => {
    const names = createDefaultTools('/tmp').getAll().map(t => t.name);
    assert.ok(names.includes('advisor'));
  });
  it('is dropped from the pool when disabled', () => {
    process.env[ADVISOR_DISABLE_ENV] = '1';
    const names = createDefaultTools('/tmp').getAll().map(t => t.name);
    assert.ok(!names.includes('advisor'));
  });
});
