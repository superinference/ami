import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  SessionMemoryExtractor,
  substituteMemoryVariables,
  loadSessionMemoryPromptTemplate,
  DEFAULT_MEMORY_PROMPT_TEMPLATE,
  type SessionFact,
} from '../src/session-memory';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

const provider: ProviderConfig = { model: 'test', apiKey: 'x', baseUrl: 'http://localhost' };

function recordingStreamFn(
  chunks: StreamChunk[],
  sink: { messages?: Message[] },
): SessionMemoryExtractor['_streamFn'] {
  return (async function* (_config: unknown, messages: Message[]) {
    sink.messages = messages;
    for (const c of chunks) yield c;
  }) as unknown as SessionMemoryExtractor['_streamFn'];
}

const contentChunk = (text: string): StreamChunk => ({ type: 'content_delta', text } as unknown as StreamChunk);

// ---------------------------------------------------------------------------
// Gap: {{variable}} substitution helper (parity: Claude's substituteVariables)
// ---------------------------------------------------------------------------
describe('substituteMemoryVariables', () => {
  it('replaces every {{name}} token with the matching value', () => {
    assert.equal(
      substituteMemoryVariables('a {{x}} b {{y}}', { x: '1', y: '2' }),
      'a 1 b 2',
    );
  });

  it('leaves unknown {{tokens}} untouched', () => {
    assert.equal(substituteMemoryVariables('keep {{missing}}', { x: '1' }), 'keep {{missing}}');
  });

  it('inserts values containing $-sequences literally (no backreference corruption)', () => {
    assert.equal(substituteMemoryVariables('{{v}}', { v: '$& $1 $$' }), '$& $1 $$');
  });

  it('does not re-expand a value that itself contains a {{token}} (single pass)', () => {
    // {{a}} -> "{{b}}" must NOT then be replaced by b's value.
    assert.equal(substituteMemoryVariables('{{a}}', { a: '{{b}}', b: 'LEAK' }), '{{b}}');
  });
});

// ---------------------------------------------------------------------------
// Gap: user-overridable extraction prompt config file
// ---------------------------------------------------------------------------
describe('loadSessionMemoryPromptTemplate', () => {
  let dir: string;
  before(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mem-')); });
  after(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('returns the default template when no override file exists', async () => {
    const out = await loadSessionMemoryPromptTemplate(dir);
    assert.equal(out, DEFAULT_MEMORY_PROMPT_TEMPLATE);
  });

  it('reads the override from <configDir>/session-memory/config/prompt.md', async () => {
    const cfg = path.join(dir, 'session-memory', 'config');
    fs.mkdirSync(cfg, { recursive: true });
    fs.writeFileSync(path.join(cfg, 'prompt.md'), 'CUSTOM {{toolResults}}');
    const out = await loadSessionMemoryPromptTemplate(dir);
    assert.equal(out, 'CUSTOM {{toolResults}}');
  });

  it('falls back to the default when the override path is a directory (read error, not ENOENT)', async () => {
    const dir2 = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mem2-'));
    // Make prompt.md a directory so readFile fails with EISDIR, not ENOENT.
    fs.mkdirSync(path.join(dir2, 'session-memory', 'config', 'prompt.md'), { recursive: true });
    const out = await loadSessionMemoryPromptTemplate(dir2);
    assert.equal(out, DEFAULT_MEMORY_PROMPT_TEMPLATE);
    fs.rmSync(dir2, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// Gap: extractFacts uses the overridable template end to end
// ---------------------------------------------------------------------------
describe('extractFacts — overridable prompt template', () => {
  let dir: string;
  before(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mem-e2e-')); });
  after(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('builds the user content from a custom prompt.md with substituted variables', async () => {
    const cfg = path.join(dir, 'session-memory', 'config');
    fs.mkdirSync(cfg, { recursive: true });
    fs.writeFileSync(
      path.join(cfg, 'prompt.md'),
      'MARKER-7f3\nTOOLS={{toolResults}}\nCONV={{recentConversation}}\nMEM={{existingMemory}}',
    );

    const ex = new SessionMemoryExtractor(provider);
    ex._configDir = dir;
    const sink: { messages?: Message[] } = {};
    ex._streamFn = recordingStreamFn([contentChunk('[]')], sink);

    const existing: SessionFact[] = [{ fact: 'uses pnpm', category: 'convention', confidence: 0.9 }];
    await ex.extractFacts(
      [{ toolName: 'bash', output: 'HELLO_OUT', isError: false }],
      [{ role: 'user', content: 'ctx-msg' }],
      undefined,
      existing,
    );

    const userMsg = sink.messages?.find(m => m.role === 'user');
    const content = typeof userMsg?.content === 'string' ? userMsg!.content : '';
    assert.match(content, /MARKER-7f3/);
    assert.match(content, /TOOLS=\[bash\]: HELLO_OUT/);
    assert.match(content, /CONV=\[user\]: ctx-msg/);
    assert.match(content, /MEM=[\s\S]*\[convention\] uses pnpm/);
  });

  it('uses the default template (manifest + sections) when no override exists', async () => {
    const dir3 = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mem-def-'));
    const ex = new SessionMemoryExtractor(provider);
    ex._configDir = dir3;
    const sink: { messages?: Message[] } = {};
    ex._streamFn = recordingStreamFn([contentChunk('[]')], sink);

    await ex.extractFacts([{ toolName: 'bash', output: 'x', isError: false }], []);
    const userMsg = sink.messages?.find(m => m.role === 'user');
    const content = typeof userMsg?.content === 'string' ? userMsg!.content : '';
    assert.match(content, /## Tool Results/);
    assert.match(content, /## Recent Conversation/);
    assert.doesNotMatch(content, /\{\{/); // no unsubstituted tokens leak
    fs.rmSync(dir3, { recursive: true, force: true });
  });
});
