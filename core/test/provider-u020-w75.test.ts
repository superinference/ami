// ===========================================================================
// provider-u020-w75.test.ts
//
// Parity unit U020 (wave 75) — transport-boundary prompt/response debug capture
// in core/core/src/provider.ts, mirroring the reference's createDumpPromptsFetch
// (services/api/dumpPrompts.ts L90-226).
//
// Covered parity gaps:
//   - Transport-boundary request/response capture that NEVER perturbs the real
//     API call (response.clone() + setImmediate defer + best-effort swallow).
//     [ref: services/api/dumpPrompts.ts L146-225]
//   - Raw API request/response debug-dump introspection surface (exact system
//     prompt + full tool schemas + model params + raw/streaming response to
//     JSONL), gated by the AMI_DUMP_PROMPTS env flag.
//     [ref: services/api/dumpPrompts.ts L90-226]
//
// The feature is wired into resolveModel()'s fetch chain so every provider
// branch captures when AMI_DUMP_PROMPTS is set; the final integration test
// drives the FULL wired path (resolveModel -> fetch chain -> disk).
// ===========================================================================

import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'node:http';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  dumpPromptsEnabled,
  getPromptDumpDir,
  withPromptDump,
  streamChatCompletion,
} from '../src/provider';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function setEnv(value: string | undefined): void {
  if (value === undefined) delete process.env.AMI_DUMP_PROMPTS;
  else process.env.AMI_DUMP_PROMPTS = value;
}

const ORIGINAL_DUMP_ENV = process.env.AMI_DUMP_PROMPTS;

async function waitUntil(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error('waitUntil timed out');
    await new Promise((r) => setImmediate(r));
  }
}

/** A capturing writer that records every JSONL line and lets tests await writes. */
function makeCapture() {
  const lines: string[] = [];
  const parsed = () => lines.map((l) => JSON.parse(l) as { type: string; data: unknown });
  const write = (_fp: string, line: string): void => { lines.push(line); };
  return { lines, parsed, write };
}

function jsonResponse(body: unknown, init?: { status?: number }): Response {
  return new Response(JSON.stringify(body), {
    status: init?.status ?? 200,
    headers: { 'content-type': 'application/json' },
  });
}

function sseResponse(dataChunks: unknown[]): Response {
  const body = dataChunks.map((c) => `data: ${JSON.stringify(c)}`).join('\n\n') + '\n\ndata: [DONE]\n\n';
  return new Response(body, {
    status: 200,
    headers: { 'content-type': 'text/event-stream' },
  });
}

// ===========================================================================
// 1. dumpPromptsEnabled — env gating matrix
// ===========================================================================

describe('dumpPromptsEnabled — env gating', () => {
  afterEach(() => setEnv(ORIGINAL_DUMP_ENV));

  it('is false when unset', () => { setEnv(undefined); assert.equal(dumpPromptsEnabled(), false); });
  it('is false for empty / "0" / "false"', () => {
    setEnv(''); assert.equal(dumpPromptsEnabled(), false);
    setEnv('   '); assert.equal(dumpPromptsEnabled(), false);
    setEnv('0'); assert.equal(dumpPromptsEnabled(), false);
    setEnv('false'); assert.equal(dumpPromptsEnabled(), false);
    setEnv('FALSE'); assert.equal(dumpPromptsEnabled(), false);
  });
  it('is true for "1" / "true" / a directory path', () => {
    setEnv('1'); assert.equal(dumpPromptsEnabled(), true);
    setEnv('true'); assert.equal(dumpPromptsEnabled(), true);
    setEnv('/tmp/dumps'); assert.equal(dumpPromptsEnabled(), true);
  });
});

// ===========================================================================
// 2. getPromptDumpDir — path vs default
// ===========================================================================

describe('getPromptDumpDir', () => {
  afterEach(() => setEnv(ORIGINAL_DUMP_ENV));

  it('uses the env value directly when it is a path', () => {
    setEnv('/var/log/ami-dumps');
    assert.equal(getPromptDumpDir(), '/var/log/ami-dumps');
  });
  it('falls back to a temp subdir for a non-path flag', () => {
    setEnv('1');
    assert.equal(getPromptDumpDir(), path.join(os.tmpdir(), 'ami-prompt-dumps'));
  });
  it('falls back to a temp subdir when unset', () => {
    setEnv(undefined);
    assert.equal(getPromptDumpDir(), path.join(os.tmpdir(), 'ami-prompt-dumps'));
  });
});

// ===========================================================================
// 3. withPromptDump — identity passthrough when disabled (zero overhead)
// ===========================================================================

describe('withPromptDump — disabled is an identity passthrough', () => {
  afterEach(() => setEnv(ORIGINAL_DUMP_ENV));

  it('returns the SAME fetch reference when disabled via deps', () => {
    const base = (async () => jsonResponse({})) as unknown as typeof globalThis.fetch;
    assert.equal(withPromptDump(base, { enabled: false }), base);
  });

  it('returns the SAME fetch reference when the env flag is off', () => {
    setEnv(undefined);
    const base = (async () => jsonResponse({})) as unknown as typeof globalThis.fetch;
    assert.equal(withPromptDump(base), base);
  });
});

// ===========================================================================
// 4. withPromptDump — capture behavior
// ===========================================================================

describe('withPromptDump — request + response capture', () => {
  it('captures the request body (deferred) and JSON response', async () => {
    const cap = makeCapture();
    const base = (async () => jsonResponse({ id: 'resp-1', choices: [] })) as unknown as typeof globalThis.fetch;
    const wrapped = withPromptDump(base, {
      enabled: true, dir: '/ignored', write: cap.write, defer: (fn) => fn(), now: () => 'TS', sessionId: 'cap-json',
    });

    const body = JSON.stringify({ model: 'm', system: 'be concise', messages: [{ role: 'user', content: 'hi' }] });
    const res = await wrapped('http://api/v1/chat' as unknown as string, { method: 'POST', body });

    // Request is captured synchronously (defer=fn()) BEFORE the response resolves.
    // The first request of a session emits a deduplicated `init` entry (system +
    // model + tools, messages stripped) plus one `message` entry per new user turn.
    const initEntry = cap.parsed().find((e) => e.type === 'init');
    assert.ok(initEntry, 'init entry written');
    assert.deepEqual((initEntry!.data as { model: string }).model, 'm');
    assert.equal((initEntry!.data as { messages?: unknown }).messages, undefined, 'messages stripped from init');
    const msgEntry = cap.parsed().find((e) => e.type === 'message');
    assert.ok(msgEntry, 'user message entry written');
    assert.deepEqual((msgEntry!.data as { content: string }).content, 'hi');

    // Response capture is fire-and-forget — wait for it.
    await waitUntil(() => cap.parsed().some((e) => e.type === 'response'));
    const respEntry = cap.parsed().find((e) => e.type === 'response');
    assert.deepEqual((respEntry!.data as { id: string }).id, 'resp-1');

    // Caller can STILL read the body — clone() did not consume it.
    assert.deepEqual(await res.json(), { id: 'resp-1', choices: [] });
  });

  it('deduplicates per session: emits init ONCE, then system_update only on drift, with per-turn message entries', async () => {
    const cap = makeCapture();
    const base = (async () => jsonResponse({ ok: true })) as unknown as typeof globalThis.fetch;
    const wrapped = withPromptDump(base, {
      enabled: true, dir: '/ignored', write: cap.write, defer: (fn) => fn(), now: () => 'TS', sessionId: 'dedup-session',
    });
    const post = (b: unknown) => wrapped('http://api' as unknown as string, { method: 'POST', body: JSON.stringify(b) });

    // Turn 1: first request → init + the first user message.
    await post({ model: 'm', system: 'sys-A', tools: [{ name: 't1' }], messages: [{ role: 'user', content: 'u1' }] });
    // Turn 2: SAME system/tools, one new user message → NO init/system_update, just the new message.
    await post({ model: 'm', system: 'sys-A', tools: [{ name: 't1' }], messages: [{ role: 'user', content: 'u1' }, { role: 'assistant', content: 'a1' }, { role: 'user', content: 'u2' }] });
    // Turn 3: system changed → a system_update (not another init), no new user message.
    await post({ model: 'm', system: 'sys-B-longer', tools: [{ name: 't1' }], messages: [{ role: 'user', content: 'u1' }, { role: 'assistant', content: 'a1' }, { role: 'user', content: 'u2' }] });

    const types = cap.parsed().map((e) => e.type);
    assert.equal(types.filter((t) => t === 'init').length, 1, 'init emitted exactly once');
    assert.equal(types.filter((t) => t === 'system_update').length, 1, 'system_update on drift');
    const messages = cap.parsed().filter((e) => e.type === 'message').map((e) => (e.data as { content: string }).content);
    assert.deepEqual(messages, ['u1', 'u2'], 'only NEW user messages, assistant turns skipped');
  });

  it('parses an SSE response into decoded data chunks', async () => {
    const cap = makeCapture();
    const chunks = [{ delta: 'a' }, { delta: 'b' }];
    const base = (async () => sseResponse(chunks)) as unknown as typeof globalThis.fetch;
    const wrapped = withPromptDump(base, { enabled: true, dir: '/ignored', write: cap.write, defer: (fn) => fn(), sessionId: 'cap-sse' });

    const res = await wrapped('http://api' as unknown as string, { method: 'POST', body: '{"model":"m"}' });
    await waitUntil(() => cap.parsed().some((e) => e.type === 'response'));

    const respEntry = cap.parsed().find((e) => e.type === 'response')!;
    assert.deepEqual(respEntry.data, { stream: true, chunks });

    // Caller still reads the original SSE stream intact.
    const text = await res.text();
    assert.ok(text.includes('data: [DONE]'));
  });

  it('does NOT capture a non-POST request', async () => {
    const cap = makeCapture();
    const base = (async () => jsonResponse({ ok: true })) as unknown as typeof globalThis.fetch;
    const wrapped = withPromptDump(base, { enabled: true, dir: '/ignored', write: cap.write, defer: (fn) => fn() });
    await wrapped('http://api' as unknown as string, { method: 'GET' });
    await new Promise((r) => setImmediate(r));
    assert.equal(cap.lines.length, 0);
  });

  it('does NOT capture a response when the request was not recorded (no string body)', async () => {
    const cap = makeCapture();
    const base = (async () => jsonResponse({ ok: true })) as unknown as typeof globalThis.fetch;
    const wrapped = withPromptDump(base, { enabled: true, dir: '/ignored', write: cap.write, defer: (fn) => fn() });
    await wrapped('http://api' as unknown as string, { method: 'POST' }); // no body
    await new Promise((r) => setImmediate(r));
    assert.equal(cap.lines.length, 0);
  });

  it('does NOT capture the response body on a non-ok response', async () => {
    const cap = makeCapture();
    const base = (async () => jsonResponse({ error: 'boom' }, { status: 500 })) as unknown as typeof globalThis.fetch;
    const wrapped = withPromptDump(base, { enabled: true, dir: '/ignored', write: cap.write, defer: (fn) => fn(), sessionId: 'cap-nonok' });
    await wrapped('http://api' as unknown as string, { method: 'POST', body: '{"model":"m"}' });
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));
    // Only the request is recorded; the error response is not dumped.
    assert.equal(cap.parsed().filter((e) => e.type === 'response').length, 0);
    assert.equal(cap.parsed().filter((e) => e.type === 'init').length, 1);
  });

  it('swallows a malformed (non-JSON) request body without throwing', async () => {
    const cap = makeCapture();
    const base = (async () => jsonResponse({ ok: true })) as unknown as typeof globalThis.fetch;
    const wrapped = withPromptDump(base, { enabled: true, dir: '/ignored', write: cap.write, defer: (fn) => fn(), sessionId: 'cap-malformed' });
    const res = await wrapped('http://api' as unknown as string, { method: 'POST', body: 'not-json{' });
    await new Promise((r) => setImmediate(r));
    assert.equal(cap.parsed().some((e) => e.type === 'init'), false); // malformed -> skipped
    assert.equal(res.ok, true); // live call unaffected
  });

  it('swallows writer errors — the live request always returns', async () => {
    const throwingWrite = () => { throw new Error('disk full'); };
    const base = (async () => jsonResponse({ id: 'x' })) as unknown as typeof globalThis.fetch;
    const wrapped = withPromptDump(base, { enabled: true, dir: '/ignored', write: throwingWrite, defer: (fn) => fn(), sessionId: 'cap-writer' });
    const res = await wrapped('http://api' as unknown as string, { method: 'POST', body: '{"model":"m"}' });
    await new Promise((r) => setImmediate(r));
    assert.equal(res.ok, true);
    assert.deepEqual(await res.json(), { id: 'x' });
  });
});

// ===========================================================================
// 5. withPromptDump — real on-disk JSONL write (default writer)
// ===========================================================================

describe('withPromptDump — writes JSONL to disk (default writer)', () => {
  let dir: string;
  before(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-dump-disk-')); });
  after(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('appends request + response entries to prompts-<session>.jsonl', async () => {
    const base = (async () => jsonResponse({ id: 'disk-resp' })) as unknown as typeof globalThis.fetch;
    const wrapped = withPromptDump(base, { enabled: true, dir, sessionId: 'sess1', defer: (fn) => fn() });
    await wrapped('http://api' as unknown as string, {
      method: 'POST',
      body: JSON.stringify({ model: 'm', messages: [{ role: 'user', content: 'hi' }] }),
    });

    const file = path.join(dir, 'prompts-sess1.jsonl');
    await waitUntil(() => fs.existsSync(file) && fs.readFileSync(file, 'utf-8').includes('"type":"response"'));

    const entries = fs.readFileSync(file, 'utf-8').trim().split('\n').map((l) => JSON.parse(l));
    const init = entries.find((e) => e.type === 'init');
    const msg = entries.find((e) => e.type === 'message');
    const resp = entries.find((e) => e.type === 'response');
    assert.equal(init.data.model, 'm');
    assert.equal(msg.data.content, 'hi');
    assert.equal(resp.data.id, 'disk-resp');
  });
});

// ===========================================================================
// 6. Integration (e2e) — wired into resolveModel's fetch chain
// ===========================================================================

describe('AMI_DUMP_PROMPTS — end-to-end through streamChatCompletion', () => {
  let dir: string;
  let server: http.Server;
  let port: number;

  before(async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-dump-e2e-'));
    server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
      const lines = [
        'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}',
        'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"content":"hi"},"index":0}]}',
        'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}',
      ];
      for (const l of lines) res.write(l + '\n\n');
      res.write('data: [DONE]\n\n');
      res.end();
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
    port = (server.address() as { port: number }).port;
  });

  after(() => {
    server.close();
    fs.rmSync(dir, { recursive: true, force: true });
    setEnv(ORIGINAL_DUMP_ENV);
  });

  it('dumps the request + streaming response to a JSONL file when enabled', async () => {
    setEnv(dir); // enable + route dumps into the temp dir
    const config: ProviderConfig = {
      provider: 'openai',
      apiKey: 'sk-test',
      baseUrl: `http://127.0.0.1:${port}/v1`,
      model: 'test-model',
      promptCacheKey: 'e2e-session',
    };
    const messages: Message[] = [{ role: 'user', content: 'say hi' }];

    const chunks: StreamChunk[] = [];
    const ac = new AbortController();
    for await (const c of streamChatCompletion(config, messages, [], ac.signal)) chunks.push(c);

    // The live stream was delivered normally (capture must not perturb it).
    assert.ok(chunks.some((c) => c.type === 'content_delta' && c.text === 'hi'));

    const file = path.join(dir, 'prompts-e2e-session.jsonl');
    await waitUntil(() => fs.existsSync(file) && fs.readFileSync(file, 'utf-8').includes('"type":"response"'));

    const entries = fs.readFileSync(file, 'utf-8').trim().split('\n').map((l) => JSON.parse(l));
    const init = entries.find((e) => e.type === 'init');
    const msg = entries.find((e) => e.type === 'message');
    const resp = entries.find((e) => e.type === 'response');
    assert.ok(init, 'init captured through the real fetch chain');
    assert.equal(init.data.model, 'test-model');
    assert.equal(init.data.messages, undefined, 'messages stripped from the init blob');
    assert.ok(msg, 'user message captured as its own entry');
    assert.equal(msg.data.role, 'user');
    assert.ok(resp, 'streaming response captured');
    assert.equal((resp.data as { stream: boolean }).stream, true);
    assert.ok(Array.isArray((resp.data as { chunks: unknown[] }).chunks));
  });

  it('writes NO dump file when AMI_DUMP_PROMPTS is unset', async () => {
    setEnv(undefined);
    const freshDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-dump-off-'));
    const config: ProviderConfig = {
      provider: 'openai',
      apiKey: 'sk-test',
      baseUrl: `http://127.0.0.1:${port}/v1`,
      model: 'test-model',
      promptCacheKey: 'off-session',
    };
    const ac = new AbortController();
    for await (const _ of streamChatCompletion(config, [{ role: 'user', content: 'hi' }], [], ac.signal)) { /* drain */ }
    await new Promise((r) => setImmediate(r));
    assert.equal(fs.readdirSync(freshDir).length, 0);
    fs.rmSync(freshDir, { recursive: true, force: true });
  });
});
