import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { generateTitle, sanitizeTitle } from '../src/title-generator';
import {
  SessionMemoryExtractor,
  validateMemoryEntry,
  sanitizeMemoryEntry,
  MAX_MEMORY_ENTRIES,
  MAX_MEMORY_ENTRY_CHARS,
  type SessionFact,
} from '../src/session-memory';
import { SubagentLimiter } from '../src/subagent-limiter';
import type { ProviderConfig, StreamChunk, Message } from '../src/types';

// ---------------------------------------------------------------------------
// Deterministic stream seams — NO network, NO real LLM. We drive the real
// title-generator / session-memory code through its documented injectable
// streamer and assert on OBSERVED return values.
// ---------------------------------------------------------------------------

const PROVIDER: ProviderConfig = {
  baseUrl: 'http://127.0.0.1:1/v1',
  apiKey: 'test-key',
  model: 'primary-model',
};

/** An async generator that emits the given text as one content_delta then done. */
function streamText(text: string) {
  return async function* (): AsyncGenerator<StreamChunk> {
    yield { type: 'content_delta', text };
    yield { type: 'done' };
  };
}

/** Emits text split into per-char deltas, to prove accumulation. */
function streamChars(text: string) {
  return async function* (): AsyncGenerator<StreamChunk> {
    for (const ch of text) yield { type: 'content_delta', text: ch };
    yield { type: 'done' };
  };
}

/** Emits an error chunk immediately. */
async function* streamError(): AsyncGenerator<StreamChunk> {
  yield { type: 'error', error: 'boom' };
}

/** Throws synchronously when iterated. */
function streamThrow() {
  return async function* (): AsyncGenerator<StreamChunk> {
    throw new Error('stream exploded');
    // eslint-disable-next-line no-unreachable
    yield { type: 'done' };
  };
}

// ===========================================================================
// sanitizeTitle — pure, fully deterministic
// ===========================================================================
describe('sanitizeTitle: flattens markdown + whitespace to a single clean line', () => {
  it('collapses newlines/tabs into single spaces and trims', () => {
    assert.equal(sanitizeTitle('  hello\n\tworld  \n'), 'hello world');
  });

  it('strips ATX headings', () => {
    assert.equal(sanitizeTitle('## My Title'), 'My Title');
    assert.equal(sanitizeTitle('###### Deep'), 'Deep');
  });

  it('strips blockquote markers', () => {
    assert.equal(sanitizeTitle('> quoted line'), 'quoted line');
  });

  it('strips unordered and ordered list markers', () => {
    assert.equal(sanitizeTitle('- item one'), 'item one');
    assert.equal(sanitizeTitle('* starred'), 'starred');
    assert.equal(sanitizeTitle('1. first'), 'first');
  });

  it('strips inline code backticks and fenced markers', () => {
    assert.equal(sanitizeTitle('`code` here'), 'code here');
    assert.equal(sanitizeTitle('```js\nlet x = 1\n```'), 'let x = 1');
  });

  it('removes bold/italic emphasis but keeps the inner text', () => {
    assert.equal(sanitizeTitle('**bold** and *italic*'), 'bold and italic');
    assert.equal(sanitizeTitle('___wow___'), 'wow');
  });

  it('is idempotent — running twice yields the same clean line', () => {
    const once = sanitizeTitle('# Heading\n\n- bullet with `code`');
    assert.equal(sanitizeTitle(once), once);
  });

  it('empty input yields empty string', () => {
    assert.equal(sanitizeTitle(''), '');
    assert.equal(sanitizeTitle('   \n\t  '), '');
  });
});

// ===========================================================================
// generateTitle — behavioral, via injected stream
// ===========================================================================
describe('generateTitle: produces a clean single-line title <=50 chars', () => {
  it('returns the model text, sanitized', async () => {
    const title = await generateTitle(
      'Refactor the auth module',
      PROVIDER,
      undefined,
      streamText('Auth Module Refactor') as never,
    );
    assert.equal(title, 'Auth Module Refactor');
  });

  it('accumulates per-char content deltas into the full title', async () => {
    const title = await generateTitle(
      'anything',
      PROVIDER,
      undefined,
      streamChars('Streamed Title') as never,
    );
    assert.equal(title, 'Streamed Title');
  });

  it('strips surrounding quotes from model output', async () => {
    const title = await generateTitle(
      'q',
      PROVIDER,
      undefined,
      streamText('"Quoted Title"') as never,
    );
    assert.equal(title, 'Quoted Title');
  });

  it('removes paired and unclosed <think> blocks', async () => {
    const paired = await generateTitle(
      'q',
      PROVIDER,
      undefined,
      streamText('<think>reasoning</think>Real Title') as never,
    );
    assert.equal(paired, 'Real Title');

    const unclosed = await generateTitle(
      'q',
      PROVIDER,
      undefined,
      streamText('Prefix Title<think>never closes') as never,
    );
    assert.equal(unclosed, 'Prefix Title');
  });

  it('flattens multi-line markdown output to a single line', async () => {
    const title = await generateTitle(
      'q',
      PROVIDER,
      undefined,
      streamText('# Big\nTitle\n- here') as never,
    );
    assert.equal(title, 'Big Title here');
    assert.ok(!title.includes('\n'));
  });

  it('truncates the title to at most 50 characters', async () => {
    const long = 'A'.repeat(120);
    const title = await generateTitle(
      'q',
      PROVIDER,
      undefined,
      streamText(long) as never,
    );
    assert.equal(title.length, 50);
    assert.equal(title, 'A'.repeat(50));
  });

  it('falls back to the sanitized user message when model output is empty', async () => {
    const title = await generateTitle(
      '# Fix the login bug',
      PROVIDER,
      undefined,
      streamText('') as never,
    );
    assert.equal(title, 'Fix the login bug');
  });

  it('fallback user message is also truncated to 50 chars', async () => {
    const userMsg = 'B'.repeat(200);
    const title = await generateTitle(
      userMsg,
      PROVIDER,
      undefined,
      streamText('   ') as never, // sanitizes to '' -> fallback
    );
    assert.equal(title.length, 50);
  });

  it('on a stream error chunk falls back to the user message', async () => {
    const title = await generateTitle(
      'Original Request',
      PROVIDER,
      undefined,
      (() => streamError)() as never,
    );
    assert.equal(title, 'Original Request');
  });

  it('a thrown stream is caught and falls back to the user message', async () => {
    const title = await generateTitle(
      'Recovered Title',
      PROVIDER,
      undefined,
      streamThrow() as never,
    );
    assert.equal(title, 'Recovered Title');
  });

  it('uses the compaction model override without affecting output shape', async () => {
    // We cannot observe the model id in the output, but the call must still
    // succeed and sanitize deterministically with an override supplied.
    const title = await generateTitle(
      'q',
      PROVIDER,
      'compaction-model',
      streamText('Override OK') as never,
    );
    assert.equal(title, 'Override OK');
  });
});

// ===========================================================================
// validateMemoryEntry / sanitizeMemoryEntry — pure
// ===========================================================================
describe('validateMemoryEntry: gates empty / oversized / suspicious entries', () => {
  it('rejects empty and whitespace-only entries', () => {
    assert.deepEqual(validateMemoryEntry(''), { valid: false, reason: 'empty' });
    assert.deepEqual(validateMemoryEntry('   '), { valid: false, reason: 'empty' });
  });

  it('rejects entries over the char cap', () => {
    const r = validateMemoryEntry('x'.repeat(MAX_MEMORY_ENTRY_CHARS + 1));
    assert.deepEqual(r, { valid: false, reason: 'too_long' });
  });

  it('accepts an entry exactly at the char cap', () => {
    const r = validateMemoryEntry('x'.repeat(MAX_MEMORY_ENTRY_CHARS));
    assert.deepEqual(r, { valid: true });
  });

  it('rejects script/eval/proto injection patterns', () => {
    for (const bad of [
      '<script>alert(1)</script>',
      'javascript:doEvil()',
      'eval(stuff)',
      'Function("x")',
      'obj.__proto__.y = 1',
      'constructor.prototype.z = 2',
    ]) {
      assert.equal(validateMemoryEntry(bad).valid, false, bad);
      assert.equal(validateMemoryEntry(bad).reason, 'suspicious_content', bad);
    }
  });

  it('accepts an ordinary fact', () => {
    assert.deepEqual(validateMemoryEntry('The build uses esbuild'), { valid: true });
  });
});

describe('sanitizeMemoryEntry: strips tags and truncates', () => {
  it('strips HTML-ish tags and trims', () => {
    assert.equal(sanitizeMemoryEntry('  <b>hi</b> there  '), 'hi there');
  });

  it('truncates overlong entries with a marker', () => {
    const out = sanitizeMemoryEntry('y'.repeat(MAX_MEMORY_ENTRY_CHARS + 50));
    assert.ok(out.length <= MAX_MEMORY_ENTRY_CHARS);
    assert.ok(out.endsWith('... [truncated]'));
  });

  it('leaves a short clean entry unchanged', () => {
    assert.equal(sanitizeMemoryEntry('plain fact'), 'plain fact');
  });
});

// ===========================================================================
// SessionMemoryExtractor.extractFacts — behavioral, via injected stream
// ===========================================================================
function extractorWith(stream: () => AsyncGenerator<StreamChunk>, compactionModel?: string) {
  const ex = new SessionMemoryExtractor(PROVIDER, compactionModel);
  ex._streamFn = stream as never;
  return ex;
}

const SAMPLE_FACT = (over: Partial<SessionFact> = {}): SessionFact => ({
  fact: 'We chose esbuild for bundling',
  category: 'decision',
  confidence: 0.9,
  ...over,
});

describe('SessionMemoryExtractor.extractFacts: parses valid facts from a transcript', () => {
  const toolResults = [{ toolName: 'bash', output: 'npm run build OK', isError: false }];
  const msgs: Message[] = [{ role: 'user', content: 'build it' }];

  it('returns facts from a plain JSON array response', async () => {
    const arr = JSON.stringify([SAMPLE_FACT()]);
    const ex = extractorWith(streamText(arr));
    const facts = await ex.extractFacts(toolResults, msgs);
    assert.equal(facts.length, 1);
    assert.equal(facts[0].fact, 'We chose esbuild for bundling');
    assert.equal(facts[0].category, 'decision');
  });

  it('extracts JSON from a fenced markdown code block', async () => {
    const body = '```json\n' + JSON.stringify([SAMPLE_FACT({ fact: 'fenced' })]) + '\n```';
    const ex = extractorWith(streamText(body));
    const facts = await ex.extractFacts(toolResults, msgs);
    assert.equal(facts.length, 1);
    assert.equal(facts[0].fact, 'fenced');
  });

  it('drops facts below the 0.7 confidence threshold', async () => {
    const arr = JSON.stringify([
      SAMPLE_FACT({ fact: 'keep', confidence: 0.7 }),
      SAMPLE_FACT({ fact: 'drop', confidence: 0.5 }),
    ]);
    const ex = extractorWith(streamText(arr));
    const facts = await ex.extractFacts(toolResults, msgs);
    assert.deepEqual(facts.map(f => f.fact), ['keep']);
  });

  it('drops facts with an unknown category', async () => {
    const arr = JSON.stringify([
      SAMPLE_FACT({ fact: 'good', category: 'convention' }),
      { fact: 'bad', category: 'nonsense', confidence: 0.9 },
    ]);
    const ex = extractorWith(streamText(arr));
    const facts = await ex.extractFacts(toolResults, msgs);
    assert.deepEqual(facts.map(f => f.fact), ['good']);
  });

  it('returns [] when the response is not a JSON array', async () => {
    const ex = extractorWith(streamText('not json at all'));
    const facts = await ex.extractFacts(toolResults, msgs);
    assert.deepEqual(facts, []);
  });

  it('returns [] when the JSON is a non-array object', async () => {
    const ex = extractorWith(streamText(JSON.stringify({ fact: 'x' })));
    const facts = await ex.extractFacts(toolResults, msgs);
    assert.deepEqual(facts, []);
  });

  it('returns [] on a stream error chunk', async () => {
    const ex = extractorWith(() => streamError);
    const facts = await ex.extractFacts(toolResults, msgs);
    assert.deepEqual(facts, []);
  });

  it('returns [] when the stream throws', async () => {
    const ex = extractorWith(streamThrow());
    const facts = await ex.extractFacts(toolResults, msgs);
    assert.deepEqual(facts, []);
  });

  it('handles error tool results and non-string message content without throwing', async () => {
    const arr = JSON.stringify([SAMPLE_FACT()]);
    const ex = extractorWith(streamText(arr));
    const facts = await ex.extractFacts(
      [{ toolName: 'bash', output: 'segfault', isError: true }],
      [{ role: 'assistant', content: [{ type: 'text', text: 'hi' }] as never }],
    );
    assert.equal(facts.length, 1);
  });

  it('honours the compaction model override and still parses', async () => {
    const ex = extractorWith(streamText(JSON.stringify([SAMPLE_FACT()])), 'small-model');
    const facts = await ex.extractFacts(toolResults, msgs);
    assert.equal(facts.length, 1);
  });

  it('only the last 4 recent messages are considered (no crash on many)', async () => {
    const many: Message[] = Array.from({ length: 10 }, (_, i) => ({
      role: 'user' as const,
      content: `m${i}`,
    }));
    const ex = extractorWith(streamText(JSON.stringify([SAMPLE_FACT()])));
    const facts = await ex.extractFacts(toolResults, many);
    assert.equal(facts.length, 1);
  });
});

describe('SessionMemoryExtractor.mergeFacts: dedup + confidence precedence + cap', () => {
  const ex = new SessionMemoryExtractor(PROVIDER);

  it('appends a new distinct fact', () => {
    const out = ex.mergeFacts([SAMPLE_FACT({ fact: 'a' })], [SAMPLE_FACT({ fact: 'b' })]);
    assert.deepEqual(out.map(f => f.fact).sort(), ['a', 'b']);
  });

  it('keeps the higher-confidence fact on a case-insensitive duplicate', () => {
    const out = ex.mergeFacts(
      [SAMPLE_FACT({ fact: 'Same Fact', confidence: 0.75 })],
      [SAMPLE_FACT({ fact: 'same fact', confidence: 0.95 })],
    );
    assert.equal(out.length, 1);
    assert.equal(out[0].confidence, 0.95);
  });

  it('keeps the existing fact when the new duplicate has lower confidence', () => {
    const out = ex.mergeFacts(
      [SAMPLE_FACT({ fact: 'dup', confidence: 0.9 })],
      [SAMPLE_FACT({ fact: 'dup', confidence: 0.75 })],
    );
    assert.equal(out.length, 1);
    assert.equal(out[0].confidence, 0.9);
  });

  it('skips a new fact that fails validation (suspicious content)', () => {
    const out = ex.mergeFacts(
      [SAMPLE_FACT({ fact: 'keep' })],
      [SAMPLE_FACT({ fact: '<script>bad</script>' })],
    );
    assert.deepEqual(out.map(f => f.fact), ['keep']);
  });

  it('sanitizes a surviving new fact (strips tags)', () => {
    const out = ex.mergeFacts([], [SAMPLE_FACT({ fact: 'clean <b>me</b> up' })]);
    assert.equal(out[0].fact, 'clean me up');
  });

  it('caps at MAX_MEMORY_ENTRIES, keeping highest confidence', () => {
    const existing: SessionFact[] = Array.from({ length: MAX_MEMORY_ENTRIES }, (_, i) =>
      SAMPLE_FACT({ fact: `old-${i}`, confidence: 0.75 }),
    );
    const out = ex.mergeFacts(existing, [SAMPLE_FACT({ fact: 'new-high', confidence: 0.99 })]);
    assert.equal(out.length, MAX_MEMORY_ENTRIES);
    assert.ok(out.some(f => f.fact === 'new-high'));
    assert.equal(out[0].confidence, 0.99);
  });
});

// ===========================================================================
// SubagentLimiter — concurrency / lifetime admission control
// ===========================================================================
describe('SubagentLimiter: admits up to the concurrency cap, then queues', () => {
  it('acquire increments active + lifetime counts', async () => {
    const lim = new SubagentLimiter(2, 0, 10);
    assert.equal(lim.activeCount, 0);
    await lim.acquire();
    assert.equal(lim.activeCount, 1);
    assert.equal(lim.lifetimeCount, 1);
  });

  it('a third acquire past the cap of 2 blocks until a release', async () => {
    const lim = new SubagentLimiter(2, 0, 10);
    await lim.acquire();
    await lim.acquire();
    assert.equal(lim.activeCount, 2);

    let resolved = false;
    const pending = lim.acquire().then(() => { resolved = true; });
    // Give the microtask queue a chance; it must still be queued.
    await Promise.resolve();
    assert.equal(resolved, false);
    assert.equal(lim.queuedCount, 1);

    lim.release();        // hands the slot to the waiter
    await pending;
    assert.equal(resolved, true);
    assert.equal(lim.queuedCount, 0);
  });

  it('release with no waiters decrements active count (not below zero)', () => {
    const lim = new SubagentLimiter(2, 0, 10);
    lim.release();
    assert.equal(lim.activeCount, 0);
  });

  it('acquire throws once the lifetime budget is exhausted', async () => {
    const lim = new SubagentLimiter(5, 0, 2);
    await lim.acquire();
    await lim.acquire();
    assert.equal(lim.isLifetimeBudgetExhausted(), true);
    await assert.rejects(() => lim.acquire(), /subagent limit reached \(2\)/);
  });

  it('a queued waiter is rejected if lifetime is exhausted when its turn comes', async () => {
    // cap 1, lifetime 1: first acquire consumes the only lifetime slot.
    const lim = new SubagentLimiter(1, 0, 1);
    await lim.acquire();                 // running=1, total=1 (== lifetime)
    // Next acquire would throw immediately (total >= lifetime), so instead
    // test the queued-waiter rejection path with lifetime 2.
    const lim2 = new SubagentLimiter(1, 0, 2);
    await lim2.acquire();                // running=1, total=1
    const waiter = lim2.acquire();       // queued (running==cap)
    await Promise.resolve();
    // Bump total to the lifetime cap behind the scenes so the waiter rejects.
    lim2.recordSpawn();                  // total=2 == lifetime
    lim2.release();                      // pops waiter -> total>=lifetime -> reject
    await assert.rejects(() => waiter, /subagent limit reached \(2\)/);
  });

  it('run() acquires around fn and releases even on throw', async () => {
    const lim = new SubagentLimiter(1, 0, 10);
    const val = await lim.run(async () => 42);
    assert.equal(val, 42);
    assert.equal(lim.activeCount, 0);

    await assert.rejects(() => lim.run(async () => { throw new Error('fn fail'); }), /fn fail/);
    // slot was released despite the throw
    assert.equal(lim.activeCount, 0);
    // run performed 2 acquisitions
    assert.equal(lim.lifetimeCount, 2);
  });

  it('canSpawnAtDepth respects maxDepth boundary', () => {
    const lim = new SubagentLimiter(5, 1, 10);
    assert.equal(lim.canSpawnAtDepth(0), true);
    assert.equal(lim.canSpawnAtDepth(1), true);
    assert.equal(lim.canSpawnAtDepth(2), false);
  });

  it('recordSpawn tracks lifetime without touching concurrency', () => {
    const lim = new SubagentLimiter(5, 0, 10);
    lim.recordSpawn();
    lim.recordSpawn();
    assert.equal(lim.lifetimeCount, 2);
    assert.equal(lim.activeCount, 0);
  });

  it('reset clears running, queued, and lifetime counts', async () => {
    const lim = new SubagentLimiter(1, 0, 10);
    await lim.acquire();
    const queued = lim.acquire(); // queues
    await Promise.resolve();
    assert.equal(lim.queuedCount, 1);
    lim.reset();
    assert.equal(lim.activeCount, 0);
    assert.equal(lim.lifetimeCount, 0);
    assert.equal(lim.queuedCount, 0);
    // detach the dangling promise so the test runner exits cleanly
    void queued.catch(() => {});
  });

  it('full lifecycle: cap of 2 with 4 tasks serializes the overflow', async () => {
    const lim = new SubagentLimiter(2, 0, 100);
    const order: number[] = [];
    const task = (id: number) =>
      lim.run(async () => {
        order.push(id);
        await Promise.resolve();
      });
    await Promise.all([task(1), task(2), task(3), task(4)]);
    assert.equal(order.length, 4);
    assert.equal(lim.activeCount, 0);
    assert.equal(lim.lifetimeCount, 4);
  });
});
