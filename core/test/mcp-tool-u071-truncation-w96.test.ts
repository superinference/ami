import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  createMcpTool,
  mcpContentNeedsTruncation,
  getMcpContentSizeEstimate,
  MCP_TOKEN_COUNT_THRESHOLD_FACTOR,
} from '../src/tools/mcp-tool';
import { estimateTokens } from '../src/utils/tokens';
import { getInMemoryErrors } from '../src/logger';

// ---------------------------------------------------------------------------
// U071 (w96) — token-aware MCP truncation + diagnostic accessor-before-init log
//
// Parity with Claude Code's TWO-STAGE truncation decision
// (utils/mcpValidation.ts:L151-L178 `mcpContentNeedsTruncation`):
//   1. a cheap rough-estimate gate at `budget × MCP_TOKEN_COUNT_THRESHOLD_FACTOR`
//      short-circuits the more expensive accurate count for small results;
//   2. an accurate second stage decides against the full budget, failing OPEN
//      (assume no truncation) on a count error.
// Plus a self-healing-style diagnostic: the `_mcpManager`-missing guard now
// records the called-before-init ordering bug on the in-memory error ring
// (retrievable via getInMemoryErrors) instead of failing silently.
//
// ESM-safe: in-memory mocks, fs.mkdtempSync temp dir, no source-text reads.
// ---------------------------------------------------------------------------

let tmpCwd: string;

before(() => {
  tmpCwd = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-u071-trunc-'));
});

after(() => {
  fs.rmSync(tmpCwd, { recursive: true, force: true });
  delete process.env.MAX_MCP_OUTPUT_TOKENS;
});

function makeContext(mcpManager: unknown, extra: Record<string, unknown> = {}): any {
  return {
    cwd: tmpCwd,
    abortSignal: new AbortController().signal,
    filesRead: new Set(),
    _mcpManager: mcpManager,
    ...extra,
  };
}

class MockManager extends EventEmitter {
  callTool: (name: string, input: unknown) => Promise<unknown>;
  constructor(callTool: (name: string, input: unknown) => Promise<unknown>) {
    super();
    this.callTool = callTool;
  }
}

// ---------------------------------------------------------------------------
// mcpContentNeedsTruncation + helpers (unit)
// ---------------------------------------------------------------------------

describe('mcpContentNeedsTruncation', () => {
  it('mirrors Claude Code MCP_TOKEN_COUNT_THRESHOLD_FACTOR (0.5)', () => {
    assert.equal(MCP_TOKEN_COUNT_THRESHOLD_FACTOR, 0.5);
  });

  it('getMcpContentSizeEstimate uses the shared ~4 chars/token estimate', () => {
    const text = 'x'.repeat(40);
    assert.equal(getMcpContentSizeEstimate(text), estimateTokens(text));
    assert.equal(getMcpContentSizeEstimate(text), 10);
  });

  it('cheap gate: content at/below half the budget skips the accurate count', () => {
    let accurateCalled = false;
    const accurate = () => { accurateCalled = true; return 1e9; };
    // 40 chars ≈ 10 tokens; budget 100 → gate at 50 tokens, 10 ≤ 50 → false.
    const needs = mcpContentNeedsTruncation('x'.repeat(40), 100, accurate);
    assert.equal(needs, false);
    assert.equal(accurateCalled, false, 'accurate count must not be called under the gate');
  });

  it('accurate stage runs only above the gate and decides against the full budget', () => {
    let accurateCalled = false;
    // 800 chars ≈ 200 tokens; budget 100 → gate at 50, 200 > 50 → accurate runs.
    const under = mcpContentNeedsTruncation('x'.repeat(800), 100, () => {
      accurateCalled = true;
      return 100; // exactly at budget → not over → false
    });
    assert.equal(accurateCalled, true);
    assert.equal(under, false);
    const over = mcpContentNeedsTruncation('x'.repeat(800), 100, () => 101);
    assert.equal(over, true);
  });

  it('fails OPEN (assume no truncation) when the accurate counter throws', () => {
    const needs = mcpContentNeedsTruncation('x'.repeat(800), 100, () => {
      throw new Error('token-count service unavailable');
    });
    assert.equal(needs, false);
  });

  it('without a seam, falls back to the density-aware (JSON) estimate above the gate', () => {
    // 800 chars, budget 100: generic estimate 200 > gate 50 → accurate stage;
    // default JSON-ratio count = 400 > 100 → true.
    assert.equal(mcpContentNeedsTruncation('x'.repeat(800), 100), true);
  });

  it('defaults the budget to the configured MAX_MCP_OUTPUT_TOKENS', () => {
    process.env.MAX_MCP_OUTPUT_TOKENS = '10';
    // 400 chars ≈ 100 tokens; budget 10 → gate 5, clearly over → true.
    assert.equal(mcpContentNeedsTruncation('x'.repeat(400)), true);
    // 8 chars ≈ 2 tokens ≤ gate 5 → false.
    assert.equal(mcpContentNeedsTruncation('xxxxxxxx'), false);
    delete process.env.MAX_MCP_OUTPUT_TOKENS;
  });
});

// ---------------------------------------------------------------------------
// execute() — token/char-based truncation replaces the raw byte ceiling (E2E)
// ---------------------------------------------------------------------------

describe('createMcpTool execute — token-aware truncation', () => {
  it('keeps a multibyte result whose CHAR length fits the budget even though its BYTE length exceeds it', async () => {
    // 40,000 '€' chars (3 bytes each) = 40k chars (~10k tokens, under the 25k
    // default budget's cheap gate) but 120k BYTES (over the 100k char/byte cap).
    // RED (revert to the unconditional `boundMcpResponse(raw, maxResultChars)`):
    // the byte ceiling truncates it and stamps mcpTruncated. GREEN (token-aware
    // gate): the result is within the token budget, so it is kept intact.
    const payload = '€'.repeat(40_000);
    const mgr = new MockManager(async () => payload);
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute!({}, makeContext(mgr));
    assert.equal((res.metadata as any)?.mcpTruncated, undefined, 'must not be byte-truncated');
    assert.ok(!res.output.includes('Response truncated'), 'no truncation marker');
    assert.ok(res.output.includes('€'.repeat(200)), 'full multibyte content preserved');
  });

  it('still truncates a result that genuinely exceeds the token budget (per-result hint)', async () => {
    // Non-content object with a tiny maxResultSizeChars hint → tiny token budget.
    const mgr = new MockManager(async () => ({ _meta: { maxResultSizeChars: 10 }, blob: 'y'.repeat(500) }));
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute!({}, makeContext(mgr));
    assert.equal((res.metadata as any).mcpTruncated, true);
    assert.ok((res.metadata as any).mcpOriginalBytes > 10);
  });

  it('leaves a small result untouched (cheap gate short-circuit)', async () => {
    const mgr = new MockManager(async () => 'hello string');
    const tool = createMcpTool('srv', 'tl', 'desc', {});
    const res = await tool.execute!({}, makeContext(mgr));
    assert.equal(res.output, 'hello string');
    assert.equal(res.metadata, undefined);
  });
});

// ---------------------------------------------------------------------------
// execute() — diagnostic log on accessor-before-init (gap 4)
// ---------------------------------------------------------------------------

describe('createMcpTool execute — diagnostic on missing MCP manager', () => {
  it('records the called-before-init bug on the error ring and returns the hard error', async () => {
    const before = getInMemoryErrors().length;
    const tool = createMcpTool('diag-srv', 'diag-tool', 'desc', {});
    const res = await tool.execute!({}, makeContext(undefined));
    assert.equal(res.isError, true);
    assert.match(res.output, /MCP not initialized/);
    const added = getInMemoryErrors().slice(before);
    assert.ok(
      added.some((e) => e.error.includes('diag-srv:diag-tool') && e.error.includes('before its MCP manager was initialized')),
      'a diagnostic entry naming the server:tool must be on the in-memory error ring',
    );
  });
});
