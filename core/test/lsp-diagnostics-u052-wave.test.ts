import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  lspDiagnosticsTool,
  lspReferencesTool,
  _lspDiagHooks,
  type LspDiagClient,
} from '../src/tools/lsp-diagnostics';
import {
  truncateWithMarker,
  truncateDiagnosticMessage,
  convertPublishDiagnostics,
  MAX_DIAGNOSTIC_MESSAGE_CHARS,
  MAX_DIAGNOSTICS_SUMMARY_CHARS,
} from '../src/lsp/diagnostic-tracking';
import { getInMemoryErrors, __resetLoggerForTest } from '../src/logger';
import type { ToolContext } from '../src/types';

const MARKER = '…[truncated]';

function ctx(overrides: Partial<ToolContext> = {}): ToolContext {
  return { cwd: '/tmp', ...overrides } as ToolContext;
}

afterEach(() => {
  __resetLoggerForTest();
  delete process.env.AMI_LSP;
});

describe('U052 wave — truncateWithMarker (shared truncation helper)', () => {
  it('returns the input unchanged when within budget', () => {
    assert.equal(truncateWithMarker('short', 100), 'short');
    assert.equal(truncateWithMarker('exact', 5), 'exact');
  });

  it('truncates and appends the marker when over budget', () => {
    const text = 'a'.repeat(40);
    const out = truncateWithMarker(text, 20);
    assert.equal(out, 'a'.repeat(20 - MARKER.length) + MARKER);
    assert.equal(out.length, 20);
  });

  it('collapses to just the marker when the budget is at/below the marker length', () => {
    const text = 'a'.repeat(40);
    assert.equal(truncateWithMarker(text, MARKER.length), MARKER);
    assert.equal(truncateWithMarker(text, 2), MARKER);
  });
});

describe('U052 wave — truncateDiagnosticMessage (defensive + truncation)', () => {
  it('passes a normal string through untouched', () => {
    assert.equal(truncateDiagnosticMessage('type error'), 'type error');
  });

  it('truncates an over-long untrusted message to the cap', () => {
    const huge = 'x'.repeat(MAX_DIAGNOSTIC_MESSAGE_CHARS + 500);
    const out = truncateDiagnosticMessage(huge);
    assert.ok(out.length <= MAX_DIAGNOSTIC_MESSAGE_CHARS);
    assert.ok(out.endsWith(MARKER));
  });

  it('coerces a non-string message defensively', () => {
    assert.equal(truncateDiagnosticMessage(42 as unknown), '42');
    assert.equal(truncateDiagnosticMessage(null as unknown), '');
    assert.equal(truncateDiagnosticMessage(undefined as unknown), '');
  });

  it('never throws on an object whose String() coercion throws', () => {
    const evil = { toString() { throw new Error('boom'); } };
    let out = '';
    assert.doesNotThrow(() => { out = truncateDiagnosticMessage(evil as unknown); });
    assert.equal(out, '<unrenderable diagnostic message>');
  });
});

describe('U052 wave — convertPublishDiagnostics truncates untrusted message text', () => {
  it('applies the per-message cap to server-provided content', () => {
    const huge = 'e'.repeat(MAX_DIAGNOSTIC_MESSAGE_CHARS + 50);
    const out = convertPublishDiagnostics({
      uri: 'file:///x.ts',
      diagnostics: [{ message: huge, severity: 1, range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } }],
    });
    assert.equal(out.length, 1);
    assert.ok(out[0].message.length <= MAX_DIAGNOSTIC_MESSAGE_CHARS);
    assert.ok(out[0].message.endsWith(MARKER));
  });
});

describe('U052 wave — find_references abort boundary', () => {
  it('short-circuits to the grep fallback when the signal is already aborted', async () => {
    const aborted = AbortSignal.abort();
    let called = false;
    const restore = _lspDiagHooks.searchSymbols;
    _lspDiagHooks.searchSymbols = () => { called = true; return []; };
    try {
      const res = await lspReferencesTool.execute({ symbol: 'Foo' }, ctx({ abortSignal: aborted }));
      assert.match(res.output, /aborted/i);
      assert.match(res.output, /grep/i);
      assert.equal(called, false, 'searchSymbols must not run after abort');
    } finally {
      _lspDiagHooks.searchSymbols = restore;
    }
  });

  it('runs normally when the signal is not aborted', async () => {
    const restore = _lspDiagHooks.searchSymbols;
    _lspDiagHooks.searchSymbols = () => [];
    try {
      const res = await lspReferencesTool.execute({ symbol: 'Foo' }, ctx({ abortSignal: new AbortController().signal }));
      assert.match(res.output, /No references found/);
    } finally {
      _lspDiagHooks.searchSymbols = restore;
    }
  });
});

describe('U052 wave — find_references output-size cap', () => {
  it('truncates a very large rendered result to the char budget', async () => {
    const restore = _lspDiagHooks.searchSymbols;
    // Many matches with long names → rendered output well over the budget.
    _lspDiagHooks.searchSymbols = () =>
      Array.from({ length: 30 }, (_, i) => ({
        name: 'N'.repeat(400),
        type: 'function',
        filePath: `/tmp/file-${i}-${'p'.repeat(200)}.ts`,
        line: i,
      }));
    try {
      const res = await lspReferencesTool.execute({ symbol: 'Foo' }, ctx());
      assert.ok(res.output.length <= MAX_DIAGNOSTICS_SUMMARY_CHARS);
      assert.ok(res.output.endsWith(MARKER));
    } finally {
      _lspDiagHooks.searchSymbols = restore;
    }
  });
});

describe('U052 wave — error observability on failure paths', () => {
  it('find_references logs a real indexer failure (distinguishable from empty results)', async () => {
    const restore = _lspDiagHooks.searchSymbols;
    _lspDiagHooks.searchSymbols = () => { throw new Error('indexer exploded'); };
    try {
      const res = await lspReferencesTool.execute({ symbol: 'Foo' }, ctx());
      assert.equal(res.isError, true);
      assert.match(res.output, /grep as fallback/);
      const errs = getInMemoryErrors().map((e) => e.error);
      assert.ok(errs.some((e) => e.includes('find_references') && e.includes('indexer exploded')), `expected logged error, got ${JSON.stringify(errs)}`);
    } finally {
      _lspDiagHooks.searchSymbols = restore;
    }
  });

  it('diagnostics logs a fault DURING LSP work', async () => {
    const restore = _lspDiagHooks.getLSPClient;
    const client: LspDiagClient = {
      getDiagnostics: async () => { throw new Error('client wedged'); },
      getAllDiagnostics: () => { throw new Error('client wedged'); },
      isConnected: () => true,
      diagnostics: { hasBaseline: () => false, getNewDiagnostics: async () => [] },
    };
    _lspDiagHooks.getLSPClient = () => client;
    try {
      const res = await lspDiagnosticsTool.execute({}, ctx());
      assert.match(res.output, /LSP not available/);
      const errs = getInMemoryErrors().map((e) => e.error);
      assert.ok(errs.some((e) => e.includes('lsp-diagnostics') && e.includes('client wedged')), `expected logged error, got ${JSON.stringify(errs)}`);
    } finally {
      _lspDiagHooks.getLSPClient = restore;
    }
  });

  it('does NOT log the benign "no client" case (LSP simply unavailable)', async () => {
    const restore = _lspDiagHooks.getLSPClient;
    _lspDiagHooks.getLSPClient = () => { throw new Error('no client'); };
    try {
      const res = await lspDiagnosticsTool.execute({}, ctx());
      assert.match(res.output, /LSP not available/);
      assert.equal(getInMemoryErrors().length, 0, 'the benign no-client path must not log an error');
    } finally {
      _lspDiagHooks.getLSPClient = restore;
    }
  });
});

describe('U052 wave — enriched tool descriptions (conventions + caveats)', () => {
  it('diagnostics description names the lazy-index / run-the-linter caveats', () => {
    const d = lspDiagnosticsTool.description;
    assert.match(d, /not yet indexed/i);
    assert.match(d, /linter\/compiler/i);
  });

  it('find_references description names the workspace-indexer / grep caveats', () => {
    const d = lspReferencesTool.description;
    assert.match(d, /indexer/i);
    assert.match(d, /grep/i);
  });
});
