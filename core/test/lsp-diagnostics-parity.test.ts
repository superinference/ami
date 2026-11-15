/**
 * Parity hardening for the `diagnostics` LSP tool (additive superset of Claude
 * Code's Problems surfacing). Each test discriminates the new behavior from the
 * old: reverting the corresponding change in src/tools/lsp-diagnostics.ts makes
 * the test fail (RED), re-applying it makes it pass (GREEN).
 */
import { describe, it, afterEach } from 'node:test';
import { mock } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  lspDiagnosticsTool,
  _lspDiagHooks,
  DEFAULT_LSP_DIAGNOSTICS_TIMEOUT_MS,
  normalizeSeverityFilter,
  filterDiagnosticsBySeverity,
  summarizeDiagnosticSeverities,
  raceDiagnosticsTimeout,
  LSP_SEVERITY_ALIASES,
  type LspDiagClient,
} from '../src/tools/lsp-diagnostics';
import type { DiagnosticFile, DiagnosticSeverityName } from '../src/lsp/diagnostic-tracking';
import type { ToolContext } from '../src/types';

const ctx: ToolContext = { cwd: '/tmp' } as ToolContext;

function diag(message: string, severity: DiagnosticSeverityName, line = 0) {
  return { message, severity, range: { start: { line, character: 0 }, end: { line, character: 1 } } };
}

function file(uri: string, diags: ReturnType<typeof diag>[]): DiagnosticFile {
  return { uri, diagnostics: diags };
}

function fakeClient(overrides: Partial<LspDiagClient> & { baseline?: boolean } = {}): LspDiagClient {
  return {
    getDiagnostics: overrides.getDiagnostics ?? (async () => null),
    getAllDiagnostics: overrides.getAllDiagnostics ?? (() => []),
    isConnected: overrides.isConnected ?? (() => false),
    diagnostics: overrides.diagnostics ?? {
      hasBaseline: () => overrides.baseline ?? false,
      getNewDiagnostics: async () => [],
    },
  };
}

afterEach(() => {
  mock.restoreAll();
  _lspDiagHooks.diagnosticsTimeoutMs = DEFAULT_LSP_DIAGNOSTICS_TIMEOUT_MS;
});

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------
describe('severity-filter helpers', () => {
  it('normalizes aliases to canonical severities and rejects unknown', () => {
    assert.equal(normalizeSeverityFilter('ERROR'), 'Error');
    assert.equal(normalizeSeverityFilter(' warn '), 'Warning');
    assert.equal(normalizeSeverityFilter('information'), 'Info');
    assert.equal(normalizeSeverityFilter('Hints'), 'Hint');
    assert.equal(normalizeSeverityFilter('bogus'), undefined);
    // alias table is exported and consistent
    assert.equal(LSP_SEVERITY_ALIASES.warnings, 'Warning');
  });

  it('filterDiagnosticsBySeverity keeps the floor and everything more severe, dropping empties', () => {
    const files = [
      file('file:///a.ts', [diag('e', 'Error'), diag('w', 'Warning'), diag('h', 'Hint')]),
      file('file:///b.ts', [diag('w2', 'Warning')]),
    ];
    const errorsOnly = filterDiagnosticsBySeverity(files, 'Error');
    assert.equal(errorsOnly.length, 1); // b.ts dropped (no errors)
    assert.deepEqual(errorsOnly[0].diagnostics.map((d) => d.message), ['e']);

    const warnAndUp = filterDiagnosticsBySeverity(files, 'Warning');
    assert.equal(warnAndUp.length, 2);
    assert.deepEqual(warnAndUp[0].diagnostics.map((d) => d.message), ['e', 'w']);
    // input is not mutated
    assert.equal(files[0].diagnostics.length, 3);
  });

  it('summarizeDiagnosticSeverities reports counts in severity order, omitting zeros', () => {
    const files = [
      file('file:///a.ts', [diag('e1', 'Error'), diag('e2', 'Error'), diag('w', 'Warning')]),
      file('file:///b.ts', [diag('h', 'Hint')]),
    ];
    assert.equal(summarizeDiagnosticSeverities(files), '2 errors, 1 warning, 1 hint');
    assert.equal(summarizeDiagnosticSeverities([file('file:///c.ts', [diag('e', 'Error')])]), '1 error');
    assert.equal(summarizeDiagnosticSeverities([]), '');
  });

  it('raceDiagnosticsTimeout resolves the value when it settles in time', async () => {
    const res = await raceDiagnosticsTimeout(Promise.resolve('ok'), 1000);
    assert.equal(res, 'ok');
  });
});

// ---------------------------------------------------------------------------
// FIX 1: severity filter — workspace path
// ---------------------------------------------------------------------------
describe('FIX 1: workspace severity filter', () => {
  it('excludes below-floor diagnostics when severity=error', async () => {
    const fake = fakeClient({
      getAllDiagnostics: () => [file('file:///tmp/a.ts', [diag('REAL_ERROR', 'Error'), diag('SOFT_WARNING', 'Warning')])],
      isConnected: () => true,
    });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({ severity: 'error' }, ctx);
    assert.ok(res.output.includes('REAL_ERROR'));
    // Discriminator: without the filter the warning would appear.
    assert.ok(!res.output.includes('SOFT_WARNING'));
  });
});

// ---------------------------------------------------------------------------
// FIX 2: invalid severity validation
// ---------------------------------------------------------------------------
describe('FIX 2: invalid severity is rejected with the valid set', () => {
  it('returns an Invalid severity message listing options', async () => {
    const fake = fakeClient({ getAllDiagnostics: () => [file('file:///tmp/a.ts', [diag('e', 'Error')])], isConnected: () => true });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({ severity: 'kinda-bad' }, ctx);
    // Discriminator: without validation the normal diagnostics would be returned.
    assert.ok(res.output.includes('Invalid severity "kinda-bad"'));
    assert.ok(res.output.includes('error, warning, info, hint'));
  });
});

// ---------------------------------------------------------------------------
// FIX 3: severity breakdown in the workspace summary
// ---------------------------------------------------------------------------
describe('FIX 3: workspace summary includes a severity breakdown', () => {
  it('appends per-severity counts to the flat total', async () => {
    const fake = fakeClient({
      getAllDiagnostics: () => [
        file('file:///tmp/a.ts', [diag('e1', 'Error'), diag('e2', 'Error')]),
        file('file:///tmp/b.ts', [diag('w1', 'Warning')]),
      ],
      isConnected: () => true,
    });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    // Old flat header is preserved (superset) ...
    assert.ok(res.output.includes('3 diagnostic issue(s) across 2 file(s)'));
    // ... with the new breakdown as the discriminator.
    assert.ok(res.output.includes('(2 errors, 1 warning)'));
  });
});

// ---------------------------------------------------------------------------
// FIX 4: timeout guard on a slow/wedged server
// ---------------------------------------------------------------------------
describe('FIX 4: async fetch is bounded by a timeout', () => {
  it('reports a timeout instead of waiting out a slow getDiagnostics', async () => {
    const slow = new Promise<DiagnosticFile>((resolve) =>
      setTimeout(() => resolve(file('file:///tmp/slow.ts', [diag('LATE_DIAGNOSTIC', 'Error')])), 200),
    );
    const fake = fakeClient({ getDiagnostics: () => slow });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    _lspDiagHooks.diagnosticsTimeoutMs = 10;
    const res = await lspDiagnosticsTool.execute!({ file_path: '/tmp/slow.ts' }, ctx);
    // Discriminator: without the bound the tool would await the real result.
    assert.ok(res.output.includes('timed out'));
    assert.ok(!res.output.includes('LATE_DIAGNOSTIC'));
    // Let the lingering slow promise settle so it does not leak into other tests.
    await slow;
  });

  it('reports a timeout on a slow baseline-delta fetch too', async () => {
    const slow = new Promise<DiagnosticFile[]>((resolve) =>
      setTimeout(() => resolve([file('file:///tmp/slow.ts', [diag('LATE', 'Error')])]), 200),
    );
    const fake = fakeClient({ diagnostics: { hasBaseline: () => true, getNewDiagnostics: () => slow } });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    _lspDiagHooks.diagnosticsTimeoutMs = 10;
    const res = await lspDiagnosticsTool.execute!({ file_path: '/tmp/slow.ts' }, ctx);
    assert.ok(res.output.includes('timed out'));
    await slow;
  });
});

// ---------------------------------------------------------------------------
// FIX 5: single-file severity filter — "all filtered out" empty state
// ---------------------------------------------------------------------------
describe('FIX 5: single-file severity floor empty state', () => {
  it('reports no errors (with the lower-severity count) when the floor removes everything', async () => {
    const fake = fakeClient({
      getDiagnostics: async () => file('file:///tmp/foo.ts', [diag('ONLY_A_WARNING', 'Warning')]),
    });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({ file_path: '/tmp/foo.ts', severity: 'error' }, ctx);
    // Discriminator: without the filter the warning diagnostic would be shown.
    assert.ok(!res.output.includes('ONLY_A_WARNING'));
    assert.ok(res.output.includes('No Error-level (or higher) diagnostics for'));
    assert.ok(res.output.includes('1 lower-severity diagnostic(s) present'));
  });

  it('still applies the severity floor on the baseline-delta path', async () => {
    const fake = fakeClient({
      diagnostics: {
        hasBaseline: () => true,
        getNewDiagnostics: async () => [file('file:///tmp/foo.ts', [diag('NEW_ERROR', 'Error'), diag('NEW_WARNING', 'Warning')])],
      },
    });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({ file_path: '/tmp/foo.ts', severity: 'error' }, ctx);
    assert.ok(res.output.includes('NEW_ERROR'));
    assert.ok(!res.output.includes('NEW_WARNING'));
  });

  it('reports a filtered empty baseline delta', async () => {
    const fake = fakeClient({
      diagnostics: {
        hasBaseline: () => true,
        getNewDiagnostics: async () => [file('file:///tmp/foo.ts', [diag('w', 'Warning')])],
      },
    });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({ file_path: '/tmp/foo.ts', severity: 'error' }, ctx);
    assert.ok(res.output.includes('No new Error-level (or higher) diagnostics introduced'));
  });
});
