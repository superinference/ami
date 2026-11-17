import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  LSPDiagnosticRegistry,
  getLSPDiagnosticRegistry,
  resetLSPDiagnosticRegistry,
  MAX_DELIVERED_FILES,
} from '../src/lsp/diagnostic-registry';
import { createDiagnosticKey, type DiagnosticFile, type Diagnostic } from '../src/lsp/diagnostic-tracking';

function diag(message: string, severity: Diagnostic['severity'] = 'Error', line = 0): Diagnostic {
  return { message, severity, range: { start: { line, character: 0 }, end: { line, character: 1 } } };
}
function file(uri: string, diagnostics: Diagnostic[]): DiagnosticFile {
  return { uri, diagnostics };
}

test('createDiagnosticKey is stable for identical diagnostics and distinct otherwise', () => {
  const a = createDiagnosticKey(diag('boom'));
  const b = createDiagnosticKey(diag('boom'));
  assert.equal(a, b);
  assert.notEqual(a, createDiagnosticKey(diag('boom', 'Warning')));
  assert.notEqual(a, createDiagnosticKey(diag('other')));
  // source/code default to null in the key
  assert.ok(createDiagnosticKey(diag('boom')).includes('"source":null'));
  assert.ok(createDiagnosticKey(diag('boom')).includes('"code":null'));
});

test('registerPendingLSPDiagnostic stores pending entries with UUID keys (rapid registrations do not collide)', () => {
  const r = new LSPDiagnosticRegistry();
  assert.equal(r.getPendingLSPDiagnosticCount(), 0);
  r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file('file:///a.ts', [diag('e1')])] });
  r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file('file:///a.ts', [diag('e1')])] });
  // Two registrations, even identical, produce two distinct pending entries.
  assert.equal(r.getPendingLSPDiagnosticCount(), 2);
});

test('checkForLSPDiagnostics drains, dedupes within-batch, and marks entries sent', () => {
  const r = new LSPDiagnosticRegistry();
  r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file('file:///a.ts', [diag('e1'), diag('e1'), diag('e2')])] });
  const out = r.checkForLSPDiagnostics();
  assert.equal(out.length, 1);
  assert.equal(out[0].serverName, 'ts');
  // within-batch dedup collapses the duplicate e1
  assert.deepEqual(out[0].files[0].diagnostics.map((d) => d.message).sort(), ['e1', 'e2']);
  // drained entries are removed from pending
  assert.equal(r.getPendingLSPDiagnosticCount(), 0);
  // nothing new on a second drain
  assert.deepEqual(r.checkForLSPDiagnostics(), []);
});

test('cross-turn dedup: an already-delivered diagnostic is not re-delivered', () => {
  const r = new LSPDiagnosticRegistry();
  r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file('file:///a.ts', [diag('e1')])] });
  const first = r.checkForLSPDiagnostics();
  assert.equal(first[0].files[0].diagnostics.length, 1);
  // same diagnostic published again in a later turn
  r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file('file:///a.ts', [diag('e1'), diag('e2')])] });
  const second = r.checkForLSPDiagnostics();
  // only the genuinely-new e2 is delivered
  assert.equal(second.length, 1);
  assert.deepEqual(second[0].files[0].diagnostics.map((d) => d.message), ['e2']);
});

test('clearDeliveredDiagnosticsForFile re-arms a file (matches by path OR uri)', () => {
  const r = new LSPDiagnosticRegistry();
  r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file('file:///tmp/a.ts', [diag('e1')])] });
  r.checkForLSPDiagnostics();
  // clear by filesystem path (as the edit hook passes) despite storage by uri
  r.clearDeliveredDiagnosticsForFile('/tmp/a.ts');
  assert.equal(r.deliveredFileCount, 0);
  r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file('file:///tmp/a.ts', [diag('e1')])] });
  const out = r.checkForLSPDiagnostics();
  assert.equal(out[0].files[0].diagnostics.length, 1, 'cleared file re-delivers its diagnostic');
});

test('volume limiting caps per file (10) and keeps errors over hints when truncating', () => {
  const r = new LSPDiagnosticRegistry();
  const many: Diagnostic[] = [];
  for (let i = 0; i < 8; i++) many.push(diag(`hint${i}`, 'Hint', i));
  for (let i = 0; i < 8; i++) many.push(diag(`err${i}`, 'Error', 100 + i));
  r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file('file:///big.ts', many)] });
  const out = r.checkForLSPDiagnostics();
  const delivered = out[0].files[0].diagnostics;
  assert.equal(delivered.length, 10, 'per-file cap is 10');
  // all 8 errors survive, only hints were dropped
  assert.equal(delivered.filter((d) => d.severity === 'Error').length, 8);
  assert.equal(delivered.filter((d) => d.severity === 'Hint').length, 2);
});

test('total cap of 30 across files', () => {
  const r = new LSPDiagnosticRegistry();
  for (let f = 0; f < 5; f++) {
    const ds: Diagnostic[] = [];
    for (let i = 0; i < 10; i++) ds.push(diag(`f${f}-e${i}`, 'Error', i));
    r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file(`file:///f${f}.ts`, ds)] });
  }
  const out = r.checkForLSPDiagnostics();
  const total = out[0].files.reduce((s, x) => s + x.diagnostics.length, 0);
  assert.equal(total, 30, 'global cap is 30');
});

test('fail-open: a diagnostic whose key computation throws is still delivered', () => {
  const r = new LSPDiagnosticRegistry();
  // A diagnostic with a circular range makes JSON.stringify (createDiagnosticKey) throw.
  const bad: Diagnostic = diag('circular');
  (bad.range as unknown as { self?: unknown }).self = bad.range;
  r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file('file:///c.ts', [bad, diag('ok')])] });
  const out = r.checkForLSPDiagnostics();
  const msgs = out[0].files[0].diagnostics.map((d) => d.message).sort();
  assert.deepEqual(msgs, ['circular', 'ok'], 'the un-keyable diagnostic is kept, not dropped');
});

test('clearAllLSPDiagnostics drops pending but keeps delivered; reset drops both', () => {
  const r = new LSPDiagnosticRegistry();
  r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file('file:///a.ts', [diag('e1')])] });
  r.checkForLSPDiagnostics();
  r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file('file:///b.ts', [diag('e2')])] });
  r.clearAllLSPDiagnostics();
  assert.equal(r.getPendingLSPDiagnosticCount(), 0);
  assert.equal(r.deliveredFileCount, 1, 'delivered cache survives clearAll');
  r.resetAllLSPDiagnosticState();
  assert.equal(r.deliveredFileCount, 0);
});

test('LRU bound evicts least-recently-used delivered files', () => {
  const r = new LSPDiagnosticRegistry(2);
  for (const name of ['a', 'b', 'c']) {
    r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file(`file:///${name}.ts`, [diag('e')])] });
    r.checkForLSPDiagnostics();
  }
  assert.equal(r.deliveredFileCount, 2, 'bounded to maxFiles');
  // a.ts was evicted → its diagnostic is deliverable again
  r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file('file:///a.ts', [diag('e')])] });
  const out = r.checkForLSPDiagnostics();
  assert.equal(out[0].files[0].diagnostics.length, 1);
});

test('non-finite maxFiles clamps to the default bound', () => {
  const r = new LSPDiagnosticRegistry(Number.NaN);
  // Register more than default would be impractical; just assert it does not throw and bounds.
  r.registerPendingLSPDiagnostic({ serverName: 'ts', files: [file('file:///a.ts', [diag('e')])] });
  r.checkForLSPDiagnostics();
  assert.equal(r.deliveredFileCount, 1);
  assert.equal(MAX_DELIVERED_FILES, 500);
});

test('singleton accessor is stable and resettable', () => {
  resetLSPDiagnosticRegistry();
  const a = getLSPDiagnosticRegistry();
  assert.equal(a, getLSPDiagnosticRegistry());
  resetLSPDiagnosticRegistry();
  assert.notEqual(a, getLSPDiagnosticRegistry());
  resetLSPDiagnosticRegistry();
});
