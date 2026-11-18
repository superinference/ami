import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { LSPClient } from '../src/lsp/client';
import { registerLspServers, resetLspServers } from '../src/lsp/config';
import { classifySubprocessError } from '../src/error-classifier';

/**
 * U073 — end-to-end coverage for the LSP client wiring the shared subprocess-error
 * classifier (`classifySubprocessError`) into a server CRASH path. A language
 * server that prints a known-recoverable stderr signature (e.g. a missing module)
 * and exits non-zero must have the classifier's remediation STEPS appended to its
 * retained `lastError` (surfaced in `/doctor`) — not just an opaque
 * "crashed with exit code N". Exercised against REAL child-process servers
 * (parity: Claude Code's verifyIt2Setup stderr → actionable remediation).
 *
 * RED/GREEN: reverting the `classifySubprocessError` append in client.ts leaves
 * `lastError` as the bare "crashed with exit code 1" with no remediation text, so
 * the remediation assertions below fail.
 */

// A server that writes a recognizable missing-dependency stderr, then exits 1.
const MISSING_DEP_CRASH_SERVER =
  "process.stderr.write(\"ModuleNotFoundError: No module named 'pylsp'\\n\");\n" +
  'setTimeout(() => process.exit(1), 5);\n';

// A server that crashes with stderr the classifier does NOT recognize.
const UNKNOWN_CRASH_SERVER =
  'process.stderr.write("kaboom: internal assertion 0xdead\\n");\n' +
  'setTimeout(() => process.exit(1), 5);\n';

let dir: string;
let missingDepScript: string;
let unknownScript: string;

function makeClient(): LSPClient {
  const client = new LSPClient();
  client._isBinaryInstalled = () => true;
  client._sleep = async () => {};
  return client;
}

before(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-u073-'));
  missingDepScript = path.join(dir, 'missing-dep-server.cjs');
  unknownScript = path.join(dir, 'unknown-crash-server.cjs');
  fs.writeFileSync(missingDepScript, MISSING_DEP_CRASH_SERVER);
  fs.writeFileSync(unknownScript, UNKNOWN_CRASH_SERVER);
});

after(() => {
  resetLspServers();
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
});

describe('LSPClient: subprocess-error remediation on crash (U073)', () => {
  it('appends classified remediation to lastError when a crash stderr is recognized', async () => {
    const client = makeClient();
    registerLspServers({
      deplang: { command: process.execPath, args: [missingDepScript], maxRestarts: 1, extensionToLanguage: { '.dep': 'deplang' } },
    });

    const ok = await client.ensureServer('deplang', dir);
    assert.equal(ok, false);
    // Let the stderr 'data' + exit handlers run.
    await new Promise((r) => setTimeout(r, 120));

    const status = client.getServerStatus('deplang');
    assert.ok(status, 'crash status must be retained');
    assert.equal(status!.state, 'error');
    const lastError = status!.lastError ?? '';
    assert.match(lastError, /crashed with exit code 1/);
    // The classifier's missing-dependency remediation steps must be present.
    const expected = classifySubprocessError("ModuleNotFoundError: No module named 'pylsp'");
    assert.equal(expected.reason, 'missing-dependency');
    for (const step of expected.remediation) {
      assert.ok(lastError.includes(step), `lastError must carry remediation step: "${step}"`);
    }
    client.shutdown();
  });

  it('leaves lastError unadorned when the crash stderr is unrecognized', async () => {
    const client = makeClient();
    registerLspServers({
      unklang: { command: process.execPath, args: [unknownScript], maxRestarts: 1, extensionToLanguage: { '.unk': 'unklang' } },
    });

    const ok = await client.ensureServer('unklang', dir);
    assert.equal(ok, false);
    await new Promise((r) => setTimeout(r, 120));

    const status = client.getServerStatus('unklang');
    assert.ok(status);
    const lastError = status!.lastError ?? '';
    assert.match(lastError, /crashed with exit code 1/);
    // No remediation dash-separated suffix for an unrecognized failure.
    assert.doesNotMatch(lastError, / — /);
    client.shutdown();
  });
});
