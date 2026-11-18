/**
 * U305 — URI-scheme special-casing + path normalization for diagnostic file headers.
 *
 * Parity with Claude Code's DiagnosticsDisplay file header (components/
 * DiagnosticsDisplay.tsx L87): the header strips both `file://` and the virtual
 * `_claude_fs_right:` scheme, rebases the path relative to the cwd, and appends a
 * 3-way scheme label — `(file://)`, `(claude_fs_right)`, or a generic
 * `(${uri.split(':')[0]})` fallback for arbitrary schemes. AMI previously rendered
 * only the absolute path with no scheme label and no cwd rebasing.
 *
 * Covers the pure formatter/normalizer (unit) AND the end-to-end wiring through the
 * `diagnostics` tool (the real consumer threads `context.cwd`).
 */
import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { pathToFileURL } from 'node:url';

import {
  diagnosticSchemeLabel,
  diagnosticHeaderPath,
  formatDiagnosticsDetail,
  CLAUDE_VIRTUAL_FS_SCHEME,
  type DiagnosticFile,
} from '../src/lsp/diagnostic-tracking';
import { lspDiagnosticsTool, _lspDiagHooks, type LspDiagClient } from '../src/tools/lsp-diagnostics';
import type { ToolContext } from '../src/types';

afterEach(() => mock.restoreAll());

describe('diagnosticSchemeLabel (3-way scheme label)', () => {
  it('labels a file:// URI "file://"', () => {
    assert.equal(diagnosticSchemeLabel('file:///proj/a.ts'), 'file://');
  });

  it('labels the virtual claude_fs_right scheme "claude_fs_right"', () => {
    assert.equal(diagnosticSchemeLabel(`${CLAUDE_VIRTUAL_FS_SCHEME}/proj/a.ts`), 'claude_fs_right');
  });

  it('extracts an arbitrary scheme as the generic fallback', () => {
    assert.equal(diagnosticSchemeLabel('git:/repo/a.ts'), 'git');
    assert.equal(diagnosticSchemeLabel('untitled:Untitled-1'), 'untitled');
  });

  it('emits no label for a bare path, a single-letter drive, or an empty/undefined URI', () => {
    assert.equal(diagnosticSchemeLabel('/proj/a.ts'), '');
    assert.equal(diagnosticSchemeLabel('C:/proj/a.ts'), '');
    assert.equal(diagnosticSchemeLabel(''), '');
    assert.equal(diagnosticSchemeLabel(undefined), '');
  });
});

describe('diagnosticHeaderPath scheme stripping + cwd rebasing', () => {
  it('strips the _claude_fs_right: scheme and rebases relative to cwd', () => {
    assert.equal(
      diagnosticHeaderPath(`${CLAUDE_VIRTUAL_FS_SCHEME}/proj/src/a.ts`, '/proj'),
      'src/a.ts',
    );
  });

  it('rebases a file:// URI relative to cwd', () => {
    assert.equal(diagnosticHeaderPath('file:///proj/src/a.ts', '/proj'), 'src/a.ts');
  });

  it('keeps the absolute path when no cwd is supplied (back-compat)', () => {
    assert.equal(diagnosticHeaderPath('file:///proj/a.ts'), '/proj/a.ts');
  });

  it('still guards a missing/empty URI', () => {
    assert.equal(diagnosticHeaderPath(undefined), '<unknown location>');
    assert.equal(diagnosticHeaderPath(''), '<unknown location>');
  });
});

describe('formatDiagnosticsDetail header: scheme label + relative rebasing', () => {
  const file: DiagnosticFile = {
    uri: 'file:///proj/src/a.ts',
    diagnostics: [{
      message: 'bad', severity: 'Error',
      range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } },
    }],
  };

  it('appends the (file://) scheme label to the header', () => {
    const out = formatDiagnosticsDetail([file]);
    assert.match(out, /\/proj\/src\/a\.ts \(file:\/\/\):/);
  });

  it('renders a cwd-relative header path when cwd is given', () => {
    const out = formatDiagnosticsDetail([file], '/proj');
    assert.match(out, /^src\/a\.ts \(file:\/\/\):/);
    assert.doesNotMatch(out, /\/proj\/src/);
  });

  it('labels the claude_fs_right virtual scheme', () => {
    const vfile: DiagnosticFile = { ...file, uri: `${CLAUDE_VIRTUAL_FS_SCHEME}/proj/src/a.ts` };
    const out = formatDiagnosticsDetail([vfile], '/proj');
    assert.match(out, /src\/a\.ts \(claude_fs_right\):/);
  });

  it('omits the label and shows <unknown location> for an empty URI', () => {
    const bad: DiagnosticFile = { ...file, uri: '' };
    const out = formatDiagnosticsDetail([bad]);
    assert.match(out, /<unknown location>:/);
    assert.doesNotMatch(out, /\(\):/);
  });
});

describe('diagnostics tool threads context.cwd into the header (end-to-end)', () => {
  function fakeClient(df: DiagnosticFile): LspDiagClient {
    return {
      getDiagnostics: async () => df,
      getAllDiagnostics: () => [],
      isConnected: () => true,
      diagnostics: { hasBaseline: () => false, getNewDiagnostics: async () => [] },
    };
  }

  it('renders the header relative to context.cwd with a (file://) label', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-u305-'));
    try {
      const abs = path.join(dir, 'pkg', 'mod.ts');
      fs.mkdirSync(path.dirname(abs), { recursive: true });
      fs.writeFileSync(abs, 'const x = 1;\n');
      const df: DiagnosticFile = {
        uri: pathToFileURL(abs).href,
        diagnostics: [{
          message: 'oops', severity: 'Error',
          range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } },
        }],
      };
      mock.method(_lspDiagHooks, 'getLSPClient', () => fakeClient(df));
      const ctx: ToolContext = { cwd: dir } as ToolContext;
      const res = await lspDiagnosticsTool.execute!({ file_path: abs, verbose: true }, ctx);
      // Relative header path + scheme label, not the absolute path.
      assert.match(res.output, /pkg\/mod\.ts \(file:\/\/\):/);
      assert.ok(!res.output.includes(`${dir}/pkg/mod.ts (file://)`));
      assert.ok(res.output.includes('oops'));
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
