// Unit U039 (wave w69) — parity gaps for core/core/src/tools/file-edit.ts:
//  - structuredPatch output: file_edit success results carry a machine-readable
//    DiffHunk[] alongside the text diff (parity: the reference FileEdit `structuredPatch`)
//  - buildStructuredPatch shares the EXACT hunk grouping of buildUnifiedDiff
//  - searchHint discovery metadata on fileEditTool (parity: the reference Tool.searchHint),
//    wired into tool_search ranking/gating
//  - prompt steering: prefer editing existing files; never proactively create
//    *.md / README docs; avoid writing emojis unless asked
//  - secret-warning message consolidation: one canonical message that names the
//    detected labels AND states the shared-with-collaborators exposure risk
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import { buildStructuredPatch, buildUnifiedDiff, secretWarning, scanForSecrets } from '../src/tools/tool-utils';
import { toolSearchTool, setSearchableTools } from '../src/tools/tool-search';
import type { ToolContext, ToolDefinition, DiffHunk } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    filesRead: new Set<string>(),
    detachedMode: true,
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u039w69-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// buildStructuredPatch helper — machine-readable DiffHunk[] parity output
// ---------------------------------------------------------------------------

describe('buildStructuredPatch helper', () => {
  it('returns [] for identical content (no change)', () => {
    assert.deepEqual(buildStructuredPatch('a\nb\nc', 'a\nb\nc'), []);
  });

  it('represents a modified line as a removal + an addition in one hunk', () => {
    const hunks = buildStructuredPatch('a\nOLD\nc', 'a\nNEW\nc');
    assert.equal(hunks.length, 1);
    const h = hunks[0];
    assert.ok(h.lines.includes('-OLD'));
    assert.ok(h.lines.includes('+NEW'));
    assert.ok(h.lines.includes(' a'));
    assert.ok(h.lines.includes(' c'));
    // Counts reflect the context + the single changed line on each side.
    assert.equal(h.oldLines, h.lines.filter(l => l.startsWith('-') || l.startsWith(' ')).length);
    assert.equal(h.newLines, h.lines.filter(l => l.startsWith('+') || l.startsWith(' ')).length);
  });

  it('emits SEPARATE hunks for two far-apart change regions', () => {
    const before = ['X', ...Array.from({ length: 20 }, (_, i) => `l${i}`), 'Y'].join('\n');
    const after = ['X2', ...Array.from({ length: 20 }, (_, i) => `l${i}`), 'Y2'].join('\n');
    const hunks = buildStructuredPatch(before, after);
    assert.equal(hunks.length, 2, 'two distant regions must not collapse into one hunk');
  });

  it('is consistent with buildUnifiedDiff hunk headers (shared grouping)', () => {
    const before = 'a\nb\nOLD\nd\ne';
    const after = 'a\nb\nNEW\nd\ne';
    const hunks = buildStructuredPatch(before, after);
    const text = buildUnifiedDiff(before, after, 'f.txt');
    for (const h of hunks) {
      assert.ok(
        text.includes(`@@ -${h.oldStart},${h.oldLines} +${h.newStart},${h.newLines} @@`),
        'every structured hunk header must appear verbatim in the text diff',
      );
    }
  });

  it('returns [] for a degenerate (too-large) input instead of an approximation', () => {
    // Force the MAX_DIFF_CELLS guard: a huge line count on both sides.
    const big = Array.from({ length: 3000 }, (_, i) => `a${i}`).join('\n');
    const big2 = Array.from({ length: 3000 }, (_, i) => `b${i}`).join('\n');
    assert.deepEqual(buildStructuredPatch(big, big2), []);
  });
});

// ---------------------------------------------------------------------------
// file_edit emits structuredPatch on every success path
// ---------------------------------------------------------------------------

function hunksModify(hunks: DiffHunk[] | undefined, minus: string, plus: string): boolean {
  if (!hunks) return false;
  return hunks.some(h => h.lines.includes(minus)) && hunks.some(h => h.lines.includes(plus));
}

describe('fileEditTool – structuredPatch on success results', () => {
  it('single exact replace carries a structuredPatch matching the edit', async () => {
    const file = path.join(tmpDir, 'a.txt');
    fs.writeFileSync(file, 'line1\nOLD\nline3\n');
    const c = ctx({ filesRead: new Set([file]) });
    const res = await fileEditTool.execute({ file_path: file, old_string: 'OLD', new_string: 'NEW' }, c);
    assert.equal(res.isError, undefined);
    assert.ok(Array.isArray(res.structuredPatch) && res.structuredPatch.length >= 1);
    assert.ok(hunksModify(res.structuredPatch, '-OLD', '+NEW'));
  });

  it('replace_all carries a structuredPatch', async () => {
    const file = path.join(tmpDir, 'b.txt');
    fs.writeFileSync(file, 'x\nFOO\ny\nFOO\nz\n');
    const c = ctx({ filesRead: new Set([file]) });
    const res = await fileEditTool.execute({ file_path: file, old_string: 'FOO', new_string: 'BAR', replace_all: true }, c);
    assert.equal(res.isError, undefined);
    assert.ok(hunksModify(res.structuredPatch, '-FOO', '+BAR'));
  });

  it('multi-edit carries a structuredPatch', async () => {
    const file = path.join(tmpDir, 'c.txt');
    fs.writeFileSync(file, 'one\ntwo\nthree\n');
    const c = ctx({ filesRead: new Set([file]) });
    const res = await fileEditTool.execute({
      file_path: file,
      edits: [{ old_string: 'one', new_string: 'ONE' }, { old_string: 'three', new_string: 'THREE' }],
    }, c);
    assert.equal(res.isError, false);
    assert.ok(hunksModify(res.structuredPatch, '-one', '+ONE'));
  });

  it('new-file create carries a structuredPatch of all-added lines', async () => {
    const file = path.join(tmpDir, 'new.txt');
    const res = await fileEditTool.execute({ file_path: file, new_string: 'hello\nworld\n' }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(Array.isArray(res.structuredPatch) && res.structuredPatch.length >= 1);
    assert.ok(res.structuredPatch!.some(h => h.lines.includes('+hello') && h.lines.includes('+world')));
  });

  it('full overwrite of an existing file carries a structuredPatch', async () => {
    const file = path.join(tmpDir, 'ov.txt');
    fs.writeFileSync(file, 'a\nb\nc\n');
    const c = ctx({ filesRead: new Set([file]) });
    const res = await fileEditTool.execute({ file_path: file, new_string: 'a\nB\nc\n' }, c);
    assert.equal(res.isError, undefined);
    assert.ok(hunksModify(res.structuredPatch, '-b', '+B'));
  });
});

// ---------------------------------------------------------------------------
// searchHint discovery metadata — wired into tool_search
// ---------------------------------------------------------------------------

describe('fileEditTool – searchHint discovery', () => {
  it('declares a searchHint carrying intent synonyms', () => {
    assert.equal(typeof fileEditTool.searchHint, 'string');
    assert.ok(/in place/.test(fileEditTool.searchHint!));
    assert.ok(/patch/.test(fileEditTool.searchHint!));
  });

  it('is discoverable via tool_search by a synonym present ONLY in the searchHint', async () => {
    const decoy: ToolDefinition = {
      name: 'decoy_tool',
      description: 'Does something unrelated to files.',
      inputSchema: { type: 'object', properties: {}, required: [] },
      isReadOnly: true,
      execute: async () => ({ output: '' }),
    };
    setSearchableTools([fileEditTool, decoy]);
    // "patch" is a required term only matchable through file_edit's searchHint
    // (it is absent from the decoy entirely). Without the searchHint, file_edit
    // would be gated out — this discriminates the feature.
    const res = await toolSearchTool.execute({ query: '+patch modify' }, ctx());
    const matches = (res.metadata as { matches?: string[] }).matches ?? [];
    assert.ok(matches.includes('file_edit'), 'file_edit must match via searchHint');
    assert.ok(!matches.includes('decoy_tool'), 'decoy without the term must be gated out');
  });
});

// ---------------------------------------------------------------------------
// Prompt steering in the tool description
// ---------------------------------------------------------------------------

describe('fileEditTool – prompt steering', () => {
  it('steers toward editing existing files over creating new ones', () => {
    assert.ok(/prefer editing an existing file/i.test(fileEditTool.description));
  });

  it('forbids proactively creating *.md / README docs unless requested', () => {
    assert.ok(/never proactively create documentation files/i.test(fileEditTool.description));
    assert.ok(/README/.test(fileEditTool.description));
  });

  it('restricts writing emojis to files unless the user asks', () => {
    assert.ok(/emoji/i.test(fileEditTool.description));
  });
});

// ---------------------------------------------------------------------------
// secretWarning — one canonical message naming labels + sharing risk
// ---------------------------------------------------------------------------

describe('secretWarning message', () => {
  it('names the detected labels and the shared-with-collaborators exposure risk', () => {
    const secrets = scanForSecrets('aws_key=AKIAIOSFODNN7EXAMPLE');
    assert.ok(secrets.length > 0, 'fixture must be detected as a secret');
    const msg = secretWarning(secrets, 'content');
    assert.ok(msg.includes(secrets[0].label), 'message must name the secret label');
    assert.ok(/everyone with access to this repository/i.test(msg), 'message must state the sharing risk');
    // Parity invariant: the raw secret bytes are never echoed.
    assert.ok(!msg.includes('AKIAIOSFODNN7EXAMPLE'));
  });

  it('distinguishes a fresh write ("content") from an in-place edit ("edited content")', () => {
    const secrets = scanForSecrets('aws_key=AKIAIOSFODNN7EXAMPLE');
    assert.ok(secretWarning(secrets, 'content').includes('in content:'));
    assert.ok(secretWarning(secrets, 'edited content').includes('in edited content:'));
  });

  it('an edit that introduces a secret is blocked with the canonical message', async () => {
    const file = path.join(tmpDir, 'conf.txt');
    fs.writeFileSync(file, 'aws_key=PLACEHOLDER\n');
    const c = ctx({ filesRead: new Set([file]) });
    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'PLACEHOLDER', new_string: 'AKIAIOSFODNN7EXAMPLE' },
      c,
    );
    assert.equal(res.isError, true);
    assert.ok(/everyone with access to this repository/i.test(res.output));
    assert.ok(!res.output.includes('AKIAIOSFODNN7EXAMPLE'), 'must not echo the secret bytes');
    // The original file is untouched.
    assert.equal(fs.readFileSync(file, 'utf-8'), 'aws_key=PLACEHOLDER\n');
  });
});
