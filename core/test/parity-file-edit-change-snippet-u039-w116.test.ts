import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import {
  boundedChangeSnippet,
  DIFF_SNIPPET_MAX_BYTES,
  DIFF_SNIPPET_CONTEXT_LINES,
} from '../src/tools/tool-utils';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

/**
 * U039/w116 — Bounded context-8 change snippet for out-of-band file changes.
 *
 * Parity target: the reference's FileEditTool/utils.ts `getSnippetForTwoFileDiff`
 * (references/claude/claude-code-main/src/tools/FileEditTool/utils.ts:L352-406),
 * surfaced via the `edited_text_file` attachment. AMI previously detected an
 * external modification (fileCache.describeChange) and ONLY errored with
 * "File has been modified since you last read it"; it never showed WHAT changed.
 *
 * This adds `boundedChangeSnippet` (context-8 structured patch → surviving
 * non-deleted lines, line-numbered, hunks joined by `...`, byte-capped at 8 KiB
 * with a `[N lines truncated]` marker) and wires it into every stale-file error
 * path of file_edit (single replace, replace_all/multi-edit, full overwrite,
 * and the pre-write re-check).
 */

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal, filesRead: new Set<string>(), ...overrides };
}

/**
 * Seed the per-cwd file cache so {@link getFileCache} reports `path` as changed
 * with the "content" reason (same mtime, different bytes — a formatter rewrote it
 * in place): write NEW to disk, then cache OLD under the file's CURRENT mtime so
 * describeChange re-reads disk and the content hash diverges.
 */
function seedStaleContent(file: string, oldContent: string): void {
  const mtime = fs.statSync(file).mtimeMs;
  getFileCache(tmpDir).set(file, oldContent, mtime);
}

beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fe-snip-w116-')); });
afterEach(() => {
  try { getFileCache(tmpDir).clear(); } catch { /* ignore */ }
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('boundedChangeSnippet (U039/w116)', () => {
  it('returns "" when the two contents are identical (nothing to surface)', () => {
    assert.equal(boundedChangeSnippet('a\nb\nc\n', 'a\nb\nc\n'), '');
  });

  it('shows surviving (non-deleted) lines with 1-based line numbers, dropping removed text', () => {
    const snippet = boundedChangeSnippet('alpha\nbravo\ncharlie\n', 'alpha\nBRAVO\ncharlie\n');
    // The replacement line is shown, line-numbered from the hunk start.
    assert.match(snippet, /2: BRAVO/);
    assert.match(snippet, /1: alpha/);
    assert.match(snippet, /3: charlie/);
    // The removed line's content never appears on its own numbered line, and no
    // raw `-`/`+` diff prefixes leak into the snippet.
    assert.doesNotMatch(snippet, /: bravo$/m);
    assert.doesNotMatch(snippet, /^[+-]/m);
  });

  it('joins non-contiguous change regions with an "..." separator', () => {
    const before = Array.from({ length: 40 }, (_, i) => `line${i}`).join('\n');
    const after = before.replace('line0', 'LINE0').replace('line39', 'LINE39');
    const snippet = boundedChangeSnippet(before, after);
    assert.match(snippet, /LINE0/);
    assert.match(snippet, /LINE39/);
    // Two far-apart edits (> 2 * context lines apart) stay separate hunks.
    assert.ok(snippet.includes('\n...\n'), 'expected a "..." hunk separator');
  });

  it('bounds the snippet to the 8 KiB cap and appends a "[N lines truncated]" marker', () => {
    const after = Array.from({ length: 4000 }, (_, i) => `brand-new-line-${i}`).join('\n');
    const snippet = boundedChangeSnippet('', after);
    assert.match(snippet, /\.\.\. \[\d+ lines truncated\] \.\.\./);
    // The kept prefix is cut at a line boundary within the byte cap (the marker is
    // appended AFTER the cut, so total length is cap + a small marker).
    const marker = snippet.slice(snippet.indexOf('\n\n... ['));
    assert.ok(Buffer.byteLength(snippet) <= DIFF_SNIPPET_MAX_BYTES + Buffer.byteLength(marker));
  });

  it('exports the parity constants (context 8, cap 8192)', () => {
    assert.equal(DIFF_SNIPPET_CONTEXT_LINES, 8);
    assert.equal(DIFF_SNIPPET_MAX_BYTES, 8192);
  });
});

describe('file_edit surfaces the change snippet on an out-of-band modification (U039/w116)', () => {
  it('single-replace: a content-changed file errors WITH the line-numbered change snippet', async () => {
    const file = path.join(tmpDir, 'mod.ts');
    const OLD = 'export const x = 1;\nexport const y = 2;\n';
    const NEW = 'export const x = 1;\nexport const y = 2; // formatted\n';
    fs.writeFileSync(file, NEW);
    seedStaleContent(file, OLD);

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'formatted', new_string: 'edited' },
      ctx({ filesRead: new Set([file]) }),
    );
    assert.equal(result.isError, true);
    assert.equal(result.behavior, 'ask');
    assert.match(result.output, /modified since you last read it/);
    // The additive capability: WHAT changed is surfaced, not only THAT it changed.
    assert.match(result.output, /What changed on disk/);
    assert.match(result.output, /formatted/);
  });

  it('full overwrite (write mode): the stale error uses the "overwriting" verb + snippet', async () => {
    const file = path.join(tmpDir, 'over.txt');
    const OLD = 'first\nsecond\n';
    const NEW = 'first\nsecond-rewritten\n';
    fs.writeFileSync(file, NEW);
    seedStaleContent(file, OLD);

    const result = await fileEditTool.execute(
      { file_path: file, content: 'brand new contents\n' },
      ctx({ filesRead: new Set([file]) }),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /overwriting/);
    assert.match(result.output, /What changed on disk/);
    assert.match(result.output, /second-rewritten/);
  });

  it('multi-edit (edits[]): the stale error carries the snippet too', async () => {
    const file = path.join(tmpDir, 'multi.ts');
    const OLD = 'a\nb\nc\n';
    const NEW = 'a\nb-touched\nc\n';
    fs.writeFileSync(file, NEW);
    seedStaleContent(file, OLD);

    const result = await fileEditTool.execute(
      { file_path: file, edits: [{ old_string: 'touched', new_string: 'z' }] },
      ctx({ filesRead: new Set([file]) }),
    );
    assert.equal(result.isError, true);
    assert.equal(result.behavior, 'ask');
    assert.match(result.output, /What changed on disk/);
    assert.match(result.output, /b-touched/);
  });

  it('a non-text deletion scenario (no cached content) degrades to the plain message, no snippet header', async () => {
    // An mtime-only cache stub holds no content; peekContent returns undefined so
    // the snippet is skipped and only the base stale message is shown.
    const file = path.join(tmpDir, 'mtime.txt');
    fs.writeFileSync(file, 'current\n');
    const cache = getFileCache(tmpDir);
    cache.trackMtime(file, fs.statSync(file).mtimeMs - 5000); // stale mtime, no content
    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'current', new_string: 'x' },
      ctx({ filesRead: new Set([file]) }),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /modified since you last read it|Read the file again/);
    assert.doesNotMatch(result.output, /What changed on disk/);
  });
});
