// Unit U039 (wave w75) — additive parity gaps for core/core/src/tools/file-edit.ts:
//  - Anti-error description hint: the fileEditTool description now explains that
//    file_read prefixes every line with "N\t" and that old_string must contain
//    ONLY the content after that tab, with original indentation preserved
//    (parity: Claude Code's FileEditTool prompt line-number-prefix guidance). The
//    existing "prefer editing existing files" and "only emojis if asked" steering
//    hints are also locked here.
//  - Richer file-not-found recovery: the single-replace ENOENT branch now reuses
//    the canonical suggestSimilarPaths helper (same base name / different
//    extension, case-only mismatch) instead of a crude 3-char-prefix substring
//    scan (parity: Claude Code's findSimilarFile).
//  - Permission-rule validity on settings edits: an edit to a Claude-compatible
//    settings.json/settings.local.json that would INTRODUCE an invalid
//    permissions.{allow,deny,ask} rule into a previously-valid file is blocked,
//    reusing the ONE canonical filterInvalidPermissionRules validator (parity:
//    Claude Code's validateInputForSettingsFileEdit permission-rule validation).
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    detachedMode: true,
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-u039w75-')));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('fileEditTool – description steering + anti-error hints', () => {
  it('explains the file_read "N<tab>" line-number prefix and that old_string omits it', () => {
    const d = fileEditTool.description;
    // The literal backslash-t escape is what appears in the description source.
    assert.ok(d.includes('N\\t'), 'must document the "N\\t" line-number prefix');
    assert.ok(/AFTER that tab/.test(d), 'must say old_string is the content AFTER the tab');
    assert.ok(/indentation/.test(d), 'must tell the model to preserve original indentation');
  });

  it('keeps the "prefer editing existing files" steering hint', () => {
    assert.ok(/prefer editing an existing file/i.test(fileEditTool.description));
  });

  it('keeps the "only emojis if the user asks" content-quality hint', () => {
    assert.ok(/emojis? into a file only if the User explicitly asks|emojis? into a file if the User explicitly asks/i.test(fileEditTool.description));
  });
});

describe('fileEditTool – richer file-not-found recovery', () => {
  it('suggests a same-basename/different-extension sibling', async () => {
    fs.writeFileSync(path.join(tmpDir, 'config.toml'), 'x = 1\n');
    const missing = path.join(tmpDir, 'config.ts');

    const res = await fileEditTool.execute(
      { file_path: missing, old_string: 'a', new_string: 'b' },
      ctx({ filesRead: new Set() }),
    );

    assert.equal(res.isError, true, res.output);
    assert.match(res.output, /File not found/);
    assert.match(res.output, /Did you mean:[^\n]*config\.toml/, res.output);
    // The cwd note is still appended so the model can self-correct a path.
    assert.match(res.output, /your current working directory is/, res.output);
  });

  it('does not over-match on a mere shared 3-letter prefix', async () => {
    // "abandon.ts" shares the first 3 letters ("aba") with "abacus.ts" but has a
    // different base name and extension stem, so it is NOT a same-stem suggestion.
    fs.writeFileSync(path.join(tmpDir, 'abandon.ts'), 'x\n');
    const missing = path.join(tmpDir, 'abacus.md');

    const res = await fileEditTool.execute(
      { file_path: missing, old_string: 'a', new_string: 'b' },
      ctx({ filesRead: new Set() }),
    );

    assert.equal(res.isError, true, res.output);
    assert.ok(!/abandon\.ts/.test(res.output), 'must not suggest an unrelated 3-char-prefix match');
  });
});

describe('fileEditTool – settings permission-rule validation', () => {
  function writeSettings(obj: unknown): string {
    const file = path.join(tmpDir, 'settings.json');
    fs.writeFileSync(file, JSON.stringify(obj, null, 2) + '\n');
    return file;
  }

  it('blocks an edit that introduces an invalid permission rule into a valid file', async () => {
    const file = writeSettings({ permissions: { allow: ['bash(npm:*)'] } });

    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'bash(npm:*)', new_string: 'bash(:*)' },
      ctx({ filesRead: new Set([file]) }),
    );

    assert.equal(res.isError, true, res.output);
    assert.match(res.output, /invalid permission rule/i, res.output);
    assert.match(res.output, /bash\(:\*\)/, res.output);
    // The file is NOT modified — the bad rule never lands.
    assert.match(fs.readFileSync(file, 'utf-8'), /bash\(npm:\*\)/);
  });

  it('allows an edit that keeps the permission rules valid', async () => {
    const file = writeSettings({ permissions: { allow: ['bash(npm:*)'] } });

    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'bash(npm:*)', new_string: 'bash(git:*)' },
      ctx({ filesRead: new Set([file]) }),
    );

    assert.equal(res.isError ?? false, false, res.output);
    assert.match(fs.readFileSync(file, 'utf-8'), /bash\(git:\*\)/);
  });

  it('allows a repair when the file already had an invalid rule (valid->valid-or-invalid is not guarded)', async () => {
    // Baseline is ALREADY invalid (bash(:*)), so the valid->invalid guard does not
    // apply: a repair-in-progress is never blocked.
    const file = writeSettings({ permissions: { allow: ['bash(:*)'] }, model: 'x' });

    const res = await fileEditTool.execute(
      { file_path: file, old_string: '"model": "x"', new_string: '"model": "y"' },
      ctx({ filesRead: new Set([file]) }),
    );

    assert.equal(res.isError ?? false, false, res.output);
    assert.match(fs.readFileSync(file, 'utf-8'), /"model": "y"/);
  });

  it('ignores a non-settings JSON file (package.json permissions key is not a rule block)', async () => {
    const file = path.join(tmpDir, 'package.json');
    fs.writeFileSync(file, JSON.stringify({ name: 'pkg', permissions: { allow: ['bash(:*)'] } }, null, 2) + '\n');

    const res = await fileEditTool.execute(
      { file_path: file, old_string: '"name": "pkg"', new_string: '"name": "pkg2"' },
      ctx({ filesRead: new Set([file]) }),
    );

    // package.json is not a permission settings file; the permission-rule guard
    // does not fire (only JSON syntax + AMI-config schema apply there).
    assert.equal(res.isError ?? false, false, res.output);
  });
});
