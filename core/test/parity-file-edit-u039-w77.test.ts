// Parity unit U039 (wave w77) — symlink-target boundary disclosure on a blocked
// file_edit write.
//
// Claude Code's edit-approval prompt surfaces the symlink boundary to the user
// (ShowInIDEPrompt): when an edit target is a symlink it discloses the resolved
// target and whether it redirects inside or outside the working directory, so the
// user understands the redirection they are approving. AMI takes a STRONGER
// posture — validateFileWritePath blocks EVERY symlink write target — but its bare
// "is a symlink — write blocked" error never told the user WHERE the link pointed,
// so the boundary crossing it was protecting against was invisible. This wave adds
// that disclosure to the block error: the resolved target plus an inside/outside
// classification. Additive transparency atop AMI's block-all-symlinks guard — no
// security weakening (the write is still blocked).
//
// RED before the fix: the block error contained no target path / "inside the
// workspace" / "outside the workspace" disclosure.
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool, disclaimSymlinkTarget } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal, ...overrides };
}

beforeEach(() => {
  tmpDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-fe-w77-')));
});
afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('file_edit – symlink-target boundary disclosure on a blocked write', () => {
  it('discloses an IN-workspace symlink target when blocking the edit', async () => {
    const real = path.join(tmpDir, 'real.txt');
    fs.writeFileSync(real, 'hello\n');
    const link = path.join(tmpDir, 'alias.txt');
    fs.symlinkSync(real, link);

    const res = await fileEditTool.execute(
      { file_path: link, old_string: 'hello', new_string: 'bye' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /symlink/);
    // The write is still blocked — AMI's stronger posture is preserved.
    assert.equal(fs.readFileSync(real, 'utf-8'), 'hello\n');
    // NEW: the block error now discloses the resolved target + its boundary side.
    assert.ok(res.output.includes(real), 'must disclose the resolved symlink target path');
    assert.match(res.output, /inside the workspace/);
  });

  it('discloses an OUT-of-workspace symlink target (helper) + tool still blocks it', async () => {
    const outsideDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-fe-w77-out-')));
    const outside = path.join(outsideDir, 'secret.txt');
    fs.writeFileSync(outside, 'secret\n');
    const link = path.join(tmpDir, 'leak.txt');
    fs.symlinkSync(outside, link);

    try {
      // The helper classifies the redirection as outside the workspace.
      const note = disclaimSymlinkTarget(link, tmpDir);
      assert.ok(note.includes(outside), 'must disclose the out-of-workspace target');
      assert.match(note, /outside the workspace/);
      // Through the tool, an out-of-workspace symlink is refused even earlier
      // (resolveFilePath escape guard) — the write never lands either way.
      const res = await fileEditTool.execute(
        { file_path: link, old_string: 'secret', new_string: 'x' },
        ctx(),
      );
      assert.equal(res.isError, true);
      assert.match(res.output, /symlink/);
      assert.equal(fs.readFileSync(outside, 'utf-8'), 'secret\n');
    } finally {
      fs.rmSync(outsideDir, { recursive: true, force: true });
    }
  });

  it('discloses a DANGLING symlink target by its declared destination (via readlink)', () => {
    // A dangling symlink is refused even earlier (resolveFilePath), so the tool
    // never reaches the validateFileWritePath disclosure site — but the helper
    // itself must still disclose the DECLARED destination of a dangling link
    // (realpath throws → readlink fallback).
    const link = path.join(tmpDir, 'dangling.txt');
    const missing = path.join(tmpDir, 'nonexistent-target.txt');
    fs.symlinkSync(missing, link);

    const note = disclaimSymlinkTarget(link, tmpDir);
    assert.ok(note.includes(missing), 'must disclose the declared (dangling) target');
    assert.match(note, /inside the workspace/);
  });

  it('the tool still blocks a dangling symlink write (stronger posture preserved)', async () => {
    const link = path.join(tmpDir, 'dangling2.txt');
    fs.symlinkSync(path.join(tmpDir, 'nope.txt'), link);
    const res = await fileEditTool.execute(
      { file_path: link, new_string: 'content' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /dangling symlink/);
  });

  it('adds NO disclosure suffix for a non-symlink validation failure', () => {
    // A path with a NUL byte fails validateFileWritePath but is not a symlink, so
    // disclaimSymlinkTarget must contribute nothing.
    assert.equal(disclaimSymlinkTarget(path.join(tmpDir, 'plain.txt'), tmpDir), '');
  });

  it('disclaimSymlinkTarget returns an inside/outside suffix for a live symlink', () => {
    const real = path.join(tmpDir, 'r.txt');
    fs.writeFileSync(real, 'x');
    const link = path.join(tmpDir, 'l.txt');
    fs.symlinkSync(real, link);
    const note = disclaimSymlinkTarget(link, tmpDir);
    assert.ok(note.includes(real));
    assert.match(note, /inside the workspace/);
  });
});
