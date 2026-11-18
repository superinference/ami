import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFileSync } from 'child_process';
import {
  recordAgentFileContribution,
  normalizeAttributionPath,
} from '../src/tools/attribution-store';
import {
  gitCommitTool,
  buildAttributionNote,
  collectCommitFileContributions,
  ATTRIBUTION_NOTES_REF,
} from '../src/tools/git-commit';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

function ctx(cwd: string, overrides?: Partial<ToolContext>): ToolContext {
  return { cwd, abortSignal: new AbortController().signal, ...overrides };
}

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf-8',
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '', GIT_CONFIG_NOSYSTEM: '1' },
    stdio: ['pipe', 'pipe', 'pipe'],
  }).trim();
}

const dirs: string[] = [];
function mkRepo(prefix: string): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), prefix)));
  dirs.push(dir);
  git(dir, 'init', '-q', '-b', 'main');
  git(dir, 'config', 'user.email', 't@t.t');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  return dir;
}

after(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

async function withEnvAsync(vars: Record<string, string | undefined>, fn: () => Promise<void>): Promise<void> {
  const prev: Record<string, string | undefined> = {};
  for (const k of Object.keys(vars)) { prev[k] = process.env[k]; }
  for (const [k, v] of Object.entries(vars)) {
    if (v === undefined) delete process.env[k]; else process.env[k] = v;
  }
  try { await fn(); } finally {
    for (const [k, v] of Object.entries(prev)) {
      if (v === undefined) delete process.env[k]; else process.env[k] = v;
    }
  }
}

// ---------------------------------------------------------------------------
// buildAttributionNote: stagedChangeChars field
// ---------------------------------------------------------------------------

describe('U244 — buildAttributionNote stagedChangeChars', () => {
  it('omits stagedChangeChars when zero / unset', () => {
    const base = buildAttributionNote({ surface: 'cli', model: 'opus', trailerApplied: true, files: ['a'] });
    assert.equal(Object.prototype.hasOwnProperty.call(base, 'stagedChangeChars'), false);
    const zero = buildAttributionNote({ surface: 'cli', model: 'opus', trailerApplied: true, files: ['a'], stagedChangeChars: 0 });
    assert.equal(Object.prototype.hasOwnProperty.call(zero, 'stagedChangeChars'), false);
  });

  it('includes stagedChangeChars when positive', () => {
    const note = buildAttributionNote({
      surface: 'cli', model: 'opus', trailerApplied: true, files: ['a'],
      stagedChangeChars: 120,
    });
    assert.equal(note.stagedChangeChars, 120);
  });
});

// ---------------------------------------------------------------------------
// collectCommitFileContributions: deleted flag
// ---------------------------------------------------------------------------

describe('U244 — collectCommitFileContributions deleted flag', () => {
  it('marks a staged-for-deletion authored file as deleted', () => {
    const dir = mkRepo('u244-del-');
    fs.writeFileSync(path.join(dir, 'd.txt'), 'content\n');
    git(dir, 'add', 'd.txt');
    git(dir, 'commit', '-q', '-m', 'base');
    // Stage the deletion (removes it from disk + indexes the deletion).
    git(dir, 'rm', '-q', 'd.txt');

    const sid = 'u244-del-' + Math.random();
    recordAgentFileContribution(sid, normalizeAttributionPath(path.resolve(dir, 'd.txt'), dir), 'content', 7, 1);

    const out = collectCommitFileContributions(sid, dir, ['d.txt']);
    assert.equal(out.length, 1);
    assert.equal(out[0].deleted, true);
    assert.equal(out[0].totalChars, 0, 'a deleted file has no on-disk content');
  });

  it('does not set deleted for a modified authored file', () => {
    const dir = mkRepo('u244-mod-');
    fs.writeFileSync(path.join(dir, 'm.txt'), 'first\n');
    git(dir, 'add', 'm.txt');
    git(dir, 'commit', '-q', '-m', 'base');
    fs.writeFileSync(path.join(dir, 'm.txt'), 'second\n');
    git(dir, 'add', 'm.txt');

    const sid = 'u244-mod-' + Math.random();
    recordAgentFileContribution(sid, normalizeAttributionPath(path.resolve(dir, 'm.txt'), dir), 'second', 6, 1);

    const out = collectCommitFileContributions(sid, dir, ['m.txt']);
    assert.equal(out.length, 1);
    assert.equal(out[0].deleted, undefined);
  });
});

// ---------------------------------------------------------------------------
// E2E: staged change volume reaches the commit note (red-green on the
// pre-commit capture — reading --cached AFTER the commit yields 0).
// ---------------------------------------------------------------------------

describe('U244 — e2e: stagedChangeChars reaches the commit note', () => {
  it('GREEN: a committed file records a positive stagedChangeChars captured pre-commit', async () => {
    const dir = mkRepo('u244-e2e-');
    const sid = 'u244-e2e-session';
    const body = 'alpha\nbeta\ngamma\ndelta\n'; // 4 inserted lines
    const edit = await fileEditTool.execute(
      { file_path: 'f.txt', new_string: body },
      ctx(dir, { sessionId: sid }),
    );
    assert.ok(!edit.isError, edit.output);

    await withEnvAsync({ AMI_COMMIT_ATTRIBUTION_NOTES: '1' }, async () => {
      const res = await gitCommitTool.execute(
        { message: 'add f', files: ['f.txt'] },
        ctx(dir, { sessionId: sid }),
      );
      assert.equal(res.isError, false, res.output);
    });

    const sha = git(dir, 'rev-parse', 'HEAD');
    const note = JSON.parse(git(dir, 'notes', `--ref=${ATTRIBUTION_NOTES_REF}`, 'show', sha));
    // 4 insertions * 40 chars/line = 160. The exact figure proves the value was
    // captured BEFORE the commit (post-commit `git diff --cached` is empty => 0),
    // discriminating the fix from the old behavior.
    assert.equal(note.stagedChangeChars, 160);
  });
});
