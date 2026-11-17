import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFileSync } from 'child_process';
import { countCharsAdded } from '../src/tools/tool-utils';
import {
  recordAgentFileContribution,
  getSessionFileAttribution,
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
// Unit: countCharsAdded (the authored-character metric)
// ---------------------------------------------------------------------------

describe('U105 — countCharsAdded', () => {
  it('counts the full length of a brand-new file (empty old)', () => {
    assert.equal(countCharsAdded('', 'hello\nworld'), 'hello\nworld'.length);
  });

  it('counts only the characters of added lines on an edit', () => {
    // old a,b,c -> new a,X,c : only "X" is added (1 char).
    assert.equal(countCharsAdded('a\nb\nc', 'a\nX\nc'), 1);
    // multi-char added line.
    assert.equal(countCharsAdded('a\nc', 'a\nBBBB\nc'), 4);
  });

  it('counts zero for no change, pure deletion, or empty new', () => {
    assert.equal(countCharsAdded('a\nb', 'a\nb'), 0);
    assert.equal(countCharsAdded('a\nb\nc', 'a'), 0);
    assert.equal(countCharsAdded('abc', ''), 0);
  });

  it('uses the net positive length delta for degenerate (huge) inputs', () => {
    const big = Array.from({ length: 3000 }, (_, i) => `l${i}`).join('\n');
    const bigger = big + '\n' + 'x'.repeat(50);
    // Exceeds MAX_DIFF_CELLS (3000x3001) -> fallback = net positive length delta.
    assert.equal(countCharsAdded(big, bigger), bigger.length - big.length);
  });
});

// ---------------------------------------------------------------------------
// Unit: attribution-store (session-scoped accumulation)
// ---------------------------------------------------------------------------

describe('U105 — attribution-store', () => {
  it('accumulates per-file authored characters within a session', () => {
    const sid = 'u105-acc-' + Math.random();
    recordAgentFileContribution(sid, '/abs/f.ts', 'hello', 5, 100);
    recordAgentFileContribution(sid, '/abs/f.ts', 'hello world', 6, 200);
    const state = getSessionFileAttribution(sid)['/abs/f.ts'];
    assert.equal(state.agentContribution, 11);
    assert.ok(state.contentHash.length === 64);
    assert.equal(state.mtime, 200);
  });

  it('isolates sessions and shares a global bucket for blank/undefined ids', () => {
    const a = 'u105-iso-a-' + Math.random();
    const b = 'u105-iso-b-' + Math.random();
    recordAgentFileContribution(a, '/x', 'x', 1, 1);
    assert.deepEqual(getSessionFileAttribution(b), {});
    // undefined and '' map to the same global bucket.
    recordAgentFileContribution(undefined, '/g', 'g', 1, 1);
    assert.ok(getSessionFileAttribution('')['/g']);
    assert.ok(getSessionFileAttribution(undefined)['/g']);
  });
});

// ---------------------------------------------------------------------------
// Unit: collectCommitFileContributions
// ---------------------------------------------------------------------------

describe('U105 — collectCommitFileContributions', () => {
  it('maps staged paths to recorded contributions with a 0 ratio when the file is unreadable', () => {
    const sid = 'u105-collect-' + Math.random();
    const cwd = '/nope-' + Math.random();
    recordAgentFileContribution(sid, path.resolve(cwd, 'f.ts'), 'content', 7, 1);
    const out = collectCommitFileContributions(sid, cwd, ['f.ts', 'untracked.ts']);
    assert.equal(out.length, 1);
    assert.equal(out[0].path, 'f.ts');
    assert.equal(out[0].agentContribution, 7);
    assert.equal(out[0].ratio, 0); // file does not exist -> totalChars 0 -> ratio 0
  });

  it('returns [] when no staged path was authored this session', () => {
    assert.deepEqual(collectCommitFileContributions('u105-empty-' + Math.random(), '/x', ['a', 'b']), []);
  });
});

// ---------------------------------------------------------------------------
// buildAttributionNote: omits the field when empty, includes it when present
// ---------------------------------------------------------------------------

describe('U105 — buildAttributionNote fileContributions', () => {
  it('omits fileContributions when none are provided (backwards-identical shape)', () => {
    const note = buildAttributionNote({ surface: 'cli', model: 'opus', trailerApplied: true, files: ['a'] });
    assert.equal(Object.prototype.hasOwnProperty.call(note, 'fileContributions'), false);
  });

  it('includes fileContributions when provided', () => {
    const note = buildAttributionNote({
      surface: 'cli', model: 'opus', trailerApplied: true, files: ['a'],
      fileContributions: [{ path: 'a', agentContribution: 3, ratio: 0.5 }],
    });
    assert.deepEqual(note.fileContributions, [{ path: 'a', agentContribution: 3, ratio: 0.5 }]);
  });
});

// ---------------------------------------------------------------------------
// E2E red-green: edit tool -> store -> git_commit note fileContributions
// ---------------------------------------------------------------------------

describe('U105 — e2e: per-file agent contribution reaches the commit note', () => {
  it('GREEN: a file authored via file_edit appears in the attribution note', async () => {
    const dir = mkRepo('u105-e2e-');
    const sid = 'u105-e2e-session';
    const body = 'line one\nline two\nline three\n';
    // Author the file through the real edit tool (records the contribution).
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
    assert.ok(Array.isArray(note.fileContributions), 'note must carry fileContributions');
    const fc = note.fileContributions.find((c: { path: string }) => c.path === 'f.txt');
    assert.ok(fc, 'the committed file must have a contribution entry');
    assert.ok(fc.agentContribution > 0, 'authored characters must be counted');
    assert.ok(fc.ratio > 0 && fc.ratio <= 1, `ratio in (0,1], got ${fc.ratio}`);
  });

  it('RED discriminator: a commit under a DIFFERENT session has no fileContributions', async () => {
    const dir = mkRepo('u105-e2e-other-');
    const authorSid = 'u105-author';
    const commitSid = 'u105-other';
    const edit = await fileEditTool.execute(
      { file_path: 'g.txt', new_string: 'some authored text\n' },
      ctx(dir, { sessionId: authorSid }),
    );
    assert.ok(!edit.isError, edit.output);

    await withEnvAsync({ AMI_COMMIT_ATTRIBUTION_NOTES: '1' }, async () => {
      const res = await gitCommitTool.execute(
        { message: 'add g', files: ['g.txt'] },
        ctx(dir, { sessionId: commitSid }),
      );
      assert.equal(res.isError, false, res.output);
    });

    const sha = git(dir, 'rev-parse', 'HEAD');
    const note = JSON.parse(git(dir, 'notes', `--ref=${ATTRIBUTION_NOTES_REF}`, 'show', sha));
    assert.equal(
      Object.prototype.hasOwnProperty.call(note, 'fileContributions'),
      false,
      'a different session must not leak contributions into the note',
    );
  });
});
