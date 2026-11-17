import { describe, it, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFileSync } from 'child_process';
import {
  gitCommitTool,
  buildAttributionNote,
  attributionNotesEnabled,
  writeCommitAttributionNote,
  ATTRIBUTION_NOTES_REF,
  _gitCommitFs,
} from '../src/tools/git-commit';
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

describe('U105 gap — attributionNotesEnabled gate', () => {
  it('is off by default and honors the truthiness convention', () => {
    assert.equal(attributionNotesEnabled({}), false);
    assert.equal(attributionNotesEnabled({ AMI_COMMIT_ATTRIBUTION_NOTES: '0' }), false);
    assert.equal(attributionNotesEnabled({ AMI_COMMIT_ATTRIBUTION_NOTES: '1' }), true);
    assert.equal(attributionNotesEnabled({ AMI_COMMIT_ATTRIBUTION_NOTES: 'true' }), true);
    assert.equal(attributionNotesEnabled({ AMI_COMMIT_ATTRIBUTION_NOTES: 'YES' }), true);
    assert.equal(attributionNotesEnabled({ AMI_COMMIT_ATTRIBUTION_NOTES: 'on' }), true);
  });
});

describe('U105 gap — buildAttributionNote record shape', () => {
  it('builds the truthfully-computable fields and never fabricates a percentage', () => {
    const fixed = new Date('2026-01-02T03:04:05.000Z');
    const note = buildAttributionNote(
      { surface: 'cli', model: 'opus-4-8', trailerApplied: true, files: ['a.ts', 'b.ts'] },
      fixed,
    );
    assert.deepEqual(note, {
      authoredBy: 'ami',
      surface: 'cli',
      model: 'opus-4-8',
      trailerApplied: true,
      files: ['a.ts', 'b.ts'],
      fileCount: 2,
      committedAt: '2026-01-02T03:04:05.000Z',
    });
    // No fabricated char-level percentage field.
    assert.equal((note as Record<string, unknown>).claudePercent, undefined);
  });

  it('maps an unknown model to null', () => {
    const note = buildAttributionNote({ surface: 'cli', model: undefined, trailerApplied: false, files: [] });
    assert.equal(note.model, null);
    assert.equal(note.fileCount, 0);
  });
});

describe('U105 gap — git_commit writes an attribution note when enabled (e2e)', () => {
  it('records a structured note on refs/notes/ami-attribution (GREEN with the feature)', async () => {
    const dir = mkRepo('ami-attr-note-');
    fs.writeFileSync(path.join(dir, 'f.txt'), 'hello\n');
    await withEnvAsync(
      { AMI_COMMIT_ATTRIBUTION_NOTES: '1', AMI_ENTRYPOINT: 'cli', AMI_COMMIT_MODEL: 'anthropic/claude-opus-4-8-20260115' },
      async () => {
        const res = await gitCommitTool.execute({ message: 'add feature', files: ['f.txt'] }, ctx(dir));
        assert.equal(res.isError, false, res.output);
      },
    );
    const sha = git(dir, 'rev-parse', 'HEAD');
    const raw = git(dir, 'notes', `--ref=${ATTRIBUTION_NOTES_REF}`, 'show', sha);
    const note = JSON.parse(raw);
    assert.equal(note.authoredBy, 'ami');
    assert.equal(note.surface, 'cli');
    assert.equal(note.model, 'opus-4-8');
    assert.equal(note.trailerApplied, true);
    assert.deepEqual(note.files, ['f.txt']);
    assert.equal(note.fileCount, 1);
    assert.match(note.committedAt, /^\d{4}-\d{2}-\d{2}T/);
  });

  it('writes NO note when the knob is unset (RED discriminator: default off)', async () => {
    const dir = mkRepo('ami-attr-off-');
    fs.writeFileSync(path.join(dir, 'g.txt'), 'hi\n');
    await withEnvAsync({ AMI_COMMIT_ATTRIBUTION_NOTES: undefined }, async () => {
      const res = await gitCommitTool.execute({ message: 'add g', files: ['g.txt'] }, ctx(dir));
      assert.equal(res.isError, false, res.output);
    });
    const sha = git(dir, 'rev-parse', 'HEAD');
    let threw = false;
    try { git(dir, 'notes', `--ref=${ATTRIBUTION_NOTES_REF}`, 'show', sha); } catch { threw = true; }
    assert.equal(threw, true, 'no attribution note must exist when the knob is off');
  });

  it('a note-writing failure never fails an otherwise-successful commit (best-effort)', async () => {
    const dir = mkRepo('ami-attr-besteffort-');
    fs.writeFileSync(path.join(dir, 'h.txt'), 'yo\n');
    // Make ONLY the attribution-note temp write throw; the commit-message temp
    // write must still succeed, so the commit lands and the note is skipped.
    const realWrite = _gitCommitFs.writeFileSync;
    mock.method(_gitCommitFs, 'writeFileSync', (file: string, data: unknown, opts: unknown) => {
      if (String(file).includes('ami-attr-note')) throw new Error('boom');
      return (realWrite as (...a: unknown[]) => unknown)(file, data, opts);
    });
    try {
      await withEnvAsync({ AMI_COMMIT_ATTRIBUTION_NOTES: '1' }, async () => {
        const res = await gitCommitTool.execute({ message: 'add h', files: ['h.txt'] }, ctx(dir));
        assert.equal(res.isError, false, res.output);
      });
    } finally {
      mock.restoreAll();
    }
    const sha = git(dir, 'rev-parse', 'HEAD');
    assert.ok(sha.length >= 7, 'commit must have landed despite the note failure');
    let threw = false;
    try { git(dir, 'notes', `--ref=${ATTRIBUTION_NOTES_REF}`, 'show', sha); } catch { threw = true; }
    assert.equal(threw, true, 'the failed note must be absent, commit unaffected');
  });
});

describe('U105 gap — writeCommitAttributionNote direct', () => {
  it('writes and overwrites the note for a commit (idempotent via -f)', async () => {
    const dir = mkRepo('ami-attr-direct-');
    fs.writeFileSync(path.join(dir, 'k.txt'), 'k\n');
    git(dir, 'add', 'k.txt');
    git(dir, 'commit', '-q', '-m', 'seed');
    const sha = git(dir, 'rev-parse', 'HEAD');
    const execOpts = { cwd: dir, timeout: 30_000, env: process.env };
    const note = buildAttributionNote({ surface: 'cli', model: 'x', trailerApplied: true, files: ['k.txt'] });
    await writeCommitAttributionNote(sha, note, execOpts);
    await writeCommitAttributionNote(sha, note, execOpts); // overwrite must not throw
    const raw = git(dir, 'notes', `--ref=${ATTRIBUTION_NOTES_REF}`, 'show', sha);
    assert.equal(JSON.parse(raw).surface, 'cli');
  });

  it('swallows an unlink failure in finally (cleanup seam)', async () => {
    const dir = mkRepo('ami-attr-unlink-');
    fs.writeFileSync(path.join(dir, 'm.txt'), 'm\n');
    git(dir, 'add', 'm.txt');
    git(dir, 'commit', '-q', '-m', 'seed');
    const sha = git(dir, 'rev-parse', 'HEAD');
    const execOpts = { cwd: dir, timeout: 30_000, env: process.env };
    const note = buildAttributionNote({ surface: 'cli', model: undefined, trailerApplied: false, files: [] });
    mock.method(_gitCommitFs, 'unlinkSync', () => { throw new Error('simulated unlink failure'); });
    try {
      // The unlink failure in finally must be swallowed — no throw escapes.
      await assert.doesNotReject(writeCommitAttributionNote(sha, note, execOpts));
    } finally {
      mock.restoreAll();
    }
    const raw = git(dir, 'notes', `--ref=${ATTRIBUTION_NOTES_REF}`, 'show', sha);
    assert.equal(JSON.parse(raw).authoredBy, 'ami');
  });
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
